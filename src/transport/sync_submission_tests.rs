use super::*;
use crate::messages::encode_raw_length;
use crate::testdata::builders::contracts::matching_symbols_request;
use crate::testdata::builders::RequestEncoder;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone, Debug, Default)]
struct SubmissionStream {
    inner: MemoryStream,
    fail: Arc<AtomicBool>,
    attempts: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl Io for SubmissionStream {
    fn read_message(&self) -> Result<Vec<u8>, Error> {
        self.inner.read_message()
    }

    fn write_all(&self, bytes: &[u8]) -> Result<(), Error> {
        self.attempts.lock().unwrap().push(bytes.to_vec());
        if self.fail.load(Ordering::SeqCst) {
            self.inner.write_all(&bytes[..2])?;
            return Err(Error::Io(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "synthetic write failure")));
        }
        self.inner.write_all(bytes)
    }
}

impl Reconnect for SubmissionStream {
    fn reconnect(&self) -> Result<(), Error> {
        self.inner.reconnect()
    }

    fn sleep(&self, duration: std::time::Duration, shutdown: &ShutdownSignal) {
        self.inner.sleep(duration, shutdown);
    }

    fn shutdown_read(&self) -> Result<(), Error> {
        self.inner.shutdown_read()
    }
}

impl Stream for SubmissionStream {}

#[derive(Clone, Copy, Debug)]
enum Registration {
    Request,
    Order,
}

impl Registration {
    fn registered(self, bus: &TcpMessageBus<SubmissionStream>) -> bool {
        match self {
            Self::Request => bus.requests.contains(&42),
            Self::Order => bus.orders.contains(&42),
        }
    }

    fn submit(self, bus: &TcpMessageBus<SubmissionStream>) -> Result<InternalSubscription, Error> {
        let packet = matching_symbols_request().request_id(42).pattern("AAP").encode_request();
        match self {
            Self::Request => bus.send_request(42, &packet),
            Self::Order => bus.send_order_request(42, &packet),
        }
    }
}

fn make_bus(fail: bool) -> (SubmissionStream, TcpMessageBus<SubmissionStream>) {
    let stream = SubmissionStream::default();
    stream.fail.store(fail, Ordering::SeqCst);
    let connection = Connection::stubbed(stream.clone(), 28);
    (stream, TcpMessageBus::new(connection).unwrap())
}

/// No cleanup thread is started: retain and deliver the actual drop signal
/// deterministically to the production handler, including after replacement.
fn process_one_cleanup(bus: &TcpMessageBus<SubmissionStream>) {
    match bus.signals_recv.try_recv().expect("submission did not send cleanup") {
        Signal::Request(id, sender) => bus.clean_request(id, &sender),
        Signal::Order(id, sender) => bus.clean_order(id, &sender),
        Signal::OrderUpdateStream(_) => panic!("unexpected order-update cleanup signal"),
    }
    assert!(bus.signals_recv.is_empty(), "unexpected additional cleanup");
}

#[test]
fn failed_writes_release_request_and_order_registrations() {
    for kind in [Registration::Request, Registration::Order] {
        let (stream, bus) = make_bus(true);
        let result = kind.submit(&bus);
        assert!(matches!(result, Err(Error::Io(ref error)) if error.kind() == std::io::ErrorKind::BrokenPipe));
        let expected = encode_raw_length(&matching_symbols_request().request_id(42).pattern("AAP").encode_request());
        assert_eq!(stream.attempts.lock().unwrap().as_slice(), std::slice::from_ref(&expected));
        assert_eq!(stream.inner.captured(), expected[..2]);
        process_one_cleanup(&bus);
        assert!(!kind.registered(&bus), "{kind:?}: failed-write registration leaked");
    }
}

#[test]
fn successful_writes_transfer_cleanup_to_the_returned_subscription() {
    for kind in [Registration::Request, Registration::Order] {
        let (stream, bus) = make_bus(false);
        let subscription = kind.submit(&bus).unwrap();
        let expected = encode_raw_length(&matching_symbols_request().request_id(42).pattern("AAP").encode_request());
        assert_eq!(stream.attempts.lock().unwrap().as_slice(), std::slice::from_ref(&expected));
        assert_eq!(stream.inner.captured(), expected);
        assert!(kind.registered(&bus));
        assert!(bus.signals_recv.is_empty());
        drop(subscription);
        process_one_cleanup(&bus);
        assert!(!kind.registered(&bus));
    }
}

#[test]
fn failed_submission_cleanup_preserves_a_newer_registration() {
    for kind in [Registration::Request, Registration::Order] {
        let (stream, bus) = make_bus(true);
        assert!(kind.submit(&bus).is_err());
        stream.fail.store(false, Ordering::SeqCst);
        let replacement = kind.submit(&bus).unwrap();
        process_one_cleanup(&bus);
        assert!(kind.registered(&bus), "stale cleanup removed replacement");
        drop(replacement);
        process_one_cleanup(&bus);
        assert!(!kind.registered(&bus));
    }
}
