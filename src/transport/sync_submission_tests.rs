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

#[test]
fn bounded_write_failure_retires_before_later_writes_or_handshake() {
    let (stream, bus) = make_bus(true);
    let packet = matching_symbols_request().request_id(42).pattern("AAP").encode_request();
    assert!(matches!(bus.send_bounded(&packet), Err(Error::Io(_))));
    stream.fail.store(false, Ordering::SeqCst);
    assert!(matches!(bus.send_message(&packet), Err(Error::Shutdown)));
    assert!(matches!(bus.connection.handshake(), Err(Error::Shutdown)));
    assert_eq!(stream.attempts.lock().unwrap().len(), 1);
    assert_eq!(stream.inner.captured(), encode_raw_length(&packet)[..2]);
}

#[test]
fn bounded_success_does_not_retire_client() {
    let (stream, bus) = make_bus(false);
    let packet = matching_symbols_request().request_id(42).pattern("AAP").encode_request();
    bus.send_bounded(&packet).unwrap();
    assert!(!bus.shutdown.is_requested());
    assert_eq!(stream.inner.captured(), encode_raw_length(&packet));
}

#[test]
fn bounded_public_start_or_cancel_failure_retires_without_retry() {
    use crate::contracts::{Contract, QueryDisposition, QueryLimits};
    for cancel in [false, true] {
        let (stream, bus) = make_bus(!cancel);
        let bus = Arc::new(bus);
        let client = crate::client::blocking::Client::stubbed(bus.clone(), crate::server_versions::CANCEL_CONTRACT_DATA);
        let mut query = client
            .prepare_contract_details(&Contract::stock("SYNTH").build(), QueryLimits::default())
            .unwrap();
        let id = query.request_id();
        if cancel {
            query.start().unwrap();
            stream.fail.store(true, Ordering::SeqCst);
            assert!(matches!(query.request_cancel(), Err(Error::Io(_))));
        } else {
            assert!(matches!(query.start(), Err(Error::Io(_))));
        }
        assert_eq!(query.disposition(), QueryDisposition::RetireRequired);
        assert!(!bus.is_connected());
        assert!(bus.bounded_requests.get(id).is_none());
        assert_eq!(stream.attempts.lock().unwrap().len(), if cancel { 2 } else { 1 });
        assert!(matches!(query.next_until(std::time::Instant::now()), Err(Error::Shutdown)));
        assert!(matches!(query.request_cancel(), Err(Error::Shutdown)));
        assert!(matches!(query.drain_until(std::time::Instant::now()), Err(Error::Shutdown)));
    }
}

#[test]
fn bounded_failed_cancel_keeps_buffered_prefix_readable_then_fuses() {
    use crate::common::test_utils::helpers::binary_proto;
    use crate::contracts::{Contract, QueryLimits};
    use crate::subscriptions::SubscriptionItem;
    use crate::testdata::builders::contracts::contract_data;
    use crate::testdata::builders::ResponseProtoEncoder;
    let (stream, bus) = make_bus(false);
    let bus = Arc::new(bus);
    let client = crate::client::blocking::Client::stubbed(bus.clone(), crate::server_versions::CANCEL_CONTRACT_DATA);
    let mut query = client
        .prepare_contract_details(&Contract::stock("SYNTH").build(), QueryLimits::default())
        .unwrap();
    query.start().unwrap();
    stream.inner.push_inbound(binary_proto(
        IncomingMessages::ContractData as i32,
        &contract_data().request_id(query.request_id()).contract_id(123).to_proto(),
    ));
    bus.dispatch().unwrap();
    stream.fail.store(true, Ordering::SeqCst);
    assert!(matches!(query.request_cancel(), Err(Error::Io(_))));
    assert!(!bus.is_connected());
    assert!(matches!(query.next_until(std::time::Instant::now()).unwrap(), Some(SubscriptionItem::Data(row)) if row.contract.contract_id == 123));
    assert!(matches!(query.next_until(std::time::Instant::now()), Err(Error::Shutdown)));
    assert!(query.next_until(std::time::Instant::now()).unwrap().is_none());
}

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
