use super::tests::drain_cleanup_signals;
use super::*;
use crate::messages::encode_raw_length;
use crate::testdata::builders::contracts::matching_symbols_request;
use crate::testdata::builders::RequestEncoder;
use std::sync::Mutex;

#[derive(Clone, Copy, Debug, Default)]
enum WriteMode {
    #[default]
    Succeed,
    Fail,
    Pending,
}

/// Frame I/O seam: failed or suspended writes have already written a prefix.
/// No socket, background writer or live broker is involved.
#[derive(Clone, Debug, Default)]
struct SubmissionStream {
    inner: MemoryStream,
    mode: Arc<Mutex<WriteMode>>,
    attempts: Arc<Mutex<Vec<Vec<u8>>>>,
}

#[async_trait]
impl AsyncIo for SubmissionStream {
    async fn read_message(&self) -> Result<Vec<u8>, Error> {
        self.inner.read_message().await
    }

    async fn write_all(&self, bytes: &[u8]) -> Result<(), Error> {
        self.attempts.lock().unwrap().push(bytes.to_vec());
        let mode = *self.mode.lock().unwrap();
        if matches!(mode, WriteMode::Succeed) {
            return self.inner.write_all(bytes).await;
        }
        self.inner.write_all(&bytes[..2]).await?;
        if matches!(mode, WriteMode::Fail) {
            Err(Error::Io(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "synthetic write failure")))
        } else {
            std::future::pending().await
        }
    }
}

#[async_trait]
impl AsyncReconnect for SubmissionStream {
    async fn reconnect(&self) -> Result<(), Error> {
        self.inner.reconnect().await
    }

    async fn sleep(&self, duration: Duration, shutdown: &ShutdownSignal) {
        self.inner.sleep(duration, shutdown).await;
    }
}

impl AsyncStream for SubmissionStream {}

#[derive(Clone, Copy, Debug)]
enum Registration {
    Request,
    Order,
}

impl Registration {
    fn channels(self, bus: &AsyncTcpMessageBus<SubmissionStream>) -> &RwLock<HashMap<i32, BroadcastSender>> {
        match self {
            Self::Request => &bus.request_channels,
            Self::Order => &bus.order_channels,
        }
    }

    async fn submit(self, bus: &AsyncTcpMessageBus<SubmissionStream>) -> Result<AsyncInternalSubscription, Error> {
        let packet = matching_symbols_request().request_id(42).pattern("AAP").encode_request();
        // The wire payload is irrelevant to registration ownership. Both
        // actual submission methods must own their channel before writing.
        match self {
            Self::Request => bus.send_request(42, packet).await,
            Self::Order => bus.send_order_request(42, packet).await,
        }
    }
}

fn make_bus(mode: WriteMode) -> (SubmissionStream, Arc<AsyncTcpMessageBus<SubmissionStream>>) {
    let stream = SubmissionStream::default();
    *stream.mode.lock().unwrap() = mode;
    let connection = AsyncConnection::stubbed(stream.clone(), 28);
    (stream, Arc::new(AsyncTcpMessageBus::new(connection).unwrap()))
}

fn assert_one_attempt(stream: &SubmissionStream, partial: bool) {
    let expected = encode_raw_length(&matching_symbols_request().request_id(42).pattern("AAP").encode_request());
    assert_eq!(stream.attempts.lock().unwrap().as_slice(), std::slice::from_ref(&expected));
    assert_eq!(stream.inner.captured(), if partial { expected[..2].to_vec() } else { expected });
}

#[tokio::test]
async fn failed_writes_release_request_and_order_registrations() {
    for kind in [Registration::Request, Registration::Order] {
        let (stream, bus) = make_bus(WriteMode::Fail);
        let result = kind.submit(&bus).await;
        assert!(matches!(result, Err(Error::Io(ref error)) if error.kind() == std::io::ErrorKind::BrokenPipe));
        assert_one_attempt(&stream, true);
        *stream.mode.lock().unwrap() = WriteMode::Succeed;
        drain_cleanup_signals(&bus).await;
        assert!(
            !kind.channels(&bus).read().await.contains_key(&42),
            "{kind:?}: failed-write registration leaked"
        );
    }
}

#[tokio::test]
async fn dropping_pending_writes_releases_request_and_order_registrations() {
    for kind in [Registration::Request, Registration::Order] {
        let (stream, bus) = make_bus(WriteMode::Pending);
        let mut submitting = Box::pin(kind.submit(&bus));
        assert!(futures::poll!(submitting.as_mut()).is_pending());
        assert!(kind.channels(&bus).read().await.contains_key(&42));
        assert_one_attempt(&stream, true);
        drop(submitting);
        *stream.mode.lock().unwrap() = WriteMode::Succeed;
        drain_cleanup_signals(&bus).await;
        assert!(
            !kind.channels(&bus).read().await.contains_key(&42),
            "{kind:?}: dropped-write registration leaked"
        );
    }
}

#[tokio::test]
async fn dropping_before_registration_writes_nothing() {
    for kind in [Registration::Request, Registration::Order] {
        let (stream, bus) = make_bus(WriteMode::Succeed);
        let gate = kind.channels(&bus).write().await;
        let mut submitting = Box::pin(kind.submit(&bus));
        assert!(futures::poll!(submitting.as_mut()).is_pending());
        drop(submitting);
        assert!(stream.attempts.lock().unwrap().is_empty());
        assert!(stream.inner.captured().is_empty());
        drop(gate);
        drain_cleanup_signals(&bus).await;
        assert!(!kind.channels(&bus).read().await.contains_key(&42));
    }
}

#[tokio::test]
async fn successful_writes_transfer_cleanup_to_the_returned_subscription() {
    for kind in [Registration::Request, Registration::Order] {
        let (stream, bus) = make_bus(WriteMode::Succeed);
        let mut subscription = kind.submit(&bus).await.unwrap();
        assert_one_attempt(&stream, false);
        drain_cleanup_signals(&bus).await;
        let sender = kind.channels(&bus).read().await.get(&42).expect("live registration removed").clone();
        sender.send(RoutedItem::Error(Error::Cancelled)).unwrap();
        assert!(matches!(subscription.try_next_routed(), Some(RoutedItem::Error(Error::Cancelled))));
        drop(subscription);
        drain_cleanup_signals(&bus).await;
        assert!(!kind.channels(&bus).read().await.contains_key(&42));
    }
}

#[tokio::test]
async fn abandoned_submission_cleanup_preserves_a_newer_registration() {
    for kind in [Registration::Request, Registration::Order] {
        let (stream, bus) = make_bus(WriteMode::Pending);
        // Stall the FIFO cleanup worker at an unrelated map. The abandoned
        // submission's signal cannot run until the replacement is live.
        let cleanup_gate = bus.order_update_stream.write().await;
        bus.cleanup_sender.send(CleanupSignal::OrderUpdateStream).unwrap();
        let mut submitting = Box::pin(kind.submit(&bus));
        assert!(futures::poll!(submitting.as_mut()).is_pending());
        drop(submitting);
        *stream.mode.lock().unwrap() = WriteMode::Succeed;
        let mut replacement = kind.submit(&bus).await.unwrap();
        drop(cleanup_gate);
        drain_cleanup_signals(&bus).await;
        let sender = kind
            .channels(&bus)
            .read()
            .await
            .get(&42)
            .expect("stale cleanup removed replacement")
            .clone();
        sender.send(RoutedItem::Error(Error::Cancelled)).unwrap();
        assert!(matches!(replacement.try_next_routed(), Some(RoutedItem::Error(Error::Cancelled))));
        drop(replacement);
        drain_cleanup_signals(&bus).await;
        assert!(!kind.channels(&bus).read().await.contains_key(&42));
    }
}
