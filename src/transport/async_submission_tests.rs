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

#[tokio::test]
async fn bounded_partial_write_drop_retires_before_queued_legacy_writer_runs() {
    let (stream, bus) = make_bus(WriteMode::Pending);
    let packet = matching_symbols_request().request_id(42).pattern("AAP").encode_request();
    let mut bounded = Box::pin(bus.send_bounded(packet.clone()));
    assert!(futures::poll!(bounded.as_mut()).is_pending());
    assert_one_attempt(&stream, true);
    let mut queued = Box::pin(bus.send_message(packet));
    assert!(futures::poll!(queued.as_mut()).is_pending());
    drop(bounded);
    *stream.mode.lock().unwrap() = WriteMode::Succeed;
    assert!(matches!(queued.await, Err(Error::Shutdown)));
    assert!(bus.shutdown.is_requested());
    assert_one_attempt(&stream, true);
    assert!(matches!(bus.connection.handshake().await, Err(Error::Shutdown)));
    assert_one_attempt(&stream, true);
}

#[tokio::test]
async fn bounded_write_failure_retires_but_success_keeps_session_reusable() {
    for mode in [WriteMode::Fail, WriteMode::Succeed] {
        let (stream, bus) = make_bus(mode);
        let packet = matching_symbols_request().request_id(42).pattern("AAP").encode_request();
        let result = bus.send_bounded(packet.clone()).await;
        if matches!(mode, WriteMode::Fail) {
            assert!(matches!(result, Err(Error::Io(_))));
            *stream.mode.lock().unwrap() = WriteMode::Succeed;
            assert!(matches!(bus.send_message(packet).await, Err(Error::Shutdown)));
            assert_one_attempt(&stream, true);
        } else {
            result.unwrap();
            assert!(!bus.shutdown.is_requested());
            assert_one_attempt(&stream, false);
        }
    }
}

#[tokio::test]
async fn bounded_public_start_drop_and_cancel_drop_close_owned_registration() {
    use crate::contracts::{Contract, QueryDisposition, QueryLimits};
    for cancel in [false, true] {
        let (stream, bus) = make_bus(if cancel { WriteMode::Succeed } else { WriteMode::Pending });
        let client = crate::Client::stubbed(bus.clone(), crate::server_versions::CANCEL_CONTRACT_DATA);
        let mut query = client
            .prepare_contract_details(&Contract::stock("SYNTH").build(), QueryLimits::default())
            .unwrap();
        let id = query.request_id();
        if cancel {
            query.start().await.unwrap();
            *stream.mode.lock().unwrap() = WriteMode::Pending;
            let mut write = Box::pin(query.request_cancel());
            assert!(futures::poll!(write.as_mut()).is_pending());
            drop(write);
        } else {
            let mut write = Box::pin(query.start());
            assert!(futures::poll!(write.as_mut()).is_pending());
            drop(write);
        }
        assert_eq!(query.disposition(), QueryDisposition::RetireRequired);
        assert!(!bus.is_connected());
        assert!(bus.bounded_requests.get(id).is_none());
        assert_eq!(stream.attempts.lock().unwrap().len(), if cancel { 2 } else { 1 });
        *stream.mode.lock().unwrap() = WriteMode::Succeed;
        assert!(matches!(bus.send_message(vec![]).await, Err(Error::Shutdown)));
    }
}

#[tokio::test]
async fn bounded_public_failed_write_retires_without_retry() {
    let (_, bus) = make_bus(WriteMode::Fail);
    let client = crate::Client::stubbed(bus.clone(), crate::server_versions::CANCEL_CONTRACT_DATA);
    let mut query = client
        .prepare_matching_symbols("SYNTH", crate::contracts::QueryLimits::default())
        .unwrap();
    let id = query.request_id();
    assert!(matches!(query.start().await, Err(Error::Io(_))));
    assert_eq!(query.disposition(), crate::contracts::QueryDisposition::RetireRequired);
    assert!(bus.bounded_requests.get(id).is_none());
    assert!(matches!(query.next().await, Err(Error::Shutdown)));
    assert!(matches!(query.request_cancel().await, Err(Error::Shutdown)));
    assert!(matches!(query.drain_until(tokio::time::Instant::now()).await, Err(Error::Shutdown)));
}

#[tokio::test]
async fn bounded_failed_cancel_keeps_buffered_prefix_readable_then_fuses() {
    use crate::common::test_utils::helpers::binary_proto;
    use crate::contracts::{Contract, QueryLimits};
    use crate::subscriptions::SubscriptionItem;
    use crate::testdata::builders::contracts::contract_data;
    use crate::testdata::builders::ResponseProtoEncoder;
    let (stream, bus) = make_bus(WriteMode::Succeed);
    let client = crate::Client::stubbed(bus.clone(), crate::server_versions::CANCEL_CONTRACT_DATA);
    let mut query = client
        .prepare_contract_details(&Contract::stock("SYNTH").build(), QueryLimits::default())
        .unwrap();
    query.start().await.unwrap();
    stream.inner.push_inbound(binary_proto(
        IncomingMessages::ContractData as i32,
        &contract_data().request_id(query.request_id()).contract_id(123).to_proto(),
    ));
    bus.read_and_route_message().await.unwrap();
    *stream.mode.lock().unwrap() = WriteMode::Fail;
    assert!(matches!(query.request_cancel().await, Err(Error::Io(_))));
    assert!(!bus.is_connected());
    assert!(matches!(query.next().await.unwrap(), Some(SubscriptionItem::Data(row)) if row.contract.contract_id == 123));
    assert!(matches!(query.next().await, Err(Error::Shutdown)));
    assert!(query.next().await.unwrap().is_none());
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
