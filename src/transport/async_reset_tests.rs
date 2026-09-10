use super::tests::make_bus;
use super::*;
use crate::common::test_utils::helpers::{binary_proto, managed_accounts_frame, next_valid_id_frame, TEST_REQ_ID_FIRST};
use crate::messages::encode_raw_length;
use crate::orders::OrderStatusKind;
use crate::server_versions;
use crate::testdata::builders::contracts::{matching_symbols_request, symbol_samples, symbol_samples_entry};
use crate::testdata::builders::orders::{open_order_end, order_status};
use crate::testdata::builders::{RequestEncoder, ResponseProtoEncoder};
use std::sync::atomic::AtomicUsize;
use std::sync::Mutex;
use tokio::sync::Notify;

const DEADLINE: Duration = Duration::from_secs(5);

/// Only the first dispatcher read loses the connection. The real reconnect
/// path must consume the queued handshake before it can reset registrations.
#[derive(Clone, Debug, Default)]
struct ResetOnceStream {
    inner: MemoryStream,
    loss_delivered: Arc<AtomicBool>,
    reconnects: Arc<AtomicUsize>,
    writes: Arc<Mutex<Vec<Vec<u8>>>>,
    written: Arc<Notify>,
}

impl ResetOnceStream {
    async fn wait_for_writes(&self, count: usize) -> Vec<Vec<u8>> {
        tokio::time::timeout(DEADLINE, async {
            loop {
                let notified = self.written.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                {
                    let writes = self.writes.lock().unwrap();
                    if writes.len() >= count {
                        return writes.clone();
                    }
                }
                notified.await;
            }
        })
        .await
        .expect("expected outbound write did not occur")
    }
}

#[async_trait]
impl AsyncIo for ResetOnceStream {
    async fn read_message(&self) -> Result<Vec<u8>, Error> {
        if !self.loss_delivered.swap(true, Ordering::SeqCst) {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "synthetic connection loss",
            )));
        }
        self.inner.read_message().await
    }

    async fn write_all(&self, bytes: &[u8]) -> Result<(), Error> {
        self.inner.write_all(bytes).await?;
        self.writes.lock().unwrap().push(bytes.to_vec());
        self.written.notify_one();
        Ok(())
    }
}

#[async_trait]
impl AsyncReconnect for ResetOnceStream {
    async fn reconnect(&self) -> Result<(), Error> {
        self.reconnects.fetch_add(1, Ordering::SeqCst);
        self.inner.reconnect().await
    }

    async fn sleep(&self, duration: Duration, shutdown: &ShutdownSignal) {
        self.inner.sleep(duration, shutdown).await;
    }
}

impl AsyncStream for ResetOnceStream {}

fn reconnecting_bus() -> (ResetOnceStream, Arc<AsyncTcpMessageBus<ResetOnceStream>>) {
    let stream = ResetOnceStream::default();
    let version = server_versions::PROTOBUF_REST_MESSAGES_3;
    stream.inner.push_inbound(format!("{version}\020240120 12:00:00 EST\0").into_bytes());
    stream.inner.push_inbound(next_valid_id_frame(90));
    stream.inner.push_inbound(managed_accounts_frame("DU_SYNTHETIC"));
    let connection = AsyncConnection::stubbed(stream.clone(), 28);
    connection.set_server_version_for_test(version);
    let bus = Arc::new(AsyncTcpMessageBus::new(connection).unwrap());
    (stream, bus)
}

#[tokio::test]
async fn reconnect_preserves_symbol_retry_registered_during_reset() {
    let (stream, bus) = reconnecting_bus();
    let client = Arc::new(crate::Client::stubbed(bus.clone(), server_versions::PROTOBUF_REST_MESSAGES_3));
    let requester = client.clone();
    let search = tokio::spawn(async move { requester.matching_symbols("AAP").await });
    stream.wait_for_writes(1).await;

    // Stop reset at the order phase, after request subscribers were notified.
    // The retry must register AND write while this guard is still held.
    let order_phase = bus.order_channels.write().await;
    bus.clone().process_messages(0, Duration::ZERO).unwrap();
    let writes = stream.wait_for_writes(4).await;
    assert_eq!(stream.reconnects.load(Ordering::SeqCst), 1, "must traverse real reconnect");
    assert_eq!(writes.len(), 4, "search, handshake, start-api, retry");
    for (index, request_id) in [(0, TEST_REQ_ID_FIRST), (3, TEST_REQ_ID_FIRST + 1)] {
        let request = matching_symbols_request().request_id(request_id).pattern("AAP").encode_request();
        assert_eq!(writes[index], encode_raw_length(&request));
    }
    assert!(writes[1].starts_with(b"API\0"), "reconnect must replay handshake");
    let connected_during_reset = bus.is_connected();

    stream.inner.push_inbound(binary_proto(
        IncomingMessages::SymbolSamples as i32,
        &symbol_samples()
            .request_id(TEST_REQ_ID_FIRST + 1)
            .entry(symbol_samples_entry(12345, "AAPL"))
            .to_proto(),
    ));
    drop(order_phase);
    let symbols = tokio::time::timeout(DEADLINE, search)
        .await
        .expect("retry did not finish")
        .expect("request task panicked")
        .expect("reset cleared the newly registered retry");
    assert_eq!(symbols.len(), 1);
    assert_eq!(symbols[0].contract.contract_id, 12345);
    assert_eq!(symbols[0].contract.symbol.as_str(), "AAPL");
    assert!(!connected_during_reset, "ready cannot precede completed reset handling");
    assert!(bus.is_connected());
    assert!(bus.process_task.read().await.is_some());
    bus.ensure_shutdown().await;
    assert!(!bus.is_connected());
}

async fn expect_reset(subscription: &mut AsyncInternalSubscription) {
    let item = tokio::time::timeout(DEADLINE, subscription.next_routed()).await.unwrap();
    assert!(matches!(item, Some(RoutedItem::Error(Error::ConnectionReset))), "{item:?}");
}

#[tokio::test]
async fn shutdown_during_reset_does_not_mark_connection_ready_again() {
    let (_, bus) = reconnecting_bus();
    let mut old = bus.send_request(100, vec![]).await.unwrap();
    let order_phase = bus.order_channels.write().await;
    bus.clone().process_messages(0, Duration::ZERO).unwrap();
    expect_reset(&mut old).await;

    // Join the dispatcher itself before cleanup can overwrite its ready flag.
    // The handle is installed by a separate task; wait for that positive event.
    tokio::time::timeout(DEADLINE, async {
        let processing = loop {
            if let Some(handle) = bus.process_task.write().await.take() {
                break handle;
            }
            tokio::task::yield_now().await;
        };
        bus.request_shutdown_sync();
        drop(order_phase);
        processing.await.expect("dispatcher task panicked");
    })
    .await
    .expect("dispatcher handle was not installed or shutdown did not finish");
    assert!(!bus.is_connected());
    assert!(!bus.connected.load(Ordering::Relaxed), "reset must not publish readiness after shutdown");
    bus.ensure_shutdown().await;
}

#[tokio::test]
async fn reset_preserves_order_replacement_after_notification() {
    let (stream, bus) = make_bus();
    let mut old = bus.send_order_request(200, vec![]).await.unwrap();
    let shared_phase = bus.shared_channel_senders.write().await;
    let resetting = bus.clone();
    let reset = tokio::spawn(async move { resetting.reset_channels().await });
    expect_reset(&mut old).await;

    let mut replacement = bus.send_order_request(200, vec![]).await.unwrap();
    drop(shared_phase);
    tokio::time::timeout(DEADLINE, reset).await.unwrap().unwrap();
    stream.push_inbound(binary_proto(
        IncomingMessages::OrderStatus as i32,
        &order_status().order_id(200).status(OrderStatusKind::Submitted).to_proto(),
    ));
    bus.read_and_route_message().await.unwrap();
    let response = tokio::time::timeout(DEADLINE, replacement.next())
        .await
        .unwrap()
        .expect("replacement channel was cleared")
        .unwrap();
    assert_eq!(response.order_id(), Some(200));
    assert_eq!(response.message_type(), IncomingMessages::OrderStatus);
    assert!(tokio::time::timeout(DEADLINE, old.next_routed()).await.unwrap().is_none());
}

#[tokio::test]
async fn shared_resubscription_waits_for_reset_and_receives_no_stale_reset() {
    let (stream, bus) = make_bus();
    let mut old = bus.send_shared_request(OutgoingMessages::RequestOpenOrders, vec![]).await.unwrap();
    let senders_gate = bus.shared_channel_senders.write().await;
    let mut reset = Box::pin(bus.reset_channels());
    assert!(futures::poll!(reset.as_mut()).is_pending());

    let mut subscribing = Box::pin(bus.send_shared_request(OutgoingMessages::RequestOpenOrders, vec![]));
    assert!(
        futures::poll!(subscribing.as_mut()).is_pending(),
        "resubscription bypassed the reset barrier"
    );
    drop(senders_gate);
    tokio::time::timeout(DEADLINE, reset).await.unwrap();
    let mut fresh = tokio::time::timeout(DEADLINE, subscribing).await.unwrap().unwrap();
    expect_reset(&mut old).await;
    assert!(old.try_next_routed().is_none(), "one reset, not one per incoming message kind");

    stream.push_inbound(binary_proto(IncomingMessages::OpenOrderEnd as i32, &open_order_end().to_proto()));
    bus.read_and_route_message().await.unwrap();
    let response = tokio::time::timeout(DEADLINE, fresh.next()).await.unwrap().unwrap().unwrap();
    assert_eq!(response.message_type(), IncomingMessages::OpenOrderEnd);
}
