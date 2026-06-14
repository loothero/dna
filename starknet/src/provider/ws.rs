use std::pin::Pin;
use std::task::{Context, Poll};

use apibara_dna_common::{Cursor, Hash};
use error_stack::{Result, ResultExt};
use futures::stream::SplitStream;
use futures::{SinkExt, Stream, StreamExt};
use starknet_rust::core::types::requests::{
    SubscribeEventsRequest as StarknetSubscribeEventsRequest,
    SubscribeNewTransactionReceiptsRequest as StarknetSubscribeNewTransactionReceiptsRequest,
    SubscribeNewTransactionsRequest as StarknetSubscribeNewTransactionsRequest,
    SubscriptionEventsRequest, SubscriptionNewTransactionReceiptsRequest,
    SubscriptionNewTransactionRequest,
};
use starknet_rust::core::types::{
    AddressFilter, ConfirmedBlockId, EmittedEventWithFinality, Felt, L2TransactionFinalityStatus,
    L2TransactionStatus, TransactionReceiptWithBlockInfo, TransactionWithL2Status,
};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use super::http::StarknetProviderError;

/// A new head message received from the Starknet websocket subscription.
#[derive(Debug)]
pub struct NewHeadMessage {
    block_number: u64,
    block_hash: Hash,
}

/// A stream of new heads from a Starknet websocket subscription.
pub struct NewHeadsStream {
    inner: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
}

/// A new transaction receipt received from a Starknet websocket subscription.
#[derive(Debug, Clone)]
pub struct NewTransactionReceiptMessage {
    pub receipt: TransactionReceiptWithBlockInfo,
    pub transaction_index: Option<u64>,
}

/// A new transaction received from a Starknet websocket subscription.
#[derive(Debug, Clone)]
pub struct NewTransactionMessage {
    pub transaction: TransactionWithL2Status,
    pub block_number: Option<u64>,
    pub transaction_index: Option<u64>,
}

/// A new event received from a Starknet websocket subscription.
#[derive(Debug, Clone)]
pub struct NewEventMessage {
    pub event: EmittedEventWithFinality,
}

/// A live Starknet transaction or receipt websocket notification.
#[derive(Debug, Clone)]
pub enum StarknetLiveMessage {
    Transaction(NewTransactionMessage),
    Receipt(NewTransactionReceiptMessage),
    Event(NewEventMessage),
}

/// A stream subscribed to Starknet pre-confirmed transaction bodies and receipts.
pub struct StarknetLiveTransactionsStream {
    inner: BoxedStarknetLiveMessageStream,
}

type BoxedStarknetLiveMessageStream =
    Pin<Box<dyn Stream<Item = Result<StarknetLiveMessage, StarknetProviderError>> + Send>>;

#[derive(Debug, Clone, Default)]
pub struct StarknetLiveEventFilter {
    pub from_address: Option<Felt>,
    pub keys: Option<Vec<Vec<Felt>>>,
}

#[derive(Debug)]
struct SubscribeRequest {
    block_id: ConfirmedBlockId,
}

#[derive(Debug)]
enum LiveSubscribeRequest {
    Events(StarknetSubscribeEventsRequest),
    NewTransactionReceipts(StarknetSubscribeNewTransactionReceiptsRequest),
    NewTransactions(StarknetSubscribeNewTransactionsRequest),
}

impl NewHeadsStream {
    /// Creates a new [`NewHeadsStream`] from a websocket URL.
    pub async fn connect(url: &str) -> Result<Self, StarknetProviderError> {
        let (ws_stream, _) = tokio_tungstenite::connect_async(url)
            .await
            .change_context(StarknetProviderError::Request)
            .attach_printable("failed to connect to ws stream")?;

        let (mut write, read) = ws_stream.split();

        // Since we are only subscribing to new heads, we don't need to keep the
        // tx around to then send messages.
        // Just subscribe and then give up the read half.
        write
            .send(
                SubscribeRequest {
                    block_id: ConfirmedBlockId::Latest,
                }
                .into(),
            )
            .await
            .change_context(StarknetProviderError::Request)
            .attach_printable("failed to send subscribe request")?;

        Ok(Self { inner: read })
    }
}

impl StarknetLiveTransactionsStream {
    /// Creates a new [`StarknetLiveTransactionsStream`] from a websocket URL.
    pub async fn connect(
        url: &str,
        event_filter: Option<StarknetLiveEventFilter>,
    ) -> Result<Self, StarknetProviderError> {
        let mut groups = live_subscribe_request_groups(event_filter);
        let first_group = groups.remove(0);
        let mut stream = connect_live_message_stream(url, first_group).await?;

        for group in groups {
            let next = connect_live_message_stream(url, group).await?;
            stream = futures::stream::select(stream, next).boxed();
        }

        Ok(Self { inner: stream })
    }
}

impl NewHeadMessage {
    pub fn cursor(&self) -> Cursor {
        Cursor::new(self.block_number, self.block_hash.clone())
    }

    pub fn try_from_message(msg: Message) -> Result<Option<Self>, StarknetProviderError> {
        #[derive(Debug, serde::Deserialize)]
        struct WsMessage {
            params: Option<WsParams>,
        }

        #[derive(Debug, serde::Deserialize)]
        struct WsParams {
            result: WsBlockResult,
        }

        #[derive(Debug, serde::Deserialize)]
        struct WsBlockResult {
            block_hash: String,
            block_number: u64,
        }

        let Message::Text(text) = msg else {
            return Ok(None);
        };

        let msg: WsMessage = serde_json::from_str(&text)
            .change_context(StarknetProviderError::Request)
            .attach_printable("failed to parse websocket message as json")?;

        let Some(params) = msg.params else {
            return Ok(None);
        };

        let block_number = params.result.block_number;
        let block_hash_hex = params.result.block_hash;

        let block_hash = decode_hex_hash(&block_hash_hex)
            .change_context(StarknetProviderError::Request)
            .attach_printable_lazy(|| format!("failed to decode block_hash: {}", block_hash_hex))?;

        Ok(Some(NewHeadMessage {
            block_number,
            block_hash,
        }))
    }
}

impl StarknetLiveMessage {
    pub fn try_from_message(msg: Message) -> Result<Option<Self>, StarknetProviderError> {
        #[derive(Debug, serde::Deserialize)]
        struct WsMessage {
            method: Option<String>,
            params: Option<serde_json::Value>,
            error: Option<serde_json::Value>,
        }

        let Message::Text(text) = msg else {
            return Ok(None);
        };

        let msg: WsMessage = serde_json::from_str(&text)
            .change_context(StarknetProviderError::Request)
            .attach_printable("failed to parse websocket message as json")?;

        if let Some(error) = msg.error {
            return Err(StarknetProviderError::Request)
                .attach_printable_lazy(|| format!("websocket json-rpc error: {error}"));
        }

        let Some(method) = msg.method.as_deref() else {
            return Ok(None);
        };

        let Some(params) = msg.params else {
            return Ok(None);
        };

        match method {
            "starknet_subscriptionEvents" => {
                let update: SubscriptionEventsRequest = serde_json::from_value(params)
                    .change_context(StarknetProviderError::Request)
                    .attach_printable("failed to parse event notification")?;

                Ok(Some(Self::Event(NewEventMessage {
                    event: update.result,
                })))
            }
            "starknet_subscriptionNewTransactionReceipts" => {
                let transaction_index = extract_result_u64(&params, "transaction_index");
                let update: SubscriptionNewTransactionReceiptsRequest =
                    serde_json::from_value(params)
                        .change_context(StarknetProviderError::Request)
                        .attach_printable("failed to parse transaction receipt notification")?;

                Ok(Some(Self::Receipt(NewTransactionReceiptMessage {
                    receipt: update.result,
                    transaction_index,
                })))
            }
            "starknet_subscriptionNewTransaction" => {
                let block_number = extract_result_u64(&params, "block_number");
                let transaction_index = extract_result_u64(&params, "transaction_index");
                let update: SubscriptionNewTransactionRequest = serde_json::from_value(params)
                    .change_context(StarknetProviderError::Request)
                    .attach_printable("failed to parse transaction notification")?;

                Ok(Some(Self::Transaction(NewTransactionMessage {
                    transaction: update.result,
                    block_number,
                    transaction_index,
                })))
            }
            _ => Ok(None),
        }
    }
}

fn decode_hex_felt(hex: &str) -> std::result::Result<Vec<u8>, hex::FromHexError> {
    let hex = hex.trim_start_matches("0x");
    let hex = if hex.len() % 2 == 1 {
        format!("0{}", hex)
    } else {
        hex.to_string()
    };
    hex::decode(&hex)
}

fn decode_hex_hash(hex: &str) -> std::result::Result<Hash, hex::FromHexError> {
    let bytes = decode_hex_felt(hex)?;
    let mut out = vec![0; 32];
    let len = bytes.len().min(32);
    out[32 - len..].copy_from_slice(&bytes[bytes.len() - len..]);
    Ok(Hash(out))
}

fn extract_result_u64(params: &serde_json::Value, field: &str) -> Option<u64> {
    let result = match params {
        serde_json::Value::Object(object) => object.get("result"),
        serde_json::Value::Array(elements) => elements.get(1),
        _ => None,
    }?;

    json_value_as_u64(result.get(field)?)
}

fn json_value_as_u64(value: &serde_json::Value) -> Option<u64> {
    if let Some(value) = value.as_u64() {
        return Some(value);
    }

    let value = value.as_str()?;
    if let Some(value) = value.strip_prefix("0x") {
        u64::from_str_radix(value, 16).ok()
    } else {
        value.parse().ok()
    }
}

fn live_subscribe_request_groups(
    event_filter: Option<StarknetLiveEventFilter>,
) -> Vec<Vec<LiveSubscribeRequest>> {
    let mut groups = Vec::new();

    if let Some(event_filter) = event_filter {
        groups.push(vec![LiveSubscribeRequest::Events(
            StarknetSubscribeEventsRequest {
                from_address: event_filter.from_address.map(AddressFilter::Single),
                keys: event_filter.keys,
                block_id: None,
                finality_status: Some(L2TransactionFinalityStatus::PreConfirmed),
            },
        )]);
    }

    groups.push(vec![
        LiveSubscribeRequest::NewTransactionReceipts(
            StarknetSubscribeNewTransactionReceiptsRequest {
                finality_status: Some(vec![L2TransactionFinalityStatus::PreConfirmed]),
                sender_address: None,
            },
        ),
        LiveSubscribeRequest::NewTransactions(StarknetSubscribeNewTransactionsRequest {
            finality_status: Some(vec![L2TransactionStatus::PreConfirmed]),
            sender_address: None,
            tags: None,
        }),
    ]);

    groups
}

async fn connect_live_message_stream(
    url: &str,
    requests: Vec<LiveSubscribeRequest>,
) -> Result<BoxedStarknetLiveMessageStream, StarknetProviderError> {
    let (ws_stream, _) = tokio_tungstenite::connect_async(url)
        .await
        .change_context(StarknetProviderError::Request)
        .attach_printable("failed to connect to ws stream")?;

    let (mut write, read) = ws_stream.split();

    for request in requests {
        write
            .send(request.into())
            .await
            .change_context(StarknetProviderError::Request)
            .attach_printable("failed to send live subscribe request")?;
    }

    Ok(decode_live_message_stream(read))
}

fn decode_live_message_stream(
    read: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
) -> BoxedStarknetLiveMessageStream {
    read.filter_map(|message| async move {
        match message {
            Ok(message) => match StarknetLiveMessage::try_from_message(message) {
                Ok(Some(message)) => Some(Ok(message)),
                Ok(None) => None,
                Err(error) => Some(Err(error)),
            },
            Err(error) => {
                let error = Err::<(), _>(error)
                    .change_context(StarknetProviderError::Request)
                    .unwrap_err();
                Some(Err(error))
            }
        }
    })
    .boxed()
}

impl Stream for NewHeadsStream {
    type Item = Result<NewHeadMessage, StarknetProviderError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.poll_next_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Ok(msg))) => match NewHeadMessage::try_from_message(msg) {
                Ok(None) => Poll::Pending,
                Ok(Some(msg)) => Poll::Ready(Some(Ok(msg))),
                Err(e) => Poll::Ready(Some(Err(e))),
            },
            Poll::Ready(Some(Err(e))) => {
                Poll::Ready(Some(Err(e).change_context(StarknetProviderError::Request)))
            }
        }
    }
}

impl Stream for StarknetLiveTransactionsStream {
    type Item = Result<StarknetLiveMessage, StarknetProviderError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl SubscribeRequest {
    pub fn into_string(self) -> String {
        use serde_json::json;
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 0,
            "method": "starknet_subscribeNewHeads",
            "params": [self.block_id]
        });
        serde_json::to_string(&payload).expect("serialization")
    }
}

impl From<SubscribeRequest> for Message {
    fn from(r: SubscribeRequest) -> Self {
        Message::Text(r.into_string().into())
    }
}

impl LiveSubscribeRequest {
    pub fn into_string(self) -> String {
        use serde_json::json;

        let (id, method, params) = match self {
            Self::Events(request) => (
                1,
                "starknet_subscribeEvents",
                serde_json::to_value(request).expect("serialization"),
            ),
            Self::NewTransactionReceipts(request) => (
                2,
                "starknet_subscribeNewTransactionReceipts",
                serde_json::to_value(request).expect("serialization"),
            ),
            Self::NewTransactions(request) => (
                3,
                "starknet_subscribeNewTransactions",
                serde_json::to_value(request).expect("serialization"),
            ),
        };

        let payload = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        serde_json::to_string(&payload).expect("serialization")
    }
}

impl From<LiveSubscribeRequest> for Message {
    fn from(r: LiveSubscribeRequest) -> Self {
        Message::Text(r.into_string().into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use starknet_rust::core::types::{
        ExecutionResult, FeePayment, PriceUnit, TransactionFinalityStatus,
    };

    fn receipt_result(hash: &str, block_number: u64) -> serde_json::Value {
        json!({
            "type": "INVOKE",
            "transaction_hash": hash,
            "actual_fee": { "amount": "0x0", "unit": "FRI" },
            "finality_status": "PRE_CONFIRMED",
            "messages_sent": [],
            "events": [],
            "execution_resources": {
                "l1_gas": 0,
                "l1_data_gas": 0,
                "l2_gas": 0
            },
            "execution_status": "SUCCEEDED",
            "block_number": block_number,
            "transaction_index": 7
        })
    }

    fn transaction_result(hash: &str) -> serde_json::Value {
        json!({
            "type": "INVOKE",
            "version": "0x1",
            "transaction_hash": hash,
            "sender_address": "0x1",
            "calldata": [],
            "max_fee": "0x0",
            "signature": [],
            "nonce": "0x0",
            "finality_status": "PRE_CONFIRMED",
            "block_number": 10,
            "transaction_index": 3
        })
    }

    fn event_result(hash: &str) -> serde_json::Value {
        json!({
            "from_address": "0x1",
            "keys": ["0x2"],
            "data": ["0x3"],
            "block_number": 10,
            "transaction_hash": hash,
            "transaction_index": 4,
            "event_index": 5,
            "finality_status": "PRE_CONFIRMED"
        })
    }

    #[test]
    fn live_subscribe_requests_use_pre_confirmed_filters() {
        let event_request = LiveSubscribeRequest::Events(StarknetSubscribeEventsRequest {
            from_address: None,
            keys: None,
            block_id: None,
            finality_status: Some(L2TransactionFinalityStatus::PreConfirmed),
        })
        .into_string();
        let event_request: serde_json::Value = serde_json::from_str(&event_request).unwrap();
        assert_eq!(event_request["method"], "starknet_subscribeEvents");
        assert_eq!(
            event_request["params"]["finality_status"],
            json!("PRE_CONFIRMED")
        );

        let receipt_request = LiveSubscribeRequest::NewTransactionReceipts(
            StarknetSubscribeNewTransactionReceiptsRequest {
                finality_status: Some(vec![L2TransactionFinalityStatus::PreConfirmed]),
                sender_address: None,
            },
        )
        .into_string();
        let receipt_request: serde_json::Value = serde_json::from_str(&receipt_request).unwrap();
        assert_eq!(
            receipt_request["method"],
            "starknet_subscribeNewTransactionReceipts"
        );
        assert_eq!(
            receipt_request["params"]["finality_status"],
            json!(["PRE_CONFIRMED"])
        );

        let transaction_request =
            LiveSubscribeRequest::NewTransactions(StarknetSubscribeNewTransactionsRequest {
                finality_status: Some(vec![L2TransactionStatus::PreConfirmed]),
                sender_address: None,
                tags: None,
            })
            .into_string();
        let transaction_request: serde_json::Value =
            serde_json::from_str(&transaction_request).unwrap();
        assert_eq!(
            transaction_request["method"],
            "starknet_subscribeNewTransactions"
        );
        assert_eq!(
            transaction_request["params"]["finality_status"],
            json!(["PRE_CONFIRMED"])
        );
    }

    #[test]
    fn live_event_subscribe_request_serializes_address_and_key_filter() {
        let event_request = LiveSubscribeRequest::Events(StarknetSubscribeEventsRequest {
            from_address: Some(AddressFilter::Single(Felt::from_hex("0x1").unwrap())),
            keys: Some(vec![vec![Felt::from_hex("0x2").unwrap()]]),
            block_id: None,
            finality_status: Some(L2TransactionFinalityStatus::PreConfirmed),
        })
        .into_string();

        let event_request: serde_json::Value = serde_json::from_str(&event_request).unwrap();
        assert_eq!(event_request["method"], "starknet_subscribeEvents");
        assert_eq!(event_request["params"]["from_address"], json!("0x1"));
        assert_eq!(event_request["params"]["keys"], json!([["0x2"]]));
        assert_eq!(
            event_request["params"]["finality_status"],
            json!("PRE_CONFIRMED")
        );
    }

    #[test]
    fn live_subscribe_request_groups_use_dedicated_event_connection() {
        let mut groups = live_subscribe_request_groups(Some(StarknetLiveEventFilter {
            from_address: Some(Felt::from_hex("0x1").unwrap()),
            keys: Some(vec![vec![Felt::from_hex("0x2").unwrap()]]),
        }));

        assert_eq!(groups.len(), 2);

        let event_group = groups.remove(0);
        assert_eq!(event_group.len(), 1);
        let event_request: serde_json::Value =
            serde_json::from_str(&event_group.into_iter().next().unwrap().into_string()).unwrap();
        assert_eq!(event_request["method"], "starknet_subscribeEvents");
        assert_eq!(event_request["params"]["from_address"], json!("0x1"));
        assert_eq!(event_request["params"]["keys"], json!([["0x2"]]));

        let transaction_group = groups.remove(0);
        assert_eq!(transaction_group.len(), 2);
        let methods = transaction_group
            .into_iter()
            .map(|request| {
                let request: serde_json::Value =
                    serde_json::from_str(&request.into_string()).unwrap();
                request["method"].as_str().unwrap().to_string()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            methods,
            vec![
                "starknet_subscribeNewTransactionReceipts",
                "starknet_subscribeNewTransactions"
            ]
        );
    }

    #[test]
    fn parses_transaction_receipt_notification() {
        let message = json!({
            "jsonrpc": "2.0",
            "method": "starknet_subscriptionNewTransactionReceipts",
            "params": {
                "subscription_id": "0x1",
                "result": receipt_result("0x123", 10)
            }
        });

        let parsed =
            StarknetLiveMessage::try_from_message(Message::Text(message.to_string().into()))
                .unwrap();

        let Some(StarknetLiveMessage::Receipt(parsed)) = parsed else {
            panic!("expected receipt notification");
        };
        assert_eq!(parsed.transaction_index, Some(7));
        assert_eq!(parsed.receipt.block.block_number(), 10);
        assert_eq!(
            parsed.receipt.receipt.execution_result(),
            &ExecutionResult::Succeeded
        );
        assert_eq!(
            parsed.receipt.receipt.finality_status(),
            &TransactionFinalityStatus::PreConfirmed
        );
        let actual_fee = match parsed.receipt.receipt {
            starknet_rust::core::types::TransactionReceipt::Invoke(receipt) => receipt.actual_fee,
            _ => panic!("expected invoke receipt"),
        };
        assert_eq!(
            actual_fee,
            FeePayment {
                amount: starknet_rust::core::types::Felt::from_hex("0x0").unwrap(),
                unit: PriceUnit::Fri
            }
        );
    }

    #[test]
    fn parses_new_head_hash_as_fixed_width_cursor_hash() {
        let message = json!({
            "jsonrpc": "2.0",
            "method": "starknet_subscriptionNewHeads",
            "params": {
                "subscription_id": "0x4",
                "result": {
                    "block_number": 10,
                    "block_hash": "0x123"
                }
            }
        });

        let parsed = NewHeadMessage::try_from_message(Message::Text(message.to_string().into()))
            .unwrap()
            .expect("head notification");

        let mut expected = vec![0; 32];
        expected[30] = 0x01;
        expected[31] = 0x23;

        assert_eq!(parsed.block_number, 10);
        assert_eq!(parsed.block_hash, Hash(expected));
    }

    #[test]
    fn parses_transaction_notification_and_ignores_subscribe_ack() {
        let ack = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": "0xabc"
        });
        assert!(
            StarknetLiveMessage::try_from_message(Message::Text(ack.to_string().into()))
                .unwrap()
                .is_none()
        );

        let message = json!({
            "jsonrpc": "2.0",
            "method": "starknet_subscriptionNewTransaction",
            "params": {
                "subscription_id": "0x2",
                "result": transaction_result("0x123")
            }
        });

        let parsed =
            StarknetLiveMessage::try_from_message(Message::Text(message.to_string().into()))
                .unwrap();

        let Some(StarknetLiveMessage::Transaction(parsed)) = parsed else {
            panic!("expected transaction notification");
        };
        assert_eq!(parsed.block_number, Some(10));
        assert_eq!(parsed.transaction_index, Some(3));
        assert_eq!(
            parsed.transaction.finality_status,
            L2TransactionStatus::PreConfirmed
        );
    }

    #[test]
    fn parses_event_notification() {
        let message = json!({
            "jsonrpc": "2.0",
            "method": "starknet_subscriptionEvents",
            "params": {
                "subscription_id": "0x3",
                "result": event_result("0x123")
            }
        });

        let parsed =
            StarknetLiveMessage::try_from_message(Message::Text(message.to_string().into()))
                .unwrap();

        let Some(StarknetLiveMessage::Event(parsed)) = parsed else {
            panic!("expected event notification");
        };
        assert_eq!(parsed.event.emitted_event.block_number, Some(10));
        assert_eq!(parsed.event.emitted_event.transaction_index, 4);
        assert_eq!(parsed.event.emitted_event.event_index, 5);
        assert_eq!(
            parsed.event.finality_status,
            TransactionFinalityStatus::PreConfirmed
        );
    }
}
