//! Ethereum public pending-transaction source using Alchemy's full-body stream.

use std::{fmt, time::Duration};

use crate::alloy_consensus::Transaction as _;
use crate::alloy_eips::eip2718::Encodable2718;
use crate::alloy_network::TransactionResponse;
use alloy_primitives::{Bytes, keccak256};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::sync::watch;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use super::{
    ETHEREUM_MAINNET_CHAIN_ID, PendingOracleCandidateSource, PendingOracleOrderingHandle,
    PendingOracleSource, PendingOracleSourceDescriptor, PendingOracleSourceError,
    PendingOracleSourceFuture, PendingOracleSourceId, PendingOracleSourceSink,
    PendingOracleTransmissionId, PendingTransportCandidate,
};

/// Alchemy WebSocket source for full Ethereum pending transaction bodies.
#[derive(Clone)]
pub struct AlchemyPendingTransactionSource {
    ws_url: String,
    source_id: PendingOracleSourceId,
    chain_id: u64,
}

impl AlchemyPendingTransactionSource {
    /// Construct a source for an Alchemy WebSocket RPC URL.
    pub fn new(ws_url: impl Into<String>) -> Self {
        Self {
            ws_url: ws_url.into(),
            source_id: PendingOracleSourceId::new("alchemy-pending-transactions"),
            chain_id: ETHEREUM_MAINNET_CHAIN_ID,
        }
    }

    /// Override the stable source identifier used in health and provenance.
    pub fn source_id(mut self, source_id: PendingOracleSourceId) -> Self {
        self.source_id = source_id;
        self
    }

    /// Set the declared chain id. It must match the pending runtime configuration.
    pub fn chain_id(mut self, chain_id: u64) -> Self {
        self.chain_id = chain_id;
        self
    }

    async fn run_forever(
        self,
        sink: PendingOracleSourceSink,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), PendingOracleSourceError> {
        let mut retry = Duration::from_secs(1);
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            match self.run_connection(&sink, &mut shutdown).await {
                Ok(()) if *shutdown.borrow() => return Ok(()),
                Ok(()) => sink.coverage_gap("Alchemy pending WebSocket ended"),
                Err(error) => sink.coverage_gap(error.to_string()),
            }
            sink.reconnecting();
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                () = tokio::time::sleep(retry) => {}
            }
            retry = (retry * 2).min(Duration::from_secs(30));
        }
    }

    async fn run_connection(
        &self,
        sink: &PendingOracleSourceSink,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<(), PendingOracleSourceError> {
        let (socket, _) = connect_async(self.ws_url.as_str())
            .await
            .map_err(transport_error)?;
        let (mut writer, mut reader) = socket.split();
        writer
            .send(Message::Text(
                json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "eth_subscribe",
                    "params": ["alchemy_pendingTransactions", {"hashesOnly": false}],
                })
                .to_string()
                .into(),
            ))
            .await
            .map_err(transport_error)?;
        writer
            .send(Message::Text(
                json!({
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "eth_subscribe",
                    "params": ["newHeads"],
                })
                .to_string()
                .into(),
            ))
            .await
            .map_err(transport_error)?;

        let mut pending_subscription = None;
        let mut head_subscription = None;
        let mut current_head = None;
        while pending_subscription.is_none() || head_subscription.is_none() {
            let message = tokio::time::timeout(Duration::from_secs(15), reader.next())
                .await
                .map_err(|_| {
                    PendingOracleSourceError::Transport(
                        "timed out waiting for Alchemy subscription acknowledgements".to_string(),
                    )
                })?
                .ok_or_else(|| {
                    PendingOracleSourceError::Transport(
                        "Alchemy WebSocket closed during subscription".to_string(),
                    )
                })?
                .map_err(transport_error)?;
            let Some(value) = message_json(message)? else {
                continue;
            };
            if let Some(error) = value.get("error") {
                return Err(PendingOracleSourceError::Transport(format!(
                    "Alchemy subscription failed: {error}"
                )));
            }
            match value.get("id").and_then(Value::as_u64) {
                Some(1) => pending_subscription = value_string(&value, "result"),
                Some(2) => head_subscription = value_string(&value, "result"),
                _ => {}
            }
        }
        sink.ready();

        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        let _ = writer.close().await;
                        return Ok(());
                    }
                }
                message = reader.next() => {
                    let Some(message) = message else {
                        return Err(PendingOracleSourceError::Transport(
                            "Alchemy pending WebSocket closed".to_string(),
                        ));
                    };
                    let message = message.map_err(transport_error)?;
                    let Some(value) = message_json(message)? else {
                        continue;
                    };
                    sink.transport_message();
                    let Some(params) = value.get("params") else {
                        continue;
                    };
                    let subscription = params.get("subscription").and_then(Value::as_str);
                    let result = params.get("result");
                    if subscription == head_subscription.as_deref() {
                        current_head = result
                            .and_then(|header| header.get("number"))
                            .and_then(Value::as_str)
                            .and_then(parse_quantity);
                    } else if subscription == pending_subscription.as_deref() {
                        let Some(result) = result else {
                            continue;
                        };
                        let Some(candidate) = self.transaction_candidate(result.clone(), current_head)
                        else {
                            continue;
                        };
                        if sink
                            .runtime()
                            .interests()
                            .iter()
                            .any(|interest| interest.matches(&candidate))
                        {
                            sink.candidate();
                            match sink.runtime().observe_candidate(candidate) {
                                Ok(report) => {
                                    for failure in report.failures {
                                        tracing::debug!(
                                            adapter_id = %failure.adapter_id,
                                            error = %failure.error,
                                            "pending oracle adapter rejected Alchemy candidate"
                                        );
                                    }
                                }
                                Err(error) => {
                                    tracing::debug!(%error, "pending oracle runtime rejected Alchemy candidate");
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    fn transaction_candidate(
        &self,
        value: Value,
        observed_at_head: Option<u64>,
    ) -> Option<PendingTransportCandidate> {
        let transaction: crate::alloy_rpc_types_eth::Transaction = serde_json::from_value(value).ok()?;
        let to = transaction.to()?;
        let calldata = transaction.input().clone();
        if calldata.len() < 4 {
            return None;
        }
        let tx_hash = transaction.tx_hash();
        let signed_envelope = Bytes::from(transaction.inner.inner().encoded_2718());
        let ordering = if keccak256(&signed_envelope) == tx_hash {
            PendingOracleOrderingHandle::RawEthereumTransaction {
                tx_hash,
                signed_envelope,
            }
        } else {
            PendingOracleOrderingHandle::TransactionHashOnly { tx_hash }
        };
        Some(PendingTransportCandidate::new(
            self.chain_id,
            PendingOracleTransmissionId::from_hash(tx_hash),
            PendingOracleSource::PublicMempool,
            self.source_id.clone(),
            to,
            calldata,
            ordering,
            observed_at_head,
        ))
    }
}

impl fmt::Debug for AlchemyPendingTransactionSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlchemyPendingTransactionSource")
            .field("ws_url", &"<redacted>")
            .field("source_id", &self.source_id)
            .field("chain_id", &self.chain_id)
            .finish()
    }
}

impl PendingOracleCandidateSource for AlchemyPendingTransactionSource {
    fn descriptor(&self) -> PendingOracleSourceDescriptor {
        PendingOracleSourceDescriptor::new(
            PendingOracleSource::PublicMempool,
            self.source_id.clone(),
        )
    }

    fn run(
        self: Box<Self>,
        sink: PendingOracleSourceSink,
        shutdown: watch::Receiver<bool>,
    ) -> PendingOracleSourceFuture {
        Box::pin(async move { self.run_forever(sink, shutdown).await })
    }
}

fn value_string(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(Value::as_str).map(str::to_owned)
}

fn parse_quantity(value: &str) -> Option<u64> {
    u64::from_str_radix(value.strip_prefix("0x")?, 16).ok()
}

fn message_json(message: Message) -> Result<Option<Value>, PendingOracleSourceError> {
    match message {
        Message::Text(text) => serde_json::from_str(text.as_str())
            .map(Some)
            .map_err(transport_error),
        Message::Binary(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(transport_error),
        Message::Close(_) => Err(PendingOracleSourceError::Transport(
            "pending WebSocket closed".to_string(),
        )),
        Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => Ok(None),
    }
}

fn transport_error(error: impl fmt::Display) -> PendingOracleSourceError {
    PendingOracleSourceError::Transport(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use alloy_primitives::B256;

    use super::*;

    #[test]
    fn rpc_transaction_reencodes_to_its_declared_hash() {
        let value = serde_json::from_str(
            r#"{
                "type":"0x2","chainId":"0x1","nonce":"0x84974","gas":"0x30d40",
                "maxFeePerGas":"0x7a4e39a","maxPriorityFeePerGas":"0x1ef37be",
                "to":"0xe73d53e3a982ab2750a0b76f9012e18b256cc243","value":"0x0",
                "accessList":[],"input":"0x1249c58b",
                "r":"0x7432676eb4f3b0f8e2c44044e6cf25e5421ac2d1d93412ccbec481e88b8102c1",
                "s":"0x1800a3996abb76a262638c58c51693d410ca6b24b6a196851e36b1b4513953ab",
                "yParity":"0x0","v":"0x0",
                "hash":"0x483b92a2d30693c28bc812a1e6747ce2af8b04694ccf08c1c85ec67bb04962ca",
                "blockHash":null,"blockNumber":null,"transactionIndex":null,
                "from":"0x82a53178e7a7e454ab31eea6063fdca338418f74","gasPrice":"0x7a4e39a"
            }"#,
        )
        .expect("transaction fixture JSON");
        let candidate = AlchemyPendingTransactionSource::new("wss://redacted")
            .transaction_candidate(value, Some(100))
            .expect("full transaction candidate");
        let expected =
            B256::from_str("0x483b92a2d30693c28bc812a1e6747ce2af8b04694ccf08c1c85ec67bb04962ca")
                .unwrap();
        let PendingOracleOrderingHandle::RawEthereumTransaction {
            tx_hash,
            signed_envelope,
        } = candidate.ordering
        else {
            panic!("expected exact signed transaction");
        };
        assert_eq!(tx_hash, expected);
        assert_eq!(keccak256(signed_envelope), expected);
    }
}
