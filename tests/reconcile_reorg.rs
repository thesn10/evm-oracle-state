use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use evm_oracle_state::alloy_network::Ethereum;
use alloy_primitives::{Address, B256, Bytes, I256, Log as PrimitiveLog, U256};
use evm_oracle_state::alloy_provider::{RootProvider, network::AnyNetwork};
use evm_oracle_state::alloy_rpc_client::RpcClient;
use evm_oracle_state::alloy_rpc_types_eth::Log as RpcLog;
use evm_oracle_state::alloy_provider::mock::Asserter;
use evm_fork_cache::cache::EvmCache;
use evm_fork_cache::reactive::{
    BlockRef, ChainStatus, InputSource, ReactiveConfig, ReactiveInput, ReactiveInputBatch,
    ReactiveInputRecord, ReactiveRuntime, ReorgReason,
};
use evm_oracle_state::{
    ANSWER_UPDATED_TOPIC, ChainlinkFeedProvider, FeedConfig, FeedMetadata, OracleError,
    OracleReactiveHandler, OracleRoundStatus, OracleTracker, OracleValueSource, RoundData,
    StalenessPolicy,
};

type ProviderFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, OracleError>> + Send + 'a>>;

#[derive(Clone, Debug)]
struct MutableProvider {
    proxy: Address,
    aggregator: Address,
    metadata: FeedMetadata,
    round: RoundData,
}

impl ChainlinkFeedProvider for MutableProvider {
    fn decimals(&self, proxy: Address) -> ProviderFuture<'_, u8> {
        assert_eq!(proxy, self.proxy);
        Box::pin(async move { Ok(self.metadata.decimals) })
    }

    fn description(&self, proxy: Address) -> ProviderFuture<'_, String> {
        assert_eq!(proxy, self.proxy);
        Box::pin(async move { Ok(self.metadata.description.clone()) })
    }

    fn version(&self, proxy: Address) -> ProviderFuture<'_, U256> {
        assert_eq!(proxy, self.proxy);
        Box::pin(async move { Ok(self.metadata.version) })
    }

    fn latest_round_data(&self, proxy: Address) -> ProviderFuture<'_, RoundData> {
        assert_eq!(proxy, self.proxy);
        Box::pin(async move { Ok(self.round.clone()) })
    }

    fn aggregator(&self, proxy: Address) -> ProviderFuture<'_, Option<Address>> {
        assert_eq!(proxy, self.proxy);
        Box::pin(async move { Ok(Some(self.aggregator)) })
    }
}

fn metadata() -> FeedMetadata {
    FeedMetadata {
        decimals: 8,
        description: "ETH / USD".to_string(),
        version: U256::from(4_u64),
    }
}

fn round(id: u64, answer: i64, updated_at: u64) -> RoundData {
    RoundData {
        round_id: U256::from(id),
        answer: I256::unchecked_from(answer),
        started_at: updated_at.saturating_sub(60),
        updated_at,
        answered_in_round: U256::from(id),
    }
}

fn word(value: U256) -> Vec<u8> {
    value.to_be_bytes::<32>().to_vec()
}

fn uint_topic(value: U256) -> B256 {
    B256::from(value.to_be_bytes::<32>())
}

fn int_topic(value: I256) -> B256 {
    B256::from(value.into_raw().to_be_bytes::<32>())
}

fn block_hash(block_number: u64) -> B256 {
    B256::repeat_byte(block_number as u8)
}

fn block_ref(block_number: u64) -> BlockRef {
    BlockRef {
        number: block_number,
        hash: block_hash(block_number),
        parent_hash: Some(block_hash(block_number.saturating_sub(1))),
        timestamp: Some(1_700_000_000 + block_number),
    }
}

fn context(block_number: u64, log_index: u64) -> evm_fork_cache::reactive::ReactiveContext {
    let block = block_ref(block_number);
    evm_fork_cache::reactive::ReactiveContext {
        chain_id: Some(1),
        source: InputSource::Synthetic,
        chain_status: ChainStatus::Included {
            block,
            confirmations: 0,
        },
        block: Some(block),
        transaction_index: Some(0),
        log_index: Some(log_index),
    }
}

fn reorged_context(block_number: u64, log_index: u64) -> evm_fork_cache::reactive::ReactiveContext {
    let block = block_ref(block_number);
    evm_fork_cache::reactive::ReactiveContext {
        chain_id: Some(1),
        source: InputSource::Synthetic,
        chain_status: ChainStatus::Reorged {
            dropped_from: block,
        },
        block: Some(block),
        transaction_index: Some(0),
        log_index: Some(log_index),
    }
}

fn rpc_log(
    address: Address,
    topics: Vec<B256>,
    data: Vec<u8>,
    block_number: u64,
    log_index: u64,
) -> RpcLog {
    RpcLog {
        inner: PrimitiveLog::new_unchecked(address, topics, Bytes::from(data)),
        block_hash: Some(block_hash(block_number)),
        block_number: Some(block_number),
        block_timestamp: Some(1_700_000_000 + block_number),
        transaction_hash: Some(B256::repeat_byte(0x55)),
        transaction_index: Some(0),
        log_index: Some(log_index),
        removed: false,
    }
}

fn batch(
    input: ReactiveInput<Ethereum>,
    context: evm_fork_cache::reactive::ReactiveContext,
) -> ReactiveInputBatch<Ethereum> {
    ReactiveInputBatch::new(vec![ReactiveInputRecord::new(input, context)])
}

async fn setup_cache() -> EvmCache {
    let asserter = Asserter::new();
    let client = RpcClient::mocked(asserter);
    let provider = RootProvider::<AnyNetwork>::new(client);
    EvmCache::new(Arc::new(provider)).await
}

async fn seeded_tracker(
    proxy: Address,
    aggregator: Address,
    initial_round: RoundData,
) -> OracleTracker {
    let provider = MutableProvider {
        proxy,
        aggregator,
        metadata: metadata(),
        round: initial_round,
    };
    let mut registry = evm_oracle_state::OracleRegistry::new_at_timestamp(1_700_000_500);
    registry
        .register_chainlink_feed(
            &provider,
            FeedConfig {
                proxy,
                label: Some("ETH/USD".to_string()),
                staleness: StalenessPolicy::max_age(60 * 60),
                ..Default::default()
            },
        )
        .await
        .expect("seed registration");
    OracleTracker::new(registry)
}

#[tokio::test]
async fn reconcile_updates_snapshot_and_rebuilds_aggregator_routing() {
    let proxy = Address::repeat_byte(0x10);
    let old_aggregator = Address::repeat_byte(0x20);
    let new_aggregator = Address::repeat_byte(0x21);
    let mut tracker = seeded_tracker(
        proxy,
        old_aggregator,
        round(100, 2_000_000_000, 1_700_000_000),
    )
    .await;
    let provider = MutableProvider {
        proxy,
        aggregator: new_aggregator,
        metadata: metadata(),
        round: round(101, 2_100_000_000, 1_700_000_300),
    };

    let report = tracker
        .reconcile(&provider)
        .await
        .expect("reconcile should read proxy state");

    assert_eq!(report.checked_feeds, 1);
    assert_eq!(report.changed_feeds.len(), 1);
    assert_eq!(report.aggregator_changes.len(), 1);
    assert_eq!(report.aggregator_changes[0].old, Some(old_aggregator));
    assert_eq!(report.aggregator_changes[0].new, Some(new_aggregator));

    let snapshot = tracker.latest(proxy).expect("reconciled snapshot");
    assert_eq!(snapshot.round.round_id, U256::from(101_u64));
    assert_eq!(
        snapshot.round.answer,
        I256::unchecked_from(2_100_000_000_i64)
    );
    assert_eq!(snapshot.aggregator, Some(new_aggregator));
    assert_eq!(snapshot.round_status, OracleRoundStatus::Fresh);
    assert_eq!(snapshot.source, OracleValueSource::Proxy);
    assert!(!snapshot.requires_reconciliation());

    let handler = OracleReactiveHandler::new(tracker.registrations().collect::<Vec<_>>());
    let new_log = rpc_log(
        new_aggregator,
        vec![
            ANSWER_UPDATED_TOPIC,
            int_topic(I256::unchecked_from(1)),
            uint_topic(U256::from(1_u64)),
        ],
        word(U256::from(1_700_000_400_u64)),
        110,
        0,
    );
    let old_log = rpc_log(
        old_aggregator,
        vec![
            ANSWER_UPDATED_TOPIC,
            int_topic(I256::unchecked_from(1)),
            uint_topic(U256::from(1_u64)),
        ],
        word(U256::from(1_700_000_400_u64)),
        110,
        0,
    );

    assert!(handler.interests().iter().any(|interest| match interest {
        evm_fork_cache::reactive::ReactiveInterest::Logs(interest) => interest.matches(&new_log),
        _ => false,
    }));
    assert!(!handler.interests().iter().any(|interest| match interest {
        evm_fork_cache::reactive::ReactiveInterest::Logs(interest) => interest.matches(&old_log),
        _ => false,
    }));
}

#[tokio::test]
async fn removed_answer_updated_does_not_leave_event_price_marked_fresh() {
    let proxy = Address::repeat_byte(0x10);
    let aggregator = Address::repeat_byte(0x20);
    let mut tracker =
        seeded_tracker(proxy, aggregator, round(100, 2_000_000_000, 1_700_000_000)).await;
    let handler = OracleReactiveHandler::new(tracker.registrations().collect::<Vec<_>>());
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime
        .register_handler(Arc::new(handler))
        .expect("register handler");
    let mut cache = setup_cache().await;

    let event_answer = I256::unchecked_from(2_200_000_000_i64);
    let mut log = rpc_log(
        aggregator,
        vec![
            ANSWER_UPDATED_TOPIC,
            int_topic(event_answer),
            uint_topic(U256::from(101_u64)),
        ],
        word(U256::from(1_700_000_400_u64)),
        120,
        0,
    );

    let applied = runtime
        .ingest_batch(
            &mut cache,
            batch(ReactiveInput::Log(log.clone()), context(120, 0)),
        )
        .expect("event applies");
    tracker
        .apply_batch_report(&applied)
        .expect("event updates typed state");
    assert_eq!(tracker.latest(proxy).unwrap().round.answer, event_answer);

    log.removed = true;
    let removed = runtime
        .ingest_batch(
            &mut cache,
            batch(ReactiveInput::Log(log), reorged_context(120, 0)),
        )
        .expect("removed log should trigger reorg report");
    tracker
        .apply_batch_report(&removed)
        .expect("reorg report should update typed state safety");

    assert!(removed.reports.iter().any(|report| match report.as_ref() {
        evm_fork_cache::reactive::ReactiveReport::Reorg(reorg) => {
            reorg.reason == ReorgReason::RemovedLog && reorg.dropped_blocks == vec![block_ref(120)]
        }
        _ => false,
    }));
    let after = tracker.latest(proxy).expect("post-reorg snapshot");
    assert!(
        after.round_status == OracleRoundStatus::Unknown
            || after.round.answer == I256::unchecked_from(2_000_000_000_i64),
        "typed oracle state must either roll back or force reconciliation"
    );
    assert!(
        after.round_status != OracleRoundStatus::Fresh
            || after.source != OracleValueSource::Event
            || after.round.answer != event_answer,
        "reorged event price must not remain a fresh event-derived snapshot"
    );
    assert!(
        after.requires_reconciliation()
            || after.round.answer == I256::unchecked_from(2_000_000_000_i64),
        "reorg handling must either force reconciliation or restore the previous snapshot"
    );
}

#[tokio::test]
async fn stale_pre_reconcile_handler_payload_does_not_overwrite_new_aggregator_state() {
    let proxy = Address::repeat_byte(0x10);
    let old_aggregator = Address::repeat_byte(0x20);
    let new_aggregator = Address::repeat_byte(0x21);
    let mut tracker = seeded_tracker(
        proxy,
        old_aggregator,
        round(100, 2_000_000_000, 1_700_000_000),
    )
    .await;
    let stale_handler = OracleReactiveHandler::new(tracker.registrations().collect::<Vec<_>>());
    let provider = MutableProvider {
        proxy,
        aggregator: new_aggregator,
        metadata: metadata(),
        round: round(101, 2_100_000_000, 1_700_000_300),
    };
    tracker
        .reconcile(&provider)
        .await
        .expect("reconcile should adopt the new aggregator");

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime
        .register_handler(Arc::new(stale_handler))
        .expect("register stale handler");
    let mut cache = setup_cache().await;
    let stale_answer = I256::unchecked_from(1_900_000_000_i64);
    let old_log = rpc_log(
        old_aggregator,
        vec![
            ANSWER_UPDATED_TOPIC,
            int_topic(stale_answer),
            uint_topic(U256::from(102_u64)),
        ],
        word(U256::from(1_700_000_450_u64)),
        130,
        0,
    );

    let stale_report = runtime
        .ingest_batch(
            &mut cache,
            batch(ReactiveInput::Log(old_log), context(130, 0)),
        )
        .expect("stale handler still emits a committed payload");
    assert_eq!(
        stale_report.applied.len(),
        1,
        "the regression setup must exercise report application, not only routing"
    );
    tracker
        .apply_batch_report(&stale_report)
        .expect("stale payload should be ignored, not treated as an error");

    let snapshot = tracker.latest(proxy).expect("snapshot remains present");
    assert_eq!(snapshot.aggregator, Some(new_aggregator));
    assert_eq!(
        snapshot.round.answer,
        I256::unchecked_from(2_100_000_000_i64)
    );
    assert_eq!(snapshot.source, OracleValueSource::Proxy);
    assert!(!snapshot.requires_reconciliation());
}
