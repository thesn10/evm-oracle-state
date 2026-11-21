#![cfg(feature = "redstone")]

use std::sync::Arc;

use evm_oracle_state::alloy_network::Ethereum;
use alloy_primitives::{Address, B256, Bytes, I256, Log as PrimitiveLog, U256};
use evm_oracle_state::alloy_provider::{RootProvider, network::AnyNetwork};
use evm_oracle_state::alloy_rpc_client::RpcClient;
use evm_oracle_state::alloy_rpc_types_eth::Log as RpcLog;
use alloy_sol_types::{SolCall, sol};
use evm_oracle_state::alloy_provider::mock::Asserter;
use evm_fork_cache::{
    cache::EvmCache,
    reactive::{
        BlockRef, ChainStatus, InputSource, ReactiveConfig, ReactiveInput, ReactiveInputBatch,
        ReactiveInputRecord, ReactiveInterest, ReactiveRuntime,
    },
    state_update::PurgeScope,
};
use evm_oracle_state::{
    ANSWER_UPDATED_TOPIC, FeedId, FeedMetadata, FeedRegistration, FeedSource,
    ORACLE_SIGNAL_NAMESPACE, OracleFeedStatus, OracleReadOverlay, OracleSignalKind, OracleTracker,
    OracleValueSource, REDSTONE_VALUE_UPDATE_TOPIC, RedstoneReactiveHandler, RoundData,
    StalenessPolicy, decode_redstone_value_update,
};

sol! {
    interface AggregatorV3Interface {
        function latestRoundData() external view returns (
            uint80 roundId,
            int256 answer,
            uint256 startedAt,
            uint256 updatedAt,
            uint80 answeredInRound
        );
    }
}

use AggregatorV3Interface::latestRoundDataCall;

#[test]
fn redstone_value_update_decodes_data_feed_id_value_and_timestamp() {
    let adapter = Address::repeat_byte(0x44);
    let data_feed_id = data_feed_id("ETH");
    let log = rpc_log(
        adapter,
        vec![REDSTONE_VALUE_UPDATE_TOPIC],
        encode_words(&[
            U256::from(2_250_000_000_u64),
            U256::from_be_slice(data_feed_id.as_slice()),
            U256::from(1_700_000_120_u64),
        ]),
        110,
        3,
    );

    let event = decode_redstone_value_update(&log).expect("ValueUpdate should decode");
    assert_eq!(event.adapter, adapter);
    assert_eq!(event.data_feed_id, data_feed_id);
    assert_eq!(event.value, U256::from(2_250_000_000_u64));
    assert_eq!(event.updated_at, 1_700_000_120);
    assert_eq!(event.block_number, Some(110));
    assert_eq!(event.log_index, Some(3));
    assert!(!event.removed);
}

#[tokio::test]
async fn redstone_multi_feed_value_update_updates_tracker_overlay_and_invalidates_cache() {
    let price_feed = Address::repeat_byte(0x10);
    let adapter = Address::repeat_byte(0x20);
    let data_feed_id = data_feed_id("ETH");
    let registration = redstone_registration(price_feed, adapter, data_feed_id);
    let mut tracker = OracleTracker::from_registrations_at_timestamp(
        vec![(
            registration.clone(),
            round(1, I256::unchecked_from(2_000_000_000_i64), 1_700_000_000),
        )],
        1_700_000_010,
    )
    .expect("seed RedStone tracker");
    let handler = RedstoneReactiveHandler::new(vec![registration]);
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime
        .register_handler(Arc::new(handler))
        .expect("register RedStone handler");
    let mut cache = setup_cache().await;

    let log = rpc_log(
        adapter,
        vec![REDSTONE_VALUE_UPDATE_TOPIC],
        encode_words(&[
            U256::from(2_250_000_000_u64),
            U256::from_be_slice(data_feed_id.as_slice()),
            U256::from(1_700_000_120_u64),
        ]),
        111,
        4,
    );
    let report = runtime
        .ingest_batch(&mut cache, batch(ReactiveInput::Log(log), context(111, 4)))
        .expect("RedStone event should commit");

    assert_eq!(report.applied.len(), 1);
    let applied = &report.applied[0];
    assert!(applied.invalidations.iter().any(|request| {
        request.address == price_feed && matches!(request.scope, PurgeScope::AllStorage)
    }));
    assert!(applied.invalidations.iter().any(|request| {
        request.address == adapter && matches!(request.scope, PurgeScope::AllStorage)
    }));
    assert!(applied.hook_signals.iter().any(|signal| {
        signal.namespace.as_ref() == ORACLE_SIGNAL_NAMESPACE
            && signal.kind.as_ref() == OracleSignalKind::PriceUpdate.as_str()
            && signal.payload.is_some()
    }));

    tracker
        .apply_batch_report(&report)
        .expect("RedStone hook should update typed state");
    let price = tracker.price("redstone-eth-usd").expect("updated price");
    assert_eq!(price.raw_answer, I256::unchecked_from(2_250_000_000_i64));
    assert_eq!(price.updated_at, 1_700_000_120);
    assert_eq!(price.source, OracleValueSource::Event);
    assert!(price.is_event_pending());
    assert_eq!(tracker.pending_reconciliations().len(), 1);

    let overlay = OracleReadOverlay::new(&tracker);
    let latest = overlay
        .try_call(price_feed, &latestRoundDataCall {}.abi_encode())
        .expect("overlay should encode latestRoundData")
        .expect("RedStone price feed wrapper should be served from typed state");
    let decoded = latestRoundDataCall::abi_decode_returns(&latest).expect("latestRoundData ABI");
    assert_eq!(decoded.answer, I256::unchecked_from(2_250_000_000_i64));
    assert_eq!(decoded.updatedAt, U256::from(1_700_000_120_u64));
}

#[tokio::test]
async fn redstone_merged_answer_updated_event_updates_from_price_feed_address() {
    let price_feed = Address::repeat_byte(0x30);
    let data_feed_id = data_feed_id("BTC");
    let registration = redstone_registration(price_feed, price_feed, data_feed_id);
    let mut tracker = OracleTracker::from_registrations_at_timestamp(
        vec![(
            registration.clone(),
            round(7, I256::unchecked_from(60_000_000_000_i64), 1_700_000_000),
        )],
        1_700_000_010,
    )
    .expect("seed RedStone tracker");
    let handler = RedstoneReactiveHandler::new(vec![registration]);
    assert!(
        handler.interests().iter().any(|interest| match interest {
            ReactiveInterest::Logs(interest) => interest.matches(&rpc_log(
                price_feed,
                vec![
                    ANSWER_UPDATED_TOPIC,
                    int_topic(I256::unchecked_from(61_000_000_000_i64)),
                    uint_topic(U256::from(8_u64)),
                ],
                word(U256::from(1_700_000_180_u64)),
                112,
                1,
            )),
            _ => false,
        }),
        "RedStone merged price feeds should subscribe to Chainlink-shaped AnswerUpdated logs"
    );
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime
        .register_handler(Arc::new(handler))
        .expect("register RedStone handler");
    let mut cache = setup_cache().await;

    let log = rpc_log(
        price_feed,
        vec![
            ANSWER_UPDATED_TOPIC,
            int_topic(I256::unchecked_from(61_000_000_000_i64)),
            uint_topic(U256::from(8_u64)),
        ],
        word(U256::from(1_700_000_180_u64)),
        112,
        1,
    );
    let report = runtime
        .ingest_batch(&mut cache, batch(ReactiveInput::Log(log), context(112, 1)))
        .expect("RedStone AnswerUpdated event should commit");
    tracker
        .apply_batch_report(&report)
        .expect("RedStone AnswerUpdated hook should update typed state");

    let price = tracker.price("redstone-eth-usd").expect("updated price");
    assert_eq!(price.raw_answer, I256::unchecked_from(61_000_000_000_i64));
    assert_eq!(price.round_data().round_id, U256::from(8_u64));
    assert_eq!(price.updated_at, 1_700_000_180);
}

#[tokio::test]
async fn redstone_value_update_ignores_non_matching_data_feed_id() {
    let price_feed = Address::repeat_byte(0x50);
    let adapter = Address::repeat_byte(0x60);
    let registration = redstone_registration(price_feed, adapter, data_feed_id("ETH"));
    let mut tracker = OracleTracker::from_registrations_at_timestamp(
        vec![(
            registration.clone(),
            round(1, I256::unchecked_from(2_000_000_000_i64), 1_700_000_000),
        )],
        1_700_000_010,
    )
    .expect("seed RedStone tracker");
    let handler = RedstoneReactiveHandler::new(vec![registration]);
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime
        .register_handler(Arc::new(handler))
        .expect("register RedStone handler");
    let mut cache = setup_cache().await;

    let log = rpc_log(
        adapter,
        vec![REDSTONE_VALUE_UPDATE_TOPIC],
        encode_words(&[
            U256::from(99_000_000_000_u64),
            U256::from_be_slice(data_feed_id("BTC").as_slice()),
            U256::from(1_700_000_120_u64),
        ]),
        113,
        9,
    );
    let report = runtime
        .ingest_batch(&mut cache, batch(ReactiveInput::Log(log), context(113, 9)))
        .expect("non-matching RedStone event should still be handled cleanly");

    tracker
        .apply_batch_report(&report)
        .expect("non-matching hook set should be ignored");
    let price = tracker.price("redstone-eth-usd").expect("original price");
    assert_eq!(price.raw_answer, I256::unchecked_from(2_000_000_000_i64));
    assert_eq!(price.updated_at, 1_700_000_000);
    assert!(tracker.pending_reconciliations().is_empty());
}

fn redstone_registration(
    price_feed: Address,
    adapter: Address,
    data_feed_id: B256,
) -> FeedRegistration {
    FeedRegistration {
        id: FeedId::new("redstone-eth-usd"),
        proxy: price_feed,
        label: Some("RedStone ETH/USD".to_string()),
        base: Some("ETH".to_string()),
        quote: Some("USD".to_string()),
        staleness: StalenessPolicy::max_age(3_600),
        current_aggregator: Some(adapter),
        aggregator_layout: None,
        metadata: FeedMetadata {
            decimals: 8,
            description: "RedStone ETH/USD".to_string(),
            version: U256::from(1_u64),
        },
        source: FeedSource::redstone_push(price_feed, adapter, data_feed_id),
        status: OracleFeedStatus::Ready,
    }
}

async fn setup_cache() -> EvmCache {
    let asserter = Asserter::new();
    let client = RpcClient::mocked(asserter);
    let provider = RootProvider::<AnyNetwork>::new(client);
    EvmCache::new(Arc::new(provider)).await
}

fn round(id: u64, answer: I256, updated_at: u64) -> RoundData {
    RoundData {
        round_id: U256::from(id),
        answer,
        started_at: updated_at,
        updated_at,
        answered_in_round: U256::from(id),
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
        transaction_hash: Some(B256::repeat_byte(0x99)),
        transaction_index: Some(0),
        log_index: Some(log_index),
        removed: false,
    }
}

fn block_hash(block_number: u64) -> B256 {
    B256::repeat_byte(block_number as u8)
}

fn context(block_number: u64, log_index: u64) -> evm_fork_cache::reactive::ReactiveContext {
    let block = BlockRef {
        number: block_number,
        hash: block_hash(block_number),
        parent_hash: Some(block_hash(block_number.saturating_sub(1))),
        timestamp: Some(1_700_000_000 + block_number),
    };
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

fn batch(
    input: ReactiveInput<Ethereum>,
    context: evm_fork_cache::reactive::ReactiveContext,
) -> ReactiveInputBatch<Ethereum> {
    ReactiveInputBatch::new(vec![ReactiveInputRecord::new(input, context)])
}

fn data_feed_id(symbol: &str) -> B256 {
    let mut bytes = [0_u8; 32];
    bytes[..symbol.len()].copy_from_slice(symbol.as_bytes());
    B256::from(bytes)
}

fn encode_words(words: &[U256]) -> Vec<u8> {
    words
        .iter()
        .flat_map(|word| word.to_be_bytes::<32>())
        .collect()
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
