#![cfg(feature = "pyth")]

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
        ReactiveInputRecord, ReactiveInterest, ReactiveRuntime, StateEffectQuality,
    },
    state_update::PurgeScope,
};
use evm_oracle_state::{
    FeedId, FeedMetadata, FeedRegistration, FeedSource, ORACLE_SIGNAL_NAMESPACE, OracleFeedStatus,
    OraclePriceUpdate, OracleReadOverlay, OracleSignalKind, OracleTracker, OracleValueSource,
    PYTH_PRICE_FEED_UPDATE_TOPIC, PythReactiveHandler, RoundData, StalenessPolicy,
    decode_pyth_price_feed_update,
};

sol! {
    struct PythPrice {
        int64 price;
        uint64 conf;
        int32 expo;
        uint256 publishTime;
    }

    interface PythInterface {
        function getPriceUnsafe(bytes32 id) external view returns (PythPrice memory price);
        function getPriceNoOlderThan(bytes32 id, uint256 age) external view returns (PythPrice memory price);
    }
}

use PythInterface::{getPriceNoOlderThanCall, getPriceUnsafeCall};

#[test]
fn pyth_price_feed_update_decodes_id_publish_time_price_and_confidence() {
    let pyth = Address::repeat_byte(0x44);
    let price_id = price_id(0x11);
    let log = pyth_update_log(
        pyth,
        price_id,
        1_700_000_120,
        -2_250_000_000_i64,
        12_345,
        110,
        3,
    );

    let event = decode_pyth_price_feed_update(&log).expect("PriceFeedUpdate should decode");
    assert_eq!(event.pyth, pyth);
    assert_eq!(event.price_id, price_id);
    assert_eq!(event.publish_time, 1_700_000_120);
    assert_eq!(event.price, -2_250_000_000_i64);
    assert_eq!(event.conf, 12_345);
    assert_eq!(event.block_number, Some(110));
    assert_eq!(event.log_index, Some(3));
    assert!(!event.removed);
}

#[tokio::test]
async fn pyth_event_updates_matching_feed_and_overlay_serves_pyth_reads() {
    let pyth = Address::repeat_byte(0x10);
    let eth_price_id = price_id(0x01);
    let btc_price_id = price_id(0x02);
    let eth_proxy = Address::repeat_byte(0x81);
    let btc_proxy = Address::repeat_byte(0x82);
    let eth_registration = pyth_registration(
        "pyth-eth-usd",
        eth_proxy,
        pyth,
        eth_price_id,
        "ETH",
        -8,
        10_000,
    );
    let btc_registration = pyth_registration(
        "pyth-btc-usd",
        btc_proxy,
        pyth,
        btc_price_id,
        "BTC",
        -8,
        20_000,
    );
    let mut tracker = OracleTracker::from_registrations_at_timestamp(
        vec![
            (
                eth_registration.clone(),
                round(1_700_000_000, I256::unchecked_from(2_000_000_000_i64)),
            ),
            (
                btc_registration.clone(),
                round(1_700_000_000, I256::unchecked_from(60_000_000_000_i64)),
            ),
        ],
        1_700_000_010,
    )
    .expect("seed Pyth tracker");
    let handler = PythReactiveHandler::new(vec![eth_registration, btc_registration]);

    assert_eq!(
        handler.interests().len(),
        1,
        "one Pyth contract with multiple price ids should subscribe once"
    );
    assert!(handler.interests().iter().any(|interest| match interest {
        ReactiveInterest::Logs(interest) => interest.matches(&pyth_update_log(
            pyth,
            eth_price_id,
            1_700_000_120,
            2_250_000_000_i64,
            12_345,
            111,
            4,
        )),
        _ => false,
    }));

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime
        .register_handler(Arc::new(handler))
        .expect("register Pyth handler");
    let mut cache = setup_cache().await;
    let report = runtime
        .ingest_batch(
            &mut cache,
            batch(
                ReactiveInput::Log(pyth_update_log(
                    pyth,
                    eth_price_id,
                    1_700_000_120,
                    2_250_000_000_i64,
                    12_345,
                    111,
                    4,
                )),
                context(111, 4),
            ),
        )
        .expect("Pyth event should commit");

    assert_eq!(report.applied.len(), 1);
    let applied = &report.applied[0];
    assert_eq!(applied.quality, StateEffectQuality::RequiresRepair);
    assert!(applied.invalidations.iter().any(|request| {
        request.address == pyth && matches!(request.scope, PurgeScope::AllStorage)
    }));

    let hook = applied
        .hook_signals
        .iter()
        .find_map(|signal| match signal.payload.as_ref() {
            Some(payload)
                if signal.namespace.as_ref() == ORACLE_SIGNAL_NAMESPACE
                    && signal.kind.as_ref() == OracleSignalKind::PriceUpdate.as_str() =>
            {
                payload.downcast_ref::<OraclePriceUpdate>()
            }
            _ => None,
        })
        .expect("Pyth price update hook");
    assert_eq!(hook.id.as_str(), "pyth-eth-usd");
    assert_eq!(hook.proxy, eth_proxy);
    assert_eq!(hook.aggregator, pyth);
    assert_eq!(hook.raw_answer, I256::unchecked_from(2_250_000_000_i64));
    assert_eq!(hook.decimals, 8);
    assert_eq!(hook.event_round_id, U256::from(1_700_000_120_u64));
    assert_eq!(hook.started_at, 1_700_000_120);
    assert_eq!(hook.updated_at, 1_700_000_120);

    tracker
        .apply_batch_report(&report)
        .expect("Pyth hook should update typed state");
    let eth_price = tracker.price("pyth-eth-usd").expect("updated ETH price");
    assert_eq!(
        eth_price.raw_answer,
        I256::unchecked_from(2_250_000_000_i64)
    );
    assert_eq!(eth_price.decimals, 8);
    assert_eq!(eth_price.source, OracleValueSource::Event);
    assert!(eth_price.is_event_pending());
    let btc_price = tracker.price("pyth-btc-usd").expect("BTC unchanged");
    assert_eq!(
        btc_price.raw_answer,
        I256::unchecked_from(60_000_000_000_i64)
    );
    assert!(
        tracker.pending_reconciliations().is_empty(),
        "Pyth events should not enqueue Chainlink proxy reconciliation"
    );

    let overlay = OracleReadOverlay::new(&tracker);
    let unsafe_bytes = overlay
        .try_call(pyth, &getPriceUnsafeCall { id: eth_price_id }.abi_encode())
        .expect("overlay call should decode")
        .expect("Pyth getPriceUnsafe should be served from typed state");
    let unsafe_price =
        getPriceUnsafeCall::abi_decode_returns(&unsafe_bytes).expect("getPriceUnsafe ABI");
    assert_eq!(unsafe_price.price, 2_250_000_000_i64);
    assert_eq!(unsafe_price.conf, 12_345);
    assert_eq!(unsafe_price.expo, -8);
    assert_eq!(unsafe_price.publishTime, U256::from(1_700_000_120_u64));

    let no_older_than_bytes = overlay
        .try_call(
            pyth,
            &getPriceNoOlderThanCall {
                id: eth_price_id,
                age: U256::from(60_u64),
            }
            .abi_encode(),
        )
        .expect("overlay call should decode")
        .expect("Pyth getPriceNoOlderThan should be served from typed state");
    let no_older_than_price = getPriceNoOlderThanCall::abi_decode_returns(&no_older_than_bytes)
        .expect("getPriceNoOlderThan ABI");
    assert_eq!(no_older_than_price.price, 2_250_000_000_i64);
    assert_eq!(no_older_than_price.conf, 12_345);
    assert_eq!(no_older_than_price.expo, -8);
    assert_eq!(
        no_older_than_price.publishTime,
        U256::from(1_700_000_120_u64)
    );

    assert!(
        overlay
            .try_call(
                pyth,
                &getPriceUnsafeCall { id: price_id(0xff) }.abi_encode(),
            )
            .expect("overlay should ignore unknown ids")
            .is_none(),
        "unknown Pyth price ids should fall through to the underlying cache/RPC"
    );
}

#[tokio::test]
async fn pyth_removed_event_marks_existing_event_snapshot_unknown() {
    let pyth = Address::repeat_byte(0x10);
    let eth_price_id = price_id(0x01);
    let eth_proxy = Address::repeat_byte(0x81);
    let registration = pyth_registration(
        "pyth-eth-usd",
        eth_proxy,
        pyth,
        eth_price_id,
        "ETH",
        -8,
        10_000,
    );
    let mut tracker = OracleTracker::from_registrations_at_timestamp(
        vec![(
            registration.clone(),
            round(1_700_000_000, I256::unchecked_from(2_000_000_000_i64)),
        )],
        1_700_000_010,
    )
    .expect("seed Pyth tracker");
    let handler = PythReactiveHandler::new(vec![registration]);
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime
        .register_handler(Arc::new(handler))
        .expect("register Pyth handler");
    let mut cache = setup_cache().await;

    let included = runtime
        .ingest_batch(
            &mut cache,
            batch(
                ReactiveInput::Log(pyth_update_log(
                    pyth,
                    eth_price_id,
                    1_700_000_120,
                    2_250_000_000_i64,
                    12_345,
                    111,
                    4,
                )),
                context(111, 4),
            ),
        )
        .expect("included Pyth event should commit");
    tracker
        .apply_batch_report(&included)
        .expect("included event should update tracker");
    assert!(tracker.price("pyth-eth-usd").unwrap().is_event_pending());

    let mut removed_log = pyth_update_log(
        pyth,
        eth_price_id,
        1_700_000_120,
        2_250_000_000_i64,
        12_345,
        111,
        4,
    );
    removed_log.removed = true;
    let removed = runtime
        .ingest_batch(
            &mut cache,
            batch_with_status(ReactiveInput::Log(removed_log), context_reorged(111, 4)),
        )
        .expect("removed Pyth event should commit");
    tracker
        .apply_batch_report(&removed)
        .expect("removed event should mark event state unknown");

    let price = tracker.price("pyth-eth-usd").expect("price after reorg");
    assert!(
        !price.is_actionable(),
        "removed Pyth event should not remain actionable"
    );
}

#[test]
fn pyth_overlay_unscales_positive_exponent_prices_to_pyth_abi_shape() {
    let pyth = Address::repeat_byte(0x10);
    let price_id = price_id(0x03);
    let proxy = Address::repeat_byte(0x83);
    let registration = pyth_registration("pyth-positive-expo", proxy, pyth, price_id, "TEST", 2, 7);
    let tracker = OracleTracker::from_registrations_at_timestamp(
        vec![(
            registration,
            round(1_700_000_000, I256::unchecked_from(12_300_i64)),
        )],
        1_700_000_010,
    )
    .expect("seed positive-exponent Pyth tracker");

    let overlay = OracleReadOverlay::new(&tracker);
    let bytes = overlay
        .try_call(pyth, &getPriceUnsafeCall { id: price_id }.abi_encode())
        .expect("overlay call should decode")
        .expect("Pyth getPriceUnsafe should be served from typed state");
    let price = getPriceUnsafeCall::abi_decode_returns(&bytes).expect("getPriceUnsafe ABI");
    assert_eq!(
        price.price, 123,
        "Pyth ABI price should be unscaled when typed raw_answer was scaled for positive expo"
    );
    assert_eq!(price.conf, 7);
    assert_eq!(price.expo, 2);
    assert_eq!(price.publishTime, U256::from(1_700_000_000_u64));
}

fn pyth_registration(
    id: &str,
    proxy: Address,
    pyth: Address,
    price_id: B256,
    base: &str,
    expo: i32,
    conf: u64,
) -> FeedRegistration {
    FeedRegistration {
        id: FeedId::new(id),
        proxy,
        label: Some(format!("Pyth {base}/USD")),
        base: Some(base.to_string()),
        quote: Some("USD".to_string()),
        staleness: StalenessPolicy::max_age(3_600),
        current_aggregator: Some(pyth),
        aggregator_layout: None,
        metadata: FeedMetadata {
            decimals: if expo <= 0 {
                u8::try_from(-expo).expect("negative Pyth expo maps to decimals")
            } else {
                0
            },
            description: format!("Pyth {base}/USD"),
            version: U256::ZERO,
        },
        source: FeedSource::pyth(pyth, price_id, expo, conf),
        status: OracleFeedStatus::Ready,
    }
}

async fn setup_cache() -> EvmCache {
    let asserter = Asserter::new();
    let client = RpcClient::mocked(asserter);
    let provider = RootProvider::<AnyNetwork>::new(client);
    EvmCache::new(Arc::new(provider)).await
}

fn round(publish_time: u64, answer: I256) -> RoundData {
    RoundData {
        round_id: U256::from(publish_time),
        answer,
        started_at: publish_time,
        updated_at: publish_time,
        answered_in_round: U256::from(publish_time),
    }
}

fn pyth_update_log(
    pyth: Address,
    price_id: B256,
    publish_time: u64,
    price: i64,
    conf: u64,
    block_number: u64,
    log_index: u64,
) -> RpcLog {
    rpc_log(
        pyth,
        vec![PYTH_PRICE_FEED_UPDATE_TOPIC, price_id],
        encode_words(&[
            U256::from(publish_time),
            I256::unchecked_from(price).into_raw(),
            U256::from(conf),
        ]),
        block_number,
        log_index,
    )
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

fn context_reorged(block_number: u64, log_index: u64) -> evm_fork_cache::reactive::ReactiveContext {
    let block = BlockRef {
        number: block_number,
        hash: block_hash(block_number),
        parent_hash: Some(block_hash(block_number.saturating_sub(1))),
        timestamp: Some(1_700_000_000 + block_number),
    };
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

fn batch(
    input: ReactiveInput<Ethereum>,
    context: evm_fork_cache::reactive::ReactiveContext,
) -> ReactiveInputBatch<Ethereum> {
    ReactiveInputBatch::new(vec![ReactiveInputRecord::new(input, context)])
}

fn batch_with_status(
    input: ReactiveInput<Ethereum>,
    context: evm_fork_cache::reactive::ReactiveContext,
) -> ReactiveInputBatch<Ethereum> {
    batch(input, context)
}

fn price_id(byte: u8) -> B256 {
    B256::repeat_byte(byte)
}

fn encode_words(words: &[U256]) -> Vec<u8> {
    words
        .iter()
        .flat_map(|word| word.to_be_bytes::<32>())
        .collect()
}

fn block_hash(block_number: u64) -> B256 {
    B256::repeat_byte(block_number as u8)
}
