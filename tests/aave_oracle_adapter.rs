#![cfg(feature = "aave")]

use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

use evm_oracle_state::alloy_eips::BlockId;
use evm_oracle_state::alloy_network::Ethereum;
use alloy_primitives::{Address, B256, Bytes, I256, Log as PrimitiveLog, U256};
use evm_oracle_state::alloy_provider::{RootProvider, network::AnyNetwork};
use evm_oracle_state::alloy_rpc_client::RpcClient;
use evm_oracle_state::alloy_rpc_types_eth::Log as RpcLog;
use evm_oracle_state::alloy_provider::mock::Asserter;
use evm_fork_cache::{
    cache::EvmCache,
    reactive::{
        BlockRef, ChainStatus, HandlerId, HookSignal, InputSource, ReactiveConfig, ReactiveInput,
        ReactiveInputBatch, ReactiveInputRecord, ReactiveInterest, ReactiveRuntime, RouteKey,
        RouteKeySpec, StateEffectQuality,
    },
};
use evm_oracle_state::{
    ANSWER_UPDATED_TOPIC, AaveAsset, AaveV3OracleAdapter, AssetId, ChainlinkFeedProvider,
    Denomination, FeedId, FeedMetadata, FeedRegistration, FeedSource, OracleFeedStatus,
    OracleHookEvent, OraclePriceUpdate, OracleReactiveHandler, OracleReconciler, OracleRoundStatus,
    OracleTracker, OracleValueSource, ProviderFuture, RoundData, StalenessPolicy,
};

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
        transaction_hash: Some(B256::repeat_byte(0x44)),
        transaction_index: Some(0),
        log_index: Some(log_index),
        removed: false,
    }
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

fn word(value: U256) -> Vec<u8> {
    value.to_be_bytes::<32>().to_vec()
}

fn uint_topic(value: U256) -> B256 {
    B256::from(value.to_be_bytes::<32>())
}

fn int_topic(value: I256) -> B256 {
    B256::from(value.into_raw().to_be_bytes::<32>())
}

fn metadata(description: &str) -> FeedMetadata {
    FeedMetadata {
        decimals: 8,
        description: description.to_string(),
        version: U256::ZERO,
    }
}

fn round(id: u64, answer: i64, updated_at: u64) -> RoundData {
    RoundData {
        round_id: U256::from(id),
        answer: I256::unchecked_from(answer),
        started_at: updated_at,
        updated_at,
        answered_in_round: U256::from(id),
    }
}

fn aave_cap_registration(
    source: Address,
    underlying_proxy: Address,
    underlying_aggregator: Address,
    cap: i64,
) -> FeedRegistration {
    FeedRegistration {
        id: FeedId::new("aave-v3-usdt"),
        proxy: source,
        label: Some("Aave V3 USDT source".to_string()),
        base: Some("USDT".to_string()),
        quote: Some("USD".to_string()),
        staleness: StalenessPolicy::max_age(24 * 60 * 60),
        current_aggregator: Some(underlying_aggregator),
        aggregator_layout: None,
        metadata: metadata("Capped USDT/USD"),
        source: FeedSource::aave_price_cap_stable(
            source,
            underlying_proxy,
            I256::unchecked_from(cap),
        ),
        status: OracleFeedStatus::Ready,
    }
}

fn aave_ratio_cap_registration(
    source: Address,
    base_to_usd_proxy: Address,
    base_to_usd_aggregator: Address,
    ratio_provider: Address,
    current_ratio: i64,
    max_ratio: i64,
    ratio_decimals: u8,
) -> FeedRegistration {
    FeedRegistration {
        id: FeedId::new("aave-v3-wsteth"),
        proxy: source,
        label: Some("Aave V3 wstETH source".to_string()),
        base: Some("wstETH".to_string()),
        quote: Some("USD".to_string()),
        staleness: StalenessPolicy::max_age(24 * 60 * 60),
        current_aggregator: Some(base_to_usd_aggregator),
        aggregator_layout: None,
        metadata: metadata("Capped wstETH/USD"),
        source: FeedSource::aave_ratio_cap(
            source,
            base_to_usd_proxy,
            ratio_provider,
            I256::unchecked_from(current_ratio),
            I256::unchecked_from(max_ratio),
            ratio_decimals,
        ),
        status: OracleFeedStatus::Ready,
    }
}

fn aave_synchronicity_peg_to_base_registration(
    source: Address,
    asset_to_peg_proxy: Address,
    asset_to_peg_aggregator: Address,
    asset_to_peg_answer: i64,
    peg_to_base_proxy: Address,
    peg_to_base_aggregator: Address,
    peg_to_base_answer: i64,
) -> FeedRegistration {
    FeedRegistration {
        id: FeedId::new("aave-v3-wbtc"),
        proxy: source,
        label: Some("Aave V3 WBTC source".to_string()),
        base: Some("WBTC".to_string()),
        quote: Some("USD".to_string()),
        staleness: StalenessPolicy::max_age(24 * 60 * 60),
        current_aggregator: Some(asset_to_peg_aggregator),
        aggregator_layout: None,
        metadata: metadata("Composed WBTC/USD"),
        source: FeedSource::aave_synchronicity_peg_to_base(
            source,
            asset_to_peg_proxy,
            asset_to_peg_aggregator,
            I256::unchecked_from(asset_to_peg_answer),
            8,
            peg_to_base_proxy,
            peg_to_base_aggregator,
            I256::unchecked_from(peg_to_base_answer),
            8,
            8,
        ),
        status: OracleFeedStatus::Ready,
    }
}

fn aave_fixed_price_registration(source: Address, price: i64) -> FeedRegistration {
    FeedRegistration {
        id: FeedId::new("aave-v3-musd"),
        proxy: source,
        label: Some("Aave V3 mUSD source".to_string()),
        base: Some("mUSD".to_string()),
        quote: Some("USD".to_string()),
        staleness: StalenessPolicy::max_age(24 * 60 * 60),
        current_aggregator: None,
        aggregator_layout: None,
        metadata: metadata("Fixed mUSD/USD"),
        source: FeedSource::aave_fixed_price(source, I256::unchecked_from(price)),
        status: OracleFeedStatus::Ready,
    }
}

fn price_update_hooks(signals: &[HookSignal]) -> Vec<OraclePriceUpdate> {
    signals
        .iter()
        .filter_map(|signal| {
            if signal.kind.as_ref() != "oracle.price_update" {
                return None;
            }
            signal
                .payload
                .as_ref()
                .and_then(|payload| payload.downcast_ref::<OraclePriceUpdate>())
                .cloned()
        })
        .collect()
}

#[test]
fn aave_builder_api_tracks_assets_declaratively() {
    let oracle = Address::repeat_byte(0x01);
    let usdt = Address::repeat_byte(0x02);
    let adapter = AaveV3OracleAdapter::new(oracle)
        .asset(
            AaveAsset::new(usdt)
                .id("aave-v3-usdt")
                .label("Aave V3 USDT")
                .base(AssetId::symbol("USDT"))
                .quote(Denomination::Usd)
                .max_age_secs(24 * 60 * 60),
        )
        .now_timestamp(1_700_000_000);

    assert_eq!(adapter.oracle(), oracle);
    assert_eq!(adapter.assets().len(), 1);
    assert_eq!(adapter.assets()[0].asset(), usdt);
}

#[tokio::test]
async fn price_cap_stable_underlying_event_emits_aave_normalized_price() {
    let source = Address::repeat_byte(0x10);
    let underlying_proxy = Address::repeat_byte(0x11);
    let underlying_aggregator = Address::repeat_byte(0x12);
    let cap = 104_000_000_i64;
    let registration = aave_cap_registration(source, underlying_proxy, underlying_aggregator, cap);
    let mut tracker = OracleTracker::from_registrations_at_timestamp(
        vec![(registration.clone(), round(100, 99_000_000, 1_700_000_000))],
        1_700_000_200,
    )
    .expect("seed tracker");
    let handler = OracleReactiveHandler::new(tracker.registrations().collect::<Vec<_>>());
    let matching_interest = handler
        .interests()
        .into_iter()
        .find_map(|interest| match interest {
            ReactiveInterest::Logs(interest) => {
                let log = rpc_log(
                    underlying_aggregator,
                    vec![
                        ANSWER_UPDATED_TOPIC,
                        int_topic(I256::unchecked_from(110_000_000_i64)),
                        uint_topic(U256::from(101_u64)),
                    ],
                    word(U256::from(1_700_000_180_u64)),
                    102,
                    0,
                );
                interest.matches(&log).then_some(interest)
            }
            _ => None,
        })
        .expect("underlying Chainlink aggregator should be subscribed");
    assert_eq!(
        matching_interest.route_key(&rpc_log(
            underlying_aggregator,
            vec![
                ANSWER_UPDATED_TOPIC,
                int_topic(I256::unchecked_from(110_000_000_i64)),
                uint_topic(U256::from(101_u64)),
            ],
            word(U256::from(1_700_000_180_u64)),
            102,
            0,
        )),
        Some(RouteKey::Address(underlying_aggregator))
    );
    assert!(matches!(
        matching_interest.route_key,
        Some(RouteKeySpec::EmitterAddress)
    ));

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime
        .register_handler(Arc::new(handler))
        .expect("register handler");
    let mut cache = setup_cache().await;
    let log = rpc_log(
        underlying_aggregator,
        vec![
            ANSWER_UPDATED_TOPIC,
            int_topic(I256::unchecked_from(110_000_000_i64)),
            uint_topic(U256::from(101_u64)),
        ],
        word(U256::from(1_700_000_180_u64)),
        102,
        0,
    );
    let batch_report = runtime
        .ingest_batch(&mut cache, batch(ReactiveInput::Log(log), context(102, 0)))
        .expect("runtime should commit handler effects");

    assert_eq!(batch_report.applied.len(), 1);
    let applied = &batch_report.applied[0];
    assert_eq!(
        applied.handler_id,
        HandlerId::new("evm-oracle-state.chainlink")
    );
    assert_eq!(applied.quality, StateEffectQuality::RequiresRepair);
    let hooks = price_update_hooks(&applied.hook_signals);
    assert_eq!(hooks.len(), 1);
    let hook = &hooks[0];
    assert_eq!(hook.proxy, source);
    assert_eq!(hook.aggregator, underlying_aggregator);
    assert_eq!(hook.raw_answer, I256::unchecked_from(cap));
    assert_eq!(hook.base.as_deref(), Some("USDT"));
    assert_eq!(hook.quote.as_deref(), Some("USD"));

    tracker
        .apply_batch_report(&batch_report)
        .expect("committed report should update typed state");
    let snapshot = tracker
        .latest(source)
        .expect("updated Aave source snapshot");
    assert_eq!(snapshot.round.answer, I256::unchecked_from(cap));
    assert_eq!(snapshot.round.updated_at, 1_700_000_180);
    assert_eq!(snapshot.source, OracleValueSource::Derived);
    assert_eq!(tracker.pending_reconciliations().len(), 1);
    assert_eq!(tracker.pending_reconciliations()[0].proxy, source);
}

#[tokio::test]
async fn ratio_cap_underlying_event_emits_capped_aave_price() {
    let source = Address::repeat_byte(0x30);
    let base_to_usd_proxy = Address::repeat_byte(0x31);
    let base_to_usd_aggregator = Address::repeat_byte(0x32);
    let ratio_provider = Address::repeat_byte(0x33);
    let registration = aave_ratio_cap_registration(
        source,
        base_to_usd_proxy,
        base_to_usd_aggregator,
        ratio_provider,
        1_080_000_000_000_000_000,
        1_050_000_000_000_000_000,
        18,
    );
    let mut tracker = OracleTracker::from_registrations_at_timestamp(
        vec![(
            registration.clone(),
            round(100, 2_058_000_000, 1_700_000_000),
        )],
        1_700_000_200,
    )
    .expect("seed tracker");
    let handler = OracleReactiveHandler::new(tracker.registrations().collect::<Vec<_>>());
    let matching_interest = handler
        .interests()
        .into_iter()
        .find_map(|interest| match interest {
            ReactiveInterest::Logs(interest) => {
                let log = rpc_log(
                    base_to_usd_aggregator,
                    vec![
                        ANSWER_UPDATED_TOPIC,
                        int_topic(I256::unchecked_from(2_000_000_000_i64)),
                        uint_topic(U256::from(101_u64)),
                    ],
                    word(U256::from(1_700_000_180_u64)),
                    103,
                    0,
                );
                interest.matches(&log).then_some(interest)
            }
            _ => None,
        })
        .expect("ratio-cap source should subscribe to its base Chainlink aggregator");
    assert_eq!(
        matching_interest.route_key(&rpc_log(
            base_to_usd_aggregator,
            vec![
                ANSWER_UPDATED_TOPIC,
                int_topic(I256::unchecked_from(2_000_000_000_i64)),
                uint_topic(U256::from(101_u64)),
            ],
            word(U256::from(1_700_000_180_u64)),
            103,
            0,
        )),
        Some(RouteKey::Address(base_to_usd_aggregator))
    );

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime
        .register_handler(Arc::new(handler))
        .expect("register handler");
    let mut cache = setup_cache().await;
    let log = rpc_log(
        base_to_usd_aggregator,
        vec![
            ANSWER_UPDATED_TOPIC,
            int_topic(I256::unchecked_from(2_000_000_000_i64)),
            uint_topic(U256::from(101_u64)),
        ],
        word(U256::from(1_700_000_180_u64)),
        103,
        0,
    );
    let batch_report = runtime
        .ingest_batch(&mut cache, batch(ReactiveInput::Log(log), context(103, 0)))
        .expect("runtime should commit handler effects");

    let hooks = price_update_hooks(&batch_report.applied[0].hook_signals);
    assert_eq!(hooks.len(), 1);
    assert_eq!(hooks[0].proxy, source);
    assert_eq!(hooks[0].aggregator, base_to_usd_aggregator);
    assert_eq!(hooks[0].raw_answer, I256::unchecked_from(2_100_000_000_i64));

    tracker
        .apply_batch_report(&batch_report)
        .expect("committed report should update typed state");
    let snapshot = tracker.latest(source).expect("ratio-cap event snapshot");
    assert_eq!(
        snapshot.round.answer,
        I256::unchecked_from(2_100_000_000_i64)
    );
    assert_eq!(tracker.pending_reconciliations()[0].proxy, source);
}

#[tokio::test]
async fn synchronicity_secondary_dependency_event_updates_user_facing_source() {
    let source = Address::repeat_byte(0x40);
    let asset_to_peg_proxy = Address::repeat_byte(0x41);
    let asset_to_peg_aggregator = Address::repeat_byte(0x42);
    let peg_to_base_proxy = Address::repeat_byte(0x43);
    let peg_to_base_aggregator = Address::repeat_byte(0x44);
    let registration = aave_synchronicity_peg_to_base_registration(
        source,
        asset_to_peg_proxy,
        asset_to_peg_aggregator,
        50_000_000,
        peg_to_base_proxy,
        peg_to_base_aggregator,
        200_000_000_000,
    );
    let mut tracker = OracleTracker::from_registrations_at_timestamp(
        vec![(
            registration.clone(),
            round(100, 100_000_000_000, 1_700_000_000),
        )],
        1_700_000_200,
    )
    .expect("seed tracker");
    let handler = OracleReactiveHandler::new(tracker.registrations().collect::<Vec<_>>());
    let matching_interest = handler
        .interests()
        .into_iter()
        .find_map(|interest| match interest {
            ReactiveInterest::Logs(interest) => {
                let log = rpc_log(
                    peg_to_base_aggregator,
                    vec![
                        ANSWER_UPDATED_TOPIC,
                        int_topic(I256::unchecked_from(210_000_000_000_i64)),
                        uint_topic(U256::from(101_u64)),
                    ],
                    word(U256::from(1_700_000_180_u64)),
                    104,
                    0,
                );
                interest.matches(&log).then_some(interest)
            }
            _ => None,
        })
        .expect("synchronicity source should subscribe to secondary dependency aggregator");
    assert_eq!(
        matching_interest.route_key(&rpc_log(
            peg_to_base_aggregator,
            vec![
                ANSWER_UPDATED_TOPIC,
                int_topic(I256::unchecked_from(210_000_000_000_i64)),
                uint_topic(U256::from(101_u64)),
            ],
            word(U256::from(1_700_000_180_u64)),
            104,
            0,
        )),
        Some(RouteKey::Address(peg_to_base_aggregator))
    );

    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime
        .register_handler(Arc::new(handler))
        .expect("register handler");
    let mut cache = setup_cache().await;
    let log = rpc_log(
        peg_to_base_aggregator,
        vec![
            ANSWER_UPDATED_TOPIC,
            int_topic(I256::unchecked_from(210_000_000_000_i64)),
            uint_topic(U256::from(101_u64)),
        ],
        word(U256::from(1_700_000_180_u64)),
        104,
        0,
    );
    let batch_report = runtime
        .ingest_batch(&mut cache, batch(ReactiveInput::Log(log), context(104, 0)))
        .expect("runtime should commit handler effects");

    let hooks = price_update_hooks(&batch_report.applied[0].hook_signals);
    assert_eq!(hooks.len(), 1);
    assert_eq!(hooks[0].proxy, source);
    assert_eq!(hooks[0].aggregator, peg_to_base_aggregator);
    assert_eq!(
        hooks[0].raw_answer,
        I256::unchecked_from(105_000_000_000_i64)
    );

    tracker
        .apply_batch_report(&batch_report)
        .expect("committed secondary dependency update should update typed state");
    let snapshot = tracker
        .latest(source)
        .expect("synchronicity event snapshot");
    assert_eq!(
        snapshot.round.answer,
        I256::unchecked_from(105_000_000_000_i64)
    );
    assert_eq!(tracker.pending_reconciliations()[0].proxy, source);
}

#[test]
fn fixed_price_aave_source_has_no_chainlink_event_interest() {
    let source = Address::repeat_byte(0x50);
    let registration = aave_fixed_price_registration(source, 100_000_000);
    let tracker = OracleTracker::from_registrations_at_timestamp(
        vec![(registration, round(1, 100_000_000, 1_700_000_000))],
        1_700_000_200,
    )
    .expect("seed tracker");

    let handler = OracleReactiveHandler::new(tracker.registrations().collect::<Vec<_>>());
    assert!(
        handler.interests().is_empty(),
        "fixed-price Aave sources should be seeded but not subscribed as Chainlink feeds"
    );
    let snapshot = tracker.latest(source).expect("fixed source snapshot");
    assert_eq!(snapshot.round.answer, I256::unchecked_from(100_000_000_i64));
    assert_eq!(snapshot.round_status, OracleRoundStatus::Fresh);
}

#[test]
fn derived_aave_sources_zero_non_positive_dependency_answers() {
    let source = Address::repeat_byte(0x60);
    let base_to_usd_proxy = Address::repeat_byte(0x61);
    let base_to_usd_aggregator = Address::repeat_byte(0x62);
    let ratio_provider = Address::repeat_byte(0x63);
    let ratio_cap = FeedSource::aave_ratio_cap(
        source,
        base_to_usd_proxy,
        ratio_provider,
        I256::unchecked_from(1_050_000_000_000_000_000_i64),
        I256::unchecked_from(1_050_000_000_000_000_000_i64),
        18,
    );
    assert_eq!(
        ratio_cap
            .normalize_answer_from_event(Some(base_to_usd_aggregator), I256::unchecked_from(0_i64)),
        I256::ZERO
    );

    let asset_to_peg_aggregator = Address::repeat_byte(0x64);
    let peg_to_base_aggregator = Address::repeat_byte(0x65);
    let synchronicity = FeedSource::aave_synchronicity_peg_to_base(
        source,
        Address::repeat_byte(0x66),
        asset_to_peg_aggregator,
        I256::unchecked_from(50_000_000_i64),
        8,
        Address::repeat_byte(0x67),
        peg_to_base_aggregator,
        I256::unchecked_from(200_000_000_000_i64),
        8,
        8,
    );
    assert_eq!(
        synchronicity.normalize_answer_from_event(
            Some(peg_to_base_aggregator),
            I256::unchecked_from(-1_i64)
        ),
        I256::ZERO
    );
}

#[tokio::test]
async fn synchronicity_secondary_dependency_reconciliation_reads_both_legs_and_confirms() {
    let source = Address::repeat_byte(0x70);
    let asset_to_peg_proxy = Address::repeat_byte(0x71);
    let asset_to_peg_aggregator = Address::repeat_byte(0x72);
    let peg_to_base_proxy = Address::repeat_byte(0x73);
    let peg_to_base_aggregator = Address::repeat_byte(0x74);
    let registration = aave_synchronicity_peg_to_base_registration(
        source,
        asset_to_peg_proxy,
        asset_to_peg_aggregator,
        50_000_000,
        peg_to_base_proxy,
        peg_to_base_aggregator,
        200_000_000_000,
    );
    let mut tracker = OracleTracker::from_registrations_at_timestamp(
        vec![(
            registration.clone(),
            round(100, 100_000_000_000, 1_700_000_000),
        )],
        1_700_000_200,
    )
    .expect("seed tracker");
    let handler = OracleReactiveHandler::new(tracker.registrations().collect::<Vec<_>>());
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime
        .register_handler(Arc::new(handler))
        .expect("register handler");
    let mut cache = setup_cache().await;
    let log = rpc_log(
        peg_to_base_aggregator,
        vec![
            ANSWER_UPDATED_TOPIC,
            int_topic(I256::unchecked_from(210_000_000_000_i64)),
            uint_topic(U256::from(101_u64)),
        ],
        word(U256::from(1_700_000_180_u64)),
        105,
        0,
    );
    let batch_report = runtime
        .ingest_batch(&mut cache, batch(ReactiveInput::Log(log), context(105, 0)))
        .expect("runtime should commit handler effects");
    tracker
        .apply_batch_report(&batch_report)
        .expect("committed secondary dependency update should update typed state");

    let request = tracker.pending_reconciliations()[0].clone();
    let provider = MultiProxyProvider::new([
        (
            asset_to_peg_proxy,
            asset_to_peg_aggregator,
            round(100, 50_000_000, 1_700_000_000),
        ),
        (
            peg_to_base_proxy,
            peg_to_base_aggregator,
            round(101, 210_000_000_000, 1_700_000_180),
        ),
    ]);
    let mut reconciler = OracleReconciler::default();
    reconciler.enqueue(request);
    let result = reconciler
        .reconcile_next(&mut tracker, &provider)
        .await
        .expect("reconcile should succeed")
        .expect("request should reconcile");

    assert_eq!(
        provider.latest_round_calls(),
        vec![asset_to_peg_proxy, peg_to_base_proxy]
    );
    assert!(matches!(
        result.hooks.as_slice(),
        [OracleHookEvent::PriceConfirmed(confirmed)]
            if confirmed.proxy == source
                && confirmed.raw_answer == I256::unchecked_from(105_000_000_000_i64)
                && confirmed.proxy_round.answer == I256::unchecked_from(105_000_000_000_i64)
                && confirmed.proxy_round.round_id == U256::from(101_u64)
    ));
    let snapshot = tracker
        .latest(source)
        .expect("confirmed synchronicity source snapshot");
    assert_eq!(
        snapshot.round.answer,
        I256::unchecked_from(105_000_000_000_i64)
    );
    assert!(!snapshot.requires_reconciliation());
}

#[tokio::test]
async fn synchronicity_full_reconcile_reads_both_legs() {
    let source = Address::repeat_byte(0x80);
    let asset_to_peg_proxy = Address::repeat_byte(0x81);
    let asset_to_peg_aggregator = Address::repeat_byte(0x82);
    let peg_to_base_proxy = Address::repeat_byte(0x83);
    let peg_to_base_aggregator = Address::repeat_byte(0x84);
    let registration = aave_synchronicity_peg_to_base_registration(
        source,
        asset_to_peg_proxy,
        asset_to_peg_aggregator,
        50_000_000,
        peg_to_base_proxy,
        peg_to_base_aggregator,
        200_000_000_000,
    );
    let mut tracker = OracleTracker::from_registrations_at_timestamp(
        vec![(
            registration.clone(),
            round(100, 100_000_000_000, 1_700_000_000),
        )],
        1_700_000_200,
    )
    .expect("seed tracker");
    let provider = MultiProxyProvider::new([
        (
            asset_to_peg_proxy,
            asset_to_peg_aggregator,
            round(100, 50_000_000, 1_700_000_000),
        ),
        (
            peg_to_base_proxy,
            peg_to_base_aggregator,
            round(101, 210_000_000_000, 1_700_000_180),
        ),
    ]);

    let report = tracker
        .reconcile(&provider)
        .await
        .expect("full reconcile should succeed");

    assert_eq!(report.checked_feeds, 1);
    assert_eq!(
        provider.latest_round_calls(),
        vec![asset_to_peg_proxy, peg_to_base_proxy]
    );
    let snapshot = tracker
        .latest(source)
        .expect("reconciled synchronicity source snapshot");
    assert_eq!(
        snapshot.round.answer,
        I256::unchecked_from(105_000_000_000_i64)
    );
    assert_eq!(snapshot.round.updated_at, 1_700_000_180);
}

#[tokio::test]
async fn price_cap_stable_reconciliation_reads_underlying_proxy_and_confirms_capped_round() {
    let source = Address::repeat_byte(0x20);
    let underlying_proxy = Address::repeat_byte(0x21);
    let underlying_aggregator = Address::repeat_byte(0x22);
    let cap = 104_000_000_i64;
    let registration = aave_cap_registration(source, underlying_proxy, underlying_aggregator, cap);
    let mut tracker = OracleTracker::from_registrations_at_timestamp(
        vec![(registration.clone(), round(100, 99_000_000, 1_700_000_000))],
        1_700_000_200,
    )
    .expect("seed tracker");
    let handler = OracleReactiveHandler::new(tracker.registrations().collect::<Vec<_>>());
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime
        .register_handler(Arc::new(handler))
        .expect("register handler");
    let mut cache = setup_cache().await;
    let log = rpc_log(
        underlying_aggregator,
        vec![
            ANSWER_UPDATED_TOPIC,
            int_topic(I256::unchecked_from(110_000_000_i64)),
            uint_topic(U256::from(101_u64)),
        ],
        word(U256::from(1_700_000_180_u64)),
        102,
        0,
    );
    let batch_report = runtime
        .ingest_batch(&mut cache, batch(ReactiveInput::Log(log), context(102, 0)))
        .expect("runtime should commit handler effects");
    tracker
        .apply_batch_report(&batch_report)
        .expect("committed report should update typed state");

    let request = tracker.pending_reconciliations()[0].clone();
    let provider = RecordingProvider::new(
        underlying_proxy,
        underlying_aggregator,
        round(101, 110_000_000, 1_700_000_180),
    );
    let mut reconciler = OracleReconciler::default();
    reconciler.enqueue(request);
    let result = reconciler
        .reconcile_next(&mut tracker, &provider)
        .await
        .expect("reconcile should succeed")
        .expect("request should reconcile");

    assert_eq!(provider.latest_round_calls(), vec![underlying_proxy]);
    assert!(matches!(
        result.hooks.as_slice(),
        [OracleHookEvent::PriceConfirmed(confirmed)]
            if confirmed.proxy == source
                && confirmed.raw_answer == I256::unchecked_from(cap)
                && confirmed.proxy_round.answer == I256::unchecked_from(cap)
    ));
    let snapshot = tracker
        .latest(source)
        .expect("confirmed Aave source snapshot");
    assert_eq!(snapshot.round.answer, I256::unchecked_from(cap));
    assert!(!snapshot.requires_reconciliation());
}

#[derive(Clone, Debug)]
struct MultiProxyProvider {
    feeds: BTreeMap<Address, (Address, RoundData)>,
    latest_round_calls: Arc<Mutex<Vec<Address>>>,
}

impl MultiProxyProvider {
    fn new(feeds: impl IntoIterator<Item = (Address, Address, RoundData)>) -> Self {
        Self {
            feeds: feeds
                .into_iter()
                .map(|(proxy, aggregator, round)| (proxy, (aggregator, round)))
                .collect(),
            latest_round_calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn latest_round_calls(&self) -> Vec<Address> {
        self.latest_round_calls.lock().unwrap().clone()
    }
}

impl ChainlinkFeedProvider for MultiProxyProvider {
    fn decimals(&self, _proxy: Address) -> ProviderFuture<'_, u8> {
        Box::pin(async { Ok(8) })
    }

    fn description(&self, _proxy: Address) -> ProviderFuture<'_, String> {
        Box::pin(async { Ok("dependency".to_string()) })
    }

    fn version(&self, _proxy: Address) -> ProviderFuture<'_, U256> {
        Box::pin(async { Ok(U256::from(1_u64)) })
    }

    fn latest_round_data(&self, proxy: Address) -> ProviderFuture<'_, RoundData> {
        self.latest_round_data_at(proxy, None)
    }

    fn latest_round_data_at(
        &self,
        proxy: Address,
        _block: Option<evm_oracle_state::OracleBlockRef>,
    ) -> ProviderFuture<'_, RoundData> {
        self.latest_round_calls.lock().unwrap().push(proxy);
        let round = self
            .feeds
            .get(&proxy)
            .unwrap_or_else(|| panic!("unexpected latestRoundData proxy {proxy:?}"))
            .1
            .clone();
        Box::pin(async move { Ok(round) })
    }

    fn aggregator(&self, proxy: Address) -> ProviderFuture<'_, Option<Address>> {
        self.aggregator_at(proxy, None)
    }

    fn aggregator_at(
        &self,
        proxy: Address,
        _block: Option<evm_oracle_state::OracleBlockRef>,
    ) -> ProviderFuture<'_, Option<Address>> {
        let aggregator = self
            .feeds
            .get(&proxy)
            .unwrap_or_else(|| panic!("unexpected aggregator proxy {proxy:?}"))
            .0;
        Box::pin(async move { Ok(Some(aggregator)) })
    }

    fn aggregator_type_and_version(
        &self,
        _aggregator: Address,
    ) -> Pin<
        Box<dyn Future<Output = Result<Option<String>, evm_oracle_state::OracleError>> + Send + '_>,
    > {
        Box::pin(async { Ok(None) })
    }

    fn aggregator_code_hash(
        &self,
        _aggregator: Address,
    ) -> Pin<
        Box<dyn Future<Output = Result<Option<B256>, evm_oracle_state::OracleError>> + Send + '_>,
    > {
        Box::pin(async { Ok(None) })
    }
}

#[derive(Clone, Debug)]
struct RecordingProvider {
    expected_proxy: Address,
    aggregator: Address,
    latest_round: RoundData,
    latest_round_calls: Arc<Mutex<Vec<Address>>>,
}

impl RecordingProvider {
    fn new(expected_proxy: Address, aggregator: Address, latest_round: RoundData) -> Self {
        Self {
            expected_proxy,
            aggregator,
            latest_round,
            latest_round_calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn latest_round_calls(&self) -> Vec<Address> {
        self.latest_round_calls.lock().unwrap().clone()
    }
}

impl ChainlinkFeedProvider for RecordingProvider {
    fn decimals(&self, _proxy: Address) -> ProviderFuture<'_, u8> {
        Box::pin(async { Ok(8) })
    }

    fn description(&self, _proxy: Address) -> ProviderFuture<'_, String> {
        Box::pin(async { Ok("USDT / USD".to_string()) })
    }

    fn version(&self, _proxy: Address) -> ProviderFuture<'_, U256> {
        Box::pin(async { Ok(U256::from(6_u64)) })
    }

    fn latest_round_data(&self, proxy: Address) -> ProviderFuture<'_, RoundData> {
        self.latest_round_data_at(proxy, None)
    }

    fn latest_round_data_at(
        &self,
        proxy: Address,
        _block: Option<evm_oracle_state::OracleBlockRef>,
    ) -> ProviderFuture<'_, RoundData> {
        assert_eq!(proxy, self.expected_proxy);
        self.latest_round_calls.lock().unwrap().push(proxy);
        Box::pin(async move { Ok(self.latest_round.clone()) })
    }

    fn aggregator(&self, proxy: Address) -> ProviderFuture<'_, Option<Address>> {
        self.aggregator_at(proxy, None)
    }

    fn aggregator_at(
        &self,
        proxy: Address,
        _block: Option<evm_oracle_state::OracleBlockRef>,
    ) -> ProviderFuture<'_, Option<Address>> {
        assert_eq!(proxy, self.expected_proxy);
        Box::pin(async move { Ok(Some(self.aggregator)) })
    }

    fn aggregator_type_and_version(
        &self,
        _aggregator: Address,
    ) -> Pin<
        Box<dyn Future<Output = Result<Option<String>, evm_oracle_state::OracleError>> + Send + '_>,
    > {
        Box::pin(async { Ok(None) })
    }

    fn aggregator_code_hash(
        &self,
        _aggregator: Address,
    ) -> Pin<
        Box<dyn Future<Output = Result<Option<B256>, evm_oracle_state::OracleError>> + Send + '_>,
    > {
        Box::pin(async { Ok(None) })
    }
}

#[allow(dead_code)]
fn _block_id_is_available_for_reconcile_signatures(block: BlockId) -> BlockId {
    block
}

#[allow(dead_code)]
fn _btree_map_is_available_for_future_source_graphs<K: Ord, V>(
    map: BTreeMap<K, V>,
) -> BTreeMap<K, V> {
    map
}
