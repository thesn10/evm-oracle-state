#![cfg(any(feature = "morpho", feature = "euler"))]

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
        ReactiveInputRecord, ReactiveRuntime,
    },
};
// This file compiles under morpho-only, euler-only, and combined feature
// builds (see the CI per-feature isolation matrix), so family-specific
// imports are gated on their family and the cross-family hook helper on both.
#[cfg(feature = "euler")]
use evm_oracle_state::EulerQuoteLeg;
use evm_oracle_state::{
    ANSWER_UPDATED_TOPIC, FeedId, FeedMetadata, FeedRegistration, FeedSource, OracleFeedStatus,
    OracleReactiveHandler, OracleReadOverlay, OracleReconciliationKind, OracleTracker, RoundData,
    StalenessPolicy,
};
#[cfg(feature = "morpho")]
use evm_oracle_state::{
    MorphoChainlinkFeed, MorphoChainlinkFeedRole, OracleDependency, OracleRoundStatus,
    OracleValueSource,
};
#[cfg(feature = "morpho")]
use evm_oracle_state::{OracleDerivedReader, OracleError, OracleHookEvent, OracleValueStatus};
#[cfg(all(feature = "morpho", feature = "euler"))]
use evm_oracle_state::{OraclePriceUpdate, OracleSignalKind};

sol! {
    interface MorphoOracleInterface {
        function price() external view returns (uint256);
    }

    interface EulerPriceOracleInterface {
        function getQuote(uint256 inAmount, address base, address quote) external view returns (uint256 outAmount);
    }
}

#[cfg(feature = "euler")]
use EulerPriceOracleInterface::getQuoteCall;
#[cfg(feature = "morpho")]
use MorphoOracleInterface::priceCall;

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

fn round(answer: i64) -> RoundData {
    RoundData {
        round_id: U256::ZERO,
        answer: I256::unchecked_from(answer),
        started_at: 1_700_000_000,
        updated_at: 1_700_000_000,
        answered_in_round: U256::ZERO,
    }
}

#[cfg(all(feature = "morpho", feature = "euler"))]
fn price_update_hooks(
    outcome: &evm_fork_cache::reactive::HandlerOutcome,
) -> Vec<&OraclePriceUpdate> {
    outcome
        .effects
        .iter()
        .filter_map(|effect| match effect {
            evm_fork_cache::reactive::ReactiveEffect::Hook(signal)
                if signal.kind.as_ref() == OracleSignalKind::PriceUpdate.as_str() =>
            {
                signal.payload.as_ref()?.downcast_ref::<OraclePriceUpdate>()
            }
            _ => None,
        })
        .collect()
}

fn metadata(decimals: u8, description: &str) -> FeedMetadata {
    FeedMetadata {
        decimals,
        description: description.to_string(),
        version: U256::ZERO,
    }
}

#[cfg(all(feature = "morpho", feature = "euler"))]
#[test]
fn shared_morpho_euler_dependency_uses_one_interest_and_fans_out_hooks() {
    let aggregator = Address::repeat_byte(0x12);
    let morpho_oracle = Address::repeat_byte(0x10);
    let morpho_feed = Address::repeat_byte(0x11);
    let euler_oracle = Address::repeat_byte(0x30);
    let base = Address::repeat_byte(0x31);
    let quote = Address::repeat_byte(0x32);
    let euler_feed = Address::repeat_byte(0x33);
    let registrations = vec![
        FeedRegistration {
            id: FeedId::new("morpho-weth-usdc"),
            proxy: morpho_oracle,
            label: Some("Morpho WETH/USDC".to_string()),
            base: Some("WETH".to_string()),
            quote: Some("USDC".to_string()),
            staleness: StalenessPolicy::max_age(3600),
            current_aggregator: Some(aggregator),
            aggregator_layout: None,
            metadata: metadata(36, "Morpho WETH/USDC"),
            source: FeedSource::morpho_chainlink_v2(
                morpho_oracle,
                I256::unchecked_from(1_i64),
                I256::unchecked_from(1_i64),
                I256::unchecked_from(1_i64),
                vec![MorphoChainlinkFeed::new(
                    MorphoChainlinkFeedRole::BaseFeed1,
                    OracleDependency::new(
                        morpho_feed,
                        aggregator,
                        I256::unchecked_from(200_000_000_i64),
                    ),
                )],
            ),
            status: OracleFeedStatus::Ready,
        },
        FeedRegistration {
            id: FeedId::new("euler-weth-usdc"),
            proxy: euler_oracle,
            label: Some("Euler WETH/USDC".to_string()),
            base: Some("WETH".to_string()),
            quote: Some("USDC".to_string()),
            staleness: StalenessPolicy::max_age(3600),
            current_aggregator: Some(aggregator),
            aggregator_layout: None,
            metadata: metadata(6, "Euler WETH/USDC"),
            source: FeedSource::euler_quote(
                euler_oracle,
                base,
                quote,
                18,
                6,
                EulerQuoteLeg::Chainlink {
                    oracle: euler_oracle,
                    feed: euler_feed,
                    aggregator,
                    answer: I256::unchecked_from(200_000_000_i64),
                    feed_decimals: 8,
                    inverse: false,
                    quote_decimals: 6,
                },
            ),
            status: OracleFeedStatus::Ready,
        },
    ];
    let handler = OracleReactiveHandler::new(registrations);
    assert_eq!(
        handler.interests().len(),
        1,
        "shared Morpho/Euler dependency should produce one AnswerUpdated interest"
    );

    let log = rpc_log(
        aggregator,
        vec![
            ANSWER_UPDATED_TOPIC,
            int_topic(I256::unchecked_from(300_000_000_i64)),
            uint_topic(U256::from(10_u64)),
        ],
        word(U256::from(1_700_000_020_u64)),
        121,
        3,
    );
    let outcome = handler
        .handle_for_test(&context(121, 3), &ReactiveInput::Log(log))
        .expect("handler outcome");
    let mut updates = price_update_hooks(&outcome);
    updates.sort_by_key(|update| update.id.as_str().to_string());

    assert_eq!(updates.len(), 2);
    assert_eq!(updates[0].id.as_str(), "euler-weth-usdc");
    assert_eq!(updates[0].raw_answer, I256::unchecked_from(3_000_000_i64));
    assert_eq!(updates[1].id.as_str(), "morpho-weth-usdc");
    assert_eq!(updates[1].raw_answer, I256::unchecked_from(300_000_000_i64));
}

#[cfg(feature = "morpho")]
#[tokio::test]
async fn morpho_chainlink_dependency_event_updates_price_and_queues_derived_reconcile() {
    let oracle = Address::repeat_byte(0x10);
    let base_feed = Address::repeat_byte(0x11);
    let base_aggregator = Address::repeat_byte(0x12);
    let quote_feed = Address::repeat_byte(0x21);
    let quote_aggregator = Address::repeat_byte(0x22);
    let registration = FeedRegistration {
        id: FeedId::new("morpho-weth-usdc"),
        proxy: oracle,
        label: Some("Morpho WETH/USDC".to_string()),
        base: Some("WETH".to_string()),
        quote: Some("USDC".to_string()),
        staleness: StalenessPolicy::max_age(3600),
        current_aggregator: Some(base_aggregator),
        aggregator_layout: None,
        metadata: metadata(36, "Morpho WETH/USDC"),
        source: FeedSource::morpho_chainlink_v2(
            oracle,
            I256::unchecked_from(1_i64),
            I256::unchecked_from(1_i64),
            I256::unchecked_from(1_i64),
            vec![
                MorphoChainlinkFeed::new(
                    MorphoChainlinkFeedRole::BaseFeed1,
                    OracleDependency::new(
                        base_feed,
                        base_aggregator,
                        I256::unchecked_from(200_i64),
                    ),
                ),
                MorphoChainlinkFeed::new(
                    MorphoChainlinkFeedRole::QuoteFeed1,
                    OracleDependency::new(
                        quote_feed,
                        quote_aggregator,
                        I256::unchecked_from(2_i64),
                    ),
                ),
            ],
        ),
        status: OracleFeedStatus::Ready,
    };
    let mut tracker = OracleTracker::from_registrations_at_timestamp(
        vec![(registration.clone(), round(100))],
        1_700_000_010,
    )
    .unwrap();
    let handler = OracleReactiveHandler::new(vec![registration]);
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(handler)).unwrap();
    let mut cache = setup_cache().await;

    let log = rpc_log(
        base_aggregator,
        vec![
            ANSWER_UPDATED_TOPIC,
            int_topic(I256::unchecked_from(300_i64)),
            uint_topic(U256::from(9_u64)),
        ],
        word(U256::from(1_700_000_020_u64)),
        120,
        7,
    );
    let report = runtime
        .ingest_batch(&mut cache, batch(ReactiveInput::Log(log), context(120, 7)))
        .unwrap();
    tracker.apply_batch_report(&report).unwrap();
    let price = tracker.price("morpho-weth-usdc").unwrap();
    assert_eq!(price.raw_answer, I256::unchecked_from(150_i64));
    assert_eq!(price.round_status, OracleRoundStatus::Fresh);
    assert_eq!(price.source, OracleValueSource::Derived);
    let pending = tracker.pending_reconciliations();
    assert_eq!(
        pending.len(),
        1,
        "derived events queue one protocol-read request"
    );
    assert_eq!(
        pending[0].kind,
        OracleReconciliationKind::DerivedProtocolRead
    );

    let overlay = OracleReadOverlay::new(&tracker);
    let returned = overlay
        .try_call(oracle, &priceCall {}.abi_encode())
        .unwrap()
        .unwrap();
    let overlay_price = priceCall::abi_decode_returns(&returned).unwrap();
    assert_eq!(overlay_price, U256::from(150_u64));
}

#[cfg(feature = "euler")]
#[tokio::test]
async fn euler_chainlink_event_updates_quote_and_overlay_get_quote() {
    let oracle = Address::repeat_byte(0x30);
    let base = Address::repeat_byte(0x31);
    let quote = Address::repeat_byte(0x32);
    let feed = Address::repeat_byte(0x33);
    let aggregator = Address::repeat_byte(0x34);
    let registration = FeedRegistration {
        id: FeedId::new("euler-weth-usdc"),
        proxy: oracle,
        label: Some("Euler WETH/USDC".to_string()),
        base: Some("WETH".to_string()),
        quote: Some("USDC".to_string()),
        staleness: StalenessPolicy::max_age(3600),
        current_aggregator: Some(aggregator),
        aggregator_layout: None,
        metadata: metadata(6, "Euler WETH/USDC"),
        source: FeedSource::euler_quote(
            oracle,
            base,
            quote,
            18,
            6,
            EulerQuoteLeg::Chainlink {
                oracle,
                feed,
                aggregator,
                answer: I256::unchecked_from(200_000_000_i64),
                feed_decimals: 8,
                inverse: false,
                quote_decimals: 6,
            },
        ),
        status: OracleFeedStatus::Ready,
    };
    let mut tracker = OracleTracker::from_registrations_at_timestamp(
        vec![(registration.clone(), round(2_000_000))],
        1_700_000_010,
    )
    .unwrap();
    let handler = OracleReactiveHandler::new(vec![registration]);
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(handler)).unwrap();
    let mut cache = setup_cache().await;

    let log = rpc_log(
        aggregator,
        vec![
            ANSWER_UPDATED_TOPIC,
            int_topic(I256::unchecked_from(300_000_000_i64)),
            uint_topic(U256::from(10_u64)),
        ],
        word(U256::from(1_700_000_020_u64)),
        121,
        3,
    );
    let report = runtime
        .ingest_batch(&mut cache, batch(ReactiveInput::Log(log), context(121, 3)))
        .unwrap();
    tracker.apply_batch_report(&report).unwrap();

    let price = tracker.price("euler-weth-usdc").unwrap();
    assert_eq!(price.raw_answer, I256::unchecked_from(3_000_000_i64));
    let pending = tracker.pending_reconciliations();
    assert_eq!(
        pending.len(),
        1,
        "derived events queue one protocol-read request"
    );
    assert_eq!(
        pending[0].kind,
        OracleReconciliationKind::DerivedProtocolRead
    );

    let overlay = OracleReadOverlay::new(&tracker);
    let calldata = getQuoteCall {
        inAmount: U256::from(1_000_000_000_000_000_000_u128),
        base,
        quote,
    }
    .abi_encode();
    let returned = overlay.try_call(oracle, &calldata).unwrap().unwrap();
    let amount = getQuoteCall::abi_decode_returns(&returned).unwrap();
    assert_eq!(amount, U256::from(3_000_000_u64));

    let inverse_calldata = getQuoteCall {
        inAmount: U256::from(3_000_000_u64),
        base: quote,
        quote: base,
    }
    .abi_encode();
    let inverse_returned = overlay
        .try_call(oracle, &inverse_calldata)
        .unwrap()
        .unwrap();
    let inverse_amount = getQuoteCall::abi_decode_returns(&inverse_returned).unwrap();
    assert_eq!(inverse_amount, U256::from(1_000_000_000_000_000_000_u128));
}

#[cfg(feature = "euler")]
#[tokio::test]
async fn euler_cross_dependency_event_recomputes_composed_quote() {
    let oracle = Address::repeat_byte(0x50);
    let base = Address::repeat_byte(0x51);
    let cross = Address::repeat_byte(0x52);
    let quote = Address::repeat_byte(0x53);
    let cross_quote_oracle = Address::repeat_byte(0x54);
    let cross_quote_feed = Address::repeat_byte(0x55);
    let cross_quote_aggregator = Address::repeat_byte(0x56);
    let registration = FeedRegistration {
        id: FeedId::new("euler-cross"),
        proxy: oracle,
        label: Some("Euler Cross".to_string()),
        base: Some("BASE".to_string()),
        quote: Some("QUOTE".to_string()),
        staleness: StalenessPolicy::max_age(3600),
        current_aggregator: Some(cross_quote_aggregator),
        aggregator_layout: None,
        metadata: metadata(6, "Euler Cross"),
        source: FeedSource::euler_cross(
            oracle,
            base,
            cross,
            quote,
            18,
            18,
            6,
            EulerQuoteLeg::FixedRate {
                oracle,
                rate: I256::unchecked_from(2_000_000_000_000_000_000_i128),
                inverse: false,
                base_decimals: 18,
                quote_decimals: 18,
            },
            EulerQuoteLeg::Chainlink {
                oracle: cross_quote_oracle,
                feed: cross_quote_feed,
                aggregator: cross_quote_aggregator,
                answer: I256::unchecked_from(100_000_000_000_i64),
                feed_decimals: 8,
                inverse: false,
                quote_decimals: 6,
            },
        ),
        status: OracleFeedStatus::Ready,
    };
    let mut tracker = OracleTracker::from_registrations_at_timestamp(
        vec![(registration.clone(), round(2_000_000_000))],
        1_700_000_010,
    )
    .unwrap();
    let handler = OracleReactiveHandler::new(vec![registration]);
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(handler)).unwrap();
    let mut cache = setup_cache().await;

    let log = rpc_log(
        cross_quote_aggregator,
        vec![
            ANSWER_UPDATED_TOPIC,
            int_topic(I256::unchecked_from(150_000_000_000_i64)),
            uint_topic(U256::from(11_u64)),
        ],
        word(U256::from(1_700_000_020_u64)),
        122,
        4,
    );
    let report = runtime
        .ingest_batch(&mut cache, batch(ReactiveInput::Log(log), context(122, 4)))
        .unwrap();
    tracker.apply_batch_report(&report).unwrap();

    let price = tracker.price("euler-cross").unwrap();
    assert_eq!(price.raw_answer, I256::unchecked_from(3_000_000_000_i64));
    let pending = tracker.pending_reconciliations();
    assert_eq!(
        pending.len(),
        1,
        "derived events queue one protocol-read request"
    );
    assert_eq!(
        pending[0].kind,
        OracleReconciliationKind::DerivedProtocolRead
    );
}

/// Two pairs resolved through one router must register under distinct
/// synthetic state keys (no `DuplicateProxy`), and the overlay must serve
/// `getQuote` for both pairs at the shared router address, dispatching on the
/// calldata base/quote — including the inverse direction and a mismatch error.
#[cfg(feature = "euler")]
#[test]
fn euler_router_multi_pair_registers_and_serves_via_router_address() {
    use evm_oracle_state::EulerQuotePair;

    let router = Address::repeat_byte(0x60);
    let weth = Address::repeat_byte(0x61);
    let usdc = Address::repeat_byte(0x62);
    let wbtc = Address::repeat_byte(0x63);
    let weth_oracle = Address::repeat_byte(0x64);
    let weth_feed = Address::repeat_byte(0x65);
    let weth_aggregator = Address::repeat_byte(0x66);
    let wbtc_oracle = Address::repeat_byte(0x67);
    let wbtc_feed = Address::repeat_byte(0x68);
    let wbtc_aggregator = Address::repeat_byte(0x69);

    let weth_key = EulerQuotePair::state_key_for(router, weth, usdc);
    let wbtc_key = EulerQuotePair::state_key_for(router, wbtc, usdc);
    assert_ne!(weth_key, wbtc_key);
    assert_ne!(weth_key, router);

    let registration = |key: Address,
                        id: &str,
                        base: Address,
                        leg_oracle: Address,
                        feed: Address,
                        aggregator: Address,
                        answer: i64| FeedRegistration {
        id: FeedId::new(id),
        proxy: key,
        label: Some(id.to_string()),
        base: Some("BASE".to_string()),
        quote: Some("USDC".to_string()),
        staleness: StalenessPolicy::max_age(3600),
        current_aggregator: Some(aggregator),
        aggregator_layout: None,
        metadata: metadata(6, id),
        source: FeedSource::euler_quote(
            // The user-facing source is the shared router; the leg keeps the
            // resolved per-pair adapter for event routing.
            router,
            base,
            usdc,
            18,
            6,
            EulerQuoteLeg::Chainlink {
                oracle: leg_oracle,
                feed,
                aggregator,
                answer: I256::unchecked_from(answer),
                feed_decimals: 8,
                inverse: false,
                quote_decimals: 6,
            },
        ),
        status: OracleFeedStatus::Ready,
    };

    let weth_registration = registration(
        weth_key,
        "euler-weth-usdc",
        weth,
        weth_oracle,
        weth_feed,
        weth_aggregator,
        200_000_000_000,
    );
    let wbtc_registration = registration(
        wbtc_key,
        "euler-wbtc-usdc",
        wbtc,
        wbtc_oracle,
        wbtc_feed,
        wbtc_aggregator,
        6_000_000_000_000,
    );

    // Registering both pairs must not trip the unique-proxy invariant.
    let tracker = OracleTracker::from_registrations_at_timestamp(
        vec![
            (weth_registration, round(2_000_000_000)),
            (wbtc_registration, round(60_000_000_000)),
        ],
        1_700_000_010,
    )
    .expect("two router pairs must register under distinct state keys");

    let overlay = OracleReadOverlay::new(&tracker);

    // Exact-direction quotes for both pairs, addressed to the router.
    let weth_quote = overlay
        .try_call(
            router,
            &getQuoteCall {
                inAmount: U256::from(1_000_000_000_000_000_000_u128),
                base: weth,
                quote: usdc,
            }
            .abi_encode(),
        )
        .unwrap()
        .expect("router-addressed WETH quote is served");
    assert_eq!(
        getQuoteCall::abi_decode_returns(&weth_quote).unwrap(),
        U256::from(2_000_000_000_u64)
    );

    let wbtc_quote = overlay
        .try_call(
            router,
            &getQuoteCall {
                inAmount: U256::from(1_000_000_000_000_000_000_u128),
                base: wbtc,
                quote: usdc,
            }
            .abi_encode(),
        )
        .unwrap()
        .expect("router-addressed WBTC quote is served");
    assert_eq!(
        getQuoteCall::abi_decode_returns(&wbtc_quote).unwrap(),
        U256::from(60_000_000_000_u64)
    );

    // Inverse direction routes to the matching pair.
    let inverse = overlay
        .try_call(
            router,
            &getQuoteCall {
                inAmount: U256::from(2_000_000_000_u64),
                base: usdc,
                quote: weth,
            }
            .abi_encode(),
        )
        .unwrap()
        .expect("inverse router-addressed quote is served");
    assert_eq!(
        getQuoteCall::abi_decode_returns(&inverse).unwrap(),
        U256::from(1_000_000_000_000_000_000_u128)
    );

    // A pair the router does not carry is a typed error, not a silent miss.
    let unknown = Address::repeat_byte(0x6f);
    assert!(
        overlay
            .try_call(
                router,
                &getQuoteCall {
                    inAmount: U256::from(1_u64),
                    base: unknown,
                    quote: usdc,
                }
                .abi_encode(),
            )
            .is_err()
    );

    // The synthetic state key still serves through the registry path.
    let by_key = overlay
        .try_call(
            weth_key,
            &getQuoteCall {
                inAmount: U256::from(1_000_000_000_000_000_000_u128),
                base: weth,
                quote: usdc,
            }
            .abi_encode(),
        )
        .unwrap()
        .expect("state-key-addressed quote is served");
    assert_eq!(
        getQuoteCall::abi_decode_returns(&by_key).unwrap(),
        U256::from(2_000_000_000_u64)
    );
}

#[cfg(feature = "euler")]
#[test]
fn euler_fixed_rate_source_has_no_chainlink_event_interest() {
    let oracle = Address::repeat_byte(0x40);
    let base = Address::repeat_byte(0x41);
    let quote = Address::repeat_byte(0x42);
    let registration = FeedRegistration {
        id: FeedId::new("euler-fixed"),
        proxy: oracle,
        label: Some("Euler Fixed".to_string()),
        base: Some("BASE".to_string()),
        quote: Some("QUOTE".to_string()),
        staleness: StalenessPolicy::max_age(3600),
        current_aggregator: None,
        aggregator_layout: None,
        metadata: metadata(6, "Euler Fixed"),
        source: FeedSource::euler_quote(
            oracle,
            base,
            quote,
            18,
            6,
            EulerQuoteLeg::FixedRate {
                oracle,
                rate: I256::unchecked_from(1_000_000_i64),
                inverse: false,
                base_decimals: 18,
                quote_decimals: 6,
            },
        ),
        status: OracleFeedStatus::Ready,
    };

    let handler = OracleReactiveHandler::new(vec![registration]);
    assert!(handler.interests().is_empty());
}

/// Regression: the Morpho `price()` recompute must match the contract's
/// 512-bit `mulDiv` semantics instead of saturating. With a large scale
/// factor and vault/feed magnitudes whose product exceeds `I256::MAX`, the
/// saturating implementation returned a silently clamped (wrong) price.
#[cfg(feature = "morpho")]
#[test]
fn morpho_recompute_uses_widened_muldiv_instead_of_saturating() {
    let oracle = Address::repeat_byte(0x10);
    let base_feed = Address::repeat_byte(0x11);
    let base_aggregator = Address::repeat_byte(0x12);
    let pow2 = |bits: usize| I256::from_raw(U256::from(1_u8) << bits);

    // scale_factor * base_vault * base_feed_answer = 2^200 * 2^60 * 2^60
    // = 2^320 > I256::MAX, but the true quotient 2^320 / 2^100 = 2^220 fits.
    let source = FeedSource::morpho_chainlink_v2(
        oracle,
        pow2(60),
        pow2(100),
        pow2(200),
        vec![MorphoChainlinkFeed::new(
            MorphoChainlinkFeedRole::BaseFeed1,
            OracleDependency::new(base_feed, base_aggregator, pow2(60)),
        )],
    );
    let recomputed = source.normalize_answer_from_event(Some(base_aggregator), pow2(60));
    assert_eq!(
        recomputed,
        pow2(220),
        "widened mulDiv must produce the exact quotient"
    );

    // A quotient that cannot fit int256 even after division degrades to zero
    // (the on-chain call would revert; zero is rejected as an invalid answer
    // downstream) instead of clamping to a plausible-looking price.
    let overflowing = FeedSource::morpho_chainlink_v2(
        oracle,
        pow2(120),
        I256::unchecked_from(1_i64),
        pow2(200),
        vec![MorphoChainlinkFeed::new(
            MorphoChainlinkFeedRole::BaseFeed1,
            OracleDependency::new(base_feed, base_aggregator, pow2(120)),
        )],
    );
    assert_eq!(
        overflowing.normalize_answer_from_event(Some(base_aggregator), pow2(120)),
        I256::ZERO,
        "unrepresentable quotients must degrade to zero, not saturate"
    );

    // The inner product mirrors the contract's checked uint256 math: a
    // base-side product that overflows uint256 (2^200 * 2^200) degrades to
    // zero as well.
    let inner_overflow = FeedSource::morpho_chainlink_v2(
        oracle,
        pow2(200),
        I256::unchecked_from(1_i64),
        I256::unchecked_from(1_i64),
        vec![MorphoChainlinkFeed::new(
            MorphoChainlinkFeedRole::BaseFeed1,
            OracleDependency::new(base_feed, base_aggregator, pow2(200)),
        )],
    );
    assert_eq!(
        inner_overflow.normalize_answer_from_event(Some(base_aggregator), pow2(200)),
        I256::ZERO,
        "inner uint256 product overflow must degrade to zero"
    );
}

// --- Derived-source reconciliation ------------------------------------------

#[cfg(feature = "morpho")]
/// Mock protocol reader driving `reconcile_derived_pending_with` offline.
struct MockDerivedReader {
    answer: Result<I256, ()>,
    reads: usize,
}

#[cfg(feature = "morpho")]
impl MockDerivedReader {
    fn returning(answer: i64) -> Self {
        Self {
            answer: Ok(I256::unchecked_from(answer)),
            reads: 0,
        }
    }

    fn failing() -> Self {
        Self {
            answer: Err(()),
            reads: 0,
        }
    }
}

#[cfg(feature = "morpho")]
impl OracleDerivedReader for MockDerivedReader {
    fn read_derived_value(
        &mut self,
        _registration: &FeedRegistration,
    ) -> Result<I256, OracleError> {
        self.reads += 1;
        self.answer
            .map_err(|()| OracleError::Provider("mock protocol read failed".to_string()))
    }
}

#[cfg(feature = "morpho")]
fn morpho_registration_for_reconcile(
    oracle: Address,
    base_feed: Address,
    base_aggregator: Address,
) -> FeedRegistration {
    FeedRegistration {
        id: FeedId::new("morpho-weth-usdc"),
        proxy: oracle,
        label: Some("Morpho WETH/USDC".to_string()),
        base: Some("WETH".to_string()),
        quote: Some("USDC".to_string()),
        staleness: StalenessPolicy::max_age(3600),
        current_aggregator: Some(base_aggregator),
        aggregator_layout: None,
        metadata: metadata(36, "Morpho WETH/USDC"),
        source: FeedSource::morpho_chainlink_v2(
            oracle,
            I256::unchecked_from(1_i64),
            I256::unchecked_from(1_i64),
            I256::unchecked_from(1_i64),
            vec![MorphoChainlinkFeed::new(
                MorphoChainlinkFeedRole::BaseFeed1,
                OracleDependency::new(base_feed, base_aggregator, I256::unchecked_from(200_i64)),
            )],
        ),
        status: OracleFeedStatus::Ready,
    }
}

#[cfg(feature = "morpho")]
fn morpho_tracker_with_pending_derived() -> (OracleTracker, Address) {
    let oracle = Address::repeat_byte(0x10);
    let base_feed = Address::repeat_byte(0x11);
    let base_aggregator = Address::repeat_byte(0x12);
    let registration = morpho_registration_for_reconcile(oracle, base_feed, base_aggregator);
    let mut tracker = OracleTracker::from_registrations_at_timestamp(
        vec![(registration.clone(), round(100))],
        1_700_000_010,
    )
    .unwrap();
    let handler = OracleReactiveHandler::new(vec![registration]);
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(handler)).unwrap();

    let log = rpc_log(
        base_aggregator,
        vec![
            ANSWER_UPDATED_TOPIC,
            int_topic(I256::unchecked_from(300_i64)),
            uint_topic(U256::from(9_u64)),
        ],
        word(U256::from(1_700_000_020_u64)),
        120,
        7,
    );
    let report = futures_executor(async {
        let mut cache = setup_cache().await;
        runtime
            .ingest_batch(&mut cache, batch(ReactiveInput::Log(log), context(120, 7)))
            .unwrap()
    });
    tracker.apply_batch_report(&report).unwrap();
    assert_eq!(tracker.pending_reconciliations().len(), 1);
    (tracker, oracle)
}

#[cfg(feature = "morpho")]
/// Tiny block_on so the pending-derived fixture can reuse the async cache
/// setup from synchronous tests.
fn futures_executor<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(future)
}

/// A protocol read equal to the event-recomputed value promotes the snapshot
/// to Confirmed/Proxy and drains the derived queue; the confirmed value then
/// passes the default liquidation policy without `allow_derived_source`.
#[cfg(feature = "morpho")]
#[test]
fn derived_reconcile_confirms_matching_protocol_read() {
    let (mut tracker, _oracle) = morpho_tracker_with_pending_derived();
    // With unit vaults/scale and one base feed, the dependency event answer
    // 300 recomputes price() = 300; a matching protocol read confirms it.
    let mut reader = MockDerivedReader::returning(300);

    let report = tracker.reconcile_derived_pending_with(&mut reader);

    assert_eq!(reader.reads, 1);
    assert_eq!(report.reconciled.len(), 1);
    assert!(report.failed.is_empty());
    assert!(
        report.reconciled[0]
            .hooks
            .iter()
            .any(|hook| matches!(hook, OracleHookEvent::PriceConfirmed(_))),
        "matching read must emit PriceConfirmed, got: {:?}",
        report.reconciled[0].hooks
    );
    assert!(tracker.pending_reconciliations().is_empty());

    let price = tracker.price("morpho-weth-usdc").unwrap();
    assert_eq!(price.raw_answer, I256::unchecked_from(300_i64));
    assert_eq!(price.value_status, OracleValueStatus::Confirmed);
    assert_eq!(price.source, OracleValueSource::Proxy);
    assert!(
        price
            .require(evm_oracle_state::PricePolicy::liquidation())
            .is_ok(),
        "confirmed derived value passes liquidation policy without opt-in"
    );
}

/// A differing protocol read installs the authoritative value as Corrected
/// with the read answer, and preserves the dependency aggregator routing.
#[cfg(feature = "morpho")]
#[test]
fn derived_reconcile_corrects_mismatched_protocol_read() {
    let (mut tracker, _oracle) = morpho_tracker_with_pending_derived();
    let mut reader = MockDerivedReader::returning(155);

    let report = tracker.reconcile_derived_pending_with(&mut reader);

    assert_eq!(report.reconciled.len(), 1);
    assert!(
        report.reconciled[0]
            .hooks
            .iter()
            .any(|hook| matches!(hook, OracleHookEvent::PriceCorrected(_))),
        "differing read must emit PriceCorrected"
    );
    assert!(tracker.pending_reconciliations().is_empty());

    let price = tracker.price("morpho-weth-usdc").unwrap();
    assert_eq!(price.raw_answer, I256::unchecked_from(155_i64));
    assert_eq!(price.value_status, OracleValueStatus::Corrected);
    assert_eq!(price.source, OracleValueSource::Proxy);
    let registration = tracker
        .registrations_iter()
        .find(|registration| registration.id.as_str() == "morpho-weth-usdc")
        .unwrap()
        .clone();
    assert_eq!(
        registration.current_aggregator,
        Some(Address::repeat_byte(0x12)),
        "dependency aggregator routing must survive derived reconciliation"
    );
}

/// A failed protocol read leaves the request queued and the snapshot pending.
#[cfg(feature = "morpho")]
#[test]
fn derived_reconcile_read_failure_leaves_request_queued() {
    let (mut tracker, _oracle) = morpho_tracker_with_pending_derived();
    let mut reader = MockDerivedReader::failing();

    let report = tracker.reconcile_derived_pending_with(&mut reader);

    assert!(report.reconciled.is_empty());
    assert_eq!(report.failed.len(), 1);
    assert!(matches!(report.failed[0].error, OracleError::Provider(_)));
    assert_eq!(
        tracker.pending_reconciliations().len(),
        1,
        "failed reads leave the request queued"
    );
    let price = tracker.price("morpho-weth-usdc").unwrap();
    assert_eq!(price.value_status, OracleValueStatus::EventPending);
}
