//! Add a brand-new oracle family to `evm-oracle-state` from *outside* the crate
//! — no fork, no `src/` edit, no new enum variant.
//!
//! This is the runnable companion to the README's "Oracle Adapter Plugins"
//! section. It implements a toy `demo-fixed-oracle` family and drives it through
//! the exact surfaces a real consumer uses: install the adapter on
//! `OracleRuntime::cache_builder()`, let discovery seed typed state, feed a
//! synthetic on-chain event through the reactive runtime, and read the updated
//! price back under an explicit `PricePolicy`. It runs fully offline — no RPC,
//! no env vars, no live node:
//!
//! ```text
//! cargo run --example custom_oracle_adapter
//! ```
//!
//! ## The extension points, in the order this file uses them
//!
//! 1. [`OracleAdapterId`] — a stable identity for an oracle family the crate
//!    has never heard of.
//! 2. `FeedSource::custom(OracleSourceDescriptor::new(...))` — keeps the feed
//!    out of the built-in Chainlink handler while preserving the common typed
//!    price, overlay, and hook APIs.
//! 3. [`OracleAdapterPlugin::discover`] — seeds `FeedRegistration` +
//!    `RoundData` pairs. A real adapter reads its protocol contracts through
//!    `ctx.cache`; this demo seeds from local data so it stays offline.
//! 4. [`OracleAdapterPlugin::reactive_handler`] — an adapter-owned
//!    `evm-fork-cache` `ReactiveHandler` that decodes the family's events and
//!    emits standard `OraclePriceUpdate` hooks in `ORACLE_SIGNAL_NAMESPACE`.
//! 5. `OracleRuntime::apply_batch_report` — because the handler emits the
//!    standard hook payload, typed tracker state updates exactly like built-in
//!    Chainlink events, and `on_price_update` callbacks fire.

use std::{
    borrow::Cow,
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use evm_oracle_state::alloy_network::{AnyNetwork, Ethereum};
use alloy_primitives::{Address, B256, Bytes, I256, Log as PrimitiveLog, U256};
use evm_oracle_state::alloy_provider::RootProvider;
use evm_oracle_state::alloy_rpc_client::RpcClient;
use evm_oracle_state::alloy_rpc_types_eth::{Filter, Log as RpcLog};
use evm_oracle_state::alloy_provider::mock::Asserter;
use evm_fork_cache::{
    StateView,
    cache::EvmCache,
    reactive::{
        BlockRef, ChainStatus, HandlerError, HandlerId, HandlerOutcome, HookSignal, InputSource,
        LogInterest, ReactiveContext, ReactiveEffect, ReactiveHandler, ReactiveInput,
        ReactiveInputBatch, ReactiveInputRecord, ReactiveInterest, ReportTag, RouteKeySpec,
        StateEffectQuality,
    },
};
use evm_oracle_state::{
    AdapterFuture, AssetId, FeedId, FeedMetadata, FeedRegistration, FeedSource,
    ORACLE_SIGNAL_NAMESPACE, OracleAdapterId, OracleAdapterPlugin, OracleDiscoveredFeed,
    OracleDiscoveryContext, OracleDiscoveryReport, OracleFeedStatus, OraclePriceUpdate,
    OracleRoundStatus, OracleRuntime, OracleSignalKind, OracleSourceDescriptor, OracleStorageSync,
    OracleTransformDescriptor, OracleValueSource, OracleValueStatus, PricePolicy, RoundData,
    StalenessPolicy, TokenAmount,
};

/// Event signature topic for our invented `DemoPriceUpdated` event.
const DEMO_PRICE_TOPIC: B256 = B256::with_last_byte(0xd7);

const PROXY: Address = Address::repeat_byte(0x51);
const EVENT_SOURCE: Address = Address::repeat_byte(0x61);

const NOW_TIMESTAMP: u64 = 1_700_000_030;
const EVENT_BLOCK: u64 = 105;
/// 100.00000000 USD with 8 feed decimals, seeded at discovery.
const INITIAL_ANSWER: i64 = 10_000_000_000;
/// 99.50000000 USD, carried by the synthetic on-chain event.
const EVENT_ANSWER: u64 = 9_950_000_000;

// -----------------------------------------------------------------------------
// (1) + (2) + (3) The adapter: identity, custom feed source, offline discovery.
// -----------------------------------------------------------------------------

/// A from-scratch adapter for the invented `demo-fixed-oracle` family.
#[derive(Clone, Debug)]
struct DemoFixedOracleAdapter {
    id: OracleAdapterId,
    feed_id: FeedId,
    proxy: Address,
    event_source: Address,
    initial_answer: I256,
}

impl DemoFixedOracleAdapter {
    fn new(proxy: Address, event_source: Address, initial_answer: I256) -> Self {
        Self {
            id: OracleAdapterId::new("demo.fixed-oracle"),
            feed_id: FeedId::new("demo-usd"),
            proxy,
            event_source,
            initial_answer,
        }
    }

    fn registration(&self) -> FeedRegistration {
        // `FeedSource::custom` is the hatch that keeps this feed away from the
        // built-in Chainlink handler: the descriptor names our adapter, the
        // user-facing source address, the typed state key, and the contract
        // whose logs carry price updates.
        let source = OracleSourceDescriptor::new(
            self.id.clone(),
            Cow::Borrowed("demo-fixed-oracle"),
            self.proxy,
            self.proxy,
            self.event_source,
        )
        .transform(OracleTransformDescriptor::Identity);

        FeedRegistration {
            id: self.feed_id.clone(),
            proxy: self.proxy,
            label: Some("DEMO/USD".to_string()),
            base: Some("DEMO".to_string()),
            quote: Some("USD".to_string()),
            staleness: StalenessPolicy::max_age(3_600),
            current_aggregator: Some(self.event_source),
            // No Chainlink OCR layout: this family has its own storage shape.
            aggregator_layout: None,
            metadata: FeedMetadata {
                decimals: 8,
                description: "DEMO / USD".to_string(),
                version: U256::ZERO,
            },
            source: FeedSource::custom(source),
            status: OracleFeedStatus::Ready,
        }
    }
}

impl OracleAdapterPlugin for DemoFixedOracleAdapter {
    fn adapter_id(&self) -> OracleAdapterId {
        self.id.clone()
    }

    /// Discovery seeds one feed from local data. A production adapter performs
    /// its protocol reads through `ctx.cache` (the shared `EvmCache`) and can
    /// report per-feed skips instead of failing the whole build.
    fn discover<'a>(
        &'a self,
        _ctx: OracleDiscoveryContext<'a>,
    ) -> AdapterFuture<'a, OracleDiscoveryReport> {
        Box::pin(async move {
            Ok(
                OracleDiscoveryReport::new().with_feed(OracleDiscoveredFeed::new(
                    self.registration(),
                    RoundData {
                        round_id: U256::from(1_u64),
                        answer: self.initial_answer,
                        started_at: 1_700_000_000,
                        updated_at: 1_700_000_000,
                        answered_in_round: U256::from(1_u64),
                    },
                )),
            )
        })
    }

    /// The adapter owns event routing for its family: the runtime installs this
    /// handler next to the built-in Chainlink handler.
    fn reactive_handler(
        &self,
        registrations: Vec<FeedRegistration>,
        _storage_sync: OracleStorageSync,
    ) -> Arc<dyn ReactiveHandler<Ethereum>> {
        Arc::new(DemoFixedOracleHandler {
            event_source: self.event_source,
            registrations,
        })
    }
}

// -----------------------------------------------------------------------------
// (4) The reactive handler: decode the family's event, emit the standard hook.
// -----------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct DemoFixedOracleHandler {
    event_source: Address,
    registrations: Vec<FeedRegistration>,
}

impl ReactiveHandler<Ethereum> for DemoFixedOracleHandler {
    fn id(&self) -> HandlerId {
        HandlerId::new("demo.fixed-oracle")
    }

    /// Declare which logs this handler wants; the reactive engine routes only
    /// matching inputs here.
    fn interests(&self) -> Vec<ReactiveInterest<Ethereum>> {
        vec![ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new()
                .address(self.event_source)
                .event_signature(DEMO_PRICE_TOPIC),
            local_matcher: None,
            route_key: Some(RouteKeySpec::EmitterAddress),
        })]
    }

    fn handle(
        &self,
        ctx: &ReactiveContext,
        input: &ReactiveInput<Ethereum>,
        _state: &dyn StateView,
    ) -> Result<HandlerOutcome, HandlerError> {
        let ReactiveInput::Log(log) = input else {
            return Ok(HandlerOutcome::empty(StateEffectQuality::NoStateEffect));
        };
        if log.address() != self.event_source
            || log.topics().first().copied() != Some(DEMO_PRICE_TOPIC)
        {
            return Ok(HandlerOutcome::empty(StateEffectQuality::NoStateEffect));
        }

        // Decode the raw answer from the event payload. A real adapter decodes
        // its family's full event ABI (answer, round, timestamps) here.
        let answer = I256::unchecked_from(
            U256::try_from_be_slice(log.data().data.as_ref())
                .unwrap_or_default()
                .to::<u64>() as i128,
        );
        let updated_at = log
            .block_timestamp
            .or_else(|| ctx.block.as_ref().and_then(|block| block.timestamp))
            .unwrap_or_default();
        let block_number = log
            .block_number
            .or_else(|| ctx.block.as_ref().map(|block| block.number));
        let block_hash = log
            .block_hash
            .or_else(|| ctx.block.as_ref().map(|block| block.hash));
        let log_index = log.log_index.or(ctx.log_index);

        // Emitting the standard `OraclePriceUpdate` payload in the oracle
        // signal namespace is the whole integration contract:
        // `OracleRuntime::apply_batch_report` turns it into tracker state.
        let effects = self
            .registrations
            .iter()
            .map(|registration| {
                ReactiveEffect::Hook(HookSignal {
                    namespace: Cow::Borrowed(ORACLE_SIGNAL_NAMESPACE),
                    kind: Cow::Borrowed(OracleSignalKind::PriceUpdate.as_str()),
                    labels: vec![ReportTag::new("feed_id", registration.id.to_string())],
                    payload: Some(Arc::new(OraclePriceUpdate {
                        id: registration.id.clone(),
                        proxy: registration.proxy,
                        aggregator: self.event_source,
                        label: registration.label.clone(),
                        base: registration.base.clone(),
                        quote: registration.quote.clone(),
                        raw_answer: answer,
                        decimals: registration.metadata.decimals,
                        // A real adapter decodes the round from the event; the
                        // demo event carries only the answer.
                        event_round_id: U256::from(2_u64),
                        started_at: updated_at,
                        updated_at,
                        block_number,
                        block_hash,
                        log_index,
                        round_status: OracleRoundStatus::Fresh,
                        // Honest status: event-derived, not yet reconciled
                        // against an authoritative read.
                        value_status: OracleValueStatus::EventPending,
                        source: OracleValueSource::Event,
                    })),
                })
            })
            .collect();

        Ok(HandlerOutcome {
            effects,
            quality: StateEffectQuality::RequiresRepair,
            tags: Vec::new(),
        })
    }
}

// -----------------------------------------------------------------------------
// (5) Drive it end to end, exactly as a consumer would.
// -----------------------------------------------------------------------------

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    // Offline cache: mocked transport, no network.
    let mut cache = mocked_cache().await;
    let hook_count = Arc::new(AtomicUsize::new(0));
    let hook_count_for_callback = Arc::clone(&hook_count);

    println!("1. install the adapter and discover feeds");
    let adapter =
        DemoFixedOracleAdapter::new(PROXY, EVENT_SOURCE, I256::unchecked_from(INITIAL_ANSWER));
    let report = OracleRuntime::cache_builder()
        .install_adapter(adapter)
        .now_timestamp(NOW_TIMESTAMP)
        // Nothing to warm for this source; also keeps the demo strictly offline.
        .disable_storage_warmup()
        .on_price_update(move |update| {
            hook_count_for_callback.fetch_add(1, Ordering::SeqCst);
            println!(
                "   [on_price_update hook] feed={} answer={} round={}",
                update.id, update.raw_answer, update.event_round_id
            );
        })
        // `build_report` surfaces per-feed adapter skips without failing the
        // whole runtime; `build` is the stricter fail-fast variant.
        .build_report(&mut cache)
        .await?;
    let mut oracle = report.runtime;
    println!(
        "   discovery: skipped={} storage_warmup_empty={}",
        report.skipped.len(),
        report.storage_warmup.is_empty()
    );

    let seeded = oracle.price("demo-usd")?;
    println!(
        "   seeded typed price: {}/{} answer={} value_status={:?} source={:?}",
        seeded.base.as_deref().unwrap_or("?"),
        seeded.quote.as_deref().unwrap_or("?"),
        seeded.raw_answer,
        seeded.value_status,
        seeded.source
    );
    assert_eq!(seeded.raw_answer, I256::unchecked_from(INITIAL_ANSWER));

    println!("\n2. adapter handler installed next to the built-in Chainlink handler");
    let mut reactive = oracle.reactive_runtime()?;
    for handler_id in oracle.reactive_handler_ids() {
        println!("   handler: {handler_id:?}");
    }

    println!("\n3. ingest a synthetic DemoPriceUpdated event");
    let log = demo_price_log(EVENT_ANSWER, EVENT_BLOCK, 7);
    let batch_report = reactive
        .ingest_batch(
            &mut cache,
            ReactiveInputBatch::new(vec![ReactiveInputRecord::new(
                ReactiveInput::Log(log),
                context(EVENT_BLOCK, 7),
            )]),
        )
        .map_err(to_boxed_error)?;
    let events = oracle.apply_batch_report(&batch_report)?;
    println!(
        "   apply_batch_report produced {} oracle hook event(s)",
        events.events.len()
    );
    for event in &events.events {
        println!("   hook event kind={}", event.kind_str());
    }
    assert_eq!(events.events.len(), 1);
    assert_eq!(hook_count.load(Ordering::SeqCst), 1);

    println!("\n4. typed state updated through the standard oracle hook");
    let updated = oracle.price("demo-usd")?;
    println!(
        "   updated typed price: answer={} updated_at={} event_pending={} source={:?}",
        updated.raw_answer,
        updated.updated_at,
        updated.is_event_pending(),
        updated.source
    );
    assert_eq!(
        updated.raw_answer,
        I256::unchecked_from(EVENT_ANSWER as i64)
    );
    assert!(updated.is_event_pending());

    println!("\n5. read under an explicit PricePolicy (honest statuses)");
    // A strict liquidation policy rejects the event-pending value...
    let strict = updated.require(PricePolicy::liquidation());
    println!(
        "   strict liquidation policy: {}",
        strict
            .err()
            .map(|error| error.to_string())
            .unwrap_or_else(|| "accepted".to_string())
    );
    // ...while opting into event-pending values accepts it.
    let checked = updated.require(PricePolicy::liquidation().allow_event_pending())?;
    let holding = TokenAmount::new(
        AssetId::symbol("DEMO"),
        U256::from(3_000_000_000_000_000_000_u128), // 3 DEMO with 18 decimals
        18,
    );
    let value = checked.value_of(holding, 8)?;
    println!(
        "   3 DEMO valued at {} raw units ({} decimals, quote {})",
        value.raw(),
        value.decimals(),
        value.denomination()
    );
    assert_eq!(value.raw(), I256::unchecked_from(29_850_000_000_i64)); // 298.50 USD

    println!("\nall assertions passed — example verified.");
    Ok(())
}

async fn mocked_cache() -> EvmCache {
    let client = RpcClient::mocked(Asserter::new());
    let provider = Arc::new(RootProvider::<AnyNetwork>::new(client));
    EvmCache::builder(provider).build().await
}

fn demo_price_log(answer: u64, block_number: u64, log_index: u64) -> RpcLog {
    RpcLog {
        inner: PrimitiveLog::new_unchecked(
            EVENT_SOURCE,
            vec![DEMO_PRICE_TOPIC],
            Bytes::from(U256::from(answer).to_be_bytes::<32>().to_vec()),
        ),
        block_hash: Some(block_hash(block_number)),
        block_number: Some(block_number),
        block_timestamp: Some(1_700_000_000 + block_number),
        transaction_hash: Some(B256::repeat_byte(0x77)),
        transaction_index: Some(0),
        log_index: Some(log_index),
        removed: false,
    }
}

fn context(block_number: u64, log_index: u64) -> ReactiveContext {
    let block = BlockRef {
        number: block_number,
        hash: block_hash(block_number),
        parent_hash: Some(block_hash(block_number.saturating_sub(1))),
        timestamp: Some(1_700_000_000 + block_number),
    };
    ReactiveContext {
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

fn block_hash(block_number: u64) -> B256 {
    B256::repeat_byte(block_number as u8)
}

fn to_boxed_error(error: impl ToString) -> Box<dyn Error> {
    std::io::Error::other(error.to_string()).into()
}
