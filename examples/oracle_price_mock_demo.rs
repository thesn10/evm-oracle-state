//! Overlay-scoped oracle price mocks — fully offline, deterministic, zero setup.
//!
//! This demo exercises the "Overlay-Scoped Price Mocks" flow from the README:
//! build an offline `EvmCache` with a synthetic Chainlink OCR2 feed (detected
//! aggregator layout + warmed hot-vars slot), create `cache.mock_overlay()`,
//! and write a caller-chosen price into that simulation layer only via
//! [`OracleOverlayMockExt::mock_oracle_price`] / [`OraclePriceMock`].
//!
//! It walks through:
//!
//! 1. the offline cache + runtime setup where direct-storage mocks apply,
//! 2. an applied mock (`report.applied() == true`) and the exact slot writes,
//! 3. reading the mocked price back through the overlay's storage — the same
//!    slots a `latestRoundData()` simulation against the aggregator would read,
//! 4. the honest `applied() == false` path for a feed whose hot slot is cold,
//! 5. dropping the overlay and showing the base cache and typed runtime state
//!    were never touched.
//!
//! No env vars, no network, safe for CI:
//!
//! ```text
//! cargo run --example oracle_price_mock_demo
//! ```

use std::{error::Error, sync::Arc};

use evm_oracle_state::alloy_network::AnyNetwork;
use alloy_primitives::{Address, I256, U256};
use evm_oracle_state::alloy_provider::RootProvider;
use evm_oracle_state::alloy_rpc_client::RpcClient;
use evm_oracle_state::alloy_provider::mock::Asserter;
use evm_fork_cache::{StateUpdate, cache::EvmCache};
use evm_oracle_state::{
    AggregatorLayoutEvidence, ChainlinkOcr2StorageAdapter, FeedId, FeedMetadata, FeedRegistration,
    FeedSource, OracleFeedStatus, OracleOverlayMockExt, OraclePriceMock, OracleRuntime,
    OracleTracker, RoundData, StalenessPolicy,
};
use evm_oracle_state::revm::{database_interface::Database, state::AccountInfo};

const ETH_PROXY: Address = Address::repeat_byte(0x10);
const ETH_AGGREGATOR: Address = Address::repeat_byte(0x20);
const BTC_PROXY: Address = Address::repeat_byte(0x11);
const BTC_AGGREGATOR: Address = Address::repeat_byte(0x21);

const NOW_TIMESTAMP: u64 = 1_700_000_100;
const BASE_UPDATED_AT: u64 = 1_700_000_000;
const MOCK_UPDATED_AT: u64 = 1_700_000_200;

/// 2000.00000000 USD with 8 feed decimals.
const BASE_ANSWER: i64 = 200_000_000_000;
/// 1750.00000000 USD — the crash scenario we want to simulate.
const MOCK_ANSWER: i64 = 175_000_000_000;

/// Unrelated bits seeded into the packed OCR2 hot-vars slot. The mock's masked
/// write must preserve these (they model the config fields that share the slot
/// with `latestAggregatorRoundId` on a real aggregator).
const HOT_VARS_SEED: u64 = 0xaaaa;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("1. offline EvmCache with a synthetic OCR2 ETH/USD feed");
    // Mocked transport: the cache never talks to a network.
    let mut cache = mocked_cache().await;
    // Direct-storage mocks require the packed hot-vars slot to be warm in the
    // cache (a live runtime warms it during storage warmup / prewarm_storage).
    cache
        .db_mut()
        .insert_account_info(ETH_AGGREGATOR, AccountInfo::default());
    cache
        .insert_storage_slot(
            ETH_AGGREGATOR,
            ChainlinkOcr2StorageAdapter::hot_vars_slot(),
            U256::from(HOT_VARS_SEED),
        )
        .expect("seed hot-vars slot");
    println!(
        "   warmed s_hotVars slot {} on aggregator {ETH_AGGREGATOR:?} (seed bits {HOT_VARS_SEED:#x})",
        ChainlinkOcr2StorageAdapter::hot_vars_slot()
    );

    println!("\n2. oracle runtime with detected OCR2 layouts");
    // Proxy round id: phase 1, aggregator-local round 7. The mock defaults to
    // the next local round (8) unless one is set explicitly.
    let eth_round_id = (U256::from(1_u64) << 64) | U256::from(7_u64);
    let runtime = OracleRuntime::from_tracker(OracleTracker::from_registrations_at_timestamp(
        vec![
            (
                ocr2_registration("eth-usd", "ETH", ETH_PROXY, ETH_AGGREGATOR),
                round(eth_round_id, BASE_ANSWER, BASE_UPDATED_AT),
            ),
            (
                ocr2_registration("btc-usd", "BTC", BTC_PROXY, BTC_AGGREGATOR),
                round(U256::from(3_u64), 3_500_000_000_000, BASE_UPDATED_AT),
            ),
        ],
        NOW_TIMESTAMP,
    )?);
    let base_price = runtime.price("eth-usd")?;
    println!(
        "   eth-usd base price: answer={} decimals={} round={} value_status={:?} source={:?}",
        base_price.raw_answer,
        base_price.decimals,
        base_price.round_id,
        base_price.value_status,
        base_price.source
    );

    println!("\n3. overlay-scoped mock: crash ETH/USD to {MOCK_ANSWER} in the overlay only");
    let mut overlay = cache.mock_overlay();
    let report = overlay.mock_oracle_price(
        &cache,
        &runtime,
        "eth-usd",
        OraclePriceMock::new(I256::unchecked_from(MOCK_ANSWER)).updated_at(MOCK_UPDATED_AT),
    )?;
    println!(
        "   report: applied={} adapter={:?} effect_quality={:?}",
        report.applied(),
        report.adapter,
        report.effect_quality
    );
    for update in &report.updates {
        print_state_update(update);
    }
    assert!(report.applied());
    assert_eq!(report.adapter, Some("chainlink-ocr2"));
    assert_eq!(report.updates.len(), 2);

    println!("\n4. read the mocked price back through the overlay");
    // These are the exact slots a `latestRoundData()` simulation against the
    // OCR2 aggregator would read inside this overlay.
    let hot_vars = Database::storage(
        &mut overlay,
        ETH_AGGREGATOR,
        ChainlinkOcr2StorageAdapter::hot_vars_slot(),
    )
    .expect("overlay hot vars");
    let round_id_mask = ChainlinkOcr2StorageAdapter::latest_aggregator_round_id_mask();
    let mocked_round_id =
        ((hot_vars & round_id_mask) >> round_id_mask.trailing_zeros()).to::<u32>();
    println!("   s_hotVars.latestAggregatorRoundId = {mocked_round_id} (local round 7 -> 8)");
    println!(
        "   unrelated packed hot-vars bits preserved: {:#x}",
        hot_vars & !round_id_mask
    );

    let transmission_slot = ChainlinkOcr2StorageAdapter::transmission_slot(mocked_round_id);
    let transmission = Database::storage(&mut overlay, ETH_AGGREGATOR, transmission_slot)
        .expect("overlay transmission");
    let (answer, observations_ts, transmission_ts) = decode_ocr2_transmission(transmission);
    println!(
        "   s_transmissions[{mocked_round_id}]: answer={answer} observationsTimestamp={observations_ts} transmissionTimestamp={transmission_ts}"
    );
    assert_eq!(mocked_round_id, 8);
    assert_eq!(answer, I256::unchecked_from(MOCK_ANSWER));
    assert_eq!(observations_ts, MOCK_UPDATED_AT);
    assert_eq!(transmission_ts, MOCK_UPDATED_AT);
    assert_eq!(hot_vars & !round_id_mask, U256::from(HOT_VARS_SEED));
    assert_eq!(
        transmission,
        ChainlinkOcr2StorageAdapter::pack_transmission_from_event(
            I256::unchecked_from(MOCK_ANSWER),
            MOCK_UPDATED_AT,
        )?
    );

    println!("\n5. honest failure: btc-usd has no warmed hot slot, so nothing is written");
    let cold_report = overlay.mock_oracle_price(
        &cache,
        &runtime,
        "btc-usd",
        I256::unchecked_from(3_000_000_000_000_i64),
    )?;
    println!(
        "   report: applied={} adapter={:?} updates={} effect_quality={:?}",
        cold_report.applied(),
        cold_report.adapter,
        cold_report.updates.len(),
        cold_report.effect_quality
    );
    println!("   why: the OCR2 adapter refuses masked writes into a cold s_hotVars slot,");
    println!("   so no verified direct write exists and the overlay stays untouched.");
    println!("   (Feeds without a detected OCR layout report applied=false the same way;");
    println!("   warm slots first with oracle.prewarm_storage(&mut cache) on live runtimes.)");
    assert!(!cold_report.applied());
    assert!(cold_report.updates.is_empty());

    println!("\n6. drop the overlay — the base cache and typed state never saw the mock");
    drop(overlay);
    assert_eq!(
        cache.cached_storage_value(ETH_AGGREGATOR, transmission_slot),
        None,
        "mocked transmission slot must not leak into the base cache"
    );
    assert_eq!(
        cache.cached_storage_value(ETH_AGGREGATOR, ChainlinkOcr2StorageAdapter::hot_vars_slot()),
        Some(U256::from(HOT_VARS_SEED)),
        "base hot-vars slot must keep its pre-mock value"
    );
    let after = runtime.price("eth-usd")?;
    assert_eq!(after.raw_answer, I256::unchecked_from(BASE_ANSWER));
    println!(
        "   base cache transmission slot: {:?}",
        cache.cached_storage_value(ETH_AGGREGATOR, transmission_slot)
    );
    println!(
        "   base cache hot-vars slot:     {}",
        cache
            .cached_storage_value(ETH_AGGREGATOR, ChainlinkOcr2StorageAdapter::hot_vars_slot())
            .map(|value| format!("{value:#x} (pre-mock seed)"))
            .unwrap_or_else(|| "None".to_string())
    );
    println!(
        "   typed runtime price:          answer={} (unchanged)",
        after.raw_answer
    );

    println!("\nall assertions passed — example verified.");
    Ok(())
}

async fn mocked_cache() -> EvmCache {
    let client = RpcClient::mocked(Asserter::new());
    let provider = Arc::new(RootProvider::<AnyNetwork>::new(client));
    EvmCache::builder(provider).build().await
}

fn ocr2_registration(
    id: &str,
    base: &str,
    proxy: Address,
    aggregator: Address,
) -> FeedRegistration {
    FeedRegistration {
        id: FeedId::new(id),
        proxy,
        label: Some(format!("{base}/USD")),
        base: Some(base.to_string()),
        quote: Some("USD".to_string()),
        staleness: StalenessPolicy::max_age(3_600),
        current_aggregator: Some(aggregator),
        // Layout detection is what qualifies the feed for verified direct
        // storage writes (and therefore for overlay mocks).
        aggregator_layout: Some(AggregatorLayoutEvidence::from_type_and_version(
            aggregator,
            "AccessControlledOCR2Aggregator 1.0.0",
            None,
        )),
        metadata: FeedMetadata {
            decimals: 8,
            description: format!("{base} / USD"),
            version: U256::from(4_u64),
        },
        source: FeedSource::Chainlink,
        status: OracleFeedStatus::Ready,
    }
}

fn round(round_id: U256, answer: i64, updated_at: u64) -> RoundData {
    RoundData {
        round_id,
        answer: I256::unchecked_from(answer),
        started_at: updated_at,
        updated_at,
        answered_in_round: round_id,
    }
}

/// Decode a packed OCR2 transmission word: int192 answer plus two uint32
/// timestamps at byte offsets 24 and 28. The demo answer is positive, so no
/// int192 sign extension is needed here.
fn decode_ocr2_transmission(word: U256) -> (I256, u64, u64) {
    let answer_mask = (U256::from(1_u8) << 192_usize) - U256::from(1_u8);
    let answer = I256::from_raw(word & answer_mask);
    let observations_ts = ((word >> 192_usize) & U256::from(u32::MAX)).to::<u64>();
    let transmission_ts = (word >> 224_usize).to::<u64>();
    (answer, observations_ts, transmission_ts)
}

fn print_state_update(update: &StateUpdate) {
    match update {
        StateUpdate::Slot {
            address,
            slot,
            value,
        } => println!("   slot write   address={address:?} slot={slot} value={value:#x}"),
        StateUpdate::SlotMasked {
            address,
            slot,
            mask,
            value,
        } => println!(
            "   masked write address={address:?} slot={slot} mask={mask:#x} value={value:#x}"
        ),
        other => println!("   state update {other:?}"),
    }
}
