use std::sync::Arc;

use evm_oracle_state::alloy_network::AnyNetwork;
use alloy_primitives::{Address, I256, U256};
use evm_oracle_state::alloy_provider::RootProvider;
use evm_oracle_state::alloy_rpc_client::RpcClient;
use evm_oracle_state::alloy_provider::mock::Asserter;
use evm_fork_cache::cache::EvmCache;
use evm_oracle_state::{
    AggregatorLayoutEvidence, ChainlinkOcr2StorageAdapter, FeedId, FeedMetadata, FeedRegistration,
    FeedSource, OracleFeedStatus, OracleOverlayMockExt, OraclePriceMock, OracleRuntime,
    OracleTracker, RoundData, StalenessPolicy,
};
use evm_oracle_state::revm::database_interface::Database;
use evm_oracle_state::revm::state::AccountInfo;

async fn mocked_cache() -> EvmCache {
    let client = RpcClient::mocked(Asserter::new());
    let provider = Arc::new(RootProvider::<AnyNetwork>::new(client));
    EvmCache::builder(provider).build().await
}

fn metadata() -> FeedMetadata {
    FeedMetadata {
        decimals: 8,
        description: "ETH / USD".to_string(),
        version: U256::from(4_u64),
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

fn ocr2_registration(proxy: Address, aggregator: Address) -> FeedRegistration {
    FeedRegistration {
        id: FeedId::new("eth-usd"),
        proxy,
        label: Some("ETH/USD".to_string()),
        base: Some("ETH".to_string()),
        quote: Some("USD".to_string()),
        staleness: StalenessPolicy::max_age(3_600),
        current_aggregator: Some(aggregator),
        aggregator_layout: Some(AggregatorLayoutEvidence::from_type_and_version(
            aggregator,
            "AccessControlledOCR2Aggregator 1.0.0",
            None,
        )),
        metadata: metadata(),
        source: FeedSource::Chainlink,
        status: OracleFeedStatus::Ready,
    }
}

fn runtime_with_registration(
    registration: FeedRegistration,
    round: RoundData,
) -> OracleRuntime<()> {
    OracleRuntime::from_tracker(
        OracleTracker::from_registrations_at_timestamp(vec![(registration, round)], 1_700_000_100)
            .expect("tracker"),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn overlay_mock_oracle_price_writes_detected_ocr2_slots_without_mutating_cache() {
    let proxy = Address::repeat_byte(0x10);
    let aggregator = Address::repeat_byte(0x20);
    let registration = ocr2_registration(proxy, aggregator);
    let current_round = (U256::from(1_u64) << 64) | U256::from(7_u64);
    let runtime =
        runtime_with_registration(registration, round(current_round, 200_000_000_000, 100));

    let mut cache = mocked_cache().await;
    cache
        .db_mut()
        .insert_account_info(aggregator, AccountInfo::default());
    cache
        .insert_storage_slot(
            aggregator,
            ChainlinkOcr2StorageAdapter::hot_vars_slot(),
            U256::ZERO,
        )
        .expect("seed hot vars");

    let mut overlay = cache.mock_overlay();
    let report = overlay
        .mock_oracle_price(
            &cache,
            &runtime,
            "eth-usd",
            OraclePriceMock::new(I256::unchecked_from(175_000_000_000_i64)).updated_at(200),
        )
        .expect("mock price");

    assert!(report.applied());
    assert_eq!(report.adapter, Some("chainlink-ocr2"));
    assert_eq!(report.updates.len(), 2);

    let hot_vars = Database::storage(
        &mut overlay,
        aggregator,
        ChainlinkOcr2StorageAdapter::hot_vars_slot(),
    )
    .expect("overlay hot vars");
    assert_eq!(
        hot_vars & ChainlinkOcr2StorageAdapter::latest_aggregator_round_id_mask(),
        ChainlinkOcr2StorageAdapter::latest_aggregator_round_id_value(8)
    );

    let transmission_slot = ChainlinkOcr2StorageAdapter::transmission_slot(8);
    let transmission = Database::storage(&mut overlay, aggregator, transmission_slot)
        .expect("overlay transmission");
    assert_eq!(
        transmission,
        ChainlinkOcr2StorageAdapter::pack_transmission_from_event(
            I256::unchecked_from(175_000_000_000_i64),
            200,
        )
        .expect("packed transmission")
    );
    assert_eq!(
        cache.cached_storage_value(aggregator, transmission_slot),
        None,
        "overlay mock must not persist transmission storage into the cache"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn overlay_mock_oracle_price_reports_unapplied_when_direct_storage_is_cold() {
    let proxy = Address::repeat_byte(0x11);
    let aggregator = Address::repeat_byte(0x22);
    let registration = ocr2_registration(proxy, aggregator);
    let runtime = runtime_with_registration(registration, round(U256::from(7_u64), 2_000, 100));

    let mut cache = mocked_cache().await;
    let mut overlay = cache.mock_overlay();
    let report = overlay
        .mock_oracle_price(&cache, &runtime, "eth-usd", I256::unchecked_from(1_500_i64))
        .expect("mock price");

    assert!(!report.applied());
    assert!(report.updates.is_empty());
}
