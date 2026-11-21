#![cfg(feature = "pending-oracle-updates")]

use std::{sync::Arc, time::SystemTime};

use evm_oracle_state::alloy_network::{AnyNetwork, Ethereum};
use alloy_primitives::{Address, B256, I256, U256};
use evm_oracle_state::alloy_provider::RootProvider;
use evm_oracle_state::alloy_rpc_client::RpcClient;
use evm_oracle_state::alloy_provider::mock::Asserter;
use evm_oracle_state::evm_fork_cache::cache::EvmCache;
use evm_oracle_state::evm_fork_cache::reactive::{
    EventSubscriber, ReactiveInputBatch, ReactiveInterest, SubscriberNextBatch,
};
use evm_oracle_state::{
    ChainlinkFeed, ChainlinkFeedProvider, ChainlinkPendingAdapter, FeedId, FeedMetadata,
    FeedRegistration, FeedSource, OracleAdapterId, OracleFeedStatus, OracleRuntime,
    PendingFeedFilter, PendingOracleAdapter, PendingOracleAdapterError,
    PendingOracleCandidateSource, PendingOracleConfig, PendingOracleEvent, PendingOracleEvidence,
    PendingOracleInterest, PendingOracleObserveOutcome, PendingOracleOrderingHandle,
    PendingOracleRecvError, PendingOracleResolution, PendingOracleResolutionKind,
    PendingOracleResolveOutcome, PendingOracleRoute, PendingOracleSimulationMaterial,
    PendingOracleSource, PendingOracleSourceDescriptor, PendingOracleSourceFuture,
    PendingOracleSourceId, PendingOracleSourceSink, PendingOracleSourceState, PendingOracleStatus,
    PendingOracleTransmission, PendingOracleTransmissionId, PendingOracleTransmissionStatus,
    PendingOracleUpdate, PendingOracleUpdateId, ProviderFuture, RoundData, StalenessPolicy,
};

#[derive(Debug)]
struct FixturePendingAdapter;

impl PendingOracleAdapter for FixturePendingAdapter {
    fn adapter_id(&self) -> OracleAdapterId {
        OracleAdapterId::new("fixture-pending")
    }

    fn interests(&self, registrations: &[FeedRegistration]) -> Vec<PendingOracleInterest> {
        vec![PendingOracleInterest::new(
            self.adapter_id(),
            registrations.iter().map(|registration| registration.proxy),
            [[0xde, 0xad, 0xbe, 0xef]],
        )]
    }

    fn decode(
        &self,
        candidate: &evm_oracle_state::PendingTransportCandidate,
        registrations: &[FeedRegistration],
    ) -> Result<Vec<PendingOracleUpdate>, PendingOracleAdapterError> {
        if !candidate.calldata.starts_with(&[0xde, 0xad, 0xbe, 0xef]) {
            return Ok(Vec::new());
        }
        Ok(vec![PendingOracleUpdate::new(
            PendingOracleUpdateId::from_hash(B256::repeat_byte(0xa1)),
            registrations
                .iter()
                .map(|registration| registration.id.clone()),
            PendingOracleEvidence::adapter(self.adapter_id(), "fixture", B256::repeat_byte(0xa2)),
        )])
    }
}

#[derive(Debug)]
struct FailingPendingAdapter;

impl PendingOracleAdapter for FailingPendingAdapter {
    fn adapter_id(&self) -> OracleAdapterId {
        OracleAdapterId::new("a-failing-pending")
    }

    fn interests(&self, registrations: &[FeedRegistration]) -> Vec<PendingOracleInterest> {
        vec![PendingOracleInterest::new(
            self.adapter_id(),
            registrations.iter().map(|registration| registration.proxy),
            [[0xde, 0xad, 0xbe, 0xef]],
        )]
    }

    fn decode(
        &self,
        _candidate: &evm_oracle_state::PendingTransportCandidate,
        _registrations: &[FeedRegistration],
    ) -> Result<Vec<PendingOracleUpdate>, PendingOracleAdapterError> {
        Err(PendingOracleAdapterError::new(
            self.adapter_id(),
            "fixture decode failure",
        ))
    }
}

#[derive(Clone, Debug)]
struct MockProvider {
    proxy: Address,
    aggregator: Address,
}

impl ChainlinkFeedProvider for MockProvider {
    fn decimals(&self, proxy: Address) -> ProviderFuture<'_, u8> {
        assert_eq!(proxy, self.proxy);
        Box::pin(async { Ok(8) })
    }

    fn description(&self, proxy: Address) -> ProviderFuture<'_, String> {
        assert_eq!(proxy, self.proxy);
        Box::pin(async { Ok("ETH / USD".to_string()) })
    }

    fn version(&self, proxy: Address) -> ProviderFuture<'_, U256> {
        assert_eq!(proxy, self.proxy);
        Box::pin(async { Ok(U256::from(4_u64)) })
    }

    fn latest_round_data(&self, proxy: Address) -> ProviderFuture<'_, RoundData> {
        assert_eq!(proxy, self.proxy);
        Box::pin(async {
            Ok(RoundData {
                round_id: U256::from(100_u64),
                answer: I256::unchecked_from(2_000_000_000_i64),
                started_at: 1_700_000_000,
                updated_at: 1_700_000_000,
                answered_in_round: U256::from(100_u64),
            })
        })
    }

    fn aggregator(&self, proxy: Address) -> ProviderFuture<'_, Option<Address>> {
        assert_eq!(proxy, self.proxy);
        let aggregator = self.aggregator;
        Box::pin(async move { Ok(Some(aggregator)) })
    }
}

#[tokio::test]
async fn pending_updates_inherit_feeds_from_the_existing_runtime_builder() {
    let proxy = Address::repeat_byte(0x10);
    let provider = MockProvider {
        proxy,
        aggregator: Address::repeat_byte(0x20),
    };

    let runtime = OracleRuntime::builder(provider)
        .now_timestamp(1_700_000_100)
        .feed(ChainlinkFeed::new(proxy).id("eth-usd"))
        .pending_updates(PendingOracleConfig::ethereum_mainnet())
        .build()
        .await
        .expect("runtime build");

    let pending = runtime
        .pending_updates()
        .expect("pending updates should be enabled");

    assert_eq!(pending.feed_ids(), &[FeedId::new("eth-usd")]);
    assert_eq!(runtime.price("eth-usd").unwrap().decimals, 8);
}

#[tokio::test]
async fn cache_builder_exposes_the_same_opt_in_pending_projection() {
    let provider = RootProvider::<AnyNetwork>::new(RpcClient::mocked(Asserter::new()));
    let mut cache = EvmCache::new(Arc::new(provider)).await;

    let runtime = OracleRuntime::cache_builder()
        .now_timestamp(1_700_000_100)
        .disable_storage_warmup()
        .pending_updates(PendingOracleConfig::ethereum_mainnet())
        .build(&mut cache)
        .await
        .expect("cache runtime build");

    assert!(runtime.pending_updates().is_some());
    assert!(runtime.pending_updates().unwrap().feed_ids().is_empty());
}

#[tokio::test]
async fn confirmed_subscriber_factory_does_not_register_an_unsupported_pending_interest() {
    let proxy = Address::repeat_byte(0x10);
    let provider = MockProvider {
        proxy,
        aggregator: Address::repeat_byte(0x20),
    };
    let runtime = OracleRuntime::builder(provider)
        .now_timestamp(1_700_000_100)
        .feed(ChainlinkFeed::new(proxy).id("eth-usd"))
        .pending_updates(PendingOracleConfig::ethereum_mainnet().public_mempool())
        .build()
        .await
        .expect("runtime build");
    let mut subscriber = RecordingSubscriber::default();

    runtime
        .register_subscriber(&mut subscriber)
        .await
        .expect("register all interests");

    assert!(
        subscriber
            .interests
            .iter()
            .all(|interest| !matches!(interest, ReactiveInterest::PendingTransactions(_)))
    );
}

#[tokio::test]
async fn a_pluggable_pending_source_reports_readiness_and_is_owned_by_the_runtime() {
    let proxy = Address::repeat_byte(0x10);
    let provider = MockProvider {
        proxy,
        aggregator: Address::repeat_byte(0x20),
    };
    let mut runtime = OracleRuntime::builder(provider)
        .now_timestamp(1_700_000_100)
        .feed(ChainlinkFeed::new(proxy).id("eth-usd"))
        .pending_updates(PendingOracleConfig::ethereum_mainnet().public_mempool())
        .build()
        .await
        .expect("runtime build");
    let mut updates = runtime.pending_updates().unwrap().subscribe();
    let source_id = PendingOracleSourceId::new("test-public-mempool");

    runtime
        .start_pending_source(ReadyThenStopSource {
            descriptor: PendingOracleSourceDescriptor::new(
                PendingOracleSource::PublicMempool,
                source_id.clone(),
            ),
        })
        .expect("start source");

    let mut states = Vec::new();
    while states.last() != Some(&PendingOracleSourceState::Stopped) {
        let streamed = updates.recv().await.expect("source lifecycle event");
        if let PendingOracleEvent::SourceHealthChanged(health) = streamed.event {
            states.push(health.state);
        }
    }

    assert_eq!(
        states,
        [
            PendingOracleSourceState::Connecting,
            PendingOracleSourceState::Ready,
            PendingOracleSourceState::Stopped,
        ]
    );
    let health = runtime
        .pending_updates()
        .unwrap()
        .source_health(&source_id)
        .expect("source health");
    assert_eq!(health.state, PendingOracleSourceState::Stopped);
    assert!(health.last_transport_message_at <= Some(SystemTime::now()));
}

#[tokio::test]
async fn custom_pending_adapter_inherits_registered_feeds_and_routes_candidates() {
    let proxy = Address::repeat_byte(0x10);
    let provider = MockProvider {
        proxy,
        aggregator: Address::repeat_byte(0x20),
    };
    let runtime = OracleRuntime::builder(provider)
        .now_timestamp(1_700_000_100)
        .feed(ChainlinkFeed::new(proxy).id("eth-usd"))
        .pending_updates(PendingOracleConfig::ethereum_mainnet().public_mempool())
        .pending_adapter(FixturePendingAdapter)
        .build()
        .await
        .expect("runtime build");
    let pending = runtime.pending_updates().expect("pending runtime");

    assert!(pending.interests().iter().any(|interest| {
        interest.adapter_id == OracleAdapterId::new("fixture-pending")
            && interest.addresses == [proxy].into_iter().collect()
            && interest.selectors == [[0xde, 0xad, 0xbe, 0xef]].into_iter().collect()
    }));

    let report = pending
        .observe_candidate(evm_oracle_state::PendingTransportCandidate::new(
            1,
            PendingOracleTransmissionId::from_hash(B256::repeat_byte(0x71)),
            PendingOracleSource::PublicMempool,
            PendingOracleSourceId::new("fixture-source"),
            proxy,
            [0xde, 0xad, 0xbe, 0xef].into(),
            PendingOracleOrderingHandle::None,
            Some(20_000_000),
        ))
        .expect("route candidate");

    assert_eq!(report.observations.len(), 1);
    assert_eq!(
        report.observations[0].adapter_id,
        OracleAdapterId::new("fixture-pending")
    );
    assert_eq!(
        report.observations[0].outcome,
        PendingOracleObserveOutcome::Observed
    );
    assert_eq!(pending.update_count(), 1);
}

#[tokio::test]
async fn pending_updates_accept_candidates_from_an_explicitly_configured_chain() {
    let proxy = Address::repeat_byte(0x10);
    let provider = MockProvider {
        proxy,
        aggregator: Address::repeat_byte(0x20),
    };
    let runtime = OracleRuntime::builder(provider)
        .now_timestamp(1_700_000_100)
        .feed(ChainlinkFeed::new(proxy).id("eth-usd"))
        .pending_updates(PendingOracleConfig::for_chain(11_155_111).public_mempool())
        .pending_adapter(FixturePendingAdapter)
        .build()
        .await
        .expect("runtime build");

    let report = runtime
        .pending_updates()
        .expect("pending runtime")
        .observe_candidate(evm_oracle_state::PendingTransportCandidate::new(
            11_155_111,
            PendingOracleTransmissionId::from_hash(B256::repeat_byte(0x72)),
            PendingOracleSource::PublicMempool,
            PendingOracleSourceId::new("sepolia-public-mempool"),
            proxy,
            [0xde, 0xad, 0xbe, 0xef].into(),
            PendingOracleOrderingHandle::None,
            Some(30_000_000),
        ))
        .expect("route candidate");

    assert_eq!(report.observations.len(), 1);
}

#[tokio::test]
async fn pending_updates_accept_a_caller_defined_transport_family() {
    let proxy = Address::repeat_byte(0x10);
    let provider = MockProvider {
        proxy,
        aggregator: Address::repeat_byte(0x20),
    };
    let source = PendingOracleSource::custom("base-flashblocks");
    let runtime = OracleRuntime::builder(provider)
        .now_timestamp(1_700_000_100)
        .feed(ChainlinkFeed::new(proxy).id("eth-usd"))
        .pending_updates(PendingOracleConfig::for_chain(8_453).source(source.clone()))
        .pending_adapter(FixturePendingAdapter)
        .build()
        .await
        .expect("runtime build");

    let report = runtime
        .pending_updates()
        .expect("pending runtime")
        .observe_candidate(evm_oracle_state::PendingTransportCandidate::new(
            8_453,
            PendingOracleTransmissionId::from_hash(B256::repeat_byte(0x73)),
            source.clone(),
            PendingOracleSourceId::new("base-flashblocks-primary"),
            proxy,
            [0xde, 0xad, 0xbe, 0xef].into(),
            PendingOracleOrderingHandle::SequencerPreconfirmation {
                tx_hash: B256::repeat_byte(0x74),
                block_number: 30_000_001,
                payload_id: B256::repeat_byte(0x75),
                sequence_index: 7,
            },
            Some(30_000_000),
        ))
        .expect("route candidate");

    assert_eq!(source.as_str(), "base-flashblocks");
    assert_eq!(report.observations.len(), 1);
}

#[tokio::test]
async fn one_adapter_failure_does_not_block_other_matching_adapters() {
    let proxy = Address::repeat_byte(0x10);
    let provider = MockProvider {
        proxy,
        aggregator: Address::repeat_byte(0x20),
    };
    let runtime = OracleRuntime::builder(provider)
        .now_timestamp(1_700_000_100)
        .feed(ChainlinkFeed::new(proxy).id("eth-usd"))
        .pending_updates(PendingOracleConfig::ethereum_mainnet().public_mempool())
        .pending_adapter(FailingPendingAdapter)
        .pending_adapter(FixturePendingAdapter)
        .build()
        .await
        .expect("runtime build");

    let report = runtime
        .pending_updates()
        .expect("pending runtime")
        .observe_candidate(evm_oracle_state::PendingTransportCandidate::new(
            1,
            PendingOracleTransmissionId::from_hash(B256::repeat_byte(0x76)),
            PendingOracleSource::PublicMempool,
            PendingOracleSourceId::new("fixture-source"),
            proxy,
            [0xde, 0xad, 0xbe, 0xef].into(),
            PendingOracleOrderingHandle::None,
            Some(20_000_000),
        ))
        .expect("route candidate");

    assert_eq!(report.observations.len(), 1);
    assert_eq!(report.failures.len(), 1);
    assert_eq!(
        report.failures[0].adapter_id,
        OracleAdapterId::new("a-failing-pending")
    );
    assert_eq!(
        report.observations[0].adapter_id,
        OracleAdapterId::new("fixture-pending")
    );
    assert_eq!(runtime.pending_updates().unwrap().update_count(), 1);
}

#[test]
fn chainlink_pending_adapter_does_not_mislabel_derived_feed_answers() {
    let aggregator = Address::repeat_byte(0x20);
    let mut registration = seeded_registration(
        "capped-eth-usd",
        Address::repeat_byte(0x10),
        aggregator,
        "Capped ETH / USD",
    );
    registration.source = FeedSource::AavePriceCapStable {
        source: Address::repeat_byte(0x30),
        underlying_proxy: Address::repeat_byte(0x31),
        price_cap: I256::unchecked_from(2_500_000_000_i64),
    };

    let interests = ChainlinkPendingAdapter.interests(&[registration]);

    assert!(interests.is_empty());
}

struct ReadyThenStopSource {
    descriptor: PendingOracleSourceDescriptor,
}

impl PendingOracleCandidateSource for ReadyThenStopSource {
    fn descriptor(&self) -> PendingOracleSourceDescriptor {
        self.descriptor.clone()
    }

    fn run(
        self: Box<Self>,
        sink: PendingOracleSourceSink,
        _shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> PendingOracleSourceFuture {
        Box::pin(async move {
            sink.ready();
            sink.transport_message();
            Ok(())
        })
    }
}

#[tokio::test]
async fn pending_updates_filter_the_existing_registry_by_feed_id() {
    let eth_proxy = Address::repeat_byte(0x10);
    let btc_proxy = Address::repeat_byte(0x11);
    let provider = MultiFeedProvider {
        aggregators: [
            (eth_proxy, Address::repeat_byte(0x20)),
            (btc_proxy, Address::repeat_byte(0x21)),
        ],
    };

    let runtime = OracleRuntime::builder(provider)
        .now_timestamp(1_700_000_100)
        .feed(ChainlinkFeed::new(eth_proxy).id("eth-usd"))
        .feed(ChainlinkFeed::new(btc_proxy).id("btc-usd"))
        .pending_updates(
            PendingOracleConfig::ethereum_mainnet()
                .registered_feeds(PendingFeedFilter::only(["btc-usd"])),
        )
        .build()
        .await
        .expect("runtime build");

    assert_eq!(
        runtime.pending_updates().unwrap().feed_ids(),
        &[FeedId::new("btc-usd")]
    );
    assert!(runtime.price("eth-usd").is_ok());
    assert!(runtime.price("btc-usd").is_ok());
}

#[tokio::test]
async fn pending_updates_follow_runtime_feed_removal_without_a_second_refresh() {
    let proxy = Address::repeat_byte(0x10);
    let provider = MockProvider {
        proxy,
        aggregator: Address::repeat_byte(0x20),
    };

    let mut runtime = OracleRuntime::builder(provider)
        .now_timestamp(1_700_000_100)
        .feed(ChainlinkFeed::new(proxy).id("eth-usd"))
        .pending_updates(PendingOracleConfig::ethereum_mainnet())
        .build()
        .await
        .expect("runtime build");

    runtime
        .unregister_feed_by_id(FeedId::new("eth-usd"))
        .expect("remove feed");

    assert!(runtime.pending_updates().unwrap().feed_ids().is_empty());
}

#[tokio::test]
async fn pending_updates_follow_runtime_feed_registration_without_duplicate_configuration() {
    let eth_proxy = Address::repeat_byte(0x10);
    let provider = MockProvider {
        proxy: eth_proxy,
        aggregator: Address::repeat_byte(0x20),
    };

    let mut runtime = OracleRuntime::builder(provider)
        .now_timestamp(1_700_000_100)
        .feed(ChainlinkFeed::new(eth_proxy).id("eth-usd"))
        .pending_updates(PendingOracleConfig::ethereum_mainnet())
        .build()
        .await
        .expect("runtime build");

    let btc_proxy = Address::repeat_byte(0x11);
    runtime
        .register_seeded_feed(
            seeded_registration(
                "btc-usd",
                btc_proxy,
                Address::repeat_byte(0x21),
                "BTC / USD",
            ),
            test_round(),
        )
        .expect("register seeded feed");

    assert_eq!(
        runtime.pending_updates().unwrap().feed_ids(),
        &[FeedId::new("btc-usd"), FeedId::new("eth-usd")]
    );
}

#[tokio::test]
async fn pending_runtime_clones_owned_by_sources_follow_later_feed_registration() {
    let eth_proxy = Address::repeat_byte(0x10);
    let provider = MockProvider {
        proxy: eth_proxy,
        aggregator: Address::repeat_byte(0x20),
    };
    let mut runtime = OracleRuntime::builder(provider)
        .now_timestamp(1_700_000_100)
        .feed(ChainlinkFeed::new(eth_proxy).id("eth-usd"))
        .pending_updates(PendingOracleConfig::ethereum_mainnet())
        .build()
        .await
        .expect("runtime build");
    let source_view = runtime.pending_updates().unwrap().clone();

    runtime
        .register_seeded_feed(
            seeded_registration(
                "btc-usd",
                Address::repeat_byte(0x11),
                Address::repeat_byte(0x21),
                "BTC / USD",
            ),
            test_round(),
        )
        .expect("register seeded feed");

    assert_eq!(
        source_view.feed_ids(),
        runtime.pending_updates().unwrap().feed_ids()
    );
    assert_eq!(
        source_view.feed_ids(),
        vec![FeedId::new("btc-usd"), FeedId::new("eth-usd")]
    );
}

#[tokio::test]
async fn pending_events_stream_without_mutating_canonical_oracle_state() {
    let proxy = Address::repeat_byte(0x10);
    let provider = MockProvider {
        proxy,
        aggregator: Address::repeat_byte(0x20),
    };

    let runtime = OracleRuntime::builder(provider)
        .now_timestamp(1_700_000_100)
        .feed(ChainlinkFeed::new(proxy).id("eth-usd"))
        .pending_updates(PendingOracleConfig::ethereum_mainnet())
        .build()
        .await
        .expect("runtime build");
    let confirmed_before = runtime.latest_round("eth-usd").unwrap();

    let pending = runtime.pending_updates().unwrap();
    let mut subscription = pending.subscribe();
    let update = PendingOracleUpdate::new(
        PendingOracleUpdateId::from_hash(B256::repeat_byte(0x42)),
        [FeedId::new("eth-usd")],
        PendingOracleEvidence::adapter(
            OracleAdapterId::new("test-pending"),
            "fixture",
            B256::repeat_byte(0x43),
        ),
    );

    assert_eq!(
        pending
            .publisher()
            .publish(PendingOracleEvent::Observed(update.clone())),
        1
    );

    let streamed = subscription.recv().await.expect("pending event");
    assert_eq!(streamed.sequence, 1);
    assert_eq!(streamed.event, PendingOracleEvent::Observed(update));
    assert_eq!(runtime.latest_round("eth-usd").unwrap(), confirmed_before);
}

#[tokio::test]
async fn pending_event_stream_reports_receiver_lag_explicitly() {
    let proxy = Address::repeat_byte(0x10);
    let provider = MockProvider {
        proxy,
        aggregator: Address::repeat_byte(0x20),
    };

    let runtime = OracleRuntime::builder(provider)
        .now_timestamp(1_700_000_100)
        .feed(ChainlinkFeed::new(proxy).id("eth-usd"))
        .pending_updates(PendingOracleConfig::ethereum_mainnet().channel_capacity(1))
        .build()
        .await
        .expect("runtime build");
    let pending = runtime.pending_updates().unwrap();
    let mut subscription = pending.subscribe();
    let publisher = pending.publisher();

    for byte in [0x41, 0x42, 0x43] {
        publisher.publish(PendingOracleEvent::Observed(test_pending_update(byte)));
    }

    assert_eq!(
        subscription.recv().await.unwrap_err(),
        PendingOracleRecvError::Lagged { missed: 2 }
    );
    assert_eq!(subscription.recv().await.unwrap().sequence, 3);
}

#[tokio::test]
async fn pending_runtime_deduplicates_identical_update_observations() {
    let proxy = Address::repeat_byte(0x10);
    let provider = MockProvider {
        proxy,
        aggregator: Address::repeat_byte(0x20),
    };
    let runtime = OracleRuntime::builder(provider)
        .now_timestamp(1_700_000_100)
        .feed(ChainlinkFeed::new(proxy).id("eth-usd"))
        .pending_updates(PendingOracleConfig::ethereum_mainnet())
        .build()
        .await
        .expect("runtime build");
    let pending = runtime.pending_updates().unwrap();
    let mut subscription = pending.subscribe();
    let update = test_pending_update(0x41);

    assert_eq!(
        pending.observe(update.clone()),
        PendingOracleObserveOutcome::Observed
    );
    assert_eq!(
        pending.observe(update.clone()),
        PendingOracleObserveOutcome::Duplicate
    );
    assert_eq!(pending.update(&update.id), Some(update.clone()));
    assert_eq!(pending.update_count(), 1);

    assert_eq!(
        subscription.recv().await.unwrap().event,
        PendingOracleEvent::Observed(update)
    );
}

#[tokio::test]
async fn transmission_replay_ignores_new_local_observation_metadata() {
    let proxy = Address::repeat_byte(0x10);
    let provider = MockProvider {
        proxy,
        aggregator: Address::repeat_byte(0x20),
    };
    let runtime = OracleRuntime::builder(provider)
        .now_timestamp(1_700_000_100)
        .feed(ChainlinkFeed::new(proxy).id("eth-usd"))
        .pending_updates(PendingOracleConfig::ethereum_mainnet())
        .build()
        .await
        .expect("runtime build");
    let pending = runtime.pending_updates().unwrap();
    let first = test_pending_transmission(0x51, "public-mempool");
    let mut replay = first.clone();
    replay.first_seen_at += std::time::Duration::from_secs(1);
    replay.observed_at_head = Some(replay.observed_at_head.unwrap_or_default() + 1);

    assert_eq!(
        pending.observe(test_pending_update(0x41).with_transmission(first)),
        PendingOracleObserveOutcome::Observed
    );
    assert_eq!(
        pending.observe(test_pending_update(0x41).with_transmission(replay)),
        PendingOracleObserveOutcome::Duplicate
    );
}

#[tokio::test]
async fn pending_runtime_merges_new_transmission_variants_for_one_update() {
    let proxy = Address::repeat_byte(0x10);
    let provider = MockProvider {
        proxy,
        aggregator: Address::repeat_byte(0x20),
    };
    let runtime = OracleRuntime::builder(provider)
        .now_timestamp(1_700_000_100)
        .feed(ChainlinkFeed::new(proxy).id("eth-usd"))
        .pending_updates(PendingOracleConfig::ethereum_mainnet())
        .build()
        .await
        .expect("runtime build");
    let pending = runtime.pending_updates().unwrap();
    let mut subscription = pending.subscribe();

    let first = test_pending_transmission(0x51, "public-mempool");
    let second = test_pending_transmission(0x52, "mev-share");
    let update = test_pending_update(0x41).with_transmission(first.clone());

    assert_eq!(
        pending.observe(update.clone()),
        PendingOracleObserveOutcome::Observed
    );
    assert_eq!(
        pending.observe(test_pending_update(0x41).with_transmission(second.clone())),
        PendingOracleObserveOutcome::TransmissionAdded { count: 1 }
    );
    assert_eq!(
        pending.observe(test_pending_update(0x41).with_transmission(second.clone())),
        PendingOracleObserveOutcome::Duplicate
    );

    let tracked = pending.update(&update.id).expect("tracked update");
    assert_eq!(tracked.transmissions, vec![first, second.clone()]);
    assert!(matches!(
        subscription.recv().await.unwrap().event,
        PendingOracleEvent::Observed(_)
    ));
    assert_eq!(
        subscription.recv().await.unwrap().event,
        PendingOracleEvent::TransmissionAdded {
            update_id: update.id,
            transmission: second,
        }
    );
}

#[tokio::test]
async fn unregistering_the_last_affected_feed_expires_a_pending_update() {
    let proxy = Address::repeat_byte(0x10);
    let provider = MockProvider {
        proxy,
        aggregator: Address::repeat_byte(0x20),
    };
    let mut runtime = OracleRuntime::builder(provider)
        .now_timestamp(1_700_000_100)
        .feed(ChainlinkFeed::new(proxy).id("eth-usd"))
        .pending_updates(PendingOracleConfig::ethereum_mainnet())
        .build()
        .await
        .expect("runtime build");
    let update = test_pending_update(0x41);
    let mut subscription = runtime.pending_updates().unwrap().subscribe();
    runtime.pending_updates().unwrap().observe(update.clone());
    assert!(matches!(
        subscription.recv().await.unwrap().event,
        PendingOracleEvent::Observed(_)
    ));

    runtime
        .unregister_feed_by_id(FeedId::new("eth-usd"))
        .expect("unregister feed");

    assert_eq!(
        subscription.recv().await.unwrap().event,
        PendingOracleEvent::Expired(update.id)
    );
    assert!(
        runtime
            .pending_updates()
            .unwrap()
            .update(&update.id)
            .is_none()
    );
}

#[tokio::test]
async fn matching_confirmation_resolves_only_the_pending_pipeline() {
    let proxy = Address::repeat_byte(0x10);
    let provider = MockProvider {
        proxy,
        aggregator: Address::repeat_byte(0x20),
    };
    let runtime = OracleRuntime::builder(provider)
        .now_timestamp(1_700_000_100)
        .feed(ChainlinkFeed::new(proxy).id("eth-usd"))
        .pending_updates(PendingOracleConfig::ethereum_mainnet())
        .build()
        .await
        .expect("runtime build");
    let confirmed_before = runtime.latest_round("eth-usd").unwrap();
    let transmission = test_pending_transmission(0x51, "public-mempool");
    let tx_hash = transmission.ordering.transaction_hash().unwrap();
    let update = test_pending_update(0x41).with_transmission(transmission.clone());
    let pending = runtime.pending_updates().unwrap();
    let mut subscription = pending.subscribe();
    pending.observe(update.clone());
    subscription.recv().await.unwrap();

    let resolution = PendingOracleResolution::new(
        update.id,
        transmission.id,
        tx_hash,
        20_000_001,
        PendingOracleResolutionKind::Landed,
    );
    assert_eq!(
        pending.resolve_confirmed_transmission(resolution.clone()),
        PendingOracleResolveOutcome::Resolved
    );
    assert_eq!(
        subscription.recv().await.unwrap().event,
        PendingOracleEvent::Resolved(resolution)
    );

    let tracked = pending.update(&update.id).unwrap();
    assert_eq!(tracked.status, PendingOracleStatus::Landed);
    assert_eq!(
        tracked.transmissions[0].status,
        PendingOracleTransmissionStatus::Landed
    );
    assert_eq!(runtime.latest_round("eth-usd").unwrap(), confirmed_before);
}

#[derive(Clone, Debug)]
struct MultiFeedProvider {
    aggregators: [(Address, Address); 2],
}

#[derive(Default)]
struct RecordingSubscriber {
    interests: Vec<ReactiveInterest<Ethereum>>,
}

impl EventSubscriber<Ethereum> for RecordingSubscriber {
    fn register_interests(
        &mut self,
        interests: &[ReactiveInterest<Ethereum>],
    ) -> evm_fork_cache::reactive::SubscriberOperation<'_, ()> {
        let interests = interests.to_vec();
        Box::pin(async move {
            self.interests = interests;
            Ok(())
        })
    }

    fn next_batch(&mut self) -> SubscriberNextBatch<'_, Ethereum> {
        Box::pin(async { Ok(None::<ReactiveInputBatch<Ethereum>>) })
    }
}

impl MultiFeedProvider {
    fn aggregator_for(&self, proxy: Address) -> Address {
        self.aggregators
            .iter()
            .find_map(|(candidate, aggregator)| (*candidate == proxy).then_some(*aggregator))
            .expect("registered test proxy")
    }
}

impl ChainlinkFeedProvider for MultiFeedProvider {
    fn decimals(&self, proxy: Address) -> ProviderFuture<'_, u8> {
        self.aggregator_for(proxy);
        Box::pin(async { Ok(8) })
    }

    fn description(&self, proxy: Address) -> ProviderFuture<'_, String> {
        self.aggregator_for(proxy);
        Box::pin(async { Ok("test / USD".to_string()) })
    }

    fn version(&self, proxy: Address) -> ProviderFuture<'_, U256> {
        self.aggregator_for(proxy);
        Box::pin(async { Ok(U256::from(4_u64)) })
    }

    fn latest_round_data(&self, proxy: Address) -> ProviderFuture<'_, RoundData> {
        self.aggregator_for(proxy);
        Box::pin(async {
            Ok(RoundData {
                round_id: U256::from(100_u64),
                answer: I256::unchecked_from(2_000_000_000_i64),
                started_at: 1_700_000_000,
                updated_at: 1_700_000_000,
                answered_in_round: U256::from(100_u64),
            })
        })
    }

    fn aggregator(&self, proxy: Address) -> ProviderFuture<'_, Option<Address>> {
        let aggregator = self.aggregator_for(proxy);
        Box::pin(async move { Ok(Some(aggregator)) })
    }
}

fn test_round() -> RoundData {
    RoundData {
        round_id: U256::from(100_u64),
        answer: I256::unchecked_from(2_000_000_000_i64),
        started_at: 1_700_000_000,
        updated_at: 1_700_000_000,
        answered_in_round: U256::from(100_u64),
    }
}

fn seeded_registration(
    id: &str,
    proxy: Address,
    aggregator: Address,
    description: &str,
) -> FeedRegistration {
    FeedRegistration {
        id: FeedId::new(id),
        proxy,
        label: Some(description.to_string()),
        base: None,
        quote: Some("USD".to_string()),
        staleness: StalenessPolicy::max_age(3_600),
        current_aggregator: Some(aggregator),
        aggregator_layout: None,
        metadata: FeedMetadata {
            decimals: 8,
            description: description.to_string(),
            version: U256::from(4_u64),
        },
        source: FeedSource::Chainlink,
        status: OracleFeedStatus::Ready,
    }
}

fn test_pending_update(byte: u8) -> PendingOracleUpdate {
    PendingOracleUpdate::new(
        PendingOracleUpdateId::from_hash(B256::repeat_byte(byte)),
        [FeedId::new("eth-usd")],
        PendingOracleEvidence::adapter(
            OracleAdapterId::new("test-pending"),
            "fixture",
            B256::repeat_byte(byte.wrapping_add(1)),
        ),
    )
}

fn test_pending_transmission(byte: u8, source: &str) -> PendingOracleTransmission {
    PendingOracleTransmission::new(
        PendingOracleTransmissionId::from_hash(B256::repeat_byte(byte)),
        PendingOracleSourceId::new(source),
        PendingOracleRoute::adapter(OracleAdapterId::new("test-pending"), "fixture"),
        PendingOracleOrderingHandle::TransactionHashOnly {
            tx_hash: B256::repeat_byte(byte.wrapping_add(1)),
        },
        PendingOracleSimulationMaterial::InsufficientMaterial,
        B256::repeat_byte(byte.wrapping_add(2)),
        Some(20_000_000),
    )
}
