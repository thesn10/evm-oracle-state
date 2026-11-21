#![cfg(feature = "pending-oracle-updates")]

use alloy_primitives::{
    Address, B256, Bytes, I256, Log as PrimitiveLog, U256, aliases::I192, aliases::U40, keccak256,
};
use evm_oracle_state::alloy_rpc_types_eth::Log as RpcLog;
use alloy_sol_types::SolEvent;
use evm_oracle_state::{
    CHAINLINK_SVR_FORWARD_SELECTOR, ChainlinkFeed, ChainlinkFeedProvider,
    OCR2_NEW_TRANSMISSION_TOPIC, OracleRuntime, PendingOracleCandidateOutcome, PendingOracleConfig,
    PendingOracleEvent, PendingOracleObserveOutcome, PendingOracleOrderingHandle,
    PendingOracleRoute, PendingOracleSimulationMaterial, PendingOracleSource,
    PendingOracleSourceId, PendingOracleStatus, PendingOracleTransmissionId,
    PendingOracleTransmissionStatus, PendingTransportCandidate, ProviderFuture, RoundData,
    decode_chainlink_ocr2_calldata,
};

alloy_sol_types::sol! {
    event NewTransmission(
        uint32 indexed aggregatorRoundId,
        int192 answer,
        address transmitter,
        uint32 observationsTimestamp,
        int192[] observations,
        bytes observers,
        int192 juelsPerFeeCoin,
        bytes32 configDigest,
        uint40 epochAndRound
    );
}

const TRANSMIT_SELECTOR: [u8; 4] = [0xb1, 0xdc, 0x65, 0xa4];
const TRANSMIT_SECONDARY_SELECTOR: [u8; 4] = [0xba, 0x0c, 0xb2, 0x9e];
const FORWARD_SELECTOR: [u8; 4] = [0x6f, 0xad, 0xcf, 0x72];

#[test]
fn decodes_direct_chainlink_ocr2_transmit() {
    let aggregator = Address::repeat_byte(0x11);
    let calldata = transmit_calldata(1_784_106_045, &test_observations(), TRANSMIT_SELECTOR);

    let decoded = decode_chainlink_ocr2_calldata(aggregator, &calldata)
        .expect("valid calldata")
        .expect("recognized transmit");

    assert_eq!(
        decoded.route,
        PendingOracleRoute::ChainlinkDirect {
            aggregator,
            secondary: false,
        }
    );
    assert_eq!(decoded.report.aggregator, aggregator);
    assert_eq!(decoded.report.observations_timestamp, 1_784_106_045);
    assert_eq!(decoded.report.observations.len(), 3);
    assert_eq!(
        decoded.report.median_answer,
        I256::unchecked_from(187_831_000_000_i64)
    );
    assert_eq!(
        decoded.report.minimum_answer,
        I256::unchecked_from(187_800_000_000_i64)
    );
    assert_eq!(
        decoded.report.maximum_answer,
        I256::unchecked_from(187_900_000_000_i64)
    );
}

#[test]
fn decodes_forwarded_chainlink_ocr2_transmit() {
    let aggregator = Address::repeat_byte(0x11);
    let forwarder = Address::repeat_byte(0x22);
    let inner = transmit_calldata(1_784_106_045, &test_observations(), TRANSMIT_SELECTOR);
    let calldata = forward_calldata(aggregator, &inner);

    let decoded = decode_chainlink_ocr2_calldata(forwarder, &calldata)
        .expect("valid calldata")
        .expect("recognized forward");

    assert_eq!(
        decoded.route,
        PendingOracleRoute::ChainlinkForwarded {
            forwarder,
            aggregator,
            secondary: false,
        }
    );
    assert_eq!(decoded.report.forwarder, Some(forwarder));
    assert_eq!(decoded.report.aggregator, aggregator);
}

#[test]
fn decodes_forwarded_chainlink_ocr2_secondary_transmit() {
    let aggregator = Address::repeat_byte(0x11);
    let forwarder = Address::repeat_byte(0x22);
    let inner = transmit_calldata(
        1_784_106_045,
        &test_observations(),
        TRANSMIT_SECONDARY_SELECTOR,
    );
    let calldata = forward_calldata(aggregator, &inner);

    let decoded = decode_chainlink_ocr2_calldata(forwarder, &calldata)
        .expect("valid calldata")
        .expect("recognized secondary forward");

    assert!(decoded.report.secondary);
    assert!(matches!(
        decoded.route,
        PendingOracleRoute::ChainlinkForwarded {
            secondary: true,
            ..
        }
    ));
}

#[test]
fn decodes_double_forwarded_atlas_svr_secondary_transmit() {
    let aggregator = Address::repeat_byte(0x11);
    let forwarder = Address::repeat_byte(0x22);
    let dapp_control = Address::repeat_byte(0x33);
    let inner_transmit = transmit_calldata(
        1_784_106_045,
        &test_observations(),
        TRANSMIT_SECONDARY_SELECTOR,
    );
    let inner_forward = forward_calldata(aggregator, &inner_transmit);
    let outer =
        forward_calldata_with_selector(CHAINLINK_SVR_FORWARD_SELECTOR, forwarder, &inner_forward);

    let decoded = decode_chainlink_ocr2_calldata(dapp_control, &outer)
        .expect("valid Atlas SVR calldata")
        .expect("recognized nested SVR forward");

    assert_eq!(
        decoded.route,
        PendingOracleRoute::ChainlinkForwarded {
            forwarder: dapp_control,
            aggregator,
            secondary: true,
        }
    );
    assert_eq!(decoded.report.forwarder, Some(dapp_control));
    assert_eq!(decoded.report.aggregator, aggregator);
    assert!(decoded.report.secondary);
    assert_eq!(
        decoded.report.median_answer,
        I256::unchecked_from(187_831_000_000_i64)
    );
    assert_eq!(decoded.report.calldata_hash, keccak256(&outer));
    assert_eq!(
        decoded.report.inner_calldata_hash,
        keccak256(&inner_transmit)
    );
}

#[test]
fn decodes_single_svr_forward_and_rejects_malformed_nested_forward() {
    let aggregator = Address::repeat_byte(0x11);
    let dapp_control = Address::repeat_byte(0x33);
    let inner_transmit = transmit_calldata(
        1_784_106_045,
        &test_observations(),
        TRANSMIT_SECONDARY_SELECTOR,
    );
    let single =
        forward_calldata_with_selector(CHAINLINK_SVR_FORWARD_SELECTOR, aggregator, &inner_transmit);

    let decoded = decode_chainlink_ocr2_calldata(dapp_control, &single)
        .expect("valid single SVR forward")
        .expect("recognized single SVR forward");
    assert_eq!(decoded.report.forwarder, Some(dapp_control));
    assert_eq!(decoded.report.aggregator, aggregator);
    assert!(decoded.report.secondary);

    let malformed_outer = CHAINLINK_SVR_FORWARD_SELECTOR.to_vec();
    assert!(decode_chainlink_ocr2_calldata(dapp_control, &malformed_outer).is_err());

    let malformed_inner = forward_calldata_with_selector(
        CHAINLINK_SVR_FORWARD_SELECTOR,
        aggregator,
        &FORWARD_SELECTOR,
    );
    assert!(decode_chainlink_ocr2_calldata(dapp_control, &malformed_inner).is_err());
}

#[tokio::test]
async fn routes_atlas_svr_double_forwarded_candidate_to_registered_feed() {
    let proxy = Address::repeat_byte(0x10);
    let aggregator = Address::repeat_byte(0x11);
    let forwarder = Address::repeat_byte(0x22);
    let dapp_control = Address::repeat_byte(0x33);
    let base_chain_id = 8453;
    let atlas_source = PendingOracleSource::custom("base-atlas-svr");
    let runtime = OracleRuntime::builder(DecoderProvider { proxy, aggregator })
        .now_timestamp(1_700_000_100)
        .feed(ChainlinkFeed::new(proxy).id("eth-usd"))
        .pending_updates(PendingOracleConfig::for_chain(base_chain_id).source(atlas_source.clone()))
        .build()
        .await
        .expect("runtime build");
    let pending = runtime.pending_updates().unwrap();
    let mut subscription = pending.subscribe();
    let tx_hash = B256::repeat_byte(0x99);
    let inner_transmit = transmit_calldata(
        1_784_106_045,
        &test_observations(),
        TRANSMIT_SECONDARY_SELECTOR,
    );
    let outer = forward_calldata_with_selector(
        CHAINLINK_SVR_FORWARD_SELECTOR,
        forwarder,
        &forward_calldata(aggregator, &inner_transmit),
    );

    let outcome = pending
        .observe_chainlink_candidate(PendingTransportCandidate::new(
            base_chain_id,
            PendingOracleTransmissionId::from_hash(tx_hash),
            atlas_source,
            PendingOracleSourceId::new("atlas-auctioneer"),
            dapp_control,
            Bytes::from(outer),
            PendingOracleOrderingHandle::TransactionHashOnly { tx_hash },
            Some(57_579_532),
        ))
        .expect("decode Atlas SVR pending candidate");

    assert_eq!(
        outcome,
        PendingOracleCandidateOutcome::Tracked(PendingOracleObserveOutcome::Observed)
    );
    let PendingOracleEvent::Observed(update) = subscription.recv().await.unwrap().event else {
        panic!("expected observed update");
    };
    assert_eq!(update.feed_ids, [evm_oracle_state::FeedId::new("eth-usd")]);
    assert_eq!(
        update.proposed_value.unwrap().raw_answer,
        I256::unchecked_from(187_831_000_000_i64)
    );
    assert_eq!(
        update.transmissions[0].route,
        PendingOracleRoute::ChainlinkForwarded {
            forwarder: dapp_control,
            aggregator,
            secondary: true,
        }
    );
}

#[test]
fn ignores_unrecognized_calldata_and_rejects_malformed_chainlink_payloads() {
    assert!(
        decode_chainlink_ocr2_calldata(Address::ZERO, &[0xde, 0xad, 0xbe, 0xef])
            .expect("unknown selector is not an error")
            .is_none()
    );

    let malformed = TRANSMIT_SELECTOR.to_vec();
    assert!(decode_chainlink_ocr2_calldata(Address::ZERO, &malformed).is_err());

    let implausible = transmit_calldata(32, &[1, 2, 3], TRANSMIT_SELECTOR);
    assert!(decode_chainlink_ocr2_calldata(Address::ZERO, &implausible).is_err());
}

#[tokio::test]
async fn routes_a_decoded_report_to_the_existing_registered_feed() {
    let proxy = Address::repeat_byte(0x10);
    let aggregator = Address::repeat_byte(0x11);
    let runtime = OracleRuntime::builder(DecoderProvider { proxy, aggregator })
        .now_timestamp(1_700_000_100)
        .feed(ChainlinkFeed::new(proxy).id("eth-usd"))
        .pending_updates(PendingOracleConfig::ethereum_mainnet().public_mempool())
        .build()
        .await
        .expect("runtime build");
    let pending = runtime.pending_updates().unwrap();
    let mut subscription = pending.subscribe();
    let tx_hash = B256::repeat_byte(0x77);
    let calldata = transmit_calldata(1_784_106_045, &test_observations(), TRANSMIT_SELECTOR);

    let outcome = pending
        .observe_chainlink_candidate(PendingTransportCandidate::new(
            1,
            PendingOracleTransmissionId::from_hash(tx_hash),
            PendingOracleSource::PublicMempool,
            PendingOracleSourceId::new("alchemy-pending"),
            aggregator,
            Bytes::from(calldata),
            PendingOracleOrderingHandle::TransactionHashOnly { tx_hash },
            Some(25_537_353),
        ))
        .expect("decode pending candidate");

    assert_eq!(
        outcome,
        PendingOracleCandidateOutcome::Tracked(PendingOracleObserveOutcome::Observed)
    );
    let event = subscription.recv().await.unwrap().event;
    let PendingOracleEvent::Observed(update) = event else {
        panic!("expected observed update");
    };
    assert_eq!(update.feed_ids, [evm_oracle_state::FeedId::new("eth-usd")]);
    assert_eq!(
        update.proposed_value.unwrap().raw_answer,
        I256::unchecked_from(187_831_000_000_i64)
    );
    assert_eq!(
        update.transmissions[0].ordering.transaction_hash(),
        Some(tx_hash)
    );
}

#[tokio::test]
async fn ethereum_pending_runtime_fails_closed_on_an_l2_candidate() {
    let proxy = Address::repeat_byte(0x10);
    let aggregator = Address::repeat_byte(0x11);
    let runtime = OracleRuntime::builder(DecoderProvider { proxy, aggregator })
        .now_timestamp(1_700_000_100)
        .feed(ChainlinkFeed::new(proxy).id("eth-usd"))
        .pending_updates(PendingOracleConfig::ethereum_mainnet().public_mempool())
        .build()
        .await
        .expect("runtime build");

    let outcome = runtime
        .pending_updates()
        .unwrap()
        .observe_chainlink_candidate(PendingTransportCandidate::new(
            10,
            PendingOracleTransmissionId::from_hash(B256::repeat_byte(0x77)),
            PendingOracleSource::PublicMempool,
            PendingOracleSourceId::new("optimism-pending"),
            aggregator,
            Bytes::new(),
            PendingOracleOrderingHandle::None,
            Some(1),
        ))
        .expect("chain gate precedes decoding");

    assert_eq!(
        outcome,
        PendingOracleCandidateOutcome::WrongChain {
            expected: 1,
            observed: 10,
        }
    );
}

#[tokio::test]
async fn matching_committed_chainlink_log_resolves_the_same_pending_transaction() {
    let proxy = Address::repeat_byte(0x10);
    let aggregator = Address::repeat_byte(0x11);
    let runtime = OracleRuntime::builder(DecoderProvider { proxy, aggregator })
        .now_timestamp(1_700_000_100)
        .feed(ChainlinkFeed::new(proxy).id("eth-usd"))
        .pending_updates(PendingOracleConfig::ethereum_mainnet().public_mempool())
        .build()
        .await
        .expect("runtime build");
    let pending = runtime.pending_updates().unwrap();
    let mut subscription = pending.subscribe();
    let tx_hash = B256::repeat_byte(0x77);
    let calldata = transmit_calldata(1_784_106_045, &test_observations(), TRANSMIT_SELECTOR);
    pending
        .observe_chainlink_candidate(PendingTransportCandidate::new(
            1,
            PendingOracleTransmissionId::from_hash(tx_hash),
            PendingOracleSource::PublicMempool,
            PendingOracleSourceId::new("alchemy-pending"),
            aggregator,
            Bytes::from(calldata),
            PendingOracleOrderingHandle::TransactionHashOnly { tx_hash },
            Some(25_537_353),
        ))
        .expect("pending candidate");
    let update = match subscription.recv().await.unwrap().event {
        PendingOracleEvent::Observed(update) => update,
        event => panic!("expected pending observation, got {event:?}"),
    };

    let event = NewTransmission {
        aggregatorRoundId: 101,
        answer: I192::try_from(187_831_000_000_i64).expect("answer fits"),
        transmitter: Address::repeat_byte(0x33),
        observationsTimestamp: 1_784_106_045,
        observations: vec![I192::try_from(187_831_000_000_i64).expect("answer fits")],
        observers: Bytes::from(vec![0x01]),
        juelsPerFeeCoin: I192::ZERO,
        configDigest: B256::repeat_byte(0x44),
        epochAndRound: U40::from(0x1a_89_04_u64),
    };
    let log = RpcLog {
        inner: PrimitiveLog::new_unchecked(
            aggregator,
            vec![
                OCR2_NEW_TRANSMISSION_TOPIC,
                B256::from(U256::from(101_u64).to_be_bytes::<32>()),
            ],
            Bytes::from(event.encode_data()),
        ),
        block_hash: Some(B256::repeat_byte(0x88)),
        block_number: Some(25_537_354),
        block_timestamp: Some(1_784_106_050),
        transaction_hash: Some(tx_hash),
        transaction_index: Some(2),
        log_index: Some(4),
        removed: false,
    };

    let resolutions = pending.observe_confirmed_log(&log);
    assert_eq!(resolutions.len(), 1);
    assert_eq!(resolutions[0].update_id, update.id);
    let tracked = pending.update(&update.id).unwrap();
    assert_eq!(tracked.status, PendingOracleStatus::Landed);
    assert_eq!(
        tracked.transmissions[0].status,
        PendingOracleTransmissionStatus::Landed
    );
    assert!(matches!(
        subscription.recv().await.unwrap().event,
        PendingOracleEvent::Resolved(_)
    ));
}

#[tokio::test]
async fn unverified_signed_bytes_never_claim_exact_simulation_fidelity() {
    let proxy = Address::repeat_byte(0x10);
    let aggregator = Address::repeat_byte(0x11);
    let runtime = OracleRuntime::builder(DecoderProvider { proxy, aggregator })
        .now_timestamp(1_700_000_100)
        .feed(ChainlinkFeed::new(proxy).id("eth-usd"))
        .pending_updates(PendingOracleConfig::ethereum_mainnet().public_mempool())
        .build()
        .await
        .expect("runtime build");
    let pending = runtime.pending_updates().unwrap();
    let mut subscription = pending.subscribe();
    let tx_hash = B256::repeat_byte(0x77);

    pending
        .observe_chainlink_candidate(PendingTransportCandidate::new(
            1,
            PendingOracleTransmissionId::from_hash(tx_hash),
            PendingOracleSource::PublicMempool,
            PendingOracleSourceId::new("untrusted-source"),
            aggregator,
            transmit_calldata(1_784_106_045, &test_observations(), TRANSMIT_SELECTOR).into(),
            PendingOracleOrderingHandle::RawEthereumTransaction {
                tx_hash,
                signed_envelope: Bytes::from_static(b"not-the-hashed-transaction"),
            },
            Some(25_537_353),
        ))
        .expect("candidate routing");

    let PendingOracleEvent::Observed(update) = subscription.recv().await.unwrap().event else {
        panic!("expected pending observation");
    };
    assert_eq!(
        update.transmissions[0].simulation,
        PendingOracleSimulationMaterial::AnswerOnlyProjection
    );
}

#[derive(Clone, Debug)]
struct DecoderProvider {
    proxy: Address,
    aggregator: Address,
}

impl ChainlinkFeedProvider for DecoderProvider {
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
                answer: I256::unchecked_from(187_700_000_000_i64),
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

fn test_observations() -> [i64; 3] {
    [187_800_000_000, 187_831_000_000, 187_900_000_000]
}

fn word(value: u64) -> [u8; 32] {
    let mut encoded = [0_u8; 32];
    encoded[24..].copy_from_slice(&value.to_be_bytes());
    encoded
}

fn signed_word(value: i64) -> [u8; 32] {
    let mut encoded = if value < 0 { [0xff_u8; 32] } else { [0_u8; 32] };
    encoded[24..].copy_from_slice(&value.to_be_bytes());
    encoded
}

fn dynamic_bytes(value: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::from(word(value.len() as u64));
    encoded.extend_from_slice(value);
    encoded.resize(encoded.len().next_multiple_of(32), 0);
    encoded
}

fn report(timestamp: u64, observations: &[i64]) -> Vec<u8> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&word(timestamp));
    encoded.extend_from_slice(&word(0));
    encoded.extend_from_slice(&word(4 * 32));
    encoded.extend_from_slice(&word(0));
    encoded.extend_from_slice(&word(observations.len() as u64));
    for observation in observations {
        encoded.extend_from_slice(&signed_word(*observation));
    }
    encoded
}

fn transmit_calldata(timestamp: u64, observations: &[i64], selector: [u8; 4]) -> Vec<u8> {
    let encoded_report = dynamic_bytes(&report(timestamp, observations));
    let head_size = 7_usize * 32;
    let mut calldata = Vec::from(selector);
    calldata.extend_from_slice(&word(1));
    calldata.extend_from_slice(&word(2));
    calldata.extend_from_slice(&word(3));
    calldata.extend_from_slice(&word(head_size as u64));
    calldata.extend_from_slice(&word((head_size + encoded_report.len()) as u64));
    calldata.extend_from_slice(&word((head_size + encoded_report.len() + 32) as u64));
    calldata.extend_from_slice(&word(0));
    calldata.extend_from_slice(&encoded_report);
    calldata.extend_from_slice(&word(0));
    calldata.extend_from_slice(&word(0));
    calldata
}

fn forward_calldata(aggregator: Address, inner: &[u8]) -> Vec<u8> {
    forward_calldata_with_selector(FORWARD_SELECTOR, aggregator, inner)
}

fn forward_calldata_with_selector(
    selector: [u8; 4],
    target_addr: Address,
    inner: &[u8],
) -> Vec<u8> {
    let mut calldata = Vec::from(selector);
    let mut target = [0_u8; 32];
    target[12..].copy_from_slice(target_addr.as_slice());
    calldata.extend_from_slice(&target);
    calldata.extend_from_slice(&word(64));
    calldata.extend_from_slice(&dynamic_bytes(inner));
    calldata
}
