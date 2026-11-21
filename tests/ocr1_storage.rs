use std::{collections::BTreeMap, sync::Arc};

use evm_oracle_state::alloy_network::Ethereum;
use alloy_primitives::{
    Address, B256, Bytes, I256, Log as PrimitiveLog, U256, aliases::I192, b256,
};
use evm_oracle_state::alloy_rpc_types_eth::Log as RpcLog;
use alloy_sol_types::SolEvent;
use evm_fork_cache::{
    StateUpdate, StateView,
    reactive::{
        BlockRef, ChainStatus, InputSource, ReactiveContext, ReactiveEffect, ReactiveHandler,
        ReactiveInput,
    },
};
use evm_oracle_state::{
    ANSWER_UPDATED_TOPIC, AggregatorLayoutEvidence, ChainlinkOcr1StorageAdapter, FeedId,
    FeedMetadata, FeedRegistration, FeedSource, OCR1_NEW_TRANSMISSION_TOPIC, OracleFeedStatus,
    OraclePriceUpdate, OracleReactiveHandler, OracleSignalKind, OracleStorageEffect,
    OracleStorageSync, StalenessPolicy,
};

alloy_sol_types::sol! {
    event NewTransmission(
        uint32 indexed aggregatorRoundId,
        int192 answer,
        address transmitter,
        int192[] observations,
        bytes observers,
        bytes32 rawReportContext
    );
}

#[derive(Default)]
struct MapStateView {
    slots: BTreeMap<(Address, U256), U256>,
}

impl MapStateView {
    fn with_slot(mut self, address: Address, slot: U256, value: U256) -> Self {
        self.slots.insert((address, slot), value);
        self
    }
}

impl StateView for MapStateView {
    fn storage(&self, address: Address, slot: U256) -> Option<U256> {
        self.slots.get(&(address, slot)).copied()
    }
}

fn metadata() -> FeedMetadata {
    FeedMetadata {
        decimals: 8,
        description: "ETH / USD".to_string(),
        version: U256::from(4_u64),
    }
}

fn registration(proxy: Address, aggregator: Address, version: &str) -> FeedRegistration {
    FeedRegistration {
        id: FeedId::new("eth-usd"),
        proxy,
        label: Some("ETH/USD".to_string()),
        base: Some("ETH".to_string()),
        quote: Some("USD".to_string()),
        staleness: StalenessPolicy::max_age(3_600),
        current_aggregator: Some(aggregator),
        aggregator_layout: Some(AggregatorLayoutEvidence::from_type_and_version(
            aggregator, version, None,
        )),
        metadata: metadata(),
        source: FeedSource::Chainlink,
        status: OracleFeedStatus::Ready,
    }
}

fn uint_topic(value: U256) -> B256 {
    B256::from(value.to_be_bytes::<32>())
}

fn int_topic(value: I256) -> B256 {
    B256::from(value.into_raw().to_be_bytes::<32>())
}

fn word(value: U256) -> Vec<u8> {
    value.to_be_bytes::<32>().to_vec()
}

fn block_hash(block_number: u64) -> B256 {
    B256::repeat_byte(block_number as u8)
}

fn context(block_number: u64, log_index: u64) -> ReactiveContext {
    let block = BlockRef {
        number: block_number,
        hash: block_hash(block_number),
        parent_hash: None,
        timestamp: Some(1_700_000_000 + block_number),
    };
    ReactiveContext {
        chain_id: Some(10),
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
        transaction_hash: Some(B256::repeat_byte(0x77)),
        transaction_index: Some(0),
        log_index: Some(log_index),
        removed: false,
    }
}

fn raw_report_context(config_digest: [u8; 16], epoch_and_round: u64) -> B256 {
    let mut bytes = [0_u8; 32];
    bytes[11..27].copy_from_slice(&config_digest);
    bytes[27..].copy_from_slice(&epoch_and_round.to_be_bytes()[3..]);
    B256::from(bytes)
}

fn ocr1_new_transmission_log(address: Address) -> RpcLog {
    let event = NewTransmission {
        aggregatorRoundId: 101,
        answer: I192::try_from(2_100_000_000_i64).expect("answer fits int192"),
        transmitter: Address::repeat_byte(0x33),
        observations: vec![I192::try_from(2_100_000_000_i64).expect("observation fits int192")],
        observers: Bytes::from(vec![0x01]),
        rawReportContext: raw_report_context([0x44; 16], 0x1a_89_04),
    };
    rpc_log(
        address,
        vec![OCR1_NEW_TRANSMISSION_TOPIC, uint_topic(U256::from(101_u64))],
        event.encode_data().to_vec(),
        120,
        0,
    )
}

#[test]
fn ocr1_adapter_emits_exact_hot_vars_and_transmission_writes() {
    let proxy = Address::repeat_byte(0x10);
    let aggregator = Address::repeat_byte(0x20);
    let sync = OracleStorageSync::new().with_adapter(Arc::new(ChainlinkOcr1StorageAdapter::new()));
    let state = MapStateView::default().with_slot(
        aggregator,
        ChainlinkOcr1StorageAdapter::hot_vars_slot(),
        U256::from(0xaaaa_u64),
    );

    let update = evm_oracle_state::Ocr1TransmissionStorageUpdate {
        aggregator,
        aggregator_round_id: U256::from(101_u64),
        answer: I256::unchecked_from(2_100_000_000_i64),
        transmission_timestamp: 1_700_000_120,
        epoch_and_round: 0x1a_89_04,
    };
    let effect = sync
        .state_effect_for_ocr1_transmission(
            &registration(
                proxy,
                aggregator,
                "AccessControlledOffchainAggregator 3.0.0",
            ),
            &update,
            &state,
        )
        .expect("storage effect");

    let OracleStorageEffect::StateUpdates { adapter, updates } = effect else {
        panic!("expected OCR1 state updates");
    };
    assert_eq!(adapter, "chainlink-ocr1");
    assert_eq!(updates.len(), 2);

    let expected_hot_mask = ChainlinkOcr1StorageAdapter::latest_epoch_and_round_mask()
        | ChainlinkOcr1StorageAdapter::latest_aggregator_round_id_mask();
    let expected_hot_value = ChainlinkOcr1StorageAdapter::latest_epoch_and_round_value(0x1a_89_04)
        .expect("epochAndRound")
        | ChainlinkOcr1StorageAdapter::latest_aggregator_round_id_value(101);
    assert!(matches!(
        &updates[0],
        StateUpdate::SlotMasked {
            address,
            slot,
            mask,
            value
        } if *address == aggregator
            && *slot == ChainlinkOcr1StorageAdapter::hot_vars_slot()
            && *mask == expected_hot_mask
            && *value == expected_hot_value
    ));

    let expected_transmission_slot = U256::from_be_slice(
        b256!("fbb6eb15126eac42271667fffe37c385a4e6d9519cfe36d59794e1db0abee9f5").as_slice(),
    );
    let expected_transmission = ChainlinkOcr1StorageAdapter::pack_transmission(
        I256::unchecked_from(2_100_000_000_i64),
        1_700_000_120,
    )
    .expect("pack exact transmission");
    assert_eq!(
        ChainlinkOcr1StorageAdapter::transmission_slot(101),
        expected_transmission_slot
    );
    assert!(matches!(
        &updates[1],
        StateUpdate::Slot {
            address,
            slot,
            value
        } if *address == aggregator
            && *slot == expected_transmission_slot
            && *value == expected_transmission
    ));
}

#[test]
fn chainlink_defaults_enable_ocr1_storage_sync() {
    let proxy = Address::repeat_byte(0x10);
    let aggregator = Address::repeat_byte(0x20);
    let state = MapStateView::default().with_slot(
        aggregator,
        ChainlinkOcr1StorageAdapter::hot_vars_slot(),
        U256::from(0xaaaa_u64),
    );
    let update = evm_oracle_state::Ocr1TransmissionStorageUpdate {
        aggregator,
        aggregator_round_id: U256::from(101_u64),
        answer: I256::unchecked_from(2_100_000_000_i64),
        transmission_timestamp: 1_700_000_120,
        epoch_and_round: 0x1a_89_04,
    };

    let effect = OracleStorageSync::chainlink_defaults()
        .state_effect_for_ocr1_transmission(
            &registration(
                proxy,
                aggregator,
                "AccessControlledOffchainAggregator 2.0.0",
            ),
            &update,
            &state,
        )
        .expect("storage effect");

    assert!(matches!(
        effect,
        OracleStorageEffect::StateUpdates {
            adapter: "chainlink-ocr1",
            ..
        }
    ));
}

#[test]
fn ocr1_adapter_declines_cold_hot_vars_slot_for_purge_fallback() {
    let proxy = Address::repeat_byte(0x10);
    let aggregator = Address::repeat_byte(0x20);
    let sync = OracleStorageSync::new().with_adapter(Arc::new(ChainlinkOcr1StorageAdapter::new()));
    let update = evm_oracle_state::Ocr1TransmissionStorageUpdate {
        aggregator,
        aggregator_round_id: U256::from(101_u64),
        answer: I256::unchecked_from(2_100_000_000_i64),
        transmission_timestamp: 1_700_000_120,
        epoch_and_round: 0x1a_89_04,
    };

    let effect = sync
        .state_effect_for_ocr1_transmission(
            &registration(
                proxy,
                aggregator,
                "AccessControlledOffchainAggregator 4.0.0",
            ),
            &update,
            &MapStateView::default(),
        )
        .expect("storage effect");

    assert!(matches!(effect, OracleStorageEffect::FallbackPurge));
}

#[test]
fn ocr1_storage_warmup_declares_hot_vars_slot() {
    let aggregator = Address::repeat_byte(0x20);
    let registration = registration(
        Address::repeat_byte(0x10),
        aggregator,
        "AccessControlledOffchainAggregator 3.0.0",
    );

    let slots =
        OracleStorageSync::chainlink_defaults().warm_slots_for_registrations([&registration]);

    assert_eq!(
        slots,
        vec![(aggregator, ChainlinkOcr1StorageAdapter::hot_vars_slot())]
    );
}

#[test]
fn ocr1_handler_subscribes_to_new_transmission_instead_of_answer_updated() {
    let proxy = Address::repeat_byte(0x10);
    let aggregator = Address::repeat_byte(0x20);
    let handler = OracleReactiveHandler::with_storage_sync(
        vec![registration(
            proxy,
            aggregator,
            "AccessControlledOffchainAggregator 3.0.0",
        )],
        OracleStorageSync::chainlink_defaults(),
    );
    let interests = handler.interests();

    assert_eq!(interests.len(), 1);
    let debug = format!("{:?}", interests[0]);
    assert!(debug.contains(&format!("{OCR1_NEW_TRANSMISSION_TOPIC:?}")));
    assert!(!debug.contains(&format!("{ANSWER_UPDATED_TOPIC:?}")));
}

#[test]
fn ocr1_new_transmission_emits_state_updates_and_timestamp_hook() {
    let proxy = Address::repeat_byte(0x10);
    let aggregator = Address::repeat_byte(0x20);
    let handler = OracleReactiveHandler::with_storage_sync(
        vec![registration(
            proxy,
            aggregator,
            "AccessControlledOffchainAggregator 3.0.0",
        )],
        OracleStorageSync::chainlink_defaults(),
    );
    let state = MapStateView::default().with_slot(
        aggregator,
        ChainlinkOcr1StorageAdapter::hot_vars_slot(),
        U256::from(0xaaaa_u64),
    );

    let outcome = handler
        .handle(
            &context(120, 0),
            &ReactiveInput::<Ethereum>::Log(ocr1_new_transmission_log(aggregator)),
            &state,
        )
        .expect("handler outcome");

    let expected_hot_mask = ChainlinkOcr1StorageAdapter::latest_epoch_and_round_mask()
        | ChainlinkOcr1StorageAdapter::latest_aggregator_round_id_mask();
    let expected_hot_value = ChainlinkOcr1StorageAdapter::latest_epoch_and_round_value(0x1a_89_04)
        .expect("epochAndRound")
        | ChainlinkOcr1StorageAdapter::latest_aggregator_round_id_value(101);
    assert!(outcome.effects.iter().any(|effect| {
        matches!(
            effect,
            ReactiveEffect::StateUpdate(StateUpdate::SlotMasked {
                address,
                slot,
                mask,
                value
            }) if *address == aggregator
                && *slot == ChainlinkOcr1StorageAdapter::hot_vars_slot()
                && *mask == expected_hot_mask
                && *value == expected_hot_value
        )
    }));

    let expected_transmission = ChainlinkOcr1StorageAdapter::pack_transmission(
        I256::unchecked_from(2_100_000_000_i64),
        1_700_000_120,
    )
    .expect("pack exact transmission");
    assert!(outcome.effects.iter().any(|effect| {
        matches!(
            effect,
            ReactiveEffect::StateUpdate(StateUpdate::Slot {
                address,
                slot,
                value
            }) if *address == aggregator
                && *slot == ChainlinkOcr1StorageAdapter::transmission_slot(101)
                && *value == expected_transmission
        )
    }));

    let update = outcome
        .effects
        .iter()
        .find_map(|effect| match effect {
            ReactiveEffect::Hook(signal)
                if signal.kind.as_ref() == OracleSignalKind::PriceUpdate.as_str() =>
            {
                signal.payload.as_ref()?.downcast_ref::<OraclePriceUpdate>()
            }
            _ => None,
        })
        .expect("price update hook");
    assert_eq!(update.started_at, 1_700_000_120);
    assert_eq!(update.updated_at, 1_700_000_120);
}

#[test]
fn ocr1_answer_updated_is_ignored_when_new_transmission_is_primary() {
    let proxy = Address::repeat_byte(0x10);
    let aggregator = Address::repeat_byte(0x20);
    let handler = OracleReactiveHandler::with_storage_sync(
        vec![registration(
            proxy,
            aggregator,
            "AccessControlledOffchainAggregator 3.0.0",
        )],
        OracleStorageSync::chainlink_defaults(),
    );
    let log = rpc_log(
        aggregator,
        vec![
            ANSWER_UPDATED_TOPIC,
            int_topic(I256::unchecked_from(2_100_000_000_i64)),
            uint_topic(U256::from(101_u64)),
        ],
        word(U256::from(1_700_000_100_u64)),
        120,
        1,
    );

    let outcome = handler
        .handle(
            &context(120, 1),
            &ReactiveInput::<Ethereum>::Log(log),
            &MapStateView::default(),
        )
        .expect("handler outcome");

    assert!(outcome.effects.is_empty());
}

#[test]
fn ocr1_pack_transmission_preserves_negative_int192_answers() {
    let word = ChainlinkOcr1StorageAdapter::pack_transmission(I256::unchecked_from(-42_i64), 7)
        .expect("pack negative answer");
    let answer_mask = (U256::from(1_u8) << 192) - U256::from(1_u8);

    assert_eq!(word & answer_mask, answer_mask - U256::from(41_u64));
    assert_eq!(word >> 192, U256::from(7_u64));
}
