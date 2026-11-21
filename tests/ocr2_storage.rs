use std::{collections::BTreeMap, sync::Arc};

use evm_oracle_state::alloy_network::Ethereum;
use alloy_primitives::{
    Address, B256, Bytes, I256, Log as PrimitiveLog, U256, aliases::I192, aliases::U40, b256,
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
    ANSWER_UPDATED_TOPIC, AggregatorLayoutEvidence, ChainlinkOcr2StorageAdapter, FeedId,
    FeedMetadata, FeedRegistration, FeedSource, OCR2_NEW_TRANSMISSION_TOPIC, OracleFeedStatus,
    OraclePriceUpdate, OracleReactiveHandler, OracleRoundStatus, OracleSignalKind,
    OracleStorageEffect, OracleStorageSync, OracleValueSource, OracleValueStatus, StalenessPolicy,
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

fn registration(proxy: Address, aggregator: Address) -> FeedRegistration {
    registration_with_source(
        "eth-usd",
        proxy,
        aggregator,
        FeedSource::Chainlink,
        "ETH/USD",
    )
}

fn registration_with_source(
    id: &str,
    proxy: Address,
    aggregator: Address,
    source: FeedSource,
    label: &str,
) -> FeedRegistration {
    FeedRegistration {
        id: FeedId::new(id),
        proxy,
        label: Some(label.to_string()),
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
        source,
        status: OracleFeedStatus::Ready,
    }
}

fn price_update(proxy: Address, aggregator: Address) -> OraclePriceUpdate {
    OraclePriceUpdate {
        id: FeedId::new("eth-usd"),
        proxy,
        aggregator,
        label: Some("ETH/USD".to_string()),
        base: Some("ETH".to_string()),
        quote: Some("USD".to_string()),
        raw_answer: I256::unchecked_from(2_100_000_000_i64),
        decimals: 8,
        event_round_id: U256::from(101_u64),
        started_at: 1_700_000_100,
        updated_at: 1_700_000_100,
        block_number: Some(120),
        block_hash: Some(block_hash(120)),
        log_index: Some(0),
        round_status: OracleRoundStatus::Fresh,
        value_status: OracleValueStatus::EventPending,
        source: OracleValueSource::Event,
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

fn ocr2_new_transmission_log(address: Address) -> RpcLog {
    let event = NewTransmission {
        aggregatorRoundId: 101,
        answer: I192::try_from(2_100_000_000_i64).expect("answer fits int192"),
        transmitter: Address::repeat_byte(0x33),
        observationsTimestamp: 1_700_000_040,
        observations: vec![I192::try_from(2_100_000_000_i64).expect("observation fits int192")],
        observers: Bytes::from(vec![0x01]),
        juelsPerFeeCoin: I192::ZERO,
        configDigest: B256::repeat_byte(0x44),
        epochAndRound: U40::from(0x1a_89_04_u64),
    };
    rpc_log(
        address,
        vec![OCR2_NEW_TRANSMISSION_TOPIC, uint_topic(U256::from(101_u64))],
        event.encode_data().to_vec(),
        120,
        0,
    )
}

#[test]
fn ocr2_adapter_emits_exact_hot_vars_and_transmission_writes() {
    let proxy = Address::repeat_byte(0x10);
    let aggregator = Address::repeat_byte(0x20);
    let adapter = ChainlinkOcr2StorageAdapter::new();
    let sync = OracleStorageSync::new().with_adapter(Arc::new(adapter));
    let state = MapStateView::default().with_slot(
        aggregator,
        ChainlinkOcr2StorageAdapter::hot_vars_slot(),
        U256::from(0xaaaa_u64),
    );

    let effect = sync
        .state_effect_for_answer(
            &registration(proxy, aggregator),
            &price_update(proxy, aggregator),
            &state,
        )
        .expect("storage effect");

    let OracleStorageEffect::StateUpdates { adapter, updates } = effect else {
        panic!("expected OCR2 state updates");
    };
    assert_eq!(adapter, "chainlink-ocr2");
    assert_eq!(updates.len(), 2);

    assert!(matches!(
        &updates[0],
        StateUpdate::SlotMasked {
            address,
            slot,
            mask,
            value
        } if *address == aggregator
            && *slot == ChainlinkOcr2StorageAdapter::hot_vars_slot()
            && *mask == (U256::from(u32::MAX) << 48)
            && *value == (U256::from(101_u64) << 48)
    ));

    let expected_transmission_slot = U256::from_be_slice(
        b256!("40bf9d9f34e44c316634927ac0b32bbadacc4320b4afbc5a2c3cc0df5c0089df").as_slice(),
    );
    let expected_transmission = U256::from(2_100_000_000_u64)
        | (U256::from(1_700_000_100_u32) << 192)
        | (U256::from(1_700_000_100_u32) << 224);
    assert_eq!(
        ChainlinkOcr2StorageAdapter::transmission_slot(101),
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
fn ocr2_adapter_declines_cold_hot_vars_slot_for_purge_fallback() {
    let proxy = Address::repeat_byte(0x10);
    let aggregator = Address::repeat_byte(0x20);
    let adapter = ChainlinkOcr2StorageAdapter::new();
    let sync = OracleStorageSync::new().with_adapter(Arc::new(adapter));

    let effect = sync
        .state_effect_for_answer(
            &registration(proxy, aggregator),
            &price_update(proxy, aggregator),
            &MapStateView::default(),
        )
        .expect("storage effect");

    assert!(matches!(effect, OracleStorageEffect::FallbackPurge));
}

#[test]
fn ocr2_storage_warmup_declares_and_dedupes_hot_vars_slot() {
    let aggregator = Address::repeat_byte(0x20);
    let first = registration_with_source(
        "eth-usd",
        Address::repeat_byte(0x10),
        aggregator,
        FeedSource::Chainlink,
        "ETH/USD",
    );
    let second = registration_with_source(
        "weth-usd",
        Address::repeat_byte(0x11),
        aggregator,
        FeedSource::Chainlink,
        "WETH/USD",
    );

    let slots =
        OracleStorageSync::chainlink_defaults().warm_slots_for_registrations([&first, &second]);

    assert_eq!(
        slots,
        vec![(aggregator, ChainlinkOcr2StorageAdapter::hot_vars_slot())]
    );
}

#[test]
fn ocr2_handler_subscribes_to_new_transmission_instead_of_answer_updated() {
    let proxy = Address::repeat_byte(0x10);
    let aggregator = Address::repeat_byte(0x20);
    let handler = OracleReactiveHandler::with_storage_sync(
        vec![registration(proxy, aggregator)],
        OracleStorageSync::new().with_adapter(Arc::new(ChainlinkOcr2StorageAdapter::new())),
    );
    let interests = handler.interests();

    assert_eq!(interests.len(), 1);
    let debug = format!("{:?}", interests[0]);
    assert!(debug.contains(&format!("{OCR2_NEW_TRANSMISSION_TOPIC:?}")));
    assert!(!debug.contains(&format!("{ANSWER_UPDATED_TOPIC:?}")));
}

#[test]
fn ocr2_new_transmission_emits_exact_state_updates_and_started_at_hook() {
    let proxy = Address::repeat_byte(0x10);
    let aggregator = Address::repeat_byte(0x20);
    let handler = OracleReactiveHandler::with_storage_sync(
        vec![registration(proxy, aggregator)],
        OracleStorageSync::new().with_adapter(Arc::new(ChainlinkOcr2StorageAdapter::new())),
    );
    let state = MapStateView::default().with_slot(
        aggregator,
        ChainlinkOcr2StorageAdapter::hot_vars_slot(),
        U256::from(0xaaaa_u64),
    );

    let outcome = handler
        .handle(
            &context(120, 0),
            &ReactiveInput::<Ethereum>::Log(ocr2_new_transmission_log(aggregator)),
            &state,
        )
        .expect("handler outcome");

    let expected_hot_mask = ChainlinkOcr2StorageAdapter::latest_epoch_and_round_mask()
        | ChainlinkOcr2StorageAdapter::latest_aggregator_round_id_mask();
    let expected_hot_value = ChainlinkOcr2StorageAdapter::latest_epoch_and_round_value(0x1a_89_04)
        .expect("epochAndRound")
        | ChainlinkOcr2StorageAdapter::latest_aggregator_round_id_value(101);
    assert!(outcome.effects.iter().any(|effect| {
        matches!(
            effect,
            ReactiveEffect::StateUpdate(StateUpdate::SlotMasked {
                address,
                slot,
                mask,
                value
            }) if *address == aggregator
                && *slot == ChainlinkOcr2StorageAdapter::hot_vars_slot()
                && *mask == expected_hot_mask
                && *value == expected_hot_value
        )
    }));

    let expected_transmission = ChainlinkOcr2StorageAdapter::pack_transmission(
        I256::unchecked_from(2_100_000_000_i64),
        1_700_000_040,
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
                && *slot == ChainlinkOcr2StorageAdapter::transmission_slot(101)
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
    assert_eq!(update.started_at, 1_700_000_040);
    assert_eq!(update.updated_at, 1_700_000_120);
}

#[test]
fn shared_ocr2_dependency_emits_one_storage_update_set_and_fans_out_hooks() {
    let chainlink_proxy = Address::repeat_byte(0x10);
    let aave_source = Address::repeat_byte(0x11);
    let aggregator = Address::repeat_byte(0x20);
    let registrations = vec![
        registration_with_source(
            "eth-usd",
            chainlink_proxy,
            aggregator,
            FeedSource::Chainlink,
            "ETH/USD",
        ),
        registration_with_source(
            "aave-capped-eth-usd",
            aave_source,
            aggregator,
            FeedSource::aave_price_cap_stable(
                aave_source,
                chainlink_proxy,
                I256::unchecked_from(2_000_000_000_i64),
            ),
            "Aave capped ETH/USD",
        ),
    ];
    let handler = OracleReactiveHandler::with_storage_sync(
        registrations,
        OracleStorageSync::new().with_adapter(Arc::new(ChainlinkOcr2StorageAdapter::new())),
    );
    let interests = handler.interests();
    assert_eq!(
        interests.len(),
        1,
        "one shared OCR2 dependency should produce one subscription interest"
    );
    let state = MapStateView::default().with_slot(
        aggregator,
        ChainlinkOcr2StorageAdapter::hot_vars_slot(),
        U256::from(0xaaaa_u64),
    );

    let outcome = handler
        .handle(
            &context(120, 0),
            &ReactiveInput::<Ethereum>::Log(ocr2_new_transmission_log(aggregator)),
            &state,
        )
        .expect("handler outcome");

    let state_updates = outcome
        .effects
        .iter()
        .filter(|effect| matches!(effect, ReactiveEffect::StateUpdate(_)))
        .count();
    assert_eq!(
        state_updates, 2,
        "shared dependency should write OCR2 hot vars and transmission once"
    );

    let mut price_updates = outcome
        .effects
        .iter()
        .filter_map(|effect| match effect {
            ReactiveEffect::Hook(signal)
                if signal.kind.as_ref() == OracleSignalKind::PriceUpdate.as_str() =>
            {
                signal.payload.as_ref()?.downcast_ref::<OraclePriceUpdate>()
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    price_updates.sort_by_key(|update| update.id.as_str().to_string());
    assert_eq!(price_updates.len(), 2);
    assert_eq!(price_updates[0].id.as_str(), "aave-capped-eth-usd");
    assert_eq!(
        price_updates[0].raw_answer,
        I256::unchecked_from(2_000_000_000_i64),
        "Aave hook should still receive its normalized capped answer"
    );
    assert_eq!(price_updates[1].id.as_str(), "eth-usd");
    assert_eq!(
        price_updates[1].raw_answer,
        I256::unchecked_from(2_100_000_000_i64)
    );
}

#[test]
fn ocr2_answer_updated_is_ignored_when_new_transmission_is_primary() {
    let proxy = Address::repeat_byte(0x10);
    let aggregator = Address::repeat_byte(0x20);
    let handler = OracleReactiveHandler::with_storage_sync(
        vec![registration(proxy, aggregator)],
        OracleStorageSync::new().with_adapter(Arc::new(ChainlinkOcr2StorageAdapter::new())),
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
fn ocr2_pack_transmission_rejects_values_outside_int192() {
    let too_large = I256::from_raw(U256::from(1_u8) << 191);
    let error = ChainlinkOcr2StorageAdapter::pack_transmission_from_event(too_large, 1)
        .expect_err("int192 overflow should fail");

    assert!(error.to_string().contains("does not fit OCR2 int192"));
}

#[test]
fn ocr2_pack_transmission_preserves_negative_int192_answers() {
    let word =
        ChainlinkOcr2StorageAdapter::pack_transmission_from_event(I256::unchecked_from(-42_i64), 7)
            .expect("pack negative answer");
    let answer_mask = (U256::from(1_u8) << 192) - U256::from(1_u8);

    assert_eq!(word & answer_mask, answer_mask - U256::from(41_u64));
    assert_eq!(word >> 192, U256::from(7_u64) | (U256::from(7_u64) << 32));
}
