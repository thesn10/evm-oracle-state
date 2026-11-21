#![cfg(feature = "redstone")]

use std::collections::BTreeMap;

use evm_oracle_state::alloy_network::Ethereum;
use alloy_primitives::{Address, B256, Bytes, I256, Log as PrimitiveLog, U256};
use evm_oracle_state::alloy_rpc_types_eth::Log as RpcLog;
use evm_fork_cache::{
    StateUpdate, StateView,
    reactive::{
        BlockRef, ChainStatus, InputSource, ReactiveContext, ReactiveEffect, ReactiveHandler,
        ReactiveInput, StateEffectQuality,
    },
    state_update::PurgeScope,
};
use evm_oracle_state::{
    ANSWER_UPDATED_TOPIC, FeedId, FeedMetadata, FeedRegistration, FeedSource,
    ORACLE_SIGNAL_NAMESPACE, OracleFeedStatus, OraclePriceUpdate, OracleSignalKind,
    REDSTONE_VALUE_UPDATE_TOPIC, RedstoneMultiFeedStorageAdapter, RedstoneReactiveHandler,
    StalenessPolicy,
};

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

#[test]
fn redstone_value_update_writes_hot_multi_feed_slot_without_invalidating() {
    let price_feed = Address::repeat_byte(0x10);
    let adapter = Address::repeat_byte(0x20);
    let data_feed_id = data_feed_id("ETH");
    let details_slot = RedstoneMultiFeedStorageAdapter::data_feed_details_slot(data_feed_id);
    let state = MapStateView::default().with_slot(adapter, details_slot, U256::from(0xaaaa_u64));
    let handler =
        RedstoneReactiveHandler::new(vec![registration(price_feed, adapter, data_feed_id)]);
    let updated_at = 1_700_000_120_u64;
    let value = U256::from(2_250_000_000_u64);

    let outcome = handler
        .handle(
            &context(111, 4),
            &ReactiveInput::<Ethereum>::Log(value_update_log(
                adapter,
                data_feed_id,
                value,
                updated_at,
            )),
            &state,
        )
        .expect("handler outcome");

    assert_eq!(outcome.quality, StateEffectQuality::ExactFromInput);
    assert!(outcome.effects.iter().any(|effect| {
        matches!(
            effect,
            ReactiveEffect::StateUpdate(StateUpdate::Slot {
                address,
                slot,
                value: written
            }) if *address == adapter
                && *slot == details_slot
                && *written == RedstoneMultiFeedStorageAdapter::pack_data_feed_details_from_event(value, updated_at).expect("packed details")
        )
    }));
    assert!(
        !outcome
            .effects
            .iter()
            .any(|effect| matches!(effect, ReactiveEffect::Invalidate(_))),
        "hot RedStone storage writes should avoid purge/refetch"
    );
    let update = price_update_hook(&outcome.effects).expect("price update hook");
    assert_eq!(update.raw_answer, I256::unchecked_from(2_250_000_000_i64));
    assert_eq!(update.updated_at, updated_at);
}

#[test]
fn redstone_value_update_falls_back_to_invalidation_when_slot_is_cold() {
    let price_feed = Address::repeat_byte(0x10);
    let adapter = Address::repeat_byte(0x20);
    let data_feed_id = data_feed_id("ETH");
    let handler =
        RedstoneReactiveHandler::new(vec![registration(price_feed, adapter, data_feed_id)]);

    let outcome = handler
        .handle(
            &context(111, 4),
            &ReactiveInput::<Ethereum>::Log(value_update_log(
                adapter,
                data_feed_id,
                U256::from(2_250_000_000_u64),
                1_700_000_120,
            )),
            &MapStateView::default(),
        )
        .expect("handler outcome");

    assert_eq!(outcome.quality, StateEffectQuality::RequiresRepair);
    assert!(outcome.effects.iter().any(|effect| {
        matches!(
            effect,
            ReactiveEffect::Invalidate(request)
                if request.address == price_feed
                    && matches!(request.scope, PurgeScope::AllStorage)
        )
    }));
    assert!(outcome.effects.iter().any(|effect| {
        matches!(
            effect,
            ReactiveEffect::Invalidate(request)
                if request.address == adapter
                    && matches!(request.scope, PurgeScope::AllStorage)
        )
    }));
    assert!(
        !outcome
            .effects
            .iter()
            .any(|effect| matches!(effect, ReactiveEffect::StateUpdate(_))),
        "cold RedStone slots should not be materialized by direct writes"
    );
}

#[test]
fn redstone_multi_feed_overflow_value_requires_hot_bigger_value_slot() {
    let price_feed = Address::repeat_byte(0x10);
    let adapter = Address::repeat_byte(0x20);
    let data_feed_id = data_feed_id("LARGE");
    let details_slot = RedstoneMultiFeedStorageAdapter::data_feed_details_slot(data_feed_id);
    let bigger_value_slot = RedstoneMultiFeedStorageAdapter::bigger_value_slot(data_feed_id);
    let value = U256::from(1_u8) << 152;
    let updated_at = 1_700_000_120_u64;
    let handler =
        RedstoneReactiveHandler::new(vec![registration(price_feed, adapter, data_feed_id)]);

    let hot_state = MapStateView::default()
        .with_slot(adapter, details_slot, U256::ZERO)
        .with_slot(adapter, bigger_value_slot, U256::ZERO);
    let outcome = handler
        .handle(
            &context(111, 4),
            &ReactiveInput::<Ethereum>::Log(value_update_log(
                adapter,
                data_feed_id,
                value,
                updated_at,
            )),
            &hot_state,
        )
        .expect("handler outcome");

    assert_eq!(
        outcome
            .effects
            .iter()
            .filter(|effect| matches!(effect, ReactiveEffect::StateUpdate(_)))
            .count(),
        2
    );
    assert!(outcome.effects.iter().any(|effect| {
        matches!(
            effect,
            ReactiveEffect::StateUpdate(StateUpdate::Slot {
                address,
                slot,
                value: written
            }) if *address == adapter && *slot == bigger_value_slot && *written == value
        )
    }));

    let missing_overflow_state =
        MapStateView::default().with_slot(adapter, details_slot, U256::ZERO);
    let fallback = handler
        .handle(
            &context(111, 4),
            &ReactiveInput::<Ethereum>::Log(value_update_log(
                adapter,
                data_feed_id,
                value,
                updated_at,
            )),
            &missing_overflow_state,
        )
        .expect("handler outcome");
    assert_eq!(fallback.quality, StateEffectQuality::RequiresRepair);
    assert!(
        fallback
            .effects
            .iter()
            .any(|effect| matches!(effect, ReactiveEffect::Invalidate(_)))
    );
}

#[test]
fn redstone_merged_answer_updated_writes_no_rounds_multi_feed_slot_when_source_is_storage() {
    let price_feed = Address::repeat_byte(0x30);
    let data_feed_id = data_feed_id("BTC");
    let details_slot = RedstoneMultiFeedStorageAdapter::data_feed_details_slot(data_feed_id);
    let state = MapStateView::default().with_slot(price_feed, details_slot, U256::from(0xaaaa_u64));
    let handler =
        RedstoneReactiveHandler::new(vec![registration(price_feed, price_feed, data_feed_id)]);
    let updated_at = 1_700_000_180_u64;
    let answer = I256::unchecked_from(61_000_000_000_i64);

    let outcome = handler
        .handle(
            &context(112, 1),
            &ReactiveInput::<Ethereum>::Log(answer_updated_log(
                price_feed,
                answer,
                U256::from(1_u64),
                updated_at,
            )),
            &state,
        )
        .expect("handler outcome");

    assert_eq!(outcome.quality, StateEffectQuality::ExactFromInput);
    assert!(outcome.effects.iter().any(|effect| {
        matches!(
            effect,
            ReactiveEffect::StateUpdate(StateUpdate::Slot {
                address,
                slot,
                value
            }) if *address == price_feed
                && *slot == details_slot
                && *value == RedstoneMultiFeedStorageAdapter::pack_data_feed_details_from_event(U256::from(61_000_000_000_u64), updated_at).expect("packed details")
        )
    }));
}

fn registration(price_feed: Address, adapter: Address, data_feed_id: B256) -> FeedRegistration {
    FeedRegistration {
        id: FeedId::new("redstone-eth-usd"),
        proxy: price_feed,
        label: Some("RedStone ETH/USD".to_string()),
        base: Some("ETH".to_string()),
        quote: Some("USD".to_string()),
        staleness: StalenessPolicy::max_age(3_600),
        current_aggregator: Some(adapter),
        aggregator_layout: None,
        metadata: FeedMetadata {
            decimals: 8,
            description: "RedStone ETH/USD".to_string(),
            version: U256::from(1_u64),
        },
        source: FeedSource::redstone_push(price_feed, adapter, data_feed_id),
        status: OracleFeedStatus::Ready,
    }
}

fn price_update_hook(effects: &[ReactiveEffect]) -> Option<&OraclePriceUpdate> {
    effects.iter().find_map(|effect| match effect {
        ReactiveEffect::Hook(signal)
            if signal.namespace.as_ref() == ORACLE_SIGNAL_NAMESPACE
                && signal.kind.as_ref() == OracleSignalKind::PriceUpdate.as_str() =>
        {
            signal.payload.as_ref()?.downcast_ref::<OraclePriceUpdate>()
        }
        _ => None,
    })
}

fn value_update_log(adapter: Address, data_feed_id: B256, value: U256, updated_at: u64) -> RpcLog {
    rpc_log(
        adapter,
        vec![REDSTONE_VALUE_UPDATE_TOPIC],
        encode_words(&[
            value,
            U256::from_be_slice(data_feed_id.as_slice()),
            U256::from(updated_at),
        ]),
        111,
        4,
    )
}

fn answer_updated_log(
    price_feed: Address,
    answer: I256,
    round_id: U256,
    updated_at: u64,
) -> RpcLog {
    rpc_log(
        price_feed,
        vec![
            ANSWER_UPDATED_TOPIC,
            int_topic(answer),
            uint_topic(round_id),
        ],
        word(U256::from(updated_at)),
        112,
        1,
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

fn data_feed_id(symbol: &str) -> B256 {
    let mut bytes = [0_u8; 32];
    bytes[..symbol.len()].copy_from_slice(symbol.as_bytes());
    B256::from(bytes)
}

fn encode_words(words: &[U256]) -> Vec<u8> {
    words
        .iter()
        .flat_map(|word| word.to_be_bytes::<32>())
        .collect()
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

fn block_hash(block_number: u64) -> B256 {
    B256::repeat_byte(block_number as u8)
}
