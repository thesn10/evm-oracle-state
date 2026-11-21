use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use crate::alloy_network::Ethereum;
use alloy_primitives::{Address, B256, I256, U256};
use crate::alloy_rpc_types_eth::Filter;
use evm_fork_cache::{
    StateView,
    reactive::{
        ChainStatus, HandlerError, HandlerId, HandlerOutcome, HookSignal, InvalidationReason,
        InvalidationRequest, LogInterest, ReactiveContext, ReactiveEffect, ReactiveHandler,
        ReactiveInput, ReactiveInterest, ReportTag, RouteKeySpec, StateEffectQuality,
    },
    state_update::PurgeScope,
};

use crate::{
    ANSWER_UPDATED_TOPIC, AnswerUpdated, FeedRegistration, OCR1_NEW_TRANSMISSION_TOPIC,
    OCR2_NEW_TRANSMISSION_TOPIC, ORACLE_LEGACY_ANSWER_UPDATED_KIND, ORACLE_SIGNAL_NAMESPACE,
    Ocr1NewTransmission, Ocr1TransmissionStorageUpdate, Ocr2NewTransmission,
    Ocr2TransmissionStorageUpdate, OraclePriceUpdate, OracleSignalKind, OracleStorageEffect,
    OracleStorageError, OracleStorageSync, OracleUpdate, OracleValueStatus, RoundData,
    decode_answer_updated, decode_ocr1_new_transmission, decode_ocr2_new_transmission,
    state::classify_round,
};

const HANDLER_ID: &str = "evm-oracle-state.chainlink";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct OracleDependencyKey {
    aggregator: Address,
    event: OracleDependencyEvent,
}

impl OracleDependencyKey {
    fn new(aggregator: Address, event: OracleDependencyEvent) -> Self {
        Self { aggregator, event }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum OracleDependencyEvent {
    AnswerUpdated,
    Ocr1NewTransmission,
    Ocr2NewTransmission,
}

impl OracleDependencyEvent {
    fn topic(self) -> B256 {
        match self {
            Self::AnswerUpdated => ANSWER_UPDATED_TOPIC,
            Self::Ocr1NewTransmission => OCR1_NEW_TRANSMISSION_TOPIC,
            Self::Ocr2NewTransmission => OCR2_NEW_TRANSMISSION_TOPIC,
        }
    }
}

/// Reactive-runtime bridge for Chainlink-compatible oracle feeds.
#[derive(Clone, Debug)]
pub struct OracleReactiveHandler {
    registrations_by_dependency: BTreeMap<OracleDependencyKey, Vec<FeedRegistration>>,
    storage_sync: OracleStorageSync,
}

impl OracleReactiveHandler {
    /// Create a handler from current feed registrations.
    pub fn new(registrations: Vec<FeedRegistration>) -> Self {
        Self::with_storage_sync(registrations, OracleStorageSync::chainlink_defaults())
    }

    /// Create a handler from current feed registrations and storage adapters.
    pub fn with_storage_sync(
        registrations: Vec<FeedRegistration>,
        storage_sync: OracleStorageSync,
    ) -> Self {
        let mut registrations_by_dependency: BTreeMap<OracleDependencyKey, Vec<FeedRegistration>> =
            BTreeMap::new();
        for registration in registrations {
            if !registration.source.uses_builtin_chainlink_handler() {
                continue;
            }
            let mut seen_dependencies = BTreeSet::new();
            for aggregator in registration
                .source
                .event_aggregators(registration.current_aggregator)
            {
                let wants_answer = registration
                    .source
                    .wants_answer_updated_from(registration.current_aggregator, aggregator);
                let wants_ocr2 =
                    !wants_answer && storage_sync.prefers_ocr2_new_transmission(&registration);
                let wants_ocr1 =
                    !wants_answer && storage_sync.prefers_ocr1_new_transmission(&registration);
                let wants_answer = wants_answer || (!wants_ocr2 && !wants_ocr1);

                if wants_answer {
                    insert_dependency_registration(
                        &mut registrations_by_dependency,
                        &mut seen_dependencies,
                        OracleDependencyKey::new(aggregator, OracleDependencyEvent::AnswerUpdated),
                        &registration,
                    );
                }
                if wants_ocr1 {
                    insert_dependency_registration(
                        &mut registrations_by_dependency,
                        &mut seen_dependencies,
                        OracleDependencyKey::new(
                            aggregator,
                            OracleDependencyEvent::Ocr1NewTransmission,
                        ),
                        &registration,
                    );
                }
                if wants_ocr2 {
                    insert_dependency_registration(
                        &mut registrations_by_dependency,
                        &mut seen_dependencies,
                        OracleDependencyKey::new(
                            aggregator,
                            OracleDependencyEvent::Ocr2NewTransmission,
                        ),
                        &registration,
                    );
                }
            }
        }
        Self {
            registrations_by_dependency,
            storage_sync,
        }
    }

    /// Stable handler id.
    pub fn id(&self) -> HandlerId {
        HandlerId::new(HANDLER_ID)
    }

    /// Interests for current aggregators.
    pub fn interests(&self) -> Vec<ReactiveInterest<Ethereum>> {
        self.registrations_by_dependency
            .keys()
            .map(|key| log_interest(key.aggregator, key.event.topic()))
            .collect()
    }

    /// Test helper that avoids requiring callers to construct a cache view.
    pub fn handle_for_test(
        &self,
        ctx: &ReactiveContext,
        input: &ReactiveInput<Ethereum>,
    ) -> Result<HandlerOutcome, HandlerError> {
        self.handle(ctx, input, &EmptyStateView)
    }

    fn handle_answer_updated(
        &self,
        ctx: &ReactiveContext,
        log: &crate::alloy_rpc_types_eth::Log,
        state: &dyn StateView,
    ) -> Result<HandlerOutcome, HandlerError> {
        let aggregator = log.address();
        let key = OracleDependencyKey::new(aggregator, OracleDependencyEvent::AnswerUpdated);
        let Some(registrations) = self.registrations_by_dependency.get(&key) else {
            return Ok(HandlerOutcome::empty(StateEffectQuality::NoStateEffect));
        };

        let event = decode_answer_updated(log)
            .map_err(|error| HandlerError::new(format!("decode AnswerUpdated failed: {error}")))?;
        let block_number = event
            .block_number
            .or_else(|| ctx.block.as_ref().map(|block| block.number));
        let block_hash = log
            .block_hash
            .or_else(|| ctx.block.as_ref().map(|block| block.hash));
        let log_index = event.log_index.or(ctx.log_index);

        let mut effects = Vec::new();
        let mut tags = Vec::new();
        let mut needs_aggregator_purge = false;

        let value_status = event_value_status(log.removed);
        let applied_direct_effect = if log.removed {
            apply_removed_log_fallback(&mut effects, &mut needs_aggregator_purge, registrations);
            false
        } else {
            apply_answer_updated_dependency_storage_effect(
                &self.storage_sync,
                &mut effects,
                &mut needs_aggregator_purge,
                registrations,
                &event,
                block_number,
                block_hash,
                log_index,
                value_status,
                state,
            )
            .map_err(|error| HandlerError::new(format!("{error}")))?
        };

        for registration in registrations {
            let normalized_answer = registration
                .source
                .normalize_answer_from_event(Some(aggregator), event.current);
            let update = OracleUpdate {
                id: registration.id.clone(),
                proxy: registration.proxy,
                aggregator,
                round: RoundData {
                    round_id: event.round_id,
                    answer: normalized_answer,
                    started_at: event.updated_at,
                    updated_at: event.updated_at,
                    answered_in_round: event.round_id,
                },
                block_number,
                log_index,
                value_status,
            };
            let price_update = answer_updated_price_update(
                registration,
                aggregator,
                &event,
                normalized_answer,
                block_number,
                block_hash,
                log_index,
                value_status,
            );
            let hook_tags = hook_tags(registration, aggregator);
            tags.extend(hook_tags.clone());
            if !log.removed {
                effects.push(ReactiveEffect::Hook(HookSignal {
                    namespace: Cow::Borrowed(ORACLE_SIGNAL_NAMESPACE),
                    kind: Cow::Borrowed(ORACLE_LEGACY_ANSWER_UPDATED_KIND),
                    labels: hook_tags.clone(),
                    payload: Some(Arc::new(update)),
                }));
            }
            effects.push(ReactiveEffect::Hook(HookSignal {
                namespace: Cow::Borrowed(ORACLE_SIGNAL_NAMESPACE),
                kind: Cow::Borrowed(OracleSignalKind::PriceUpdate.as_str()),
                labels: hook_tags,
                payload: Some(Arc::new(price_update)),
            }));
        }

        prepend_aggregator_purge(&mut effects, needs_aggregator_purge, aggregator);

        Ok(HandlerOutcome {
            effects,
            quality: state_effect_quality(applied_direct_effect),
            tags,
        })
    }

    fn handle_ocr2_new_transmission(
        &self,
        ctx: &ReactiveContext,
        log: &crate::alloy_rpc_types_eth::Log,
        state: &dyn StateView,
    ) -> Result<HandlerOutcome, HandlerError> {
        let aggregator = log.address();
        let key = OracleDependencyKey::new(aggregator, OracleDependencyEvent::Ocr2NewTransmission);
        let Some(registrations) = self.registrations_by_dependency.get(&key) else {
            return Ok(HandlerOutcome::empty(StateEffectQuality::NoStateEffect));
        };

        let event = decode_ocr2_new_transmission(log).map_err(|error| {
            HandlerError::new(format!("decode OCR2 NewTransmission failed: {error}"))
        })?;
        let transmission_timestamp = ctx
            .block
            .as_ref()
            .and_then(|block| block.timestamp)
            .or(log.block_timestamp)
            .ok_or_else(|| {
                HandlerError::new("OCR2 NewTransmission handling requires block timestamp")
            })?;
        let block_number = event
            .block_number
            .or_else(|| ctx.block.as_ref().map(|block| block.number));
        let block_hash = log
            .block_hash
            .or_else(|| ctx.block.as_ref().map(|block| block.hash));
        let log_index = event.log_index.or(ctx.log_index);

        let mut effects = Vec::new();
        let mut tags = Vec::new();
        let mut needs_aggregator_purge = false;

        let transmission_update = Ocr2TransmissionStorageUpdate {
            aggregator,
            aggregator_round_id: event.aggregator_round_id,
            answer: event.answer,
            observations_timestamp: event.observations_timestamp,
            transmission_timestamp,
            epoch_and_round: event.epoch_and_round,
        };
        let value_status = event_value_status(log.removed);
        let applied_direct_effect = if log.removed {
            apply_removed_log_fallback(&mut effects, &mut needs_aggregator_purge, registrations);
            false
        } else {
            apply_ocr2_dependency_storage_effect(
                &self.storage_sync,
                &mut effects,
                &mut needs_aggregator_purge,
                registrations,
                &transmission_update,
                state,
            )
            .map_err(|error| HandlerError::new(format!("{error}")))?
        };
        for registration in registrations {
            let price_update = ocr2_price_update(
                registration,
                aggregator,
                &event,
                transmission_timestamp,
                block_number,
                block_hash,
                log_index,
                value_status,
            );
            let hook_tags = hook_tags(registration, aggregator);
            tags.extend(hook_tags.clone());
            effects.push(ReactiveEffect::Hook(HookSignal {
                namespace: Cow::Borrowed(ORACLE_SIGNAL_NAMESPACE),
                kind: Cow::Borrowed(OracleSignalKind::PriceUpdate.as_str()),
                labels: hook_tags,
                payload: Some(Arc::new(price_update)),
            }));
        }

        prepend_aggregator_purge(&mut effects, needs_aggregator_purge, aggregator);

        Ok(HandlerOutcome {
            effects,
            quality: state_effect_quality(applied_direct_effect),
            tags,
        })
    }

    fn handle_ocr1_new_transmission(
        &self,
        ctx: &ReactiveContext,
        log: &crate::alloy_rpc_types_eth::Log,
        state: &dyn StateView,
    ) -> Result<HandlerOutcome, HandlerError> {
        let aggregator = log.address();
        let key = OracleDependencyKey::new(aggregator, OracleDependencyEvent::Ocr1NewTransmission);
        let Some(registrations) = self.registrations_by_dependency.get(&key) else {
            return Ok(HandlerOutcome::empty(StateEffectQuality::NoStateEffect));
        };

        let event = decode_ocr1_new_transmission(log).map_err(|error| {
            HandlerError::new(format!("decode OCR1 NewTransmission failed: {error}"))
        })?;
        let transmission_timestamp = ctx
            .block
            .as_ref()
            .and_then(|block| block.timestamp)
            .or(log.block_timestamp)
            .ok_or_else(|| {
                HandlerError::new("OCR1 NewTransmission handling requires block timestamp")
            })?;
        let block_number = event
            .block_number
            .or_else(|| ctx.block.as_ref().map(|block| block.number));
        let block_hash = log
            .block_hash
            .or_else(|| ctx.block.as_ref().map(|block| block.hash));
        let log_index = event.log_index.or(ctx.log_index);

        let mut effects = Vec::new();
        let mut tags = Vec::new();
        let mut needs_aggregator_purge = false;

        let transmission_update = Ocr1TransmissionStorageUpdate {
            aggregator,
            aggregator_round_id: event.aggregator_round_id,
            answer: event.answer,
            transmission_timestamp,
            epoch_and_round: event.epoch_and_round,
        };
        let value_status = event_value_status(log.removed);
        let applied_direct_effect = if log.removed {
            apply_removed_log_fallback(&mut effects, &mut needs_aggregator_purge, registrations);
            false
        } else {
            apply_ocr1_dependency_storage_effect(
                &self.storage_sync,
                &mut effects,
                &mut needs_aggregator_purge,
                registrations,
                &transmission_update,
                state,
            )
            .map_err(|error| HandlerError::new(format!("{error}")))?
        };
        for registration in registrations {
            let price_update = ocr1_price_update(
                registration,
                aggregator,
                &event,
                transmission_timestamp,
                block_number,
                block_hash,
                log_index,
                value_status,
            );
            let hook_tags = hook_tags(registration, aggregator);
            tags.extend(hook_tags.clone());
            effects.push(ReactiveEffect::Hook(HookSignal {
                namespace: Cow::Borrowed(ORACLE_SIGNAL_NAMESPACE),
                kind: Cow::Borrowed(OracleSignalKind::PriceUpdate.as_str()),
                labels: hook_tags,
                payload: Some(Arc::new(price_update)),
            }));
        }

        prepend_aggregator_purge(&mut effects, needs_aggregator_purge, aggregator);

        Ok(HandlerOutcome {
            effects,
            quality: state_effect_quality(applied_direct_effect),
            tags,
        })
    }
}

fn insert_dependency_registration(
    registrations_by_dependency: &mut BTreeMap<OracleDependencyKey, Vec<FeedRegistration>>,
    seen_dependencies: &mut BTreeSet<OracleDependencyKey>,
    key: OracleDependencyKey,
    registration: &FeedRegistration,
) {
    if seen_dependencies.insert(key) {
        registrations_by_dependency
            .entry(key)
            .or_default()
            .push(registration.clone());
    }
}

fn log_interest(aggregator: Address, topic: B256) -> ReactiveInterest<Ethereum> {
    ReactiveInterest::Logs(LogInterest {
        provider_filter: Filter::new().address(aggregator).event_signature(topic),
        local_matcher: None,
        route_key: Some(RouteKeySpec::EmitterAddress),
    })
}

#[allow(clippy::too_many_arguments)]
fn apply_answer_updated_dependency_storage_effect(
    storage_sync: &OracleStorageSync,
    effects: &mut Vec<ReactiveEffect>,
    needs_aggregator_purge: &mut bool,
    registrations: &[FeedRegistration],
    event: &AnswerUpdated,
    block_number: Option<u64>,
    block_hash: Option<B256>,
    log_index: Option<u64>,
    value_status: OracleValueStatus,
    state: &dyn StateView,
) -> Result<bool, OracleStorageError> {
    let mut direct_candidates = Vec::new();
    let mut fallback_proxies = BTreeSet::new();

    for registration in registrations {
        if registration
            .source
            .supports_direct_chainlink_storage_effects()
        {
            direct_candidates.push(registration);
        } else {
            fallback_proxies.insert(registration.proxy);
        }
    }

    let mut applied_direct_effect = false;
    for registration in direct_candidates.iter().copied() {
        let raw_update = answer_updated_price_update(
            registration,
            event.aggregator,
            event,
            event.current,
            block_number,
            block_hash,
            log_index,
            value_status,
        );
        if apply_state_updates(
            effects,
            storage_sync.state_effect_for_answer(registration, &raw_update, state)?,
        ) {
            applied_direct_effect = true;
            break;
        }
    }

    if !applied_direct_effect {
        fallback_proxies.extend(
            direct_candidates
                .into_iter()
                .map(|registration| registration.proxy),
        );
    }

    append_fallback_invalidations(effects, needs_aggregator_purge, fallback_proxies);
    Ok(applied_direct_effect)
}

fn apply_ocr2_dependency_storage_effect(
    storage_sync: &OracleStorageSync,
    effects: &mut Vec<ReactiveEffect>,
    needs_aggregator_purge: &mut bool,
    registrations: &[FeedRegistration],
    update: &Ocr2TransmissionStorageUpdate,
    state: &dyn StateView,
) -> Result<bool, OracleStorageError> {
    let mut fallback_proxies = BTreeSet::new();

    for registration in registrations {
        if apply_state_updates(
            effects,
            storage_sync.state_effect_for_ocr2_transmission(registration, update, state)?,
        ) {
            return Ok(true);
        }
        fallback_proxies.insert(registration.proxy);
    }

    append_fallback_invalidations(effects, needs_aggregator_purge, fallback_proxies);
    Ok(false)
}

fn apply_ocr1_dependency_storage_effect(
    storage_sync: &OracleStorageSync,
    effects: &mut Vec<ReactiveEffect>,
    needs_aggregator_purge: &mut bool,
    registrations: &[FeedRegistration],
    update: &Ocr1TransmissionStorageUpdate,
    state: &dyn StateView,
) -> Result<bool, OracleStorageError> {
    let mut fallback_proxies = BTreeSet::new();

    for registration in registrations {
        if apply_state_updates(
            effects,
            storage_sync.state_effect_for_ocr1_transmission(registration, update, state)?,
        ) {
            return Ok(true);
        }
        fallback_proxies.insert(registration.proxy);
    }

    append_fallback_invalidations(effects, needs_aggregator_purge, fallback_proxies);
    Ok(false)
}

/// Removed (reorged-out) logs describe values the chain rolled back. Never
/// re-apply them as exact storage writes — purge the affected feed proxies and
/// the emitting aggregator so raw reads refetch canonical state. The paired
/// hooks still fire with [`OracleValueStatus::RequiresRepair`] so trackers
/// mark the affected snapshots for authoritative reconciliation.
fn apply_removed_log_fallback(
    effects: &mut Vec<ReactiveEffect>,
    needs_aggregator_purge: &mut bool,
    registrations: &[FeedRegistration],
) {
    let proxies: BTreeSet<Address> = registrations
        .iter()
        .map(|registration| registration.proxy)
        .collect();
    append_fallback_invalidations(effects, needs_aggregator_purge, proxies);
}

fn apply_state_updates(effects: &mut Vec<ReactiveEffect>, effect: OracleStorageEffect) -> bool {
    match effect {
        OracleStorageEffect::StateUpdates { updates, .. } => {
            effects.extend(updates.into_iter().map(ReactiveEffect::StateUpdate));
            true
        }
        OracleStorageEffect::FallbackPurge => false,
    }
}

fn append_fallback_invalidations(
    effects: &mut Vec<ReactiveEffect>,
    needs_aggregator_purge: &mut bool,
    proxies: BTreeSet<Address>,
) {
    if proxies.is_empty() {
        return;
    }

    *needs_aggregator_purge = true;
    effects.extend(proxies.into_iter().map(|proxy| {
        ReactiveEffect::Invalidate(InvalidationRequest {
            scope: PurgeScope::AllStorage,
            address: proxy,
            reason: InvalidationReason::HandlerRequested,
        })
    }));
}

fn prepend_aggregator_purge(
    effects: &mut Vec<ReactiveEffect>,
    needs_aggregator_purge: bool,
    aggregator: Address,
) {
    if needs_aggregator_purge {
        effects.insert(
            0,
            ReactiveEffect::Invalidate(InvalidationRequest {
                scope: PurgeScope::AllStorage,
                address: aggregator,
                reason: InvalidationReason::HandlerRequested,
            }),
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn answer_updated_price_update(
    registration: &FeedRegistration,
    aggregator: Address,
    event: &AnswerUpdated,
    raw_answer: I256,
    block_number: Option<u64>,
    block_hash: Option<B256>,
    log_index: Option<u64>,
    value_status: OracleValueStatus,
) -> OraclePriceUpdate {
    let round = RoundData {
        round_id: event.round_id,
        answer: raw_answer,
        started_at: event.updated_at,
        updated_at: event.updated_at,
        answered_in_round: event.round_id,
    };
    let round_status = classify_round(&round, event.updated_at, &registration.staleness);
    OraclePriceUpdate {
        id: registration.id.clone(),
        proxy: registration.proxy,
        aggregator,
        label: registration.label.clone(),
        base: registration.base.clone(),
        quote: registration.quote.clone(),
        raw_answer,
        decimals: registration.metadata.decimals,
        event_round_id: event.round_id,
        started_at: event.updated_at,
        updated_at: event.updated_at,
        block_number,
        block_hash,
        log_index,
        round_status,
        value_status,
        source: registration.source.event_value_source(),
    }
}

#[allow(clippy::too_many_arguments)]
fn ocr2_price_update(
    registration: &FeedRegistration,
    aggregator: Address,
    event: &Ocr2NewTransmission,
    transmission_timestamp: u64,
    block_number: Option<u64>,
    block_hash: Option<B256>,
    log_index: Option<u64>,
    value_status: OracleValueStatus,
) -> OraclePriceUpdate {
    let raw_answer = registration
        .source
        .normalize_answer_from_event(Some(aggregator), event.answer);
    let round = RoundData {
        round_id: event.aggregator_round_id,
        answer: raw_answer,
        started_at: event.observations_timestamp,
        updated_at: transmission_timestamp,
        answered_in_round: event.aggregator_round_id,
    };
    let round_status = classify_round(&round, transmission_timestamp, &registration.staleness);
    OraclePriceUpdate {
        id: registration.id.clone(),
        proxy: registration.proxy,
        aggregator,
        label: registration.label.clone(),
        base: registration.base.clone(),
        quote: registration.quote.clone(),
        raw_answer,
        decimals: registration.metadata.decimals,
        event_round_id: event.aggregator_round_id,
        started_at: event.observations_timestamp,
        updated_at: transmission_timestamp,
        block_number,
        block_hash,
        log_index,
        round_status,
        value_status,
        source: registration.source.event_value_source(),
    }
}

#[allow(clippy::too_many_arguments)]
fn ocr1_price_update(
    registration: &FeedRegistration,
    aggregator: Address,
    event: &Ocr1NewTransmission,
    transmission_timestamp: u64,
    block_number: Option<u64>,
    block_hash: Option<B256>,
    log_index: Option<u64>,
    value_status: OracleValueStatus,
) -> OraclePriceUpdate {
    let raw_answer = registration
        .source
        .normalize_answer_from_event(Some(aggregator), event.answer);
    let round = RoundData {
        round_id: event.aggregator_round_id,
        answer: raw_answer,
        started_at: transmission_timestamp,
        updated_at: transmission_timestamp,
        answered_in_round: event.aggregator_round_id,
    };
    let round_status = classify_round(&round, transmission_timestamp, &registration.staleness);
    OraclePriceUpdate {
        id: registration.id.clone(),
        proxy: registration.proxy,
        aggregator,
        label: registration.label.clone(),
        base: registration.base.clone(),
        quote: registration.quote.clone(),
        raw_answer,
        decimals: registration.metadata.decimals,
        event_round_id: event.aggregator_round_id,
        started_at: transmission_timestamp,
        updated_at: transmission_timestamp,
        block_number,
        block_hash,
        log_index,
        round_status,
        value_status,
        source: registration.source.event_value_source(),
    }
}

fn event_value_status(removed: bool) -> OracleValueStatus {
    if removed {
        OracleValueStatus::RequiresRepair
    } else {
        OracleValueStatus::EventPending
    }
}

fn state_effect_quality(applied_direct_effect: bool) -> StateEffectQuality {
    if applied_direct_effect {
        StateEffectQuality::ExactFromInput
    } else {
        StateEffectQuality::RequiresRepair
    }
}

fn hook_tags(registration: &FeedRegistration, aggregator: Address) -> Vec<ReportTag> {
    vec![
        ReportTag::new("feed_id", registration.id.to_string()),
        ReportTag::new("proxy", format!("{:?}", registration.proxy)),
        ReportTag::new("aggregator", format!("{aggregator:?}")),
    ]
}

impl ReactiveHandler<Ethereum> for OracleReactiveHandler {
    fn id(&self) -> HandlerId {
        self.id()
    }

    fn interests(&self) -> Vec<ReactiveInterest<Ethereum>> {
        self.interests()
    }

    fn handle(
        &self,
        ctx: &ReactiveContext,
        input: &ReactiveInput<Ethereum>,
        state: &dyn StateView,
    ) -> Result<HandlerOutcome, HandlerError> {
        let ReactiveInput::Log(log) = input else {
            return Ok(HandlerOutcome::empty(StateEffectQuality::NoStateEffect));
        };
        if log.removed && !matches!(ctx.chain_status, ChainStatus::Reorged { .. }) {
            return Ok(HandlerOutcome::empty(StateEffectQuality::NoStateEffect));
        }

        match log.topics().first().copied() {
            Some(ANSWER_UPDATED_TOPIC) => self.handle_answer_updated(ctx, log, state),
            Some(OCR1_NEW_TRANSMISSION_TOPIC) => self.handle_ocr1_new_transmission(ctx, log, state),
            Some(OCR2_NEW_TRANSMISSION_TOPIC) => self.handle_ocr2_new_transmission(ctx, log, state),
            _ => Ok(HandlerOutcome::empty(StateEffectQuality::NoStateEffect)),
        }
    }
}

struct EmptyStateView;

impl StateView for EmptyStateView {
    fn storage(&self, _address: Address, _slot: U256) -> Option<U256> {
        None
    }
}
