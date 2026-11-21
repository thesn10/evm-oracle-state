//! A real-time oracle state engine built on a forked-EVM state cache
//! ([`evm_fork_cache`]).
//!
//! `evm-oracle-state` is the oracle domain crate layered over
//! [`evm_fork_cache`]: the companion crate owns generic cache invalidation,
//! reactive batching, and reorg reporting, while this crate owns
//! oracle-specific metadata, event decoding, typed snapshots, staleness
//! policy, reconciliation, and feed lookup. Chainlink-compatible feeds are
//! registered through their proxy addresses; adapter-owned oracle families
//! (Aave, Morpho, Euler, Pyth, RedStone) plug into the same typed state,
//! hook, and overlay surfaces behind feature flags.
//!
//! The central contract:
//!
//! > Events are fast-path hints; proxy reads are authoritative.
//!
//! Oracle log events update typed state immediately, so simulations can
//! observe a new answer with no RPC on the hot path; authoritative proxy
//! reads confirm, correct, or repair that state afterwards:
//!
//! ```text
//! oracle log event (AnswerUpdated / OCR1+OCR2 NewTransmission / adapter event)
//!   -> reactive handler decodes it; an OraclePriceUpdate hook fires immediately
//!   -> OracleTracker stores an EventPending typed snapshot
//!   -> OracleReadOverlay serves registered oracle view calls from typed state
//!   -> OracleReconciler reads the proxy latestRoundData()
//!   -> the snapshot becomes Confirmed or Corrected (or Stale / RequiresRepair)
//! ```
//!
//! # Main types
//!
//! The internal modules are private; everything below is re-exported at the
//! crate root:
//!
//! - [`OracleRuntime`] — the high-level runtime, built from a provider
//!   ([`OracleRuntime::builder`]) or an existing cache
//!   ([`OracleRuntime::cache_builder`]): [`ChainlinkFeed`] registrations,
//!   typed event callbacks, storage/code warmup, and the reactive
//!   install / refresh / uninstall lifecycle.
//! - [`OracleAdapter`] — the cache-native registration path over an
//!   [`evm_fork_cache::cache::EvmCache`]: [`Feed`] builders,
//!   Multicall3-batched discovery reads, and skip-aware build reports.
//! - [`OracleTracker`] — the typed state store: [`OracleSnapshot`] lookup
//!   with freshness classification, [`OraclePriceUpdate`] hook payloads, and
//!   reconciliation through [`OracleReconciler`].
//! - [`OracleRegistry`] — feed registrations ([`FeedRegistration`],
//!   [`FeedId`]), aggregator and dependency bookkeeping, and feed-readiness
//!   reporting.
//! - [`OracleReactiveHandler`] — dependency-aware log routing bridged into
//!   the [`evm_fork_cache`] reactive runtime; detected OCR1/OCR2 layouts get
//!   exact direct storage writes, everything else takes the conservative
//!   purge/refetch fallback.
//! - [`OracleReadOverlay`] — serves registered oracle view calls
//!   (`latestRoundData()`, adapter reads such as Morpho `price()`, Euler
//!   `getQuote(...)`, Pyth `getPriceUnsafe(...)`) from typed state, so
//!   simulations see event prices before a follow-up RPC read completes.
//! - [`PricePolicy`] / [`CheckedPrice`] — the typed price vocabulary
//!   ([`AssetId`], [`Denomination`], [`TokenAmount`]): promote raw oracle
//!   answers into policy-vetted values before liquidation or valuation
//!   decisions.
//! - [`OracleAdapterPlugin`] — the extension seam for oracle families that
//!   are not plain Chainlink proxies: adapter-owned discovery plus an
//!   adapter-owned reactive handler over the same runtime.
//! - Feature-gated adapters — [`AaveV3OracleAdapter`] (`aave`),
//!   [`MorphoBlueOracleAdapter`] (`morpho`), [`EulerOracleAdapter`]
//!   (`euler`), [`PythOracleAdapter`] (`pyth`), and
//!   [`RedstoneOracleAdapter`] (`redstone`).
//! - [`PendingOracleRuntime`] (`pending-oracle-updates`) — an independent,
//!   opt-in speculative channel over the existing feed registry, with pluggable
//!   oracle decoders and transport sources. It never mutates canonical tracker
//!   state or confirmed hooks.
//!
//! See the crate's `examples/local_oracle_lifecycle.rs` for the
//! compile-checked quickstart: a deterministic, RPC-free walk through
//! register → synthetic event → hook → overlay read → reconciliation.

#![warn(missing_docs)]
// docs.rs builds with `--cfg docsrs` (see `[package.metadata.docs.rs]`), which
// enables nightly `doc_cfg`: every feature-gated item renders with its
// "Available on crate feature … only" badge, derived automatically from the
// existing `#[cfg]`s with no per-item annotations (auto-cfg is part of
// `doc_cfg` since 1.92, rust-lang/rust#138907). Inert on stable builds.
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(all(feature = "alloy1", feature = "alloy2"))]
compile_error!("features `alloy1` and `alloy2` are mutually exclusive; pass `--no-default-features --features alloy2`");

#[cfg(not(any(feature = "alloy1", feature = "alloy2")))]
compile_error!("exactly one of features `alloy1` or `alloy2` must be enabled");

/// Re-export of [`alloy_primitives`]: the `Address` / `U256` / `B256` / `Log`
/// vocabulary this crate's API speaks.
///
/// Import from here (`evm_oracle_state::alloy_primitives`) to use exactly the
/// version this crate's signatures expect without pinning `alloy-primitives`
/// yourself.
pub use alloy_primitives;
/// Re-export of the [`evm_fork_cache`] companion crate: `EvmCache`, the
/// reactive runtime, storage programs, and the typed errors that appear on
/// this crate's cache and reactive seams.
///
/// `evm-fork-cache` is a 0.x **public dependency**: a semver-breaking bump
/// there (e.g. 0.2 → 0.3) is necessarily a breaking release of this crate too,
/// and the two are released in lockstep. Importing it through this re-export
/// (`evm_oracle_state::evm_fork_cache`) guarantees the versions match.
pub use evm_fork_cache;
pub use evm_fork_cache::{
    alloy_consensus, alloy_eips, alloy_network, alloy_provider, alloy_pubsub, alloy_rpc_client,
    alloy_rpc_types_eth, revm,
};

#[cfg(feature = "aave")]
mod aave;
mod adapter;
mod cache_reader;
mod code;
mod config;
mod derived;
mod error;
#[cfg(feature = "euler")]
mod euler;
mod events;
mod feed;
mod layout;
mod mocking;
#[cfg(feature = "morpho")]
mod morpho;
mod overlay;
#[cfg(feature = "pending-oracle-updates")]
mod pending;
mod price;
mod provider;
#[cfg(feature = "pyth")]
mod pyth;
mod reactive;
#[cfg(feature = "redstone")]
mod redstone;
mod registry;
mod report;
mod runtime;
mod signal;
mod state;
mod storage;
mod tracker;
mod types;

#[cfg(feature = "aave")]
pub use aave::{AaveAsset, AaveV3OracleAdapter};
pub use adapter::{
    AdapterFuture, OracleAdapterId, OracleAdapterPlugin, OracleDiscoveredFeed,
    OracleDiscoveryContext, OracleDiscoveryReport, OracleSourceDescriptor,
    OracleTransformDescriptor,
};
pub use cache_reader::{
    EvmCacheChainlinkReader, OracleAdapter, OracleAdapterBuildReport, OracleAdapterBuilder,
    OracleAdapterFeedSkip, OracleAdapterSkipReason, OracleFeedSkip,
};
pub use code::{
    OracleCodeInstall, OracleCodeInstallError, OracleCodeInstallMode, OracleCodeMismatch,
    OracleCodePolicyViolation, OracleCodeRegistry, OracleCodeSeed, OracleCodeUnverifiable,
    OracleCodeWarmupPolicy, OracleCodeWarmupReport,
};
pub use config::{FeedConfig, StalenessPolicy};
pub use derived::OracleDerivedReader;
pub use error::{
    ChainlinkEventDecodeError, OracleConfigError, OracleError, OracleSignalDecodeError,
};
#[cfg(feature = "euler")]
pub use euler::{EulerOracleAdapter, EulerQuotePair};
pub use events::{
    ANSWER_UPDATED_TOPIC, AnswerUpdated, NEW_ROUND_TOPIC, NewRound, OCR1_NEW_TRANSMISSION_TOPIC,
    OCR2_NEW_TRANSMISSION_TOPIC, Ocr1NewTransmission, Ocr2NewTransmission, decode_answer_updated,
    decode_new_round, decode_ocr1_new_transmission, decode_ocr2_new_transmission,
};
#[cfg(feature = "pyth")]
pub use events::{
    PYTH_PRICE_FEED_UPDATE_TOPIC, PythPriceFeedUpdate, decode_pyth_price_feed_update,
};
#[cfg(feature = "redstone")]
pub use events::{REDSTONE_VALUE_UPDATE_TOPIC, RedstoneValueUpdate, decode_redstone_value_update};
pub use feed::{Feed, FeedAccess, FeedBuilder, FeedBuilderState, FeedReady};
pub use layout::{
    AggregatorLayout, AggregatorLayoutConfidence, AggregatorLayoutEvidence,
    classify_type_and_version,
};
pub use mocking::{OracleMockError, OracleMockReport, OracleOverlayMockExt, OraclePriceMock};
#[cfg(feature = "morpho")]
pub use morpho::{MorphoBlueMarket, MorphoBlueOracleAdapter, MorphoMarketParams};
pub use overlay::OracleReadOverlay;
#[cfg(feature = "pending-oracle-ethereum")]
pub use pending::AlchemyPendingTransactionSource;
#[cfg(feature = "pending-oracle-mev-share")]
pub use pending::MevSharePendingTransactionSource;
#[cfg(feature = "pending-oracle-updates")]
pub use pending::{
    CHAINLINK_FORWARD_SELECTOR, CHAINLINK_TRANSMIT_SECONDARY_SELECTOR, CHAINLINK_TRANSMIT_SELECTOR,
    ChainlinkPendingAdapter, ChainlinkPendingReport, DecodedChainlinkOcr2,
    ETHEREUM_MAINNET_CHAIN_ID, PendingFeedFilter, PendingOracleAdapter, PendingOracleAdapterError,
    PendingOracleAdapterFailure, PendingOracleAdapterObservation, PendingOracleCandidateOutcome,
    PendingOracleCandidateReport, PendingOracleCandidateSource, PendingOracleConfig,
    PendingOracleCoverageGap, PendingOracleDecodeError, PendingOracleEvent, PendingOracleEvidence,
    PendingOracleInterest, PendingOracleObserveOutcome, PendingOracleOrderingHandle,
    PendingOraclePublisher, PendingOracleRecvError, PendingOracleResolution,
    PendingOracleResolutionKind, PendingOracleResolveOutcome, PendingOracleRoute,
    PendingOracleRuntime, PendingOracleSimulationFidelity, PendingOracleSimulationMaterial,
    PendingOracleSource, PendingOracleSourceDescriptor, PendingOracleSourceError,
    PendingOracleSourceFuture, PendingOracleSourceHealth, PendingOracleSourceId,
    PendingOracleSourceSession, PendingOracleSourceSink, PendingOracleSourceState,
    PendingOracleStatus, PendingOracleStreamEvent, PendingOracleSubscription,
    PendingOracleTransmission, PendingOracleTransmissionId, PendingOracleTransmissionStatus,
    PendingOracleUpdate, PendingOracleUpdateId, PendingOracleValue, PendingTransportCandidate,
    decode_chainlink_ocr2_calldata,
};
pub use price::{CheckedPrice, OraclePrice, PricePolicy, PricePolicyViolation};
pub use provider::{ChainlinkFeedProvider, OracleBlockRef, ProviderFuture};
#[cfg(feature = "pyth")]
pub use pyth::{PythFeed, PythOracleAdapter, PythReactiveHandler};
pub use reactive::OracleReactiveHandler;
#[cfg(feature = "redstone")]
pub use redstone::{
    RedstoneFeed, RedstoneMultiFeedStorageAdapter, RedstoneOracleAdapter,
    RedstonePriceFeedsStorageAdapter, RedstoneReactiveHandler,
};
pub use registry::{
    AggregatorChange, EulerQuoteLeg, FeedId, FeedRegistration, FeedSource, MorphoChainlinkFeed,
    MorphoChainlinkFeedRole, OracleDependency, OracleFeedReadinessReport, OracleRegistry,
};
pub use report::{
    OracleBatchReport, OracleChangeImpact, OracleFeedChange, OracleFeedChangeKind, OracleIncident,
};
pub use runtime::{
    ChainlinkFeed, OracleCacheRuntimeBuildReport, OracleCacheRuntimeBuilder,
    OracleColdStartWarmupReport, OracleReactiveInstallReport, OracleReactiveRefreshReport,
    OracleReactiveUninstallReport, OracleRuntime, OracleRuntimeBuilder,
    OracleRuntimeMutationReport, OracleStorageWarmupFailure, OracleStorageWarmupMode,
    OracleStorageWarmupReport,
};
pub use signal::{
    ORACLE_LEGACY_ANSWER_UPDATED_KIND, ORACLE_SIGNAL_NAMESPACE, OracleSignal, OracleSignalKind,
};
pub use state::{
    FeedMetadata, OracleFeedStatus, OracleRoundStatus, OracleSnapshot, OracleValueSource,
    OracleValueStatus, RoundData,
};
pub use storage::{
    ChainlinkOcr1StorageAdapter, ChainlinkOcr2StorageAdapter, Ocr1TransmissionStorageUpdate,
    Ocr2TransmissionStorageUpdate, OracleStorageAdapter, OracleStorageEffect, OracleStorageError,
    OracleStorageSync,
};
pub use tracker::{
    DerivedReconcileFailure, DerivedReconcileReport, OracleAggregatorChanged, OracleHookEvent,
    OraclePriceConfirmed, OraclePriceCorrected, OraclePriceStale, OraclePriceUpdate,
    OracleReconciler, OracleReconciliationKind, OracleReconciliationRequest,
    OracleReconciliationResult, OracleTracker, OracleUpdate, ReconcileReport,
};
pub use types::{AssetId, Denomination, TokenAmount, ValuedAmount};

/// Compiles the README's code samples as doctests so the documented snippets
/// cannot drift from the real API. Exists only under `cfg(doctest)` — it is
/// never part of the built crate or the rendered docs. Gated on every adapter
/// feature because the README demonstrates the Aave / Morpho / Euler / Pyth /
/// RedStone surfaces; the adapter-features test run (local + CI) compiles it.
#[cfg(all(
    doctest,
    feature = "aave",
    feature = "morpho",
    feature = "euler",
    feature = "pyth",
    feature = "redstone"
))]
#[doc = include_str!("../README.md")]
pub struct ReadmeDoctests;
