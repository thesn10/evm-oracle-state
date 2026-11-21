use std::{borrow::Cow, collections::BTreeMap, future::Future, pin::Pin, sync::Arc};

use crate::alloy_network::Ethereum;
use alloy_primitives::{Address, I256};
use evm_fork_cache::{cache::EvmCache, reactive::ReactiveHandler};

use crate::{FeedRegistration, OracleAdapterFeedSkip, OracleError, OracleStorageSync, RoundData};

/// Boxed async result returned by oracle adapter extension points.
pub type AdapterFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, OracleError>> + 'a>>;

/// Stable identifier for an oracle adapter family.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OracleAdapterId(Cow<'static, str>);

impl OracleAdapterId {
    /// Create a stable adapter id.
    pub fn new(id: impl Into<Cow<'static, str>>) -> Self {
        Self(id.into())
    }

    /// Borrow the adapter id string.
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }
}

impl std::fmt::Display for OracleAdapterId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl AsRef<str> for OracleAdapterId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// Open transform descriptor for non-Chainlink-native source adapters.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum OracleTransformDescriptor {
    /// Event/read answers are already in the feed's user-facing denomination.
    #[default]
    Identity,
    /// Clamp answers above a configured cap.
    PriceCap {
        /// Maximum answer exposed to users.
        cap: I256,
    },
    /// Adapter-owned transform identified by adapter and transform kind.
    Custom {
        /// Adapter responsible for interpreting this transform.
        adapter_id: OracleAdapterId,
        /// Adapter-local transform kind.
        kind: Cow<'static, str>,
        /// String metadata for adapter-owned transform parameters.
        metadata: BTreeMap<String, String>,
    },
}

/// Open descriptor for an adapter-owned oracle source.
///
/// Sources built from this descriptor ([`crate::FeedSource::custom`]) default
/// to **Chainlink-style proxy reconciliation**: after every accepted event,
/// and during [`crate::OracleTracker::reconcile`], the tracker calls
/// `latestRoundData()` on [`Self::read_proxy`] and treats the result as
/// authoritative. There is no opt-out flag, so `read_proxy` must point at a
/// contract that answers `latestRoundData()` — for non-Chainlink-shaped
/// sources, point it at a Chainlink-compatible view wrapper.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OracleSourceDescriptor {
    /// Adapter that owns source-specific semantics.
    pub adapter_id: OracleAdapterId,
    /// Adapter-local source kind.
    pub source_kind: Cow<'static, str>,
    /// User-facing oracle/proxy address.
    pub user_facing_proxy: Address,
    /// Address read for authoritative round data. Must answer Chainlink
    /// `latestRoundData()`; reconciliation reads it after every accepted
    /// event (see the struct-level docs).
    pub read_proxy: Address,
    /// Address whose events should drive this feed.
    pub event_source: Address,
    /// User-facing answer transform.
    pub transform: OracleTransformDescriptor,
    /// String metadata for adapter-owned source parameters.
    pub metadata: BTreeMap<String, String>,
}

impl OracleSourceDescriptor {
    /// Create a custom source descriptor with identity answer transform and no metadata.
    pub fn new(
        adapter_id: OracleAdapterId,
        source_kind: impl Into<Cow<'static, str>>,
        user_facing_proxy: Address,
        read_proxy: Address,
        event_source: Address,
    ) -> Self {
        Self {
            adapter_id,
            source_kind: source_kind.into(),
            user_facing_proxy,
            read_proxy,
            event_source,
            transform: OracleTransformDescriptor::Identity,
            metadata: BTreeMap::new(),
        }
    }

    /// Set the source transform descriptor.
    pub fn transform(mut self, transform: OracleTransformDescriptor) -> Self {
        self.transform = transform;
        self
    }

    /// Replace all adapter-owned source metadata.
    pub fn metadata(mut self, metadata: BTreeMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }

    /// Add one adapter-owned metadata entry.
    pub fn metadata_entry(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

/// One feed discovered and seeded by an oracle adapter plugin.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct OracleDiscoveredFeed {
    /// Registration inserted into the oracle tracker.
    pub registration: FeedRegistration,
    /// Initial round used to seed typed state.
    pub round: RoundData,
}

impl OracleDiscoveredFeed {
    /// Create a discovered feed from a registration and initial round.
    pub fn new(registration: FeedRegistration, round: RoundData) -> Self {
        Self {
            registration,
            round,
        }
    }
}

/// Result of adapter discovery.
#[non_exhaustive]
#[derive(Clone, Debug, Default)]
pub struct OracleDiscoveryReport {
    /// Feeds that were registered successfully.
    pub feeds: Vec<OracleDiscoveredFeed>,
    /// Optional feed skips reported by best-effort adapters.
    pub skipped: Vec<OracleAdapterFeedSkip>,
}

impl OracleDiscoveryReport {
    /// Create an empty discovery report.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add one discovered feed to this report.
    pub fn with_feed(mut self, feed: OracleDiscoveredFeed) -> Self {
        self.feeds.push(feed);
        self
    }

    /// Add one skipped feed to this report.
    pub fn with_skip(mut self, skip: OracleAdapterFeedSkip) -> Self {
        self.skipped.push(skip);
        self
    }
}

/// Cache-native discovery context passed to oracle adapter plugins.
pub struct OracleDiscoveryContext<'a> {
    /// Cache used for all on-chain reads during discovery.
    pub cache: &'a mut EvmCache,
    /// Wall-clock timestamp used to classify seeded feed state.
    pub now_timestamp: u64,
}

/// Public plugin interface for oracle families layered over `evm-fork-cache`.
pub trait OracleAdapterPlugin: Send + Sync {
    /// Stable adapter id.
    fn adapter_id(&self) -> OracleAdapterId;

    /// Discover and seed feed registrations through an `EvmCache`.
    fn discover<'a>(
        &'a self,
        ctx: OracleDiscoveryContext<'a>,
    ) -> AdapterFuture<'a, OracleDiscoveryReport>;

    /// Build the reactive handler that owns this adapter's event routing.
    fn reactive_handler(
        &self,
        registrations: Vec<FeedRegistration>,
        storage_sync: OracleStorageSync,
    ) -> Arc<dyn ReactiveHandler<Ethereum>>;
}
