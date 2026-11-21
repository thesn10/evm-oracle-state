//! Oracle-family adapters for speculative transport candidates.

use std::collections::BTreeSet;

use alloy_primitives::{Address, keccak256};
use crate::alloy_rpc_types_eth::Log;

use crate::{FeedRegistration, FeedSource, OracleAdapterId, decode_ocr2_new_transmission};

use super::{
    CHAINLINK_FORWARD_SELECTOR, CHAINLINK_SVR_FORWARD_SELECTOR,
    CHAINLINK_TRANSMIT_SECONDARY_SELECTOR, CHAINLINK_TRANSMIT_SELECTOR, PendingOracleEvidence,
    PendingOracleObserveOutcome, PendingOracleOrderingHandle, PendingOracleSimulationMaterial,
    PendingOracleTransmission, PendingOracleUpdate, PendingOracleValue, PendingTransportCandidate,
    decode_chainlink_ocr2_calldata,
};

/// Coarse upstream filter requested by one pending oracle adapter.
///
/// An empty address set means any destination. Sources may use these interests
/// to reduce traffic, but adapters still validate every submitted candidate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingOracleInterest {
    /// Adapter requesting this interest.
    pub adapter_id: OracleAdapterId,
    /// Eligible outer transaction destinations, or empty for any destination.
    pub addresses: BTreeSet<Address>,
    /// Eligible four-byte calldata selectors.
    pub selectors: BTreeSet<[u8; 4]>,
}

impl PendingOracleInterest {
    /// Construct an interest from destination and selector iterators.
    pub fn new(
        adapter_id: OracleAdapterId,
        addresses: impl IntoIterator<Item = Address>,
        selectors: impl IntoIterator<Item = [u8; 4]>,
    ) -> Self {
        Self {
            adapter_id,
            addresses: addresses.into_iter().collect(),
            selectors: selectors.into_iter().collect(),
        }
    }

    /// Return whether this interest accepts the candidate's outer call.
    pub fn matches(&self, candidate: &PendingTransportCandidate) -> bool {
        let Some(selector) = candidate.calldata.get(..4) else {
            return false;
        };
        (self.addresses.is_empty() || self.addresses.contains(&candidate.to))
            && self
                .selectors
                .iter()
                .any(|expected| selector == expected.as_slice())
    }
}

/// Adapter-owned pending decode failure.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PendingOracleAdapterError {
    /// The candidate came from a different chain than the configured runtime.
    #[error("pending candidate is for chain {observed}, expected {expected}")]
    WrongChain {
        /// Configured chain id.
        expected: u64,
        /// Candidate chain id.
        observed: u64,
    },
    /// The candidate's source family was not enabled.
    #[error("pending source {0:?} is not enabled")]
    SourceDisabled(super::PendingOracleSource),
    /// An oracle-family decoder rejected otherwise matching calldata.
    #[error("pending adapter {adapter_id} failed to decode candidate: {message}")]
    Decode {
        /// Adapter that rejected the candidate.
        adapter_id: OracleAdapterId,
        /// Stable human-readable diagnostic.
        message: String,
    },
}

impl PendingOracleAdapterError {
    /// Construct an adapter decode error.
    pub fn new(adapter_id: OracleAdapterId, message: impl Into<String>) -> Self {
        Self::Decode {
            adapter_id,
            message: message.into(),
        }
    }
}

/// Result of recording one adapter-decoded update.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingOracleAdapterObservation {
    /// Adapter that decoded the update.
    pub adapter_id: OracleAdapterId,
    /// Stable update identity returned by that adapter.
    pub update_id: super::PendingOracleUpdateId,
    /// Pending tracker outcome.
    pub outcome: PendingOracleObserveOutcome,
}

/// One adapter-local failure recorded while other adapters continued routing.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingOracleAdapterFailure {
    /// Adapter that failed to decode the candidate.
    pub adapter_id: OracleAdapterId,
    /// Adapter-provided failure.
    pub error: PendingOracleAdapterError,
}

/// Result of routing one candidate through every interested pending adapter.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PendingOracleCandidateReport {
    /// Successfully decoded and recorded observations.
    pub observations: Vec<PendingOracleAdapterObservation>,
    /// Adapter-local failures that did not block other adapters.
    pub failures: Vec<PendingOracleAdapterFailure>,
}

/// Oracle-family extension point for pre-inclusion candidates.
pub trait PendingOracleAdapter: Send + Sync + 'static {
    /// Stable adapter identity.
    fn adapter_id(&self) -> OracleAdapterId;

    /// Describe transport-level filters for the current registered feed scope.
    fn interests(&self, registrations: &[FeedRegistration]) -> Vec<PendingOracleInterest>;

    /// Decode zero or more updates from one transport candidate.
    fn decode(
        &self,
        candidate: &PendingTransportCandidate,
        registrations: &[FeedRegistration],
    ) -> Result<Vec<PendingOracleUpdate>, PendingOracleAdapterError>;

    /// Return whether a committed log is the oracle-family confirmation for an update.
    fn confirms(&self, _update: &PendingOracleUpdate, _log: &Log) -> bool {
        false
    }
}

/// Pending adapter for direct and forwarded Chainlink OCR2 reports.
#[derive(Clone, Copy, Debug, Default)]
pub struct ChainlinkPendingAdapter;

impl ChainlinkPendingAdapter {
    /// Stable id used by the built-in Chainlink pending adapter.
    pub const ID: &'static str = "chainlink-ocr2";
}

impl PendingOracleAdapter for ChainlinkPendingAdapter {
    fn adapter_id(&self) -> OracleAdapterId {
        OracleAdapterId::new(Self::ID)
    }

    fn interests(&self, registrations: &[FeedRegistration]) -> Vec<PendingOracleInterest> {
        let aggregators = registrations
            .iter()
            .filter(|registration| matches!(&registration.source, FeedSource::Chainlink))
            .filter_map(|registration| registration.current_aggregator);
        let aggregators = aggregators.collect::<BTreeSet<_>>();
        if aggregators.is_empty() {
            return Vec::new();
        }
        vec![
            PendingOracleInterest::new(
                self.adapter_id(),
                aggregators,
                [
                    CHAINLINK_TRANSMIT_SELECTOR,
                    CHAINLINK_TRANSMIT_SECONDARY_SELECTOR,
                ],
            ),
            PendingOracleInterest::new(
                self.adapter_id(),
                [],
                [CHAINLINK_FORWARD_SELECTOR, CHAINLINK_SVR_FORWARD_SELECTOR],
            ),
        ]
    }

    fn decode(
        &self,
        candidate: &PendingTransportCandidate,
        registrations: &[FeedRegistration],
    ) -> Result<Vec<PendingOracleUpdate>, PendingOracleAdapterError> {
        let Some(decoded) = decode_chainlink_ocr2_calldata(candidate.to, &candidate.calldata)
            .map_err(|error| {
                PendingOracleAdapterError::new(self.adapter_id(), error.to_string())
            })?
        else {
            return Ok(Vec::new());
        };
        let feed_ids = registrations
            .iter()
            .filter(|registration| {
                matches!(&registration.source, FeedSource::Chainlink)
                    && registration.current_aggregator == Some(decoded.report.aggregator)
            })
            .map(|registration| registration.id.clone())
            .collect::<Vec<_>>();
        if feed_ids.is_empty() {
            return Ok(Vec::new());
        }

        let simulation = match &candidate.ordering {
            PendingOracleOrderingHandle::RawEthereumTransaction {
                tx_hash,
                signed_envelope,
            } if keccak256(signed_envelope) == *tx_hash => {
                PendingOracleSimulationMaterial::ExactSignedTransaction
            }
            _ => PendingOracleSimulationMaterial::AnswerOnlyProjection,
        };
        let transmission = PendingOracleTransmission::new(
            candidate.id,
            candidate.source_id.clone(),
            decoded.route.clone(),
            candidate.ordering.clone(),
            simulation,
            decoded.report.calldata_hash,
            candidate.observed_at_head,
        );
        Ok(vec![
            PendingOracleUpdate::new(
                decoded.update_id(),
                feed_ids,
                PendingOracleEvidence::ChainlinkReport(decoded.report.clone()),
            )
            .with_proposed_value(PendingOracleValue {
                raw_answer: decoded.report.median_answer,
                observed_at: decoded.report.observations_timestamp,
            })
            .with_transmission(transmission),
        ])
    }

    fn confirms(&self, update: &PendingOracleUpdate, log: &Log) -> bool {
        let PendingOracleEvidence::ChainlinkReport(report) = &update.evidence else {
            return false;
        };
        decode_ocr2_new_transmission(log).is_ok_and(|confirmed| {
            !confirmed.removed
                && confirmed.aggregator == report.aggregator
                && confirmed.answer == report.median_answer
                && confirmed.observations_timestamp == report.observations_timestamp
        })
    }
}

pub(crate) fn adapter_owns_update(
    adapter_id: &OracleAdapterId,
    update: &PendingOracleUpdate,
) -> bool {
    match &update.evidence {
        PendingOracleEvidence::ChainlinkReport(_) => {
            adapter_id.as_str() == ChainlinkPendingAdapter::ID
        }
        PendingOracleEvidence::Adapter {
            adapter_id: owner, ..
        } => owner == adapter_id,
    }
}
