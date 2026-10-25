use super::encode::{EncodedData, encode};
use super::{DATA_PARTS_RATIO, DataId, ServePolicies, ServePolicy, SpiceData};
use itertools::Itertools as _;
use near_chain::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::reed_solomon::ReedSolomonEncoderCache;
use near_primitives::spice::partial_data::{SpiceDataCommitment, SpiceDataPart};
use near_primitives::types::AccountId;
use near_store::adapter::chain_store::ChainStoreAdapter;
use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;
use strum::IntoStaticStr;

/// A producer's push of data it produced: its own part, for every recipient.
pub(crate) struct Push {
    /// The nodes the push goes to; none of them is a producer.
    pub(crate) recipients: HashSet<AccountId>,
    pub(crate) commitment: SpiceDataCommitment,
    pub(crate) part: SpiceDataPart,
}

/// Outcome of serving one well-formed requested item.
/// An honest requester can get any of these.
#[must_use]
#[derive(Debug)]
pub(crate) enum ServeOutcome {
    /// The commitment and the requested parts.
    Parts { commitment: SpiceDataCommitment, parts: Vec<SpiceDataPart> },
    /// This node has not stored the item's data.
    NotStored,
}

/// Why an inbound data request was rejected as malformed. A peer cannot produce any of these
/// without violating the request grammar, so each is attributable to the sender.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, IntoStaticStr, thiserror::Error)]
#[strum(serialize_all = "snake_case")]
pub enum MalformedDataRequest {
    #[error("no entries")]
    NoEntries,
    #[error("too many entries")]
    TooManyEntries,
    #[error("entry without ordinals")]
    EntryWithoutOrdinals,
    #[error("too many ordinals")]
    TooManyOrdinals,
    #[error("ordinal outside the producer set")]
    OrdinalOutsideProducerSet,
    #[error("shard id outside the shard layout")]
    UnknownShard,
    #[error("asked a node that does not produce the data")]
    NotAProducer,
}

impl MalformedDataRequest {
    pub fn as_str(self) -> &'static str {
        self.into()
    }
}

/// Why serving one requested item failed: malformed request, or a read on this node failed.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ServeError {
    #[error("malformed request: {0}")]
    Requester(#[from] MalformedDataRequest),
    #[error("chain read failed: {0}")]
    Chain(#[from] Error),
}

/// Computes the parts this node sends for the data it produces: its own part for a push,
/// the requested parts for a pull. Encodes the data on each call.
pub(crate) struct SpiceDataServer<P: ServePolicy = ServePolicies> {
    encoders: ReedSolomonEncoderCache,
    chain_store: ChainStoreAdapter,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    policies: P,
}

impl<P: ServePolicy> SpiceDataServer<P> {
    pub(crate) fn new(
        chain_store: ChainStoreAdapter,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        policies: P,
    ) -> Self {
        Self {
            encoders: ReedSolomonEncoderCache::new(DATA_PARTS_RATIO),
            chain_store,
            epoch_manager,
            policies,
        }
    }

    /// The push of `data`, which `me` produced as a producer of `id`.
    pub(crate) fn build_push(
        &mut self,
        me: &AccountId,
        id: &DataId,
        data: &SpiceData,
    ) -> Result<Push, Error> {
        let producers = self.policies.producers(id)?;
        let recipients = self
            .policies
            .recipients(id)?
            .into_iter()
            .filter(|account| !producers.contains(account))
            .collect();
        let own_ordinal = producers
            .iter()
            .position(|producer| producer == me)
            .expect("a node pushes only data it produced");
        let EncodedData { commitment, mut parts } =
            encode(&self.encoders.entry(producers.len()), data);
        Ok(Push { recipients, commitment, part: parts.swap_remove(own_ordinal) })
    }

    /// The requested `ordinals` of `id`, re-encoded from the stored data.
    pub(crate) fn requested_parts(
        &mut self,
        me: &AccountId,
        id: &DataId,
        ordinals: &BTreeSet<u64>,
    ) -> Result<ServeOutcome, ServeError> {
        let header = self.chain_store.get_block_header(id.block_hash())?;
        let shard_layout =
            self.epoch_manager.get_shard_layout(header.epoch_id()).map_err(Error::from)?;
        if !id.shard_endpoints().iter().all(|shard_id| shard_layout.shard_ids().contains(shard_id))
        {
            return Err(MalformedDataRequest::UnknownShard.into());
        }
        let producers = self.policies.producers(id)?;
        if !producers.contains(me) {
            return Err(MalformedDataRequest::NotAProducer.into());
        }
        if ordinals.last().is_some_and(|highest| *highest >= producers.len() as u64) {
            return Err(MalformedDataRequest::OrdinalOutsideProducerSet.into());
        }
        let Some(data) = self.policies.stored_data(id) else {
            return Ok(ServeOutcome::NotStored);
        };
        let EncodedData { commitment, parts } =
            encode(&self.encoders.entry(producers.len()), &data);
        let parts = parts.into_iter().filter(|part| ordinals.contains(&part.part_ord)).collect();
        Ok(ServeOutcome::Parts { commitment, parts })
    }
}
