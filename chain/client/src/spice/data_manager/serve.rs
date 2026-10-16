use super::encode::{EncodedData, encode};
use super::{DataId, DataPolicy, SpiceData, SpiceDataManager};
use near_chain::Error;
use near_primitives::spice::partial_data::{SpiceDataCommitment, SpiceDataPart};
use near_primitives::types::AccountId;
use std::collections::{BTreeSet, HashSet};

/// A producer's push of data it produced: its own part, for every recipient.
pub(crate) struct Push {
    pub(crate) recipients: HashSet<AccountId>,
    pub(crate) commitment: SpiceDataCommitment,
    pub(crate) part: SpiceDataPart,
}

/// Outcome of serving one requested item.
#[must_use]
#[derive(Debug)]
pub(crate) enum Served {
    /// The commitment and the requested parts.
    Parts { commitment: SpiceDataCommitment, parts: Vec<SpiceDataPart> },
    /// This node is not a producer of the item.
    NotProducer,
    /// This node has not stored the item's data.
    NotStored,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ServeError {
    #[error("request names a shard outside its layout")]
    UnknownShard,
    #[error("request asks for an ordinal outside the producer set")]
    OrdinalOutsideProducerSet,
    #[error("chain read failed: {0}")]
    Chain(#[from] Error),
}

impl<P: DataPolicy> SpiceDataManager<P> {
    /// The push of `data`, which `me` produced as a producer of `id`.
    pub(crate) fn push(
        &mut self,
        me: &AccountId,
        id: &DataId,
        data: &SpiceData,
    ) -> Result<Push, Error> {
        let producers = self.policies.producers(id)?;
        let recipients = self.policies.recipients(id)?;
        let own_ordinal = producers
            .iter()
            .position(|producer| producer == me)
            .expect("a node pushes only data it produced");
        let EncodedData { commitment, mut parts } =
            encode(&self.encoders.entry(producers.len()), data);
        Ok(Push { recipients, commitment, part: parts.swap_remove(own_ordinal) })
    }

    /// Serves `ordinals` of `id` to a request, re-encoding the data from the store.
    pub(crate) fn serve(
        &mut self,
        me: &AccountId,
        id: &DataId,
        ordinals: &BTreeSet<u64>,
    ) -> Result<Served, ServeError> {
        if !self.policies.names_known_shards(id)? {
            return Err(ServeError::UnknownShard);
        }
        let producers = self.policies.producers(id)?;
        if !producers.contains(me) {
            return Ok(Served::NotProducer);
        }
        if ordinals.last().is_some_and(|highest| *highest >= producers.len() as u64) {
            return Err(ServeError::OrdinalOutsideProducerSet);
        }
        let Some(data) = self.policies.stored_data(id) else {
            return Ok(Served::NotStored);
        };
        let EncodedData { commitment, parts } =
            encode(&self.encoders.entry(producers.len()), &data);
        let parts = parts.into_iter().filter(|part| ordinals.contains(&part.part_ord)).collect();
        Ok(Served::Parts { commitment, parts })
    }

    /// The producers of `id`, in the parts' encoding order.
    pub(crate) fn producers(&self, id: &DataId) -> Result<Vec<AccountId>, Error> {
        self.policies.producers(id)
    }
}
