use primitives::traits::WitnessSelector;
use primitives::types::UID;
use rand::thread_rng;
use rand::prelude::IteratorRandom;
use std::collections::HashSet;

/// Witness selector used by the beacon chain where each new block index has a new set of witnesses.
pub struct BeaconWitnessSelector {
    witnesses: HashSet<UID>,
    witnesses_ordered: Vec<UID>,
    /// Witnesses without `owner_uid`.
    other_witnesses: Vec<UID>,
}

impl BeaconWitnessSelector {
    pub fn new(witnesses: HashSet<UID>, owner_uid: UID) -> Self {
        let mut witnesses_ordered : Vec<_> = (&witnesses).into_iter().cloned().collect();
        witnesses_ordered.sort();
        let other_witnesses : Vec<_> = (&witnesses).into_iter().filter_map(|w|
                if w == &owner_uid {
                    None } else {
                    Some(*w)
                }
            ).collect();

        Self {
            witnesses,
            witnesses_ordered,
            other_witnesses,
        }
    }

    #[inline]
    fn num_witnesses(&self) -> usize {
        self.witnesses.len()
    }
}

impl WitnessSelector for BeaconWitnessSelector {
    /// On beacon chain we use the same set of witnesses for the entire beacon block.
    #[inline]
    fn epoch_witnesses(&self, _epoch: u64) -> &HashSet<UID> {
        &self.witnesses
    }

    #[inline]
    fn epoch_leader(&self, epoch: u64) -> UID {
        self.witnesses_ordered[epoch as usize % self.num_witnesses()]
    }

    fn random_witnesses(&self, _epoch: u64, sample_size: usize) -> HashSet<UID> {
        self.other_witnesses
            .iter()
            .choose_multiple(&mut thread_rng(), sample_size)
            .into_iter()
            .cloned()
            .collect()
    }
}
