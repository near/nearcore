use super::{
    extend_with_cache, prune_index, read_with_cache, write_with_cache, BlockChainStorage,
    GenericStorage, CACHE_SIZE, COL_ACCEPTED_AUTHORITY, COL_PARTICIPATION, COL_PROCESSED_BLOCKS,
    COL_PROPOSAL, COL_THRESHOLD,
};
use crate::storages::ChainId;
use crate::KeyValueDB;
use lru::LruCache;
use primitives::beacon::{SignedBeaconBlock, SignedBeaconBlockHeader};
use primitives::types::{AuthorityMask, AuthorityStake, Epoch, Slot};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Beacon chain does not require additional behavior besides storing and retrieving blocks. Later,
/// we can add authority info.
pub struct BeaconChainStorage {
    generic_storage: BlockChainStorage<SignedBeaconBlockHeader, SignedBeaconBlock>,
    /// Proposals per slot in which they occur.
    proposals: LruCache<Vec<u8>, Vec<AuthorityStake>>,
    /// Participation of authorities per slot in which they have happened.
    participation: LruCache<Vec<u8>, AuthorityMask>,
    /// Records the blocks that it processed for the given epochs.
    processed_blocks: LruCache<Vec<u8>, HashSet<Slot>>,

    // The following is derived information which we do not want to recompute.
    /// Computed thresholds for each epoch.
    thresholds: LruCache<Vec<u8>, u64>,
    /// Authorities that were accepted for the given slots.
    accepted_authorities: LruCache<Vec<u8>, Vec<AuthorityStake>>,
}

impl GenericStorage<SignedBeaconBlockHeader, SignedBeaconBlock> for BeaconChainStorage {
    #[inline]
    fn blockchain_storage_mut(
        &mut self,
    ) -> &mut BlockChainStorage<SignedBeaconBlockHeader, SignedBeaconBlock> {
        &mut self.generic_storage
    }
}

impl BeaconChainStorage {
    pub fn new(storage: Arc<KeyValueDB>) -> Self {
        Self {
            generic_storage: BlockChainStorage::new(storage, ChainId::BeaconChain),
            proposals: LruCache::new(CACHE_SIZE),
            participation: LruCache::new(CACHE_SIZE),
            processed_blocks: LruCache::new(CACHE_SIZE),
            thresholds: LruCache::new(CACHE_SIZE),
            accepted_authorities: LruCache::new(CACHE_SIZE),
        }
    }

    /// whether there is authority info in storage
    pub fn is_authority_empty(&self) -> bool {
        let proposals: HashMap<_, _> =
            self.generic_storage.storage.iter(Some(COL_PROPOSAL)).collect();
        proposals.is_empty()
    }

    pub fn prune_authority_storage(
        &mut self,
        slot_filter: &Fn(Slot) -> bool,
        epoch_filter: &Fn(Epoch) -> bool,
    ) {
        prune_index(
            self.generic_storage.storage.as_ref(),
            COL_PROPOSAL,
            &mut self.proposals,
            slot_filter,
        )
        .expect("Failed to prune storage");
        prune_index(
            self.generic_storage.storage.as_ref(),
            COL_PARTICIPATION,
            &mut self.participation,
            slot_filter,
        )
        .expect("Failed to prune storage");
        prune_index(
            self.generic_storage.storage.as_ref(),
            COL_PROCESSED_BLOCKS,
            &mut self.processed_blocks,
            epoch_filter,
        )
        .expect("Failed to prune storage");
        prune_index(
            self.generic_storage.storage.as_ref(),
            COL_THRESHOLD,
            &mut self.thresholds,
            epoch_filter,
        )
        .expect("Failed to prune storage");
        prune_index(
            self.generic_storage.storage.as_ref(),
            COL_ACCEPTED_AUTHORITY,
            &mut self.accepted_authorities,
            slot_filter,
        )
        .expect("Failed to prune storage");
    }

    pub fn get_proposal(&mut self, slot: Slot) -> Option<&Vec<AuthorityStake>> {
        read_with_cache(
            self.generic_storage.storage.as_ref(),
            COL_PROPOSAL,
            &mut self.proposals,
            &self.generic_storage.enc_index(slot),
        )
        .expect("Failed to read from storage")
    }

    pub fn set_proposal(&mut self, slot: Slot, proposal: Vec<AuthorityStake>) {
        write_with_cache(
            self.generic_storage.storage.as_ref(),
            COL_PROPOSAL,
            &mut self.proposals,
            &self.generic_storage.enc_index(slot),
            proposal,
        )
        .expect("Failed to write to storage")
    }

    pub fn get_participation(&mut self, slot: Slot) -> Option<&AuthorityMask> {
        read_with_cache(
            self.generic_storage.storage.as_ref(),
            COL_PARTICIPATION,
            &mut self.participation,
            &self.generic_storage.enc_index(slot),
        )
        .expect("Failed to read from storage")
    }

    pub fn set_participation(&mut self, slot: Slot, mask: AuthorityMask) {
        write_with_cache(
            self.generic_storage.storage.as_ref(),
            COL_PARTICIPATION,
            &mut self.participation,
            &self.generic_storage.enc_index(slot),
            mask,
        )
        .expect("Failed to write to storage")
    }

    pub fn get_processed_blocks(&mut self, epoch: Epoch) -> Option<&HashSet<Slot>> {
        read_with_cache(
            self.generic_storage.storage.as_ref(),
            COL_PROCESSED_BLOCKS,
            &mut self.processed_blocks,
            &self.generic_storage.enc_index(epoch),
        )
        .expect("Failed to read from storage")
    }

    pub fn set_processed_blocks(&mut self, epoch: Epoch, processed_blocks: HashSet<Slot>) {
        write_with_cache(
            self.generic_storage.storage.as_ref(),
            COL_PROCESSED_BLOCKS,
            &mut self.processed_blocks,
            &self.generic_storage.enc_index(epoch),
            processed_blocks,
        )
        .expect("Failed to write to storage")
    }

    pub fn get_threshold(&mut self, epoch: Epoch) -> Option<&u64> {
        read_with_cache(
            self.generic_storage.storage.as_ref(),
            COL_THRESHOLD,
            &mut self.thresholds,
            &self.generic_storage.enc_index(epoch),
        )
        .expect("Failed to read from storage")
    }

    pub fn set_threshold(&mut self, epoch: Epoch, threshold: u64) {
        write_with_cache(
            self.generic_storage.storage.as_ref(),
            COL_THRESHOLD,
            &mut self.thresholds,
            &self.generic_storage.enc_index(epoch),
            threshold,
        )
        .expect("Failed to write to storage")
    }

    pub fn get_accepted_authorities(&mut self, slot: Slot) -> Option<&Vec<AuthorityStake>> {
        read_with_cache(
            self.generic_storage.storage.as_ref(),
            COL_ACCEPTED_AUTHORITY,
            &mut self.accepted_authorities,
            &self.generic_storage.enc_index(slot),
        )
        .expect("Failed to read from storage")
    }

    pub fn set_accepted_authorities(&mut self, slot: Slot, authorities: Vec<AuthorityStake>) {
        write_with_cache(
            self.generic_storage.storage.as_ref(),
            COL_ACCEPTED_AUTHORITY,
            &mut self.accepted_authorities,
            &self.generic_storage.enc_index(slot),
            authorities,
        )
        .expect("Failed to write to storage")
    }

    pub fn extend_accepted_authorities(&mut self, authorities: HashMap<Slot, Vec<AuthorityStake>>) {
        let updates = authorities
            .into_iter()
            .map(|(k, v)| (self.generic_storage.enc_index(k).to_vec(), v))
            .collect();
        extend_with_cache(
            self.generic_storage.storage.as_ref(),
            COL_ACCEPTED_AUTHORITY,
            &mut self.accepted_authorities,
            updates,
        )
        .expect("Failed to write to storage");
    }
}
