use crate::store::{ChainStore, ChainStoreAccess};
use near_chain_primitives::error::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block_header::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::types::BlockHeight;
use near_primitives::types::{EpochId, ShardIndex};
use near_primitives::version::ProtocolFeature;
use near_store::Store;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Default)]
struct NewChunkTracker {
    sync_hash: Option<CryptoHash>,
    num_new_chunks: HashMap<CryptoHash, HashMap<ShardIndex, usize>>,
}

fn add_new_chunks(
    num_new_chunks: &mut HashMap<ShardIndex, usize>,
    header: &BlockHeader,
) -> Result<bool, Error> {
    let mut done = true;
    for (shard_index, num_new_chunks) in num_new_chunks.iter_mut() {
        let Some(included) = header.chunk_mask().get(*shard_index) else {
            return Err(Error::Other(format!(
                "can't get shard index {} in chunk mask for block {}",
                shard_index,
                header.hash()
            )));
        };
        if *included {
            *num_new_chunks += 1;
        }
        if *num_new_chunks < 2 {
            done = false;
        }
    }
    Ok(done)
}

impl NewChunkTracker {
    fn add_epoch_first_block(&mut self, header: &BlockHeader) -> Result<(), Error> {
        let num_new_chunks =
            (0..header.chunk_mask().len()).map(|shard_index| (shard_index, 0)).collect();
        self.num_new_chunks.insert(*header.hash(), num_new_chunks);
        Ok(())
    }

    /// If the sync hash is not already found, adds this block's hash and the number of new
    /// chunks seen so far to self.num_new_chunks. This also removes any state associated with blocks
    /// before the last final block, which keeps the memory usage bounded.
    fn add_block<T: ChainStoreAccess>(
        &mut self,
        chain_store: &T,
        header: &BlockHeader,
    ) -> Result<(), Error> {
        if self.sync_hash.is_some() {
            return Ok(());
        }

        let prev_header = chain_store.get_block_header(header.prev_hash())?;

        if prev_header.height() == chain_store.get_genesis_height()
            || prev_header.epoch_id() != header.epoch_id()
        {
            return self.add_epoch_first_block(header);
        }

        let mut num_new_chunks = match self.num_new_chunks.get(header.prev_hash()) {
            Some(n) => n.clone(),
            None => {
                return Err(Error::Other(format!(
                    "previous block {} not found in state sync hash tracker",
                    header.prev_hash()
                )))
            }
        };

        let done = add_new_chunks(&mut num_new_chunks, header)?;
        if done {
            // TODO(current_epoch_state_sync): this will not be correct if this block doesn't end up finalized on the main chain.
            // We should fix it by setting the sync hash when it's finalized, which requires making changes to how we take state snapshots.
            self.sync_hash = Some(*header.hash());
            self.num_new_chunks = HashMap::new();
            return Ok(());
        }

        self.num_new_chunks.insert(*header.hash(), num_new_chunks);

        if header.last_final_block() != &CryptoHash::default() {
            // We don't need to keep info for old blocks around. After a block is finalized, we don't need anything before it
            let last_final_height =
                chain_store.get_block_header(header.last_final_block())?.height();

            self.num_new_chunks.retain(|block_hash, _| {
                let old_header = match chain_store.get_block_header(block_hash) {
                    Ok(h) => h,
                    Err(error) => {
                        tracing::warn!(target: "chain", ?block_hash, ?error, "Could not find block header while updating sync hash");
                        return false;
                    }
                };
                old_header.height() >= last_final_height
            });
        }
        Ok(())
    }
}

struct SyncHashTrackerInner(HashMap<EpochId, NewChunkTracker>);

impl SyncHashTrackerInner {
    /// Finds the sync hash for the given epoch if it already exists in our chain. Otherwise
    /// initializes all the state necessary to find it later as we add blocks to the chain.
    fn init_epoch<T: ChainStoreAccess>(
        &mut self,
        chain_store: &T,
        epoch_first_block: &CryptoHash,
    ) -> Result<(), Error> {
        let mut tracker = NewChunkTracker::default();

        let mut header = chain_store.get_block_header(epoch_first_block)?;
        let epoch_id = *header.epoch_id();

        loop {
            tracker.add_block(chain_store, &header)?;
            if tracker.sync_hash.is_some() {
                break;
            }

            let next_hash = match chain_store.get_next_block_hash(header.hash()) {
                Ok(h) => h,
                Err(Error::DBNotFoundErr(_)) => break,
                Err(e) => return Err(e),
            };

            header = chain_store.get_block_header(&next_hash)?;
        }

        self.0.insert(epoch_id, tracker);
        Ok(())
    }

    // Initializes state for the last couple epochs
    fn new(
        chain_store: &ChainStore,
        epoch_manager: &dyn EpochManagerAdapter,
        genesis_height: BlockHeight,
    ) -> Result<Self, Error> {
        let mut me = Self(HashMap::new());

        let head = match chain_store.head() {
            Ok(h) => h,
            Err(Error::DBNotFoundErr(_)) => return Ok(me),
            Err(e) => return Err(e),
        };

        if head.height == genesis_height {
            return Ok(me);
        }

        let block_info = epoch_manager.get_block_info(&head.last_block_hash)?;
        me.init_epoch(chain_store, block_info.epoch_first_block())?;

        let first_block = chain_store.get_block_header(block_info.epoch_first_block())?;
        let block_info = epoch_manager.get_block_info(first_block.prev_hash())?;
        if block_info.height() != genesis_height {
            me.init_epoch(chain_store, block_info.epoch_first_block())?;
        }

        Ok(me)
    }

    fn get_sync_hash(&self, epoch_id: &EpochId) -> Option<CryptoHash> {
        self.0.get(epoch_id).map(|tracker| tracker.sync_hash).flatten()
    }

    fn add_block<T: ChainStoreAccess>(
        &mut self,
        chain_store: &T,
        header: &BlockHeader,
    ) -> Result<(), Error> {
        let _span = tracing::debug_span!(target: "chain", "SyncHashTracker::add_block").entered();

        let tracker = self.0.entry(*header.epoch_id()).or_default();
        tracker.add_block(chain_store, header)?;

        // TODO: gc old epochs
        Ok(())
    }
}

/// Keeps state associated with finding a suitable sync hash for each epoch.
#[derive(Clone)]
pub struct SyncHashTracker(Arc<RwLock<SyncHashTrackerInner>>);

impl SyncHashTracker {
    pub fn new(
        chain_store: &ChainStore,
        epoch_manager: &dyn EpochManagerAdapter,
        genesis_height: BlockHeight,
    ) -> Result<Self, Error> {
        let t = SyncHashTrackerInner::new(chain_store, epoch_manager, genesis_height)?;
        Ok(Self(Arc::new(RwLock::new(t))))
    }

    /// If the StateSyncHashUpdate protocol feature is enabled, this returns the hash of the first block for which at least
    /// two new chunks have been produced for every shard in the epoch after the first block. Otherwise, it returns
    /// the hash of the first block in the epoch. This is the "sync_hash" that will be used as a reference point
    /// when state syncing the current epoch's state.
    pub fn get_sync_hash(
        &self,
        chain_store: &ChainStore,
        epoch_manager: &dyn EpochManagerAdapter,
        genesis_hash: &CryptoHash,
        block_hash: &CryptoHash,
    ) -> Result<Option<CryptoHash>, Error> {
        if block_hash == genesis_hash {
            // We shouldn't be trying to sync state from before the genesis block
            return Ok(None);
        }
        let header = chain_store.get_block_header(block_hash)?;
        let protocol_version = epoch_manager.get_epoch_protocol_version(header.epoch_id())?;
        if ProtocolFeature::StateSyncHashUpdate.enabled(protocol_version) {
            Ok(self.0.read().unwrap().get_sync_hash(header.epoch_id()))
        } else {
            // In the first epoch, it doesn't make sense to sync state to the previous epoch.
            if header.epoch_id() == &EpochId::default() {
                return Ok(None);
            }
            Ok(Some(*epoch_manager.get_block_info(block_hash)?.epoch_first_block()))
        }
    }

    /// This should be called when a new block is added to the chain. If the StateSyncHashUpdate feature
    /// is enabled, it adds the current block's new chunks to the number of new chunks per shard for that epoch.
    pub fn add_block<T: ChainStoreAccess>(
        &self,
        chain_store: &T,
        header: &BlockHeader,
    ) -> Result<(), Error> {
        self.0.write().unwrap().add_block(chain_store, header)?;
        Ok(())
    }
}

pub static SYNC_TRACKER: once_cell::sync::OnceCell<SyncHashTracker> =
    once_cell::sync::OnceCell::new();

pub fn set_tracker(
    store: Store,
    epoch_manager: &dyn EpochManagerAdapter,
    genesis_height: BlockHeight,
) {
    let chain_store = ChainStore::new(store, genesis_height, false);
    let sync_hash_tracker =
        SyncHashTracker::new(&chain_store, epoch_manager, genesis_height).unwrap();
    let old = SYNC_TRACKER.set(sync_hash_tracker);
    assert!(old.is_ok());
}
