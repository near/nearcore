use crate::store::{ChainStore, ChainStoreAccess};
use near_chain_primitives::error::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block_header::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{EpochId, ShardId};
use near_primitives::version::ProtocolFeature;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Default)]
struct NewChunkTracker {
    sync_hash: Option<CryptoHash>,
    num_new_chunks: HashMap<CryptoHash, HashMap<ShardId, usize>>,
}

fn add_new_chunks(
    epoch_manager: &dyn EpochManagerAdapter,
    num_new_chunks: &mut HashMap<ShardId, usize>,
    header: &BlockHeader,
) -> Result<bool, Error> {
    let shard_layout = epoch_manager.get_shard_layout(header.epoch_id())?;

    let mut done = true;
    for (shard_id, num_new_chunks) in num_new_chunks.iter_mut() {
        let shard_index = shard_layout.get_shard_index(*shard_id);
        let Some(included) = header.chunk_mask().get(shard_index) else {
            return Err(Error::Other(format!(
                "can't get shard {} in chunk mask for block {}",
                shard_id,
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
    fn add_epoch_first_block(
        &mut self,
        epoch_manager: &dyn EpochManagerAdapter,
        header: &BlockHeader,
    ) -> Result<(), Error> {
        let shard_ids = epoch_manager.shard_ids(header.epoch_id())?;
        let num_new_chunks = shard_ids.iter().map(|shard_id| (*shard_id, 0)).collect();
        self.num_new_chunks.insert(*header.hash(), num_new_chunks);
        Ok(())
    }

    fn add_block(
        &mut self,
        chain_store: &ChainStore,
        epoch_manager: &dyn EpochManagerAdapter,
        header: &BlockHeader,
    ) -> Result<(), Error> {
        if self.sync_hash.is_some() {
            return Ok(());
        }

        if epoch_manager.is_next_block_epoch_start(header.prev_hash())? {
            return self.add_epoch_first_block(epoch_manager, header);
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

        let done = add_new_chunks(epoch_manager, &mut num_new_chunks, header)?;
        if done {
            // TODO(current_epoch_state_sync): this will not be correct if this block doesn't end up finalized on the main chain
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
    fn init_epoch(
        &mut self,
        chain_store: &ChainStore,
        epoch_manager: &dyn EpochManagerAdapter,
        epoch_first_block: &CryptoHash,
    ) -> Result<(), Error> {
        let mut tracker = NewChunkTracker::default();

        let mut header = chain_store.get_block_header(epoch_first_block)?;
        let epoch_id = *header.epoch_id();

        loop {
            tracker.add_block(chain_store, epoch_manager, &header)?;
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

    fn new(
        chain_store: &ChainStore,
        epoch_manager: &dyn EpochManagerAdapter,
        genesis_hash: &CryptoHash,
    ) -> Result<Self, Error> {
        let mut me = Self(HashMap::new());

        let head = match chain_store.head() {
            Ok(h) => h,
            Err(Error::DBNotFoundErr(_)) => return Ok(me),
            Err(e) => return Err(e),
        };

        if &head.last_block_hash == genesis_hash {
            return Ok(me);
        }

        let block_info = epoch_manager.get_block_info(&head.last_block_hash)?;
        me.init_epoch(chain_store, epoch_manager, block_info.epoch_first_block())?;

        let first_block = chain_store.get_block_header(block_info.epoch_first_block())?;
        if first_block.prev_hash() != genesis_hash {
            let block_info = epoch_manager.get_block_info(first_block.prev_hash())?;
            me.init_epoch(chain_store, epoch_manager, block_info.epoch_first_block())?;
        }

        Ok(me)
    }

    fn get_sync_hash(&self, epoch_id: &EpochId) -> Option<CryptoHash> {
        self.0.get(epoch_id).map(|tracker| tracker.sync_hash).flatten()
    }

    fn add_block(
        &mut self,
        chain_store: &ChainStore,
        epoch_manager: &dyn EpochManagerAdapter,
        header: &BlockHeader,
    ) -> Result<(), Error> {
        let _span = tracing::debug_span!(target: "chain", "SyncHashTracker::add_block").entered();

        let tracker = self.0.entry(*header.epoch_id()).or_default();
        tracker.add_block(chain_store, epoch_manager, header)?;

        if epoch_manager.is_next_block_epoch_start(header.prev_hash())? {
            let current_epoch_info = epoch_manager.get_epoch_info(header.epoch_id())?;
            // We only want to keep a few epochs around. This will mean that we will refuse to find sync hashes and
            // therefore respond to state sync requests for old epochs, but this would be true anyway since we don't keep
            // snapshots for old epochs around
            self.0.retain(|epoch_id, _tracker| {
                let epoch_info = match epoch_manager.get_epoch_info(epoch_id) {
                    Ok(info) => info,
                    Err(error) => {
                        tracing::warn!(target: "chain", ?epoch_id, ?error, "Could not find epoch info");
                        return false;
                    }
                };
                epoch_info.epoch_height() + 1 >= current_epoch_info.epoch_height()
            });
        }
        Ok(())
    }
}

pub struct SyncHashTracker(Arc<RwLock<SyncHashTrackerInner>>);

impl SyncHashTracker {
    pub fn new(
        chain_store: &ChainStore,
        epoch_manager: &dyn EpochManagerAdapter,
        genesis_hash: &CryptoHash,
    ) -> Result<Self, Error> {
        let t = SyncHashTrackerInner::new(chain_store, epoch_manager, genesis_hash)?;
        Ok(Self(Arc::new(RwLock::new(t))))
    }

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

    pub fn add_block(
        &self,
        chain_store: &ChainStore,
        epoch_manager: &dyn EpochManagerAdapter,
        header: &BlockHeader,
    ) -> Result<(), Error> {
        let protocol_version = epoch_manager.get_epoch_protocol_version(header.epoch_id())?;
        if ProtocolFeature::StateSyncHashUpdate.enabled(protocol_version) {
            self.0.write().unwrap().add_block(chain_store, epoch_manager, header)?;
        }
        Ok(())
    }
}
