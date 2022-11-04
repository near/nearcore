use crate::{ChainStore, ChainStoreAccess, RuntimeAdapter};
use crossbeam_channel::{unbounded, Receiver, Sender};
use near_chain_primitives::Error;
use near_primitives::types::{BlockHeight, ShardId};
use near_store::flat_state::FlatStorageStateStatus;
use std::sync::{Arc, Mutex};
use tracing::info;

/// Number of parts to which we divide shard state for parallel traversal.
// TODO: consider changing it for different shards, ensure that shard memory usage / `NUM_PARTS` < X MiB.
#[allow(unused)]
const NUM_PARTS: u64 = 4_000;

/// Number of traversed parts during a single step of fetching state.
#[allow(unused)]
const PART_STEP: u64 = 50;

/// Creates flat storage and tracks creation status for the given shard.
pub struct FlatStorageShardCreator {
    pub status: FlatStorageStateStatus,
    pub shard_id: ShardId,
    /// Tracks number of traversed state parts during a single step.
    #[allow(unused)]
    pub traversed_state_parts: Option<u64>,
    /// Used by threads which traverse state parts to tell that traversal is finished.
    #[allow(unused)]
    pub traversed_parts_sender: Sender<u64>,
    /// Used by main thread to update the number of traversed state parts.
    #[allow(unused)]
    pub traversed_parts_receiver: Receiver<u64>,
}

impl FlatStorageShardCreator {
    pub fn new(status: FlatStorageStateStatus, shard_id: ShardId) -> Self {
        let (traversed_parts_sender, traversed_parts_receiver) = unbounded();
        Self {
            status,
            shard_id,
            traversed_state_parts: None,
            traversed_parts_sender,
            traversed_parts_receiver,
        }
    }
}

/// Creates flat storages for all shards.
pub struct FlatStorageCreator {
    /// Height on top of which this struct was created.
    pub start_height: BlockHeight,
    pub shard_creators: Vec<Arc<Mutex<FlatStorageShardCreator>>>,
    pub runtime_adapter: Arc<dyn RuntimeAdapter>,
    /// Used to spawn threads for traversing state parts.
    pub pool: rayon::ThreadPool,
}

impl FlatStorageCreator {
    pub fn new(runtime_adapter: Arc<dyn RuntimeAdapter>, chain_store: &ChainStore) -> Option<Self> {
        let chain_head = chain_store.head().unwrap();
        let num_shards = runtime_adapter.num_shards(&chain_head.epoch_id).unwrap();
        let start_height = chain_head.height;
        let mut shard_creators: Vec<Arc<Mutex<FlatStorageShardCreator>>> = vec![];
        let mut creation_needed = false;
        for shard_id in 0..num_shards {
            let status = runtime_adapter.try_create_flat_storage_state_for_shard(
                shard_id,
                chain_store.head().unwrap().height,
                chain_store,
            );
            info!(target: "chain", %shard_id, "Flat storage creation status: {:?}", status);
            match status {
                FlatStorageStateStatus::Ready | FlatStorageStateStatus::DontCreate => {}
                _ => {
                    creation_needed = true;
                }
            }
            shard_creators
                .push(Arc::new(Mutex::new(FlatStorageShardCreator::new(status, shard_id))));
        }

        if creation_needed {
            Some(Self {
                start_height,
                shard_creators,
                runtime_adapter: runtime_adapter.clone(),
                pool: rayon::ThreadPoolBuilder::new()
                    .num_threads(PART_STEP as usize)
                    .build()
                    .unwrap(),
            })
        } else {
            None
        }
    }

    pub fn update_status(&self, shard_id: ShardId, _chain_store: &ChainStore) -> Result<(), Error> {
        if shard_id as usize >= self.shard_creators.len() {
            // We can request update for not supported shard if resharding happens. We don't support it yet, so we just
            // return Ok.
            return Ok(());
        }

        let guard = self.shard_creators[shard_id as usize].lock().unwrap();
        match guard.status.clone() {
            FlatStorageStateStatus::SavingDeltas => {
                // Once final head height > start height, we can switch to next step.
                // Then, ChainStore is used to get state roots, block infos and flat storage creation in the end.
                Ok(())
            }
            _ => {
                panic!("Status {:?} is not supported yet", guard.status);
            }
        }
    }
}
