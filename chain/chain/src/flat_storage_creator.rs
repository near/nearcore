use crate::{ChainStore, ChainStoreAccess, RuntimeAdapter};
use crossbeam_channel::{unbounded, Receiver, Sender};
use near_chain_primitives::Error;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{BlockHeight, ShardId};
use std::sync::{Arc, Mutex};
use tracing::info;

pub const STATUS_KEY: &[u8; 6] = b"STATUS";

/// Number of parts to which we divide shard state for parallel traversal.
// TODO: consider changing it for different shards, ensure that shard memory usage / `NUM_PARTS` < X MiB.
const NUM_PARTS: u64 = 4_000;

/// Number of traversed parts during a single step of fetching state.
const PART_STEP: u64 = 50;

/// Status of flat storage creation for the shard.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CreationStatus {
    /// We can't start fetching state on node start, because `FlatStorageDelta`s are not saved to disk by default.
    /// During this step, we save current chain head, start saving all deltas for blocks after chain head and wait until
    /// final chain head moves after saved chain head.
    SavingDeltas,
    /// We can start fetching state to fill flat storage for some final chain head, because all deltas after it are
    /// saved to disk. It is done in `NUM_PARTS` / `PART_STEP` steps, during each step we spawn background threads to
    /// fill some part of state.
    /// Status contains block hash for which we fetch the shard state and step of fetching state. Progress of each step
    /// is saved to disk, so if creation is interrupted during some step, it won't repeat previous steps and will start
    /// from this step again.
    FetchingState((CryptoHash, u64)),
    /// Flat storage is initialized but its head is too far away from chain final head. We need to apply deltas until
    /// the head reaches final head.
    CatchingUp,
    /// Flat storage head is the same as chain final head. We can create `FlatStorageState`.
    Finished,
}

/// Creates flat storage and tracks creation status for the given shard.
pub struct FlatStorageShardCreator {
    pub status: CreationStatus,
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
    pub fn new(shard_id: ShardId, _chain_store: &ChainStore) -> Self {
        let (traversed_parts_sender, traversed_parts_receiver) = unbounded();
        // TODO: read flat storage data and set correct status. ChainStore will be used to get flat storage heads and
        // block heights.
        let status = CreationStatus::SavingDeltas;
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
        let shard_creators: Vec<Arc<Mutex<FlatStorageShardCreator>>> = (0..num_shards)
            .map(|shard_id| {
                Arc::new(Mutex::new(FlatStorageShardCreator::new(shard_id, chain_store)))
            })
            .collect();
        let mut creation_needed = false;
        for shard_creator in shard_creators.iter() {
            let guard = shard_creator.lock().unwrap();
            let shard_id = guard.shard_id;
            info!(target: "chain", %shard_id, "Flat storage creation status: {:?}", guard.status);

            #[cfg(feature = "protocol_feature_flat_state")]
            if matches!(guard.status, CreationStatus::Finished) {
                runtime_adapter.create_flat_storage_state_for_shard(
                    shard_id,
                    chain_store.head().unwrap().height,
                    chain_store,
                );
            } else {
                creation_needed = true;
            }
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

    pub fn get_status(&self, shard_id: ShardId) -> CreationStatus {
        let guard = self.shard_creators[shard_id as usize].lock().unwrap();
        guard.status.clone()
    }

    pub fn update_status(&self, shard_id: ShardId, _chain_store: &ChainStore) -> Result<(), Error> {
        let mut guard = self.shard_creators[shard_id as usize].lock().unwrap();

        match guard.status.clone() {
            CreationStatus::SavingDeltas => {
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
