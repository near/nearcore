use crate::{ChainStore, ChainStoreAccess, RuntimeAdapter};
#[cfg(feature = "protocol_feature_flat_state")]
use assert_matches::assert_matches;
use crossbeam_channel::{unbounded, Receiver, Sender};
use near_chain_primitives::Error;
use near_primitives::types::{BlockHeight, ShardId};
#[cfg(feature = "protocol_feature_flat_state")]
use near_store::flat_state::store_helper;
use near_store::flat_state::{FlatStorageStateStatus, NUM_PARTS_IN_ONE_STEP};
use std::sync::Arc;
#[cfg(feature = "protocol_feature_flat_state")]
use tracing::debug;
use tracing::info;

/// If we launched a node with enabled flat storage but it doesn't have flat storage data on disk, we have to create it.
/// This struct is responsible for this process for the given shard.
/// See doc comment on [`FlatStorageStateStatus`] for the details of the process.
pub struct FlatStorageShardCreator {
    #[allow(unused)]
    shard_id: ShardId,
    /// Height on top of which this struct was created.
    #[allow(unused)]
    start_height: BlockHeight,
    #[allow(unused)]
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    /// Tracks number of traversed state parts during a single step.
    #[allow(unused)]
    fetched_state_parts: Option<u64>,
    /// Used by threads which traverse state parts to tell that traversal is finished.
    #[allow(unused)]
    fetched_parts_sender: Sender<u64>,
    /// Used by main thread to update the number of traversed state parts.
    #[allow(unused)]
    fetched_parts_receiver: Receiver<u64>,
}

impl FlatStorageShardCreator {
    pub fn new(
        shard_id: ShardId,
        start_height: BlockHeight,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
    ) -> Self {
        let (fetched_parts_sender, fetched_parts_receiver) = unbounded();
        Self {
            shard_id,
            start_height,
            runtime_adapter,
            fetched_state_parts: None,
            fetched_parts_sender,
            fetched_parts_receiver,
        }
    }

    #[cfg(feature = "protocol_feature_flat_state")]
    pub(crate) fn update_status(&mut self, chain_store: &ChainStore) -> Result<(), Error> {
        let current_status =
            store_helper::get_flat_storage_state_status(chain_store.store(), self.shard_id);
        match current_status {
            FlatStorageStateStatus::SavingDeltas => {
                let final_head = chain_store.final_head()?;
                let shard_id = self.shard_id;

                if final_head.height > self.start_height {
                    // If it holds, deltas for all blocks after final head are saved to disk, because they have bigger
                    // heights than one on which we launched a node. Check that it is true:
                    for height in final_head.height + 1..=chain_store.head()?.height {
                        for (_, hashes) in
                            chain_store.get_all_block_hashes_by_height(height)?.iter()
                        {
                            for hash in hashes {
                                debug!(target: "chain", %shard_id, %height, %hash, "Checking delta existence");
                                assert_matches!(
                                    store_helper::get_delta(
                                        chain_store.store(),
                                        shard_id,
                                        hash.clone(),
                                    ),
                                    Ok(Some(_))
                                );
                            }
                        }
                    }

                    // We continue saving deltas, and also start fetching state.
                    let block_hash = final_head.last_block_hash;
                    let mut store_update = chain_store.store().store_update();
                    store_helper::set_flat_head(&mut store_update, shard_id, &block_hash);
                    store_helper::set_fetching_state_step(&mut store_update, shard_id, 0u64);
                    store_update.commit()?;
                }
                Ok(())
            }
            FlatStorageStateStatus::FetchingState((_block_hash, _fetching_state_step)) => {
                // TODO: spawn threads and collect results
                Ok(())
            }
            _ => {
                panic!("Status {:?} is not supported yet", current_status);
            }
        }
    }
}

/// Creates flat storages for all shards.
pub struct FlatStorageCreator {
    pub shard_creators: Vec<FlatStorageShardCreator>,
    /// Used to spawn threads for traversing state parts.
    pub pool: rayon::ThreadPool,
}

impl FlatStorageCreator {
    pub fn new(runtime_adapter: Arc<dyn RuntimeAdapter>, chain_store: &ChainStore) -> Option<Self> {
        let chain_head = chain_store.head().unwrap();
        let num_shards = runtime_adapter.num_shards(&chain_head.epoch_id).unwrap();
        let start_height = chain_head.height;
        let mut shard_creators: Vec<FlatStorageShardCreator> = vec![];
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
            shard_creators.push(FlatStorageShardCreator::new(
                shard_id,
                start_height,
                runtime_adapter.clone(),
            ));
        }

        if creation_needed {
            Some(Self {
                shard_creators,
                pool: rayon::ThreadPoolBuilder::new()
                    .num_threads(NUM_PARTS_IN_ONE_STEP as usize)
                    .build()
                    .unwrap(),
            })
        } else {
            None
        }
    }

    pub fn update_status(
        &mut self,
        shard_id: ShardId,
        #[allow(unused_variables)] chain_store: &ChainStore,
    ) -> Result<(), Error> {
        if shard_id as usize >= self.shard_creators.len() {
            // We can request update for not supported shard if resharding happens. We don't support it yet, so we just
            // return Ok.
            return Ok(());
        }

        #[cfg(feature = "protocol_feature_flat_state")]
        self.shard_creators[shard_id as usize].update_status(chain_store)?;

        Ok(())
    }
}
