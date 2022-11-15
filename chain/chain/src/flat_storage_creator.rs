use crate::{ChainStore, ChainStoreAccess, RuntimeAdapter};
#[cfg(feature = "protocol_feature_flat_state")]
use assert_matches::assert_matches;
use crossbeam_channel::{unbounded, Receiver, Sender};
use near_chain_primitives::Error;
use near_primitives::shard_layout::ShardUId;
use near_primitives::state::ValueRef;
use near_primitives::state_part::PartId;
use near_primitives::types::{BlockHeight, ShardId, StateRoot};
use near_store::flat_state::FlatStorageStateStatus;
#[cfg(feature = "protocol_feature_flat_state")]
use near_store::flat_state::{store_helper, FetchingStateStatus};
#[cfg(feature = "protocol_feature_flat_state")]
use near_store::flat_state::{NUM_PARTS_IN_ONE_STEP, STATE_PART_MEMORY_LIMIT};
use near_store::migrations::BatchedStoreUpdate;
#[cfg(feature = "protocol_feature_flat_state")]
use near_store::DBCol;
#[cfg(feature = "protocol_feature_flat_state")]
use near_store::FlatStateDelta;
use near_store::Store;
use near_store::{Trie, TrieDBStorage, TrieTraversalItem};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
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
    /// Tracks number of state parts which are not fetched yet during a single step.
    #[allow(unused)]
    remaining_state_parts: Option<u64>,
    /// Used by threads which traverse state parts to tell that traversal is finished.
    #[allow(unused)]
    fetched_parts_sender: Sender<u64>,
    /// Used by main thread to update the number of traversed state parts.
    #[allow(unused)]
    fetched_parts_receiver: Receiver<u64>,
    #[allow(unused)]
    visited_trie_items: u64,
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
            remaining_state_parts: None,
            fetched_parts_sender,
            fetched_parts_receiver,
            visited_trie_items: 0,
        }
    }

    #[allow(unused)]
    fn state_part_hex_prefix(path_begin: &[u8]) -> String {
        let path_prefix = match path_begin.last() {
            Some(16) => &path_begin[..path_begin.len() - 1],
            _ => &path_begin,
        };
        path_prefix
            .iter()
            .map(|&n| char::from_digit(n as u32, 16).expect("nibble should be <16"))
            .collect()
    }

    /// Fetch state part, write all state items to flat storage and send the number of items to the given channel.
    #[allow(unused)]
    fn fetch_state_part(
        store: Store,
        shard_uid: ShardUId,
        state_root: StateRoot,
        part_id: PartId,
        progress: Arc<AtomicU64>,
        result_sender: Sender<u64>,
    ) {
        let trie_storage = TrieDBStorage::new(store.clone(), shard_uid);
        let trie = Trie::new(Box::new(trie_storage), state_root, None);
        let path_begin = trie.find_path_for_part_boundary(part_id.idx, part_id.total).unwrap();
        let path_end = trie.find_path_for_part_boundary(part_id.idx + 1, part_id.total).unwrap();
        let hex_prefix = Self::state_part_hex_prefix(&path_begin);
        debug!(target: "store", "Preload state part from {hex_prefix}");
        let mut trie_iter = trie.iter().unwrap();

        let mut store_update = BatchedStoreUpdate::new(&store, 10_000_000);
        let mut num_items = 0;
        for TrieTraversalItem { hash, key } in
            trie_iter.visit_nodes_interval(&path_begin, &path_end).unwrap()
        {
            match key {
                None => {}
                Some(key) => {
                    let value = trie.storage.retrieve_raw_bytes(&hash).unwrap();
                    let value_ref = ValueRef::new(&value);
                    #[cfg(feature = "protocol_feature_flat_state")]
                    store_update
                        .set_ser(DBCol::FlatState, &key, &value_ref)
                        .expect("Failed to put value in FlatState");
                    #[cfg(not(feature = "protocol_feature_flat_state"))]
                    let (_, _) = (key, value_ref);

                    num_items += 1;
                }
            }
        }
        store_update.finish().unwrap();

        progress.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let processed_parts = progress.load(std::sync::atomic::Ordering::Relaxed);

        debug!(target: "store",
            "Preload subtrie at {hex_prefix} done, \
            loaded {num_items} state items, \
            proccessed parts: {processed_parts}"
        );

        result_sender.send(num_items).unwrap();
    }

    #[cfg(feature = "protocol_feature_flat_state")]
    pub(crate) fn update_status(
        &mut self,
        chain_store: &ChainStore,
        thread_pool: &rayon::ThreadPool,
    ) -> Result<(), Error> {
        let current_status =
            store_helper::get_flat_storage_state_status(chain_store.store(), self.shard_id);
        let shard_id = self.shard_id;
        match &current_status {
            FlatStorageStateStatus::SavingDeltas => {
                let final_head = chain_store.final_head()?;

                if final_head.height > self.start_height {
                    // If it holds, deltas for all blocks after final head are saved to disk, because they have bigger
                    // heights than one on which we launched a node. Check that it is true:
                    for height in final_head.height + 1..=chain_store.head()?.height {
                        for (_, hashes) in chain_store
                            .get_all_block_hashes_by_height(height)
                            .unwrap_or_default()
                            .iter()
                        {
                            for hash in hashes {
                                debug!(target: "store", %shard_id, %height, %hash, "Checking delta existence");
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
                    let store = self.runtime_adapter.store().clone();
                    let epoch_id = self.runtime_adapter.get_epoch_id(&block_hash)?;
                    let shard_uid = self.runtime_adapter.shard_id_to_uid(shard_id, &epoch_id)?;
                    let trie_storage = TrieDBStorage::new(store.clone(), shard_uid);
                    let state_root =
                        chain_store.get_chunk_extra(&block_hash, &shard_uid)?.state_root().clone();
                    let trie = Trie::new(Box::new(trie_storage), state_root, None);
                    let root_node = trie.retrieve_root_node().unwrap();
                    let num_state_parts =
                        root_node.memory_usage / STATE_PART_MEMORY_LIMIT.as_u64() + 1;
                    let status = FetchingStateStatus {
                        part_id: 0,
                        num_parts_in_step: NUM_PARTS_IN_ONE_STEP,
                        num_parts: num_state_parts,
                    };
                    debug!(target: "store", ?status, "Switching status to fetching state");

                    let mut store_update = chain_store.store().store_update();
                    store_helper::set_flat_head(&mut store_update, shard_id, &block_hash);
                    store_helper::set_fetching_state_status(&mut store_update, shard_id, status);
                    store_update.commit()?;
                }
                Ok(())
            }
            FlatStorageStateStatus::FetchingState(fetching_state_status) => {
                let store = self.runtime_adapter.store().clone();
                let block_hash = store_helper::get_flat_head(&store, shard_id).unwrap();
                let start_part_id = fetching_state_status.part_id;
                let num_parts_in_step = fetching_state_status.num_parts_in_step;
                let num_parts = fetching_state_status.num_parts;
                let next_start_part_id = num_parts.min(start_part_id + num_parts_in_step);

                match self.remaining_state_parts.clone() {
                    None => {
                        // We need to spawn threads to fetch state parts and fill flat storage data.
                        let epoch_id = self.runtime_adapter.get_epoch_id(&block_hash)?;
                        let shard_uid =
                            self.runtime_adapter.shard_id_to_uid(shard_id, &epoch_id)?;
                        let state_root = chain_store
                            .get_chunk_extra(&block_hash, &shard_uid)?
                            .state_root()
                            .clone();
                        let progress = Arc::new(std::sync::atomic::AtomicU64::new(0));
                        debug!(
                            target: "store", %shard_id, %block_hash, %start_part_id, %next_start_part_id, %num_parts,
                            "Spawning threads to fetch state parts for flat storage"
                        );

                        for part_id in start_part_id..next_start_part_id {
                            let inner_store = store.clone();
                            let inner_state_root = state_root.clone();
                            let inner_progress = progress.clone();
                            let inner_sender = self.fetched_parts_sender.clone();
                            thread_pool.spawn(move || {
                                Self::fetch_state_part(
                                    inner_store,
                                    shard_uid,
                                    inner_state_root,
                                    PartId::new(part_id, num_parts),
                                    inner_progress,
                                    inner_sender,
                                );
                            })
                        }

                        self.remaining_state_parts = Some(next_start_part_id - start_part_id);
                        Ok(())
                    }
                    Some(state_parts) if state_parts > 0 => {
                        // If not all state parts were fetched, try receiving new results.
                        let mut updated_state_parts = state_parts;
                        while let Ok(n) = self.fetched_parts_receiver.try_recv() {
                            updated_state_parts -= 1;
                            self.visited_trie_items += n;
                        }
                        self.remaining_state_parts = Some(updated_state_parts);
                        Ok(())
                    }
                    Some(_) => {
                        // Mark that we don't wait for new state parts.
                        self.remaining_state_parts = None;

                        let mut store_update = chain_store.store().store_update();
                        if next_start_part_id < num_parts {
                            // If there are still remaining state parts, switch status to the new range of state parts.
                            let new_status = FetchingStateStatus {
                                part_id: next_start_part_id,
                                num_parts_in_step,
                                num_parts,
                            };
                            debug!(target: "chain", %shard_id, %block_hash, ?new_status);
                            store_helper::set_fetching_state_status(
                                &mut store_update,
                                shard_id,
                                new_status,
                            );
                        } else {
                            // If all parts were fetched, we can start catchup.
                            debug!(target: "chain", %shard_id, %block_hash, "Finished fetching state");
                            store_helper::remove_fetching_state_status(&mut store_update, shard_id);
                            store_helper::start_catchup(&mut store_update, shard_id);
                        }
                        store_update.commit()?;

                        Ok(())
                    }
                }
            }
            FlatStorageStateStatus::CatchingUp => {
                let store = self.runtime_adapter.store();
                let old_flat_head = store_helper::get_flat_head(store, shard_id).unwrap();
                let mut flat_head = old_flat_head.clone();
                let chain_final_head = chain_store.final_head()?;
                let mut merged_delta = FlatStateDelta::default();

                // Merge up to 50 deltas of the next blocks until we reach chain final head.
                // TODO: consider merging 10 deltas at once to limit memory usage
                for _ in 0..50 {
                    let height = chain_store.get_block_height(&flat_head).unwrap();
                    if height > chain_final_head.height {
                        panic!("New flat head moved too far: new head = {flat_head}, height = {height}, final block height = {}", chain_final_head.height);
                    }
                    if height == chain_final_head.height {
                        break;
                    }
                    flat_head = chain_store.get_next_block_hash(&flat_head).unwrap();
                    let delta =
                        store_helper::get_delta(store, shard_id, flat_head).unwrap().unwrap();
                    merged_delta.merge(delta.as_ref());
                }

                if old_flat_head != flat_head {
                    // If flat head changes, save all changes to store.
                    let old_height = chain_store.get_block_height(&old_flat_head).unwrap();
                    let height = chain_store.get_block_height(&flat_head).unwrap();
                    debug!(target: "chain", %shard_id, %old_flat_head, %old_height, %flat_head, %height, "Catching up flat head");
                    let mut store_update = self.runtime_adapter.store().store_update();
                    store_helper::set_flat_head(&mut store_update, shard_id, &flat_head);
                    merged_delta.apply_to_flat_state(&mut store_update);

                    if height == chain_final_head.height {
                        // If we reached chain final head, we can finish catchup and finally create flat storage.
                        store_helper::finish_catchup(&mut store_update, shard_id);
                        store_update.commit()?;
                        debug!(target: "chain", %shard_id, %flat_head, %height, "Creating flat storage");
                        assert_eq!(
                            self.runtime_adapter.try_create_flat_storage_state_for_shard(
                                shard_id,
                                chain_store.head().unwrap().height,
                                chain_store,
                            ),
                            FlatStorageStateStatus::Ready
                        );
                    } else {
                        store_update.commit()?;
                    }
                }

                Ok(())
            }
            FlatStorageStateStatus::Ready => Ok(()),
            FlatStorageStateStatus::DontCreate => {
                panic!("We initiated flat storage creation for shard {shard_id} but according to flat storage state status in db it cannot be created");
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
                pool: rayon::ThreadPoolBuilder::new().num_threads(4).build().unwrap(),
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
        self.shard_creators[shard_id as usize].update_status(chain_store, &self.pool)?;

        Ok(())
    }
}
