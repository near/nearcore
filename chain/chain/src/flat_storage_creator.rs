//! Logic for creating flat storage in parallel to chain processing.
//!
//! The main struct responsible is `FlatStorageShardCreator`.
//! After its creation, `update_status` is called periodically, which executes some part of flat storage creation
//! depending on what the current status is:
//! `SavingDeltas`: checks if we moved chain final head forward enough to have all flat storage deltas written on disk.
//! `FetchingState`: spawns threads for fetching some range state parts, waits for receiving results, writes key-value
//! parts to flat storage column on disk and spawns threads for new range once current range is finished.
//! `CatchingUp`: moves flat storage head forward, so it may reach chain final head.
//! `Ready`: flat storage is created and it is up-to-date.

use crate::types::RuntimeAdapter;
use crate::{ChainStore, ChainStoreAccess};
use assert_matches::assert_matches;
use crossbeam_channel::{unbounded, Receiver, Sender};
use near_chain_primitives::Error;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::shard_layout::ShardUId;
use near_primitives::state::FlatStateValue;
use near_primitives::state_part::PartId;
use near_primitives::types::{AccountId, BlockHeight, StateRoot};
use near_store::flat::{
    store_helper, BlockInfo, FetchingStateStatus, FlatStateChanges, FlatStorageCreationMetrics,
    FlatStorageCreationStatus, FlatStorageReadyStatus, FlatStorageStatus, NUM_PARTS_IN_ONE_STEP,
    STATE_PART_MEMORY_LIMIT,
};
use near_store::Store;
use near_store::{Trie, TrieDBStorage, TrieTraversalItem};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tracing::{debug, info};

/// If we launched a node with enabled flat storage but it doesn't have flat storage data on disk, we have to create it.
/// This struct is responsible for this process for the given shard.
/// See doc comment on [`FlatStorageCreationStatus`] for the details of the process.
pub struct FlatStorageShardCreator {
    shard_uid: ShardUId,
    /// Height on top of which this struct was created.
    start_height: BlockHeight,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    runtime: Arc<dyn RuntimeAdapter>,
    /// Tracks number of state parts which are not fetched yet during a single step.
    /// Stores Some(parts) if threads for fetching state were spawned and None otherwise.
    remaining_state_parts: Option<u64>,
    /// Used by threads which traverse state parts to tell that traversal is finished.
    fetched_parts_sender: Sender<u64>,
    /// Used by main thread to update the number of traversed state parts.
    fetched_parts_receiver: Receiver<u64>,
    metrics: FlatStorageCreationMetrics,
}

impl FlatStorageShardCreator {
    /// Maximal number of blocks which can be caught up during one step.
    const CATCH_UP_BLOCKS: usize = 50;

    pub fn new(
        shard_uid: ShardUId,
        start_height: BlockHeight,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        runtime: Arc<dyn RuntimeAdapter>,
    ) -> Self {
        let (fetched_parts_sender, fetched_parts_receiver) = unbounded();
        Self {
            shard_uid,
            start_height,
            epoch_manager,
            runtime,
            remaining_state_parts: None,
            fetched_parts_sender,
            fetched_parts_receiver,
            metrics: FlatStorageCreationMetrics::new(shard_uid.shard_id()),
        }
    }

    #[allow(unused)]
    fn nibbles_to_hex(key_nibbles: &[u8]) -> String {
        let path_prefix = match key_nibbles.last() {
            Some(16) => &key_nibbles[..key_nibbles.len() - 1],
            _ => &key_nibbles,
        };
        path_prefix
            .iter()
            .map(|&n| char::from_digit(n as u32, 16).expect("nibble should be <16"))
            .collect()
    }

    /// Fetch state part, write all state items to flat storage and send the number of items to the given channel.
    fn fetch_state_part(
        store: Store,
        shard_uid: ShardUId,
        state_root: StateRoot,
        part_id: PartId,
        progress: Arc<AtomicU64>,
        result_sender: Sender<u64>,
    ) {
        let trie_storage = TrieDBStorage::new(store.clone(), shard_uid);
        let trie = Trie::new(Rc::new(trie_storage), state_root, None);
        let path_begin = trie.find_state_part_boundary(part_id.idx, part_id.total).unwrap();
        let path_end = trie.find_state_part_boundary(part_id.idx + 1, part_id.total).unwrap();
        let hex_path_begin = Self::nibbles_to_hex(&path_begin);
        debug!(target: "store", "Preload state part from {hex_path_begin}");
        let mut trie_iter = trie.iter().unwrap();

        let mut store_update = store.store_update();
        let mut num_items = 0;
        for TrieTraversalItem { hash, key } in
            trie_iter.visit_nodes_interval(&path_begin, &path_end).unwrap()
        {
            if let Some(key) = key {
                let value = trie.storage.retrieve_raw_bytes(&hash).unwrap();
                store_helper::set_flat_state_value(
                    &mut store_update,
                    shard_uid,
                    key,
                    Some(FlatStateValue::value_ref(&value)),
                );
                num_items += 1;
            }
        }
        store_update.commit().unwrap();

        let processed_parts = progress.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;

        debug!(target: "store",
            "Preload subtrie at {hex_path_begin} done, \
            loaded {num_items} state items, \
            proccessed parts: {processed_parts}"
        );

        if let Err(e) = result_sender.send(num_items) {
            // Log error with `eprintln` because `tracing` stops working after
            // SIGTERM.
            eprintln!(
                "During flat storage creation, state part sender channel \
                was disconnected: {e}. Results of fetching state part \
                {part_id:?} for shard {shard_uid} will not be recorded \
                and flat storage creation will stall. Please restart the node."
            );
        }
    }

    /// Checks current flat storage creation status, execute work related to it and possibly switch to next status.
    /// Creates flat storage when all intermediate steps are finished.
    /// Returns boolean indicating if flat storage was created.
    pub fn update_status(
        &mut self,
        chain_store: &ChainStore,
        thread_pool: &rayon::ThreadPool,
    ) -> Result<bool, Error> {
        let shard_id = self.shard_uid.shard_id();
        let current_status =
            store_helper::get_flat_storage_status(chain_store.store(), self.shard_uid)
                .expect("failed to read flat storage status");
        self.metrics.set_status(&current_status);
        match &current_status {
            FlatStorageStatus::Empty => {
                let mut store_update = chain_store.store().store_update();
                store_helper::set_flat_storage_status(
                    &mut store_update,
                    self.shard_uid,
                    FlatStorageStatus::Creation(FlatStorageCreationStatus::SavingDeltas),
                );
                store_update.commit()?;
            }
            FlatStorageStatus::Creation(FlatStorageCreationStatus::SavingDeltas) => {
                let final_head = chain_store.final_head()?;
                let final_height = final_head.height;

                if final_height > self.start_height {
                    // If it holds, deltas for all blocks after final head are saved to disk, because they have bigger
                    // heights than one on which we launched a node. Check that it is true:
                    for height in final_height + 1..=chain_store.head()?.height {
                        // We skip heights for which there are no blocks, because certain heights can be skipped.
                        for (_, hashes) in
                            chain_store.get_all_block_hashes_by_height(height)?.iter()
                        {
                            for hash in hashes {
                                debug!(target: "store", %shard_id, %height, %hash, "Checking delta existence");
                                assert_matches!(
                                    store_helper::get_delta_changes(
                                        chain_store.store(),
                                        self.shard_uid,
                                        *hash
                                    ),
                                    Ok(Some(_))
                                );
                            }
                        }
                    }

                    // We continue saving deltas, and also start fetching state.
                    let block_hash = final_head.last_block_hash;
                    let store = self.runtime.store().clone();
                    let epoch_id = self.epoch_manager.get_epoch_id(&block_hash)?;
                    let shard_uid = self.epoch_manager.shard_id_to_uid(shard_id, &epoch_id)?;
                    let trie_storage = TrieDBStorage::new(store, shard_uid);
                    let state_root =
                        *chain_store.get_chunk_extra(&block_hash, &shard_uid)?.state_root();
                    let trie = Trie::new(Rc::new(trie_storage), state_root, None);
                    let root_node = trie.retrieve_root_node().unwrap();
                    let num_state_parts =
                        root_node.memory_usage / STATE_PART_MEMORY_LIMIT.as_u64() + 1;
                    let status = FetchingStateStatus {
                        block_hash,
                        part_id: 0,
                        num_parts_in_step: NUM_PARTS_IN_ONE_STEP,
                        num_parts: num_state_parts,
                    };
                    info!(target: "store", %shard_id, %final_height, ?status, "Switching status to fetching state");

                    let mut store_update = chain_store.store().store_update();
                    self.metrics.set_flat_head_height(final_head.height);
                    store_helper::set_flat_storage_status(
                        &mut store_update,
                        self.shard_uid,
                        FlatStorageStatus::Creation(FlatStorageCreationStatus::FetchingState(
                            status,
                        )),
                    );
                    store_update.commit()?;
                }
            }
            FlatStorageStatus::Creation(FlatStorageCreationStatus::FetchingState(
                fetching_state_status,
            )) => {
                let store = self.runtime.store().clone();
                let block_hash = fetching_state_status.block_hash;
                let start_part_id = fetching_state_status.part_id;
                let num_parts_in_step = fetching_state_status.num_parts_in_step;
                let num_parts = fetching_state_status.num_parts;
                let next_start_part_id = num_parts.min(start_part_id + num_parts_in_step);
                self.metrics.set_remaining_state_parts(num_parts - start_part_id);

                match self.remaining_state_parts {
                    None => {
                        // We need to spawn threads to fetch state parts and fill flat storage data.
                        let epoch_id = self.epoch_manager.get_epoch_id(&block_hash)?;
                        let shard_uid = self.epoch_manager.shard_id_to_uid(shard_id, &epoch_id)?;
                        let state_root =
                            *chain_store.get_chunk_extra(&block_hash, &shard_uid)?.state_root();
                        let progress = Arc::new(std::sync::atomic::AtomicU64::new(0));
                        debug!(
                            target: "store", %shard_id, %block_hash, %start_part_id, %next_start_part_id, %num_parts,
                            "Spawning threads to fetch state parts for flat storage"
                        );

                        for part_id in start_part_id..next_start_part_id {
                            let inner_store = store.clone();
                            let inner_progress = progress.clone();
                            let inner_sender = self.fetched_parts_sender.clone();
                            let inner_threads_used = self.metrics.threads_used();
                            thread_pool.spawn(move || {
                                inner_threads_used.inc();
                                Self::fetch_state_part(
                                    inner_store,
                                    shard_uid,
                                    state_root,
                                    PartId::new(part_id, num_parts),
                                    inner_progress,
                                    inner_sender,
                                );
                                inner_threads_used.dec();
                            })
                        }

                        self.remaining_state_parts = Some(next_start_part_id - start_part_id);
                    }
                    Some(state_parts) if state_parts > 0 => {
                        // If not all state parts were fetched, try receiving new results.
                        let mut updated_state_parts = state_parts;
                        while let Ok(num_items) = self.fetched_parts_receiver.try_recv() {
                            updated_state_parts -= 1;
                            self.metrics.inc_fetched_state(num_items);
                        }
                        self.remaining_state_parts = Some(updated_state_parts);
                    }
                    Some(_) => {
                        // Mark that we don't wait for new state parts.
                        self.remaining_state_parts = None;

                        let mut store_update = chain_store.store().store_update();
                        if next_start_part_id < num_parts {
                            // If there are still remaining state parts, switch status to the new range of state parts.
                            // We will spawn new rayon tasks on the next status update.
                            let new_status = FetchingStateStatus {
                                block_hash,
                                part_id: next_start_part_id,
                                num_parts_in_step,
                                num_parts,
                            };
                            debug!(target: "chain", %shard_id, %block_hash, ?new_status);
                            store_helper::set_flat_storage_status(
                                &mut store_update,
                                self.shard_uid,
                                FlatStorageStatus::Creation(
                                    FlatStorageCreationStatus::FetchingState(new_status),
                                ),
                            );
                        } else {
                            // If all parts were fetched, we can start catchup.
                            info!(target: "chain", %shard_id, %block_hash, "Finished fetching state");
                            self.metrics.set_remaining_state_parts(0);
                            store_helper::remove_delta(
                                &mut store_update,
                                self.shard_uid,
                                block_hash,
                            );
                            store_helper::set_flat_storage_status(
                                &mut store_update,
                                self.shard_uid,
                                FlatStorageStatus::Creation(FlatStorageCreationStatus::CatchingUp(
                                    block_hash,
                                )),
                            );
                        }
                        store_update.commit()?;
                    }
                }
            }
            FlatStorageStatus::Creation(FlatStorageCreationStatus::CatchingUp(old_flat_head)) => {
                let store = self.runtime.store();
                let mut flat_head = *old_flat_head;
                let chain_final_head = chain_store.final_head()?;
                let mut merged_changes = FlatStateChanges::default();
                let mut store_update = self.runtime.store().store_update();

                // Merge up to 50 deltas of the next blocks until we reach chain final head.
                // TODO: consider merging 10 deltas at once to limit memory usage
                for _ in 0..Self::CATCH_UP_BLOCKS {
                    let height = chain_store.get_block_height(&flat_head).unwrap();
                    if height > chain_final_head.height {
                        panic!("New flat head moved too far: new head = {flat_head}, height = {height}, final block height = {}", chain_final_head.height);
                    }
                    // Stop if we reached chain final head.
                    if flat_head == chain_final_head.last_block_hash {
                        break;
                    }
                    flat_head = chain_store.get_next_block_hash(&flat_head).unwrap();
                    let changes = store_helper::get_delta_changes(store, self.shard_uid, flat_head)
                        .unwrap()
                        .unwrap();
                    merged_changes.merge(changes);
                    store_helper::remove_delta(&mut store_update, self.shard_uid, flat_head);
                }

                if (old_flat_head != &flat_head) || (flat_head == chain_final_head.last_block_hash)
                {
                    // If flat head changes, save all changes to store.
                    let epoch_id = self.epoch_manager.get_epoch_id(&flat_head)?;
                    let shard_uid = self.epoch_manager.shard_id_to_uid(shard_id, &epoch_id)?;
                    let old_height = chain_store.get_block_height(&old_flat_head).unwrap();
                    let flat_head_block_header = chain_store.get_block_header(&flat_head).unwrap();
                    let height = flat_head_block_header.height();
                    debug!(target: "chain", %shard_id, %old_flat_head, %old_height, %flat_head, %height, "Catching up flat head");
                    self.metrics.set_flat_head_height(height);
                    merged_changes.apply_to_flat_state(&mut store_update, shard_uid);
                    store_helper::set_flat_storage_status(
                        &mut store_update,
                        shard_uid,
                        FlatStorageStatus::Creation(FlatStorageCreationStatus::CatchingUp(
                            flat_head,
                        )),
                    );
                    store_update.commit()?;

                    // If we reached chain final head, we can finish catchup and finally create flat storage.
                    if flat_head == chain_final_head.last_block_hash {
                        // GC deltas from forks which could have appeared on chain during catchup.
                        // Assuming that flat storage creation finishes in < 2 days, all deltas metadata cannot occupy
                        // more than 2 * (Blocks per day = 48 * 60 * 60) * (BlockInfo size = 72) ~= 12.4 MB.
                        let mut store_update = self.runtime.store().store_update();
                        let deltas_metadata = store_helper::get_all_deltas_metadata(&store, shard_uid)
                            .unwrap_or_else(|_| {
                                panic!("Cannot read flat state deltas metadata for shard {shard_id} from storage")
                            });
                        let mut gc_count = 0;
                        for delta_metadata in deltas_metadata {
                            if delta_metadata.block.height <= chain_final_head.height {
                                store_helper::remove_delta(
                                    &mut store_update,
                                    self.shard_uid,
                                    delta_metadata.block.hash,
                                );
                                gc_count += 1;
                            }
                        }

                        // If we reached chain final head, we can finish catchup and finally create flat storage.
                        store_helper::set_flat_storage_status(
                            &mut store_update,
                            self.shard_uid,
                            FlatStorageStatus::Ready(FlatStorageReadyStatus {
                                flat_head: BlockInfo {
                                    hash: flat_head,
                                    prev_hash: *flat_head_block_header.prev_hash(),
                                    height,
                                },
                            }),
                        );
                        store_update.commit()?;
                        info!(target: "chain", %shard_id, %flat_head, %height, "Garbage collected {gc_count} deltas");
                        if let Some(manager) = self.runtime.get_flat_storage_manager() {
                            manager.create_flat_storage_for_shard(shard_uid).unwrap();
                        }
                        info!(target: "chain", %shard_id, %flat_head, %height, "Flat storage creation done");
                    }
                }
            }
            FlatStorageStatus::Ready(_) => return Ok(true),
            FlatStorageStatus::Disabled => {
                panic!("initiated flat storage creation for shard {shard_id} while it is disabled");
            }
        };
        Ok(false)
    }
}

/// Creates flat storages for all shards.
pub struct FlatStorageCreator {
    pub shard_creators: HashMap<ShardUId, FlatStorageShardCreator>,
    /// Used to spawn threads for traversing state parts.
    pub pool: rayon::ThreadPool,
}

impl FlatStorageCreator {
    /// For each of tracked shards, either creates flat storage if it is already stored on DB,
    /// or starts migration to flat storage which updates DB in background and creates flat storage afterwards.
    pub fn new(
        me: Option<&AccountId>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
        runtime: Arc<dyn RuntimeAdapter>,
        chain_store: &ChainStore,
        num_threads: usize,
    ) -> Result<Option<Self>, Error> {
        let chain_head = chain_store.head()?;
        let num_shards = epoch_manager.num_shards(&chain_head.epoch_id)?;
        let mut shard_creators: HashMap<ShardUId, FlatStorageShardCreator> = HashMap::new();
        let mut creation_needed = false;
        let flat_storage_manager = if let Some(manager) = runtime.get_flat_storage_manager() {
            manager
        } else {
            return Ok(None);
        };
        for shard_id in 0..num_shards {
            if shard_tracker.care_about_shard(me, &chain_head.prev_block_hash, shard_id, true) {
                let shard_uid = epoch_manager.shard_id_to_uid(shard_id, &chain_head.epoch_id)?;
                let status = flat_storage_manager.get_flat_storage_status(shard_uid);

                match status {
                    FlatStorageStatus::Ready(_) => {
                        flat_storage_manager.create_flat_storage_for_shard(shard_uid).unwrap();
                    }
                    FlatStorageStatus::Empty | FlatStorageStatus::Creation(_) => {
                        creation_needed = true;
                        shard_creators.insert(
                            shard_uid,
                            FlatStorageShardCreator::new(
                                shard_uid,
                                chain_head.height,
                                epoch_manager.clone(),
                                runtime.clone(),
                            ),
                        );
                    }
                    FlatStorageStatus::Disabled => {}
                }
            }
        }

        let flat_storage_creator = if creation_needed {
            Some(Self {
                shard_creators,
                pool: rayon::ThreadPoolBuilder::new().num_threads(num_threads).build().unwrap(),
            })
        } else {
            None
        };
        Ok(flat_storage_creator)
    }

    /// Updates statuses of underlying flat storage creation processes. Returns boolean
    /// indicating if all flat storages are created.
    pub fn update_status(&mut self, chain_store: &ChainStore) -> Result<bool, Error> {
        // TODO (#7327): If resharding happens, we may want to throw an error here.
        // TODO (#7327): If flat storage is created, the creator probably should be removed.

        let mut all_created = true;
        for shard_creator in self.shard_creators.values_mut() {
            all_created &= shard_creator.update_status(chain_store, &self.pool)?;
        }
        Ok(all_created)
    }
}
