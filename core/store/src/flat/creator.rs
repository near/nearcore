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

use crate::flat::{
    store_helper, BlockInfo, FetchingStateStatus, FlatStateChanges, FlatStorageCreationMetrics,
    FlatStorageCreationStatus, FlatStorageReadyStatus, FlatStorageStatus, NUM_PARTS_IN_ONE_STEP,
    STATE_PART_MEMORY_LIMIT,
};
use crate::{Store, Trie, TrieDBStorage, TrieTraversalItem};
use crossbeam_channel::{unbounded, Receiver, Sender};
use near_primitives::shard_layout::ShardUId;
use near_primitives::state::ValueRef;
use near_primitives::state_part::PartId;
use near_primitives::types::{BlockHeight, StateRoot};
use std::cmp::min;
use std::collections::HashMap;
use std::io;
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

    pub fn new(shard_uid: ShardUId, start_height: BlockHeight) -> Self {
        let (fetched_parts_sender, fetched_parts_receiver) = unbounded();
        Self {
            shard_uid,
            start_height,
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
        let trie = Trie::new(Box::new(trie_storage), state_root, None);
        let path_begin = trie.find_path_for_part_boundary(part_id.idx, part_id.total).unwrap();
        let path_end = trie.find_path_for_part_boundary(part_id.idx + 1, part_id.total).unwrap();
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
                let value_ref = ValueRef::new(&value);
                store_helper::set_ref(&mut store_update, shard_uid, key, Some(value_ref))
                    .expect("Failed to put value in FlatState");
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

        result_sender.send(num_items).unwrap();
    }

    /// Checks current flat storage creation status, execute work related to it and possibly switch to next status.
    /// Creates flat storage when all intermediate steps are finished.
    /// Returns boolean indicating if flat storage was created.
    pub fn update_status(
        &mut self,
        store: &Store,
        final_head: BlockInfo,
        final_head_state_root: StateRoot,
        thread_pool: &rayon::ThreadPool,
    ) -> io::Result<bool> {
        let shard_id = self.shard_uid.shard_id();
        let current_status = store_helper::get_flat_storage_status(&store, self.shard_uid);
        self.metrics.set_status(&current_status);
        match current_status {
            FlatStorageStatus::Empty => {
                let mut store_update = store.store_update();
                store_helper::set_flat_storage_status(
                    &mut store_update,
                    self.shard_uid,
                    FlatStorageStatus::Creation(FlatStorageCreationStatus::SavingDeltas),
                );
                store_update.commit()?;
            }
            FlatStorageStatus::Creation(FlatStorageCreationStatus::SavingDeltas) => {
                let final_height = final_head.height;
                if final_height > self.start_height {
                    // We continue saving deltas, and also start fetching state.
                    let trie_storage = TrieDBStorage::new(store.clone(), self.shard_uid);
                    let trie = Trie::new(Box::new(trie_storage), final_head_state_root, None);
                    let root_node = trie.retrieve_root_node().unwrap();
                    let num_state_parts =
                        root_node.memory_usage / STATE_PART_MEMORY_LIMIT.as_u64() + 1;
                    let status = FetchingStateStatus {
                        block: final_head,
                        state_root: final_head_state_root,
                        part_id: 0,
                        num_parts_in_step: NUM_PARTS_IN_ONE_STEP,
                        num_parts: num_state_parts,
                    };
                    info!(target: "store", %shard_id, %final_height, ?status, "Switching status to fetching state");

                    let mut store_update = store.store_update();
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
                FetchingStateStatus {
                    block,
                    state_root,
                    part_id: start_part_id,
                    num_parts_in_step,
                    num_parts,
                },
            )) => {
                let block_hash = block.hash;
                let next_start_part_id = min(num_parts, start_part_id + num_parts_in_step);
                self.metrics.set_remaining_state_parts(num_parts - start_part_id);

                match self.remaining_state_parts {
                    None => {
                        // We need to spawn threads to fetch state parts and fill flat storage data.
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
                                    self.shard_uid,
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

                        let mut store_update = store.store_update();
                        if next_start_part_id < num_parts {
                            // If there are still remaining state parts, switch status to the new range of state parts.
                            // We will spawn new rayon tasks on the next status update.
                            let new_status = FetchingStateStatus {
                                block,
                                state_root,
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
                let mut flat_head = old_flat_head;
                let mut merged_changes = FlatStateChanges::default();
                let mut store_update = store.store_update();
                let delta_blocks: Vec<_> = store_helper::get_all_deltas_metadata(
                    store,
                    self.shard_uid,
                )
                .unwrap_or_else(|_| {
                    panic!(
                        "Cannot read flat state deltas metadata for shard {shard_id} from storage"
                    )
                })
                .iter()
                .map(|metadata| metadata.block)
                .collect();
                let final_head_path = find_block_path(&delta_blocks, &final_head);

                // Merge up to 50 deltas of the next blocks until we reach chain final head.
                // TODO: consider merging 10 deltas at once to limit memory usage
                let mut blocks_caught_up = 0;
                // kek
                let mut height = final_head.height;
                for block in final_head_path.iter().take(Self::CATCH_UP_BLOCKS) {
                    assert!(block.prev_hash == flat_head);
                    flat_head = block.hash;
                    height = block.height;
                    let changes = store_helper::get_delta_changes(store, self.shard_uid, flat_head)
                        .unwrap()
                        .unwrap();
                    merged_changes.merge(changes);
                    store_helper::remove_delta(&mut store_update, self.shard_uid, flat_head);
                    blocks_caught_up += 1;
                }

                debug!(target: "chain", %shard_id, %old_flat_head, %flat_head, %height, blocks_caught_up, "Catching up flat head");
                self.metrics.set_flat_head_height(height);
                merged_changes.apply_to_flat_state(&mut store_update, self.shard_uid);
                if flat_head == final_head.hash {
                    // GC deltas from forks which could have appeared on chain during catchup.
                    // Assuming that flat storage creation finishes in < 2 days, all deltas metadata cannot occupy
                    // more than 2 * (Blocks per day = 48 * 60 * 60) * (BlockInfo size = 72) ~= 12.4 MB.
                    let mut store_update = store.store_update();
                    let mut gc_count = 0;
                    for block in delta_blocks {
                        if block.height <= final_head.height {
                            store_helper::remove_delta(
                                &mut store_update,
                                self.shard_uid,
                                block.hash,
                            );
                            gc_count += 1;
                        }
                    }
                    info!(target: "chain", %shard_id, %flat_head, %height, "Garbage collected {gc_count} deltas");

                    // If we reached chain final head, we can finish catchup and finally create flat storage.
                    store_helper::set_flat_storage_status(
                        &mut store_update,
                        self.shard_uid,
                        FlatStorageStatus::Ready(FlatStorageReadyStatus { flat_head: final_head }),
                    );
                    // kek
                    //self.runtime_adapter.create_flat_storage_for_shard(shard_uid);
                    info!(target: "chain", %shard_id, %flat_head, %height, "Flat storage creation done");
                } else {
                    store_helper::set_flat_storage_status(
                        &mut store_update,
                        self.shard_uid,
                        FlatStorageStatus::Creation(FlatStorageCreationStatus::CatchingUp(
                            flat_head,
                        )),
                    );
                }
                store_update.commit()?;
            }
            FlatStorageStatus::Ready(_) => return Ok(true),
            FlatStorageStatus::Disabled => {
                panic!("initiated flat storage creation for shard {shard_id} while it is disabled");
            }
        };
        Ok(false)
    }
}

fn find_block_path(all_blocks: &[BlockInfo], block: &BlockInfo) -> Vec<BlockInfo> {
    let map: HashMap<_, _> = all_blocks.iter().map(|b| (b.hash, b)).collect();
    let mut ret = vec![];
    if map.contains_key(&block.hash) {
        ret.push(block.clone());
    }
    while let Some(&cur) = ret.last().and_then(|b| map.get(&b.prev_hash)) {
        ret.push(cur.clone());
    }
    ret.reverse();
    ret
}
