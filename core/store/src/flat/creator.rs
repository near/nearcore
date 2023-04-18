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
use crate::{Store, Trie, TrieDBStorage, TrieTraversalItem, WrappedTrieChanges, StoreUpdate};
use crossbeam_channel::{unbounded, Receiver, Sender};
use itertools::Itertools;
use near_o11y::metrics::IntGauge;
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

use super::FlatStateDelta;
use super::types::{CatchingUpStatus, SavingDeltasStatus};

/// If we launched a node with enabled flat storage but it doesn't have flat storage data on disk, we have to create it.
/// This struct is responsible for this process for the given shard.
pub struct FlatStorageShardCreator {
    context: CreatorContext,
    state: CreatorState,
}

struct CreatorContext {
    shard_uid: ShardUId,
    metrics: FlatStorageCreationMetrics,
}

/// See doc comment on [`FlatStorageCreationStatus`] for the details of the process.
#[derive(Clone)]
enum CreatorState {
    Empty,
    SavingDeltas(SavingDeltasStatus),
    FetchingState(FetchingStateState),
    CatchingUp(CatchingUpStatus),
    Done,
}

impl CreatorState {
    fn handle_empty_state(
        context: &CreatorContext,
        store: &Store,
        block: BlockInfo,
    ) -> io::Result<CreatorState> {
        let next_status = SavingDeltasStatus {
            start_height: block.height,
        };
        let mut store_update = store.store_update();
        store_helper::set_flat_storage_status(
            &mut store_update,
            context.shard_uid,
            FlatStorageCreationStatus::SavingDeltas(next_status.clone()).into(),
        );
        store_update.commit()?;
        Ok(CreatorState::SavingDeltas(next_status))
    }

    fn handle_saving_deltas(
        status: &SavingDeltasStatus,
        context: &CreatorContext,
        store: &Store,
        final_head: BlockInfo,
        final_head_state_root: StateRoot,
    ) -> io::Result<CreatorState> {
        let final_height = final_head.height;
        if final_height <= status.start_height {
            return Ok(CreatorState::SavingDeltas(status.clone()));
        }
        let shard_id = context.shard_uid.shard_id();
        let next_status = FetchingStateStatus {
            block: final_head,
            state_root: final_head_state_root,
            part_id: 0,
            num_parts: calc_num_state_parts(store, context.shard_uid, final_head_state_root),
        };
        let mut store_update = store.store_update();
        store_helper::set_flat_storage_status(
            &mut store_update,
            context.shard_uid,
            FlatStorageCreationStatus::FetchingState(next_status.clone()).into(),
        );
        store_update.commit()?;
        context.metrics.set_flat_head_height(final_height);
        info!(target: "store", %shard_id, %final_height, ?status, "Switching status to fetching state");
        Ok(CreatorState::FetchingState(FetchingStateState::new(next_status, None)))
    }

    fn handle_fetching_state(
        state: &FetchingStateState,
        context: &CreatorContext,
        store: &Store,
        thread_pool: &rayon::ThreadPool,
    ) -> io::Result<CreatorState> {
        let next_start_part_id = Self::next_start_part_id(&state.status);
        let FetchingStateStatus { block, state_root, part_id: start_part_id, num_parts } =
            state.status.clone();
        let block_hash = block.hash;
        context.metrics.set_remaining_state_parts(num_parts - start_part_id);
        let shard_id = context.shard_uid.shard_id();
        let next_state = if let Some(state_parts) = state.remaining_state_parts {
            // If not all state parts were fetched, try receiving new results.
            let mut remaining_state_parts = state_parts;
            while let Ok(num_items) = state.fetched_parts_receiver.try_recv() {
                remaining_state_parts -= 1;
                context.metrics.inc_fetched_state(num_items);
            }
            if remaining_state_parts > 0 {
                CreatorState::FetchingState(FetchingStateState {
                    remaining_state_parts: Some(remaining_state_parts),
                    ..state.clone()
                })
            } else if !next_start_part_id < num_parts {
                // There are still remaining state parts, so switch status to the new range of state parts.
                let new_status = FetchingStateStatus {
                    block,
                    state_root,
                    part_id: next_start_part_id,
                    num_parts,
                };
                debug!(target: "chain", %shard_id, %block_hash, ?new_status);
                let mut store_update = store.store_update();
                store_helper::set_flat_storage_status(
                    &mut store_update,
                    context.shard_uid,
                    FlatStorageCreationStatus::FetchingState(new_status.clone()).into(),
                );
                store_update.commit()?;
                CreatorState::FetchingState(Self::schedule_save_parts(
                    &new_status,
                    context,
                    store,
                    thread_pool,
                ))
            } else {
                // All parts were fetched, we can start catchup.
                info!(target: "chain", %shard_id, ?block, "Finished fetching state");
                context.metrics.set_remaining_state_parts(0);
                let next_status = CatchingUpStatus { flat_head: block };
                let mut store_update = store.store_update();
                store_helper::remove_delta(&mut store_update, context.shard_uid, block_hash);
                store_helper::set_flat_storage_status(
                    &mut store_update,
                    context.shard_uid,
                    FlatStorageCreationStatus::CatchingUp(next_status.clone()).into(),
                );
                store_update.commit()?;
                CreatorState::CatchingUp(next_status)
            }
        } else {
            CreatorState::FetchingState(Self::schedule_save_parts(
                &state.status,
                context,
                store,
                thread_pool,
            ))
        };
        Ok(next_state)
    }

    fn handle_catching_up(
        status: &CatchingUpStatus,
        context: &CreatorContext,
        store: &Store,
        final_head: BlockInfo,
    ) -> io::Result<CreatorState> {
        /// Maximal number of blocks which can be caught up during one step.
        const CATCH_UP_BLOCKS: usize = 50;

        let shard_id = context.shard_uid.shard_id();
        let delta_blocks = store_helper::get_all_deltas_metadata(store, context.shard_uid)
            .unwrap_or_else(|_| {
                panic!("Cannot read flat state deltas metadata for shard {shard_id} from storage")
            })
            .into_iter()
            .map(|metadata| metadata.block)
            .collect_vec();
        let final_head_path = find_block_path(&delta_blocks, &final_head);

        let mut store_update = store.store_update();

        // Merge up to 50 deltas of the next blocks until we reach chain final head.
        let catch_up_blocks = final_head_path.iter().take(CATCH_UP_BLOCKS).collect_vec();
        let mut flat_head = status.flat_head.clone();
        let mut merged_changes = FlatStateChanges::default();
        for &block in &catch_up_blocks {
            assert!(block.prev_hash == flat_head.hash);
            let changes = store_helper::get_delta_changes(store, context.shard_uid, block.hash)
                .unwrap()
                .unwrap();
            merged_changes.merge(changes);
            store_helper::remove_delta(&mut store_update, context.shard_uid, block.hash);
            flat_head = block.clone();
        }

        debug!(
            target: "chain",
            %shard_id,
            old_flat_head = ?status.flat_head,
            new_flat_head = ?flat_head,
            blocks_caught_up = catch_up_blocks.len(),
            "Catching up flat head"
        );
        context.metrics.set_flat_head_height(flat_head.height);
        merged_changes.apply_to_flat_state(&mut store_update, context.shard_uid);

        let next_state = if flat_head == final_head {
            // GC deltas from forks which could have appeared on chain during catchup.
            // Assuming that flat storage creation finishes in < 2 days, all deltas metadata cannot occupy
            // more than 2 * (Blocks per day = 48 * 60 * 60) * (BlockInfo size = 72) ~= 12.4 MB.
            let mut store_update = store.store_update();
            let mut deltas_gc_count = 0;
            for block in delta_blocks {
                if block.height <= final_head.height {
                    store_helper::remove_delta(&mut store_update, context.shard_uid, block.hash);
                    deltas_gc_count += 1;
                }
            }
            info!(target: "chain", %shard_id, ?flat_head, %deltas_gc_count, "Flat storage creation done");

            // If we reached chain final head, we can finish catchup and finally create flat storage.
            store_helper::set_flat_storage_status(
                &mut store_update,
                context.shard_uid,
                FlatStorageStatus::Ready(FlatStorageReadyStatus { flat_head }),
            );
            store_update.commit()?;
            CreatorState::Done
        } else {
            let next_status = CatchingUpStatus { flat_head };
            let mut store_update = store.store_update();
            store_helper::set_flat_storage_status(
                &mut store_update,
                context.shard_uid,
                FlatStorageCreationStatus::CatchingUp(next_status.clone()).into(),
            );
            store_update.commit()?;
            CreatorState::CatchingUp(next_status)
        };
        Ok(next_state)
    }

    fn schedule_save_parts(
        status: &FetchingStateStatus,
        context: &CreatorContext,
        store: &Store,
        thread_pool: &rayon::ThreadPool,
    ) -> FetchingStateState {
        let next_start_part_id = Self::next_start_part_id(&status);
        let FetchingStateStatus { block, state_root, part_id: start_part_id, num_parts } =
            status.clone();
        // We need to spawn threads to fetch state parts and fill flat storage data.
        let progress = Arc::new(AtomicU64::new(0));
        debug!(
            target: "store",
            shard_id = %context.shard_uid.shard_id(),
            ?block,
            %start_part_id,
            %next_start_part_id,
            %num_parts,
            "Spawning threads to fetch state parts for flat storage"
        );
        let state =
            FetchingStateState::new(status.clone(), Some(next_start_part_id - start_part_id));
        for part_id in start_part_id..next_start_part_id {
            let saver = StatePartSaver {
                part_id: PartId::new(part_id, num_parts),
                state_root,
                shard_uid: context.shard_uid,
                store: store.clone(),
                progress: progress.clone(),
                result_sender: state.fetched_parts_sender.clone(),
            };
            saver.spawn_save(thread_pool, context.metrics.threads_used());
        }
        state
    }

    fn next_start_part_id(fetching_state_status: &FetchingStateStatus) -> u64 {
        min(fetching_state_status.num_parts, fetching_state_status.part_id + NUM_PARTS_IN_ONE_STEP)
    }
}

#[derive(Clone)]
struct FetchingStateState {
    status: FetchingStateStatus,
    /// Tracks number of state parts which are not fetched yet during a single step.
    /// Stores Some(parts) if threads for fetching state were spawned and None otherwise.
    remaining_state_parts: Option<u64>,
    /// Used by threads which traverse state parts to tell that traversal is finished.
    fetched_parts_sender: Sender<u64>,
    /// Used by main thread to update the number of traversed state parts.
    fetched_parts_receiver: Receiver<u64>,
}

impl FetchingStateState {
    fn new(status: FetchingStateStatus, remaining_state_parts: Option<u64>) -> Self {
        let (fetched_parts_sender, fetched_parts_receiver) = unbounded();
        FetchingStateState {
            status,
            remaining_state_parts,
            fetched_parts_sender,
            fetched_parts_receiver,
        }
    }
}

impl FlatStorageShardCreator {
    pub fn empty(shard_uid: ShardUId) -> Self {
        todo!()
    }

    pub fn from_status(status: FlatStorageCreationStatus) -> Self {
        todo!()
    }

    fn new(shard_uid: ShardUId, state: CreatorState) -> Self {
        Self {
            state,
            context: CreatorContext {
                shard_uid,
                metrics: FlatStorageCreationMetrics::new(shard_uid.shard_id()),
            },
        }
    }

    pub fn process_final_block(
        &mut self,
        store: &Store,
        final_head: BlockInfo,
        final_head_state_root: StateRoot,
    ) -> io::Result<()> {
        if let CreatorState::SavingDeltas(status) = &self.state {
            self.state = CreatorState::handle_saving_deltas(
                status,
                &self.context,
                store,
                final_head,
                final_head_state_root,
            )?;
        }
        Ok(())
    }

    pub fn add_delta(
        &mut self,
        store: &Store,
        detla: FlatStateDelta,
    ) -> io::Result<StoreUpdate> {
        if matches!(self.state, CreatorState::Empty) {
            self.state = CreatorState::handle_empty_state(&self.context, store, detla.metadata.block)?;
        }
        todo!("save deltas");
    }

    pub fn update(
        &mut self,
        store: &Store,
        final_head: BlockInfo,
        thread_pool: &rayon::ThreadPool,
    ) -> io::Result<bool> {
        self.state = match &self.state {
            CreatorState::Empty | CreatorState::SavingDeltas(_) => { todo!("nothing") },
            CreatorState::FetchingState(state) => {
                CreatorState::handle_fetching_state(state, &self.context, store, thread_pool)?
            }
            CreatorState::CatchingUp(status) => {
                CreatorState::handle_catching_up(status, &self.context, store, final_head)?
            }
            CreatorState::Done => panic!("Done"),
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

struct StatePartSaver {
    part_id: PartId,
    store: Store,
    shard_uid: ShardUId,
    state_root: StateRoot,
    progress: Arc<AtomicU64>,
    result_sender: Sender<u64>,
}

impl StatePartSaver {
    fn spawn_save(self, thread_pool: &rayon::ThreadPool, threads_used: IntGauge) {
        thread_pool.spawn(move || {
            threads_used.inc();
            self.save();
            threads_used.dec();
        });
    }

    /// Fetch state part, write all state items to flat storage and send the number of items to the given channel.
    fn save(self) {
        let part_id = self.part_id;
        let trie_storage = TrieDBStorage::new(self.store.clone(), self.shard_uid);
        let trie = Trie::new(Box::new(trie_storage), self.state_root, None);
        let path_begin = trie.find_path_for_part_boundary(part_id.idx, part_id.total).unwrap();
        let path_end = trie.find_path_for_part_boundary(part_id.idx + 1, part_id.total).unwrap();
        let hex_path_begin = Self::nibbles_to_hex(&path_begin);
        debug!(target: "store", "Preload state part from {hex_path_begin}");
        let mut trie_iter = trie.iter().unwrap();

        let mut store_update = self.store.store_update();
        let mut num_items = 0;
        for TrieTraversalItem { hash, key } in
            trie_iter.visit_nodes_interval(&path_begin, &path_end).unwrap()
        {
            if let Some(key) = key {
                let value = trie.storage.retrieve_raw_bytes(&hash).unwrap();
                let value_ref = ValueRef::new(&value);
                store_helper::set_ref(&mut store_update, self.shard_uid, key, Some(value_ref))
                    .expect("Failed to put value in FlatState");
                num_items += 1;
            }
        }
        store_update.commit().unwrap();

        let processed_parts = self.progress.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;

        debug!(target: "store",
            "Preload subtrie at {hex_path_begin} done, \
            loaded {num_items} state items, \
            proccessed parts: {processed_parts}"
        );

        self.result_sender.send(num_items).unwrap();
    }

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
}

fn calc_num_state_parts(
    store: &Store,
    shard_uid: ShardUId,
    final_head_state_root: StateRoot,
) -> u64 {
    let trie_storage = TrieDBStorage::new(store.clone(), shard_uid);
    let trie = Trie::new(Box::new(trie_storage), final_head_state_root, None);
    let root_node = trie.retrieve_root_node().unwrap();
    root_node.memory_usage / STATE_PART_MEMORY_LIMIT.as_u64() + 1
}
