use borsh::{BorshSerialize, BorshDeserialize};
use crossbeam_channel::{unbounded, Receiver, Sender};
use near_o11y::metrics::{IntCounter, IntGauge};
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::state::ValueRef;
use near_primitives::state_part::PartId;
use near_primitives::types::{BlockHeight, StateRoot};
use crate::flat::store_helper;
use crate::migrations::BatchedStoreUpdate;
use crate::{Store, DBCol, FLAT_STORAGE_HEAD_HEIGHT, Trie, TrieDBStorage, TrieTraversalItem};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tracing::debug;
use tracing::info;

use super::FlatStorage;
use super::delta::FlatStateDelta;
use super::types::{BlockHash, ChainAccessForFlatStorage};

pub const NUM_PARTS_IN_ONE_STEP: u64 = 20;

/// Memory limit for state part being fetched.
pub const STATE_PART_MEMORY_LIMIT: bytesize::ByteSize = bytesize::ByteSize(10 * bytesize::MIB);

/// Current step of fetching state to fill flat storage.
#[derive(BorshSerialize, BorshDeserialize, Copy, Clone, Debug, PartialEq, Eq)]
pub struct FetchingStateStatus {
    /// Hash of block on top of which we create flat storage.
    pub block_hash: CryptoHash,
    /// Number of the first state part to be fetched in this step.
    pub part_id: u64,
    /// Number of parts fetched in one step.
    pub num_parts_in_step: u64,
    /// Total number of state parts.
    pub num_parts: u64,
}

/// If a node has flat storage enabled but it didn't have flat storage data on disk, its creation should be initiated.
/// Because this is a heavy work requiring ~5h for testnet rpc node and ~10h for testnet archival node, we do it on
/// background during regular block processing.
/// This struct reveals what is the current status of creating flat storage data on disk.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum FlatStorageCreationStatus {
    /// Flat storage state does not exist. We are saving `FlatStorageDelta`s to disk.
    /// During this step, we save current chain head, start saving all deltas for blocks after chain head and wait until
    /// final chain head moves after saved chain head.
    SavingDeltas,
    /// Flat storage state misses key-value pairs. We need to fetch Trie state to fill flat storage for some final chain
    /// head. It is the heaviest work, so it is done in multiple steps, see comment for `FetchingStateStatus` for more
    /// details.
    /// During each step we spawn background threads to fill some contiguous range of state keys.
    /// Status contains block hash for which we fetch the shard state and number of current step. Progress of each step
    /// is saved to disk, so if creation is interrupted during some step, we don't repeat previous steps, starting from
    /// the saved step again.
    FetchingState(FetchingStateStatus),
    /// Flat storage data exists on disk but block which is corresponds to is earlier than chain final head.
    /// We apply deltas from disk until the head reaches final head.
    /// Includes block hash of flat storage head.
    CatchingUp(CryptoHash),
    /// Flat storage is ready to use.
    Ready,
    /// Flat storage cannot be created.
    DontCreate,
}

impl Into<i64> for &FlatStorageCreationStatus {
    /// Converts status to integer to export to prometheus later.
    /// Cast inside enum does not work because it is not fieldless.
    fn into(self) -> i64 {
        match self {
            FlatStorageCreationStatus::SavingDeltas => 0,
            FlatStorageCreationStatus::FetchingState(_) => 1,
            FlatStorageCreationStatus::CatchingUp(_) => 2,
            FlatStorageCreationStatus::Ready => 3,
            FlatStorageCreationStatus::DontCreate => 4,
        }
    }
}

/// Metrics reporting about flat storage creation progress on each status update.
struct FlatStorageCreationMetrics {
    status: IntGauge,
    flat_head_height: IntGauge,
    remaining_state_parts: IntGauge,
    fetched_state_parts: IntCounter,
    fetched_state_items: IntCounter,
    threads_used: IntGauge,
}

pub struct FlatStorageCreator {
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

impl FlatStorageCreator {
    /// Maximal number of blocks which can be caught up during one step.
    const CATCH_UP_BLOCKS: usize = 50;

    pub fn new(
        shard_uid: ShardUId,
        start_height: BlockHeight,
    ) -> Self {
        let (fetched_parts_sender, fetched_parts_receiver) = unbounded();
        // `itoa` is much faster for printing shard_id to a string than trivial alternatives.
        let mut buffer = itoa::Buffer::new();
        let shard_id_label = buffer.format(shard_uid.shard_id);

        Self {
            shard_uid,
            start_height,
            remaining_state_parts: None,
            fetched_parts_sender,
            fetched_parts_receiver,
            metrics: FlatStorageCreationMetrics {
                status: crate::flat_state_metrics::FLAT_STORAGE_CREATION_STATUS
                    .with_label_values(&[shard_id_label]),
                flat_head_height: FLAT_STORAGE_HEAD_HEIGHT.with_label_values(&[shard_id_label]),
                remaining_state_parts:
                    crate::flat_state_metrics::FLAT_STORAGE_CREATION_REMAINING_STATE_PARTS
                        .with_label_values(&[shard_id_label]),
                fetched_state_parts:
                    crate::flat_state_metrics::FLAT_STORAGE_CREATION_FETCHED_STATE_PARTS
                        .with_label_values(&[shard_id_label]),
                fetched_state_items:
                    crate::flat_state_metrics::FLAT_STORAGE_CREATION_FETCHED_STATE_ITEMS
                        .with_label_values(&[shard_id_label]),
                threads_used: crate::flat_state_metrics::FLAT_STORAGE_CREATION_THREADS_USED
                    .with_label_values(&[shard_id_label]),
            },
        }
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

    /// Fetch state part, write all state items to flat storage and send the number of items to the given channel.
    fn fetch_state_part(
        store: &Store,
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
                    store_update
                        .set_ser(DBCol::FlatState, &key, &value_ref)
                        .expect("Failed to put value in FlatState");
                    num_items += 1;
                }
            }
        }
        store_update.finish().unwrap();

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
        thread_pool: &rayon::ThreadPool,
        final_block_hash: BlockHash,
        final_block_state_root: CryptoHash,
        chain_access: &dyn ChainAccessForFlatStorage,
    ) -> std::io::Result<Option<FlatStorage>> {
        let shard_id = self.shard_uid.shard_id();
        let current_status =
            store_helper::get_flat_storage_creation_status(store, shard_id);
        self.metrics.status.set((&current_status).into());
        let final_block_height = chain_access.get_block_info(&final_block_hash).height;
        match &current_status {
            FlatStorageCreationStatus::SavingDeltas => {
                if final_block_height > self.start_height {
                    // If it holds, deltas for all blocks after final head are saved to disk, because they have bigger
                    // heights than one on which we launched a node. Check that it is true:
                    // TODO(pugachag): is this really needed?
                    /*
                    for height in final_height + 1..=chain_store.head()?.height {
                        // We skip heights for which there are no blocks, because certain heights can be skipped.
                        // TODO (#8057): make `get_all_block_hashes_by_height` return empty hashmap instead of error
                        // in such case.
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
                    */

                    // We continue saving deltas, and also start fetching state.
                    let trie_storage = TrieDBStorage::new(store.clone(), self.shard_uid);
                    let trie = Trie::new(Box::new(trie_storage), final_block_state_root, None);
                    let root_node = trie.retrieve_root_node().unwrap();
                    let num_state_parts =
                        root_node.memory_usage / STATE_PART_MEMORY_LIMIT.as_u64() + 1;
                    let status = FetchingStateStatus {
                        block_hash: final_block_hash,
                        part_id: 0,
                        num_parts_in_step: NUM_PARTS_IN_ONE_STEP,
                        num_parts: num_state_parts,
                    };
                    info!(target: "store", %shard_id, %final_block_height, ?status, "Switching status to fetching state");

                    let mut store_update = store.store_update();
                    self.metrics.flat_head_height.set(final_block_height as i64);
                    store_helper::set_flat_storage_creation_status(
                        &mut store_update,
                        shard_id,
                        FlatStorageCreationStatus::FetchingState(status),
                    );
                    store_update.commit()?;
                }
            }
            FlatStorageCreationStatus::FetchingState(fetching_state_status) => {
                let store = store.clone();
                let block_hash = fetching_state_status.block_hash;
                let start_part_id = fetching_state_status.part_id;
                let num_parts_in_step = fetching_state_status.num_parts_in_step;
                let num_parts = fetching_state_status.num_parts;
                let next_start_part_id = num_parts.min(start_part_id + num_parts_in_step);
                self.metrics.remaining_state_parts.set((num_parts - start_part_id) as i64);

                match self.remaining_state_parts.clone() {
                    None => {
                        // We need to spawn threads to fetch state parts and fill flat storage data.
                        let progress = Arc::new(std::sync::atomic::AtomicU64::new(0));
                        debug!(
                            target: "store", %shard_id, %block_hash, %start_part_id, %next_start_part_id, %num_parts,
                            "Spawning threads to fetch state parts for flat storage"
                        );

                        for part_id in start_part_id..next_start_part_id {
                            let inner_state_root = final_block_state_root.clone();
                            let inner_progress = progress.clone();
                            let inner_sender = self.fetched_parts_sender.clone();
                            let inner_threads_used = self.metrics.threads_used.clone();
                            thread_pool.spawn(move || {
                                inner_threads_used.inc();
                                Self::fetch_state_part(
                                    &store,
                                    self.shard_uid,
                                    inner_state_root,
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
                            self.metrics.fetched_state_items.inc_by(num_items);
                            self.metrics.fetched_state_parts.inc();
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
                                block_hash,
                                part_id: next_start_part_id,
                                num_parts_in_step,
                                num_parts,
                            };
                            debug!(target: "chain", %shard_id, %block_hash, ?new_status);
                            store_helper::set_flat_storage_creation_status(
                                &mut store_update,
                                shard_id,
                                FlatStorageCreationStatus::FetchingState(new_status),
                            );
                        } else {
                            // If all parts were fetched, we can start catchup.
                            info!(target: "chain", %shard_id, %block_hash, "Finished fetching state");
                            self.metrics.remaining_state_parts.set(0);
                            store_helper::set_flat_storage_creation_status(
                                &mut store_update,
                                shard_id,
                                FlatStorageCreationStatus::CatchingUp(block_hash),
                            );
                        }
                        store_update.commit()?;
                    }
                }
            }
            FlatStorageCreationStatus::CatchingUp(old_flat_head) => {
                let mut flat_head = old_flat_head.clone();
                let mut merged_delta = FlatStateDelta::default();

                // TODO(pugachag): this should not be handled here, use shared code
                // Merge up to 50 deltas of the next blocks until we reach chain final head.
                // TODO: consider merging 10 deltas at once to limit memory usage
                for _ in 0..Self::CATCH_UP_BLOCKS {
                    let height = chain_access.get_block_info(&flat_head).height;
                    if height > final_block_height {
                        panic!("New flat head moved too far: new head = {flat_head}, height = {height}, final block height = {}", final_block_height);
                    }
                    // Stop if we reached chain final head.
                    if flat_head == final_block_hash {
                        break;
                    }
                    // TODO(pugachag): find next final block at height + 1
                    // flat_head = chain_store.get_next_block_hash(&flat_head).unwrap();
                    let delta =
                        store_helper::get_delta(store, shard_id, flat_head).unwrap().unwrap();
                    merged_delta.merge(delta.as_ref());
                }
                if (old_flat_head != &flat_head) || (flat_head == final_block_hash)
                {
                    // If flat head changes, save all changes to store.
                    let old_height = chain_access.get_block_info(&old_flat_head).height;
                    let height = chain_access.get_block_info(&flat_head).height;
                    debug!(target: "chain", %shard_id, %old_flat_head, %old_height, %flat_head, %height, "Catching up flat head");
                    self.metrics.flat_head_height.set(height as i64);
                    let mut store_update = store.store_update();
                    merged_delta.apply_to_flat_state(&mut store_update);

                    if flat_head == final_block_hash {
                        // If we reached chain final head, we can finish catchup and finally create flat storage.
                        store_helper::remove_flat_storage_creation_status(
                            &mut store_update,
                            shard_id,
                        );
                        store_helper::set_flat_head(&mut store_update, shard_id, &flat_head);
                        store_update.commit()?;
                        // TODO(pugachag): return new instance
                        /*
                        self.runtime_adapter.create_flat_storage_state_for_shard(
                            shard_id,
                            chain_store.head().unwrap().height,
                            chain_store,
                        );
                        */
                        info!(target: "chain", %shard_id, %flat_head, %height, "Flat storage creation done");
                    } else {
                        store_helper::set_flat_storage_creation_status(
                            &mut store_update,
                            shard_id,
                            FlatStorageCreationStatus::CatchingUp(flat_head),
                        );
                        store_update.commit()?;
                    }
                }
            }
            FlatStorageCreationStatus::Ready => {}
            FlatStorageCreationStatus::DontCreate => {
                panic!("We initiated flat storage creation for shard {shard_id} but according to flat storage state status in db it cannot be created");
            }
        };
        Ok(None)
    }
}