//! Logic for writing trie nodes with the appropriate shard prefix.
//! This logic runs in parallel to chain processing.

use crate::types::RuntimeAdapter;
use near_chain_configs::{MutableConfigValue, ReshardingConfig};
use near_primitives::{hash::CryptoHash, state};
use near_store::{ShardUId, adapter::StoreAdapter, flat::ParentSplitParameters};
use std::sync::Arc;

pub struct TrieStoreResharder {
    runtime: Arc<dyn RuntimeAdapter>,
    /// Configuration for resharding.
    resharding_config: MutableConfigValue<ReshardingConfig>,
}

impl TrieStoreResharder {
    pub fn new(
        runtime: Arc<dyn RuntimeAdapter>,
        resharding_config: MutableConfigValue<ReshardingConfig>,
    ) -> Self {
        Self { runtime, resharding_config }
    }

    fn split_shard_task_impl(
        &self,
        shard_uid: ShardUId,
        state_root: CryptoHash, // TODO: fix
        parent_split_parameters: ParentSplitParameters,
    ) -> Result<(), String> {
        // Determines after how many bytes worth of key-values the process stops to commit changes
        // and to check cancellation.
        let batch_size = self.resharding_config.get().batch_size.as_u64() as usize;
        // Delay between every batch.
        let batch_delay = self.resharding_config.get().batch_delay.unsigned_abs();

        // Iterate over the memtrie and write the new trie nodes to the new store.
        let tries = self.runtime.get_tries();

        let shard_uid = parent_split_parameters.left_child_shard; // TODO: fix
        tries.get_trie_for_shard(shard_uid, state_root);
        let store = self.runtime.store().trie_store();
        let mut update = store.store_update();
        // update.increment_refcount(shard_uid, hash, data);

        todo!("unfinished");
    }
}

/// All different states of task execution for [TrieStoreReshardingEventStatus].
#[derive(Clone, Debug, Copy, Eq, PartialEq)]
pub enum TaskExecutionStatus {
    Started,
    NotStarted,
}

pub enum TrieStoreReshardingEventStatus {
    SplitShard(ShardUId, ParentSplitParameters, TaskExecutionStatus),
}

/// Result of a scheduled flat storage resharding task.
#[derive(Clone, Debug, Copy, Eq, PartialEq)]
pub enum ReshardingTaskResult {
    Successful { num_batches_done: usize },
    Failed,
    Cancelled,
    Postponed,
}

#[cfg(test)]
mod tests {
    use near_epoch_manager::test_utils;
    use near_primitives::shard_layout::ShardLayout;
    use near_store::{
        Trie,
        test_utils::{
            TestTriesBuilder, create_test_store, test_populate_flat_storage, test_populate_trie,
        },
        trie::AccessOptions,
    };

    use super::*;

    fn iterate_batch(
        trie: &Trie,
        previous_batch_last_key: Option<Vec<u8>>,
        batch_items: usize,
    ) -> Option<Vec<u8>> {
        let read_trie = trie.lock_for_iter();
        let mut iter = read_trie.iter().expect("failed to get iterator");
        if let Some(previous_batch_last_key) = &previous_batch_last_key {
            iter.seek(previous_batch_last_key).expect("failed to seek prefix");
            // So far, we just reached the state where last iteration terminated.
            // We need to clear the recorder to avoid double counting.
            _ = trie
                .recorded_as_trie_changes(Trie::EMPTY_ROOT)
                .expect("failed to get trie changes");
            // Skip this key as it was handled in the previous iteration
            iter.next().map(|result| result.expect("failed to iterate"));
        }

        for result in iter {
            let Ok((key, _value)) = result else {
                panic!("failed to iterate");
            };
            eprintln!("{key:?}, {_value:?}");
            // TODO: should we expect here?
            if trie.recorder_stats().map(|stats| stats.items_count).unwrap_or(0) >= batch_items {
                return Some(key);
            }
        }

        None // No more items to iterate
    }

    #[test]
    fn test_basics() {
        let shard_layout = ShardLayout::multi_shard(2, 0);
        let (shard_id, child_shard_id) = (
            shard_layout.get_shard_id(0).expect("failed to get shard id"),
            shard_layout.get_shard_id(1).expect("failed to get child shard id"),
        );
        let (shard_uid, child_shard_uid) = (
            ShardUId::from_shard_id_and_layout(shard_id, &shard_layout),
            ShardUId::from_shard_id_and_layout(child_shard_id, &shard_layout),
        );

        let store = create_test_store();
        let tries = TestTriesBuilder::new()
            .with_store(store.clone())
            .with_flat_storage(true)
            .with_in_memory_tries(true)
            .with_shard_layout(shard_layout)
            .build();

        let block_id = CryptoHash::default();

        // Create arbitrary data to populate the trie
        let initial = (0..1000)
            .map(|i| (Vec::from(test_utils::fake_hash(i)), Some(vec![i as u8])))
            .collect::<Vec<_>>();

        test_populate_flat_storage(&tries, shard_uid, &block_id, &block_id, &initial);
        let root = test_populate_trie(&tries, &Trie::EMPTY_ROOT, shard_uid, initial.clone());

        // Iterate memtrie
        let trie: Trie = tries.get_trie_for_shard(shard_uid, root).recording_reads_new_recorder();

        let batch_size = 20;
        let mut last_key: Option<Vec<u8>> = None;
        loop {
            last_key = iterate_batch(&trie, last_key, batch_size);
            let trie_changes =
                trie.recorded_as_trie_changes(root).expect("failed to get trie changes");
            let mut update = store.trie_store().store_update();
            tries.apply_all(&trie_changes, child_shard_uid, &mut update);
            update.commit().expect("failed to commit update");
            eprintln!("Recorded changes, last_key = {:?}", last_key);

            if last_key.is_none() {
                break;
            }
        }

        // Now we should be able to read the data from the child shard
        // Note view_trie bypasses memtrie and reads directly from the store.
        let child_trie = tries.get_view_trie_for_shard(child_shard_uid, root);
        for (key, expected) in initial {
            let value = child_trie.get(&key, AccessOptions::DEFAULT).expect("failed to get value");
            assert_eq!(expected, value);
        }
    }
}
