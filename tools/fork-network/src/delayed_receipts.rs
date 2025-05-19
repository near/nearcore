use crate::storage_mutator::ShardUpdateState;

use near_crypto::PublicKey;
use near_primitives::borsh;
use near_primitives::receipt::{Receipt, ReceiptOrStateStoredReceipt, TrieQueueIndices};
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{ShardIndex, StateRoot};
use near_store::{ShardTries, Trie};

use anyhow::Context;
use std::borrow::Cow;
use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap};

// Keeps track of the mapping of delayed receipts from a source shard to the
// shards their receivers will belong to in the forked chain
// We keep only the indices to save on memory usage even though we could store the receipts themselves.
// Later when we commit the changes we read the receipts from the trie again.
pub(crate) struct DelayedReceiptTracker {
    source_shard_uid: ShardUId,
    // indices[target_shard_idx] contains the delayed receipt indices in self.source_shard_uid that
    // will belong to target_shard_idx after the account IDs are mapped
    indices: Vec<BTreeSet<u64>>,
}

impl DelayedReceiptTracker {
    pub(crate) fn new(source_shard_uid: ShardUId, num_shards: usize) -> Self {
        Self { source_shard_uid, indices: vec![BTreeSet::new(); num_shards] }
    }

    pub(crate) fn push(&mut self, target_shard_idx: ShardIndex, index: u64) {
        if !self.indices[target_shard_idx].insert(index) {
            tracing::warn!(
                "two delayed receipts with index {} found in shard {}",
                index,
                self.source_shard_uid,
            );
        };
    }
}

fn remove_source_receipt_index(
    trie_updates: &mut [HashMap<u64, Option<Vec<u8>>>],
    source_shard_uid: ShardUId,
    target_shard_layout: &ShardLayout,
    index: u64,
) {
    if !target_shard_layout.shard_uids().any(|s| s == source_shard_uid) {
        return;
    }
    let shard_idx = target_shard_layout.get_shard_index(source_shard_uid.shard_id()).unwrap();
    if let Entry::Vacant(e) = trie_updates[shard_idx].entry(index) {
        e.insert(None);
    }
}

fn read_delayed_receipt(
    trie: &Trie,
    source_shard_uid: ShardUId,
    index: u64,
) -> anyhow::Result<Option<Receipt>> {
    let key = TrieKey::DelayedReceipt { index };
    let value =
        near_store::get_pure::<ReceiptOrStateStoredReceipt>(trie, &key).with_context(|| {
            format!(
                "failed reading delayed receipt idx {} from shard {} trie",
                index, source_shard_uid,
            )
        })?;
    Ok(match value {
        Some(r) => Some(r.into_receipt()),
        None => {
            tracing::warn!(
                "Expected delayed receipt with index {} in shard {} not found",
                index,
                source_shard_uid,
            );
            None
        }
    })
}

fn set_target_delayed_receipt(
    trie_updates: &mut HashMap<u64, Option<Vec<u8>>>,
    target_index: &mut u64,
    mut receipt: Receipt,
    default_key: &PublicKey,
) {
    near_mirror::genesis::map_receipt(&mut receipt, None, default_key);

    let value = ReceiptOrStateStoredReceipt::Receipt(Cow::Owned(receipt));
    let value = borsh::to_vec(&value).unwrap();
    trie_updates.insert(*target_index, Some(value));
    *target_index += 1;
}

// This should be called after push() has been called on each DelayedReceiptTracker in `trackers`
// for each receipt in its shard. This reads and maps the accounts and keys in all the receipts and
// writes them to the right shards.
pub(crate) fn write_delayed_receipts(
    shard_tries: &ShardTries,
    update_state: &[ShardUpdateState],
    trackers: Vec<DelayedReceiptTracker>,
    source_state_roots: &HashMap<ShardUId, StateRoot>,
    target_shard_layout: &ShardLayout,
    default_key: &PublicKey,
) -> anyhow::Result<()> {
    for t in &trackers {
        assert_eq!(update_state.len(), t.indices.len());
    }

    let tries = trackers
        .iter()
        .map(|tracker| {
            let state_root = source_state_roots.get(&tracker.source_shard_uid).unwrap();
            let trie = shard_tries.get_trie_for_shard(tracker.source_shard_uid, *state_root);
            (tracker.source_shard_uid, trie)
        })
        .collect::<HashMap<_, _>>();

    // TODO: commit these updates periodically so we don't read everything to memory, which might be too much.
    let mut trie_updates = vec![HashMap::new(); update_state.len()];
    let mut next_index = vec![0; update_state.len()];

    // TODO: Shouldn't matter too much how we assign them, but we could consider
    // changing this to try to be somewhat fair and take from other shards
    // before taking twice from the same shard

    for (source_shard_uid, target_shard_idx, index) in trackers.into_iter().flat_map(|tracker| {
        tracker.indices.into_iter().enumerate().flat_map(move |(target_shard_idx, indices)| {
            indices
                .into_iter()
                .map(move |index| (tracker.source_shard_uid, target_shard_idx, index))
        })
    }) {
        let trie = tries.get(&source_shard_uid).unwrap();

        remove_source_receipt_index(
            &mut trie_updates,
            source_shard_uid,
            target_shard_layout,
            index,
        );

        let Some(receipt) = read_delayed_receipt(trie, source_shard_uid, index)? else {
            continue;
        };

        let target_index = &mut next_index[target_shard_idx];

        set_target_delayed_receipt(
            &mut trie_updates[target_shard_idx],
            target_index,
            receipt,
            default_key,
        );
    }

    for (shard_idx, (updates, update_state)) in
        trie_updates.into_iter().zip(update_state.iter()).enumerate()
    {
        let shard_id = target_shard_layout.get_shard_id(shard_idx).unwrap();
        let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, target_shard_layout);

        let mut updates = updates
            .into_iter()
            .map(|(index, value)| (TrieKey::DelayedReceipt { index }, value))
            .collect::<Vec<_>>();

        let next_available_index = next_index[shard_idx];
        let indices = TrieQueueIndices { first_index: 0, next_available_index };
        let value = borsh::to_vec(&indices).unwrap();
        updates.push((TrieKey::DelayedReceiptIndices, Some(value)));
        crate::storage_mutator::commit_shard(shard_uid, &shard_tries, update_state, updates)
            .context("failed committing trie changes")?;
    }

    Ok(())
}
