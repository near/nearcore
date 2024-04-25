use crate::{get, set, TrieAccess, TrieUpdate};
use near_primitives::errors::StorageError;
use near_primitives::receipt::{DelayedReceiptIndices, Receipt};
use near_primitives::trie_key::TrieKey;

/// Type safe access to delayed receipts queue stored in the state.
///
/// The struct keeps changes to indices in fields and only writes it back to
/// state once, when `DelayedReceiptsQueue::write_back()` is called.
#[must_use = "modified indices must be written back to state"]
pub struct DelayedReceiptQueue {
    indices: DelayedReceiptIndices,
    dirty: bool,
}

/// Read-only iterator over receipt queues stored in the state trie.
///
/// This iterator currently only supports delayed receipts but is already
/// written general to work with the new queues that are going to be added for
/// congestion control.
pub struct ReceiptIterator<'a> {
    trie_keys: Box<dyn Iterator<Item = TrieKey>>,
    trie: &'a dyn TrieAccess,
}

impl DelayedReceiptQueue {
    pub fn load(trie: &dyn TrieAccess) -> Result<Self, StorageError> {
        let indices = get(trie, &TrieKey::DelayedReceiptIndices)?.unwrap_or_default();
        Ok(Self { indices, dirty: false })
    }

    pub fn push(
        &mut self,
        state_update: &mut TrieUpdate,
        receipt: &Receipt,
    ) -> Result<(), StorageError> {
        let index = self.indices.next_available_index;
        set(state_update, TrieKey::DelayedReceipt { index }, receipt);

        self.indices.next_available_index = index
            .checked_add(1)
            .expect("Next available index for delayed receipt exceeded the integer limit");
        self.dirty = true;
        Ok(())
    }

    pub fn pop(&mut self, state_update: &mut TrieUpdate) -> Result<Option<Receipt>, StorageError> {
        if self.indices.first_index >= self.indices.next_available_index {
            return Ok(None);
        }
        let key = TrieKey::DelayedReceipt { index: self.indices.first_index };
        let receipt: Receipt = get(state_update, &key)?.ok_or_else(|| {
            StorageError::StorageInconsistentState(format!(
                "Delayed receipt #{} should be in the state",
                self.indices.first_index
            ))
        })?;
        state_update.remove(key);
        // Math checked above, first_index < next_available_index
        self.indices.first_index += 1;

        self.dirty = true;
        Ok(Some(receipt))
    }

    /// Write changes of queue indices to state update.
    pub fn write_back(self, state_update: &mut TrieUpdate) {
        if self.dirty {
            set(state_update, TrieKey::DelayedReceiptIndices, &self.indices);
        }
    }

    pub fn len(&self) -> u64 {
        self.indices.len()
    }

    pub fn iter<'a>(&self, trie: &'a dyn TrieAccess) -> ReceiptIterator<'a> {
        ReceiptIterator {
            trie_keys: Box::new(
                (self.indices.first_index..self.indices.next_available_index)
                    .map(move |index| TrieKey::DelayedReceipt { index }),
            ),
            trie,
        }
    }
}

impl<'a> Iterator for ReceiptIterator<'a> {
    type Item = Result<Receipt, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        let key = self.trie_keys.next()?;
        let result = match get(self.trie, &key) {
            Err(e) => Err(e),
            Ok(None) => Err(StorageError::StorageInconsistentState(
                "Receipt referenced by index should be in the state".to_owned(),
            )),
            Ok(Some(receipt)) => Ok(receipt),
        };
        Some(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{gen_receipts, TestTriesBuilder};
    use crate::Trie;
    use near_primitives::shard_layout::ShardUId;

    #[test]
    fn test_delayed_receipts_queue() {
        // empty queues
        check_delayed_receipt_queue(&[]);

        // with random receipts
        let mut rng = rand::thread_rng();
        check_delayed_receipt_queue(&gen_receipts(&mut rng, 1));
        check_delayed_receipt_queue(&gen_receipts(&mut rng, 10));
        check_delayed_receipt_queue(&gen_receipts(&mut rng, 1000));
    }

    /// Add given receipts to the delayed receipts queue, then use
    /// `ReceiptIterator` to read them back and assert it has the same receipts
    /// in the same order. Then pop from the queue and check they are the same
    /// receipts.
    #[track_caller]
    fn check_delayed_receipt_queue(input_receipts: &[Receipt]) {
        let mut trie = init_state();
        let mut queue = DelayedReceiptQueue::load(&trie).expect("creating queue must not fail");

        for receipt in input_receipts {
            queue.push(&mut trie, receipt).expect("pushing must not fail");
        }
        let iterated_receipts: Vec<Receipt> =
            queue.iter(&trie).collect::<Result<_, _>>().expect("iterating should not fail");

        // check 1: receipts should be in queue even before writing back
        assert_eq!(input_receipts, iterated_receipts, "receipts were not recorded in queue");

        // check 2: write back and load again to see if values are persisted
        queue.write_back(&mut trie);
        let mut queue = DelayedReceiptQueue::load(&trie).expect("creating queue must not fail");
        let iterated_receipts: Vec<Receipt> =
            queue.iter(&trie).collect::<Result<_, _>>().expect("iterating should not fail");
        assert_eq!(input_receipts, iterated_receipts, "receipts were not persisted correctly");

        // check 3: pop receipts from queue and check if all are returned in the right order
        let mut popped = vec![];
        while let Some(receipt) = queue.pop(&mut trie).expect("pop must not fail") {
            popped.push(receipt);
        }
        assert_eq!(input_receipts, popped, "receipts were not popped correctly");
    }

    fn init_state() -> TrieUpdate {
        let shard_layout_version = 1;
        let tries = TestTriesBuilder::new().with_shard_layout(shard_layout_version, 2).build();
        let state_root = Trie::EMPTY_ROOT;
        let shard_uid = ShardUId { version: shard_layout_version, shard_id: 0 };
        let trie = tries.get_trie_for_shard(shard_uid, state_root);
        TrieUpdate::new(trie)
    }
}
