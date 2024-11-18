use crate::{get, get_pure, set, TrieAccess, TrieUpdate};
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::errors::{IntegerOverflowError, StorageError};
use near_primitives::receipt::{
    BufferedReceiptIndices, ReceiptOrStateStoredReceipt, TrieQueueIndices,
};
use near_primitives::trie_key::TrieKey;
use near_primitives::types::ShardId;

/// Read-only iterator over items stored in a TrieQueue.
pub struct TrieQueueIterator<'a, Queue: TrieQueue> {
    indices: std::ops::Range<u64>,
    trie_queue: &'a Queue,
    trie: &'a dyn TrieAccess,
    side_effects: bool,
}

/// Type safe access to delayed receipts queue stored in the state. Only use one
/// at the time for the same queue!
///
/// The struct keeps a in-memory copy of the queue indics to avoid reading it
/// from the trie on every access. Modification are written back to the
/// TrieUpdate immediately on every update.
///
/// But if you load two instances of this type at the same time, modifications
/// on one won't be synced to the other!
pub struct DelayedReceiptQueue {
    indices: TrieQueueIndices,
}

/// Type safe access to outgoing receipt buffers from this shard to all other
/// shards. Only use one at the time!
///
/// Call [`ShardsOutgoingReceiptBuffer::to_shard`] to access queue operations on
/// a buffer to a specific shard.
pub struct ShardsOutgoingReceiptBuffer {
    shards_indices: BufferedReceiptIndices,
}

/// Type safe access to buffered receipts to a specific shard.
///
/// Construct this from a parent `ShardsOutgoingReceiptBuffer` by calling
/// [`ShardsOutgoingReceiptBuffer::to_shard`]. Modification are written back to
/// the TrieUpdate immediately on every update.
///
/// Due to the shared indices, modifying two `OutgoingReceiptBuffer` instances
/// independently would lead to inconsistencies. The mutable borrow ensures at
/// compile-time this does not happen.
pub struct OutgoingReceiptBuffer<'parent> {
    shard_id: ShardId,
    parent: &'parent mut ShardsOutgoingReceiptBuffer,
}

/// Common code for persistent queues stored in the trie.
///
/// Here we use a trait to share code between different implementations of the
/// queue. Each impl defines how it loads and stores the queue indices and the
/// queue items. Based on that, a common push(), pop(), len(), and iter()
/// implementation is provided as trait default implementation.
pub trait TrieQueue {
    type Item<'a>: BorshDeserialize + BorshSerialize;

    /// Read queue indices of the queue from the trie, depending on impl.
    fn load_indices(&self, trie: &dyn TrieAccess) -> Result<TrieQueueIndices, StorageError>;

    /// Read indices from a cached field.
    fn indices(&self) -> TrieQueueIndices;

    /// Read and write indices from a cached field.
    fn indices_mut(&mut self) -> &mut TrieQueueIndices;

    /// Write changed indices back to the trie, using the correct trie key
    /// depending on impl.
    fn write_indices(&self, state_update: &mut TrieUpdate);

    /// Construct the trie key for a queue item depending on impl.
    fn trie_key(&self, queue_index: u64) -> TrieKey;

    fn push_back(
        &mut self,
        state_update: &mut TrieUpdate,
        item: &Self::Item<'_>,
    ) -> Result<(), IntegerOverflowError> {
        self.debug_check_unchanged(state_update);

        let index = self.indices().next_available_index;
        let key = self.trie_key(index);
        set(state_update, key, item);

        self.indices_mut().next_available_index =
            index.checked_add(1).ok_or(IntegerOverflowError)?;
        self.write_indices(state_update);
        Ok(())
    }

    fn pop_front(
        &mut self,
        state_update: &mut TrieUpdate,
    ) -> Result<Option<Self::Item<'static>>, StorageError> {
        self.debug_check_unchanged(state_update);

        let indices = self.indices();
        if indices.first_index >= indices.next_available_index {
            return Ok(None);
        }
        let key = self.trie_key(indices.first_index);
        let item: Self::Item<'static> = get(state_update, &key)?.ok_or_else(|| {
            StorageError::StorageInconsistentState(format!(
                "TrieQueue::Item #{} should be in the state",
                indices.first_index
            ))
        })?;
        state_update.remove(key);
        // Math checked above, first_index < next_available_index
        self.indices_mut().first_index += 1;
        self.write_indices(state_update);
        Ok(Some(item))
    }

    fn pop_back(
        &mut self,
        state_update: &mut TrieUpdate,
    ) -> Result<Option<Self::Item<'static>>, StorageError> {
        self.debug_check_unchanged(state_update);

        let indices = self.indices();
        if indices.first_index >= indices.next_available_index {
            return Ok(None);
        }
        // Math checked above: first_index < next_available_index => next_available_index > 0
        let last_item_index = indices.next_available_index - 1;
        let key = self.trie_key(last_item_index);
        let item: Self::Item<'static> = get(state_update, &key)?.ok_or_else(|| {
            StorageError::StorageInconsistentState(format!(
                "TrieQueue::Item #{} should be in the state",
                last_item_index
            ))
        })?;
        state_update.remove(key);
        self.indices_mut().next_available_index = last_item_index;
        self.write_indices(state_update);
        Ok(Some(item))
    }

    /// Modify the first item in a non-empty queue.
    /// `modify_fn` consumes the first item, modifies it, and returns `Option<Item>`.
    /// If `modify_fn` returns `Some`, the item is updated in the queue.
    /// If `modify_fn` returns `None`, the item is removed from the queue.
    /// Panics if the queue is empty.
    /// TODO(bandwidth_scheduler) - consider adding a push_front method.
    /// Indices could be converted to i64, serialization is the same as u64 for non-negative values.
    fn modify_first<'a>(
        &mut self,
        state_update: &mut TrieUpdate,
        modify_fn: impl Fn(Self::Item<'a>) -> Option<Self::Item<'a>>,
    ) -> Result<(), StorageError> {
        let indices = self.indices();
        if indices.first_index >= indices.next_available_index {
            panic!("TrieQueue::modify_first called on an empty queue! indices: {:?}", indices);
        }
        let key = self.trie_key(indices.first_index);
        let first_item: Self::Item<'_> = get(state_update, &key)?.ok_or_else(|| {
            StorageError::StorageInconsistentState(format!(
                "TrieQueue::Item #{} should be in the state",
                indices.first_index
            ))
        })?;
        let modified_item = modify_fn(first_item);
        match modified_item {
            Some(item) => set(state_update, key, &item),
            None => {
                // Modify function returned None, remove the first item.
                let _removed = self.pop_front(state_update)?;
            }
        }
        Ok(())
    }

    /// Remove up to `n` values from the end of the queue and return how many
    /// were actually remove.
    ///
    /// Unlike `pop`, this method does not return the actual items or even
    /// check if they existed in state.
    fn pop_n(&mut self, state_update: &mut TrieUpdate, n: u64) -> Result<u64, StorageError> {
        self.debug_check_unchanged(state_update);

        let indices = self.indices();
        let to_remove = std::cmp::min(
            n,
            indices.next_available_index.checked_sub(indices.first_index).unwrap_or(0),
        );

        for index in indices.first_index..(indices.first_index + to_remove) {
            let key = self.trie_key(index);
            state_update.remove(key);
        }

        if to_remove > 0 {
            self.indices_mut().first_index = indices
                .first_index
                .checked_add(to_remove)
                .expect("first_index + to_remove should be < next_available_index");
            self.write_indices(state_update);
        }
        Ok(to_remove)
    }

    fn len(&self) -> u64 {
        self.indices().len()
    }

    fn iter<'a>(
        &'a self,
        trie: &'a dyn TrieAccess,
        side_effects: bool,
    ) -> TrieQueueIterator<'a, Self>
    where
        Self: Sized,
    {
        if side_effects {
            self.debug_check_unchanged(trie);
        }
        TrieQueueIterator {
            indices: self.indices().first_index..self.indices().next_available_index,
            trie_queue: self,
            trie,
            side_effects,
        }
    }

    /// Check the queue has not been modified in the trie view.
    ///
    /// This is a semi-expensive operation. The values should be cached in
    /// memory in at least one layer. But we still want to avoid it in
    /// production.
    #[cfg(debug_assertions)]
    fn debug_check_unchanged(&self, trie: &dyn TrieAccess) {
        debug_assert_eq!(self.indices(), self.load_indices(trie).unwrap());
    }

    #[cfg(not(debug_assertions))]
    fn debug_check_unchanged(&self, _trie: &dyn TrieAccess) {
        // nop in release build
    }
}

impl DelayedReceiptQueue {
    pub fn load(trie: &dyn TrieAccess) -> Result<Self, StorageError> {
        let indices = crate::get_delayed_receipt_indices(trie)?;
        Ok(Self { indices: indices.into() })
    }
}

impl TrieQueue for DelayedReceiptQueue {
    type Item<'a> = ReceiptOrStateStoredReceipt<'a>;

    fn load_indices(&self, trie: &dyn TrieAccess) -> Result<TrieQueueIndices, StorageError> {
        crate::get_delayed_receipt_indices(trie).map(TrieQueueIndices::from)
    }

    fn indices(&self) -> TrieQueueIndices {
        self.indices.clone()
    }

    fn indices_mut(&mut self) -> &mut TrieQueueIndices {
        &mut self.indices
    }

    fn write_indices(&self, state_update: &mut TrieUpdate) {
        set(state_update, TrieKey::DelayedReceiptIndices, &self.indices);
    }

    fn trie_key(&self, index: u64) -> TrieKey {
        TrieKey::DelayedReceipt { index }
    }
}

impl ShardsOutgoingReceiptBuffer {
    pub fn load(trie: &dyn TrieAccess) -> Result<Self, StorageError> {
        let shards_indices = crate::get_buffered_receipt_indices(trie)?;
        Ok(Self { shards_indices })
    }

    pub fn to_shard(&mut self, shard_id: ShardId) -> OutgoingReceiptBuffer {
        OutgoingReceiptBuffer { shard_id, parent: self }
    }

    /// Returns shard IDs of all shards that have a buffer stored.
    pub fn shards(&self) -> Vec<ShardId> {
        self.shards_indices.shard_buffers.keys().copied().collect()
    }

    pub fn buffer_len(&self, shard_id: ShardId) -> Option<u64> {
        self.shards_indices.shard_buffers.get(&shard_id).map(TrieQueueIndices::len)
    }

    fn write_indices(&self, state_update: &mut TrieUpdate) {
        set(state_update, TrieKey::BufferedReceiptIndices, &self.shards_indices);
    }
}

impl TrieQueue for OutgoingReceiptBuffer<'_> {
    type Item<'a> = ReceiptOrStateStoredReceipt<'a>;

    fn load_indices(&self, trie: &dyn TrieAccess) -> Result<TrieQueueIndices, StorageError> {
        let all_indices: BufferedReceiptIndices =
            get(trie, &TrieKey::BufferedReceiptIndices)?.unwrap_or_default();
        let indices = all_indices.shard_buffers.get(&self.shard_id).cloned().unwrap_or_default();
        Ok(indices)
    }

    fn indices(&self) -> TrieQueueIndices {
        self.parent.shards_indices.shard_buffers.get(&self.shard_id).cloned().unwrap_or_default()
    }

    fn indices_mut(&mut self) -> &mut TrieQueueIndices {
        self.parent.shards_indices.shard_buffers.entry(self.shard_id).or_default()
    }

    fn write_indices(&self, state_update: &mut TrieUpdate) {
        self.parent.write_indices(state_update);
    }

    fn trie_key(&self, index: u64) -> TrieKey {
        TrieKey::BufferedReceipt { index, receiving_shard: self.shard_id }
    }
}

impl<'a, Queue: TrieQueue> Iterator for TrieQueueIterator<'a, Queue> {
    type Item = Result<Queue::Item<'static>, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        let index = self.indices.next()?;
        let key = self.trie_queue.trie_key(index);
        let value =
            if self.side_effects { get(self.trie, &key) } else { get_pure(self.trie, &key) };
        let result = match value {
            Err(e) => Err(e),
            Ok(None) => Err(StorageError::StorageInconsistentState(
                "TrieQueue::Item referenced by index should be in the state".to_owned(),
            )),
            Ok(Some(item)) => Ok(item),
        };
        Some(result)
    }
}

impl<'a, Queue: TrieQueue> DoubleEndedIterator for TrieQueueIterator<'a, Queue> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let index = self.indices.next_back()?;
        let key = self.trie_queue.trie_key(index);
        let value =
            if self.side_effects { get(self.trie, &key) } else { get_pure(self.trie, &key) };
        let result = match value {
            Err(e) => Err(e),
            Ok(None) => Err(StorageError::StorageInconsistentState(
                "TrieQueue::Item referenced by index should be in the state".to_owned(),
            )),
            Ok(Some(item)) => Ok(item),
        };
        Some(result)
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::*;
    use crate::test_utils::{gen_receipts, TestTriesBuilder};
    use crate::Trie;
    use near_primitives::receipt::Receipt;
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

        // load a queue to fill it with receipts
        {
            let mut queue = DelayedReceiptQueue::load(&trie).expect("creating queue must not fail");
            check_push_to_receipt_queue(input_receipts, &mut trie, &mut queue);
        }

        // drop queue and load another one to see if values are persisted
        {
            let mut queue = DelayedReceiptQueue::load(&trie).expect("creating queue must not fail");
            check_receipt_queue_contains_receipts(input_receipts, &mut trie, &mut queue);
        }
    }

    #[test]
    fn test_outgoing_receipt_buffer_separately() {
        // empty queues
        check_outgoing_receipt_buffer_separately(&[]);

        // with random receipts
        let mut rng = rand::thread_rng();
        check_outgoing_receipt_buffer_separately(&gen_receipts(&mut rng, 1));
        check_outgoing_receipt_buffer_separately(&gen_receipts(&mut rng, 10));
        check_outgoing_receipt_buffer_separately(&gen_receipts(&mut rng, 1000));
    }

    /// Check if inserting, reading, and popping from the outgoing buffers
    /// works, loading one buffer at the time.
    #[track_caller]
    fn check_outgoing_receipt_buffer_separately(input_receipts: &[Receipt]) {
        let mut trie = init_state();
        for id in 0..2u32 {
            // load a buffer to fill it with receipts
            {
                let mut buffers = ShardsOutgoingReceiptBuffer::load(&trie)
                    .expect("creating buffers must not fail");
                let mut buffer = buffers.to_shard(ShardId::from(id));
                check_push_to_receipt_queue(input_receipts, &mut trie, &mut buffer);
            }

            // drop queue and load another one to see if values are persisted
            {
                let mut buffers = ShardsOutgoingReceiptBuffer::load(&trie)
                    .expect("creating buffers must not fail");
                let mut buffer = buffers.to_shard(ShardId::from(id));
                check_receipt_queue_contains_receipts(input_receipts, &mut trie, &mut buffer);
            }
        }
    }

    /// Check if inserting, reading, and popping from the outgoing buffers
    /// works, loading buffers to all shards together.
    #[test]
    fn test_outgoing_receipt_buffer_combined() {
        // empty queues
        check_outgoing_receipt_buffer_combined(&[]);

        // with random receipts
        let mut rng = rand::thread_rng();
        check_outgoing_receipt_buffer_combined(&gen_receipts(&mut rng, 1));
        check_outgoing_receipt_buffer_combined(&gen_receipts(&mut rng, 10));
        check_outgoing_receipt_buffer_combined(&gen_receipts(&mut rng, 1000));
    }

    #[track_caller]
    fn check_outgoing_receipt_buffer_combined(input_receipts: &[Receipt]) {
        let mut trie = init_state();
        // load shard_buffers once and hold on to it for the entire duration
        let mut shard_buffers =
            ShardsOutgoingReceiptBuffer::load(&trie).expect("creating buffers must not fail");
        for id in 0..2u32 {
            // load a buffer to fill it with receipts
            {
                let mut buffer = shard_buffers.to_shard(ShardId::from(id));
                check_push_to_receipt_queue(input_receipts, &mut trie, &mut buffer);
            }

            // drop queue and load another one to see if values are persisted
            {
                let mut buffer = shard_buffers.to_shard(ShardId::from(id));
                check_receipt_queue_contains_receipts(input_receipts, &mut trie, &mut buffer);
            }
        }
    }

    /// Add given receipts to the  receipts queue, then use `ReceiptIterator` to
    /// read them back and assert it has the same receipts in the same order.
    #[track_caller]
    fn check_push_to_receipt_queue(
        input_receipts: &[Receipt],
        trie: &mut TrieUpdate,
        queue: &mut impl for<'a> TrieQueue<Item<'a> = ReceiptOrStateStoredReceipt<'a>>,
    ) {
        for receipt in input_receipts {
            let receipt = ReceiptOrStateStoredReceipt::Receipt(Cow::Borrowed(receipt));
            queue.push_back(trie, &receipt).expect("pushing must not fail");
        }
        let iterated_receipts: Vec<ReceiptOrStateStoredReceipt> =
            queue.iter(trie, true).collect::<Result<_, _>>().expect("iterating should not fail");
        let iterated_receipts: Vec<Receipt> =
            iterated_receipts.into_iter().map(|receipt| receipt.into_receipt()).collect();

        // check 1: receipts should be in queue and contained in the iterator
        assert_eq!(input_receipts, iterated_receipts, "receipts were not recorded in queue");
    }

    /// Assert receipts are in the queue and accessible from an iterator and
    /// from popping one by one.
    #[track_caller]
    fn check_receipt_queue_contains_receipts(
        input_receipts: &[Receipt],
        trie: &mut TrieUpdate,
        queue: &mut impl for<'a> TrieQueue<Item<'a> = ReceiptOrStateStoredReceipt<'a>>,
    ) {
        // check 2: assert newly loaded queue still contains the receipts
        let iterated_receipts: Vec<ReceiptOrStateStoredReceipt> =
            queue.iter(trie, true).collect::<Result<_, _>>().expect("iterating should not fail");
        let iterated_receipts: Vec<Receipt> =
            iterated_receipts.into_iter().map(|receipt| receipt.into_receipt()).collect();
        assert_eq!(input_receipts, iterated_receipts, "receipts were not persisted correctly");

        // check 3: pop receipts from queue and check if all are returned in the right order
        let mut popped = vec![];
        while let Some(receipt) = queue.pop_front(trie).expect("pop must not fail") {
            let receipt = receipt.into_receipt();
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
