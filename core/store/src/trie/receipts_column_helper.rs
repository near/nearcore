use crate::{TrieAccess, TrieUpdate, get, get_pure, set};
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::errors::{IntegerOverflowError, StorageError};
use near_primitives::receipt::{
    BufferedReceiptIndices, ReceiptOrStateStoredReceipt, TrieQueueIndices,
};
use near_primitives::trie_key::TrieKey;
use near_primitives::types::ShardId;

/// Read-only iterator over items stored in a TrieQueue.
struct TrieQueueIterator<'a, Queue>
where
    Queue: TrieQueue,
{
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
    // TODO - remove the lifetime once we get rid of Cow in StateStoredReceipt.
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
            Some(item) => {
                set(state_update, key, &item);
                self.write_indices(state_update);
            }
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
    ) -> impl DoubleEndedIterator<Item = Result<Self::Item<'static>, StorageError>> + 'a
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

impl<Q> Iterator for TrieQueueIterator<'_, Q>
where
    Q: TrieQueue,
{
    type Item = Result<Q::Item<'static>, StorageError>;

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

impl<Q> DoubleEndedIterator for TrieQueueIterator<'_, Q>
where
    Q: TrieQueue,
{
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
    use std::collections::VecDeque;

    use super::*;
    use crate::Trie;
    use crate::test_utils::{TestTriesBuilder, gen_receipts};
    use near_primitives::receipt::Receipt;
    use near_primitives::shard_layout::ShardLayout;
    use rand::seq::SliceRandom;
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha20Rng;

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
        let shard_layout = ShardLayout::multi_shard(2, shard_layout_version);
        let shard_uid = shard_layout.shard_uids().next().unwrap();
        let state_root = Trie::EMPTY_ROOT;

        let tries = TestTriesBuilder::new().with_shard_layout(shard_layout).build();
        let trie = tries.get_trie_for_shard(shard_uid, state_root);
        TrieUpdate::new(trie)
    }

    // Queue used to test the TrieQueue trait.
    struct TestTrieQueue {
        indices: TrieQueueIndices,
    }

    impl TestTrieQueue {
        pub fn new() -> Self {
            Self { indices: Default::default() }
        }

        pub fn load(trie: &dyn TrieAccess) -> Result<Self, StorageError> {
            Ok(Self { indices: Self::read_indices(trie)? })
        }

        fn read_indices(trie: &dyn TrieAccess) -> Result<TrieQueueIndices, StorageError> {
            Ok(get(
                trie,
                &TrieKey::BufferedReceiptGroupsQueueData { receiving_shard: Self::fake_shard_id() },
            )?
            .unwrap_or_default())
        }

        /// TestQueue reuses the trie columns used for storing buffered receipt groups queue.
        /// It pretends to be a buffered receipt groups queue for the shard with the maximum ID.
        /// Reusing the columns is the easiest way to implement the TrieQueue trait
        /// for a test queue that doesn't have a dedicated trie column.
        fn fake_shard_id() -> ShardId {
            ShardId::max()
        }
    }

    impl TrieQueue for TestTrieQueue {
        type Item<'a> = i32;

        fn load_indices(&self, trie: &dyn TrieAccess) -> Result<TrieQueueIndices, StorageError> {
            Self::read_indices(trie)
        }

        fn indices(&self) -> TrieQueueIndices {
            self.indices.clone()
        }

        fn indices_mut(&mut self) -> &mut TrieQueueIndices {
            &mut self.indices
        }

        fn write_indices(&self, state_update: &mut TrieUpdate) {
            set(
                state_update,
                TrieKey::BufferedReceiptGroupsQueueData { receiving_shard: Self::fake_shard_id() },
                &self.indices,
            );
        }

        fn trie_key(&self, index: u64) -> TrieKey {
            TrieKey::BufferedReceiptGroupsQueueItem {
                receiving_shard: Self::fake_shard_id(),
                index,
            }
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum RandomOperation {
        PushBack(i32),
        PopFront,
        PopBack,
        ModifyFirst,
        PopN(usize),
    }

    /// Generates random TrieQueue operations.
    /// Special care is taken to ensure that queue size sometimes reaches zero.
    /// For 100 operations the queue has a balanced number of pushes and pops.
    /// For the next 100 operations, the pops outweigh pushes 2:1. The queue will most likely
    /// be cleared during this phase.
    /// And so on, the phases repeat.
    fn generate_random_operation(rng: &mut impl Rng, i: usize) -> RandomOperation {
        // The mode of operations switches every 100 operations.
        let mode = if (i / 100) % 2 == 0 { "random_balanced" } else { "mostly_pops" };

        let weighted_items = match mode {
            "random_balanced" => {
                // Same number of items removed and added on average
                vec![
                    (RandomOperation::PushBack(rng.gen_range(0..1000)), 3), // On average adds 3 items
                    (RandomOperation::PopFront, 1), // On average removes 1 item
                    (RandomOperation::PopBack, 1),  // On average removes 1 item
                    (RandomOperation::ModifyFirst, 1), // Doesn't remove any items
                    (RandomOperation::PopN(rng.gen_range(0..=2)), 1), // On average removes 1 item
                ]
            }
            "mostly_pops" => {
                // Pops are 2x as likely as pushes, to clear the queue
                vec![
                    (RandomOperation::PushBack(rng.gen_range(0..1000)), 3), // On average adds 3 items
                    (RandomOperation::PopFront, 2), // On average removes 2 items
                    (RandomOperation::PopBack, 2),  // On average removes 2 items
                    (RandomOperation::ModifyFirst, 2), // Doesn't remove any items
                    (RandomOperation::PopN(rng.gen_range(0..=2)), 2), // On average removes 2 items
                ]
            }
            other => panic!("Unknown mode: {}", other),
        };

        weighted_items.choose_weighted(rng, |item| item.1).unwrap().0
    }

    // Load the queue from the trie, discarding the current data stored in the variable.
    // Ensures that all changes are written to the trie after every operation.
    fn maybe_reload_queue(trie: &TrieUpdate, queue: &mut TestTrieQueue, rng: &mut impl Rng) {
        if rng.r#gen::<bool>() {
            *queue = TestTrieQueue::load(trie).unwrap();
        }
    }

    /// Test the TrieQueue trait by performing random operations on the queue
    /// and comparing the results with a VecDeque.
    #[test]
    fn test_random_trie_queue_operations() {
        let rng = &mut ChaCha20Rng::seed_from_u64(0);

        let mut trie = init_state();
        // Trie queue
        let mut trie_queue = TestTrieQueue::new();
        // In-memory queue to compare against
        let mut memory_queue = VecDeque::new();

        // Run 20_000 random operations on the queue
        for i in 0..20_000 {
            maybe_reload_queue(&mut trie, &mut trie_queue, rng);

            let op = generate_random_operation(rng, i);
            match op {
                RandomOperation::PushBack(value) => {
                    trie_queue.push_back(&mut trie, &value).unwrap();
                    memory_queue.push_back(value);
                }
                RandomOperation::PopFront => {
                    let trie_item = trie_queue.pop_front(&mut trie).unwrap();
                    let memory_item = memory_queue.pop_front();
                    assert_eq!(trie_item, memory_item);
                }
                RandomOperation::PopBack => {
                    let trie_item = trie_queue.pop_back(&mut trie).unwrap();
                    let memory_item = memory_queue.pop_back();
                    assert_eq!(trie_item, memory_item);
                }
                RandomOperation::ModifyFirst => {
                    if memory_queue.is_empty() {
                        // modify_first panics if the queue is empty
                        continue;
                    }

                    let modify_fn: Box<dyn Fn(i32) -> Option<i32>> = if rng.r#gen::<bool>() {
                        Box::new(|item: i32| -> Option<i32> { Some(item.wrapping_add(1)) })
                    } else {
                        Box::new(|_: i32| -> Option<i32> { None })
                    };

                    trie_queue.modify_first(&mut trie, &modify_fn).unwrap();

                    let memory_item = memory_queue.pop_front().unwrap();
                    if let Some(new_item) = modify_fn(memory_item) {
                        memory_queue.push_front(new_item);
                    }
                }
                RandomOperation::PopN(n) => {
                    let trie_popped = trie_queue.pop_n(&mut trie, n as u64).unwrap();
                    let mut memory_popped = 0;
                    for _ in 0..n {
                        if memory_queue.pop_front().is_some() {
                            memory_popped += 1;
                        }
                    }
                    assert_eq!(trie_popped, memory_popped as u64);
                }
            }

            maybe_reload_queue(&mut trie, &mut trie_queue, rng);

            if rng.r#gen::<bool>() {
                // Compare a random prefix from both queues
                let prefix_len = if rng.r#gen::<bool>() || memory_queue.is_empty() {
                    memory_queue.len()
                } else {
                    rng.gen_range(0..memory_queue.len())
                };
                let mut trie_items = Vec::new();
                let mut trie_iter = trie_queue.iter(&trie, rng.r#gen::<bool>());
                for _ in 0..prefix_len {
                    let trie_item = trie_iter.next().unwrap().unwrap();
                    trie_items.push(trie_item);
                }
                let memory_items =
                    memory_queue.iter().take(prefix_len).copied().collect::<Vec<_>>();
                assert_eq!(trie_items, memory_items);
            }
        }
    }
}
