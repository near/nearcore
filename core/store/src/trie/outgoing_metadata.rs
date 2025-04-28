use std::collections::BTreeMap;

use borsh::{BorshDeserialize, BorshSerialize};
use bytesize::ByteSize;
use near_primitives::errors::StorageError;
use near_primitives::receipt::TrieQueueIndices;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{Gas, ShardId};

use near_schema_checker_lib::ProtocolSchema;

use crate::{TrieUpdate, get, set};

use super::TrieAccess;
use super::receipts_column_helper::TrieQueue;

/// Keeps metadata about receipts stored in the outgoing buffers.
#[derive(Debug)]
pub struct OutgoingMetadatas {
    /// Metadata information for outgoing buffer to each shard.
    metadatas: BTreeMap<ShardId, ReceiptGroupsQueue>,
    /// Parameters which control size of receipt groups.
    groups_config: ReceiptGroupsConfig,
}

impl OutgoingMetadatas {
    /// New empty metadata.
    pub fn new(groups_config: ReceiptGroupsConfig) -> Self {
        Self { metadatas: BTreeMap::new(), groups_config }
    }

    /// Load shard metadata from the trie.
    /// Returns empty metadata for protocol versions which don't support metadata.
    /// Make sure to pass shard ids for every shard that has receipts in the outgoing buffer,
    /// otherwise the metadata for it will be overwritten with empty metadata.
    pub fn load(
        trie: &dyn TrieAccess,
        shard_ids: impl IntoIterator<Item = ShardId>,
        groups_config: ReceiptGroupsConfig,
    ) -> Result<Self, StorageError> {
        let mut metadatas = BTreeMap::new();
        for shard_id in shard_ids {
            let metadata = ReceiptGroupsQueue::load(trie, shard_id)?;
            if let Some(metadata) = metadata {
                metadatas.insert(shard_id, metadata);
            }
        }
        Ok(Self { metadatas, groups_config })
    }

    /// Update the metadata when a new receipt is added at the end of the outgoing receipts buffer.
    /// If the metadata doesn't exist for the shard, a new one will be created.
    pub fn update_on_receipt_pushed(
        &mut self,
        shard_id: ShardId,
        receipt_size: ByteSize,
        receipt_gas: Gas,
        state_update: &mut TrieUpdate,
    ) -> Result<(), StorageError> {
        let metadata =
            self.metadatas.entry(shard_id).or_insert_with(|| ReceiptGroupsQueue::new(shard_id));
        metadata.update_on_receipt_pushed(
            receipt_size,
            receipt_gas,
            state_update,
            &self.groups_config,
        )
    }

    /// Update the metadata when a receipt is removed from the front of the outgoing receipts buffer.
    /// Can be called only for receipts for which `update_on_receipt_pushed` was called before.
    pub fn update_on_receipt_popped(
        &mut self,
        shard_id: ShardId,
        receipt_size: ByteSize,
        receipt_gas: Gas,
        state_update: &mut TrieUpdate,
    ) -> Result<(), StorageError> {
        let metadata = self
            .metadatas
            .get_mut(&shard_id)
            .expect("Metadata for this shard should've been created when a receipt was pushed.");
        metadata.update_on_receipt_popped(receipt_size, receipt_gas, state_update)
    }

    /// Get metadata for the outgoing buffer to this shard.
    pub fn get_metadata_for_shard(&self, shard_id: &ShardId) -> Option<&ReceiptGroupsQueue> {
        self.metadatas.get(shard_id)
    }
}

/// Information about a group of consecutive receipts stored in the outgoing buffer.
#[derive(Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub enum ReceiptGroup {
    V0(ReceiptGroupV0),
}

#[derive(Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ReceiptGroupV0 {
    /// Total size of receipts in this group.
    /// Should be no larger than `max_receipt_size`, otherwise the bandwidth
    /// scheduler will not be able to grant the bandwidth needed to send
    /// the receipts in this group.
    pub size: u64,
    /// Total gas of receipts in this group.
    pub gas: u128,
}

impl ReceiptGroup {
    /// Create a new group which will contain one receipt with this size and gas.
    pub fn new(receipt_size: ByteSize, receipt_gas: Gas) -> ReceiptGroup {
        ReceiptGroup::V0(ReceiptGroupV0 { size: receipt_size.as_u64(), gas: receipt_gas.into() })
    }

    /// Total size of receipts in this group.
    pub fn size(&self) -> u64 {
        match self {
            ReceiptGroup::V0(group) => group.size,
        }
    }

    pub fn size_mut(&mut self) -> &mut u64 {
        match self {
            ReceiptGroup::V0(group) => &mut group.size,
        }
    }

    /// Total gas of receipts in this group.
    pub fn gas(&self) -> u128 {
        match self {
            ReceiptGroup::V0(group) => group.gas,
        }
    }

    pub fn gas_mut(&mut self) -> &mut u128 {
        match self {
            ReceiptGroup::V0(group) => &mut group.gas,
        }
    }

    pub fn is_empty(&self) -> bool {
        // There are no receipts with size equal to zero.
        // Total size being zero means that there are no receipts in this group.
        let empty = self.size() == 0;
        if empty {
            assert_eq!(self.gas(), 0, "Gas should be zero for an empty group.");
        }
        empty
    }
}

/// Parameters which control size and gas of receipt groups stored in the ReceiptGroupsQueue.
/// By default receipts are added to the last group in the queue.
/// A new group is started if adding a new receipt would make the group exceed the upper bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceiptGroupsConfig {
    /// All receipt groups aim to have a size below this threshold.
    /// A group can be larger that this if a single receipt has size larger than the limit.
    pub size_upper_bound: ByteSize,
    /// All receipt groups aim to have gas below this threshold.
    pub gas_upper_bound: Gas,
}

impl ReceiptGroupsConfig {
    pub fn default_config() -> Self {
        // TODO(bandwidth_scheduler) - put in runtime config
        ReceiptGroupsConfig { size_upper_bound: ByteSize::kb(100), gas_upper_bound: Gas::MAX }
    }

    /// Decide whether a new receipt should be added to the last group
    /// or a new group be started for this receipt.
    /// Enforces the bounds on size and gas.
    pub fn should_start_new_group(
        &self,
        last_group: &ReceiptGroup,
        new_receipt_size: ByteSize,
        new_receipt_gas: Gas,
    ) -> bool {
        let mut group_size = last_group.size();
        add_size_checked(&mut group_size, new_receipt_size);
        if group_size > self.size_upper_bound.as_u64() {
            // The new group would be too large, start a new group.
            return true;
        }

        let mut group_gas = last_group.gas();
        add_gas_checked(&mut group_gas, new_receipt_gas);
        if group_gas > self.gas_upper_bound.into() {
            // The new group would have too much gas, start a new group.
            return true;
        }

        false
    }
}

/// Data of ReceiptGroupsQueue that is stored in the state.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub enum ReceiptGroupsQueueData {
    V0(ReceiptGroupsQueueDataV0),
}

#[derive(Debug, Clone, Default, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ReceiptGroupsQueueDataV0 {
    /// Indices of the receipt groups TrieQueue.
    pub indices: TrieQueueIndices,
    /// Total size of all receipts in the queue.
    pub total_size: u64,
    /// Total gas of all receipts in the queue.
    pub total_gas: u128,
    /// Total number of receipts in the queue.
    pub total_receipts_num: u64,
}

/// A queue of receipt groups (see struct `ReceiptGroup`).
/// Each group represents a set of consecutive receipts stored in the outgoing buffer.
/// When a new receipt is buffered, it's either added to the last group in the queue
/// or a new group is started if the last group is already large.
/// When it is removed from the buffer, the first group is updated.
/// Groups represent aggregate data about receipts which can be iterated over
/// without having to iterate over individual receipts. It's used by the bandwidth scheduler
/// to determine the size and structure of receipts in the outgoing buffer and make
/// bandwidth requests based on them.
#[derive(Debug)]
pub struct ReceiptGroupsQueue {
    /// Corresponds to receipts stored in the outgoing buffer to this shard.
    receiver_shard: ShardId,
    /// Persistent data, stored in the trie.
    data: ReceiptGroupsQueueDataV0,
}

impl ReceiptGroupsQueue {
    /// Create a new empty queue.
    pub fn new(receiver_shard: ShardId) -> ReceiptGroupsQueue {
        ReceiptGroupsQueue { receiver_shard, data: ReceiptGroupsQueueDataV0::default() }
    }

    /// Load a queue from the trie.
    /// Returns None if the queue doesn't exist.
    pub fn load(
        trie: &dyn TrieAccess,
        receiver_shard: ShardId,
    ) -> Result<Option<Self>, StorageError> {
        let data_opt = Self::load_data(trie, receiver_shard)?;
        let receipt_groups_queue_opt = data_opt.map(|data| Self { receiver_shard, data });
        Ok(receipt_groups_queue_opt)
    }

    fn load_data(
        trie: &dyn TrieAccess,
        receiver_shard: ShardId,
    ) -> Result<Option<ReceiptGroupsQueueDataV0>, StorageError> {
        let data_opt: Option<ReceiptGroupsQueueData> = get(
            trie,
            &TrieKey::BufferedReceiptGroupsQueueData { receiving_shard: receiver_shard },
        )?;
        let data_v0_opt = data_opt.map(|data_enum| match data_enum {
            ReceiptGroupsQueueData::V0(data_v0) => data_v0,
        });
        Ok(data_v0_opt)
    }

    fn save_data(&self, state_update: &mut TrieUpdate) {
        let data_enum = ReceiptGroupsQueueData::V0(self.data.clone());
        set(
            state_update,
            TrieKey::BufferedReceiptGroupsQueueData { receiving_shard: self.receiver_shard },
            &data_enum,
        );
    }

    pub fn update_on_receipt_pushed(
        &mut self,
        receipt_size: ByteSize,
        receipt_gas: Gas,
        state_update: &mut TrieUpdate,
        groups_config: &ReceiptGroupsConfig,
    ) -> Result<(), StorageError> {
        add_size_checked(&mut self.data.total_size, receipt_size);
        add_gas_checked(&mut self.data.total_gas, receipt_gas);
        self.data.total_receipts_num = self
            .data
            .total_receipts_num
            .checked_add(1)
            .expect("Overflow! - Number of receipts doesn't fit into u64!");

        // Take out the last group from the queue and inspect it.
        match self.pop_back(state_update)? {
            Some(mut last_group) => {
                if groups_config.should_start_new_group(&last_group, receipt_size, receipt_gas) {
                    // Adding the new receipt to the last group would make the group too large.
                    // Start a new group for the receipt.
                    self.push_back(state_update, &last_group).expect("Integer overflow on push");
                    self.push_back(state_update, &ReceiptGroup::new(receipt_size, receipt_gas))
                        .expect("Integer overflow on push");
                } else {
                    // It's okay to add the new receipt to the last group, do it.
                    add_size_checked(last_group.size_mut(), receipt_size);
                    add_gas_checked(last_group.gas_mut(), receipt_gas);
                    self.push_back(state_update, &last_group).expect("Integer overflow on push");
                }
            }
            None => {
                // No groups in the queue, start a new group which contains the new receipt.
                self.push_back(state_update, &ReceiptGroup::new(receipt_size, receipt_gas))
                    .expect("Integer overflow on push");
            }
        }

        Ok(())
    }

    pub fn update_on_receipt_popped(
        &mut self,
        receipt_size: ByteSize,
        receipt_gas: Gas,
        state_update: &mut TrieUpdate,
    ) -> Result<(), StorageError> {
        subtract_size_checked(&mut self.data.total_size, receipt_size);
        subtract_gas_checked(&mut self.data.total_gas, receipt_gas);

        self.data.total_receipts_num = self
            .data
            .total_receipts_num
            .checked_sub(1)
            .expect("Underflow! - More receipts were popped than pushed!");

        assert!(self.data.indices.len() > 0, "No receipt groups to pop from!");

        self.modify_first(state_update, |mut first_group| {
            subtract_size_checked(first_group.size_mut(), receipt_size);
            subtract_gas_checked(first_group.gas_mut(), receipt_gas);
            if first_group.is_empty() {
                // No more receipts in the first group, remove it.
                None
            } else {
                // Still some receipts in the group. Save the updated group.
                Some(first_group)
            }
        })
    }

    /// Iterate over the sizes of receipt groups stored in the queue.
    pub fn iter_receipt_group_sizes<'a>(
        &'a self,
        trie: &'a dyn TrieAccess,
        side_effects: bool,
    ) -> impl Iterator<Item = Result<u64, StorageError>> + 'a {
        self.iter(trie, side_effects).map(|group_res| group_res.map(|group| group.size()))
    }

    /// Total size of all receipts in the queue.
    pub fn total_size(&self) -> u64 {
        self.data.total_size
    }

    /// Total gas of all receipts in the queue.
    pub fn total_gas(&self) -> u128 {
        self.data.total_gas
    }

    /// Total number of receipts in the queue.
    pub fn total_receipts_num(&self) -> u64 {
        self.data.total_receipts_num
    }
}

impl TrieQueue for ReceiptGroupsQueue {
    type Item<'a> = ReceiptGroup;

    fn load_indices(&self, trie: &dyn TrieAccess) -> Result<TrieQueueIndices, StorageError> {
        Ok(Self::load_data(trie, self.receiver_shard)?.unwrap_or_default().indices)
    }

    fn indices(&self) -> TrieQueueIndices {
        self.data.indices.clone()
    }

    fn indices_mut(&mut self) -> &mut TrieQueueIndices {
        &mut self.data.indices
    }

    fn write_indices(&self, state_update: &mut TrieUpdate) {
        self.save_data(state_update);
    }

    fn trie_key(&self, index: u64) -> TrieKey {
        TrieKey::BufferedReceiptGroupsQueueItem { receiving_shard: self.receiver_shard, index }
    }
}

fn add_size_checked(total: &mut u64, delta: ByteSize) {
    *total = total
        .checked_add(delta.as_u64())
        .expect("add_size_checked - Overflow! Reached exabytes of size!");
}

fn subtract_size_checked(total: &mut u64, delta: ByteSize) {
    *total = total
        .checked_sub(delta.as_u64())
        .expect("subtract_size_checked - Underflow! Negative size!");
}

fn add_gas_checked(total: &mut u128, delta: Gas) {
    *total = total
        .checked_add(delta.into())
        .expect("add_gas_checked - Overflow! Total gas doesn't fit into u128!");
}

fn subtract_gas_checked(total: &mut u128, delta: Gas) {
    *total =
        total.checked_sub(delta.into()).expect("subtract_gas_checked - Underflow! Negative gas!")
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::convert::Infallible;

    use bytesize::ByteSize;
    use near_primitives::bandwidth_scheduler::{
        BandwidthRequest, BandwidthRequestValues, BandwidthSchedulerParams,
    };
    use near_primitives::shard_layout::{ShardLayout, ShardUId};
    use near_primitives::types::{Gas, ShardId};
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha20Rng;

    use crate::test_utils::TestTriesBuilder;
    use crate::trie::receipts_column_helper::TrieQueue;
    use crate::{Trie, TrieUpdate};

    use super::{ReceiptGroup, ReceiptGroupV0, ReceiptGroupsConfig, ReceiptGroupsQueue};
    use testlib::bandwidth_scheduler::get_random_receipt_size_for_test;

    #[test]
    fn test_receipt_groups_config() {
        let config =
            ReceiptGroupsConfig { size_upper_bound: ByteSize::kb(100), gas_upper_bound: 100 };

        let group = ReceiptGroup::new(ByteSize::kb(50), 50);

        assert_eq!(config.should_start_new_group(&group, ByteSize::kb(10), 0), false);
        assert_eq!(config.should_start_new_group(&group, ByteSize::kb(50), 0), false);
        assert_eq!(config.should_start_new_group(&group, ByteSize::kb(100), 0), true);
        assert_eq!(config.should_start_new_group(&group, ByteSize::kb(0), 0), false);

        assert_eq!(config.should_start_new_group(&group, ByteSize::kb(0), 10), false);
        assert_eq!(config.should_start_new_group(&group, ByteSize::kb(0), 50), false);
        assert_eq!(config.should_start_new_group(&group, ByteSize::kb(0), 100), true);
        assert_eq!(config.should_start_new_group(&group, ByteSize::kb(0), 0), false);

        assert_eq!(config.should_start_new_group(&group, ByteSize::kb(10), 30), false);
        assert_eq!(config.should_start_new_group(&group, ByteSize::kb(100), 30), true);
        assert_eq!(config.should_start_new_group(&group, ByteSize::kb(30), 100), true);
    }

    fn make_trie_update() -> TrieUpdate {
        let shard_layout_version = 1;
        let shard_layout = ShardLayout::multi_shard(2, shard_layout_version);
        let shard_uid = shard_layout.shard_uids().next().unwrap();
        let state_root = Trie::EMPTY_ROOT;

        let tries = TestTriesBuilder::new().with_shard_layout(shard_layout).build();
        let trie = tries.get_trie_for_shard(shard_uid, state_root);
        TrieUpdate::new(trie)
    }

    #[test]
    fn test_receipt_groups_queue() {
        let trie_update = &mut make_trie_update();
        let config =
            ReceiptGroupsConfig { size_upper_bound: ByteSize::kb(100), gas_upper_bound: Gas::MAX };
        let mut queue = ReceiptGroupsQueue::new(ShardId::new(0));

        fn group_sizes(queue: &ReceiptGroupsQueue, trie: &TrieUpdate) -> Vec<u64> {
            queue.iter_receipt_group_sizes(trie, false).map(|size_res| size_res.unwrap()).collect()
        }

        let ten_kb = ByteSize::kb(10);
        let fifty_kb = ByteSize::kb(50);
        let hundred_kb = ByteSize::kb(100);
        let two_hundred_kb = ByteSize::kb(200);

        assert_eq!(group_sizes(&queue, trie_update), Vec::<u64>::new());

        queue.update_on_receipt_pushed(ten_kb, 10, trie_update, &config).unwrap();
        assert_eq!(group_sizes(&queue, trie_update), vec![10_000]);

        queue.update_on_receipt_pushed(ten_kb, 10, trie_update, &config).unwrap();
        assert_eq!(group_sizes(&queue, trie_update), vec![20_000]);

        queue.update_on_receipt_pushed(fifty_kb, 10, trie_update, &config).unwrap();
        assert_eq!(group_sizes(&queue, trie_update), vec![70_000]);

        queue.update_on_receipt_popped(ten_kb, 10, trie_update).unwrap();
        assert_eq!(group_sizes(&queue, trie_update), vec![60_000]);

        queue.update_on_receipt_pushed(hundred_kb, 10, trie_update, &config).unwrap();
        assert_eq!(group_sizes(&queue, trie_update), vec![60_000, 100_000]);

        queue.update_on_receipt_pushed(ten_kb, 10, trie_update, &config).unwrap();
        assert_eq!(group_sizes(&queue, trie_update), vec![60_000, 100_000, 10_000]);

        queue.update_on_receipt_pushed(two_hundred_kb, 10, trie_update, &config).unwrap();
        assert_eq!(group_sizes(&queue, trie_update), vec![60_000, 100_000, 10_000, 200_000]);

        queue.update_on_receipt_popped(ten_kb, 10, trie_update).unwrap();
        assert_eq!(group_sizes(&queue, trie_update), vec![50_000, 100_000, 10_000, 200_000]);

        queue.update_on_receipt_popped(fifty_kb, 10, trie_update).unwrap();
        assert_eq!(group_sizes(&queue, trie_update), vec![100_000, 10_000, 200_000]);

        queue.update_on_receipt_popped(hundred_kb, 10, trie_update).unwrap();
        assert_eq!(group_sizes(&queue, trie_update), vec![10_000, 200_000]);

        queue.update_on_receipt_popped(ten_kb, 10, trie_update).unwrap();
        assert_eq!(group_sizes(&queue, trie_update), vec![200_000]);

        queue.update_on_receipt_popped(two_hundred_kb, 10, trie_update).unwrap();
        assert_eq!(group_sizes(&queue, trie_update), Vec::<u64>::new());
    }

    /// Equivalent to the `ReceiptGroup` struct, used in testing.
    #[derive(Debug, Clone, Copy)]
    struct TestReceiptGroup {
        total_size: u128,
        total_gas: u128,
        /// Number of receipts to double-check that group is empty at the right time.
        receipts_num: u128,
    }

    impl From<TestReceiptGroup> for ReceiptGroup {
        fn from(group: TestReceiptGroup) -> Self {
            ReceiptGroup::V0(ReceiptGroupV0 { size: group.total_size as u64, gas: group.total_gas })
        }
    }

    /// In-memory version of `ReceiptGroupsQueue`.
    /// The implementation is much simpler than the real trie-based one.
    /// It's used to test the correctness of the trie-based implementation.
    struct TestReceiptGroupQueue {
        groups: VecDeque<TestReceiptGroup>,
    }

    impl TestReceiptGroupQueue {
        pub fn new() -> Self {
            Self { groups: VecDeque::new() }
        }

        pub fn update_on_receipt_pushed(
            &mut self,
            receipt_size: ByteSize,
            receipt_gas: Gas,
            config: &ReceiptGroupsConfig,
        ) {
            let new_receipt_group = TestReceiptGroup {
                total_size: receipt_size.as_u64().into(),
                total_gas: receipt_gas.into(),
                receipts_num: 1,
            };

            match self.groups.pop_back() {
                Some(last_group) => {
                    if config.should_start_new_group(&last_group.into(), receipt_size, receipt_gas)
                    {
                        self.groups.push_back(last_group);
                        self.groups.push_back(new_receipt_group);
                    } else {
                        self.groups.push_back(TestReceiptGroup {
                            total_size: last_group.total_size + receipt_size.as_u64() as u128,
                            total_gas: last_group.total_gas + receipt_gas as u128,
                            receipts_num: last_group.receipts_num + 1,
                        });
                    }
                }
                None => self.groups.push_back(new_receipt_group),
            }
        }

        pub fn update_on_receipt_popped(&mut self, receipt_size: ByteSize, receipt_gas: Gas) {
            let mut first_group = self.groups.pop_front().unwrap();
            first_group.total_size -= receipt_size.as_u64() as u128;
            first_group.total_gas -= receipt_gas as u128;
            first_group.receipts_num -= 1;

            if first_group.receipts_num > 0 {
                self.groups.push_front(first_group);
            } else {
                assert_eq!(first_group.total_size, 0);
                assert_eq!(first_group.total_gas, 0);
            }
        }
    }

    /// Test the `ReceiptGroupsQueue` by adding/removing random receipts and comparing the results
    /// with the in-memory implementation.
    fn receipt_groups_queue_random_test(rng_seed: u64) {
        let rng = &mut ChaCha20Rng::seed_from_u64(rng_seed);

        let config =
            ReceiptGroupsConfig { size_upper_bound: ByteSize::kb(100), gas_upper_bound: 100_000 };
        let mut groups_queue = ReceiptGroupsQueue::new(ShardId::new(0));
        let mut test_queue = TestReceiptGroupQueue::new();
        let mut buffered_receipts: VecDeque<(ByteSize, Gas)> = VecDeque::new();

        let num_receipts = rng.gen_range(0..1000);
        let min_receipt_gas = 100;
        let max_receipt_gas = 1000;

        // First push some receipts to the queue.
        let mut receipts: Vec<(ByteSize, Gas)> = Vec::new();
        for _ in 0..num_receipts {
            let receipt_size = ByteSize::b(get_random_receipt_size_for_test(rng));
            let receipt_gas = rng.gen_range(min_receipt_gas..max_receipt_gas);
            receipts.push((receipt_size, receipt_gas));
        }

        let trie_update = &mut make_trie_update();

        // Then perform random pushes and pops.
        let mut next_receipt_to_push_idx = 0;
        loop {
            let can_push = next_receipt_to_push_idx < receipts.len();
            let can_pop = !buffered_receipts.is_empty();

            if !can_pop && !can_push {
                break;
            }

            let should_push = if !can_pop {
                true
            } else if !can_push {
                false
            } else {
                rng.r#gen::<bool>()
            };

            if should_push {
                let (receipt_size, receipt_gas) = receipts[next_receipt_to_push_idx];
                next_receipt_to_push_idx += 1;
                groups_queue
                    .update_on_receipt_pushed(receipt_size, receipt_gas, trie_update, &config)
                    .unwrap();
                test_queue.update_on_receipt_pushed(receipt_size, receipt_gas, &config);
                buffered_receipts.push_back((receipt_size, receipt_gas));
            } else {
                let (receipt_size, receipt_gas) = buffered_receipts.pop_front().unwrap();
                groups_queue
                    .update_on_receipt_popped(receipt_size, receipt_gas, trie_update)
                    .unwrap();
                test_queue.update_on_receipt_popped(receipt_size, receipt_gas);
            }

            if rng.r#gen::<bool>() {
                // Reload the queue from trie. Tests that all changes are persisted after every operation.
                groups_queue =
                    ReceiptGroupsQueue::load(trie_update, ShardId::new(0)).unwrap().unwrap();
            }

            let groups_queue_groups: Vec<ReceiptGroup> =
                groups_queue.iter(trie_update, false).map(|group_res| group_res.unwrap()).collect();
            let test_queue_groups: Vec<ReceiptGroup> =
                test_queue.groups.iter().copied().map(|g| g.into()).collect();
            assert_eq!(groups_queue_groups, test_queue_groups);

            let total_size = groups_queue.total_size();
            let total_gas = groups_queue.total_gas();
            let expected_total_size: u64 =
                buffered_receipts.iter().map(|(size, _)| size.as_u64()).sum();
            let expected_total_gas =
                buffered_receipts.iter().map(|(_, gas)| *gas as u128).sum::<u128>();
            assert_eq!(total_size, expected_total_size);
            assert_eq!(total_gas, expected_total_gas);
            assert_eq!(groups_queue.total_receipts_num(), buffered_receipts.len() as u64);
        }
    }

    #[test]
    fn test_receipt_groups_queue_random_operations() {
        for seed in 0..10 {
            receipt_groups_queue_random_test(seed);
        }
    }

    /// The diff between two consecutive values that can be requested in a bandwidth request
    /// is ~103 kB. The upper bound of the receipt group size is 100 kB. The groups are small
    /// enough that bandwidth requests produced from group sizes are optimal, optimal meaning
    /// the same as if the requests were made based on individual receipt sizes.
    #[test]
    fn test_receipt_groups_produce_optimal_bandwidth_request() {
        let scheduler_params = BandwidthSchedulerParams::for_test(6);
        let request_values = BandwidthRequestValues::new(&scheduler_params).values;
        assert!(request_values[1] - request_values[0] > 102_000);

        let groups_config =
            ReceiptGroupsConfig { size_upper_bound: ByteSize::kb(100), gas_upper_bound: Gas::MAX };

        for test_num in 0..100 {
            let rng = &mut ChaCha20Rng::seed_from_u64(test_num);

            let initial_receipts_num = rng.gen_range(1..100);

            let mut buffered_receipts = VecDeque::new();
            let mut test_queue = TestReceiptGroupQueue::new();
            for _ in 0..initial_receipts_num {
                let receipt_size = ByteSize::b(get_random_receipt_size_for_test(rng));
                buffered_receipts.push_back(receipt_size);
                test_queue.update_on_receipt_pushed(receipt_size, 1, &groups_config);
            }

            let pop_push_num = rng.gen_range(0..100);
            for _ in 0..pop_push_num {
                let popped_receipt_size = buffered_receipts.pop_front().unwrap();
                test_queue.update_on_receipt_popped(popped_receipt_size, 1);

                // Ideal bandwidth request produced from individual receipt sizes.
                let ideal_bandwidth_request = BandwidthRequest::make_from_receipt_sizes(
                    ShardUId::single_shard().shard_id(),
                    buffered_receipts.iter().map(|s| Ok::<u64, Infallible>(s.as_u64())),
                    &scheduler_params,
                )
                .unwrap();
                // Bandwidth request produced from receipt groups.
                let groups_bandwidth_request = BandwidthRequest::make_from_receipt_sizes(
                    ShardUId::single_shard().shard_id(),
                    test_queue.groups.iter().map(|g| Ok::<u64, Infallible>(g.total_size as u64)),
                    &scheduler_params,
                )
                .unwrap();

                // Bandwidth request produced from receipt groups should be optimal (same as from
                // individual receipt sizes).
                assert_eq!(ideal_bandwidth_request, groups_bandwidth_request);

                let new_receipt_size = ByteSize::b(get_random_receipt_size_for_test(rng));
                buffered_receipts.push_back(new_receipt_size);
                test_queue.update_on_receipt_pushed(new_receipt_size, 1, &groups_config);
            }
        }
    }
}
