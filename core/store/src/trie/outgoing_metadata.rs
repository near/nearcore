use std::collections::BTreeMap;

use borsh::{BorshDeserialize, BorshSerialize};
use bytesize::ByteSize;
use near_primitives::errors::StorageError;
use near_primitives::receipt::TrieQueueIndices;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{Gas, ShardId};
use near_primitives::version::ProtocolFeature;
use near_schema_checker_lib::ProtocolSchema;
use near_vm_runner::logic::ProtocolVersion;

use crate::{get, set, TrieUpdate};

use super::receipts_column_helper::TrieQueue;
use super::TrieAccess;

/// Keeps metadata about receipts stored in the outgoing buffers.
#[derive(Debug)]
pub struct OutgoingMetadatas {
    /// Metadata information for outgoing buffer to each shard.
    metadatas: BTreeMap<ShardId, ReceiptGroupsQueue>,
    /// Parameteres which control size of receipt groups.
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
        protocol_version: ProtocolVersion,
    ) -> Result<Self, StorageError> {
        if !ProtocolFeature::BandwidthScheduler.enabled(protocol_version) {
            return Ok(Self::new(groups_config));
        }

        let mut metadatas = BTreeMap::new();
        for shard_id in shard_ids.into_iter() {
            let metadata = ReceiptGroupsQueue::load(trie, shard_id, groups_config)?;
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
        let metadata = self
            .metadatas
            .entry(shard_id)
            .or_insert_with(|| ReceiptGroupsQueue::new(shard_id, self.groups_config));
        metadata.update_on_receipt_pushed(receipt_size, receipt_gas, state_update)
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
#[derive(Debug, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub enum ReceiptGroup {
    V0(ReceiptGroupV0),
}

#[derive(Debug, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ReceiptGroupV0 {
    /// Total size of receipts in this group.
    /// Should be no larger than `max_receipt_size`, otherwise the bandwidth
    /// scheduler will not be able to grant the bandwidth neeed to send
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
/// There is a lower bound and upper bound on size and gas.
/// By default receipts are added to the last group in the queue.
/// A new group is started if:
///     A) adding a new receipt would make the group exceed the upper bound
///     B) the last group already has size or gas above the lower bound
///
/// This way we ensure that the size and gas of the groups are within the bounds,
/// as long as size/gas of a single receipt doesn't exceed the upper bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceiptGroupsConfig {
    /// All receipt groups aim to have a size above this threshold.
    pub size_lower_bound: ByteSize,
    /// All receipt groups aim to have a size below this threshold.
    /// Should be no larger than `max_receipt_size`, otherwise the bandwidth
    /// scheduler will not be able to grant the bandwidth neeed to send
    /// the receipts in this group.
    pub size_upper_bound: ByteSize,
    /// All receipt groups aim to have gas above this threshold.
    pub gas_lower_bound: Gas,
    /// All receipt groups aim to have gas below this threshold.
    pub gas_upper_bound: Gas,
}

impl ReceiptGroupsConfig {
    pub fn default_config() -> Self {
        // TODO(bandwidth_scheduler) - put in runtime config
        ReceiptGroupsConfig {
            size_lower_bound: ByteSize::kb(90),
            size_upper_bound: ByteSize::b(4 * 1024 * 1024), // max_receipt_size
            gas_lower_bound: Gas::MAX,
            gas_upper_bound: Gas::MAX,
        }
    }

    /// Decide whether a new receipt should be added to the last group
    /// or a new group be started for this receipt.
    /// Enforces the lower and upper bounds on size and gas.
    pub fn should_start_new_group(
        &self,
        last_group: &ReceiptGroup,
        new_receipt_size: ByteSize,
        new_receipt_gas: Gas,
    ) -> bool {
        let mut group_size = last_group.size();
        if group_size > self.gas_lower_bound {
            // The existing group has size above the threshold, start a new group.
            return true;
        }

        add_size_checked(&mut group_size, new_receipt_size);
        if group_size > self.size_upper_bound.as_u64() {
            // The new group would be too large, start a new group.
            return true;
        }

        let mut group_gas = last_group.gas();
        if group_gas > self.gas_lower_bound.into() {
            // The existing group has gas above the threshold, start a new group.
            return true;
        }

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
    /// Parameters which control size and gas of receipt groups.
    groups_config: ReceiptGroupsConfig,
}

impl ReceiptGroupsQueue {
    /// Create a new empty queue.
    pub fn new(receiver_shard: ShardId, config: ReceiptGroupsConfig) -> ReceiptGroupsQueue {
        ReceiptGroupsQueue {
            receiver_shard,
            data: ReceiptGroupsQueueDataV0::default(),
            groups_config: config,
        }
    }

    /// Load a queue from the trie.
    /// Returns None if the queue doesn't exist.
    pub fn load(
        trie: &dyn TrieAccess,
        receiver_shard: ShardId,
        config: ReceiptGroupsConfig,
    ) -> Result<Option<Self>, StorageError> {
        let data_opt = Self::load_data(trie, receiver_shard)?;
        let receipt_groups_queue_opt =
            data_opt.map(|data| Self { receiver_shard, data, groups_config: config });
        Ok(receipt_groups_queue_opt)
    }

    fn load_data(
        trie: &dyn TrieAccess,
        receiver_shard: ShardId,
    ) -> Result<Option<ReceiptGroupsQueueDataV0>, StorageError> {
        let data_opt: Option<ReceiptGroupsQueueData> = get(
            trie,
            &TrieKey::OutgoingReceiptGroupsQueueData { receiving_shard: receiver_shard },
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
            TrieKey::OutgoingReceiptGroupsQueueData { receiving_shard: self.receiver_shard },
            &data_enum,
        );
    }

    pub fn update_on_receipt_pushed(
        &mut self,
        receipt_size: ByteSize,
        receipt_gas: Gas,
        state_update: &mut TrieUpdate,
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
                if self.groups_config.should_start_new_group(&last_group, receipt_size, receipt_gas)
                {
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
        substract_size_checked(&mut self.data.total_size, receipt_size);
        substract_gas_checked(&mut self.data.total_gas, receipt_gas);

        self.data.total_receipts_num = self
            .data
            .total_receipts_num
            .checked_sub(1)
            .expect("Underflow! - More receipts were popped than pushed!");

        assert!(self.data.indices.len() > 0, "No receipt groups to pop from!");

        self.modify_first(state_update, |mut first_group| {
            substract_size_checked(first_group.size_mut(), receipt_size);
            substract_gas_checked(first_group.gas_mut(), receipt_gas);
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
        TrieKey::OutgoingReceiptGroupsQueueItem { receiving_shard: self.receiver_shard, index }
    }
}

fn add_size_checked(total: &mut u64, delta: ByteSize) {
    *total = total
        .checked_add(delta.as_u64())
        .expect("add_size_checked - Overflow! Reached exabytes of size!");
}

fn substract_size_checked(total: &mut u64, delta: ByteSize) {
    *total = total
        .checked_sub(delta.as_u64())
        .expect("substract_size_checked - Underflow! Negative size!");
}

fn add_gas_checked(total: &mut u128, delta: Gas) {
    *total = total
        .checked_add(delta.into())
        .expect("add_gas_checked - Overflow! Total gas doesn't fit into u128!");
}

fn substract_gas_checked(total: &mut u128, delta: Gas) {
    *total =
        total.checked_sub(delta.into()).expect("substract_gas_checked - Underflow! Negative gas!")
}
