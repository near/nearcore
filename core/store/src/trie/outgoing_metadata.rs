use std::collections::BTreeMap;

use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::errors::StorageError;
use near_primitives::receipt::TrieQueueIndices;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::ShardId;
use near_primitives::version::ProtocolFeature;
use near_schema_checker_lib::ProtocolSchema;
use near_vm_runner::logic::ProtocolVersion;

use crate::TrieUpdate;

use super::receipts_column_helper::TrieQueue;
use super::TrieAccess;

/// Keeps metadata about receipts stored in the outgoing buffers.
#[derive(Debug)]
pub struct OutgoingMetadatas {
    /// Metadata for each shard.
    pub metadatas: BTreeMap<ShardId, OutgoingBufferMetadata>,
    /// Receipts are grouped into groups and metadatas keep group-level information.
    /// A new group is started when the size of the last group exceeds this threshold.
    pub group_size_threshold: u64,
}

impl OutgoingMetadatas {
    pub fn new(group_size_threshold: u64) -> Self {
        Self { metadatas: BTreeMap::new(), group_size_threshold }
    }

    /// Load shard metadata from the trie.
    pub fn load(
        trie: &dyn TrieAccess,
        shard_ids: impl Iterator<Item = ShardId>,
        group_size_threshold: u64,
        protocol_version: ProtocolVersion,
    ) -> Result<Self, StorageError> {
        if !ProtocolFeature::BandwidthScheduler.enabled(protocol_version) {
            return Ok(Self::new(group_size_threshold));
        }

        let mut metadatas = BTreeMap::new();
        for shard_id in shard_ids {
            let receipt_groups_info =
                ReceiptGroupsInfo::load_or_default(trie, shard_id, group_size_threshold)?;
            metadatas.insert(shard_id, OutgoingBufferMetadata { receipt_groups_info });
        }
        Ok(Self { metadatas, group_size_threshold })
    }

    /// Update the metadata when a new receipt is added at the end of the buffer.
    pub fn on_receipt_buffered(
        &mut self,
        shard_id: ShardId,
        receipt_size: u64,
        state_update: &mut TrieUpdate,
    ) -> Result<(), StorageError> {
        let metadata = self
            .metadatas
            .entry(shard_id)
            .or_insert_with(|| OutgoingBufferMetadata::new(shard_id, self.group_size_threshold));
        metadata.on_receipt_buffered(receipt_size, state_update)
    }

    /// Update the metadata when a receipt is removed from the front of the buffer.
    pub fn on_receipt_removed(
        &mut self,
        shard_id: ShardId,
        receipt_size: u64,
        state_update: &mut TrieUpdate,
    ) -> Result<(), StorageError> {
        let metadata = self.metadatas.get_mut(&shard_id).expect("Metadata should exist");
        metadata.on_receipt_removed(receipt_size, state_update)
    }
}

/// Keeps metadata about receipts stored in outgoing buffer to some shard.
#[derive(Debug)]
pub struct OutgoingBufferMetadata {
    receipt_groups_info: ReceiptGroupsInfo,
}

impl OutgoingBufferMetadata {
    pub fn new(to_shard: ShardId, group_size_threshold: u64) -> Self {
        OutgoingBufferMetadata {
            receipt_groups_info: ReceiptGroupsInfo::new(to_shard, group_size_threshold),
        }
    }

    pub fn load_or_default(
        to_shard: ShardId,
        group_size_threshold: u64,
        trie: &dyn TrieAccess,
    ) -> Result<Self, StorageError> {
        let receipt_groups_info =
            ReceiptGroupsInfo::load_or_default(trie, to_shard, group_size_threshold)?;
        Ok(Self { receipt_groups_info })
    }

    /// Update the metadata when a new receipt is added at the end of the buffer.
    pub fn on_receipt_buffered(
        &mut self,
        receipt_size: u64,
        state_update: &mut TrieUpdate,
    ) -> Result<(), StorageError> {
        self.receipt_groups_info.on_receipt_pushed(receipt_size, state_update)
    }

    /// Update the metadata when a receipt is removed from the front of the buffer.
    pub fn on_receipt_removed(
        &mut self,
        receipt_size: u64,
        state_update: &mut TrieUpdate,
    ) -> Result<(), StorageError> {
        self.receipt_groups_info.on_receipt_popped(receipt_size, state_update)
    }

    /// Iterate over the sizes of receipts stored in the outgoing buffer.
    /// Multiple consecutive receipts are grouped into a single group.
    /// The iterator returns the size of each group, not individual receipts.
    pub fn iter_receipt_group_sizes<'a>(
        &'a self,
        trie: &'a dyn TrieAccess,
        side_effects: bool,
    ) -> impl Iterator<Item = Result<u64, StorageError>> + 'a {
        self.receipt_groups_info.iter_receipt_group_sizes(trie, side_effects)
    }
}

/// Keeps information about the size of receipts stored in the outgoing buffer.
/// Consecutive receipts are grouped into groups and it keeps aggregate information about the groups,
/// not individual receipts. Receipts are grouped to keep the size of the struct small.
#[derive(Debug)]
struct ReceiptGroupsInfo {
    /// Receipts stored in the outgoing buffer, grouped into groups of consecutive receipts.
    /// Each group stores aggregate information about the receipts in the group.
    pub groups: ReceiptGroupInfoQueue,
    /// A new receipt group is started when the size of the last group exceeds this threshold.
    /// All groups (except for the first and last one) have at least this size.
    pub group_size_threshold: u64,
}

impl ReceiptGroupsInfo {
    pub fn new(to_shard: ShardId, group_size_threshold: u64) -> Self {
        Self { groups: ReceiptGroupInfoQueue::new(to_shard), group_size_threshold }
    }

    pub fn load_or_default(
        trie: &dyn TrieAccess,
        to_shard: ShardId,
        group_size_threshold: u64,
    ) -> Result<Self, StorageError> {
        let groups = ReceiptGroupInfoQueue::load_or_default(to_shard, trie)?;
        Ok(Self { groups, group_size_threshold })
    }

    /// Update the groups when a new receipt is added at the end of the buffer.
    pub fn on_receipt_pushed(
        &mut self,
        receipt_size: u64,
        state_update: &mut TrieUpdate,
    ) -> Result<(), StorageError> {
        let last_group_opt = self.groups.pop_back(state_update)?;

        match last_group_opt {
            Some(last_group) if last_group.group_size() >= self.group_size_threshold => {
                // The last group's size exceeds the threshold, start a new group.
                self.groups.push_back(state_update, &last_group).expect("overflow");
                self.groups
                    .push_back(
                        state_update,
                        &ReceiptGroupInfo::V0(ReceiptGroupInfoV0 { group_size: receipt_size }),
                    )
                    .expect("overflow");
            }
            Some(mut last_group) => {
                // Add the receipt to the last group, its size doesn't exceed the threshold yet.
                *last_group.group_size_mut() = last_group.group_size().checked_add(receipt_size)
                .unwrap_or_else(|| panic!(
                    "OutgoingBufferMetadataV0::on_receipt_pushed fatal error - group size exceeds u32. \
                    This should never happen - size of two receipts fits into u32 and the group size threshold \
                    is lower than the size of two receipts. last_group_size: {}, receipt_size: {}",
                    last_group.group_size(), receipt_size));
                self.groups.push_back(state_update, &last_group).expect("overflow");
            }
            None => {
                // There are no groups yet, start a new group.
                self.groups
                    .push_back(
                        state_update,
                        &ReceiptGroupInfo::V0(ReceiptGroupInfoV0 { group_size: receipt_size }),
                    )
                    .expect("overflow");
            }
        }

        Ok(())
    }

    /// Update the groups when a receipt is removed from the front of the buffer.
    pub fn on_receipt_popped(
        &mut self,
        receipt_size: u64,
        state_update: &mut TrieUpdate,
    ) -> Result<(), StorageError> {
        // Iterate with side effects
        let first_group = self.groups.iter(state_update, true).next().expect(
            "OutgoingBufferMetadataV0::on_receipt_popped fatal error - there are no groups",
        )?;

        if first_group.group_size() == receipt_size {
            self.groups.pop_front(state_update)?;
        } else {
            self.groups.modify_first(state_update, |group| {
                *group.group_size_mut() =
                    group.group_size().checked_sub(receipt_size).unwrap_or_else(|| {
                        panic!(
                            "OutgoingBufferMetadataV0::on_receipt_popped fatal error - \
                        size of popped receipt exceeds size of first receipt group ({} > {})",
                            receipt_size,
                            first_group.group_size()
                        )
                    });
            })
        }

        Ok(())
    }

    /// Iterate over the sizes of receipt groups.
    pub fn iter_receipt_group_sizes<'a>(
        &'a self,
        trie: &'a dyn TrieAccess,
        side_effects: bool,
    ) -> impl Iterator<Item = Result<u64, StorageError>> + 'a {
        self.groups.iter(trie, side_effects).map(|res| res.map(|g| g.group_size()))
    }
}

#[derive(Debug)]
struct ReceiptGroupInfoQueue {
    shard_id: ShardId,
    indices: TrieQueueIndices,
}

impl ReceiptGroupInfoQueue {
    pub fn new(shard_id: ShardId) -> Self {
        Self { shard_id, indices: TrieQueueIndices::default() }
    }

    pub fn load_or_default(shard_id: ShardId, trie: &dyn TrieAccess) -> Result<Self, StorageError> {
        let indices = crate::get_outgoing_receipts_groups_inidices(trie, shard_id)?;
        Ok(Self { shard_id, indices })
    }
}

impl TrieQueue for ReceiptGroupInfoQueue {
    type Item<'a> = ReceiptGroupInfo;

    fn load_indices(&self, trie: &dyn TrieAccess) -> Result<TrieQueueIndices, StorageError> {
        crate::get_outgoing_receipts_groups_inidices(trie, self.shard_id)
    }

    fn indices(&self) -> TrieQueueIndices {
        self.indices.clone()
    }

    fn indices_mut(&mut self) -> &mut TrieQueueIndices {
        &mut self.indices
    }

    fn write_indices(&self, state_update: &mut TrieUpdate) {
        crate::set(
            state_update,
            TrieKey::OutgoingReceiptsGroupsIndices { receiving_shard: self.shard_id },
            &self.indices,
        );
    }

    fn trie_key(&self, index: u64) -> TrieKey {
        TrieKey::OutgoingReceiptsGroup { receiving_shard: self.shard_id, index }
    }
}

/// Aggregate information about a group of consecutive receipts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
enum ReceiptGroupInfo {
    V0(ReceiptGroupInfoV0),
}

impl ReceiptGroupInfo {
    pub fn group_size(&self) -> u64 {
        match self {
            Self::V0(group) => group.group_size,
        }
    }

    pub fn group_size_mut(&mut self) -> &mut u64 {
        match self {
            Self::V0(group) => &mut group.group_size,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
struct ReceiptGroupInfoV0 {
    pub group_size: u64,
}
