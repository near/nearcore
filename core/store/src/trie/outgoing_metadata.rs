use std::collections::VecDeque;

use borsh::{BorshDeserialize, BorshSerialize};
use near_schema_checker_lib::ProtocolSchema;

/// Keeps metadata about receipts stored in outgoing buffer to some shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutgoingBufferMetadata {
    pub receipt_group_sizes: ReceiptGroupSizes,
}

impl OutgoingBufferMetadata {
    pub fn new(group_size_threshold: u32) -> Self {
        OutgoingBufferMetadata {
            receipt_group_sizes: ReceiptGroupSizes::V0(ReceiptGroupSizesV0::new(
                group_size_threshold,
            )),
        }
    }

    /// Update the metadata when a new receipt is added at the end of the buffer.
    pub fn on_receipt_buffered(&mut self, receipt_size: u64) {
        match &mut self.receipt_group_sizes {
            ReceiptGroupSizes::V0(v0) => v0.on_receipt_pushed(receipt_size),
        }
    }

    /// Update the metadata when a receipt is removed from the front of the buffer.
    pub fn on_receipt_removed(&mut self, receipt_size: u64) {
        match &mut self.receipt_group_sizes {
            ReceiptGroupSizes::V0(v0) => v0.on_receipt_popped(receipt_size),
        }
    }

    /// Iterate over the sizes of receipts stored in the outgoing buffer.
    /// Multiple consecutive receipts are grouped into a single group.
    /// The iterator returns the size of each group, not individual receipts.
    pub fn iter_receipt_group_sizes(&self) -> impl Iterator<Item = u64> + '_ {
        match &self.receipt_group_sizes {
            ReceiptGroupSizes::V0(v0) => v0.iter_receipt_group_sizes(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub enum ReceiptGroupSizes {
    V0(ReceiptGroupSizesV0),
}

/// Keeps information about the size of receipts stored in the outgoing buffer.
/// Consecutive receipts are grouped into groups and it keeps aggregate information about the groups,
/// not individual receipts. Receipts are grouped to keep the size of the struct small.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ReceiptGroupSizesV0 {
    /// Receipts stored in the outgoing buffer, grouped into groups of consecutive receipts.
    /// Each group stores aggregate information about the receipts in the group.
    pub groups: VecDeque<ReceiptGroup>,
    /// A new receipt group is started when the size of the last group exceeds this threshold.
    /// All groups (except for the first and last one) have at least this size.
    pub group_size_threshold: u32,
}

impl ReceiptGroupSizesV0 {
    pub fn new(group_size_threshold: u32) -> Self {
        Self { groups: VecDeque::new(), group_size_threshold }
    }

    /// Update the groups when a new receipt is added at the end of the buffer.
    pub fn on_receipt_pushed(&mut self, receipt_size: u64) {
        let receipt_size: u32 = receipt_size_to_u32(receipt_size);

        match self.groups.back_mut() {
            Some(last_group) if last_group.group_size >= self.group_size_threshold => {
                // The last group's size exceeds the threshold, start a new group.
                self.groups.push_back(ReceiptGroup { group_size: receipt_size });
            }
            Some(last_group) => {
                // Add the receipt to the last group, its size doesn't exceed the threshold yet.
                last_group.group_size = last_group.group_size.checked_add(receipt_size)
                .unwrap_or_else(|| panic!(
                    "OutgoingBufferMetadataV0::on_receipt_pushed fatal error - group size exceeds u32. \
                    This should never happen - size of two receipts fits into u32 and the group size threshold \
                    is lower than the size of two receipts. last_group_size: {}, receipt_size: {}",
                    last_group.group_size, receipt_size));
            }
            None => {
                // There are no groups yet, start a new group.
                self.groups.push_back(ReceiptGroup { group_size: receipt_size });
            }
        }
    }

    /// Update the groups when a receipt is removed from the front of the buffer.
    pub fn on_receipt_popped(&mut self, receipt_size: u64) {
        let receipt_size: u32 = receipt_size_to_u32(receipt_size);

        let first_group = self.groups.front_mut().expect(
            "OutgoingBufferMetadataV0::on_receipt_popped fatal error - there are no groups",
        );

        first_group.group_size =
            first_group.group_size.checked_sub(receipt_size).unwrap_or_else(|| {
                panic!(
                    "OutgoingBufferMetadataV0::on_receipt_popped fatal error - \
                    size of popped receipt exceeds size of first receipt group ({} > {})",
                    receipt_size, first_group.group_size
                )
            });

        if first_group.group_size == 0 {
            self.groups.pop_front();
        }
    }

    /// Iterate over the sizes of receipt groups.
    pub fn iter_receipt_group_sizes(&self) -> impl Iterator<Item = u64> + '_ {
        self.groups.iter().map(|group| group.group_size.into())
    }
}

fn receipt_size_to_u32(receipt_size: u64) -> u32 {
    receipt_size.try_into().unwrap_or_else(|_| panic!(
        "Receipt size exceeds u32, this is unexpected. Receipts are under 4MB, u32 can hold 4GB. receipt_size: {}",
        receipt_size
    ))
}

/// Aggregate information about a group of consecutive receipts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ReceiptGroup {
    /// Total size of receipts in this group.
    /// Size is stored as u32 to keep the struct small.
    pub group_size: u32,
}
