use std::hash::Hash;
use std::collections::HashMap;
use super::group::{Group, GroupsPerEpoch};
use super::hashable_message::HashableMessage;

/// Messages that approve a Group that satisfy certain criteria.
/// Examples:
/// * endorsements of representative message(s) of epoch X.
/// * promises of kickout(s) of epoch X.
#[derive(Debug)]
pub struct GroupApprovals<T: Hash> {
    /// Message being approved -> (owner uid -> message that do the approval).
    pub approvals: HashMap<HashableMessage<T>, Group<T>>,
}

impl<T: Hash> GroupApprovals<T> {
    pub fn new() -> Self {
        GroupApprovals {
            approvals: HashMap::new(),
        }
    }

    /// Creates a GroupApproval by approving a Group.
    pub fn approve_group(group: &Group<T>, owner_uid: u64, approval: &HashableMessage<T>) -> Self {
        let mut result = Self::new();
        for (_message_owner, hashable_messages) in &group.messages_by_owner {
            for hashable_message in hashable_messages {
                result.approvals.entry(hashable_message.clone())
                    .or_insert_with(|| Group::new()).insert(owner_uid, approval);
            }
        }
        result
    }

    pub fn insert(&mut self, message: &HashableMessage<T>, owner_uid: u64, approval: &HashableMessage<T>) {
        self.approvals.entry(message.clone()).or_insert_with(|| Group::new()).insert(owner_uid, approval);
    }

    pub fn union_update(&mut self, other: &Self) {
        for (message, per_message) in &other.approvals {
            let mut own_per_message = self.approvals.entry(message.clone())
                .or_insert_with(|| Group::new());
            own_per_message.union_update(per_message);
        }
    }
}

/// Mapping of group approvals to epochs that they approve.
#[derive(Debug)]
pub struct GroupApprovalPerEpoch<T: Hash> {
    approvals_per_epoch: HashMap<u64, GroupApprovals<T>>,
}

impl<T: Hash> GroupApprovalPerEpoch<T> {
    pub fn new() -> Self {
        GroupApprovalPerEpoch {
            approvals_per_epoch: HashMap::new(),
        }
    }

    /// Creates GroupApprovalPerEpoch by approving Groups in each epoch.
    pub fn approve_groups(groups: &GroupsPerEpoch<T>, owner_uid: u64, approval: &HashableMessage<T>) -> Self {
        let mut result = Self::new();
        for (epoch, group) in &groups.messages_by_epoch {
            result.approvals_per_epoch.insert(*epoch, GroupApprovals::approve_group(
                group, owner_uid, approval));
        }
        result
    }

    pub fn insert(&mut self, epoch: u64, message: &HashableMessage<T>, owner_uid: u64,
                  approval: &HashableMessage<T>) {
        self.approvals_per_epoch.entry(epoch).or_insert_with(|| GroupApprovals::new())
            .insert(message, owner_uid, approval);
    }

    pub fn contains_epoch(&self, epoch: &u64) -> bool {
        self.approvals_per_epoch.contains_key(epoch)
    }

    pub fn union_update(&mut self, other: &Self) {
        for (epoch, per_epoch) in &other.approvals_per_epoch {
            self.approvals_per_epoch.entry(*epoch).or_insert_with(|| GroupApprovals::new())
                .union_update(per_epoch);
        }
    }
}
