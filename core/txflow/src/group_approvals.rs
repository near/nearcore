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

    fn contains_owner(&self, owner_uid: &u64) -> bool {
        (&self.approvals).into_iter()
            .any(|(_, group)| group.contains_owner(owner_uid))
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

    pub fn insert(&mut self, epoch: u64, message: &HashableMessage<T>, owner_uid: u64,
                  approval: &HashableMessage<T>) {
        self.approvals_per_epoch.entry(epoch).or_insert_with(|| GroupApprovals::new())
            .insert(message, owner_uid, approval);
    }

    pub fn contains_any_approval(&self, epoch: &u64, owner_uid: &u64) -> bool {
        match self.approvals_per_epoch.get(epoch) {
            None => false,
            Some(group_approvals) => group_approvals.contains_owner(owner_uid)
        }
    }

    pub fn contains_approval(&self, epoch: &u64, owner_uid: &u64, message: &HashableMessage<T>) -> bool {
        match self.approvals_per_epoch.get(epoch) {
            None => false,
            Some(group_approvals) => {
                match group_approvals.approvals.get(message) {
                    None => false,
                    Some(approvals) => approvals.contains_owner(owner_uid)
                }
            }
        }
    }

    pub fn union_update(&mut self, other: &Self) {
        for (epoch, per_epoch) in &other.approvals_per_epoch {
            self.approvals_per_epoch.entry(*epoch).or_insert_with(|| GroupApprovals::new())
                .union_update(per_epoch);
        }
    }

    pub fn contains_epoch(&self, epoch: &u64) -> bool {
        self.approvals_per_epoch.contains_key(epoch)
    }
}
