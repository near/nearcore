use std::hash::Hash;
use std::collections::{HashMap, HashSet};

use primitives::traits::WitnessSelectorLike;

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

    pub fn superapproved_messages<P>(&self, witness_selector: &P) -> GroupsPerEpoch<T>
        where P: WitnessSelectorLike {
        let mut result = GroupsPerEpoch::new();
        for (epoch, per_epoch) in &self.approvals_per_epoch {
            let epoch_witnesses = witness_selector.epoch_witnesses(*epoch);
            for (message, _approvals) in &per_epoch.approvals {
                let witnesses: HashSet<u64> = _approvals.messages_by_owner.keys().map(|x| *x).collect();
                if (&witnesses & epoch_witnesses).len() > epoch_witnesses.len()*2/3 {
                    // We compute owner_uid only when there is a supermajority approval.
                    // This allows to avoid for the message to borrow its own message, because it
                    // will never be the case that the message which is a representative message or
                    // a kickout message has a supermajority support once posted, unless there is
                    // a single participant.
                    // TODO: Make it work for a single participant.
                    let owner_uid = {
                        let r = (&message.message).upgrade().expect("Parent messages should be present");
                        let data = &r.borrow().data;
                        data.body.owner_uid
                    };
                    result.insert(*epoch, owner_uid, message);
                    // There can be only one superapproved messaged when forked.
                    break;
                }
            }
        }
        result
    }
}
