use std::collections::{HashMap, HashSet};

use message::Message;
use primitives::traits::{PayloadLike, WitnessSelectorLike};

use super::group::{Group, GroupsPerEpoch};

/// Messages that approve a Group that satisfy certain criteria.
/// Examples:
/// * endorsements of representative message(s) of epoch X.
/// * promises of kickout(s) of epoch X.
#[derive(Debug)]
pub struct GroupApprovals<'a, P: 'a + PayloadLike> {
    /// Message being approved -> (owner uid -> message that do the approval).
    pub approvals: HashMap<&'a Message<'a, P>, Group<'a, P>>,
}

impl<'a, P: 'a + PayloadLike> GroupApprovals<'a, P> {
    pub fn new() -> Self {
        GroupApprovals {
            approvals: HashMap::new(),
        }
    }

    pub fn approve_group(group: &'a Group<'a, P>, approver: &'a Message<'a, P>) -> Self {
        let mut result = Self::new();
        for messages in group.messages_by_owner.values() {
            for message in messages {
                result.approvals.entry(message).or_insert_with(|| Group::new())
                    .insert(approver);
            }
        }
        result
    }

    pub fn insert(&mut self, message: &'a Message<'a, P>, approval: &'a Message<'a, P>) {
        self.approvals.entry(message).or_insert_with(|| Group::new()).insert(approval);
    }

    pub fn union_update(&mut self, other: &Self) {
        for (message, per_message) in &other.approvals {
            let mut own_per_message = self.approvals.entry(message)
                .or_insert_with(|| Group::new());
            own_per_message.union_update(per_message);
        }
    }

    fn contains_owner(&self, owner_uid: &u64) -> bool {
        (&self.approvals).values().any(|group| group.contains_owner(owner_uid))
    }
}

/// Mapping of group approvals to epochs that they approve.
#[derive(Debug)]
pub struct GroupApprovalPerEpoch<'a, P: 'a + PayloadLike> {
    approvals_per_epoch: HashMap<u64, GroupApprovals<'a, P>>,
}

impl<'a, P: 'a + PayloadLike> GroupApprovalPerEpoch<'a, P> {
    pub fn new() -> Self {
        GroupApprovalPerEpoch {
            approvals_per_epoch: HashMap::new(),
        }
    }

    pub fn approve_groups_per_epoch(groups: &'a GroupsPerEpoch<'a, P>, approver: &'a Message<'a, P>) -> Self {
        let mut result = Self::new();
        for (epoch, group) in &groups.messages_by_epoch {
            result.approvals_per_epoch.insert(*epoch, GroupApprovals::approve_group(group, approver));
        }
        result
    }

    pub fn insert(&mut self, epoch: u64, message: &'a Message<'a, P>, approval: &'a Message<'a, P>) {
        self.approvals_per_epoch.entry(epoch).or_insert_with(|| GroupApprovals::new())
            .insert(message, approval);
    }

    pub fn contains_any_approval(&self, epoch: &u64, owner_uid: &u64) -> bool {
        match self.approvals_per_epoch.get(epoch) {
            None => false,
            Some(group_approvals) => group_approvals.contains_owner(owner_uid)
        }
    }

    /// Whether there is an approval of the message of the epoch by the owner_uid.
    pub fn contains_approval(&self, epoch: &u64, owner_uid: &u64, message: &Message<'a, P>) -> bool {
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

    pub fn superapproved_messages<W>(&self, witness_selector: &W) -> GroupsPerEpoch<'a, P>
        where W: WitnessSelectorLike {
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
                    result.insert(*epoch, *message);
                    // There can be only one superapproved messaged when forked.
                    break;
                }
            }
        }
        result
    }
}
