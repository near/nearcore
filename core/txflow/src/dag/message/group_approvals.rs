use std::collections::{HashMap, HashSet};

use primitives::consensus::{Payload, WitnessSelector};
use primitives::types::UID;

use super::group::{Group, GroupsPerEpoch};
use super::Message;

/// Messages that approve a Group that satisfy certain criteria.
/// Examples:
/// * endorsements of representative message(s) of epoch X.
/// * promises of kickout(s) of epoch X.
#[derive(Debug)]
pub struct GroupApprovals<'a, P: 'a + Payload> {
    /// Message being approved -> (owner uid -> message that do the approval).
    pub approvals: HashMap<&'a Message<'a, P>, Group<'a, P>>,
}

impl<'a, P: 'a + Payload> GroupApprovals<'a, P> {
    pub fn new() -> Self {
        GroupApprovals { approvals: HashMap::new() }
    }

    pub fn approve_group(group: &'a Group<'a, P>, approver: &'a Message<'a, P>) -> Self {
        let mut result = Self::new();
        for messages in group.messages_by_owner.values() {
            for message in messages {
                result.approvals.entry(message).or_insert_with(Group::new).insert(approver);
            }
        }
        result
    }

    pub fn insert(&mut self, message: &'a Message<'a, P>, approval: &'a Message<'a, P>) {
        self.approvals.entry(message).or_insert_with(Group::new).insert(approval);
    }

    pub fn union_update(&mut self, other: &Self) {
        for (message, per_message) in &other.approvals {
            let own_per_message = self.approvals.entry(message).or_insert_with(Group::new);
            own_per_message.union_update(per_message);
        }
    }

    fn contains_owner(&self, owner_uid: UID) -> bool {
        (&self.approvals).values().any(|group| group.contains_owner(owner_uid))
    }
}

/// Mapping of group approvals to epochs that they approve.
#[derive(Debug)]
pub struct GroupApprovalPerEpoch<'a, P: 'a + Payload> {
    approvals_per_epoch: HashMap<u64, GroupApprovals<'a, P>>,
}

impl<'a, P: 'a + Payload> GroupApprovalPerEpoch<'a, P> {
    pub fn new() -> Self {
        GroupApprovalPerEpoch { approvals_per_epoch: HashMap::new() }
    }

    pub fn approve_groups_per_epoch(
        groups: &'a GroupsPerEpoch<'a, P>,
        approver: &'a Message<'a, P>,
    ) -> Self {
        let mut result = Self::new();
        for (epoch, group) in &groups.messages_by_epoch {
            result
                .approvals_per_epoch
                .insert(*epoch, GroupApprovals::approve_group(group, approver));
        }
        result
    }

    pub fn insert(
        &mut self,
        epoch: u64,
        message: &'a Message<'a, P>,
        approval: &'a Message<'a, P>,
    ) {
        self.approvals_per_epoch
            .entry(epoch)
            .or_insert_with(GroupApprovals::new)
            .insert(message, approval);
    }

    pub fn contains_any_approval(&self, epoch: u64, owner_uid: UID) -> bool {
        match self.approvals_per_epoch.get(&epoch) {
            None => false,
            Some(group_approvals) => group_approvals.contains_owner(owner_uid),
        }
    }

    /// Check if there is an epoch greater than `epoch` for which we gave an approval.
    pub fn contains_any_future_approvals(&self, epoch: u64, owner_uid: UID) -> bool {
        (&self.approvals_per_epoch)
            .iter()
            .any(|(e, group_approvals)| *e > epoch && group_approvals.contains_owner(owner_uid))
    }

    /// Whether there is an approval of the message of the epoch by the owner_uid.
    pub fn contains_approval(&self, epoch: u64, owner_uid: UID, message: &Message<'a, P>) -> bool {
        match self.approvals_per_epoch.get(&epoch) {
            None => false,
            Some(group_approvals) => match group_approvals.approvals.get(message) {
                None => false,
                Some(approvals) => approvals.contains_owner(owner_uid),
            },
        }
    }

    pub fn union_update(&mut self, other: &Self) {
        for (epoch, per_epoch) in &other.approvals_per_epoch {
            self.approvals_per_epoch
                .entry(*epoch)
                .or_insert_with(GroupApprovals::new)
                .union_update(per_epoch);
        }
    }

    // Used in test only.
    #[allow(dead_code)]
    pub fn contains_epoch(&self, epoch: u64) -> bool {
        self.approvals_per_epoch.contains_key(&epoch)
    }

    /// Get messages that are superapproved.
    pub fn superapproved_messages<W>(&self, witness_selector: &W) -> GroupsPerEpoch<'a, P>
    where
        W: WitnessSelector,
    {
        let mut result = GroupsPerEpoch::new();
        for (epoch, per_epoch) in &self.approvals_per_epoch {
            let epoch_witnesses = witness_selector.epoch_witnesses(*epoch);
            for (message, _approvals) in &per_epoch.approvals {
                let witnesses: HashSet<u64> =
                    _approvals.messages_by_owner.keys().cloned().collect();
                if (epoch_witnesses.len() * 2 / 3 + 1) <= (&witnesses & epoch_witnesses).len() {
                    result.insert(*epoch, *message);
                }
            }
        }
        result
    }

    /// Check whether there are messages that become superapproved once we add the complementary
    /// approvals.
    pub fn new_superapproved_messages<W>(
        &self,
        complementary_approvals: &GroupsPerEpoch<'a, P>,
        complementary_owner_uid: UID,
        witness_selector: &W,
    ) -> GroupsPerEpoch<'a, P>
    where
        W: WitnessSelector,
    {
        let mut result = GroupsPerEpoch::new();
        for (epoch, per_epoch) in &self.approvals_per_epoch {
            let epoch_witnesses = witness_selector.epoch_witnesses(*epoch);
            for (message, _approvals) in &per_epoch.approvals {
                let witnesses: HashSet<u64> =
                    _approvals.messages_by_owner.keys().cloned().collect();
                let num_missing = (epoch_witnesses.len() * 2 / 3 + 1) as i64
                    - ((&witnesses & epoch_witnesses).len() as i64);
                if num_missing == 1 {
                    let one_more_approval =
                        match complementary_approvals.messages_by_epoch.get(epoch) {
                            None => false,
                            Some(group) => {
                                match group.messages_by_owner.get(&message.data.body.owner_uid) {
                                    None => false,
                                    Some(approvals) => approvals.contains(message),
                                }
                            }
                        };
                    let valid_approver =
                        (epoch_witnesses - &witnesses).contains(&complementary_owner_uid);
                    if one_more_approval & valid_approver {
                        // TODO: With the current implementation a single verifier will publish
                        // only every second epoch.
                        result.insert(*epoch, *message);
                    }
                }
            }
        }
        result
    }
}
