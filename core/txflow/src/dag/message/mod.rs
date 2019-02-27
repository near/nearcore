pub mod group;
mod group_approvals;

use std::borrow::Borrow;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

use primitives::consensus::{Payload, WitnessSelector};
use primitives::types;

use self::group::GroupsPerEpoch;
use self::group_approvals::GroupApprovalPerEpoch;

static UNINITIALIZED_MESSAGE_ERR: &'static str =
    "This usage of the message requires its initialization.";

/// Represents the message of the DAG, T is the payload parameter. For in-shard TxFlow and
/// beacon-chain TxFlow T takes different values.
#[derive(Debug)]
pub struct Message<'a, P: 'a + Payload> {
    pub data: types::SignedMessageData<P>,

    pub parents: HashSet<&'a Message<'a, P>>,

    // The following fields are computed based on the approved messages, using init call.
    is_initialized: bool,
    /// The computed epoch of the message. If this message is restored from the epoch block then
    /// the epoch is taken from the data.
    pub computed_epoch: u64,
    /// The hash of the message. Depends on the epoch.
    pub computed_hash: types::TxFlowHash,
    /// If this message is a representative this stores which epoch it is representative of.
    computed_is_representative: Option<u64>,
    /// Computed flag of whether this message is a kickout.
    computed_is_kickout: bool,
    /// Computed flag whether this message was created by an epoch leader.
    computed_is_epoch_leader: bool,
    /// Computed endorsements given by this message.
    computed_endorsements: GroupsPerEpoch<'a, P>,
    /// Computed promises given by this message.
    computed_promises: GroupsPerEpoch<'a, P>,
    /// Computed epochs that became complete due to this message.
    computed_complete_epochs: GroupsPerEpoch<'a, P>,
    /// Computed epochs that correspond to representative messages that are now publishable.
    pub computed_publishable_epochs: GroupsPerEpoch<'a, P>,

    // The following are the approved messages, grouped by different criteria.
    /// Epoch -> messages that have that epoch.
    pub approved_epochs: GroupsPerEpoch<'a, P>,
    /// Epoch -> a/all representatives of that epoch (supports forks).
    approved_representatives: GroupsPerEpoch<'a, P>,
    /// Epoch -> a/all kickouts of that epoch (supports forks).
    approved_kickouts: GroupsPerEpoch<'a, P>,
    /// Endorsements of representatives (supports endorsements on forked representatives).
    approved_endorsements: GroupApprovalPerEpoch<'a, P>,
    /// Promises to kickout a representative message (supports promises on forked kickouts).
    approved_promises: GroupApprovalPerEpoch<'a, P>,
    /// Epoch -> Either a representative message that has >2/3 endorsements or a kickout message
    /// that has >2/3 promises. Epoch should always have one element, but our design does not restrict to it.
    /// TODO: Rework the design so that the restriction is inherent, this also should improve the
    /// performance.
    approved_complete_epochs: GroupsPerEpoch<'a, P>,
    /// Epoch -> representative message that is publishable.
    approved_publishable_epochs: GroupsPerEpoch<'a, P>,
    // NOTE, a single message can be simultaneously:
    // a) a representative message of epoch X;
    // b) an endorsement of a representative message of epoch Y, Y<X;
    // c) a promise for a kickout message of a representative message of epoch Z, Z<X (Z can be
    //    equal to Y).
    //
    // It can also be simultaneously:
    // a) a kickout message of a representative message of epoch A;
    // b) an endorsement of a representative message of epoch Y, Y<A;
    // c) a promise for a kickout message of a representative message of epoch Z, Z<A (Z can be
    //    equal to Y).
    //
    // * In both cases for (b) and (c) a message can give multiple endorsements and promises as long
    //   as Y and Z satisfy the constraints.
    // * Endorsements are explicit since they require a part of the BLS signature. Promises,
    //   kickouts, and representative messages are implied by the parent messages.
    // * A message cannot be both a representative message of epoch X and a kickout message of epoch X-1.
    // * Also a representative message is supposed to endorse itself which is done by an owner
    //   including the part of the BLS signature in it. If the signature is not included that it
    //   does not endorse itself and is considered to be a recoverable deviation from the protocol.
}

impl<'a, P: Payload> Hash for Message<'a, P> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if !self.is_initialized {
            panic!(UNINITIALIZED_MESSAGE_ERR)
        }
        state.write_u64(self.computed_hash);
    }
}

impl<'a, P: Payload> PartialEq for Message<'a, P> {
    fn eq(&self, other: &Message<'a, P>) -> bool {
        if !self.is_initialized || !other.is_initialized {
            panic!(UNINITIALIZED_MESSAGE_ERR)
        }
        self.computed_hash == other.computed_hash
    }
}

impl<'a, P: Payload> Eq for Message<'a, P> {}

impl<'a, P: Payload> Borrow<u64> for &'a Message<'a, P> {
    fn borrow(&self) -> &u64 {
        if !self.is_initialized {
            panic!(UNINITIALIZED_MESSAGE_ERR)
        }
        &self.computed_hash
    }
}

impl<'a, P: Payload> Message<'a, P> {
    pub fn new(data: types::SignedMessageData<P>) -> Message<'a, P> {
        Message {
            data,
            parents: HashSet::new(),
            is_initialized: false,
            computed_epoch: 0,
            computed_hash: 0,
            computed_is_representative: None,
            computed_is_kickout: false,
            computed_is_epoch_leader: false,
            computed_endorsements: GroupsPerEpoch::new(),
            computed_promises: GroupsPerEpoch::new(),
            computed_complete_epochs: GroupsPerEpoch::new(),
            computed_publishable_epochs: GroupsPerEpoch::new(),

            approved_epochs: GroupsPerEpoch::new(),
            approved_representatives: GroupsPerEpoch::new(),
            approved_kickouts: GroupsPerEpoch::new(),
            approved_endorsements: GroupApprovalPerEpoch::new(),
            approved_promises: GroupApprovalPerEpoch::new(),
            approved_complete_epochs: GroupsPerEpoch::new(),
            approved_publishable_epochs: GroupsPerEpoch::new(),
        }
    }

    fn compute_hash(&mut self) {
        let mut hasher = DefaultHasher::new();
        self.data.body.hash(&mut hasher);
        self.computed_hash = hasher.finish();
    }

    pub fn assume_computed_hash_epoch(&mut self) {
        if !self.is_initialized {
            panic!(UNINITIALIZED_MESSAGE_ERR)
        }
        self.data.body.epoch = self.computed_epoch;
        self.compute_hash();
        self.data.hash = self.computed_hash;
    }

    /// Computes the aggregated data from the parents and updates the message.
    pub fn aggregate_parents<W>(&mut self, witness_selector: &W)
    where
        W: WitnessSelector,
    {
        for p in &self.parents {
            if !p.is_initialized {
                panic!(UNINITIALIZED_MESSAGE_ERR)
            }

            self.approved_epochs.union_update(&p.approved_epochs);
            self.approved_epochs.insert(p.computed_epoch, *p);

            self.approved_representatives.union_update(&p.approved_representatives);
            if let Some(epoch) = p.computed_is_representative {
                self.approved_representatives.insert(epoch, *p);
                // Representative message endorses itself.
                self.approved_endorsements.insert(epoch, *p, *p);
            }

            self.approved_kickouts.union_update(&p.approved_kickouts);
            if p.computed_is_kickout {
                self.approved_kickouts.insert(p.computed_epoch - 1, *p);
                // Kickout message promises to itself.
                self.approved_promises.insert(p.computed_epoch - 1, *p, *p);
            }

            self.approved_endorsements.union_update(&p.approved_endorsements);
            self.approved_endorsements.union_update(
                &GroupApprovalPerEpoch::approve_groups_per_epoch(&p.computed_endorsements, p),
            );

            self.approved_promises.union_update(&p.approved_promises);
            self.approved_promises.union_update(&GroupApprovalPerEpoch::approve_groups_per_epoch(
                &p.computed_promises,
                p,
            ));

            self.approved_complete_epochs.union_update(&p.approved_complete_epochs);
            self.approved_complete_epochs.union_update(&p.computed_complete_epochs);

            self.approved_publishable_epochs.union_update(&p.approved_publishable_epochs);
            self.approved_publishable_epochs.union_update(&p.computed_publishable_epochs);
        }
        self.approved_complete_epochs
            .union_update(&self.approved_endorsements.superapproved_messages(witness_selector));
        self.approved_complete_epochs
            .union_update(&self.approved_promises.superapproved_messages(witness_selector));
    }

    pub fn approve(&self, message: &Message<'a, P>) -> bool {
        self.approved_epochs.contains_message(message)
    }

    /// Determines the previous epoch of the current owner. Otherwise returns the starting_epoch.
    fn prev_epoch(&'a self) -> Option<&'a u64> {
        // Iterate over past messages that were created by the current owner and return their max
        // epoch.
        self.approved_epochs.filter_by_owner(self.data.body.owner_uid).map(|(epoch, _)| epoch).max()
    }

    /// Determines whether the epoch of the current message should increase.
    fn should_promote<W>(&self, prev_epoch: u64, witness_selector: &W) -> bool
    where
        W: WitnessSelector,
    {
        match self.approved_epochs.filter_by_epoch(prev_epoch) {
            None => false,
            Some(epoch_messages) => {
                let total_witnesses = witness_selector.epoch_witnesses(prev_epoch);
                let existing_witnesses: HashSet<u64> =
                    epoch_messages.messages_by_owner.keys().cloned().collect();
                //existing_witnesses.insert(owner_uid);
                (total_witnesses & &existing_witnesses).len() > total_witnesses.len() * 2 / 3
            }
        }
    }

    /// Determines whether this message is a representative message.
    /// The message is a representative of epoch X:
    /// a) if this is the first message of the epoch's leader in the epoch X that satisfies either of
    /// the conditions:
    /// a.1) X = 0.
    /// a.2) It approves the representative message of the epoch X-1.
    /// b) or this message has epoch Y, Y>X and the same owner has a kickout that kicks out epoch
    /// X-1 with >2/3 approvals .
    /// Returns:
    ///  Some(epoch) -- the epoch that it is representative of.
    ///  None if it is not a representative.
    fn is_representative<W>(&self, _witness_selector: &W) -> Option<u64>
    where
        W: WitnessSelector,
    {
        // Skip if we already approved representative or kickout for this epoch.
        if self.approved_representatives.contains_epoch(self.computed_epoch)
            || self.approved_kickouts.contains_epoch(self.computed_epoch)
        {
            return None;
        }
        if self.computed_is_epoch_leader {
            if self.computed_epoch == 0 {
                // Scenario (a.1).
                return Some(0);
            } else if self.approved_representatives.contains_epoch(self.computed_epoch - 1) {
                // Scenario (a.2).
                return Some(self.computed_epoch);
            };
        }
        {
            // Scenario (b).
            // Iterate over fully approved kickouts that we are owners of and for which we do not
            // have matching representatives.
            let owner_uid = self.data.body.owner_uid;

            for prev_epoch in (0..self.computed_epoch).rev() {
                if let Some(group) =
                    self.approved_complete_epochs.messages_by_epoch.get(&prev_epoch)
                {
                    if let Some(messages) = group.messages_by_owner.get(&owner_uid) {
                        if let Some(message) = messages.iter().next() {
                            if message.computed_is_kickout
                               // Check that we haven't created representative for this case yet.
                               && !self.approved_representatives.contains_epoch(prev_epoch + 1)
                            {
                                return Some(prev_epoch + 1);
                            }
                        } else {
                            panic!("Should contain at least one element");
                        }
                    }
                }
            }
            None
        }
    }

    /// Determines whether this message is a kickout message.
    /// The message is a kickout message for epoch X-1 if this is the first message of the epoch's
    /// leader in the epoch X that does not approve the representative message of the epoch X-1.
    fn is_kickout<W>(&self, _witness_selector: &W) -> bool
    where
        W: WitnessSelector,
    {
        self.computed_epoch > 0
            && !self.approved_representatives.contains_epoch(self.computed_epoch - 1)
    }

    /// Determines whether this message serves as an endorsement to some representatives.
    /// Message is an endorsement of representative of epoch X if:
    /// * it approves the representative;
    /// * it contains the part of the BLS signature that signs the representative message;
    /// * it does not approve its own promise to kickout of epoch X. (See the note on the precedence).
    /// Note, a representative message is an endorsement to itself. Unless it does not include the
    /// part of the BLS signature which is a misbehavior.
    /// * it does not approve an endorsement by the same owner for the same representative message.
    ///
    /// The precedence of endorsing over promising:
    /// If a message simultaneously approves both a representative for epoch X and a kickout of
    /// epoch X then according to our definitions it is both an endorsement and a promise, which
    /// cannot happen simultaneously since the message approves itself. We therefore break this
    /// tie by making it an endorsement. Implementation-wise, this is resolved by computing
    /// endorsements before promises.
    fn compute_endorsements<W>(&mut self, witness_selector: &W) -> GroupsPerEpoch<'a, P>
    where
        W: WitnessSelector,
    {
        let owner_uid = &self.data.body.owner_uid;

        let mut result = GroupsPerEpoch::new();
        for (epoch, reprs) in &self.approved_representatives.messages_by_epoch {
            // Skip if the message's owner has this message outside its schedule.
            if !witness_selector.epoch_witnesses(*epoch).contains(owner_uid) {
                continue;
            };
            // Check if we gave a promise to a kickout in this epoch.
            if self.approved_promises.contains_any_approval(*epoch, *owner_uid) {
                continue;
            };
            // Check if we endorsed representative with higher epoch.
            if self.approved_endorsements.contains_any_future_approvals(*epoch, *owner_uid) {
                continue;
            };
            for owner_repr in reprs.messages_by_owner.values() {
                for repr in owner_repr {
                    // Check if we already gave an endorsement to exactly the same representative.
                    if !self.approved_endorsements.contains_approval(*epoch, *owner_uid, repr) {
                        result.insert(*epoch, repr);
                    }
                }
            }
        }
        result
    }

    /// Determines whether this message serves as a promise to some kickouts.
    /// Message is a promise to kickout which kickouts representative of epoch X if:
    /// * it approves the kickout;
    /// * it does not approve its own endorsement of a representative of epoch X. (See the note on
    ///   the precendence);
    /// * it does not approve a kickout by the same owner for the same representative message.
    /// Note, a kickout is a promise to itself.
    fn compute_promises<W>(&mut self, witness_selector: &W) -> GroupsPerEpoch<'a, P>
    where
        W: WitnessSelector,
    {
        let owner_uid = &self.data.body.owner_uid;
        // We do not need to subtract endorsements, because we check if we approved representatives.
        let mut result = GroupsPerEpoch::new();
        for (epoch, kickouts) in &self.approved_kickouts.messages_by_epoch {
            // Skip if the message's owner has this message outside its schedule.
            if !witness_selector.epoch_witnesses(*epoch).contains(owner_uid) {
                continue;
            };
            // Ignore kickouts for epochs for which we have representative messages.
            if self.approved_representatives.contains_epoch(*epoch) {
                continue;
            };
            // Check if we endorsed this epoch.
            if self.approved_endorsements.contains_any_approval(*epoch, *owner_uid)
                || self.computed_endorsements.contains_epoch(*epoch)
            {
                continue;
            };
            for owner_kickout in kickouts.messages_by_owner.values() {
                for kickout in owner_kickout {
                    // Check if we already gave a promise to exactly the same kickout.
                    if !self.approved_promises.contains_approval(*epoch, *owner_uid, kickout) {
                        result.insert(*epoch, kickout);
                    }
                }
            }
        }
        result
    }

    /// Given the previous publishable representative finds the next one.
    /// * Examines the approved representative messages of `prev`;
    /// * Takes the one that represents the largest epoch that was not published yet.
    fn find_next_publishable_repr(
        &self,
        prev: &'a Message<'a, P>,
    ) -> Option<(&u64, &'a Message<'a, P>)> {
        // This is quite inefficient, but we are making changes to TxFlow algorithm anyway.
        (&prev.approved_representatives.messages_by_epoch)
            .iter()
            // Filter out representatives that are already publishable.
            .filter_map(|(epoch, repr_group)| {
                if self.approved_publishable_epochs.contains_epoch(*epoch) {
                    None
                } else {
                    let (_, representatives) = repr_group
                        .messages_by_owner
                        .iter()
                        .next()
                        .expect("Expected at least one representative owner.");
                    let repr = representatives
                        .iter()
                        .next()
                        .expect("Expected at least one representative.");
                    Some((epoch, *repr))
                }
            })
            // Find the one with the max epoch.
            .max_by(|a, b| a.0.cmp(&b.0))
    }

    /// Computes epoch, is_representative, is_kickout using parents' information.
    /// If recompute_epoch = false then the epoch is not recomputed but taken from data, it is
    /// useful when the message is retrieved from a block on the beacon chain.
    pub fn init<W>(
        &mut self,
        recompute_epoch: bool,
        recompute_hash: bool,
        starting_epoch: u64,
        witness_selector: &W,
    ) where
        W: WitnessSelector,
    {
        let owner_uid = self.data.body.owner_uid;
        self.aggregate_parents(witness_selector);

        // Compute epoch, if required.
        self.computed_epoch = if recompute_epoch {
            if let Some(prev_epoch) = self.prev_epoch() {
                if self.should_promote(*prev_epoch, witness_selector) {
                    *prev_epoch + 1
                } else {
                    *prev_epoch
                }
            } else {
                starting_epoch
            }
        } else {
            self.data.body.epoch
        };

        // Compute the hash.
        if recompute_hash {
            self.compute_hash();
        } else {
            self.computed_hash = self.data.hash
        };

        // Compute if this is an epoch leader.
        self.computed_is_epoch_leader =
            witness_selector.epoch_leader(self.computed_epoch) == owner_uid;
        self.computed_is_representative = self.is_representative(witness_selector);

        if self.computed_is_epoch_leader
            && self.computed_is_representative.is_none()
            // Check if we have already approved representative or kickout message for the same
            // epoch.
            && !self.approved_representatives.contains_epoch(self.computed_epoch)
            && !self.approved_kickouts.contains_epoch(self.computed_epoch)
        {
            self.computed_is_kickout = self.is_kickout(witness_selector);
        }

        self.computed_endorsements = self.compute_endorsements(witness_selector);
        self.computed_promises = self.compute_promises(witness_selector);

        // Record the epochs that will be complete once we apply the new endorsements and promises.
        // First, record representatives.
        self.computed_complete_epochs = self.approved_endorsements.new_superapproved_messages(
            &self.computed_endorsements,
            self.data.body.owner_uid,
            witness_selector,
        );
        // All representatives that now have enough endorsements are publishable.
        self.computed_publishable_epochs.union_update(&self.computed_complete_epochs);
        let mut more_publishable_epochs = GroupsPerEpoch::new();
        for (z_epoch, z_repr) in &self.computed_publishable_epochs.messages_by_epoch {
            assert_eq!(
                z_repr.messages_by_owner.len(),
                1,
                "Only one sufficiently endorsed message per epoch is expected"
            );
            if !self.approved_publishable_epochs.contains_epoch(*z_epoch) {
                let mut z_repr = *z_repr
                    .messages_by_owner
                    .values()
                    .next()
                    .expect("Expected at least one message")
                    .iter()
                    .next()
                    .expect("Expected at least one message");
                while let Some((next_epoch, next_repr)) = self.find_next_publishable_repr(z_repr) {
                    more_publishable_epochs.insert(*next_epoch, next_repr);
                    z_repr = next_repr;
                }
            }
        }
        self.computed_publishable_epochs.union_update(&more_publishable_epochs);

        // Second, record kickouts.
        self.computed_complete_epochs.union_update(
            &self.approved_promises.new_superapproved_messages(
                &self.computed_promises,
                self.data.body.owner_uid,
                witness_selector,
            ),
        );

        self.is_initialized = true;
    }
}

#[cfg(test)]
#[macro_use]
mod testing_utils;
mod generated_tests;
mod nightly_generated_test;

#[cfg(test)]
mod tests {
    use primitives::consensus::WitnessSelector;
    use primitives::types::UID;
    use std::collections::{HashMap, HashSet};
    use typed_arena::Arena;

    struct FakeWitnessSelector {
        schedule: HashMap<u64, HashSet<UID>>,
    }

    impl FakeWitnessSelector {
        fn new() -> FakeWitnessSelector {
            FakeWitnessSelector {
                schedule: map! {
                0 => set!{0, 1, 2, 3}, 1 => set!{1, 2, 3, 4},
                2 => set!{2, 3, 4, 5}, 3 => set!{3, 4, 5, 6}},
            }
        }
    }

    impl WitnessSelector for FakeWitnessSelector {
        fn epoch_witnesses(&self, epoch: u64) -> &HashSet<UID> {
            self.schedule.get(&epoch).unwrap()
        }
        fn epoch_leader(&self, epoch: u64) -> UID {
            *self.epoch_witnesses(epoch).iter().min().unwrap()
        }
        fn random_witnesses(&self, _epoch: u64, _sample_size: usize) -> HashSet<UID> {
            unimplemented!()
        }
    }

    // TODO: For promo tests below, report incorrectly provided epoch as an adversarial behavior.
    #[test]
    fn epoch_promo() {
        let arena = Arena::new();
        let selector = FakeWitnessSelector::new();
        let root;
        simple_messages!(0, &selector, arena [[0, 0, false; 1, 0, false; 3, 0, false;] => 3, 0, true => root;]);
        // The data.epoch, which is 0, is ignored because it is recomputed.
        assert_eq!(root.computed_epoch, 1);
    }

    #[test]
    fn epoch_no_promo() {
        let arena = Arena::new();
        let selector = FakeWitnessSelector::new();
        let root;
        simple_messages!(0, &selector, arena [[0, 0, false; 1, 0, false; 2, 0, false;] => 3, 0, true => root;]);
        // The data.epoch, which is 0, is ignored because it is recomputed.
        assert_eq!(root.computed_epoch, 0);
    }

    #[test]
    fn epoch_borderline_promo() {
        let arena = Arena::new();
        let selector = FakeWitnessSelector::new();
        let root;
        simple_messages!(0, &selector, arena [[3, 0, false; 1, 0, false; 2, 0, false;] => 3, 0, true => root;]);
        assert_eq!(root.computed_epoch, 1);
    }

    #[test]
    fn epoch_borderline_no_skip_zero() {
        let arena = Arena::new();
        let selector = FakeWitnessSelector::new();
        let root;
        simple_messages!(0, &selector, arena [[4, 0, false; 1, 0, false; 2, 0, false;] => 3, 0, true => root;]);
        assert_eq!(root.computed_epoch, 0);
    }

    #[test]
    fn epoch_nopromo() {
        let arena = Arena::new();
        let selector = FakeWitnessSelector::new();
        let root;
        // Owners 4 and 5 are irrelevant for epoch 0.
        simple_messages!(0, &selector, arena [[4, 0, false; 5, 0, false; 2, 0, false;] => 3, 0, true => root;]);
        assert_eq!(root.computed_epoch, 0);
    }

    #[test]
    fn repr() {
        let arena = Arena::new();
        let selector = FakeWitnessSelector::new();
        let (a, b, c, d);
        simple_messages!(0, &selector, arena [[0, 0, false => a; 1, 0, false => b; 2, 0, false => c;] => 1, 1, true => d;]);
        assert_eq!(a.computed_is_representative, Some(0)); // Scenario (a).
        assert!(b.computed_is_representative.is_none());
        assert!(c.computed_is_representative.is_none());
        assert_eq!(d.computed_is_representative, Some(1)); // Scenario (b).
    }

    #[test]
    fn no_repr_but_kickout() {
        let arena = Arena::new();
        let selector = FakeWitnessSelector::new();
        let root;
        simple_messages!(0, &selector, arena [[1, 0, false; 2, 0, false; 3, 0, false;] => 1, 1, true => root;]);
        assert_eq!(root.computed_epoch, 1); // The message is promoted ...
        assert!(root.computed_is_epoch_leader); // ... and it is an epoch leader ...
        assert!(root.computed_is_representative.is_none()); // ... but the prev repr is missing, so it is not a repr...
        assert!(root.computed_is_kickout); // ... but a kickout.
    }
}
