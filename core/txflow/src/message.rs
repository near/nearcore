use std::cell::RefCell;
use std::rc::{Rc, Weak};
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

use primitives::traits::WitnessSelectorLike;
use primitives::types;
use super::group::GroupsPerEpoch;
use super::group_approvals::GroupApprovalPerEpoch;
use super::hashable_message::HashableMessage;

pub type MessageRef<T> = Rc<RefCell<Message<T>>>;
pub type MessageWeakRef<T> = Weak<RefCell<Message<T>>>;

// TODO: Consider using arena like in https://github.com/SimonSapin/rust-forest once TypedArena becomes stable.
/// Represents the message of the DAG, T is the payload parameter. For in-shard TxFlow and
/// beacon-chain TxFlow T takes different values.
#[derive(Debug)]
pub struct Message<T: Hash> {
    pub data: types::SignedMessageData<T>,

    self_ref: MessageWeakRef<T>,
    parents: Vec<MessageRef<T>>,

    // The following fields are computed based on the approved messages.
    /// The computed epoch of the message. If this message is restored from the epoch block then
    /// the epoch is taken from the data.
    pub computed_epoch: Option<u64>,
    /// The hash of the message. Depends on the epoch.
    pub computed_hash: Option<u64>,
    /// Computed flag of whether this message is representative.
    computed_is_representative: Option<bool>,
    /// Computed flag of whether this message is a kickout.
    computed_is_kickout: Option<bool>,
    /// Computed flag whether this message was created by an epoch leader.
    computed_is_epoch_leader: Option<bool>,

    // The following are the approved messages, grouped by different criteria.
    /// Epoch -> messages that have that epoch.
    approved_epochs: GroupsPerEpoch<T>,
    /// Epoch -> a/all representatives of that epoch (supports forks).
    approved_representatives: GroupsPerEpoch<T>,
    /// Epoch -> a/all kickouts of that epoch (supports forks).
    approved_kickouts: GroupsPerEpoch<T>,
    /// Endorsements of representatives (supports endorsements on forked representatives).
    approved_endorsements: GroupApprovalPerEpoch<T>,
    /// Promises to kickout a representative message (supports promises on forked kickouts).
    approved_promises: GroupApprovalPerEpoch<T>,
    /// Epoch -> Either a representative message that has >2/3 endorsements or a kickout message
    /// that has >2/3 promises. Epoch should always have one element, but we do not assert it yet.
    complete_epochs: GroupsPerEpoch<T>,
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

impl<T: Hash> Message<T> {
    pub fn new(data: types::SignedMessageData<T>) -> MessageRef<T> {
        let result = Rc::new(RefCell::new(
            Message {
                data,
                self_ref: Weak::new(),
                parents: vec![],
                computed_epoch: None,
                computed_hash: None,
                computed_is_representative: None,
                computed_is_kickout: None,
                computed_is_epoch_leader: None,
                approved_epochs: GroupsPerEpoch::new(),
                approved_representatives: GroupsPerEpoch::new(),
                approved_kickouts: GroupsPerEpoch::new(),
                approved_endorsements: GroupApprovalPerEpoch::new(),
                approved_promises: GroupApprovalPerEpoch::new(),
                complete_epochs: GroupsPerEpoch::new(),
            }));
        // Keep weak reference to itself.
        result.borrow_mut().self_ref = Rc::downgrade(&result);
        result
    }

    pub fn link(parent: &MessageRef<T>, child: &MessageRef<T>) {
        child.borrow_mut().parents.push(Rc::clone(&parent));
    }

    pub fn unlink(parent: &MessageRef<T>, child: &MessageRef<T>) {
        child.borrow_mut().parents.retain(|ref x| !Rc::ptr_eq(&x, &parent));
    }

    /// Computes the aggregated data from the parents and updates the message.
    pub fn aggregate_parents(&mut self) {
        for p in &self.parents {
            self.approved_epochs.union_update(&p.borrow().approved_epochs);
            self.approved_representatives.union_update(&p.borrow().approved_representatives);
            self.approved_kickouts.union_update(&p.borrow().approved_kickouts);
            self.approved_endorsements.union_update(&p.borrow().approved_endorsements);
            self.approved_promises.union_update(&p.borrow().approved_promises);
            self.complete_epochs.union_update(&p.borrow().complete_epochs);
        }
    }

    /// Determines the previous epoch of the current owner. Otherwise returns the starting_epoch.
    fn prev_epoch<'a>(&'a self, starting_epoch: &'a u64) -> &'a u64 {
        // Iterate over past messages that were created by the current owner and return their max
        // epoch. If such message not found then return starting_epoch.
        self.approved_epochs.filter_by_owner(self.data.body.owner_uid).map(|(epoch, _)| epoch)
            .max().unwrap_or(starting_epoch)
    }

    /// Determines whether the epoch of the current message should increase.
    fn should_promote<P>(&self, prev_epoch: u64, witness_selector: &P) -> bool
        where P : WitnessSelectorLike {
        match self.approved_epochs.filter_by_epoch(prev_epoch) {
            None => false,
            Some(epoch_messages) => {
                let owner_uid = self.data.body.owner_uid;
                let total_witnesses = witness_selector.epoch_witnesses(prev_epoch);
                let mut existing_witnesses: HashSet<u64> = epoch_messages.messages_by_owner.keys().map(|x|x.clone()).collect();
                existing_witnesses.insert(owner_uid);
                (total_witnesses & &existing_witnesses).len() > total_witnesses.len()*2/3
            }
        }
    }

    /// Determines whether this message is a representative message.
    /// The message is a representative of epoch X if this is the first message of the epoch's leader
    /// in the epoch X that satisfies either of the conditions:
    /// a) X = 0.
    /// b) It approves the representative message of the epoch X-1.
    /// c) It approves the kickout message for the epoch X-1 and it approves >2/3 promises for that
    ///    kickout message.
    fn is_representative<P: WitnessSelectorLike>(&self, is_leader: bool, _witness_selector: &P) -> bool {
        let epoch = self.computed_epoch.expect("Epoch should be computed by now");
        if !is_leader {
            // If it is not a leader then don't bother.
            false
        } else if epoch == 0 {
            // Scenario (a).
            true
        } else if self.approved_representatives.contains_epoch(epoch-1) {
            // Scenario (b).
            true
        } else {
            // Scenario (c).
            self.complete_epochs.contains_epoch(epoch-1)
        }
    }

    /// Determines whether this message is a kickout message.
    /// The message is a kickout message for epoch X-1 if this is the first message of the epoch's
    /// leader in the epoch X that does not approve the representative message of the epoch X-1.
    fn is_kickout<P: WitnessSelectorLike>(&self, is_leader:bool, _witness_selector: &P) -> bool {
        let epoch = self.computed_epoch.expect("Epoch should be computed by now");
        if !is_leader {
            false
        } else if epoch > 0 && !self.approved_representatives.contains_epoch(epoch-1) {
            true } else {
            false }
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
    fn compute_endorsements<P>(&mut self, witness_selector: &P) -> GroupApprovalPerEpoch<T>
        where P : WitnessSelectorLike {
        let owner_uid = &self.data.body.owner_uid;
        let hash = self.computed_hash.expect("Hash should be computed before.");

        let mut result = GroupApprovalPerEpoch::<T>::new();
        for (epoch, reprs) in &self.approved_representatives.messages_by_epoch {
            // Skip if the message's owner has this message outside its schedule.
            if !witness_selector.epoch_witnesses(*epoch).contains(owner_uid) {continue};
            // Check if we gave a promise to a kickout in this epoch.
            if self.approved_promises.contains_any_approval(epoch, owner_uid) {continue};
            for (_repr_owner_uid, owner_repr) in &reprs.messages_by_owner {
                for repr in owner_repr {
                    // Check if we already gave an endorsement to exactly the same representative.
                    if !self.approved_endorsements.contains_approval(epoch, owner_uid, repr) {
                       result.insert(*epoch, repr, *owner_uid,
                                     &HashableMessage {hash, message: self.self_ref.clone()});
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
    fn compute_promises<P>(&mut self, witness_selector: &P) -> GroupApprovalPerEpoch<T>
        where P : WitnessSelectorLike {
        let owner_uid = &self.data.body.owner_uid;
        let hash = self.computed_hash.expect("Hash should be computed before.");
        // We do not need to subtract endorsements, because we check if we approved representatives.
        let mut result = GroupApprovalPerEpoch::<T>::new();
        for (epoch, kickouts) in &self.approved_kickouts.messages_by_epoch {
            // Skip if the message's owner has this message outside its schedule.
            if !witness_selector.epoch_witnesses(*epoch).contains(owner_uid) {continue};
            // Ignore kickouts for epochs for which we have representative messages.
            if self.approved_representatives.contains_epoch(*epoch) {continue};
            // Check if we endorsed this epoch.
            if self.approved_endorsements.contains_any_approval(epoch, owner_uid) {continue};
            for (_kickout_owner_uid, owner_kickout) in &kickouts.messages_by_owner {
                for kickout in owner_kickout {
                    // Check if we already gave a promise to exactly the same kickout.
                    if !self.approved_promises.contains_approval(epoch, owner_uid, kickout) {
                        result.insert(*epoch, kickout, *owner_uid,
                        &HashableMessage {hash, message: self.self_ref.clone()})
                    }
                }
            }
        }
        result
    }

    /// Computes epoch, is_representative, is_kickout using parents' information.
    /// If recompute_epoch = false then the epoch is not recomputed but taken from data.
    pub fn populate_from_parents<P>(&mut self,
                                    recompute_epoch: bool,
                                    starting_epoch: u64,
                                    witness_selector: &P) where P : WitnessSelectorLike {
        let owner_uid = self.data.body.owner_uid;
        self.aggregate_parents();

        // Compute epoch, if required.
        let epoch = if recompute_epoch {
            let prev_epoch = self.prev_epoch(&starting_epoch);
            if self.should_promote(*prev_epoch, witness_selector) {
                *prev_epoch + 1 } else  {
                *prev_epoch }

        } else {
            self.data.body.epoch
        };
        self.computed_epoch = Some(epoch);

        // Compute the hash.
        let hash = {
            let mut hasher = DefaultHasher::new();
            self.data.body.hash(&mut hasher);
            hasher.finish()
        };
        self.computed_hash = Some(hash);
        let hashable_self_ref = HashableMessage{hash, message: self.self_ref.clone()};

        // Update aggregator per epoch.
        self.approved_epochs.insert(epoch, owner_uid, &hashable_self_ref);

        // Compute if this is an epoch leader.
        let is_leader = witness_selector.epoch_leader(epoch) == owner_uid;
        self.computed_is_epoch_leader = Some(is_leader);

        // Compute if it is a representative.
        match self.is_representative(is_leader, witness_selector) {
            true => {
                self.computed_is_representative = Some(true);
                self.approved_representatives.insert(epoch, owner_uid, &hashable_self_ref);
            },
            false => {self.computed_is_representative = Some(false); }
        }

        // Compute if it is a kick-out message.
        match self.is_kickout(is_leader, witness_selector) {
            // TODO: Skip this check if is_representative returned true.
            true => {
                self.computed_is_kickout = Some(true);
                self.approved_kickouts.insert(epoch-1, owner_uid, &hashable_self_ref);
            },
            false => {self.computed_is_kickout = Some(false);}
        }

        // Compute endorsements for representative messages.
        let endorsements = self.compute_endorsements(witness_selector);
        self.approved_endorsements.union_update(&endorsements);
        self.complete_epochs.union_update(&self.approved_endorsements.superapproved_messages(witness_selector));

        // Compute promises to kickouts.
        let promises = self.compute_promises(witness_selector);
        self.approved_promises.union_update(&promises);
        self.complete_epochs.union_update(&self.approved_promises.superapproved_messages(witness_selector));
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn two_nodes() {
        tuplet!((a,b) = test_messages!(1=>10, 1=>10));
        link_messages!(&a => &b);
    }

    #[test]
    fn three_nodes() {
        tuplet!((a,b,c) = test_messages!(1=>10, 1=>10, 1=>10));
        link_messages!(&a => &b, &b => &c, &a => &c);
    }

    #[test]
    fn unlink() {
        tuplet!((a,b) = test_messages!(1=>10, 1=>10));
        link_messages!(&a => &b);
        Message::unlink(&a, &b);
        assert!(b.borrow().parents.is_empty());
    }

    struct FakeWitnessSelector {
       schedule: HashMap<u64, HashSet<u64>>,
    }
    impl FakeWitnessSelector {
       fn new() -> FakeWitnessSelector {
          FakeWitnessSelector {
              schedule: map!{
              0 => set!{0, 1, 2, 3}, 1 => set!{1, 2, 3, 4},
              8 => set!{3, 0, 1, 2}, 9 => set!{0, 1, 2, 3}, 10 => set!{1, 2, 3, 4}}
          }
       }
    }

    impl WitnessSelectorLike for FakeWitnessSelector {
        fn epoch_witnesses(&self, epoch: u64) -> &HashSet<u64> {
            self.schedule.get(&epoch).unwrap()
        }
        fn epoch_leader(&self, epoch: u64) -> u64 {
            *self.epoch_witnesses(epoch).iter().min().unwrap()
        }
    }

    #[test]
    fn test_epoch_computation_no_promo() {
        let selector = FakeWitnessSelector::new();
        // Corner case of having messages for epoch 10 without messages for epoch 9.
        tuplet!((a,b,c,d,e) = test_messages!(9=>0, 10=>1, 10=>2, 10=>3, 9=>0));
        link_messages!(&a=>&e, &b=>&e, &c=>&e, &d=>&e);
        populate_messages!(9, selector, &a=>false, &b=>false, &c=>false, &d=>false, &e=>true);

        assert_eq!(e.borrow().computed_epoch.unwrap(), 9);
    }

    #[test]
    fn test_epoch_computation_promo() {
        let selector = FakeWitnessSelector::new();
        tuplet!((a,b,c,d,e) = test_messages!(9=>0, 9=>1, 9=>2, 10=>3, 9=>0));
        link_messages!(&a=>&e, &b=>&e, &c=>&e, &d=>&e);
        populate_messages!(9, selector, &a=>false, &b=>false, &c=>false, &d=>false, &e=>true);

        assert_eq!(e.borrow().computed_epoch.unwrap(), 10);
    }

    #[test]
    fn test_representatives_scenarios_a_b() {
        let selector = FakeWitnessSelector::new();
        tuplet!((a,b,c,d,e) = test_messages!(0=>0, 0=>1, 0=>2, 1=>3, 0=>1));
        link_messages!(&a=>&e, &b=>&e, &c=>&e, &d=>&e);
        populate_messages!(0, selector, &a=>false, &b=>false, &c=>false, &d=>false, &e=>true);

        assert!(a.borrow().computed_is_representative.unwrap());
        assert!(!b.borrow().computed_is_representative.unwrap());
        assert!(!c.borrow().computed_is_representative.unwrap());
        assert!(!d.borrow().computed_is_representative.unwrap());
        assert_eq!(e.borrow().computed_epoch.unwrap(), 1);
        assert!(e.borrow().computed_is_representative.unwrap());
    }

    #[test]
    fn test_no_kickout() {
        let selector = FakeWitnessSelector::new();
        tuplet!((a,b,c,d,e) = test_messages!(0=>0, 0=>1, 0=>2, 1=>3, 0=>1));
        link_messages!(&a=>&e, &b=>&e, &c=>&e, &d=>&e);
        populate_messages!(0, selector, &a=>false, &b=>false, &c=>false, &d=>false, &e=>true);

        assert!(!a.borrow().computed_is_kickout.unwrap());
        assert!(!b.borrow().computed_is_kickout.unwrap());
        assert!(!c.borrow().computed_is_kickout.unwrap());
        assert!(!d.borrow().computed_is_kickout.unwrap());
        assert!(!e.borrow().computed_is_kickout.unwrap());
    }

    #[test]
    fn test_kickout() {
        let selector = FakeWitnessSelector::new();
        tuplet!((a,b,c,d,e) = test_messages!(9=>0, 9=>1, 9=>2, 10=>3, 9=>1));
        link_messages!(&a=>&e, &b=>&e, &c=>&e, &d=>&e);
        populate_messages!(9, selector, &a=>false, &b=>false, &c=>false, &d=>false, &e=>true);

        assert!(!a.borrow().computed_is_representative.unwrap());
        assert!(e.borrow().computed_is_kickout.unwrap());
    }

    #[test]
    fn test_promise() {
        let selector = FakeWitnessSelector::new();
        tuplet!((a,b,c,d,e,f) = test_messages!(9=>0, 9=>1, 9=>2, 10=>3, 9=>1, 10=>2));
        link_messages!(&a=>&e, &b=>&e, &c=>&e, &d=>&e, &e=>&f);
        populate_messages!(9, selector, &a=>false, &b=>false, &c=>false, &d=>false, &e=>true, &f=>true);

        assert!(!a.borrow().computed_is_representative.unwrap());
        assert!(e.borrow().computed_is_kickout.unwrap());
        assert!(f.borrow().approved_promises.contains_epoch(&9));
    }

    #[test]
    fn test_endorsement() {
        let selector = FakeWitnessSelector::new();
        tuplet!((a,b,c,d,e,f) = test_messages!(0=>0, 0=>1, 0=>2, 1=>3, 0=>1, 1=>2));
        link_messages!(&a=>&e, &b=>&e, &c=>&e, &d=>&e, &e=>&f);
        populate_messages!(0, selector, &a=>false, &b=>false, &c=>false, &d=>false, &e=>true, &f=>true);

        assert!(a.borrow().computed_is_representative.unwrap());
        assert!(e.borrow().computed_is_representative.unwrap());
        assert!(f.borrow().approved_endorsements.contains_any_approval(&1, &2));
    }
}
