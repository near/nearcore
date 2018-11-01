use std::cell::RefCell;
use std::rc::{Rc, Weak};
use std::collections::{HashMap, HashSet};
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
    // * A message cannot be both a representative message of epoch X and a kickout message of epoch
    //   X-1, because the former takes the precedence.
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
            // TODO: Implement this scenario.
            false
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

    fn compute_promises(&mut self) -> GroupApprovalPerEpoch<T> {
        let owner_uid = self.data.body.owner_uid;
        let hash = self.computed_hash.expect("Hash should be computed before.");
        // Ignore kickouts for which we approve representative message.
        let kickouts = (&self.approved_kickouts).difference_by_epoch(&self.approved_representatives);

        GroupApprovalPerEpoch::approve_groups(
            &kickouts, owner_uid, &HashableMessage { hash, message: self.self_ref.clone() })
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

        // Compute promises to kickouts.
        let promises = self.compute_promises();
        self.approved_promises.union_update(&promises);
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Hash)]
    pub struct FakePayload {

    }

    fn simple_message() -> MessageRef<FakePayload> {
        Message::new(types::SignedMessageData {
            owner_sig: 0,
            hash: 0,
            body: types::MessageDataBody {
                owner_uid: 0,
                parents: vec![],
                epoch: 0,
                payload: FakePayload {},
                endorsements: vec![],
            },
        })
    }

    #[test]
    fn two_nodes() {
        let a = simple_message();
        let b = simple_message();
        Message::link(&a, &b);
    }

    #[test]
    fn three_nodes() {
        let a = simple_message();
        let b = simple_message();
        let c = simple_message();
        Message::link(&a,&b);
        Message::link(&b,&c);
        Message::link(&a,&c);
    }

    #[test]
    fn unlink() {
        let a = simple_message();
        let b = simple_message();
        Message::link(&a, &b);
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
              9 => set!{0, 1, 2, 3}, 10 => set!{1, 2, 3, 4}}
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

    type FakeMessageRef = MessageRef<FakePayload>;

    fn simple_graph(epoch_a: u64, owner_a: u64,
                    epoch_b: u64, owner_b: u64,
                    epoch_c: u64, owner_c: u64,
                    epoch_d: u64, owner_d: u64,
                    starting_epoch: u64, owner_e: u64
    ) -> (FakeMessageRef, FakeMessageRef, FakeMessageRef, FakeMessageRef, FakeMessageRef) {
        let selector = FakeWitnessSelector::new();
        let a = simple_message();
        let b = simple_message();
        let c = simple_message();
        let d = simple_message();
        let e = simple_message();
        {
            let a_data = &mut a.borrow_mut().data;
            a_data.body.epoch = epoch_a;
            a_data.body.owner_uid = owner_a;
            let b_data = &mut b.borrow_mut().data;
            b_data.body.epoch = epoch_b;
            b_data.body.owner_uid = owner_b;
            let c_data = &mut c.borrow_mut().data;
            c_data.body.epoch = epoch_c;
            c_data.body.owner_uid = owner_c;
            let d_data = &mut d.borrow_mut().data;
            d_data.body.epoch = epoch_d;
            d_data.body.owner_uid = owner_d;
            let e_data = &mut e.borrow_mut().data;
            e_data.body.owner_uid = owner_e;
        }
        let starting_epoch = starting_epoch;
        for m in vec![&a, &b, &c, &d] {
            m.borrow_mut().populate_from_parents(false, starting_epoch, &selector);
            Message::link(m, &e);
        }

        e.borrow_mut().populate_from_parents(true, starting_epoch,&selector);
        return (a, b, c, d, e)
    }

    #[test]
    fn test_epoch_computation_no_promo() {
        let starting_epoch = 9;
        // Corner case of having messages for epoch 10 without messages for epoch 9.
        let (_a, _b, _c, _d, e)
        = simple_graph(9, 0, 10, 1, 10, 2, 10, 3, starting_epoch, 0);

        assert_eq!(e.borrow().computed_epoch.unwrap(), 9);
    }

    #[test]
    fn test_epoch_computation_promo() {
        let starting_epoch = 9;
        let (_a, _b, _c, _d, e)
        = simple_graph(9, 0, 9, 1, 9, 2, 10, 3, starting_epoch, 0);

        assert_eq!(e.borrow().computed_epoch.unwrap(), 10);
    }

    #[test]
    fn test_representatives_scenarios_a_b() {
        let starting_epoch = 0;
        let (a, b, c, d, e)
        = simple_graph(0, 0,
                       0, 1,
                       0, 2,
                       1, 3,
                       starting_epoch, 1);

        assert!(a.borrow().computed_is_representative.unwrap());
        assert!(!b.borrow().computed_is_representative.unwrap());
        assert!(!c.borrow().computed_is_representative.unwrap());
        assert!(!d.borrow().computed_is_representative.unwrap());
        assert_eq!(e.borrow().computed_epoch.unwrap(), 1);
        assert!(e.borrow().computed_is_representative.unwrap());
    }

    #[test]
    fn test_no_kickout() {
        let starting_epoch = 0;
        let (a, b, c, d, e)
        = simple_graph(0, 0,
                       0, 1,
                       0, 2,
                       1, 3,
                       starting_epoch, 1);

        assert!(!a.borrow().computed_is_kickout.unwrap());
        assert!(!b.borrow().computed_is_kickout.unwrap());
        assert!(!c.borrow().computed_is_kickout.unwrap());
        assert!(!d.borrow().computed_is_kickout.unwrap());
        assert!(!e.borrow().computed_is_kickout.unwrap());
    }

    #[test]
    fn test_kickout() {
        let starting_epoch = 9;
        let (a, _b, _c, _d, e)
        = simple_graph(9, 0, 9, 1, 9, 2, 10, 3, starting_epoch, 1);

        assert!(!a.borrow().computed_is_representative.unwrap());
        assert!(e.borrow().computed_is_kickout.unwrap());
    }

    #[test]
    fn test_promise() {
        let starting_epoch = 9;
        let (a, _b, _c, _d, e)
        = simple_graph(9, 0, 9, 1, 9, 2, 10, 3, starting_epoch, 1);
        let f = simple_message();
        let selector = FakeWitnessSelector::new();
        {
            let f_data = &mut f.borrow_mut().data;
            f_data.body.epoch = 10;
            f_data.body.owner_uid = 2;
        }
        Message::link(&e, &f);
        f.borrow_mut().populate_from_parents(true, starting_epoch, &selector);

        assert!(!a.borrow().computed_is_representative.unwrap());
        assert!(e.borrow().computed_is_kickout.unwrap());
        assert!(f.borrow().approved_promises.contains_epoch(&9));
    }
}
