use std::cell::RefCell;
use std::rc::{Rc, Weak};
use std::collections::HashMap;

use super::types;
use primitives::traits::WitnessSelector;

pub type MessageRef = Rc<RefCell<Message>>;
pub type MessageWeakRef = Weak<RefCell<Message>>;

/// Epoch -> message.
pub type EpochMap = HashMap<u64, MessageWeakRef>;

/// Epoch -> (node uid -> message).
pub type EpochMapMap = HashMap<u64, HashMap<u64, MessageWeakRef>>;


// TODO: Consider using arena like in https://github.com/SimonSapin/rust-forest once TypedArena becomes stable.
/// Represents the message of the DAG.
#[derive(Debug)]
pub struct Message {
    pub data: types::SignedMessageData,

    self_ref: MessageWeakRef,
    parents: Vec<MessageRef>,

    // The following fields are computed based on the approved messages.
    /// The computed epoch of the message. If this message is restored from the epoch block then
    /// the epoch is taken from the data.
    computed_epoch: Option<u64>,
    /// Computed flag of whether this message is representative.
    computed_is_representative: Option<bool>,
    /// Computed flag of whether this message is a kickout.
    computed_is_kickout: Option<bool>,

    // The following are the approved messages, grouped by different criteria.
    /// Epoch -> messages that have that epoch, grouped as:
    /// owner_uid who created the message -> the message.
    approved_epochs: EpochMapMap,
    /// Epoch -> a/any representative of that epoch (if there are several forked representative
    /// messages than any of them).
    approved_representatives: EpochMap,
    /// Epoch -> a/any kickout message of a representative with epoch Epoch (if there are several
    /// forked kickout messages then any of them).
    approved_kickouts: EpochMap,
    /// Epoch -> endorsements that endorse a/any (if there are several forked representative messages
    /// then any of them) representative message of that epoch, grouped as:
    /// owner_uid who created endorsement -> the endorsement message.
    approved_endorsements: EpochMapMap,
    /// Epoch -> promises that approve a/any (if there are several forked kickout messages then any
    /// of them) kickout message (it by the def. has epoch Epoch+1) that kicks out representative
    /// message of epoch Epoch, grouped as:
    /// owner_uid who created the promise -> the promise message.
    approved_promises: EpochMapMap,

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

/// Aggregate EpochMapMaps from parents.
macro_rules! aggregate_mapmaps {
    ( $self:ident, $( $field_name:ident ),+ ) => {
        for p in &$self.parents {
            $(
                for (epoch, per_epoch) in &p.borrow().$field_name {
                   let a = $self.$field_name.entry(*epoch)
                       .or_insert_with(|| HashMap::new());
                    a.extend(per_epoch.into_iter().map(|(k, v)| (k.clone(), v.clone())));
                }
            )+
        }
    };
}


impl Message {
    pub fn new(data: types::SignedMessageData) -> MessageRef {
        let result = Rc::new(RefCell::new(
            Message {
                self_ref: Weak::new(),
                data,
                parents: vec![],
                computed_epoch: None,
                computed_is_representative: None,
                computed_is_kickout: None,
                approved_epochs: HashMap::new(),
                approved_representatives: HashMap::new(),
                approved_kickouts: HashMap::new(),
                approved_endorsements: HashMap::new(),
                approved_promises: HashMap::new(),
            }));
        // Keep weak reference to itself.
        result.borrow_mut().self_ref = Rc::downgrade(&result);
        result
    }

    pub fn link(parent: &MessageRef, child: &MessageRef) {
        child.borrow_mut().parents.push(Rc::clone(&parent));
    }

    pub fn unlink(parent: &MessageRef, child: &MessageRef) {
        child.borrow_mut().parents.retain(|ref x| !Rc::ptr_eq(&x, &parent));
    }

    /// Computes the aggregated data from the parents and updates the message.
    pub fn aggregate_parents(&mut self) {
        aggregate_mapmaps!(self, approved_epochs, approved_endorsements, approved_promises);
    }

    /// Determines the previous epoch of the current owner. Otherwise returns the starting_epoch.
    fn prev_epoch(&self, starting_epoch: u64) -> u64 {
        // Iterate over past messages that were created by the current owner and return their max
        // epoch. If such message not found then return starting_epoch.
        self.parents.iter().filter_map(|parent| {
            parent.borrow().approved_epochs.iter()
                .filter(|(_, epoch_messages)|
                    epoch_messages.contains_key(&self.data.body.owner_uid)
                ).map(|(epoch, _)| *epoch).max()
        }).max().unwrap_or(starting_epoch)
    }

    /// Computes epoch, is_representative, is_kickout using parents' information.
    /// If recompute_epoch = false then the epoch is not recomputed but taken from data.
    pub fn populate_from_parents<T>(&mut self,
                                    recompute_epoch: bool,
                                    starting_epoch: u64,
                                    _witness_selector: &T) where T : WitnessSelector {
        self.aggregate_parents();
        let owner_uid = self.data.body.owner_uid;

        // Compute epoch, if required.
        //let epoch = if recompute_epoch {
        //    self.compute_epoch(starting_epoch) } else {
        //    self.data.body.epoch };
        //let self_ref = self.self_ref.clone();
        //self.approved_epochs.entry(epoch).or_insert_with(|| map!{owner_uid => self_ref});
        //self.computed_epoch = Some(epoch);

        // Compute
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    fn simple_message() -> MessageRef {
        Message::new(types::SignedMessageData {
            owner_sig: 0,
            hash: 0,
            body: types::MessageDataBody {
                owner_uid: 0,
                parents: vec![],
                epoch: 0,
                transactions: vec![],
                epoch_block_header: None,
                endorsement: None,
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

    #[test]
    fn test_aggregates() {
        let pp1 = simple_message();
        let pp2 = simple_message();
        let p1 = simple_message();
        let p2 = simple_message();
        let c = simple_message();
        Message::link(&pp1,&p1);
        Message::link(&pp2,&p2);
        Message::link(&p1,&c);
        Message::link(&p2,&c);
        p1.borrow_mut().approved_epochs.insert(10, map!{0 => Rc::downgrade(&pp1)});
        p2.borrow_mut().approved_epochs.insert(11, map!{1 => Rc::downgrade(&pp2)});
        c.borrow_mut().aggregate_parents();
        let expected: HashMap<u64, HashMap<u64, MessageWeakRef>> = map!{
        10 => map!{0 => Rc::downgrade(&pp1)},
        11 => map!{1 => Rc::downgrade(&pp2)} };
        let actual= &c.borrow().approved_epochs;
        assert_eq!(actual.len(), expected.len());
        for (epoch, epoch_content) in &expected {
            assert!(actual.contains_key(epoch));
            for (uid, message) in epoch_content {
               assert!(Rc::ptr_eq(&message.upgrade().unwrap(),
                                  &actual.get(&epoch).unwrap().get(uid).unwrap().upgrade().unwrap()));
            }
        }
    }
}
