use std::cell::RefCell;
use std::rc::{Rc, Weak};
use std::collections::HashMap;

use super::types;
//use primitives::traits::WitnessSelector;

pub type MessageRef = Rc<RefCell<Message>>;
pub type MessageWeakRef = Weak<RefCell<Message>>;

/// Epoch of the message -> (node uid -> message). A subset of messages approved by the current
/// message, possibly including the message itself.
pub type AggregatedMessages = HashMap<u64, HashMap<u64, MessageWeakRef>>;


// TODO: Consider using arena like in https://github.com/SimonSapin/rust-forest once TypedArena becomes stable.
/// Represents the message of the DAG.
#[derive(Debug)]
pub struct Message {
    pub data: types::SignedMessageData,

    self_ref: Option<MessageWeakRef>,
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
    approved_epochs: AggregatedMessages,
    approved_representatives: AggregatedMessages,
    approved_kickouts: AggregatedMessages,
    approved_endorsements: AggregatedMessages,
}

/// Aggregates messages for the given list of fields.
macro_rules! collect_aggregates {
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
                self_ref: None,
                data,
                parents: vec![],
                computed_epoch: None,
                computed_is_representative: None,
                computed_is_kickout: None,
                approved_epochs: HashMap::new(),
                approved_representatives: HashMap::new(),
                approved_kickouts: HashMap::new(),
                approved_endorsements: HashMap::new(),
            }));
        // Keep weak reference to itself.
        result.borrow_mut().self_ref = Some(Rc::downgrade(&result));
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
        collect_aggregates!(self, approved_epochs, approved_representatives, approved_kickouts, approved_endorsements);
    }

    // fn compute_epoch(&self, starting_epoch: u64) -> u64{
    // }

    // /// Computes epoch, is_representative, is_kickout using parents' information.
    // /// If recompute_epoch = false then the epoch is not recomputed but taken from data.
    // pub fn compute_counters(&mut self, recompute_epoch: bool, starting_epoch: u64) {
    //     self.aggregate_parents();
    //     let owner_uid = self.data.body.owner_uid;

    //     // First, compute epoch.
    //     let epoch = if recompute_epoch {
    //         self.computed_epoch(starting_epoch) } else {
    //         self.data.body.epoch };
    //     self.approved_epochs.entry(epoch).or_insert_with(|| map!{owner_uid => });
    // }
}


#[cfg(test)]
mod tests {
    use super::*;

    /// Handy utility to create maps.
    macro_rules! map(
        { $($key:expr => $value:expr),+ } => {
            {
                let mut m = ::std::collections::HashMap::new();
                $(
                    m.insert($key, $value);
                )+
                m
            }
        };
    );

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
