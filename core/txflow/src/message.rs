use std::cell::RefCell;
use std::rc::Rc;
use std::collections::HashMap;

use super::types;

pub type MessageRef = Rc<RefCell<Message>>;
/// Epoch of the message -> (node uid -> message). A subset of messages approved by the current
/// message, possibly including the message itself.
pub type AggregatedMessages = HashMap<u64, HashMap<u64, MessageRef>>;

// TODO: Consider using arena like in https://github.com/SimonSapin/rust-forest once TypedArena becomes stable.
/// Represents the message of the DAG.
#[derive(Debug)]
pub struct Message {
    pub data: types::SignedMessageData,
    pub parents: Vec<MessageRef>,
    pub children: Vec<MessageRef>,
    /// Whether the current message is the representative message of a leader.
    pub is_representative: bool,

    pub approved_epochs: AggregatedMessages,
    pub approved_representatives: AggregatedMessages,
    pub approved_kickouts: AggregatedMessages,
    pub approved_endorsements: AggregatedMessages,
}

/// Aggregates messages for the given list of fields.
macro_rules! collect_aggregates {
    ( $self:ident, $( $field_name:ident ),* ) => {
        for p in &$self.parents {
            $(
                for (epoch, per_epoch) in &p.borrow().$field_name {
                   let a = $self.$field_name.entry(*epoch)
                       .or_insert_with(|| HashMap::new());
                    a.extend(per_epoch.into_iter().map(|(k, v)| (k.clone(), v.clone())));
                }
            )*
        }
    };
}

impl Message {
    pub fn new(data: types::SignedMessageData) -> MessageRef {
        Rc::new(RefCell::new(
            Message {data,
                parents: vec![],
                children: vec![],
                is_representative: false,
                approved_epochs: HashMap::new(),
                approved_representatives: HashMap::new(),
                approved_kickouts: HashMap::new(),
                approved_endorsements: HashMap::new(),
            }
        ))
    }

    pub fn link(parent: &MessageRef, child: &MessageRef) {
        parent.borrow_mut().children.push(Rc::clone(&child));
        child.borrow_mut().parents.push(Rc::clone(&parent));
    }

    pub fn unlink(parent: &MessageRef, child: &MessageRef) {
        parent.borrow_mut().children.retain(|ref x| !Rc::ptr_eq(&x, &child));
        child.borrow_mut().parents.retain(|ref x| !Rc::ptr_eq(&x, &parent));
    }

    /// Computes the aggregated data from the parents and updates the message.
    pub fn aggregate_parents(&mut self) {
        collect_aggregates!(self, approved_epochs, approved_representatives, approved_kickouts, approved_endorsements);
    }
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
        assert!(a.borrow().children.is_empty());
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
        p1.borrow_mut().approved_epochs.insert(10, map!{0 => pp1.clone()});
        p2.borrow_mut().approved_epochs.insert(11, map!{1 => pp2.clone()});
        c.borrow_mut().aggregate_parents();
        let expected: HashMap<u64, HashMap<u64, MessageRef>> = map!{10 => map!{0 => pp1.clone()}, 11 => map!{1 => pp2.clone()} };
        let actual= &c.borrow().approved_epochs;
        assert_eq!(actual.len(), expected.len());
        for (epoch, epoch_content) in &expected {
            assert!(actual.contains_key(epoch));
            for (uid, message) in epoch_content {
               assert!(Rc::ptr_eq(message, actual.get(&epoch).unwrap().get(uid).unwrap()));
            }
        }
    }
}
