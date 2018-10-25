use std::cell::RefCell;
use std::rc::Rc;

use super::types;

type MessageRef = Rc<RefCell<Message>>;

// TODO: Consider using arena like in https://github.com/SimonSapin/rust-forest once TypedArena becomes stable.
/// Represents the message of the DAG.
pub struct Message {
    pub data: types::SignedMessageData,
    pub parents: Vec<MessageRef>,
    pub children: Vec<MessageRef>,
}

impl Message {
    pub fn new(data: types::SignedMessageData) -> Message {
       Message {data, parents: vec![], children: vec![]}
    }

    pub fn link(parent: &MessageRef, child: &MessageRef) {
        parent.borrow_mut().children.push(Rc::clone(&child));
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    fn simple_signed_message_data() -> types::SignedMessageData {
       types::SignedMessageData {
           owner_sig: 0,
           hash: 0,
           body: types::MessageDataBody {
               owner_uid: 0,
               parents: vec![],
               epoch: 0,
               is_representative: false,
               is_endorsement: false,
               transactions: vec![],
               epoch_block_header: None
           }
       }
    }

    fn simple_message() -> MessageRef {
        Rc::new(RefCell::new(
            Message::new(simple_signed_message_data())
        ))
    }

   #[test]
   fn one_node() {
       Message::new(
           simple_signed_message_data()
       );
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
}
