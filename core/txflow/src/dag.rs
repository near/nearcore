use primitives::types::*;
use primitives::traits::{WitnessSelector};

use std::rc::Rc;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hasher, Hash};

use super::message::{MessageRef, Message};

/// The data-structure of the TxFlow DAG that supports adding messages and updating counters/flags,
/// but does not support communication-related logic. Also does verification of the messages
/// received from other nodes.
pub struct DAG<'a, T, W: 'a+WitnessSelector> {
    /// UID of the node.
    owner_uid: u64,
    /// Message hash -> Message. Stores all messages known that the current root.
    messages: HashMap<u64, MessageRef<T>>,
    /// Message hash -> Message. Stores all current roots.
    roots: HashMap<u64, MessageRef<T>>,
    /// Epoch -> a/all representative message for that epoch.
    // representatives: HashMap<u64, HashSet<MessageRef<T>>>,
    /// Epoch -> a/all kickout messages for that epoch.
    // kickouts: HashMap<u64, HashSet<MessageRef<T>>>,

    witness_selector: &'a W,
    starting_epoch: u64,
}

impl<'a, T: Hash, W:'a+WitnessSelector> DAG<'a, T, W> {
    pub fn new(owner_uid: u64, starting_epoch: u64, witness_selector: &'a W) -> Result<DAG<T, W>, &'static str> {
        Ok(DAG{
            owner_uid,
            messages: HashMap::new(),
            roots: HashMap::new(),
            witness_selector,
            starting_epoch,
        })
    }

    /// Compute hash of the MessageDataBody.
    fn hash_message_data_body(body: &MessageDataBody<T>) -> u64 {
        let mut hasher = DefaultHasher::new();
        body.hash(&mut hasher);
        hasher.finish()
    }

    /// Verify that this message does not violate the protocol.
    fn verify_message(&self, _message: &MessageRef<T>) -> Result<(), &'static str> {
        Ok({})
    }

    fn unlink_parents(parents: &Vec<MessageRef<T>>, child: &MessageRef<T>) {
        for p in parents {
            Message::unlink(p, child);
        };
    }

    // Takes ownership of the message.
    pub fn add_existing_message(&mut self, message_data: SignedMessageData<T>) -> Result<(), &'static str> {
        // Check whether this is a new message.
        if self.messages.contains_key(&message_data.hash) {
            return Ok({})
        }

        // Wrap message data and connect to the parents so that the verification can be run.
        let message = Message::new(message_data);
        let mut linked_parents: Vec<MessageRef<T>> = vec![];
        for p_hash in &message.borrow().data.body.parents {
            match self.messages.get(p_hash) {
                Some(p) => {
                    linked_parents.push(Rc::clone(p));
                    Message::link(p, &message);
                },
                None => {
                    DAG::<'a,T,W>::unlink_parents(&linked_parents, &message);
                    return Err("Some parents of the message are unknown")
                }
            }
        }

        // Compute approved epochs and endorsements.
        message.borrow_mut().populate_from_parents(true,
                                                   self.starting_epoch, self.witness_selector);

        // Verify the message.
        if let Err(e) = self.verify_message(&message) {
            DAG::<'a,T,W>::unlink_parents(&linked_parents, &message);
            return Err(e)
        }

        // Finally, remember the message and update the roots.
        let hash = message.borrow().data.hash;
        let moved_message = self.messages.insert(hash,message).unwrap();
        for p in &linked_parents {
            self.roots.remove(&p.borrow().data.hash);
        }
        self.roots.insert(hash, moved_message);
        Ok({})
    }

    /// Creates a new message that points to all existing roots. Takes ownership of the payload and
    /// the endorsements.
    pub fn create_root_message(&mut self, payload: T, endorsements: Vec<Endorsement>) {
        let message = Message::new(
            SignedMessageData {
                owner_sig: 0,  // Will populate once the epoch is computed.
                hash: 0,  // Will populate once the epoch is computed.
                body: MessageDataBody {
                    owner_uid: self.owner_uid,
                    parents: self.roots.keys().cloned().collect(),
                    epoch: 0,  // Will be computed later.
                    payload,
                    endorsements,
                }
            }
        );
        for p in self.roots.values() {
           Message::link(p, &message);
        }
        message.borrow_mut().populate_from_parents(true, self.starting_epoch,
                                                   self.witness_selector);
        message.borrow_mut().data.body.epoch = message.borrow().computed_epoch
            .expect("The previous call to populate_from_parents should have populated computed_epoch");
        // Compute the correct hash
        let hash = DAG::<'a, T, W>::hash_message_data_body(&message.borrow().data.body);
        message.borrow_mut().data.hash = hash;
        // TODO: Compute the signature, too.
        let moved_message = self.messages.insert(hash, message)
            .expect("Hash collision: old message already has the same hash as the new one.");
        self.roots.clear();
        self.roots.insert(hash, moved_message);
    }
}
