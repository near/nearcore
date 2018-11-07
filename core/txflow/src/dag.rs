use primitives::types::*;
use primitives::traits::{WitnessSelectorLike};

use std::rc::Rc;
use std::collections::HashMap;
use std::hash::Hash;

use super::message::{MessageRef, Message};

/// The data-structure of the TxFlow DAG that supports adding messages and updating counters/flags,
/// but does not support communication-related logic. Also does verification of the messages
/// received from other nodes.
pub struct DAG<'a, T: Hash, W: 'a+ WitnessSelectorLike> {
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

impl<'a, T: Hash, W:'a+ WitnessSelectorLike> DAG<'a, T, W> {
    pub fn new(owner_uid: u64, starting_epoch: u64, witness_selector: &'a W) -> Self {
        DAG{
            owner_uid,
            messages: HashMap::new(),
            roots: HashMap::new(),
            witness_selector,
            starting_epoch,
        }
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
                    Self::unlink_parents(&linked_parents, &message);
                    return Err("Some parents of the message are unknown")
                }
            }
        }

        // Compute approved epochs and endorsements.
        message.borrow_mut().populate_from_parents(true,
                                                   self.starting_epoch, self.witness_selector);

        // Verify the message.
        if let Err(e) = self.verify_message(&message) {
            Self::unlink_parents(&linked_parents, &message);
            return Err(e)
        }

        // Finally, remember the message and update the roots.
        let hash = message.borrow().data.hash;
        self.messages.insert(hash, message.clone());
        for p in &linked_parents {
            self.roots.remove(&p.borrow().data.hash);
        }
        self.roots.insert(hash, message.clone());
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
        message.borrow_mut().data.hash = message.borrow().computed_hash
            .expect("The previous call to populate_from_parents should have populated computed_hash");

        let hash = message.borrow_mut().data.hash;
        let moved_message = self.messages.insert(hash, message)
            .expect("Hash collision: old message already has the same hash as the new one.");
        self.roots.clear();
        self.roots.insert(hash, moved_message);
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use ::testing_utils::{FakePayload, simple_message};

    struct FakeWitnessSelector {
        schedule: HashMap<u64, HashSet<u64>>,
    }
    impl FakeWitnessSelector {
        fn new() -> FakeWitnessSelector {
            FakeWitnessSelector {
                schedule: map! { 0 => set!{0, 1, 2, 3}, 1 => set!{1, 2, 3, 4} }
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
    fn one_node_dag() {
        let selector = FakeWitnessSelector::new();
        let mut dag: DAG<FakePayload, FakeWitnessSelector> = DAG::new(0, 0, &selector);
        assert!(dag.add_existing_message(simple_message(0, 1)).is_ok());
    }
}