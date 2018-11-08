use primitives::types::*;
use primitives::traits::{WitnessSelectorLike};

use std::collections::HashSet;
use std::hash::Hash;

use super::message::Message;
use typed_arena::Arena;

/// The data-structure of the TxFlow DAG that supports adding messages and updating counters/flags,
/// but does not support communication-related logic. Also does verification of the messages
/// received from other nodes.
pub struct DAG<'a, T: Hash, W: 'a+ WitnessSelectorLike> {
    /// UID of the node.
    owner_uid: u64,
    arena: Arena<Message<'a, T>>,
    /// Stores all messages known to the current root.
    messages: HashSet<&'a Message<'a, T>>,
    /// Stores all current roots.
    roots: HashSet<&'a Message<'a, T>>,

    witness_selector: &'a W,
    starting_epoch: u64,
}

impl<'a, T: Hash, W:'a+ WitnessSelectorLike> DAG<'a, T, W> {
    pub fn new(owner_uid: u64, starting_epoch: u64, witness_selector: &'a W) -> Self {
        DAG{
            owner_uid,
            arena: Arena::new(),
            messages: HashSet::new(),
            roots: HashSet::new(),
            witness_selector,
            starting_epoch,
        }
    }

    /// Verify that this message does not violate the protocol.
    fn verify_message(&self, _message: &Message<T>) -> Result<(), &'static str> {
        Ok({})
    }

    // Takes ownership of the message.
    pub fn add_existing_message(&mut self, message_data: SignedMessageData<T>) -> Result<(), &'static str> {
        // Check whether this is a new message.
        if self.messages.contains_key(&message_data.hash) {
            return Ok({})
        }

        // Wrap message data and connect to the parents so that the verification can be run.
        let mut message = Message::new(message_data);
        let mut linked_parents: Vec<MessageRef<T>> = vec![];
        for p_hash in message.data.body.parents {
            if let Some(&p) = self.messages.get(p_hash) {
                message.parents.push(p);
            } else {
                return Err("Some parents of the message are unknown");
            }
        }

        // Compute epochs, endorsements, etc.
        message.populate_from_parents(true,
                                      self.starting_epoch, self.witness_selector);

        // Verify the message.
        if let Err(e) = self.verify_message(&message) {
            return Err(e)
        }

        // Finally, take ownership of the message and update the roots.
        let message_ref = &*self.arena.alloc(message);

        self.messages.insert(message_ref);
        for p in message_ref.parents {
            self.roots.remove(p);
        }
        self.roots.insert(message_ref);
        Ok({})
    }

    /// Creates a new message that points to all existing roots. Takes ownership of the payload and
    /// the endorsements.
    pub fn create_root_message(&mut self, payload: T, endorsements: Vec<Endorsement>) {
        let mut message = Message::new(
            SignedMessageData {
                owner_sig: 0,  // Will populate once the epoch is computed.
                hash: 0,  // Will populate once the epoch is computed.
                body: MessageDataBody {
                    owner_uid: self.owner_uid,
                    parents: self.roots.into_iter().collect(),
                    epoch: 0,  // Will be computed later.
                    payload,
                    endorsements,
                }
            }
        );
        message.populate_from_parents(true, self.starting_epoch, self.witness_selector);
        message.assume_computed_hash_epoch();

        // Finally, take ownership of the new root.
        let message_ref = &*self.arena.alloc(message);
        self.messages.insert(message_ref);
        self.roots.clear();
        self.roots.insert(message_ref);
    }
}


#[cfg(test)]
mod tests {
//    use super::*;
//    use std::collections::HashSet;
//    use ::testing_utils::{FakePayload, simple_message};
//
//    struct FakeWitnessSelector {
//        schedule: HashMap<u64, HashSet<u64>>,
//    }
//    impl FakeWitnessSelector {
//        fn new() -> FakeWitnessSelector {
//            FakeWitnessSelector {
//                schedule: map! { 0 => set!{0, 1, 2, 3}, 1 => set!{1, 2, 3, 4} }
//            }
//        }
//    }
//
//    impl WitnessSelectorLike for FakeWitnessSelector {
//        fn epoch_witnesses(&self, epoch: u64) -> &HashSet<u64> {
//            self.schedule.get(&epoch).unwrap()
//        }
//        fn epoch_leader(&self, epoch: u64) -> u64 {
//            *self.epoch_witnesses(epoch).iter().min().unwrap()
//        }
//    }
//
//    #[test]
//    fn one_node_dag() {
//        let selector = FakeWitnessSelector::new();
//        let mut dag: DAG<FakePayload, FakeWitnessSelector> = DAG::new(0, 0, &selector);
//        assert!(dag.add_existing_message(simple_message(0, 1)).is_ok());
//    }


    use super::Arena;
    struct V<'a> {
        pub parents: Vec<&'a V<'a>>,
    }

    #[test]
    fn simple_graph() {
        let arena = Arena::new();
        let first = &*arena.alloc(V{parents: vec![]});
        let second = &*arena.alloc(V{parents: vec![first]});
        let third =  &*arena.alloc(V{parents: vec![first, second]});
        let tmp = third.parents[1].parents[0];
    }
}