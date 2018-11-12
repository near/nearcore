use primitives::types::*;
use primitives::traits::{WitnessSelector, Payload};

use std::collections::HashSet;

use super::message::Message;
use typed_arena::Arena;

// TODO: This code has incorrect lifetimes. Fix it.
/// The data-structure of the TxFlow DAG that supports adding messages and updating counters/flags,
/// but does not support communication-related logic. Also does verification of the messages
/// received from other nodes.
pub struct DAG<'a, P: 'a + Payload, W: 'a + WitnessSelector> {
    /// UID of the node.
    owner_uid: u64,
    arena: &'a Arena<Message<'a, P>>,
    /// Stores all messages known to the current root.
    messages: HashSet<&'a Message<'a, P>>,
    /// Stores all current roots.
    roots: HashSet<&'a Message<'a, P>>,

    witness_selector: &'a W,
    starting_epoch: u64,
}

impl<'a, P: 'a + Payload, W:'a+ WitnessSelector> DAG<'a, P, W> {
    pub fn new(arena: &'a Arena<Message<'a, P>>, owner_uid: u64, starting_epoch: u64, witness_selector: &'a W) -> Self {
        DAG {
            owner_uid,
            arena,
            messages: HashSet::new(),
            roots: HashSet::new(),
            witness_selector,
            starting_epoch,
        }
    }

    /// Verify that this message does not violate the protocol.
    fn verify_message(&self, _message: &Message<'a, P>) -> Result<(), &'static str> {
        Ok({})
    }

    // Takes ownership of the message.
    pub fn add_existing_message(&mut self, message_data: SignedMessageData<P>) -> Result<(), &'static str> {
        // Check whether this is a new message.
        if self.messages.contains(&message_data.hash) {
            return Ok({})
        }

        // Wrap message data and connect to the parents so that the verification can be run.
        let mut message = Message::new(message_data);
        for p_hash in &message.data.body.parents {
            if let Some(&p) = self.messages.get(p_hash) {
                message.parents.insert(p);
            } else {
                return Err("Some parents of the message are unknown");
            }
        }

        // Compute epochs, endorsements, etc.
        message.init(true, self.starting_epoch, self.witness_selector);

        // Verify the message.
        if let Err(e) = self.verify_message(&message) {
            return Err(e)
        }

        // Finally, take ownership of the message and update the roots.
        let message_ref = &*self.arena.alloc(message);

        self.messages.insert(message_ref);
        for p in &message_ref.parents {
            self.roots.remove(p);
        }
        self.roots.insert(message_ref);
        Ok({})
    }

    /// Creates a new message that points to all existing roots. Takes ownership of the payload and
    /// the endorsements.
    pub fn create_root_message(&mut self, payload: P, endorsements: Vec<Endorsement>) {
        let mut message = Message::new(
            SignedMessageData {
                owner_sig: 0,  // Will populate once the epoch is computed.
                hash: 0,  // Will populate once the epoch is computed.
                body: MessageDataBody {
                    owner_uid: self.owner_uid,
                    parents: (&self.roots).into_iter().map(|m| m.computed_hash).collect(),
                    epoch: 0,  // Will be computed later.
                    payload,
                    endorsements,
                }
            }
        );
        message.init(true, self.starting_epoch, self.witness_selector);
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
    use super::*;
    use std::collections::{HashSet, HashMap};
    use typed_arena::Arena;

    struct FakeWitnessSelector {
        schedule: HashMap<u64, HashSet<u64>>,
    }

    impl FakeWitnessSelector {
        fn new() -> FakeWitnessSelector {
            FakeWitnessSelector {
                schedule: map!{
               0 => set!{0, 1, 2, 3}, 1 => set!{1, 2, 3, 4},
               2 => set!{2, 3, 4, 5}, 3 => set!{3, 4, 5, 6}}
            }
        }
    }

    impl WitnessSelector for FakeWitnessSelector {
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
        let arena = Arena::new();
        let data_arena = Arena::new();
        let mut dag = DAG::new(&arena, 0, 0, &selector);
        let (a, b, c, d, e);
        simple_bare_messages!(data_arena [[0, 0 => a; 1, 2 => b;] => 2, 3 => c;]);
        simple_bare_messages!(data_arena [[=> a; 3, 4 => d;] => 4, 5 => e;]);
        assert!(dag.add_existing_message((*a).clone()).is_ok());
        // TODO: Fix lifetimes.
        assert!(dag.add_existing_message((*b).clone()).is_ok());
    }
}
