mod message;

use primitives::types::*;
use primitives::traits::{WitnessSelector, Payload};

use std::collections::HashSet;

use self::message::Message;
use typed_arena::Arena;

/// The data-structure of the TxFlow DAG that supports adding messages and updating counters/flags,
/// but does not support communication-related logic. Also does verification of the messages
/// received from other nodes.
/// It uses unsafe code to implement a self-referential struct and the interface makes sure that
/// the references never outlive the instances.
pub struct DAG<'a, P: 'a + Payload, W: 'a + WitnessSelector> {
    /// UID of the node.
    owner_uid: UID,
    arena: Arena<Box<Message<'a, P>>>,
    /// Stores all messages known to the current root.
    messages: HashSet<&'a Message<'a, P>>,
    /// Stores all current roots.
    roots: HashSet<&'a Message<'a, P>>,

    witness_selector: &'a W,
    starting_epoch: u64,
}

impl<'a, P: 'a + Payload, W:'a+ WitnessSelector> DAG<'a, P, W> {
    pub fn new(owner_uid: UID, starting_epoch: u64, witness_selector: &'a W) -> Self {
        DAG {
            owner_uid,
            arena: Arena::new(),
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
        let mut message = Box::new(Message::new(message_data));
        let parent_hashes:Vec<StructHash> = message.data.body.parents.iter().cloned().collect();

        for p_hash in parent_hashes {
            if let Some(&p) = self.messages.get(&p_hash) {
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
        for p in &message.parents {
            self.roots.remove(p);
        }

        let message_ptr = self.arena.alloc(message).as_ref() as *const Message<'a, P>;
        self.messages.insert(unsafe{&*message_ptr});
        self.roots.insert(unsafe{&*message_ptr});
        Ok({})
    }

    /// Creates a new message that points to all existing roots. Takes ownership of the payload and
    /// the endorsements.
    pub fn create_root_message(&mut self, payload: P, endorsements: Vec<Endorsement>) -> &'a Message<'a, P> {
        let mut message = Box::new(Message::new(
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
        ));
        message.init(true, self.starting_epoch, self.witness_selector);
        message.assume_computed_hash_epoch();

        // Finally, take ownership of the new root.
        let message_ptr = self.arena.alloc(message).as_ref() as *const Message<'a, P>;
        self.messages.insert(unsafe { &*message_ptr });
        self.roots.clear();
        self.roots.insert(unsafe { &*message_ptr });
        unsafe { &*message_ptr }
    }
}


#[cfg(test)]
mod tests {


    use super::*;
    use std::collections::{HashSet, HashMap};
    use typed_arena::Arena;

    struct FakeWitnessSelector {
        schedule: HashMap<u64, HashSet<UID>>,
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
        fn epoch_leader(&self, epoch: u64) -> UID {
            *self.epoch_witnesses(epoch).iter().min().unwrap()
        }
    }

    #[test]
    fn check_correct_epoch_simple(){
        let selector = FakeWitnessSelector::new();
        let data_arena = Arena::new();
        let mut all_messages = vec![];
        let mut dag = DAG::new(0, 0, &selector);

        // Parent have greater epoch than children
        simple_bare_messages!(data_arena, all_messages [[1, 2;] => 1, 1;]);

        let mut ok = true;

        for m in all_messages {
            assert_eq!(dag.add_existing_message((*m).clone()).is_ok(), ok);
            ok = !ok;
        }
    }

    #[test]
    fn check_correct_epoch_complex(){
        // When a message can have epoch k, but since it doesn't have messages
        // with smaller epochs it creates them

        let selector = FakeWitnessSelector::new();
        let data_arena = Arena::new();
        let mut all_messages = vec![];
        let mut dag = DAG::new(0, 0, &selector);

        let (b, d, f, g);
        simple_bare_messages!(data_arena, all_messages [[0, 0; 1, 0 => b; 2, 0;] => 0, 1 => d;]);
        simple_bare_messages!(data_arena, all_messages [[=> d;] => 0, 2;]);

        for m in &all_messages {
            assert!(dag.add_existing_message((*m).clone()).is_ok());
        }

        simple_bare_messages!(data_arena, all_messages [[=> d; => b;] => 1, 2 => f;]);
        simple_bare_messages!(data_arena, all_messages [[=> d; => b;] => 1, 1 => g;]);

        // Incorrect epoch
        assert!(dag.add_existing_message((*f).clone()).is_err());

        // Correct epoch
        assert!(dag.add_existing_message((*g).clone()).is_ok());
    }

    #[test]
    fn notice_simple_fork() {
        let selector = FakeWitnessSelector::new();
        let data_arena = Arena::new();
        let mut all_messages = vec![];
        let mut dag = DAG::new(0, 0, &selector);

        let (a, b, c);
        simple_bare_messages!(data_arena, all_messages [[0, 0; 1, 0;] => 1, 1 => a;]);
        simple_bare_messages!(data_arena, all_messages [[2, 0; 1, 0;] => 1, 1 => b;]);

        for m in &all_messages {
            assert!(dag.add_existing_message((*m).clone()).is_ok());
        }

        simple_bare_messages!(data_arena, all_messages [[=> a; => b;] => 3, 2 => c;]);
        assert!(dag.add_existing_message((*c).clone()).is_err());
    }

    #[test]
    fn feed_complex_topology() {
        let selector = FakeWitnessSelector::new();
        let data_arena = Arena::new();
        let mut all_messages = vec![];
        let mut dag = DAG::new(0, 0, &selector);
        let (a, b);
        simple_bare_messages!(data_arena, all_messages [[0, 0 => a; 1, 2;] => 2, 3 => b;]);
        simple_bare_messages!(data_arena, all_messages [[=> a; 3, 4;] => 4, 5;]);
        simple_bare_messages!(data_arena, all_messages [[=> a; => b; 0, 0;] => 4, 3;]);

        // Feed messages in DFS order which ensures that the parents are fed before the children.
        for m in all_messages {
            assert!(dag.add_existing_message((*m).clone()).is_ok());
        }
    }

    #[test]
    fn check_missing_messages_as_feeding() {
        let selector = FakeWitnessSelector::new();
        let data_arena = Arena::new();
        let mut all_messages = vec![];
        let mut dag = DAG::new(0, 0, &selector);
        let (a, b, c, d, e);
        simple_bare_messages!(data_arena, all_messages [[0, 0 => a; 1, 2 => b;] => 2, 3 => c;]);
        simple_bare_messages!(data_arena, all_messages [[=> a; 3, 4 => d;] => 4, 5 => e;]);
        assert!(dag.add_existing_message((*a).clone()).is_ok());
        // Check we cannot add message e yet, because it's parent d was not received, yet.
        assert!(dag.add_existing_message((*e).clone()).is_err());
        assert!(dag.add_existing_message((*d).clone()).is_ok());
        // Check that we have two dangling roots now.
        assert_eq!(dag.roots.len(), 2);
        // Now we can add message e, because we know all its parents!
        assert!(dag.add_existing_message((*e).clone()).is_ok());
        // Check that there is only one root now.
        assert_eq!(dag.roots.len(), 1);
        // Still we cannot add message c, because b is missing.
        assert!(dag.add_existing_message((*c).clone()).is_err());
        // Now add b and c.
        assert!(dag.add_existing_message((*b).clone()).is_ok());
        assert!(dag.add_existing_message((*c).clone()).is_ok());
        // Check that we again have to dangling roots -- e and c.
        assert_eq!(dag.roots.len(), 2);
    }

    #[test]
    fn create_roots() {
        let selector = FakeWitnessSelector::new();
        let data_arena = Arena::new();
        let mut all_messages = vec![];
        let mut dag = DAG::new(0, 0, &selector);
        let (a, b, c, d, e);
        simple_bare_messages!(data_arena, all_messages [[0, 0 => a; 1, 2 => b;] => 2, 3 => c;]);

        assert!(dag.add_existing_message((*a).clone()).is_ok());
        let message = dag.create_root_message(::testing_utils::FakePayload{}, vec![]);
        d = &message.data;

        simple_bare_messages!(data_arena, all_messages [[=> b; => d;] => 4, 5 => e;]);

        // Check that we cannot message e, because b was not added yet.
        assert!(dag.add_existing_message((*e).clone()).is_err());

        assert!(dag.add_existing_message((*b).clone()).is_ok());
        assert!(dag.add_existing_message((*e).clone()).is_ok());
        assert!(dag.add_existing_message((*c).clone()).is_ok());
    }

    // Test whether our implementation of a self-referential struct is movable.
    #[test]
    fn movable() {
        let data_arena = Arena::new();
        let selector = FakeWitnessSelector::new();
        let mut dag = DAG::new(0, 0, &selector);
        let (a, b);
        // Add some messages.
        {
            let mut all_messages = vec![];
            simple_bare_messages!(data_arena, all_messages [[0, 0 => a; 1, 2;] => 2, 3 => b;]);
            simple_bare_messages!(data_arena, all_messages [[=> a; => b; 0, 0;] => 4, 3;]);
            for m in all_messages {
                assert!(dag.add_existing_message((*m).clone()).is_ok());
            }
        }
        // Move the DAG.
        let mut moved_dag = dag;
        // And add some more messages.
        {
            let mut all_messages = vec![];
            simple_bare_messages!(data_arena, all_messages [[=> a; => b; 0, 0;] => 4, 3;]);
            for m in all_messages {
                assert!(moved_dag.add_existing_message((*m).clone()).is_ok());
            }
        }
    }
}
