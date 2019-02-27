mod message;
mod reporter;

use primitives::signature::DEFAULT_SIGNATURE;
use primitives::consensus::{Payload, WitnessSelector, ConsensusBlockBody};
use primitives::types::*;

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

use self::message::Message;
pub use self::reporter::{
    DAGMisbehaviorReporter, MisbehaviorReporter, NoopMisbehaviorReporter, ViolationType,
};
use typed_arena::Arena;

/// The data-structure of the TxFlow DAG that supports adding messages and updating counters/flags,
/// but does not support communication-related logic. Also does verification of the messages
/// received from other nodes and store detected violations.
/// It uses unsafe code to implement a self-referential struct and the interface makes sure that
/// the references never outlive the instances.
pub struct DAG<
    'a,
    P: 'a + Payload + Default,
    W: 'a + WitnessSelector,
    M: 'a + MisbehaviorReporter = NoopMisbehaviorReporter,
> {
    /// UID of the node.
    owner_uid: UID,
    beacon_block_index: u64,
    arena: Arena<Box<Message<'a, P>>>,
    /// Stores all messages known to the current root.
    messages: HashSet<&'a Message<'a, P>>,
    /// Stores all current roots.
    roots: HashSet<&'a Message<'a, P>>,
    /// Store last message from each participant in the DAG.
    /// In case of a fork only one is stored arbitrarely.
    recent_message: HashMap<UID, &'a Message<'a, P>>,
    /// All epochs that were already published.
    published_epochs: HashSet<u64>,
    /// All messages that were ever published.
    published_messages: HashSet<&'a Message<'a, P>>,

    witness_selector: &'a W,
    starting_epoch: u64,

    misbehavior: Box<RefCell<M>>,
}

impl<'a, P: 'a + Payload + Default, W: WitnessSelector, M: 'a + MisbehaviorReporter> DAG<'a, P, W, M> {
    pub fn new(owner_uid: UID, beacon_block_index: u64, starting_epoch: u64, witness_selector: &'a W) -> Self {
        DAG {
            owner_uid,
            beacon_block_index,
            arena: Arena::new(),
            messages: HashSet::new(),
            roots: HashSet::new(),
            recent_message: HashMap::new(),
            published_epochs: HashSet::new(),
            published_messages: HashSet::new(),
            witness_selector,
            starting_epoch,
            misbehavior: Box::new(RefCell::new(M::new())),
        }
    }

    /// Whether there is one root only and it was created by the current owner.
    pub fn is_current_owner_root(&self) -> bool {
        self.current_root_data().map(|d| d.body.owner_uid == self.owner_uid).unwrap_or(false)
    }

    /// There is one or more roots (meaning it is not a very start of the DAG with no messages)
    /// and at least one of these roots is not by the current owner.
    pub fn is_root_not_updated(&self) -> bool {
        !self.roots.is_empty()
            && (&self.roots).iter().any(|m| m.data.body.owner_uid != self.owner_uid)
    }

    /// Return true if there are several roots.
    pub fn has_dangling_roots(&self) -> bool {
        self.roots.len() > 1
    }

    /// If there is one root it returns its data.
    pub fn current_root_data(&self) -> Option<&SignedMessageData<P>> {
        if self.roots.len() == 1 {
            self.roots.iter().next().map(|m| &m.data)
        } else {
            None
        }
    }

    pub fn contains_message(&self, hash: TxFlowHash) -> bool {
        self.messages.contains(&hash)
    }

    /// Create a copy of the message data from the dag given hash.
    pub fn copy_message_data_by_hash(&self, hash: TxFlowHash) -> Option<SignedMessageData<P>> {
        self.messages.get(&hash).map(|m| m.data.clone())
    }

    /// Check if a message form a fork with at least one message from the point of view of the DAG.
    /// Notice in case there is a multi-fork at least first fork is reported.
    /// IMPORTANT: The way is currently implemented don't log all forks.
    /// If Alice create a fork (A0, A1) this fork is detected properly but the last message stored
    /// is A1, and if a new message A2 approve A1 but not A0, then fork between A2 and A0 is
    /// not detected. In cases of fork it might be ok detecting one fork, cause even if more
    /// forks from the same participant happens, he will be slashed anyway. It is not a good idea
    /// store all pair of message forming a fork, cause there might be a quadratic number of forks
    /// (related to the number of messages).
    ///
    /// Example: (A0 -> A2 -> A4 -> ... and A1 -> A3 -> A5 -> ...)
    fn detect_fork(&self, message: &Message<'a, P>) -> Option<(TxFlowHash, TxFlowHash)> {
        match self.recent_message.get(&message.data.body.owner_uid) {
            Some(head) if !message.approve(head) => {
                Some((head.computed_hash, message.computed_hash))
            }
            _ => None,
        }
    }

    // Does inefficient DFS collecting parents from under the given representative.
    fn collect_parents(&mut self, message: &'a Message<'a, P>, parents: &mut Vec<&'a Message<'a, P>>) {
        if self.published_messages.contains(message) {
            return;
        }
        parents.push(message);
        self.published_messages.insert(message);
        for p in &message.parents {
           self.collect_parents(p, parents);
        }
    }

    /// Computes new consensus enabled by the given message.
    fn publishable_to_consensus(&mut self, message: &Message<'a, P>) -> Vec<ConsensusBlockBody<P>> {
        let mut publishable: Vec<_> =
            (&message.computed_publishable_epochs.messages_by_epoch).iter().filter(|(epoch, _)|
                // Check that we haven't published this epoch already.
                !self.published_epochs.contains(epoch)
            ).collect();
        // Lowest epochs first.
        publishable.sort_by(|(epoch1, _), (epoch2, _)| epoch1.cmp(epoch2));

        // Returned consensuses. Lowest epoch first.
        let mut res = vec![];
        // TODO(#125) Currently this goes through without beacon chain consensus. Once we have a
        // beacon chain consensus the epoch will be used.
        for (epoch, group) in publishable {
            let repr = group.messages_by_owner.values().next()
                .expect("At least one message expected.")
                .iter().next().expect("At least one message expected.");
            let mut parents = vec![];
            self.collect_parents(repr, &mut parents);
            let mut payload = P::default();
            for m in parents.iter() {
                payload.union_update(m.data.body.payload.clone());
            }
            res.push(ConsensusBlockBody {
                payload,
                beacon_block_index: self.beacon_block_index
            });
            self.published_epochs.insert(*epoch);
        }
        res
    }

    /// Verify correctness of this message regarding txflow protocol.
    /// Report all misbehavior as soon as they are detected.
    fn verify_message(&mut self, message: &Message<'a, P>) -> Result<(), &'static str> {
        // Check epoch
        if message.computed_epoch != message.data.body.epoch {
            let mb = ViolationType::BadEpoch(message.computed_hash);
            self.misbehavior.borrow_mut().report(mb);
        }

        // Check fork
        if let Some(fork_data) = self.detect_fork(message) {
            let mb = ViolationType::ForkAttempt(fork_data.0, fork_data.1);
            self.misbehavior.borrow_mut().report(mb);
        }

        Ok(())
    }

    /// Takes ownership of the message, maybe adds it to the DAG. Produces the list of consensuses
    /// that this message has enabled.
    pub fn add_existing_message(
        &mut self,
        message_data: SignedMessageData<P>,
    ) -> Result<Vec<ConsensusBlockBody<P>>, &'static str> {
        // Check whether this is a new message.
        if self.messages.contains(&message_data.hash) {
            return Ok(vec![]);
        }

        // Wrap message data and connect to the parents so that the verification can be run.
        let mut message = Box::new(Message::new(message_data));
        let parent_hashes = message.data.body.parents.clone();

        for p_hash in parent_hashes {
            if let Some(&p) = self.messages.get(&p_hash) {
                message.parents.insert(p);
            } else {
                return Err("Some parents of the message are unknown");
            }
        }

        // Compute epochs, endorsements, etc.
        message.init(true, true, self.starting_epoch, self.witness_selector);

        // Verify the message.
        self.verify_message(&message)?;

        // Finally, take ownership of the message and update the roots.
        for p in &message.parents {
            self.roots.remove(p);
        }

        let owner = message.data.body.owner_uid;
        let message_ptr = self.arena.alloc(message).as_ref() as *const Message<'a, P>;
        self.messages.insert(unsafe { &*message_ptr });
        self.roots.insert(unsafe { &*message_ptr });
        self.recent_message.insert(owner, unsafe { &*message_ptr });

        // Compute consensuses enabled by this message.
        let consensuses = self.publishable_to_consensus(unsafe { &*message_ptr });
        Ok(consensuses)
    }

    /// Creates a new message that points to all existing roots. Takes ownership of the payload and
    /// the endorsements.
    pub fn create_root_message(
        &mut self,
        payload: P,
        endorsements: Vec<Endorsement>,
    ) -> (&'a Message<'a, P>, Vec<ConsensusBlockBody<P>>) {
        let mut message = Box::new(Message::new(SignedMessageData {
            owner_sig: DEFAULT_SIGNATURE, // Will populate once the epoch is computed.
            hash: 0,                      // Will populate once the epoch is computed.
            body: MessageDataBody {
                owner_uid: self.owner_uid,
                parents: (&self.roots).iter().map(|m| m.computed_hash).collect(),
                epoch: 0, // Will be computed later.
                payload,
                endorsements,
            },
            beacon_block_index: self.beacon_block_index,
        }));
        message.parents = self.roots.clone();
        message.init(true, false, self.starting_epoch, self.witness_selector);
        message.assume_computed_hash_epoch();

        // Finally, take ownership of the new root.
        let message_ptr = self.arena.alloc(message).as_ref() as *const Message<'a, P>;
        self.messages.insert(unsafe { &*message_ptr });
        self.roots.clear();
        self.roots.insert(unsafe { &*message_ptr });

        // Compute consensuses enabled by this message.
        let consensuses = self.publishable_to_consensus(unsafe { &*message_ptr });
        (unsafe { &*message_ptr }, consensuses)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use primitives::types::UID;
    use std::collections::{HashMap, HashSet};
    use typed_arena::Arena;

    struct FakeWitnessSelector {
        schedule: HashMap<u64, HashSet<UID>>,
    }

    impl FakeWitnessSelector {
        fn new() -> FakeWitnessSelector {
            FakeWitnessSelector {
                schedule: map! {
                0 => set!{0, 1, 2, 3}, 1 => set!{1, 2, 3, 4},
                2 => set!{2, 3, 4, 5}, 3 => set!{3, 4, 5, 6}},
            }
        }
    }

    impl WitnessSelector for FakeWitnessSelector {
        fn epoch_witnesses(&self, epoch: u64) -> &HashSet<UID> {
            self.schedule.get(&epoch).unwrap()
        }
        fn epoch_leader(&self, epoch: u64) -> UID {
            *self.epoch_witnesses(epoch).iter().min().unwrap()
        }
        fn random_witnesses(&self, _epoch: u64, _sample_size: usize) -> HashSet<UID> {
            unimplemented!()
        }
    }

    #[test]
    fn incorrect_epoch_simple() {
        let selector = FakeWitnessSelector::new();
        let data_arena = Arena::new();
        let mut all_messages = vec![];
        let mut dag: DAG<_, _, DAGMisbehaviorReporter> = DAG::new(0, 0, 0, &selector);

        // Parent have greater epoch than children
        let (a, b);
        simple_bare_messages!(data_arena, all_messages [[1, 2 => a;] => 1, 1 => b;]);

        assert!(dag.add_existing_message((*a).clone()).is_ok());
        assert!(dag.add_existing_message((*b).clone()).is_ok());

        for message in &dag.messages {
            assert_eq!(message.computed_epoch, 0);
        }

        // Both messages have invalid epoch number so two reports were made
        assert_eq!(dag.misbehavior.borrow().violations.len(), 2);

        for violation in &dag.misbehavior.borrow().violations {
            if let ViolationType::BadEpoch(_) = violation {
                // expected violation type
            } else {
                assert!(false);
            }
        }
    }

    #[test]
    fn correct_epoch_complex() {
        // When a message can have epoch k, but since it doesn't have messages
        // with smaller epochs it creates them.

        let selector = FakeWitnessSelector::new();
        let data_arena = Arena::new();
        let mut all_messages = vec![];
        let mut dag: DAG<_, _, DAGMisbehaviorReporter> = DAG::new(0, 0, 0, &selector);

        let (a, b);
        simple_bare_messages!(data_arena, all_messages [[0, 0; 1, 0; 3, 0;] => 0, 1 => a;]);
        simple_bare_messages!(data_arena, all_messages [[=> a;] => 3, 2 => b;]);

        for m in &all_messages {
            assert!(dag.add_existing_message((*m).clone()).is_ok());
        }

        for message in &dag.messages {
            if message.computed_hash != b.hash {
                assert_eq!(message.computed_epoch, message.data.body.epoch);
            } else {
                assert_eq!(message.computed_epoch, 1);
            }
        }
    }

    #[test]
    fn feed_complex_topology() {
        let selector = FakeWitnessSelector::new();
        let data_arena = Arena::new();
        let mut all_messages = vec![];
        let mut dag: DAG<_, _> = DAG::new(0, 0, 0, &selector);
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
        let mut dag: DAG<_, _> = DAG::new(0, 0, 0, &selector);
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
        let mut dag: DAG<_, _> = DAG::new(0, 0, 0, &selector);
        let (a, b, c, d, e);
        simple_bare_messages!(data_arena, all_messages [[0, 0 => a; 1, 2 => b;] => 2, 3 => c;]);

        assert!(dag.add_existing_message((*a).clone()).is_ok());
        let (message, _) = dag.create_root_message(crate::testing_utils::FakePayload {}, vec![]);
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
        let selector = FakeWitnessSelector::new();
        let data_arena = Arena::new();
        let mut dag: DAG<_, _> = DAG::new(0, 0, 0, &selector);
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

    /// Unfinished test
    #[test]
    fn notice_simple_fork() {
        let selector = FakeWitnessSelector::new();
        let data_arena = Arena::new();
        let mut all_messages = vec![];
        let mut dag: DAG<_, _, DAGMisbehaviorReporter> = DAG::new(0, 0, 0, &selector);

        let a;

        simple_bare_messages!(data_arena, all_messages [[0, 0; 1, 0 => a;] => 3, 0;]);
        simple_bare_messages!(data_arena, all_messages [[2, 0; => a;] => 3, 0;]);

        for m in &all_messages {
            assert!(dag.add_existing_message((*m).clone()).is_ok());
        }

        println!("Violations: {}", &dag.misbehavior.borrow().violations.len());
    }
}
