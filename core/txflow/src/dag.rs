use primitives::types::{SignedEpochBlockHeader, SignedTransaction};

use std::collections::{HashMap, HashSet};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hasher, Hash};

use super::types;
use super::message::{MessageRef, Message};

/// The data-structure of the TxFlow DAG that supports adding messages and updating counters/flags,
/// but does not support communication-related logic. Also does verification of the messages
/// received from other nodes.
pub struct DAG {
    /// UID of the node.
    owner_uid: u64,
    /// Message hash -> Message.
    messages: HashMap<u64, MessageRef>,
    /// Message hashes of the roots.
    roots: HashSet<u64>,
    /// The current epoch of the messages created by the node.
    current_epoch: u64,
    /// Epoch -> Representative message of that epoch.
    representatives: HashMap<u64, MessageRef>,
    /// Epoch -> Kickout message of that epoch.
    kickouts: HashMap<u64, MessageRef>,
}

impl DAG {
    pub fn new(owner_uid: u64, starting_epoch: u64) -> Result<DAG, &'static str> {
        Ok(DAG{
            owner_uid,
            messages: HashMap::new(),
            roots: HashSet::new(),
            current_epoch: starting_epoch,
            representatives: HashMap::new(),
            kickouts: HashMap::new(),
        })
    }

    // Takes ownership of the payload.
    fn create_message(owner_uid: u64, parents: Vec<u64>, epoch: u64, is_representative: bool,
                      is_endorsement: bool, transactions: Vec<SignedTransaction>,
                      epoch_block_header: Option<SignedEpochBlockHeader>
    ) -> types::SignedMessageData {
        let body = types::MessageDataBody {
            owner_uid,
            parents,
            epoch,
            transactions,
            epoch_block_header
        };
        let mut hasher = DefaultHasher::new();
        body.hash(&mut hasher);
        let hash = hasher.finish();
        types::SignedMessageData {
            // TODO: Actually compute the signature.
            owner_sig: 0,
            hash,
            body
        }
    }

    fn update_state(&mut self, message: &types::SignedMessageData) {
        // Update epoch_counter.
        self.epoch_counter.entry(message.body.epoch).or_insert_with(|| HashSet::new())
            .insert(message.body.owner_uid);

        // Compute endorsements.
        // If message p is missing from the graph then compute assume that it endorses epoch 0.
        let endorsing_epoch: u64 = message.body.parents.iter()
            .map(|p| self.message_endorsement.get(p).unwrap_or(&0)).sum();
        self.message_endorsement.insert(message.hash, endorsing_epoch);
        self.representative_endorsements.entry(endorsing_epoch).or_insert_with(|| HashSet::new())
            .insert(message.body.owner_uid);
    }

    // Verify that the received message is valid: has correct hash, signature, epoch, and is_representative
    // tags.
    fn verify_message(&self, _message: &MessageRef) -> Result<(), &'static str> {
        Ok({})
    }

    fn epoch_leader(&self, _epoch: u64) -> u64 {
        // TODO: call a closure.
        42
    }

    // Takes ownership of the message.
    pub fn add_existing_message(&mut self, message_data: types::SignedMessageData) -> Result<(), &'static str> {
        // Check whether this is a new message.
        if self.messages.contains_key(&message_data.hash) {
            return Ok({})
        }

        // Wrap message data and connect to the parents so that the verification can be run.
        let message = Message::new(message_data);
        let mut linked_parents = vec![];
        let unlink_parents = || {
            // Unlink the previously linked parents.
            for linked_p in linked_parents {
                Message::unlink(p, &message);
            };
        };
        for p_hash in message.borrow().data.body.parents {
            match self.messages.get(&p_hash) {
                Some(p) => {
                    linked_parents.push(p);
                    Message::link(p, &message);
                },
                None => {
                    unlink_parents();
                    return Err("Some parents of the message are unknown")
                }
            }
        }

        // Compute approved epochs and endorsements.
        message.borrow_mut().compute_approved_epochs();

        // Verify the message.
        if let Err(e) = self.verify_message(&message) {
            unlink_parents();
            return Err(e)
        }

        // Finally, remember the message and update the roots.
        let moved_message = self.messages.insert(message_data.hash, message).unwrap();
        for p in linked_parents {
            self.roots.remove(&p.borrow().data.hash);
        }
        self.roots.insert(moved_message.borrow().data.hash);
        Ok({})
    }

    // Takes ownership of the payload.
    pub fn create_root_message(&mut self,
                               is_endorsement: bool,
                               transactions: Vec<SignedTransaction>,
                               epoch_block_header: Option<SignedEpochBlockHeader>) {
        // Check if this is leader's representative.
        let is_representative = self.epoch_leader(self.current_epoch) == self.owner_uid
            && !self.epoch_representatives.contains_key(&self.current_epoch);
        let message = DAG::create_message(
            self.owner_uid,
            self.roots.iter().cloned().collect(),
            self.current_epoch,
            is_representative,
            is_endorsement,
            transactions,
            epoch_block_header
        );
        self.update_state(&message);
        let moved_message = self.messages.insert(message.hash, message)
            .expect("Hash collision: old message already has the same hash as the new one.");
        self.roots.clear();
        self.roots.insert(moved_message.hash);
    }
}
