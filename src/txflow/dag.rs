use std::collections::{HashMap, HashSet};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hasher, Hash};

use super::types;

pub struct DAG {
    // uid of the owner of this graph.
    owner_uid: u64,
    // Message hash -> Message.
    messages: HashMap<u64, types::SignedMessage>,
    // Message hashes.
    roots: HashSet<u64>,
    // Epoch -> set of owner_uid that have messages with
    epoch_counter: HashMap<u64, HashSet<u64>>,
    // Epoch -> hash of the commit message of that epoch.
    epoch_commits: HashMap<u64, u64>,
    // Message hash -> epoch of the commit that it endorses.
    message_endorsement: HashMap<u64, u64>,
    // Commit message hash -> uids of the owners of the messages that endorse it.
    commit_endorsements: HashMap<u64, HashSet<u64>>,
    // The current epoch of the messages created by the current node.
    current_epoch: u64
}

impl DAG {
    pub fn new(owner_uid: u64, starting_epoch: u64) -> Result<DAG, &'static str> {
        Ok(DAG{
            owner_uid,
            messages: HashMap::new(),
            roots: HashSet::new(),
            epoch_counter: HashMap::new(),
            epoch_commits: HashMap::new(),
            message_endorsement: HashMap::new(),
            commit_endorsements: HashMap::new(),
            current_epoch: starting_epoch
        })
    }

    // Takes ownership of the payload.
    fn create_message(owner_uid: u64, parents: Vec<u64>, epoch: u64, is_commit: bool,
                      payload: types::Payload) -> types::SignedMessage {
        let body = types::MessageBody{
            owner_uid,
            parents,
            epoch,
            is_commit,
            payload
        };
        let mut hasher = DefaultHasher::new();
        body.hash(&mut hasher);
        let hash = hasher.finish();
        types::SignedMessage {
            // TODO: Actually compute the signature.
            owner_sig: 0,
            hash,
            body
        }
    }

    fn update_state(&mut self, message: &types::SignedMessage) {
        // Update epoch_counter.
        self.epoch_counter.entry(message.body.epoch).or_insert_with(|| HashSet::new())
            .insert(message.body.owner_uid);

        // Compute endorsements.
        // If message p is missing from the graph then compute assume that it endorses epoch 0.
        let endorsing_epoch: u64 = message.body.parents.iter()
            .map(|p| self.message_endorsement.get(p).unwrap_or(&0)).sum();
        self.message_endorsement.insert(message.hash, endorsing_epoch);
        self.commit_endorsements.entry(endorsing_epoch).or_insert_with(|| HashSet::new())
            .insert(message.body.owner_uid);
    }

    // Verify that the received message is valid: has correct hash, signature, epoch, and is_commit
    // tags.
    fn verify_message(&self, _message: &types::SignedMessage) -> Result<(), &'static str> {
        Ok({})
    }

    fn epoch_leader(&self, _epoch: u64) -> u64 {
        // TODO: call a closure.
        self.owner_uid
    }

    // Takes ownership of the message.
    pub fn add_existing_message(&mut self, message: types::SignedMessage) -> Result<(), &'static str> {
        // It does double look-up in the self.messages. Consider optimizing it.
        if self.messages.contains_key(&message.hash) {
            return Ok({})
        }
        self.verify_message(&message)?;
        self.update_state(&message);
        let moved_message = self.messages.insert(message.hash, message).unwrap();
        for p in moved_message.body.parents.iter() {
            self.roots.remove(p);
        }
        self.roots.insert(moved_message.hash);
        Ok({})
    }

    // Takes ownership of the payload.
    pub fn create_root_message(&mut self, payload: types::Payload) {
        // Check if this is leader's commit.
        let is_commit = self.epoch_leader(self.current_epoch) == self.owner_uid
            && !self.epoch_commits.contains_key(&self.current_epoch);
        let message = DAG::create_message(
            self.owner_uid,
            self.roots.iter().cloned().collect(),
            self.current_epoch,
            is_commit,
            payload
        );
        self.update_state(&message);
        let moved_message = self.messages.insert(message.hash, message)
            .expect("Hash collision: old message already has the same hash as the new one.");
        self.roots.clear();
        self.roots.insert(moved_message.hash);
    }
}
