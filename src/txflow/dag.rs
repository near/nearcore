use std::collections::{HashMap, HashSet};
use std::collections::hash_map::Entry;

use super::types;

pub struct DAG {
    messages: HashMap<u64, types::SignedMessage>,  // Message hash -> Message.
    roots: HashSet<u64>,  // Message hashes.
    // Epoch -> set of owner_uid that have messages with
    epoch_counter: HashMap<u64, HashSet<u64>>,
}


impl DAG {

    // Takes ownership of the payload.
    fn create_message(epoch: u64, owner_uid: u64, parents: Vec<u64>, payload: types::Payload) {

    }

    fn update_state(&mut self, message: &types::SignedMessage) {

    }

    // Takes ownership of the message.
    pub fn add_existing_message(&mut self, message: types::SignedMessage) {
        if let None = self.messages.get(&message.hash) {
            self.update_state(&message);
            self.messages.insert(message.hash, message);
        }
    }
    pub fn create_root_message(&mut self, payload: &types::Payload) {

    }
}
