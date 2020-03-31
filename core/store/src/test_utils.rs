use std::collections::HashMap;
use std::sync::Arc;

use rand::seq::SliceRandom;
use rand::Rng;

use crate::db::TestDB;
use crate::trie::Trie;
use crate::Store;

/// Creates an in-memory database.
pub fn create_test_store() -> Arc<Store> {
    let db = Arc::new(TestDB::new());
    Arc::new(Store::new(db))
}

/// Creates a Trie using an in-memory database.
pub fn create_trie() -> Arc<Trie> {
    let store = create_test_store();
    Arc::new(Trie::new(store))
}

pub fn gen_changes(rng: &mut impl Rng, max_size: usize) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
    let alphabet = &b"abcdefgh"[0..rng.gen_range(2, 8)];
    let max_length = rng.gen_range(2, 8);

    let mut state: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let mut result = Vec::new();
    let delete_probability = rng.gen_range(0.1, 0.5);
    let size = rng.gen_range(0, max_size) + 1;
    for _ in 0..size {
        let key_length = rng.gen_range(1, max_length);
        let key: Vec<u8> = (0..key_length).map(|_| alphabet.choose(rng).unwrap().clone()).collect();

        let delete = rng.gen_range(0.0, 1.0) < delete_probability;
        if delete {
            let mut keys: Vec<_> = state.keys().cloned().collect();
            keys.push(key);
            let key = keys.choose(rng).unwrap().clone();
            state.remove(&key);
            result.push((key.clone(), None));
        } else {
            let value_length = rng.gen_range(1, max_length);
            let value: Vec<u8> =
                (0..value_length).map(|_| alphabet.choose(rng).unwrap().clone()).collect();
            result.push((key.clone(), Some(value.clone())));
            state.insert(key, value);
        }
    }
    result
}

#[cfg(test)]
pub(crate) fn simplify_changes(
    changes: &Vec<(Vec<u8>, Option<Vec<u8>>)>,
) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
    let mut state: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    for (key, value) in changes.iter() {
        if let Some(value) = value {
            state.insert(key.clone(), value.clone());
        } else {
            state.remove(key);
        }
    }
    let mut result: Vec<_> = state.into_iter().map(|(k, v)| (k, Some(v))).collect();
    result.sort();
    result
}
