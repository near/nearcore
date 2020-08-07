use std::collections::HashMap;
use std::sync::Arc;

use rand::seq::SliceRandom;
use rand::Rng;

use crate::db::TestDB;
use crate::trie::ShardTries;
use crate::{ShardTriesSnapshot, Store, TrieCaches};
use near_primitives::hash::CryptoHash;
use near_primitives::types::ShardId;

pub trait ShardTriesTestUtils {
    fn snapshot(&self) -> ShardTriesSnapshot;
}

impl ShardTriesTestUtils for ShardTries {
    fn snapshot(&self) -> ShardTriesSnapshot {
        ShardTriesSnapshot::new(self.store.clone(), self.caches.clone())
    }
}

/// Creates an in-memory database.
pub fn create_test_store() -> Store {
    let db = Arc::pin(TestDB::new());
    Store::new(db)
}

/// Creates a Trie using an in-memory database.
pub fn create_tries() -> ShardTries {
    let store = create_test_store();
    ShardTries::new(store.clone(), TrieCaches::new(1))
}

pub fn test_populate_trie(
    tries: &ShardTries,
    root: &CryptoHash,
    shard_id: ShardId,
    changes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
) -> CryptoHash {
    let trie = tries.snapshot().get_trie_for_shard(shard_id, *root);
    assert_eq!(trie.storage.as_caching_storage().unwrap().shard_id, 0);
    let trie_changes = trie.update(changes.iter().cloned()).unwrap();
    let (store_update, root) = tries.apply_all(&trie_changes, 0).unwrap();
    store_update.commit().unwrap();
    let deduped = simplify_changes(&changes);
    let trie = tries.snapshot().get_trie_for_shard(shard_id, root);
    for (key, value) in deduped {
        assert_eq!(trie.get(&key), Ok(value));
    }
    root
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

#[cfg(test)]
mod tests {
    use crate::create_store;
    use crate::db::DBCol::ColState;

    #[test]
    fn snapshot_sanity() {
        let tmp_dir = tempfile::Builder::new().prefix("_test_snapshot_sanity").tempdir().unwrap();
        let store = create_store(tmp_dir.path().to_str().unwrap());
        let snapshot1 = store.storage.clone().get_snapshot();
        let snapshot2 = {
            let mut store_update = store.store_update();
            store_update.set(ColState, &[1], &[1]);
            store_update.commit().unwrap();
            store.storage.clone().get_snapshot()
        };
        let snapshot3 = {
            let mut store_update = store.store_update();
            store_update.set(ColState, &[1], &[2]);
            store_update.commit().unwrap();
            store.storage.clone().get_snapshot()
        };
        let snapshot4 = {
            let mut store_update = store.store_update();
            store_update.delete(ColState, &[1]);
            store_update.commit().unwrap();
            store.storage.clone().get_snapshot()
        };

        assert_eq!(snapshot1.get(ColState, &[1]).unwrap(), None);
        assert_eq!(snapshot2.get(ColState, &[1]).unwrap(), Some(vec![1]));
        assert_eq!(snapshot3.get(ColState, &[1]).unwrap(), Some(vec![2]));
        assert_eq!(snapshot4.get(ColState, &[1]).unwrap(), None);
    }
}
