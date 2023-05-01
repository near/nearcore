use borsh::BorshDeserialize;
use near_primitives::hash::CryptoHash;

use crate::{DBCol, Store, TrieDBStorage, TrieStorage};

use super::store_helper::decode_flat_state_db_key;
use super::types::INLINE_DISK_VALUE_THRESHOLD;
use super::FlatStateValue;

pub fn inline_flat_state_values(store: &Store, read_value_threads: usize) {
    let (send, recv) = crossbeam::channel::bounded(2 * read_value_threads);
    let mut join_handles = Vec::new();
    for _ in 0..read_value_threads {
        join_handles.push(spawn_read_value_thread(store.clone(), recv.clone()));
    }
    for entry in store.iter(DBCol::FlatState) {
        let (key, value_bytes) = entry.unwrap();
        if let FlatStateValue::Ref(value_ref) =
            FlatStateValue::try_from_slice(&value_bytes).unwrap()
        {
            if value_ref.length as usize <= INLINE_DISK_VALUE_THRESHOLD {
                send.send((key, value_ref.hash)).unwrap();
            }
        }
    }
    for join_handle in join_handles {
        join_handle.join().unwrap();
    }
}

fn spawn_read_value_thread(
    store: Store,
    recv: crossbeam::channel::Receiver<(Box<[u8]>, CryptoHash)>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || loop {
        match recv.recv() {
            Ok((key, value)) => inline_value(&store, &key, &value),
            Err(_) => return,
        }
    })
}

fn inline_value(store: &Store, key: &[u8], value_hash: &CryptoHash) {
    let shard_uid = decode_flat_state_db_key(key).unwrap().0;
    let storage = TrieDBStorage::new(store.clone(), shard_uid);
    let value_bytes = storage.retrieve_raw_bytes(value_hash).unwrap();
    let mut store_update = store.store_update();
}
