use crate::flat::{store_helper, FlatStorageError, FlatStorageManager};
use crate::{ShardTries, Store, Trie, TrieConfig, TrieDBStorage, TrieStorage};
use near_primitives::{shard_layout::ShardUId, state::FlatStateValue};
use std::time::Instant;

// This function creates a new trie from flat storage for a given shard_uid
// store: location of RocksDB store from where we read flatstore
// write_store: location of RocksDB store where we write the newly constructred trie
// shard_uid: The shard which we are recreating
//
// Please note that the trie is created for the block state with height equal to flat_head
// flat state can comtain deltas after flat_head and can be different from tip of the blockchain.
pub fn construct_trie_from_flat(store: Store, write_store: Store, shard_uid: ShardUId) {
    let trie_storage = TrieDBStorage::new(store.clone(), shard_uid);
    let flat_state_to_trie_kv =
        |entry: Result<(Vec<u8>, FlatStateValue), FlatStorageError>| -> (Vec<u8>, Vec<u8>) {
            let (key, value) = entry.unwrap();
            let value = match value {
                FlatStateValue::Ref(ref_value) => {
                    trie_storage.retrieve_raw_bytes(&ref_value.hash).unwrap().to_vec()
                }
                FlatStateValue::Inlined(inline_value) => inline_value,
            };
            (key, value)
        };

    let mut iter = store_helper::iter_flat_state_entries(shard_uid, &store, None, None)
        .map(flat_state_to_trie_kv);

    // new ShardTries for write storage location
    let tries = ShardTries::new(
        write_store.clone(),
        TrieConfig::default(),
        &[shard_uid],
        FlatStorageManager::new(write_store),
    );
    let mut trie_root = Trie::EMPTY_ROOT;

    let timer = Instant::now();
    while let Some(batch) = get_trie_update_batch(&mut iter) {
        // Apply and commit changes
        let batch_size = batch.len();
        let new_trie = tries.get_trie_for_shard(shard_uid, trie_root);
        let trie_changes = new_trie.update(batch).unwrap();
        let mut store_update = tries.store_update();
        tries.apply_all(&trie_changes, shard_uid, &mut store_update);
        store_update.commit().unwrap();
        trie_root = trie_changes.new_root;

        println!("{:.2?} : Processed {} entries", timer.elapsed(), batch_size);
    }

    println!("{:.2?} : Completed building trie with root {}", timer.elapsed(), trie_root);
}

fn get_trie_update_batch(
    iter: &mut impl Iterator<Item = (Vec<u8>, Vec<u8>)>,
) -> Option<Vec<(Vec<u8>, Option<Vec<u8>>)>> {
    let size_limit = 500 * 1000_000; // 500 MB
    let mut size = 0;
    let mut entries = Vec::new();
    while let Some((key, value)) = iter.next() {
        size += key.len() + value.len();
        entries.push((key, Some(value)));
        if size > size_limit {
            break;
        }
    }
    (!entries.is_empty()).then_some(entries)
}
