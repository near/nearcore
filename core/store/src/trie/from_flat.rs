use crate::flat::{store_helper, FlatStorageManager};
use crate::{ShardTries, Store, Trie, TrieConfig, TrieDBStorage, TrieStorage};
use near_primitives::{shard_layout::ShardUId, state::FlatStateValue};
use std::time::Instant;

pub fn construct_trie_from_flat(store: Store, write_store: Store, shard_uid: ShardUId) {
    let timer = Instant::now();
    let trie_storage = TrieDBStorage::new(store.clone(), shard_uid);

    let mut entries =
        store_helper::iter_flat_state_entries(shard_uid, &store, None, None).map(|entry| {
            let (key, value) = entry.unwrap();
            let value = match value {
                FlatStateValue::Ref(ref_value) => {
                    trie_storage.retrieve_raw_bytes(&ref_value.hash).unwrap().to_vec()
                }
                FlatStateValue::Inlined(inline_value) => inline_value,
            };
            (key, value)
        });

    // new ShardTries for write storage location
    let tries = ShardTries::new(
        write_store.clone(),
        TrieConfig::default(),
        &[shard_uid],
        FlatStorageManager::new(write_store),
    );
    let mut trie_root = Trie::EMPTY_ROOT;

    while let Some(batch) = get_batch(&mut entries) {
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

fn get_batch(
    entry_itr: &mut impl Iterator<Item = (Vec<u8>, Vec<u8>)>,
) -> Option<Vec<(Vec<u8>, Option<Vec<u8>>)>> {
    let size_limit = 500 * 1000_000; // 500 MB
    let mut size = 0;
    let mut entries = Vec::new();
    while let Some((key, value)) = entry_itr.next() {
        size += key.len() + value.len();
        entries.push((key, Some(value)));
        if size > size_limit {
            break;
        }
    }
    (!entries.is_empty()).then_some(entries)
}
