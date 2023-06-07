use crate::flat::{store_helper, FlatStorageManager};
use crate::{ShardTries, Store, Trie, TrieConfig, TrieDBStorage, TrieStorage};
use near_primitives::{shard_layout::ShardUId, state::FlatStateValue};
use std::time::Instant;

pub fn construct_trie_from_flat(store: Store, write_store: Store, shard_uid: ShardUId) {
    let timer = Instant::now();
    let trie_storage = TrieDBStorage::new(store.clone(), shard_uid);
    let mut num_entries = 0;
    let entries =
        store_helper::iter_flat_state_entries(shard_uid, &store, None, None).map(|entry| {
            let (key, value) = entry.unwrap();
            let value = match value {
                FlatStateValue::Ref(ref_value) => {
                    trie_storage.retrieve_raw_bytes(&ref_value.hash).unwrap().to_vec()
                }
                FlatStateValue::Inlined(inline_value) => inline_value,
            };

            num_entries += 1;
            if num_entries % 500 == 0 {
                println!("Processed {} entries at time {:?}", num_entries, timer.elapsed());
            }

            (key, Some(value))
        });

    let tries = ShardTries::new(
        write_store.clone(),
        TrieConfig::default(),
        &[shard_uid],
        FlatStorageManager::new(write_store.clone()),
    );
    let new_trie = tries.get_trie_for_shard(shard_uid, Trie::EMPTY_ROOT);
    let trie_changes = new_trie.update(entries).unwrap();

    println!(
        "Done processing {} entries. New root {} processed at time {:?}",
        num_entries,
        trie_changes.new_root,
        timer.elapsed()
    );

    let mut store_update = tries.store_update();
    tries.apply_all(&trie_changes, shard_uid, &mut store_update);
    store_update.commit().unwrap();

    println!("Committed at time {:?}", timer.elapsed());
}
