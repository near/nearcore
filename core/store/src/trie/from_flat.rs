use crate::flat::{store_helper, FlatStorageManager};
use crate::{ShardTries, Store, Trie, TrieConfig};
use near_primitives::{shard_layout::ShardUId, state::FlatStateValue};

pub fn construct_trie_from_flat(store: Store, shard_uid: ShardUId) {
    let new_shard_uid = ShardUId { version: shard_uid.version, shard_id: 10 };
    let trie_config = TrieConfig::default();
    let flat_storage_manager = FlatStorageManager::new(store.clone());
    let tries = ShardTries::new(
        store.clone(),
        trie_config,
        &[shard_uid, new_shard_uid],
        flat_storage_manager,
    );
    let trie = tries.get_view_trie_for_shard(shard_uid, Trie::EMPTY_ROOT);

    let entries =
        store_helper::iter_flat_state_entries(shard_uid, &store, None, None).map(|entry| {
            let (key, value) = entry.unwrap();
            let value = match value {
                FlatStateValue::Ref(ref_value) => {
                    trie.storage.retrieve_raw_bytes(&ref_value.hash).unwrap().to_vec()
                }
                FlatStateValue::Inlined(inline_value) => inline_value,
            };
            println!("KV {} : {}", String::from_utf8_lossy(&key), value.len());
            (key, Some(value))
        });

    let mut store_update = tries.store_update();
    let new_trie = tries.get_trie_for_shard(new_shard_uid, Trie::EMPTY_ROOT);
    let trie_changes = new_trie.update(entries).unwrap();

    println!("Old root {}", trie_changes.old_root);
    println!("New root {}", trie_changes.new_root);

    tries.apply_all(&trie_changes, new_shard_uid, &mut store_update);
    store_update.commit().unwrap();

    println!("Committed");
}
