#[macro_use]
extern crate bencher;
extern crate rand;

use bencher::Bencher;

extern crate storage;

use std::sync::Arc;
use storage::test_utils::create_memory_db;
use storage::{KeyValueDB, Trie};

use rand::random;

fn rand_bytes() -> Vec<u8> {
    (0..10).map(|_| random::<u8>()).collect()
}

fn trie_lookup(bench: &mut Bencher) {
    let storage: Arc<KeyValueDB> = Arc::new(create_memory_db());
    let trie = Trie::new(storage.clone());
    let root = Trie::empty_root();
    let mut changes = vec![];
    for _ in 0..100 {
        changes.push((rand_bytes(), Some(rand_bytes())));
    }
    let other_changes = changes.clone();
    let (db_changes, root) = trie.update(&root, changes.drain(..));
    trie.apply_changes(db_changes).expect("Failed to commit");

    bench.iter(|| {
        for _ in 0..1 {
            for (key, _) in other_changes.iter() {
                trie.get(&root, &key).unwrap();
            }
        }
    });
}

fn trie_update(bench: &mut Bencher) {
    let storage: Arc<KeyValueDB> = Arc::new(create_memory_db());
    let trie = Trie::new(storage.clone());
    let root = Trie::empty_root();
    let mut changes = vec![];
    for _ in 0..100 {
        changes.push((rand_bytes(), Some(rand_bytes())));
    }

    bench.iter(|| {
        let mut this_changes = changes.clone();
        let (_, _) = trie.update(&root, this_changes.drain(..));
    });
}

benchmark_group!(benches, trie_lookup, trie_update);
benchmark_main!(benches);
