#[macro_use]
extern crate bencher;
extern crate rand;
extern crate storage;

use bencher::Bencher;
use rand::random;

use storage::test_utils::create_trie;
use storage::Trie;


fn rand_bytes() -> Vec<u8> {
    (0..10).map(|_| random::<u8>()).collect()
}

fn trie_lookup(bench: &mut Bencher) {
    let trie = create_trie();
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
    let trie = create_trie();
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
