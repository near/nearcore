#[macro_use]
extern crate bencher;

use bencher::Bencher;
use rand::random;

use near_store::test_utils::create_trie;
use near_store::Trie;

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
    let (state_update, root) = trie.update(&root, changes.drain(..)).unwrap().into(trie.clone()).unwrap();
    state_update.commit().expect("Failed to commit");

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
        let _ = trie.update(&root, this_changes.drain(..));
    });
}

benchmark_group!(benches, trie_lookup, trie_update);
benchmark_main!(benches);
