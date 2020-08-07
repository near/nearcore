#[macro_use]
extern crate bencher;

use bencher::Bencher;
use rand::random;

use near_store::test_utils::{create_tries, ShardTriesTestUtils};
use near_store::Trie;

fn rand_bytes() -> Vec<u8> {
    (0..10).map(|_| random::<u8>()).collect()
}

fn trie_lookup(bench: &mut Bencher) {
    let tries = create_tries();
    let root = Trie::empty_root();
    let mut changes = vec![];
    for _ in 0..100 {
        changes.push((rand_bytes(), Some(rand_bytes())));
    }
    let other_changes = changes.clone();
    let trie = tries.snapshot().get_trie_for_shard(0, root);
    let trie_changes = trie.update(changes.drain(..)).unwrap();
    let (state_update, root) = tries.apply_all(&trie_changes, 0).unwrap();
    state_update.commit().expect("Failed to commit");

    let trie = tries.snapshot().get_trie_for_shard(0, root);
    bench.iter(|| {
        for _ in 0..1 {
            for (key, _) in other_changes.iter() {
                trie.get(&key).unwrap();
            }
        }
    });
}

fn trie_update(bench: &mut Bencher) {
    let tries = create_tries();
    let trie = tries.snapshot().get_trie_for_shard(0, Trie::empty_root());
    let mut changes = vec![];
    for _ in 0..100 {
        changes.push((rand_bytes(), Some(rand_bytes())));
    }

    bench.iter(|| {
        let mut this_changes = changes.clone();
        let _ = trie.update(this_changes.drain(..));
    });
}

benchmark_group!(benches, trie_lookup, trie_update);
benchmark_main!(benches);
