#[macro_use]
extern crate bencher;

use bencher::Bencher;
use rand::random;

use near_primitives::shard_layout::ShardUId;
use near_store::test_utils::create_tries;
use near_store::Trie;

fn rand_bytes() -> Vec<u8> {
    (0..10).map(|_| random::<u8>()).collect()
}

fn trie_lookup(bench: &mut Bencher) {
    let tries = create_tries();
    let mut trie = tries.get_trie_for_shard(ShardUId::single_shard(), Trie::EMPTY_ROOT);
    let mut changes = vec![];
    for _ in 0..100 {
        changes.push((rand_bytes(), Some(rand_bytes())));
    }
    let other_changes = changes.clone();
    let trie_changes = trie.update(changes.drain(..)).unwrap();
    let (state_update, root) = tries.apply_all(&trie_changes, ShardUId::single_shard());
    state_update.commit().expect("Failed to commit");
    trie.set_root(root);

    bench.iter(|| {
        for _ in 0..1 {
            for (key, _) in other_changes.iter() {
                trie.get(key).unwrap();
            }
        }
    });
}

fn trie_update(bench: &mut Bencher) {
    let tries = create_tries();
    let trie = tries.get_trie_for_shard(ShardUId::single_shard(), Trie::EMPTY_ROOT);
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
