#[macro_use]
extern crate bencher;

use bencher::Bencher;
use rand::{random, Rng};
use std::time::Instant;

use near_primitives::shard_layout::ShardUId;
use near_store::test_utils::create_tries;
use near_store::Trie;

fn rand_bytes() -> Vec<u8> {
    (0..10).map(|_| random::<u8>()).collect()
}

fn rand_nibbles() -> Vec<u8> {
    (0..10)
        .map(|_| rand::thread_rng().gen_range(0, 2) + 16 * rand::thread_rng().gen_range(0, 2))
        .collect()
}

fn long_rand_bytes() -> Vec<u8> {
    (0..1000).map(|_| random::<u8>()).collect()
}

fn trie_lookup(bench: &mut Bencher) {
    let tries = create_tries();
    let trie = tries.get_trie_for_shard(ShardUId::single_shard());
    let root = Trie::empty_root();
    let mut changes = vec![];
    for _ in 0..3000 {
        changes.push((rand_nibbles(), Some(long_rand_bytes())));
    }
    let other_changes = changes.clone();
    let trie_changes = trie.update(&root, changes.drain(..)).unwrap();
    let (state_update, root) = tries.apply_all(&trie_changes, ShardUId::single_shard()).unwrap();
    state_update.commit().expect("Failed to commit");

    let f = || {
        for (key, _) in other_changes.iter() {
            trie.get(&root, key).unwrap();
        }
    };
    bench.iter(move || {
        let start = Instant::now();
        f();
        let took = start.elapsed();
        println!("took {:?}", took);
    });
}

fn trie_update(bench: &mut Bencher) {
    let tries = create_tries();
    let trie = tries.get_trie_for_shard(ShardUId::single_shard());
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
