#[macro_use]
extern crate bencher;

use bencher::Bencher;
use near_primitives::shard_layout::ShardUId;
use near_store::Trie;
use near_store::test_utils::TestTriesBuilder;
use near_store::trie::AccessOptions;
use rand::random;

fn rand_bytes() -> Vec<u8> {
    (0..10).map(|_| random::<u8>()).collect()
}

fn trie_lookup(bench: &mut Bencher) {
    let (changed_keys, trie) = {
        let tries = TestTriesBuilder::new().build();

        let trie = tries.get_trie_for_shard(ShardUId::single_shard(), Trie::EMPTY_ROOT);
        let mut changes = vec![];
        for _ in 0..100 {
            changes.push((rand_bytes(), Some(rand_bytes())));
        }
        let changed_keys =
            changes.iter().map(|(key, _value)| key.clone()).collect::<Vec<Vec<u8>>>();
        let trie_changes = trie.update(changes, AccessOptions::DEFAULT).unwrap();
        let mut state_update = tries.store_update();
        let root = tries.apply_all(&trie_changes, ShardUId::single_shard(), &mut state_update);
        state_update.commit().expect("Failed to commit");

        let trie = tries.get_trie_for_shard(ShardUId::single_shard(), root);
        (changed_keys, trie)
    };

    bench.iter(|| {
        for _ in 0..1 {
            for key in &changed_keys {
                trie.get(key, AccessOptions::DEFAULT).unwrap();
            }
        }
    });
}

fn trie_update(bench: &mut Bencher) {
    let tries = TestTriesBuilder::new().build();
    let trie = tries.get_trie_for_shard(ShardUId::single_shard(), Trie::EMPTY_ROOT);
    let mut changes = vec![];
    for _ in 0..100 {
        changes.push((rand_bytes(), Some(rand_bytes())));
    }

    bench.iter(|| {
        let _ = trie.update(changes.iter().cloned(), AccessOptions::DEFAULT);
    });
}

benchmark_group!(benches, trie_lookup, trie_update);
benchmark_main!(benches);
