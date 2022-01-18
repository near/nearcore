use std::sync::Arc;

use crate::chain::Chain;
use crate::test_utils::KeyValueRuntime;
use crate::types::{ChainGenesis, Tip};
use crate::DoomslugThresholdMode;
use near_crypto::KeyType;
use near_primitives::block::Block;
use near_primitives::merkle::PartialMerkleTree;
use near_primitives::shard_layout::ShardUId;
use near_primitives::types::{NumBlocks, NumShards, StateRoot};
use near_primitives::validator_signer::InMemoryValidatorSigner;
use near_store::test_utils::{create_test_store, gen_changes};
use near_store::{ShardTries, StoreUpdate, Trie, WrappedTrieChanges};
use rand::Rng;

fn get_chain(num_shards: NumShards) -> Chain {
    get_chain_with_epoch_length_and_num_shards(10, num_shards)
}

fn get_chain_with_epoch_length_and_num_shards(
    epoch_length: NumBlocks,
    num_shards: NumShards,
) -> Chain {
    let store = create_test_store();
    let chain_genesis = ChainGenesis::test();
    let validators = vec![vec!["test1"]];
    let runtime_adapter = Arc::new(KeyValueRuntime::new_with_validators(
        store,
        validators
            .into_iter()
            .map(|inner| inner.into_iter().map(|account_id| account_id.parse().unwrap()).collect())
            .collect(),
        1,
        num_shards,
        epoch_length,
    ));
    Chain::new(runtime_adapter, &chain_genesis, DoomslugThresholdMode::NoApprovals).unwrap()
}

// Build a chain of num_blocks on top of prev_block
fn do_fork(
    mut prev_block: Block,
    mut prev_state_roots: Vec<StateRoot>,
    tries: ShardTries,
    chain: &mut Chain,
    num_blocks: u64,
    states: &mut Vec<(Block, Vec<StateRoot>, Vec<Vec<(Vec<u8>, Option<Vec<u8>>)>>)>,
    max_changes: usize,
    verbose: bool,
) {
    let mut rng = rand::thread_rng();
    let signer = Arc::new(InMemoryValidatorSigner::from_seed(
        "test1".parse().unwrap(),
        KeyType::ED25519,
        "test1",
    ));
    let num_shards = prev_state_roots.len() as u64;
    let runtime_adapter = chain.runtime_adapter.clone();
    for i in 0..num_blocks {
        runtime_adapter
            .get_next_epoch_id_from_prev_block(prev_block.hash())
            .expect("block must exist");
        let block = Block::empty(&prev_block, &*signer);

        let head = chain.head().unwrap();
        let mut store_update = chain.mut_store().store_update();
        if i == 0 {
            store_update.save_block_merkle_tree(*prev_block.hash(), PartialMerkleTree::default());
        }
        store_update.save_block(block.clone());
        store_update.inc_block_refcount(block.header().prev_hash()).unwrap();
        store_update.save_block_header(block.header().clone()).unwrap();
        let tip = Tip::from_header(block.header());
        if head.height < tip.height {
            store_update.save_head(&tip).unwrap();
        }

        let mut trie_changes_shards = Vec::new();
        for shard_id in 0..num_shards {
            let shard_uid = ShardUId { version: 0, shard_id: shard_id as u32 };
            let trie_changes_data = gen_changes(&mut rng, max_changes);
            let state_root = prev_state_roots[shard_id as usize];
            let trie = tries.get_trie_for_shard(shard_uid);
            let trie_changes = trie.update(&state_root, trie_changes_data.iter().cloned()).unwrap();
            if verbose {
                println!("state new {:?} {:?}", block.header().height(), trie_changes_data);
            }

            let new_root = trie_changes.new_root;
            let wrapped_trie_changes = WrappedTrieChanges::new(
                tries.clone(),
                shard_uid,
                trie_changes,
                Default::default(),
                *block.hash(),
            );
            store_update.save_trie_changes(wrapped_trie_changes);

            prev_state_roots[shard_id as usize] = new_root;
            trie_changes_shards.push(trie_changes_data);
        }
        store_update.commit().unwrap();
        states.push((block.clone(), prev_state_roots.clone(), trie_changes_shards));
        prev_block = block.clone();
    }
}

// This test infrastructure do the following:
// Build Chain 1 from the full data, then GC it.
// Build Chain 2 from the data removing everything that should be removed after GC.
// Make sure that Chain 1 == Chain 2.
fn gc_fork_common(simple_chains: Vec<SimpleChain>, max_changes: usize) {
    // Running the test
    println!(
        "Running gc test: max_changes per block = {:?}, simple_chains_len = {:?} simple_chains = {:?}",
        max_changes,
        simple_chains.len(),
        simple_chains
    );
    let verbose = false;

    let num_shards = rand::thread_rng().gen_range(1, 3);

    // Init Chain 1
    let mut chain1 = get_chain(num_shards);
    let tries1 = chain1.runtime_adapter.get_tries();
    let mut rng = rand::thread_rng();
    let shard_to_check_trie = rng.gen_range(0, num_shards);
    let shard_uid = ShardUId { version: 0, shard_id: shard_to_check_trie as u32 };
    let trie1 = tries1.get_trie_for_shard(shard_uid);
    let genesis1 = chain1.get_block_by_height(0).unwrap().clone();
    let mut states1 = vec![];
    states1.push((
        genesis1,
        vec![Trie::empty_root(); num_shards as usize],
        vec![Vec::new(); num_shards as usize],
    ));

    for simple_chain in simple_chains.iter() {
        let (source_block1, state_root1, _) = states1[simple_chain.from as usize].clone();
        do_fork(
            source_block1.clone(),
            state_root1,
            tries1.clone(),
            &mut chain1,
            simple_chain.length,
            &mut states1,
            max_changes,
            verbose,
        );
    }

    // GC execution
    let clear_data = chain1.clear_data(tries1, 100);
    if clear_data.is_err() {
        println!("clear data failed = {:?}", clear_data);
        assert!(false);
    }

    let mut chain2 = get_chain(num_shards);
    let tries2 = chain2.runtime_adapter.get_tries();
    let trie2 = tries2.get_trie_for_shard(shard_uid);

    // Find gc_height
    let mut gc_height = simple_chains[0].length - 51;
    for (i, simple_chain) in simple_chains.iter().enumerate() {
        if (!simple_chain.is_removed) && gc_height > simple_chain.from && i > 0 {
            gc_height = simple_chain.from
        }
    }
    if verbose {
        println!("GC height = {:?}", gc_height);
    }

    let mut start_index = 1; // zero is for genesis
    let mut state_roots2 = vec![];
    state_roots2.push(Trie::empty_root());

    for simple_chain in simple_chains.iter() {
        if simple_chain.is_removed {
            for _ in 0..simple_chain.length {
                // This chain is deleted in Chain1
                state_roots2.push(Trie::empty_root());
            }
            start_index += simple_chain.length;
            continue;
        }

        let mut state_root2 = state_roots2[simple_chain.from as usize];
        let state_root1 = states1[simple_chain.from as usize].1[shard_to_check_trie as usize];
        assert!(trie1.iter(&state_root1).is_ok());
        assert_eq!(state_root1, state_root2);

        for i in start_index..start_index + simple_chain.length {
            let mut store_update2 = chain2.mut_store().store_update();
            let (block1, state_root1, changes1) = states1[i as usize].clone();
            // Apply to Trie 2 the same changes (changes1) as applied to Trie 1
            let trie_changes2 = trie2
                .update(&state_root2, changes1[shard_to_check_trie as usize].iter().cloned())
                .unwrap();
            // i == gc_height is the only height should be processed here
            if block1.header().height() > gc_height || i == gc_height {
                let mut trie_store_update2 = StoreUpdate::new_with_tries(tries2.clone());
                tries2
                    .apply_insertions(&trie_changes2, shard_uid, &mut trie_store_update2)
                    .unwrap();
                state_root2 = trie_changes2.new_root;
                assert_eq!(state_root1[shard_to_check_trie as usize], state_root2);
                store_update2.merge(trie_store_update2);
            } else {
                let (trie_store_update2, new_root2) =
                    tries2.apply_all(&trie_changes2, shard_uid).unwrap();
                state_root2 = new_root2;
                store_update2.merge(trie_store_update2);
            }
            state_roots2.push(state_root2);
            store_update2.commit().unwrap();
        }
        start_index += simple_chain.length;
    }

    let mut start_index = 1; // zero is for genesis
    for simple_chain in simple_chains.iter() {
        if simple_chain.is_removed {
            start_index += simple_chain.length;
            continue;
        }
        for i in start_index..start_index + simple_chain.length {
            let (block1, state_root1, _) = states1[i as usize].clone();
            let state_root1 = state_root1[shard_to_check_trie as usize];
            if block1.header().height() > gc_height || i == gc_height {
                assert!(trie1.iter(&state_root1).is_ok());
                assert!(trie2.iter(&state_root1).is_ok());
                let a = trie1
                    .iter(&state_root1)
                    .unwrap()
                    .map(|item| item.unwrap().0)
                    .collect::<Vec<_>>();
                let b = trie2
                    .iter(&state_root1)
                    .unwrap()
                    .map(|item| item.unwrap().0)
                    .collect::<Vec<_>>();
                assert_eq!(a, b);
            }
        }
        start_index += simple_chain.length;
    }
}

// from is an index in blocks array, length is the number of blocks in a chain on top of blocks[from],
// is_removed = should this chain be removed after GC
#[derive(Debug, Clone)]
pub struct SimpleChain {
    from: u64,
    length: u64,
    is_removed: bool,
}

// This test creates only chain
#[test]
fn test_gc_remove_simple_chain_sanity() {
    for max_changes in 1..=20 {
        let chains = vec![SimpleChain { from: 0, length: 101, is_removed: false }];
        // remove 50 blocks, height = 50 will be the earliest one which exists
        gc_fork_common(chains, max_changes);
    }
}

// Creates simple shorter fork and GCs it
fn test_gc_remove_fork_common(max_changes_limit: usize) {
    for max_changes in 1..=max_changes_limit {
        for fork_length in 1..=10 {
            let chains = vec![
                SimpleChain { from: 0, length: 101, is_removed: false },
                SimpleChain { from: 10, length: fork_length, is_removed: true },
            ];
            gc_fork_common(chains, max_changes);
        }
        for fork_length in 30..=40 {
            let chains = vec![
                SimpleChain { from: 0, length: 101, is_removed: false },
                SimpleChain { from: 10, length: fork_length, is_removed: true },
            ];
            gc_fork_common(chains, max_changes);
        }
    }
}

#[test]
fn test_gc_remove_fork_small() {
    test_gc_remove_fork_common(1)
}

#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn test_gc_remove_fork_large() {
    test_gc_remove_fork_common(20)
}

#[test]
fn test_gc_remove_fork_fail_often() {
    for _tries in 0..10 {
        let chains = vec![
            SimpleChain { from: 0, length: 101, is_removed: false },
            SimpleChain { from: 10, length: 35, is_removed: true },
        ];
        gc_fork_common(chains, 1);
        let chains = vec![
            SimpleChain { from: 0, length: 101, is_removed: false },
            SimpleChain { from: 10, length: 80, is_removed: false },
        ];
        gc_fork_common(chains, 6);
    }
}

// Creates simple shorter fork and NOT GCs it
fn test_gc_not_remove_fork_common(max_changes_limit: usize) {
    for max_changes in 1..=max_changes_limit {
        for fork_length in 41..=50 {
            let chains = vec![
                SimpleChain { from: 0, length: 101, is_removed: false },
                SimpleChain { from: 10, length: fork_length, is_removed: false },
            ];
            gc_fork_common(chains, max_changes);
        }
        for fork_length in 80..=90 {
            let chains = vec![
                SimpleChain { from: 0, length: 101, is_removed: false },
                SimpleChain { from: 10, length: fork_length, is_removed: false },
            ];
            gc_fork_common(chains, max_changes);
        }
    }
}

#[test]
fn test_gc_not_remove_fork_small() {
    test_gc_not_remove_fork_common(1)
}

#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn test_gc_not_remove_fork_large() {
    test_gc_not_remove_fork_common(20)
}

// Creates simple longer fork and NOT GCs it
#[test]
fn test_gc_not_remove_longer_fork() {
    for fork_length in 91..=100 {
        let chains = vec![
            SimpleChain { from: 0, length: 101, is_removed: false },
            SimpleChain { from: 10, length: fork_length, is_removed: false },
        ];
        gc_fork_common(chains, 1);
    }
}

// This test creates forks from genesis
#[test]
fn test_gc_forks_from_genesis() {
    for fork_length in 1..=10 {
        let chains = vec![
            SimpleChain { from: 0, length: 101, is_removed: false },
            SimpleChain { from: 0, length: fork_length, is_removed: true },
        ];
        gc_fork_common(chains, 1);
    }
    for fork_length in 45..=50 {
        let chains = vec![
            SimpleChain { from: 0, length: 101, is_removed: false },
            SimpleChain { from: 0, length: fork_length, is_removed: true },
        ];
        gc_fork_common(chains, 1);
    }
    for fork_length in 51..=55 {
        let chains = vec![
            SimpleChain { from: 0, length: 101, is_removed: false },
            SimpleChain { from: 0, length: fork_length, is_removed: false },
        ];
        gc_fork_common(chains, 1);
    }
    for fork_length in 0..=10 {
        let chains = vec![
            SimpleChain { from: 0, length: 101, is_removed: false },
            SimpleChain { from: 0, length: 51 + fork_length, is_removed: false },
            SimpleChain { from: 0, length: fork_length, is_removed: true },
            SimpleChain { from: 0, length: 50 - fork_length, is_removed: true },
        ];
        gc_fork_common(chains, 1);
    }
}

#[test]
fn test_gc_overlap() {
    for max_changes in 1..=20 {
        let chains = vec![
            SimpleChain { from: 0, length: 101, is_removed: false },
            SimpleChain { from: 10, length: 70, is_removed: false },
            SimpleChain { from: 20, length: 25, is_removed: true },
            SimpleChain { from: 30, length: 30, is_removed: false },
            SimpleChain { from: 40, length: 1, is_removed: true },
        ];
        gc_fork_common(chains, max_changes);
    }
}

fn test_gc_boundaries_common(max_changes_limit: usize) {
    for max_changes in 1..=max_changes_limit {
        for i in 45..=51 {
            for len in 1..=5 {
                let chains = vec![
                    SimpleChain { from: 0, length: 101, is_removed: false },
                    SimpleChain { from: i, length: len, is_removed: i + len <= 50 },
                ];
                gc_fork_common(chains, max_changes);
            }
        }
    }
}

#[test]
fn test_gc_boundaries_small() {
    test_gc_boundaries_common(1)
}

#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn test_gc_boundaries_large() {
    test_gc_boundaries_common(20)
}

fn test_gc_random_common(runs: u64) {
    let mut rng = rand::thread_rng();
    for _tests in 0..runs {
        let canonical_len = 100 + rng.gen_range(0, 20);
        let mut chains = vec![SimpleChain { from: 0, length: canonical_len, is_removed: false }];
        for _num_chains in 1..10 {
            let from = rng.gen_range(0, 50);
            let len = rng.gen_range(0, 50) + 1;
            chains.push(SimpleChain {
                from,
                length: len,
                is_removed: from + len < canonical_len - 50,
            });
            gc_fork_common(chains.clone(), rng.gen_range(0, 20) + 1);
        }
    }
}

#[test]
fn test_gc_random_small() {
    test_gc_random_common(3);
}

#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn test_gc_random_large() {
    test_gc_random_common(25);
}

#[test]
fn test_gc_pine_small() {
    let mut chains = vec![SimpleChain { from: 0, length: 101, is_removed: false }];
    for i in 1..50 {
        chains.push(SimpleChain { from: i, length: 1, is_removed: true });
    }
    for i in 50..100 {
        chains.push(SimpleChain { from: i, length: 1, is_removed: false });
    }
    gc_fork_common(chains, 3);

    let mut chains = vec![SimpleChain { from: 0, length: 101, is_removed: false }];
    for i in 1..49 {
        chains.push(SimpleChain { from: i, length: 2, is_removed: true });
    }
    for i in 49..99 {
        chains.push(SimpleChain { from: i, length: 2, is_removed: false });
    }
    gc_fork_common(chains, 2);

    let mut chains = vec![SimpleChain { from: 0, length: 101, is_removed: false }];
    for i in 1..48 {
        chains.push(SimpleChain { from: i, length: 3, is_removed: true });
    }
    for i in 48..98 {
        chains.push(SimpleChain { from: i, length: 3, is_removed: false });
    }
    gc_fork_common(chains, 1);

    let mut chains = vec![SimpleChain { from: 0, length: 101, is_removed: false }];
    for i in 1..40 {
        chains.push(SimpleChain { from: i, length: 11, is_removed: true });
    }
    for i in 40..90 {
        chains.push(SimpleChain { from: i, length: 11, is_removed: false });
    }
    gc_fork_common(chains, 1);
}

#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn test_gc_pine() {
    for max_changes in 1..=20 {
        let mut chains = vec![SimpleChain { from: 0, length: 101, is_removed: false }];
        for i in 1..50 {
            chains.push(SimpleChain { from: i, length: 1, is_removed: true });
        }
        for i in 50..100 {
            chains.push(SimpleChain { from: i, length: 1, is_removed: false });
        }
        gc_fork_common(chains, max_changes);

        let mut chains = vec![SimpleChain { from: 0, length: 101, is_removed: false }];
        for i in 1..40 {
            chains.push(SimpleChain { from: i, length: 11, is_removed: true });
        }
        for i in 40..90 {
            chains.push(SimpleChain { from: i, length: 11, is_removed: false });
        }
        gc_fork_common(chains, max_changes);
    }
}

fn test_gc_star_common(max_changes_limit: usize) {
    for max_changes in 1..=max_changes_limit {
        let mut chains = vec![SimpleChain { from: 0, length: 101, is_removed: false }];
        for i in 1..=17 {
            chains.push(SimpleChain { from: 33, length: i, is_removed: true });
        }
        for i in 18..67 {
            chains.push(SimpleChain { from: 33, length: i, is_removed: false });
        }
        gc_fork_common(chains, max_changes);
    }
}

#[test]
fn test_gc_star_small() {
    test_gc_star_common(1)
}

#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn test_gc_star_large() {
    test_gc_star_common(20)
}
