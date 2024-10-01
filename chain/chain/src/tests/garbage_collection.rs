use near_async::time::Clock;
use rand::Rng;
use std::sync::Arc;

use crate::chain::Chain;
use crate::garbage_collection::GCMode;
use crate::test_utils::{
    get_chain, get_chain_with_epoch_length, get_chain_with_epoch_length_and_num_shards,
    get_chain_with_num_shards,
};
use crate::types::Tip;
use crate::{ChainStoreAccess, StoreValidator};

use near_chain_configs::{GCConfig, GenesisConfig, DEFAULT_GC_NUM_EPOCHS_TO_KEEP};
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block::Block;
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::merkle::PartialMerkleTree;
use near_primitives::shard_layout::ShardUId;
use near_primitives::test_utils::{create_test_signer, TestBlockBuilder};
use near_primitives::types::{BlockHeight, NumBlocks, StateRoot};
use near_primitives::validator_signer::ValidatorSigner;
use near_store::test_utils::gen_changes;
use near_store::{DBCol, ShardTries, Trie, WrappedTrieChanges};

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
    final_block_height: Option<u64>,
) {
    if verbose {
        println!(
            "Prev Block: @{} {:?} {:?} {:?}",
            prev_block.header().height(),
            prev_block.header().epoch_id(),
            prev_block.header().next_epoch_id(),
            prev_block.hash()
        );
    }
    let mut rng = rand::thread_rng();
    let signer = Arc::new(create_test_signer("test1"));
    let num_shards = prev_state_roots.len() as u64;
    for i in 0..num_blocks {
        let next_epoch_id = chain
            .epoch_manager
            .get_next_epoch_id_from_prev_block(prev_block.hash())
            .expect("block must exist");
        let block = if next_epoch_id == *prev_block.header().next_epoch_id() {
            TestBlockBuilder::new(Clock::real(), &prev_block, signer.clone()).build()
        } else {
            let prev_hash = prev_block.hash();
            let epoch_id = *prev_block.header().next_epoch_id();
            if verbose {
                println!(
                    "Creating block with new epoch id {:?} @{}",
                    next_epoch_id,
                    prev_block.header().height() + 1
                );
            }
            let next_bp_hash = Chain::compute_bp_hash(
                chain.epoch_manager.as_ref(),
                next_epoch_id,
                epoch_id,
                &prev_hash,
            )
            .unwrap();
            TestBlockBuilder::new(Clock::real(), &prev_block, signer.clone())
                .epoch_id(epoch_id)
                .next_epoch_id(next_epoch_id)
                .next_bp_hash(next_bp_hash)
                .build()
        };

        if verbose {
            println!(
                "Block: @{} {:?} {:?} {:?}",
                block.header().height(),
                block.header().epoch_id(),
                block.header().next_epoch_id(),
                block.hash()
            );
        }

        if i == 0 {
            let mut store_update = chain.mut_chain_store().store_update();
            store_update.save_block_merkle_tree(*prev_block.hash(), PartialMerkleTree::default());
            store_update.commit().unwrap();
        }

        let head = chain.head().unwrap();
        let epoch_manager = chain.epoch_manager.clone();
        let mut store_update = chain.mut_chain_store().store_update();
        store_update.save_block(block.clone());
        store_update.inc_block_refcount(block.header().prev_hash()).unwrap();
        store_update.save_block_header(block.header().clone()).unwrap();
        let tip = Tip::from_header(block.header());
        if head.height < tip.height {
            store_update.save_head(&tip).unwrap();
        }
        let final_height =
            final_block_height.unwrap_or_else(|| block.header().height().saturating_sub(2));
        let epoch_manager_update = epoch_manager
            .add_validator_proposals(
                BlockInfo::from_header(block.header(), final_height),
                *block.header().random_value(),
            )
            .unwrap();
        store_update.merge(epoch_manager_update);

        let mut trie_changes_shards = Vec::new();
        for shard_id in 0..num_shards {
            let shard_uid = ShardUId { version: 0, shard_id: shard_id as u32 };
            let trie_changes_data = gen_changes(&mut rng, max_changes);
            let state_root = prev_state_roots[shard_id as usize];
            let trie = tries.get_trie_for_shard(shard_uid, state_root);
            let trie_changes = trie.update(trie_changes_data.iter().cloned()).unwrap();
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
                block.header().height(),
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

    let num_shards = rand::thread_rng().gen_range(1..3);

    // Init Chain 1
    let mut chain1 = get_chain_with_num_shards(Clock::real(), num_shards);
    let tries1 = chain1.runtime_adapter.get_tries();
    let mut rng = rand::thread_rng();
    let shard_to_check_trie = rng.gen_range(0..num_shards);
    let shard_uid = ShardUId { version: 0, shard_id: shard_to_check_trie as u32 };
    let genesis1 = chain1.get_block_by_height(0).unwrap();
    let mut states1 = vec![];
    states1.push((
        genesis1,
        vec![Trie::EMPTY_ROOT; num_shards as usize],
        vec![Vec::new(); num_shards as usize],
    ));

    let mut final_height = None;
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
            final_height,
        );
        if final_height.is_none() {
            final_height = states1.last().unwrap().0.header().height().checked_sub(2);
        }
    }

    // GC execution
    chain1.clear_data(&GCConfig { gc_blocks_limit: 1000, ..GCConfig::default() }).unwrap();

    let tries2 = get_chain_with_num_shards(Clock::real(), num_shards).runtime_adapter.get_tries();

    // Find gc_height
    let mut gc_height = simple_chains[0].length - 41;
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
    state_roots2.push(Trie::EMPTY_ROOT);

    for simple_chain in simple_chains.iter() {
        if simple_chain.is_removed {
            for _ in 0..simple_chain.length {
                // This chain is deleted in Chain1
                state_roots2.push(Trie::EMPTY_ROOT);
            }
            start_index += simple_chain.length;
            continue;
        }

        let mut state_root2 = state_roots2[simple_chain.from as usize];
        let state_root1 = states1[simple_chain.from as usize].1[shard_to_check_trie as usize];
        tries1.get_trie_for_shard(shard_uid, state_root1).disk_iter().unwrap();
        assert_eq!(state_root1, state_root2);

        for i in start_index..start_index + simple_chain.length {
            let (block1, state_root1, changes1) = states1[i as usize].clone();
            // Apply to Trie 2 the same changes (changes1) as applied to Trie 1
            let trie_changes2 = tries2
                .get_trie_for_shard(shard_uid, state_root2)
                .update(changes1[shard_to_check_trie as usize].iter().cloned())
                .unwrap();
            // i == gc_height is the only height should be processed here
            let mut update2 = tries2.store_update();
            if block1.header().height() > gc_height || i == gc_height {
                tries2.apply_insertions(&trie_changes2, shard_uid, &mut update2);
                state_root2 = trie_changes2.new_root;
                assert_eq!(state_root1[shard_to_check_trie as usize], state_root2);
            } else {
                let new_root2 = tries2.apply_all(&trie_changes2, shard_uid, &mut update2);
                state_root2 = new_root2;
            }
            state_roots2.push(state_root2);
            update2.commit().unwrap();
        }
        start_index += simple_chain.length;
    }

    let mut start_index = 1; // zero is for genesis
    for simple_chain in simple_chains.iter() {
        if simple_chain.is_removed {
            for i in start_index..start_index + simple_chain.length {
                let (block1, _, _) = states1[i as usize].clone();
                // Make sure that blocks were removed.
                assert_eq!(
                    chain1.block_exists(block1.hash()).unwrap(),
                    false,
                    "Block {:?}@{} should have been removed - as it belongs to removed fork.",
                    block1.hash(),
                    block1.header().height()
                );
            }
            start_index += simple_chain.length;
            continue;
        }
        for i in start_index..start_index + simple_chain.length {
            let (block1, state_root1, _) = states1[i as usize].clone();
            let state_root1 = state_root1[shard_to_check_trie as usize];
            if block1.header().height() > gc_height || i == gc_height {
                assert_eq!(
                    chain1.block_exists(block1.hash()).unwrap(),
                    true,
                    "Block {:?}@{} should exist",
                    block1.hash(),
                    block1.header().height()
                );
                let a = tries1
                    .get_trie_for_shard(shard_uid, state_root1)
                    .disk_iter()
                    .unwrap()
                    .map(|item| item.unwrap().0)
                    .collect::<Vec<_>>();
                let b = tries2
                    .get_trie_for_shard(shard_uid, state_root1)
                    .disk_iter()
                    .unwrap()
                    .map(|item| item.unwrap().0)
                    .collect::<Vec<_>>();
                assert_eq!(a, b);
            } else {
                // Make sure that blocks were removed.
                assert_eq!(
                    chain1.block_exists(block1.hash()).unwrap(),
                    false,
                    "Block {:?}@{} should have been removed.",
                    block1.hash(),
                    block1.header().height()
                );
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
        for fork_length in 51..=60 {
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
    for fork_length in 45..=60 {
        let chains = vec![
            SimpleChain { from: 0, length: 101, is_removed: false },
            SimpleChain { from: 0, length: fork_length, is_removed: true },
        ];
        gc_fork_common(chains, 1);
    }
    for fork_length in 61..=65 {
        let chains = vec![
            SimpleChain { from: 0, length: 101, is_removed: false },
            SimpleChain { from: 0, length: fork_length, is_removed: false },
        ];
        gc_fork_common(chains, 1);
    }
    for fork_length in 0..=10 {
        let chains = vec![
            SimpleChain { from: 0, length: 101, is_removed: false },
            SimpleChain { from: 0, length: 61 + fork_length, is_removed: false },
            SimpleChain { from: 0, length: fork_length, is_removed: true },
            SimpleChain { from: 0, length: 60 - fork_length, is_removed: true },
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
            SimpleChain { from: 30, length: 31, is_removed: false },
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
                    SimpleChain { from: i, length: len, is_removed: i + len <= 60 },
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
        let canonical_len = 101;
        let mut chains = vec![SimpleChain { from: 0, length: canonical_len, is_removed: false }];
        for _num_chains in 1..10 {
            let from = rng.gen_range(0..50);
            let len = rng.gen_range(0..50) + 1;
            chains.push(SimpleChain {
                from,
                length: len,
                is_removed: from + len < canonical_len - 40,
            });
            gc_fork_common(chains.clone(), rng.gen_range(0..20) + 1);
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
    for i in 1..100 {
        chains.push(SimpleChain { from: i, length: 1, is_removed: i < 60 });
    }
    gc_fork_common(chains, 3);

    let mut chains = vec![SimpleChain { from: 0, length: 101, is_removed: false }];
    for i in 1..99 {
        chains.push(SimpleChain { from: i, length: 2, is_removed: i < 59 });
    }
    gc_fork_common(chains, 2);

    let mut chains = vec![SimpleChain { from: 0, length: 101, is_removed: false }];
    for i in 1..98 {
        chains.push(SimpleChain { from: i, length: 3, is_removed: i < 58 });
    }
    gc_fork_common(chains, 1);

    let mut chains = vec![SimpleChain { from: 0, length: 101, is_removed: false }];
    for i in 1..90 {
        chains.push(SimpleChain { from: i, length: 11, is_removed: i < 50 });
    }
    gc_fork_common(chains, 1);
}

#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn test_gc_pine() {
    for max_changes in 1..=20 {
        let mut chains = vec![SimpleChain { from: 0, length: 101, is_removed: false }];
        for i in 1..100 {
            chains.push(SimpleChain { from: i, length: 1, is_removed: i < 60 });
        }
        gc_fork_common(chains, max_changes);

        let mut chains = vec![SimpleChain { from: 0, length: 101, is_removed: false }];
        for i in 1..90 {
            chains.push(SimpleChain { from: i, length: 11, is_removed: i < 50 });
        }
        gc_fork_common(chains, max_changes);
    }
}

fn test_gc_star_common(max_changes_limit: usize) {
    for max_changes in 1..=max_changes_limit {
        let mut chains = vec![SimpleChain { from: 0, length: 101, is_removed: false }];
        for i in 1..=67 {
            chains.push(SimpleChain { from: 33, length: i, is_removed: i <= 27 });
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

#[test]
// This test covers the situation when fork happens far away from the end of the epoch.
// Currently, we start fork cleanup, once we are at the epoch boundary, by setting 'fork_tail'.
// Then in each step, we move the fork_tail backwards, cleaning up any forks that we might encounter.
// But in order not to do too much work in one go, we check up to 1000 blocks (and we continue during the next execution of clean).
// This test checks what happens when fork is more than 1k blocks from the epoch end - and checks that it gets properly cleaned
// during the second run.
fn test_fork_far_away_from_epoch_end() {
    let verbose = false;
    let max_changes = 1;
    let fork_clean_step = 100;
    let epoch_length = fork_clean_step + 10;
    let simple_chains = [
        SimpleChain { from: 0, length: 5, is_removed: false },
        SimpleChain { from: 5, length: 2, is_removed: true },
        // We want the chain to end up exactly at the new epoch start.
        SimpleChain { from: 5, length: 5 * epoch_length - 5 + 1, is_removed: false },
    ];

    let num_shards = 1;
    let mut chain =
        get_chain_with_epoch_length_and_num_shards(Clock::real(), epoch_length, num_shards);
    let tries = chain.runtime_adapter.get_tries();
    let genesis = chain.get_block_by_height(0).unwrap();
    let mut states = vec![(
        genesis,
        vec![Trie::EMPTY_ROOT; num_shards as usize],
        vec![Vec::new(); num_shards as usize],
    )];

    for (simple_chain, final_height) in simple_chains.iter().zip([None, Some(3), None]) {
        let (source_block, state_root, _) = states[simple_chain.from as usize].clone();
        do_fork(
            source_block.clone(),
            state_root,
            tries.clone(),
            &mut chain,
            simple_chain.length,
            &mut states,
            max_changes,
            verbose,
            final_height,
        );
    }

    // GC execution
    chain
        .clear_data(&GCConfig {
            gc_blocks_limit: 100,
            gc_fork_clean_step: fork_clean_step,
            ..GCConfig::default()
        })
        .expect("Clear data failed");

    // The run above would clear just the first 5 blocks from the beginning, but shouldn't clear any forks
    // yet - as fork_tail only clears the 'last' 1k blocks.
    for i in 1..5 {
        let (block, _, _) = states[i as usize].clone();
        assert_eq!(
            chain.block_exists(block.hash()).unwrap(),
            false,
            "Block {:?}@{} should have been removed.",
            block.hash(),
            block.header().height()
        );
    }
    // But blocks from the fork - shouldn't be removed yet.
    for i in 5..7 {
        let (block, _, _) = states[i as usize].clone();
        assert_eq!(
            chain.block_exists(block.hash()).unwrap(),
            true,
            "Block {:?}@{} should NOT be removed.",
            block.hash(),
            block.header().height()
        );
    }
    // Now let's add one more block - and now the fork (and the rest) should be successfully removed.
    {
        let (source_block, state_root, _) = states.last().unwrap().clone();
        do_fork(
            source_block,
            state_root,
            tries,
            &mut chain,
            1,
            &mut states,
            max_changes,
            verbose,
            None,
        );
    }
    chain
        .clear_data(&GCConfig { gc_blocks_limit: 100, ..GCConfig::default() })
        .expect("Clear data failed");
    // And now all these blocks should be safely removed.
    for i in 6..50 {
        let (block, _, _) = states[i as usize].clone();
        assert_eq!(
            chain.block_exists(block.hash()).unwrap(),
            false,
            "Block {:?}@{} should have been removed.",
            block.hash(),
            block.header().height()
        );
    }
}

/// Test that garbage collection works properly. The blocks behind gc head should be garbage
/// collected while the blocks that are ahead of it should not.
#[test]
fn test_clear_old_data() {
    let max_height = 14usize;
    let mut chain = get_chain_with_epoch_length(Clock::real(), 1);
    let epoch_manager = chain.epoch_manager.clone();
    let genesis = chain.get_block_by_height(0).unwrap();
    let signer = Arc::new(create_test_signer("test1"));
    let mut prev_block = genesis;
    let mut blocks = vec![prev_block.clone()];
    for i in 1..=max_height {
        add_block(
            &mut chain,
            epoch_manager.as_ref(),
            &mut prev_block,
            &mut blocks,
            signer.clone(),
            i as BlockHeight,
        );
    }

    chain.clear_data(&GCConfig { gc_blocks_limit: 100, ..GCConfig::default() }).unwrap();

    for i in 0..=max_height {
        println!("height = {} hash = {}", i, blocks[i].hash());
        let expected_removed = i < max_height - DEFAULT_GC_NUM_EPOCHS_TO_KEEP as usize;
        let get_block_result = chain.get_block(blocks[i].hash());
        let blocks_by_heigh =
            chain.mut_chain_store().get_all_block_hashes_by_height(i as BlockHeight).unwrap();
        assert_eq!(get_block_result.is_err(), expected_removed);
        assert_eq!(blocks_by_heigh.is_empty(), expected_removed);
    }
}

// Adds block to the chain at given height after prev_block.
fn add_block(
    chain: &mut Chain,
    epoch_manager: &dyn EpochManagerAdapter,
    prev_block: &mut Block,
    blocks: &mut Vec<Block>,
    signer: Arc<ValidatorSigner>,
    height: u64,
) {
    let next_epoch_id = epoch_manager
        .get_next_epoch_id_from_prev_block(prev_block.hash())
        .expect("block must exist");
    let mut store_update = chain.mut_chain_store().store_update();

    let block = if next_epoch_id == *prev_block.header().next_epoch_id() {
        TestBlockBuilder::new(Clock::real(), &prev_block, signer).height(height).build()
    } else {
        let prev_hash = prev_block.hash();
        let epoch_id = *prev_block.header().next_epoch_id();
        let next_bp_hash =
            Chain::compute_bp_hash(epoch_manager, next_epoch_id, epoch_id, &prev_hash).unwrap();
        TestBlockBuilder::new(Clock::real(), &prev_block, signer)
            .height(height)
            .epoch_id(epoch_id)
            .next_epoch_id(next_epoch_id)
            .next_bp_hash(next_bp_hash)
            .build()
    };
    blocks.push(block.clone());
    store_update.save_block(block.clone());
    store_update.inc_block_refcount(block.header().prev_hash()).unwrap();
    store_update.save_block_header(block.header().clone()).unwrap();
    store_update.save_head(&Tip::from_header(block.header())).unwrap();
    store_update
        .chain_store_cache_update
        .height_to_hashes
        .insert(height, Some(*block.header().hash()));
    store_update.save_next_block_hash(prev_block.hash(), *block.hash());
    let epoch_manager_update = epoch_manager
        .add_validator_proposals(
            BlockInfo::from_header(block.header(), block.header().height().saturating_sub(2)),
            *block.header().random_value(),
        )
        .unwrap();
    store_update.merge(epoch_manager_update);
    store_update.commit().unwrap();
    *prev_block = block.clone();
}

#[test]
fn test_clear_old_data_fixed_height() {
    let mut chain = get_chain(Clock::real());
    let epoch_manager = chain.epoch_manager.clone();
    let genesis = chain.get_block_by_height(0).unwrap();
    let signer = Arc::new(create_test_signer("test1"));
    let mut prev_block = genesis;
    let mut blocks = vec![prev_block.clone()];
    for i in 1..10 {
        add_block(
            &mut chain,
            epoch_manager.as_ref(),
            &mut prev_block,
            &mut blocks,
            signer.clone(),
            i,
        );
    }

    assert!(chain.get_block(blocks[4].hash()).is_ok());
    assert!(chain.get_block(blocks[5].hash()).is_ok());
    assert!(chain.get_block(blocks[6].hash()).is_ok());
    assert!(chain.get_block_header(blocks[5].hash()).is_ok());
    assert_eq!(
        chain
            .mut_chain_store()
            .get_all_block_hashes_by_height(5)
            .unwrap()
            .values()
            .flatten()
            .collect::<Vec<_>>(),
        vec![blocks[5].hash()]
    );
    assert!(chain.mut_chain_store().get_next_block_hash(blocks[5].hash()).is_ok());

    let trie = chain.runtime_adapter.get_tries();
    let mut store_update = chain.mut_chain_store().store_update();
    assert!(store_update
        .clear_block_data(epoch_manager.as_ref(), *blocks[5].hash(), GCMode::Canonical(trie))
        .is_ok());
    store_update.commit().unwrap();

    assert!(chain.get_block(blocks[4].hash()).is_err());
    assert!(chain.get_block(blocks[5].hash()).is_ok());
    assert!(chain.get_block(blocks[6].hash()).is_ok());
    // block header should be available
    assert!(chain.get_block_header(blocks[4].hash()).is_ok());
    assert!(chain.get_block_header(blocks[5].hash()).is_ok());
    assert!(chain.get_block_header(blocks[6].hash()).is_ok());
    assert!(chain.mut_chain_store().get_all_block_hashes_by_height(4).unwrap().is_empty());
    assert!(!chain.mut_chain_store().get_all_block_hashes_by_height(5).unwrap().is_empty());
    assert!(!chain.mut_chain_store().get_all_block_hashes_by_height(6).unwrap().is_empty());
    assert!(chain.mut_chain_store().get_next_block_hash(blocks[4].hash()).is_err());
    assert!(chain.mut_chain_store().get_next_block_hash(blocks[5].hash()).is_ok());
    assert!(chain.mut_chain_store().get_next_block_hash(blocks[6].hash()).is_ok());
}

/// Test that `gc_blocks_limit` works properly
#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
#[allow(unreachable_code)]
fn test_clear_old_data_too_many_heights() {
    // TODO(#10634): panics on `clear_data` -> `clear_resharding_data` ->
    // `MockEpochManager::is_next_block_epoch_start` apparently because
    // epoch manager is not updated at all. Should we fix it together with
    // removing `MockEpochManager`?
    return;

    for i in 1..5 {
        println!("gc_blocks_limit == {:?}", i);
        test_clear_old_data_too_many_heights_common(i);
    }
    test_clear_old_data_too_many_heights_common(25);
    test_clear_old_data_too_many_heights_common(50);
    test_clear_old_data_too_many_heights_common(87);
}

fn test_clear_old_data_too_many_heights_common(gc_blocks_limit: NumBlocks) {
    let mut chain = get_chain_with_epoch_length(Clock::real(), 1);
    let genesis = chain.get_block_by_height(0).unwrap();
    let signer = Arc::new(create_test_signer("test1"));
    let mut prev_block = genesis;
    let mut blocks = vec![prev_block.clone()];
    {
        let mut store_update = chain.chain_store().store().store_update();
        let block_info = BlockInfo::default();
        store_update.insert_ser(DBCol::BlockInfo, prev_block.hash().as_ref(), &block_info).unwrap();
        store_update.commit().unwrap();
    }
    for i in 1..1000 {
        let block =
            TestBlockBuilder::new(Clock::real(), &prev_block, signer.clone()).height(i).build();
        blocks.push(block.clone());

        let mut store_update = chain.mut_chain_store().store_update();
        store_update.save_block(block.clone());
        store_update.inc_block_refcount(block.header().prev_hash()).unwrap();
        store_update.save_block_header(block.header().clone()).unwrap();
        store_update.save_head(&Tip::from_header(&block.header())).unwrap();
        {
            let mut store_update = store_update.store().store_update();
            let block_info = BlockInfo::default();
            store_update.insert_ser(DBCol::BlockInfo, block.hash().as_ref(), &block_info).unwrap();
            store_update.commit().unwrap();
        }
        store_update
            .chain_store_cache_update
            .height_to_hashes
            .insert(i, Some(*block.header().hash()));
        store_update.save_next_block_hash(&prev_block.hash(), *block.hash());
        store_update.commit().unwrap();

        prev_block = block.clone();
    }

    for iter in 0..10 {
        println!("ITERATION #{:?}", iter);
        assert!(chain.clear_data(&GCConfig { gc_blocks_limit, ..GCConfig::default() }).is_ok());

        // epoch didn't change so no data is garbage collected.
        for i in 0..1000 {
            if i < (iter + 1) * gc_blocks_limit as usize {
                assert!(chain.get_block(&blocks[i].hash()).is_err());
                assert!(chain
                    .mut_chain_store()
                    .get_all_block_hashes_by_height(i as BlockHeight)
                    .unwrap()
                    .is_empty());
            } else {
                assert!(chain.get_block(&blocks[i].hash()).is_ok());
                assert!(!chain
                    .mut_chain_store()
                    .get_all_block_hashes_by_height(i as BlockHeight)
                    .unwrap()
                    .is_empty());
            }
        }
        let mut genesis = GenesisConfig::default();
        genesis.genesis_height = 0;
        let mut store_validator = StoreValidator::new(
            None,
            genesis.clone(),
            chain.epoch_manager.clone(),
            chain.shard_tracker.clone(),
            chain.runtime_adapter.clone(),
            chain.chain_store().store().clone(),
            false,
        );
        store_validator.validate();
        println!("errors = {:?}", store_validator.errors);
        assert!(!store_validator.is_failed());
    }
}

#[test]
fn test_fork_chunk_tail_updates() {
    let mut chain = get_chain(Clock::real());
    let epoch_manager = chain.epoch_manager.clone();
    let genesis = chain.get_block_by_height(0).unwrap();
    let signer = Arc::new(create_test_signer("test1"));
    let mut prev_block = genesis;
    let mut blocks = vec![prev_block.clone()];
    for i in 1..10 {
        add_block(
            &mut chain,
            epoch_manager.as_ref(),
            &mut prev_block,
            &mut blocks,
            signer.clone(),
            i,
        );
    }
    assert_eq!(chain.tail().unwrap(), 0);

    {
        let mut store_update = chain.mut_chain_store().store_update();
        assert_eq!(store_update.tail().unwrap(), 0);
        store_update.update_tail(1).unwrap();
        store_update.commit().unwrap();
    }
    // Chunk tail should be auto updated to genesis (if not set) and fork_tail to the tail.
    {
        let store_update = chain.mut_chain_store().store_update();
        assert_eq!(store_update.tail().unwrap(), 1);
        assert_eq!(store_update.fork_tail().unwrap(), 1);
        assert_eq!(store_update.chunk_tail().unwrap(), 0);
    }
    {
        let mut store_update = chain.mut_chain_store().store_update();
        store_update.update_fork_tail(3);
        store_update.commit().unwrap();
    }
    {
        let mut store_update = chain.mut_chain_store().store_update();
        store_update.update_tail(2).unwrap();
        store_update.commit().unwrap();
    }
    {
        let store_update = chain.mut_chain_store().store_update();
        assert_eq!(store_update.tail().unwrap(), 2);
        assert_eq!(store_update.fork_tail().unwrap(), 3);
        assert_eq!(store_update.chunk_tail().unwrap(), 0);
    }
}
