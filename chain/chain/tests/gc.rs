#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use near_chain::chain::{check_refcount_map, Chain, ChainGenesis};
    use near_chain::test_utils::KeyValueRuntime;
    use near_chain::types::Tip;
    use near_chain::DoomslugThresholdMode;
    use near_crypto::KeyType;
    use near_primitives::block::Block;
    use near_primitives::types::{NumBlocks, StateRoot};
    use near_primitives::validator_signer::InMemoryValidatorSigner;
    use near_store::test_utils::{create_test_store, gen_changes};
    use near_store::{Trie, WrappedTrieChanges};

    fn get_chain() -> Chain {
        get_chain_with_epoch_length(10)
    }

    fn get_chain_with_epoch_length(epoch_length: NumBlocks) -> Chain {
        let store = create_test_store();
        let chain_genesis = ChainGenesis::test();
        let validators = vec![vec!["test1"]];
        let runtime_adapter = Arc::new(KeyValueRuntime::new_with_validators(
            store.clone(),
            validators
                .into_iter()
                .map(|inner| inner.into_iter().map(Into::into).collect())
                .collect(),
            1,
            1,
            epoch_length,
        ));
        Chain::new(runtime_adapter, &chain_genesis, DoomslugThresholdMode::NoApprovals).unwrap()
    }

    // Build a chain of num_blocks on top of prev_block
    fn do_fork(
        mut prev_block: Block,
        mut prev_state_root: StateRoot,
        trie: Arc<Trie>,
        chain: &mut Chain,
        num_blocks: u64,
        states: &mut Vec<(Block, StateRoot, Vec<(Vec<u8>, Option<Vec<u8>>)>)>,
        max_changes: usize,
        verbose: bool,
    ) {
        let mut rng = rand::thread_rng();
        let signer =
            Arc::new(InMemoryValidatorSigner::from_seed("test1", KeyType::ED25519, "test1"));
        for _ in 0..num_blocks {
            let block = Block::empty(&prev_block, &*signer);

            let head = chain.head().unwrap();
            let mut store_update = chain.mut_store().store_update();
            store_update.save_block(block.clone());
            store_update.inc_block_refcount(&block.header.prev_hash).unwrap();
            store_update.save_block_header(block.header.clone());
            let tip = Tip::from_header(&block.header);
            if head.height < tip.height {
                store_update.save_head(&tip).unwrap();
            }

            let trie_changes_data = gen_changes(&mut rng, max_changes);
            let state_root = prev_state_root;
            let trie_changes = trie.update(&state_root, trie_changes_data.iter().cloned()).unwrap();
            if verbose {
                println!("state new {:?} {:?}", block.header.inner_lite.height, trie_changes_data);
            }

            let (trie_store_update, new_root) =
                trie_changes.clone().into_no_deletions(trie.clone()).unwrap();
            states.push((block.clone(), new_root.clone(), trie_changes_data.clone()));
            store_update.merge(trie_store_update);

            let wrapped_trie_changes = WrappedTrieChanges::new(
                trie.clone(),
                trie_changes,
                Default::default(),
                block.hash(),
            );
            store_update.save_trie_changes(wrapped_trie_changes);
            store_update.commit().unwrap();

            assert!(trie.iter(&new_root).is_ok());

            prev_block = block.clone();
            prev_state_root = new_root;
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

        // Init Chain 1
        let mut chain1 = get_chain();
        let trie1 = chain1.runtime_adapter.get_trie().clone();
        let genesis1 = chain1.get_block_by_height(0).unwrap().clone();
        let mut states1 = vec![];
        states1.push((genesis1.clone(), Trie::empty_root(), vec![]));

        for simple_chain in simple_chains.iter() {
            let (source_block1, state_root1, _) = states1[simple_chain.from as usize].clone();
            do_fork(
                source_block1.clone(),
                state_root1,
                trie1.clone(),
                &mut chain1,
                simple_chain.length,
                &mut states1,
                max_changes,
                verbose,
            );
        }

        assert!(check_refcount_map(&mut chain1).is_ok());
        // GC execution
        let clear_data = chain1.clear_data(trie1.clone());
        if clear_data.is_err() {
            println!("clear data failed = {:?}", clear_data);
            assert!(false);
        }
        assert!(check_refcount_map(&mut chain1).is_ok());

        let mut chain2 = get_chain();
        let trie2 = chain2.runtime_adapter.get_trie().clone();

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
            let state_root1 = states1[simple_chain.from as usize].1;
            assert!(trie1.iter(&state_root1).is_ok());
            assert_eq!(state_root1, state_root2);

            for i in start_index..start_index + simple_chain.length {
                let mut store_update2 = chain2.mut_store().store_update();
                let (block1, state_root1, changes1) = states1[i as usize].clone();
                // Apply to Trie 2 the same changes (changes1) as applied to Trie 1
                let trie_changes2 = trie2.update(&state_root2, changes1.iter().cloned()).unwrap();
                // i == gc_height is the only height should be processed here
                if block1.header.inner_lite.height > gc_height || i == gc_height {
                    let (trie_store_update2, new_root2) =
                        trie_changes2.clone().into_no_deletions(trie2.clone()).unwrap();
                    state_root2 = new_root2;
                    assert_eq!(state_root1, state_root2);
                    store_update2.merge(trie_store_update2);
                } else {
                    let (trie_store_update2, new_root2) =
                        trie_changes2.clone().into(trie2.clone()).unwrap();
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
                if block1.header.inner_lite.height > gc_height || i == gc_height {
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

    #[cfg(feature = "expensive_tests")]
    #[test]
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

    #[cfg(feature = "expensive_tests")]
    #[test]
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

    #[cfg(feature = "expensive_tests")]
    #[test]
    fn test_gc_boundaries_large() {
        test_gc_boundaries_common(20)
    }

    fn test_gc_random_common(runs: u64) {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        for _tests in 0..runs {
            let canonical_len = 100 + rng.gen_range(0, 20);
            let mut chains =
                vec![SimpleChain { from: 0, length: canonical_len, is_removed: false }];
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

    #[cfg(feature = "expensive_tests")]
    #[test]
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

    #[cfg(feature = "expensive_tests")]
    #[test]
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

    #[cfg(feature = "expensive_tests")]
    #[test]
    fn test_gc_star_large() {
        test_gc_star_common(20)
    }
}
