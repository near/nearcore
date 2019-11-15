#[cfg(test)]
#[cfg(feature = "expensive_tests")]
mod tests {
    use near_chain::test_utils::setup;
    use near_chain::FinalityGadget;
    use near_chain::{Chain, ChainStore, ChainStoreAccess, ChainStoreUpdate};
    use near_crypto::{Signature, Signer};
    use near_epoch_manager::test_utils::{record_block, setup_default_epoch_manager};
    use near_epoch_manager::EpochManager;
    use near_primitives::block::{Approval, Block, Weight};
    use near_primitives::hash::CryptoHash;
    use near_primitives::types::{AccountId, BlockIndex, EpochId};
    use rand::seq::SliceRandom;
    use rand::Rng;
    use std::collections::{HashMap, HashSet};

    fn create_block(
        em: &mut EpochManager,
        prev: &Block,
        height: BlockIndex,
        chain_store: &mut ChainStore,
        signer: &dyn Signer,
        approvals: Vec<Approval>,
        total_block_producers: usize,
    ) -> Block {
        let is_this_block_epoch_start = em.is_next_block_epoch_start(&prev.hash()).unwrap();

        let epoch_id = if is_this_block_epoch_start {
            // This is a bit weird way to define an epoch, but replicating the exact way we use today is
            // unnecessarily complex. Using last `pre_commit` is sufficient to ensure that the `epoch_id`
            // of the next epoch will be the same for as long as the last committed block in the prev
            // epoch was the same.
            EpochId(prev.header.inner.last_quorum_pre_commit)
        } else {
            prev.header.inner.epoch_id.clone()
        };

        let mut block = Block::empty(prev, signer);
        block.header.inner.approvals = approvals.clone();
        block.header.inner.height = height;
        block.header.inner.total_weight = (height as u128).into();
        block.header.inner.epoch_id = epoch_id.clone();

        let quorums = FinalityGadget::compute_quorums(
            prev.hash(),
            epoch_id,
            height,
            approvals.clone(),
            chain_store,
            total_block_producers,
        )
        .unwrap()
        .clone();

        block.header.inner.last_quorum_pre_vote = quorums.last_quorum_pre_vote;
        block.header.inner.last_quorum_pre_commit = em
            .push_final_block_back_if_needed(prev.hash(), quorums.last_quorum_pre_commit)
            .unwrap();

        block.header.inner.score = if quorums.last_quorum_pre_vote == CryptoHash::default() {
            0.into()
        } else {
            chain_store.get_block_header(&quorums.last_quorum_pre_vote).unwrap().inner.total_weight
        };

        block.header.init();

        let mut chain_store_update = ChainStoreUpdate::new(chain_store);
        chain_store_update.save_block_header(block.header.clone());
        chain_store_update.commit().unwrap();

        record_block(
            em,
            block.header.inner.prev_hash,
            block.hash(),
            block.header.inner.height,
            vec![],
        );

        block
    }

    fn apr(account_id: AccountId, reference_hash: CryptoHash, parent_hash: CryptoHash) -> Approval {
        Approval { account_id, reference_hash, parent_hash, signature: Signature::default() }
    }

    fn print_chain(chain: &mut Chain, mut hash: CryptoHash) {
        while hash != CryptoHash::default() {
            let header = chain.get_block_header(&hash).unwrap();
            println!(
                "    {}: {} (epoch: {}, qv: {}, qc: {}), approvals: {:?}",
                header.inner.height,
                header.hash(),
                header.inner.epoch_id.0,
                header.inner.last_quorum_pre_vote,
                header.inner.last_quorum_pre_commit,
                header.inner.approvals
            );
            hash = header.inner.prev_hash;
        }
    }

    fn check_safety(
        chain: &mut Chain,
        new_final_hash: CryptoHash,
        old_final_height: BlockIndex,
        old_final_hash: CryptoHash,
    ) {
        let on_chain_hash =
            chain.get_header_on_chain_by_height(&new_final_hash, old_final_height).unwrap().hash();
        let ok = on_chain_hash == old_final_hash;

        if !ok {
            println!(
                "New hash: {:?}, new height: {:?}, on_chain_hash: {:?}",
                new_final_hash,
                chain.mut_store().get_block_height(&new_final_hash),
                on_chain_hash
            );
            print_chain(chain, new_final_hash);
            println!("Old hash: {:?}, old height: {:?}", old_final_hash, old_final_height,);
            print_chain(chain, old_final_hash);
            assert!(false);
        }
    }

    /// Tests safety with epoch switches
    ///
    /// Runs many iterations with the following parameters:
    ///  - complexity: number of blocks created during the iterations
    ///  - likelihood_random: how likely is a block to be built on top of random prev block
    ///  - likelihood_heavy: how likely is a block to be built on top of one of the blocks with the highest score
    ///  - likelihood_last: how likely is a block to be built on top of last built block
    ///  - adversaries: are there nodes that behave adversarially (if `true` then 2 out of 7 bps create approvals
    ///       randomly instead of following the protocol)
    ///
    /// Uses the same utility to perform epoch switches that the actual epoch manager uses, thus testing
    /// the actual live conditions. Uses two block producer sets that switch randomly between epochs.
    /// Makes sure that all the finalized blocks are on the same chain.
    ///
    #[test]
    fn test_fuzzy_safety() {
        for (complexity, num_iters) in vec![
            (30, 2000),
            //(20, 2000),
            (50, 100),
            (100, 50),
            (200, 50),
            (500, 20),
            (1000, 10),
            (2000, 10),
            (2000, 10),
        ] {
            for adversaries in vec![false, true] {
                let mut good_iters = 0;

                let block_producers1 = vec![
                    "test1.1".to_string(),
                    "test1.2".to_string(),
                    "test1.3".to_string(),
                    "test1.4".to_string(),
                    "test1.5".to_string(),
                    "test1.6".to_string(),
                    "test1.7".to_string(),
                ];
                let block_producers2 = vec![
                    "test2.1".to_string(),
                    "test2.2".to_string(),
                    "test2.3".to_string(),
                    "test2.4".to_string(),
                    "test2.5".to_string(),
                    "test2.6".to_string(),
                    "test2.7".to_string(),
                ];
                let total_block_producers = block_producers1.len();

                let mut epoch_to_bps = HashMap::new();

                for iter in 0..num_iters {
                    let likelihood_random = rand::thread_rng().gen_range(1, 3);
                    let likelihood_heavy = rand::thread_rng().gen_range(1, 11);
                    let likelihood_last = rand::thread_rng().gen_range(1, 11);

                    println!(
                        "Starting iteration {} at complexity {} and likelihoods {}, {}, {}",
                        iter, complexity, likelihood_random, likelihood_heavy, likelihood_last
                    );
                    let (mut chain, _, signer) = setup();
                    let mut em = setup_default_epoch_manager(
                        block_producers1.iter().map(|_| ("test", 1000000)).collect(),
                        10,
                        4,
                        7,
                        0,
                        50,
                        50,
                    );

                    let genesis_block = chain.get_block(&chain.genesis().hash()).unwrap().clone();

                    let mut last_final_block_hash = CryptoHash::default();
                    let mut last_final_block_height = 0;
                    let mut largest_height = 0;
                    let mut finalized_hashes = HashSet::new();
                    let mut largest_weight: HashMap<AccountId, Weight> = HashMap::new();
                    let mut largest_score: HashMap<AccountId, Weight> = HashMap::new();
                    let mut last_approvals: HashMap<CryptoHash, HashMap<AccountId, Approval>> =
                        HashMap::new();

                    let mut all_blocks = vec![genesis_block.clone()];
                    record_block(
                        &mut em,
                        genesis_block.header.inner.prev_hash,
                        genesis_block.hash(),
                        genesis_block.header.inner.height,
                        vec![],
                    );
                    for _i in 0..complexity {
                        let max_score =
                            all_blocks.iter().map(|block| block.header.inner.score).max().unwrap();
                        let random_max_score_block = all_blocks
                            .iter()
                            .filter(|block| block.header.inner.score == max_score)
                            .collect::<Vec<_>>()
                            .choose(&mut rand::thread_rng())
                            .unwrap()
                            .clone()
                            .clone();
                        let last_block = all_blocks.last().unwrap().clone();
                        let prev_block = (0..likelihood_random)
                            .map(|_| all_blocks.choose(&mut rand::thread_rng()).unwrap())
                            .chain((0..likelihood_heavy).map(|_| &random_max_score_block))
                            .chain((0..likelihood_last).map(|_| &last_block))
                            .collect::<Vec<_>>()
                            .choose(&mut rand::thread_rng())
                            .unwrap()
                            .clone();
                        let mut last_approvals_entry = last_approvals
                            .get(&prev_block.hash())
                            .unwrap_or(&HashMap::new())
                            .clone();
                        let mut approvals = vec![];

                        let block_producers = epoch_to_bps
                            .entry(prev_block.header.inner.epoch_id.0)
                            .or_insert_with(|| {
                                vec![block_producers1.clone(), block_producers2.clone()]
                                    .choose(&mut rand::thread_rng())
                                    .unwrap()
                                    .clone()
                            });

                        for (i, block_producer) in block_producers.iter().enumerate() {
                            if rand::thread_rng().gen::<bool>() {
                                continue;
                            }

                            let reference_hash = if i < 2 && adversaries {
                                // malicious
                                let prev_reference = if let Some(prev_approval) =
                                    last_approvals_entry.get(block_producer)
                                {
                                    prev_approval.reference_hash
                                } else {
                                    genesis_block.hash().clone()
                                };

                                let mut possible_references = vec![prev_reference];
                                {
                                    let mut prev_block_hash = prev_block.hash();
                                    for _j in 0..10 {
                                        if prev_block_hash == prev_reference {
                                            break;
                                        }
                                        possible_references.push(prev_block_hash);
                                        prev_block_hash = chain
                                            .mut_store()
                                            .get_block_header(&prev_block_hash)
                                            .unwrap()
                                            .inner
                                            .prev_hash;
                                    }
                                }

                                possible_references.choose(&mut rand::thread_rng()).unwrap().clone()
                            } else {
                                // honest
                                let old_largest_weight =
                                    *largest_weight.get(block_producer).unwrap_or(&0u128.into());
                                let old_largest_score =
                                    *largest_score.get(block_producer).unwrap_or(&0u128.into());

                                match FinalityGadget::get_my_approval_reference_hash_inner(
                                    prev_block.hash(),
                                    last_approvals_entry.get(block_producer).cloned(),
                                    old_largest_weight,
                                    old_largest_score,
                                    chain.mut_store(),
                                ) {
                                    Some(hash) => hash,
                                    None => continue,
                                }
                            };

                            let approval = apr(
                                block_producer.clone(),
                                reference_hash.clone(),
                                prev_block.hash(),
                            );
                            approvals.push(approval.clone());
                            last_approvals_entry.insert(block_producer.clone(), approval);
                            largest_weight.insert(
                                block_producer.clone(),
                                prev_block.header.inner.total_weight,
                            );
                            largest_score
                                .insert(block_producer.clone(), prev_block.header.inner.score);
                        }

                        let new_block = create_block(
                            &mut em,
                            &prev_block,
                            prev_block.header.inner.height + 1,
                            chain.mut_store(),
                            &*signer,
                            approvals,
                            total_block_producers,
                        );

                        let final_block = new_block.header.inner.last_quorum_pre_commit;
                        if final_block != CryptoHash::default() {
                            let new_final_block_height =
                                chain.get_block_header(&final_block).unwrap().inner.height;
                            if last_final_block_height != 0 {
                                if new_final_block_height > last_final_block_height {
                                    check_safety(
                                        &mut chain,
                                        final_block,
                                        last_final_block_height,
                                        last_final_block_hash,
                                    );
                                } else if new_final_block_height < last_final_block_height {
                                    check_safety(
                                        &mut chain,
                                        last_final_block_hash,
                                        new_final_block_height,
                                        final_block,
                                    );
                                } else {
                                    if final_block != last_final_block_hash {
                                        print_chain(&mut chain, final_block);
                                        print_chain(&mut chain, last_final_block_hash);
                                        assert_eq!(final_block, last_final_block_hash);
                                    }
                                }
                            }

                            finalized_hashes.insert(final_block);

                            if new_final_block_height >= last_final_block_height {
                                last_final_block_hash = final_block;
                                last_final_block_height = new_final_block_height;
                            }
                        }

                        if new_block.header.inner.height > largest_height {
                            largest_height = new_block.header.inner.height;
                        }

                        last_approvals.insert(new_block.hash().clone(), last_approvals_entry);

                        all_blocks.push(new_block);
                    }
                    println!("Finished iteration {}, largest finalized height: {}, largest height: {}, final blocks: {}", iter, last_final_block_height, largest_height, finalized_hashes.len());
                    if last_final_block_height > 0 {
                        good_iters += 1;
                    }
                }
                println!("Good iterations: {}/{}", good_iters, num_iters);
                if complexity < 100 {
                    assert!(good_iters >= num_iters / 4);
                } else if complexity < 500 {
                    assert!(good_iters >= num_iters / 2);
                } else {
                    assert_eq!(good_iters, num_iters);
                }
            }
        }
    }
}
