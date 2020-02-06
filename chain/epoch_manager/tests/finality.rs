#[cfg(test)]
#[cfg(feature = "expensive_tests")]
mod tests {
    use std::collections::{HashMap, HashSet};

    use rand::seq::SliceRandom;
    use rand::Rng;

    use near_chain::test_utils::setup;
    use near_chain::{create_light_client_block_view, FinalityGadget};
    use near_chain::{Chain, ChainStore, ChainStoreAccess, ChainStoreUpdate};
    use near_crypto::{KeyType, PublicKey, Signature};
    use near_epoch_manager::test_utils::{record_block, setup_default_epoch_manager};
    use near_epoch_manager::EpochManager;
    use near_primitives::block::{Approval, Block, BlockHeader, BlockScore};
    use near_primitives::hash::CryptoHash;
    use near_primitives::merkle::combine_hash;
    use near_primitives::types::{AccountId, BlockHeight, EpochId, ValidatorStake};
    use near_primitives::validator_signer::ValidatorSigner;
    use near_primitives::views::ValidatorStakeView;

    fn create_block(
        em: &mut EpochManager,
        prev: &Block,
        height: BlockHeight,
        chain_store: &mut ChainStore,
        signer: &dyn ValidatorSigner,
        approvals: Vec<Approval>,
        stakes: &Vec<ValidatorStake>,
    ) -> (Block, CryptoHash) {
        let is_this_block_epoch_start = em.is_next_block_epoch_start(&prev.hash()).unwrap();

        let epoch_id = if is_this_block_epoch_start {
            // This is a bit weird way to define an epoch, but replicating the exact way we use today is
            // unnecessarily complex. Using last `pre_commit` is sufficient to ensure that the `epoch_id`
            // of the next epoch will be the same for as long as the last committed block in the prev
            // epoch was the same.
            EpochId(prev.header.inner_rest.last_quorum_pre_commit)
        } else {
            prev.header.inner_lite.epoch_id.clone()
        };

        let mut block = Block::empty(prev, signer);
        block.header.inner_rest.approvals = approvals.clone();
        block.header.inner_lite.height = height;
        block.header.inner_lite.epoch_id = epoch_id.clone();

        let quorums = FinalityGadget::compute_quorums(
            prev.hash(),
            epoch_id,
            height,
            approvals.clone(),
            chain_store,
            &stakes,
        )
        .unwrap()
        .clone();

        block.header.inner_rest.last_quorum_pre_vote = quorums.last_quorum_pre_vote;
        block.header.inner_rest.last_quorum_pre_commit = em
            .push_final_block_back_if_needed(prev.hash(), quorums.last_quorum_pre_commit)
            .unwrap();

        block.header.inner_rest.score = if quorums.last_quorum_pre_vote == CryptoHash::default() {
            0.into()
        } else {
            chain_store
                .get_block_header(&quorums.last_quorum_pre_vote)
                .unwrap()
                .inner_lite
                .height
                .into()
        };

        block.header.init();

        let mut chain_store_update = ChainStoreUpdate::new(chain_store);
        chain_store_update.save_block_header(block.header.clone());
        chain_store_update.commit().unwrap();

        record_block(
            em,
            block.header.prev_hash,
            block.hash(),
            block.header.inner_lite.height,
            vec![],
        );

        (block, quorums.last_quorum_pre_commit)
    }

    fn apr(account_id: AccountId, reference_hash: CryptoHash, parent_hash: CryptoHash) -> Approval {
        Approval {
            account_id,
            reference_hash: Some(reference_hash),
            target_height: 0,
            is_endorsement: true,
            parent_hash,
            signature: Signature::default(),
        }
    }

    fn print_chain(chain: &mut Chain, mut hash: CryptoHash) {
        while hash != CryptoHash::default() {
            let header = chain.get_block_header(&hash).unwrap();
            println!(
                "    {}: {} (epoch: {}, qv: {}, qc: {}), approvals: {:?}",
                header.inner_lite.height,
                header.hash(),
                header.inner_lite.epoch_id.0,
                header.inner_rest.last_quorum_pre_vote,
                header.inner_rest.last_quorum_pre_commit,
                header.inner_rest.approvals
            );
            hash = header.prev_hash;
        }
    }

    fn check_safety(
        chain: &mut Chain,
        new_final_hash: CryptoHash,
        old_final_height: BlockHeight,
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

    /// Verify that the conditions from the spec (https://github.com/nearprotocol/NEPs/pull/25) are passing
    fn check_light_client_block(
        block_header: &BlockHeader,
        pushed_back_commit_hash: &CryptoHash,
        chain_store: &mut ChainStore,
        stakes: &Vec<ValidatorStake>,
    ) {
        let stakes_view: Vec<ValidatorStakeView> =
            stakes.iter().map(|x| x.clone().into()).collect::<Vec<_>>();
        let light_client_block_view = create_light_client_block_view(
            block_header,
            pushed_back_commit_hash,
            chain_store,
            &stakes_view,
            None,
        )
        .unwrap();

        // imitate verifying light client block
        assert_eq!(light_client_block_view.qv_approvals.len(), stakes_view.len());
        assert_eq!(light_client_block_view.qc_approvals.len(), stakes_view.len());

        let mut qv_blocks = HashSet::new();
        let mut qc_blocks = HashSet::new();

        let mut prev_hash = block_header.hash();
        let mut passed_qv = false;
        for future_inner_hash in light_client_block_view.future_inner_hashes {
            let cur_hash = combine_hash(future_inner_hash, prev_hash);
            if cur_hash == light_client_block_view.qv_hash {
                passed_qv = true;
            }
            if passed_qv {
                qc_blocks.insert(cur_hash);
            } else {
                qv_blocks.insert(cur_hash);
            }

            prev_hash = cur_hash
        }
        assert!(passed_qv);

        let mut total_stake = 0;
        let mut qv_stake = 0;
        let mut qc_stake = 0;

        for (qv_approval, (qc_approval, stake)) in light_client_block_view
            .qv_approvals
            .iter()
            .zip(light_client_block_view.qc_approvals.iter().zip(stakes.iter()))
        {
            if let Some(qv_approval) = qv_approval {
                qv_stake += stake.stake;

                // each qv_approval must have the new_block or a block in the progeny of the new_block as parent block
                // and the new_block or a block in its ancestry as the reference block. qv_blocks has all the blocks
                // in the progeny of the new_block, excluding new_block itself
                if !qv_blocks.contains(&qv_approval.parent_hash)
                    && qv_approval.parent_hash != block_header.hash()
                {
                    assert!(false);
                }
                if qv_blocks.contains(&qv_approval.reference_hash) {
                    assert!(false);
                }
            }

            if let Some(qc_approval) = qc_approval {
                qc_stake += stake.stake;
                if !qc_blocks.contains(&qc_approval.parent_hash) {
                    assert!(false);
                }
                // The reference hash of the qc_approval must be the new_block itself, or in its ancestry.
                // Since the parent_hash is in the qc_blocks, and a valid approval has the reference has in the ancestry
                // of the parent_hash, it is enough to check that the reference check is not in qv_blocks or qc_blocks
                if qc_blocks.contains(&qc_approval.reference_hash)
                    || qv_blocks.contains(&qc_approval.reference_hash)
                {
                    assert!(false);
                }
            }

            total_stake += stake.stake;
        }

        let threshold = total_stake * 2 / 3;
        if qv_stake <= threshold {
            println!(
                "Total stake: {}, qv_stake: {}, qc_stake: {}",
                total_stake, qv_stake, qc_stake
            );
            assert!(false);
        }
        if qc_stake <= threshold {
            println!(
                "Total stake: {}, qv_stake: {}, qc_stake: {}",
                total_stake, qv_stake, qc_stake
            );
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
    fn test_fuzzy_safety_inner(test_light_client: bool) {
        for (complexity, num_iters) in vec![
            (30, if test_light_client { 200 } else { 2000 }),
            (50, 100),
            (100, 50),
            (200, 50),
            (500, 20),
            (1000, 10),
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

                let mut epoch_to_bps = HashMap::new();
                let mut epoch_to_stakes = HashMap::new();

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
                    let mut last_final_height = 0;
                    let mut largest_height = 0;
                    let mut finalized_hashes = HashSet::new();
                    let mut largest_heights: HashMap<AccountId, BlockHeight> = HashMap::new();
                    let mut largest_scores: HashMap<AccountId, BlockScore> = HashMap::new();
                    let mut last_approvals: HashMap<CryptoHash, HashMap<AccountId, Approval>> =
                        HashMap::new();

                    let mut all_blocks = vec![genesis_block.clone()];
                    record_block(
                        &mut em,
                        genesis_block.header.prev_hash,
                        genesis_block.hash(),
                        genesis_block.header.inner_lite.height,
                        vec![],
                    );
                    for _i in 0..complexity {
                        let max_score = all_blocks
                            .iter()
                            .map(|block| block.header.inner_rest.score)
                            .max()
                            .unwrap();
                        let random_max_score_block = all_blocks
                            .iter()
                            .filter(|block| block.header.inner_rest.score == max_score)
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
                            .entry(prev_block.header.inner_lite.epoch_id.0)
                            .or_insert_with(|| {
                                vec![block_producers1.clone(), block_producers2.clone()]
                                    .choose(&mut rand::thread_rng())
                                    .unwrap()
                                    .clone()
                            });

                        let stakes = epoch_to_stakes
                            .entry(prev_block.header.inner_lite.epoch_id.0)
                            .or_insert_with(|| {
                                if rand::thread_rng().gen() {
                                    block_producers
                                        .iter()
                                        .map(|account_id| {
                                            ValidatorStake::new(
                                                account_id.clone(),
                                                PublicKey::empty(KeyType::ED25519),
                                                rand::thread_rng().gen_range(100, 300),
                                            )
                                        })
                                        .collect::<Vec<_>>()
                                } else {
                                    block_producers
                                        .iter()
                                        .map(|account_id| {
                                            ValidatorStake::new(
                                                account_id.clone(),
                                                PublicKey::empty(KeyType::ED25519),
                                                1,
                                            )
                                        })
                                        .collect::<Vec<_>>()
                                }
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
                                    prev_approval.reference_hash.unwrap()
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
                                            .prev_hash;
                                    }
                                }

                                possible_references.choose(&mut rand::thread_rng()).unwrap().clone()
                            } else {
                                // honest
                                let old_largest_height =
                                    *largest_heights.get(block_producer).unwrap_or(&0u64);
                                let old_largest_score =
                                    *largest_scores.get(block_producer).unwrap_or(&0u64.into());

                                match FinalityGadget::get_my_approval_reference_hash_inner(
                                    prev_block.hash(),
                                    last_approvals_entry.get(block_producer).cloned(),
                                    old_largest_height,
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
                            largest_heights.insert(
                                block_producer.clone(),
                                prev_block.header.inner_lite.height,
                            );
                            largest_scores
                                .insert(block_producer.clone(), prev_block.header.inner_rest.score);
                        }

                        let (new_block, quorum_pre_commit_before_pushing_back) = create_block(
                            &mut em,
                            &prev_block,
                            prev_block.header.inner_lite.height + 1,
                            chain.mut_store(),
                            &*signer,
                            approvals,
                            &stakes,
                        );

                        let final_block_hash = new_block.header.inner_rest.last_quorum_pre_commit;
                        if final_block_hash != CryptoHash::default() {
                            let new_final_height = chain
                                .get_block_header(&final_block_hash)
                                .unwrap()
                                .inner_lite
                                .height;
                            if last_final_height != 0 {
                                if new_final_height > last_final_height {
                                    check_safety(
                                        &mut chain,
                                        final_block_hash,
                                        last_final_height,
                                        last_final_block_hash,
                                    );
                                } else if new_final_height < last_final_height {
                                    check_safety(
                                        &mut chain,
                                        last_final_block_hash,
                                        new_final_height,
                                        final_block_hash,
                                    );
                                } else {
                                    if final_block_hash != last_final_block_hash {
                                        print_chain(&mut chain, final_block_hash);
                                        print_chain(&mut chain, last_final_block_hash);
                                        assert_eq!(final_block_hash, last_final_block_hash);
                                    }
                                }
                            }

                            finalized_hashes.insert(final_block_hash);

                            if new_final_height > last_final_height {
                                if test_light_client {
                                    let mut chain_store_update =
                                        ChainStoreUpdate::new(chain.mut_store());
                                    let mut last_block_hash = new_block.hash();
                                    while last_block_hash != last_final_block_hash {
                                        let prev_header = chain_store_update
                                            .get_block_header(&last_block_hash)
                                            .unwrap();
                                        let prev_hash = prev_header.prev_hash;
                                        chain_store_update
                                            .save_next_block_hash(&prev_hash, last_block_hash);
                                        last_block_hash = prev_hash;
                                    }
                                    chain_store_update.commit().unwrap();
                                }

                                if test_light_client {
                                    let final_block_header = chain
                                        .get_block_header(&quorum_pre_commit_before_pushing_back)
                                        .unwrap()
                                        .clone();
                                    check_light_client_block(
                                        &final_block_header,
                                        &final_block_hash,
                                        chain.mut_store(),
                                        stakes,
                                    );
                                }

                                last_final_block_hash = final_block_hash;
                                last_final_height = new_final_height;
                            }
                        }

                        if new_block.header.inner_lite.height > largest_height {
                            largest_height = new_block.header.inner_lite.height;
                        }

                        last_approvals.insert(new_block.hash().clone(), last_approvals_entry);

                        all_blocks.push(new_block);
                    }
                    println!("Finished iteration {}, largest finalized height: {}, largest height: {}, final blocks: {}", iter, last_final_height, largest_height, finalized_hashes.len());
                    if last_final_height > 0 {
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

    #[test]
    fn test_fuzzy_safety() {
        test_fuzzy_safety_inner(false);
    }

    #[test]
    fn test_fuzzy_light_client() {
        test_fuzzy_safety_inner(true);
    }
}
