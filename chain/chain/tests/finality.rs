use near_chain::test_utils::setup;
use near_chain::{ChainStore, ChainStoreAccess, ChainStoreUpdate};
use near_chain::{FinalityGadget, FinalityGadgetQuorums};
use near_crypto::{Signature, Signer};
use near_primitives::block::{Approval, Block, Weight};
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, BlockIndex};
use near_store::test_utils::create_test_store;
use rand::seq::SliceRandom;
use rand::Rng;
use std::collections::{HashMap, HashSet};

fn compute_quorums_slow(
    mut prev_hash: CryptoHash,
    approvals: Vec<Approval>,
    chain_store: &mut dyn ChainStoreAccess,
    total_block_producers: usize,
) -> FinalityGadgetQuorums {
    let mut all_approvals = approvals;

    let mut quorum_pre_vote = CryptoHash::default();
    let mut quorum_pre_commit = CryptoHash::default();

    let mut all_heights_and_hashes = vec![];

    while prev_hash != CryptoHash::default() {
        let block_header = chain_store.get_block_header(&prev_hash).unwrap();

        all_heights_and_hashes.push((block_header.hash().clone(), block_header.inner.height));

        prev_hash = block_header.inner.prev_hash.clone();
        all_approvals.extend(block_header.inner.approvals.clone());
    }

    let all_approvals = all_approvals
        .into_iter()
        .map(|approval| {
            let reference_height =
                chain_store.get_block_header(&approval.reference_hash).unwrap().inner.height;
            let parent_height =
                chain_store.get_block_header(&approval.parent_hash).unwrap().inner.height;

            assert!(reference_height <= parent_height);

            (approval.account_id, reference_height, parent_height)
        })
        .collect::<Vec<_>>();

    for (hash, height) in all_heights_and_hashes.iter().rev() {
        let mut surrounding = HashSet::new();
        for approval in all_approvals.iter() {
            if approval.1 <= *height && approval.2 >= *height {
                surrounding.insert(approval.0.clone());
            }
        }

        if surrounding.len() > total_block_producers * 2 / 3 {
            quorum_pre_vote = hash.clone();
        }

        for (_, other_height) in all_heights_and_hashes.iter().rev() {
            if other_height > height {
                let mut surrounding_both = HashSet::new();
                let mut surrounding_left = HashSet::new();
                for approval in all_approvals.iter() {
                    if approval.1 <= *height
                        && approval.2 >= *height
                        && approval.1 <= *other_height
                        && approval.2 >= *other_height
                    {
                        surrounding_both.insert(approval.0.clone());
                    }
                    if approval.1 <= *height && approval.2 >= *height && approval.2 < *other_height
                    {
                        surrounding_left.insert(approval.0.clone());
                    }
                }

                if surrounding_both.len() > total_block_producers * 2 / 3
                    && surrounding_left.len() > total_block_producers * 2 / 3
                {
                    quorum_pre_commit = hash.clone();
                }
            }
        }
    }

    FinalityGadgetQuorums {
        last_quorum_pre_vote: quorum_pre_vote,
        last_quorum_pre_commit: quorum_pre_commit,
    }
}

fn create_block(
    prev: &Block,
    height: BlockIndex,
    chain_store: &mut ChainStore,
    signer: &dyn Signer,
    approvals: Vec<Approval>,
    total_block_producers: usize,
) -> Block {
    let fg = FinalityGadget {};
    let mut block = Block::empty(prev, signer);
    block.header.inner.approvals = approvals.clone();
    block.header.inner.height = height;
    block.header.inner.total_weight = (height as u128).into();

    /*println!(
        "Creating block at height {} with parent {:?} and approvals {:?}",
        height,
        prev.hash(),
        approvals
    );*/

    let slow_quorums =
        compute_quorums_slow(prev.hash(), approvals.clone(), chain_store, total_block_producers)
            .clone();
    let fast_quorums = fg
        .compute_quorums(prev.hash(), height, approvals.clone(), chain_store, total_block_producers)
        .unwrap()
        .clone();

    block.header.inner.last_quorum_pre_vote = fast_quorums.last_quorum_pre_vote;
    block.header.inner.last_quorum_pre_commit = fast_quorums.last_quorum_pre_commit;

    block.header.inner.score = if fast_quorums.last_quorum_pre_vote == CryptoHash::default() {
        0.into()
    } else {
        chain_store.get_block_header(&fast_quorums.last_quorum_pre_vote).unwrap().inner.total_weight
    };

    block.header.init();

    //println!("Created; Hash: {:?}", block.hash());

    assert_eq!(slow_quorums, fast_quorums);

    let mut chain_store_update = ChainStoreUpdate::new(chain_store);
    chain_store_update.save_block_header(block.header.clone());
    chain_store_update.commit().unwrap();
    block
}

fn apr(account_id: AccountId, reference_hash: CryptoHash, parent_hash: CryptoHash) -> Approval {
    Approval { account_id, reference_hash, parent_hash, signature: Signature::default() }
}

#[test]
fn test_finality_genesis() {
    let store = create_test_store();
    let mut chain_store = ChainStore::new(store);

    let expected_quorums = FinalityGadgetQuorums {
        last_quorum_pre_vote: CryptoHash::default(),
        last_quorum_pre_commit: CryptoHash::default(),
    };
    let slow_quorums =
        compute_quorums_slow(CryptoHash::default(), vec![], &mut chain_store, 10).clone();
    let fast_quorums =
        compute_quorums_slow(CryptoHash::default(), vec![], &mut chain_store, 10).clone();

    assert_eq!(expected_quorums, slow_quorums);
    assert_eq!(expected_quorums, fast_quorums);
}

#[test]
fn test_finality_genesis2() {
    let (mut chain, _, signer) = setup();
    let fg = FinalityGadget {};
    let total_block_producers = 4;

    let genesis_block = chain.get_block(&chain.genesis().hash()).unwrap().clone();

    let block1 = create_block(
        &genesis_block,
        1,
        chain.mut_store(),
        &*signer,
        vec![
            apr("test1".to_string(), genesis_block.hash(), genesis_block.hash()),
            apr("test2".to_string(), genesis_block.hash(), genesis_block.hash()),
            apr("test3".to_string(), genesis_block.hash(), genesis_block.hash()),
        ],
        total_block_producers,
    );

    let expected_quorums = FinalityGadgetQuorums {
        last_quorum_pre_vote: genesis_block.hash(),
        last_quorum_pre_commit: CryptoHash::default(),
    };

    let slow_quorums =
        compute_quorums_slow(block1.hash(), vec![], chain.mut_store(), total_block_producers)
            .clone();
    let fast_quorums = fg
        .compute_quorums(block1.hash(), 2, vec![], chain.mut_store(), total_block_producers)
        .unwrap()
        .clone();

    assert_eq!(expected_quorums, slow_quorums);
    assert_eq!(expected_quorums, fast_quorums);
}

#[test]
fn test_finality_basic() {
    let (mut chain, _, signer) = setup();
    let fg = FinalityGadget {};
    let total_block_producers = 4;

    let genesis_block = chain.get_block(&chain.genesis().hash()).unwrap().clone();

    let block1 =
        create_block(&genesis_block, 1, chain.mut_store(), &*signer, vec![], total_block_producers);
    let block2 = create_block(
        &block1,
        2,
        chain.mut_store(),
        &*signer,
        vec![
            apr("test1".to_string(), block1.hash(), block1.hash()),
            apr("test2".to_string(), block1.hash(), block1.hash()),
            apr("test3".to_string(), block1.hash(), block1.hash()),
        ],
        total_block_producers,
    );
    let block3 = create_block(
        &block2,
        3,
        chain.mut_store(),
        &*signer,
        vec![
            apr("test1".to_string(), block1.hash(), block2.hash()),
            apr("test2".to_string(), block1.hash(), block2.hash()),
            apr("test3".to_string(), block1.hash(), block2.hash()),
        ],
        total_block_producers,
    );

    let expected_quorums = FinalityGadgetQuorums {
        last_quorum_pre_vote: block2.hash(),
        last_quorum_pre_commit: block1.hash(),
    };

    let slow_quorums =
        compute_quorums_slow(block3.hash(), vec![], chain.mut_store(), total_block_producers)
            .clone();
    let fast_quorums = fg
        .compute_quorums(block3.hash(), 4, vec![], chain.mut_store(), total_block_producers)
        .unwrap()
        .clone();

    assert_eq!(expected_quorums, slow_quorums);
    assert_eq!(expected_quorums, fast_quorums);
}

#[test]
fn test_finality_fewer_approvals_per_block() {
    let (mut chain, _, signer) = setup();
    let fg = FinalityGadget {};
    let total_block_producers = 4;

    let genesis_block = chain.get_block(&chain.genesis().hash()).unwrap().clone();

    let block1 =
        create_block(&genesis_block, 1, chain.mut_store(), &*signer, vec![], total_block_producers);
    let block2 = create_block(
        &block1,
        2,
        chain.mut_store(),
        &*signer,
        vec![
            apr("test1".to_string(), block1.hash(), block1.hash()),
            apr("test2".to_string(), block1.hash(), block1.hash()),
        ],
        total_block_producers,
    );
    let block3 = create_block(
        &block2,
        3,
        chain.mut_store(),
        &*signer,
        vec![
            apr("test1".to_string(), block1.hash(), block2.hash()),
            apr("test3".to_string(), block1.hash(), block2.hash()),
        ],
        total_block_producers,
    );
    let block4 = create_block(
        &block3,
        4,
        chain.mut_store(),
        &*signer,
        vec![
            apr("test1".to_string(), block1.hash(), block3.hash()),
            apr("test2".to_string(), block1.hash(), block3.hash()),
        ],
        total_block_producers,
    );
    let block5 = create_block(
        &block4,
        5,
        chain.mut_store(),
        &*signer,
        vec![
            apr("test1".to_string(), block1.hash(), block4.hash()),
            apr("test3".to_string(), block1.hash(), block4.hash()),
        ],
        total_block_producers,
    );

    let expected_quorums = FinalityGadgetQuorums {
        last_quorum_pre_vote: block3.hash(),
        last_quorum_pre_commit: block1.hash(),
    };

    let slow_quorums =
        compute_quorums_slow(block5.hash(), vec![], chain.mut_store(), total_block_producers)
            .clone();
    let fast_quorums = fg
        .compute_quorums(block5.hash(), 6, vec![], chain.mut_store(), total_block_producers)
        .unwrap()
        .clone();

    assert_eq!(expected_quorums, slow_quorums);
    assert_eq!(expected_quorums, fast_quorums);
}

#[test]
fn test_finality_quorum_precommit_cases() {
    for target in 0..=1 {
        let (mut chain, _, signer) = setup();
        let fg = FinalityGadget {};
        let total_block_producers = 4;

        let genesis_block = chain.get_block(&chain.genesis().hash()).unwrap().clone();

        let block1 = create_block(
            &genesis_block,
            1,
            chain.mut_store(),
            &*signer,
            vec![],
            total_block_producers,
        );
        let block2 =
            create_block(&block1, 2, chain.mut_store(), &*signer, vec![], total_block_producers);

        let block3 = create_block(
            &block2,
            3,
            chain.mut_store(),
            &*signer,
            vec![
                apr("test1".to_string(), block1.hash(), block2.hash()),
                apr("test2".to_string(), block1.hash(), block2.hash()),
                apr("test3".to_string(), block1.hash(), block2.hash()),
            ],
            total_block_producers,
        );

        let target_hash = if target == 0 { block1.hash() } else { block3.hash() };

        let block4 = create_block(
            &block3,
            4,
            chain.mut_store(),
            &*signer,
            vec![
                apr("test1".to_string(), target_hash, block3.hash()),
                apr("test2".to_string(), target_hash, block3.hash()),
                apr("test3".to_string(), target_hash, block3.hash()),
            ],
            total_block_producers,
        );

        let expected_quorums = FinalityGadgetQuorums {
            last_quorum_pre_vote: block3.hash(),
            last_quorum_pre_commit: if target == 0 { block2.hash() } else { CryptoHash::default() },
        };

        let slow_quorums =
            compute_quorums_slow(block4.hash(), vec![], chain.mut_store(), total_block_producers)
                .clone();
        let fast_quorums = fg
            .compute_quorums(block4.hash(), 5, vec![], chain.mut_store(), total_block_producers)
            .unwrap()
            .clone();

        assert_eq!(expected_quorums, slow_quorums);
        assert_eq!(expected_quorums, fast_quorums);
    }
}

#[test]
fn test_my_approvals() {
    let (mut chain, _, signer) = setup();
    let fg = FinalityGadget {};
    let total_block_producers = 4;
    let account_id = "test".to_string();

    let genesis_block = chain.get_block(&chain.genesis().hash()).unwrap().clone();

    let block1 =
        create_block(&genesis_block, 1, chain.mut_store(), &*signer, vec![], total_block_producers);
    let block2 =
        create_block(&block1, 2, chain.mut_store(), &*signer, vec![], total_block_producers);
    let block3 =
        create_block(&block2, 3, chain.mut_store(), &*signer, vec![], total_block_producers);
    let block4 =
        create_block(&block3, 4, chain.mut_store(), &*signer, vec![], total_block_producers);
    let block5 =
        create_block(&block1, 5, chain.mut_store(), &*signer, vec![], total_block_producers);
    let block6 =
        create_block(&block4, 6, chain.mut_store(), &*signer, vec![], total_block_producers);
    let block7 =
        create_block(&block6, 7, chain.mut_store(), &*signer, vec![], total_block_producers);
    let block8 =
        create_block(&block6, 8, chain.mut_store(), &*signer, vec![], total_block_producers);
    let block9 =
        create_block(&block7, 9, chain.mut_store(), &*signer, vec![], total_block_producers);

    // Intentionally skipping block8 in both expeted and in the loop below, block8 is processed
    //     separately at the end
    let expected_reference_hashes = vec![
        block1.hash(), // for block1
        block1.hash(), // ...
        block1.hash(),
        block1.hash(),
        block5.hash(),
        block6.hash(), // ...
        block6.hash(), // for block7
        block6.hash(), // for block9
    ];

    for (i, (block, expected_reference)) in
        vec![block1, block2, block3, block4, block5, block6, block7, block9]
            .into_iter()
            .zip(expected_reference_hashes)
            .enumerate()
    {
        println!("Block {}", i);

        let reference_hash =
            fg.get_my_approval_reference_hash(block.hash(), chain.mut_store()).unwrap();
        assert_eq!(reference_hash, expected_reference);
        let approval = Approval::new(block.hash(), reference_hash, &*signer, account_id.clone());
        let mut chain_store_update = ChainStoreUpdate::new(chain.mut_store());
        fg.process_approval(&Some(account_id.clone()), &approval, &mut chain_store_update).unwrap();
        chain_store_update.commit().unwrap();
    }

    let reference_hash = fg.get_my_approval_reference_hash(block8.hash(), chain.mut_store());
    assert!(reference_hash.is_none());
}

#[test]
fn test_fuzzy_finality() {
    let num_complexities = 20;
    let num_iters = 10;

    let block_producers =
        vec!["test1".to_string(), "test2".to_string(), "test3".to_string(), "test4".to_string()];
    let total_block_producers = block_producers.len();

    for complexity in 1..=num_complexities {
        for iter in 0..num_iters {
            println!("Starting iteration {} at complexity {}", iter, complexity);
            let (mut chain, _, signer) = setup();

            let genesis_block = chain.get_block(&chain.genesis().hash()).unwrap().clone();

            let mut last_approvals: HashMap<CryptoHash, HashMap<AccountId, Approval>> =
                HashMap::new();

            let mut all_blocks = vec![genesis_block.clone()];
            for _i in 0..complexity {
                let prev_block = all_blocks.choose(&mut rand::thread_rng()).unwrap();
                let mut last_approvals_entry =
                    last_approvals.get(&prev_block.hash()).unwrap_or(&HashMap::new()).clone();
                let mut approvals = vec![];
                for block_producer in block_producers.iter() {
                    if rand::thread_rng().gen::<bool>() {
                        continue;
                    }
                    let prev_reference =
                        if let Some(prev_approval) = last_approvals_entry.get(block_producer) {
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

                    let reference_block =
                        possible_references.choose(&mut rand::thread_rng()).unwrap();
                    let approval =
                        apr(block_producer.clone(), reference_block.clone(), prev_block.hash());
                    approvals.push(approval.clone());
                    last_approvals_entry.insert(block_producer.clone(), approval);
                }

                let new_block = create_block(
                    &prev_block,
                    prev_block.header.inner.height + 1,
                    chain.mut_store(),
                    &*signer,
                    approvals,
                    total_block_producers,
                );

                last_approvals.insert(new_block.hash().clone(), last_approvals_entry);

                all_blocks.push(new_block);
            }
        }
    }
}

#[test]
fn test_fuzzy_safety() {
    for adversaries in vec![false, true] {
        for (complexity, num_iters) in vec![(10, 100), (20, 100), (50, 10), (100, 2)] {
            let mut good_iters = 0;

            let block_producers = vec![
                "test1".to_string(),
                "test2".to_string(),
                "test3".to_string(),
                "test4".to_string(),
                "test5".to_string(),
                "test6".to_string(),
                "test7".to_string(),
            ];
            let total_block_producers = block_producers.len();

            for iter in 0..num_iters {
                println!("Starting iteration {} at complexity {}", iter, complexity);
                let (mut chain, _, signer) = setup();

                let genesis_block = chain.get_block(&chain.genesis().hash()).unwrap().clone();

                let mut last_final_block_hash = CryptoHash::default();
                let mut last_final_block_height = 0;
                let mut largest_weight: HashMap<AccountId, Weight> = HashMap::new();
                let mut largest_score: HashMap<AccountId, Weight> = HashMap::new();
                let mut last_approvals: HashMap<CryptoHash, HashMap<AccountId, Approval>> =
                    HashMap::new();

                let mut all_blocks = vec![genesis_block.clone()];
                for _i in 0..complexity {
                    let max_score =
                        all_blocks.iter().map(|block| block.header.inner.score).max().unwrap();
                    let prev_block = [
                        all_blocks.choose(&mut rand::thread_rng()).unwrap().clone(),
                        all_blocks
                            .iter()
                            .filter(|block| block.header.inner.score == max_score)
                            .collect::<Vec<_>>()
                            .choose(&mut rand::thread_rng())
                            .unwrap()
                            .clone()
                            .clone(),
                        all_blocks.last().unwrap().clone(),
                        all_blocks.last().unwrap().clone(),
                    ]
                    .choose(&mut rand::thread_rng())
                    .unwrap()
                    .clone();
                    let mut last_approvals_entry =
                        last_approvals.get(&prev_block.hash()).unwrap_or(&HashMap::new()).clone();
                    let mut approvals = vec![];
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
                            let fg = FinalityGadget {};
                            let old_largest_weight =
                                *largest_weight.get(block_producer).unwrap_or(&0u128.into());
                            let old_largest_score =
                                *largest_score.get(block_producer).unwrap_or(&0u128.into());

                            match fg.get_my_approval_reference_hash_inner(
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

                        let approval =
                            apr(block_producer.clone(), reference_hash.clone(), prev_block.hash());
                        approvals.push(approval.clone());
                        last_approvals_entry.insert(block_producer.clone(), approval);
                        largest_weight
                            .insert(block_producer.clone(), prev_block.header.inner.total_weight);
                        largest_score.insert(block_producer.clone(), prev_block.header.inner.score);
                    }

                    let new_block = create_block(
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
                                assert_eq!(
                                    chain
                                        .get_header_on_chain_by_height(
                                            &final_block,
                                            last_final_block_height
                                        )
                                        .unwrap()
                                        .hash(),
                                    last_final_block_hash
                                );
                            } else if new_final_block_height < last_final_block_height {
                                assert_eq!(
                                    chain
                                        .get_header_on_chain_by_height(
                                            &last_final_block_hash,
                                            new_final_block_height
                                        )
                                        .unwrap()
                                        .hash(),
                                    final_block
                                );
                            } else {
                                assert_eq!(final_block, last_final_block_hash);
                            }
                        }
                        last_final_block_hash = final_block;
                        last_final_block_height = new_final_block_height;
                    }

                    last_approvals.insert(new_block.hash().clone(), last_approvals_entry);

                    all_blocks.push(new_block);
                }
                if last_final_block_height > 0 {
                    good_iters += 1;
                }
            }
            println!("Good iterations: {}/{}", good_iters, num_iters);
            assert!(good_iters > num_iters / 4);
        }
    }
}
