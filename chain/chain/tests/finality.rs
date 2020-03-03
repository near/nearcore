use std::collections::{HashMap, HashSet};

use rand::seq::SliceRandom;
use rand::Rng;

use near_chain::test_utils::setup;
use near_chain::{ChainStore, ChainStoreAccess, ChainStoreUpdate};
use near_chain::{FinalityGadget, FinalityGadgetQuorums};
use near_crypto::{KeyType, PublicKey, Signature};
use near_primitives::block::{Approval, Block};
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, Balance, BlockHeight, EpochId, ValidatorStake};
use near_primitives::validator_signer::ValidatorSigner;
use near_store::test_utils::create_test_store;

fn compute_quorums_slow(
    mut prev_hash: CryptoHash,
    approvals: Vec<Approval>,
    chain_store: &mut dyn ChainStoreAccess,
    stakes: Vec<ValidatorStake>,
) -> FinalityGadgetQuorums {
    let mut all_approvals = approvals;

    let mut quorum_pre_vote = CryptoHash::default();
    let mut quorum_pre_commit = CryptoHash::default();

    let mut all_heights_and_hashes = vec![];

    let account_id_to_stake =
        stakes.iter().map(|x| (&x.account_id, x.stake)).collect::<HashMap<_, _>>();
    assert!(account_id_to_stake.len() == stakes.len());
    let threshold = account_id_to_stake.values().sum::<u128>() * 2u128 / 3u128;

    while prev_hash != CryptoHash::default() {
        let block_header = chain_store.get_block_header(&prev_hash).unwrap();

        all_heights_and_hashes.push((block_header.hash().clone(), block_header.inner_lite.height));

        prev_hash = block_header.prev_hash.clone();
        all_approvals.extend(block_header.inner_rest.approvals.clone());
    }

    let all_approvals = all_approvals
        .into_iter()
        .map(|approval| {
            let reference_height = chain_store
                .get_block_header(&approval.reference_hash.unwrap())
                .unwrap()
                .inner_lite
                .height;
            let parent_height =
                chain_store.get_block_header(&approval.parent_hash).unwrap().inner_lite.height;

            assert!(reference_height <= parent_height);

            (approval.account_id, reference_height, parent_height)
        })
        .collect::<Vec<_>>();

    for (hash, height) in all_heights_and_hashes.iter().rev() {
        let mut surrounding = HashSet::new();
        let mut surrounding_stake = 0 as Balance;
        for approval in all_approvals.iter() {
            if approval.1 <= *height && approval.2 >= *height {
                if !surrounding.contains(&approval.0) {
                    surrounding_stake += *account_id_to_stake.get(&approval.0).unwrap();
                    surrounding.insert(approval.0.clone());
                }
            }
        }

        if surrounding_stake > threshold {
            quorum_pre_vote = hash.clone();
        }

        for (_, other_height) in all_heights_and_hashes.iter().rev() {
            if other_height > height {
                let mut surrounding_both = HashSet::new();
                let mut surrounding_left = HashSet::new();
                let mut surrounding_both_stake = 0 as Balance;
                let mut surrounding_left_stake = 0 as Balance;
                for approval in all_approvals.iter() {
                    if approval.1 <= *height
                        && approval.2 >= *height
                        && approval.1 <= *other_height
                        && approval.2 >= *other_height
                        && !surrounding_both.contains(&approval.0)
                    {
                        surrounding_both_stake += *account_id_to_stake.get(&approval.0).unwrap();
                        surrounding_both.insert(approval.0.clone());
                    }
                    if approval.1 <= *height
                        && approval.2 >= *height
                        && approval.2 < *other_height
                        && !surrounding_left.contains(&approval.0)
                    {
                        surrounding_left_stake += *account_id_to_stake.get(&approval.0).unwrap();
                        surrounding_left.insert(approval.0.clone());
                    }
                }

                if surrounding_both_stake > threshold && surrounding_left_stake > threshold {
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
    height: BlockHeight,
    chain_store: &mut ChainStore,
    signer: &dyn ValidatorSigner,
    approvals: Vec<Approval>,
    stakes: Vec<ValidatorStake>,
) -> Block {
    let mut block = Block::empty(prev, signer);
    block.header.inner_rest.approvals = approvals.clone();
    block.header.inner_lite.height = height;

    let slow_quorums =
        compute_quorums_slow(prev.hash(), approvals.clone(), chain_store, stakes.clone()).clone();
    let fast_quorums = FinalityGadget::compute_quorums(
        prev.hash(),
        EpochId(CryptoHash::default()),
        height,
        approvals.clone(),
        chain_store,
        &stakes.clone(),
    )
    .unwrap()
    .clone();

    block.header.inner_rest.last_quorum_pre_vote = fast_quorums.last_quorum_pre_vote;
    block.header.inner_rest.last_quorum_pre_commit = fast_quorums.last_quorum_pre_commit;

    block.header.inner_rest.score = if fast_quorums.last_quorum_pre_vote == CryptoHash::default() {
        0.into()
    } else {
        chain_store
            .get_block_header(&fast_quorums.last_quorum_pre_vote)
            .unwrap()
            .inner_lite
            .height
            .into()
    };

    block.header.init();

    assert_eq!(slow_quorums, fast_quorums);

    let mut chain_store_update = ChainStoreUpdate::new(chain_store);
    chain_store_update.save_block_header(block.header.clone());
    chain_store_update.commit().unwrap();
    block
}

fn apr(account_id: AccountId, reference_hash: CryptoHash, parent_hash: CryptoHash) -> Approval {
    Approval {
        account_id,
        reference_hash: Some(reference_hash),
        is_endorsement: true,
        target_height: 0,
        parent_hash,
        signature: Signature::default(),
    }
}

fn gen_stakes(n: usize) -> Vec<ValidatorStake> {
    (0..n)
        .map(|x| {
            ValidatorStake::new(format!("test{}", x + 1), PublicKey::empty(KeyType::ED25519), 1)
        })
        .collect()
}

#[test]
fn test_finality_genesis() {
    let store = create_test_store();
    let mut chain_store = ChainStore::new(store);

    let stakes = gen_stakes(10);

    let expected_quorums = FinalityGadgetQuorums {
        last_quorum_pre_vote: CryptoHash::default(),
        last_quorum_pre_commit: CryptoHash::default(),
    };
    let slow_quorums =
        compute_quorums_slow(CryptoHash::default(), vec![], &mut chain_store, stakes.clone())
            .clone();
    let fast_quorums =
        compute_quorums_slow(CryptoHash::default(), vec![], &mut chain_store, stakes.clone())
            .clone();

    assert_eq!(expected_quorums, slow_quorums);
    assert_eq!(expected_quorums, fast_quorums);
}

#[test]
fn test_finality_genesis2() {
    let (mut chain, _, signer) = setup();
    let stakes = gen_stakes(4);

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
        stakes.clone(),
    );

    let expected_quorums = FinalityGadgetQuorums {
        last_quorum_pre_vote: genesis_block.hash(),
        last_quorum_pre_commit: CryptoHash::default(),
    };

    let slow_quorums =
        compute_quorums_slow(block1.hash(), vec![], chain.mut_store(), stakes.clone()).clone();
    let fast_quorums = FinalityGadget::compute_quorums(
        block1.hash(),
        EpochId(CryptoHash::default()),
        2,
        vec![],
        chain.mut_store(),
        &stakes.clone(),
    )
    .unwrap()
    .clone();

    assert_eq!(expected_quorums, slow_quorums);
    assert_eq!(expected_quorums, fast_quorums);
}

#[test]
fn test_finality_basic() {
    let (mut chain, _, signer) = setup();
    let stakes = gen_stakes(4);

    let genesis_block = chain.get_block(&chain.genesis().hash()).unwrap().clone();

    let block1 =
        create_block(&genesis_block, 1, chain.mut_store(), &*signer, vec![], stakes.clone());
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
        stakes.clone(),
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
        stakes.clone(),
    );

    let expected_quorums = FinalityGadgetQuorums {
        last_quorum_pre_vote: block2.hash(),
        last_quorum_pre_commit: block1.hash(),
    };

    let slow_quorums =
        compute_quorums_slow(block3.hash(), vec![], chain.mut_store(), stakes.clone()).clone();
    let fast_quorums = FinalityGadget::compute_quorums(
        block3.hash(),
        EpochId(CryptoHash::default()),
        4,
        vec![],
        chain.mut_store(),
        &stakes.clone(),
    )
    .unwrap()
    .clone();

    assert_eq!(expected_quorums, slow_quorums);
    assert_eq!(expected_quorums, fast_quorums);
}

#[test]
fn test_finality_weight() {
    let (mut chain, _, signer) = setup();
    let mut stakes = gen_stakes(4);
    stakes[0].stake = 8;

    let genesis_block = chain.get_block(&chain.genesis().hash()).unwrap().clone();

    let block1 =
        create_block(&genesis_block, 1, chain.mut_store(), &*signer, vec![], stakes.clone());
    let block2 = create_block(
        &block1,
        2,
        chain.mut_store(),
        &*signer,
        vec![apr("test1".to_string(), block1.hash(), block1.hash())],
        stakes.clone(),
    );
    let block3 = create_block(
        &block2,
        3,
        chain.mut_store(),
        &*signer,
        vec![apr("test1".to_string(), block1.hash(), block2.hash())],
        stakes.clone(),
    );

    let expected_quorums = FinalityGadgetQuorums {
        last_quorum_pre_vote: block2.hash(),
        last_quorum_pre_commit: block1.hash(),
    };

    let slow_quorums =
        compute_quorums_slow(block3.hash(), vec![], chain.mut_store(), stakes.clone()).clone();
    let fast_quorums = FinalityGadget::compute_quorums(
        block3.hash(),
        EpochId(CryptoHash::default()),
        4,
        vec![],
        chain.mut_store(),
        &stakes.clone(),
    )
    .unwrap()
    .clone();

    assert_eq!(expected_quorums, slow_quorums);
    assert_eq!(expected_quorums, fast_quorums);
}

#[test]
fn test_finality_fewer_approvals_per_block() {
    let (mut chain, _, signer) = setup();
    let stakes = gen_stakes(4);

    let genesis_block = chain.get_block(&chain.genesis().hash()).unwrap().clone();

    let block1 =
        create_block(&genesis_block, 1, chain.mut_store(), &*signer, vec![], stakes.clone());
    let block2 = create_block(
        &block1,
        2,
        chain.mut_store(),
        &*signer,
        vec![
            apr("test1".to_string(), block1.hash(), block1.hash()),
            apr("test2".to_string(), block1.hash(), block1.hash()),
        ],
        stakes.clone(),
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
        stakes.clone(),
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
        stakes.clone(),
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
        stakes.clone(),
    );

    let expected_quorums = FinalityGadgetQuorums {
        last_quorum_pre_vote: block3.hash(),
        last_quorum_pre_commit: block1.hash(),
    };

    let slow_quorums =
        compute_quorums_slow(block5.hash(), vec![], chain.mut_store(), stakes.clone()).clone();
    let fast_quorums = FinalityGadget::compute_quorums(
        block5.hash(),
        EpochId(CryptoHash::default()),
        6,
        vec![],
        chain.mut_store(),
        &stakes.clone(),
    )
    .unwrap()
    .clone();

    assert_eq!(expected_quorums, slow_quorums);
    assert_eq!(expected_quorums, fast_quorums);
}

#[test]
fn test_finality_quorum_precommit_cases() {
    for target in 0..=1 {
        let (mut chain, _, signer) = setup();
        let stakes = gen_stakes(4);

        let genesis_block = chain.get_block(&chain.genesis().hash()).unwrap().clone();

        let block1 =
            create_block(&genesis_block, 1, chain.mut_store(), &*signer, vec![], stakes.clone());
        let block2 = create_block(&block1, 2, chain.mut_store(), &*signer, vec![], stakes.clone());

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
            stakes.clone(),
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
            stakes.clone(),
        );

        let expected_quorums = FinalityGadgetQuorums {
            last_quorum_pre_vote: block3.hash(),
            last_quorum_pre_commit: if target == 0 { block2.hash() } else { CryptoHash::default() },
        };

        let slow_quorums =
            compute_quorums_slow(block4.hash(), vec![], chain.mut_store(), stakes.clone()).clone();
        let fast_quorums = FinalityGadget::compute_quorums(
            block4.hash(),
            EpochId(CryptoHash::default()),
            5,
            vec![],
            chain.mut_store(),
            &stakes.clone(),
        )
        .unwrap()
        .clone();

        assert_eq!(expected_quorums, slow_quorums);
        assert_eq!(expected_quorums, fast_quorums);
    }
}

#[test]
fn test_my_approvals() {
    let (mut chain, _, signer) = setup();
    let stakes = gen_stakes(4);

    let genesis_block = chain.get_block(&chain.genesis().hash()).unwrap().clone();

    let block1 =
        create_block(&genesis_block, 1, chain.mut_store(), &*signer, vec![], stakes.clone());
    let block2 = create_block(&block1, 2, chain.mut_store(), &*signer, vec![], stakes.clone());
    let block3 = create_block(&block2, 3, chain.mut_store(), &*signer, vec![], stakes.clone());
    let block4 = create_block(&block3, 4, chain.mut_store(), &*signer, vec![], stakes.clone());
    let block5 = create_block(&block1, 5, chain.mut_store(), &*signer, vec![], stakes.clone());
    let block6 = create_block(&block4, 6, chain.mut_store(), &*signer, vec![], stakes.clone());
    let block7 = create_block(&block6, 7, chain.mut_store(), &*signer, vec![], stakes.clone());
    let block8 = create_block(&block6, 8, chain.mut_store(), &*signer, vec![], stakes.clone());
    let block9 = create_block(&block7, 9, chain.mut_store(), &*signer, vec![], stakes.clone());

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
            FinalityGadget::get_my_approval_reference_hash(block.hash(), chain.mut_store())
                .unwrap();
        assert_eq!(reference_hash, expected_reference);
        let approval = Approval::new(block.hash(), Some(reference_hash), 0, true, &*signer);
        let mut chain_store_update = ChainStoreUpdate::new(chain.mut_store());
        FinalityGadget::process_approval(
            &Some(signer.validator_id().clone()),
            &approval,
            &mut chain_store_update,
        )
        .unwrap();
        chain_store_update.commit().unwrap();
    }

    let reference_hash =
        FinalityGadget::get_my_approval_reference_hash(block8.hash(), chain.mut_store());
    assert!(reference_hash.is_none());
}

#[test]
fn test_fuzzy_finality() {
    let num_complexities = 20;
    let num_iters = 10;

    let block_producers =
        vec!["test1".to_string(), "test2".to_string(), "test3".to_string(), "test4".to_string()];
    let stakes = gen_stakes(block_producers.len());

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

                    let reference_block =
                        possible_references.choose(&mut rand::thread_rng()).unwrap();
                    let approval =
                        apr(block_producer.clone(), reference_block.clone(), prev_block.hash());
                    approvals.push(approval.clone());
                    last_approvals_entry.insert(block_producer.clone(), approval);
                }

                let new_block = create_block(
                    &prev_block,
                    prev_block.header.inner_lite.height + 1,
                    chain.mut_store(),
                    &*signer,
                    approvals,
                    stakes.clone(),
                );

                last_approvals.insert(new_block.hash().clone(), last_approvals_entry);

                all_blocks.push(new_block);
            }
        }
    }
}
