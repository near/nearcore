use crate::env::test_env::TestEnv;
use near_chain::{ChainStoreAccess, Provenance};
use near_chain_configs::Genesis;
use near_crypto::KeyType;
use near_o11y::testonly::init_test_logger;
use near_primitives::block::{Approval, ApprovalInner, ApprovalType};
use near_primitives::hash::CryptoHash;
use near_primitives::validator_signer::InMemoryValidatorSigner;
use std::sync::Arc;

/// This file contains tests that test the interaction of client and doomslug, including how client handles approvals, etc.
/// It does not include the unit tests for the Doomslug class. That is located in chain/chain/src/doomslug.rs

// This tests the scenario that if the chain switch back and forth in between two forks, client code
// can process the skip messages correctly and send it to doomslug. Specifically, it tests the following
// case:
// existing chain looks like 0 - 1
//                             \ 2
// test that if the node receives Skip(2, 4), it can process it successfully.
#[test]
fn test_processing_skips_on_forks() {
    init_test_logger();

    // We don't want to mock epoch manager and for forks we want to pick block heights that
    // correspond to different block producers.
    let first_fork_heigh = 1;
    let second_fork_heigh = 3;
    let genesis =
        Genesis::test(["test0", "test1"].into_iter().map(|acc| acc.parse().unwrap()).collect(), 2);
    let mut env =
        TestEnv::builder_from_genesis(&genesis).clients_count(2).validator_seats(2).build();
    let b1 = env.clients[1].produce_block(first_fork_heigh).unwrap().unwrap();
    let b2 = env.clients[0].produce_block(second_fork_heigh).unwrap().unwrap();
    assert_eq!(b1.header().prev_hash(), b2.header().prev_hash());
    env.process_block(1, b1, Provenance::NONE);
    env.process_block(1, b2, Provenance::NONE);
    let validator_signer =
        InMemoryValidatorSigner::from_seed("test1".parse().unwrap(), KeyType::ED25519, "test1");
    let approval =
        Approval::new(CryptoHash::default(), 1, second_fork_heigh + 1, &validator_signer);
    env.clients[1].collect_block_approval(&approval, ApprovalType::SelfApproval);
    assert!(
        !env.clients[1]
            .doomslug
            .approval_status_at_height(&(second_fork_heigh + 1))
            .approvals
            .is_empty()
    );
}

/// When a skip approval arrives and multiple blocks exist at the parent
/// height (e.g. forks around an epoch boundary), each block may belong to a
/// different epoch with a different block-producer schedule.  If the code
/// picks the "wrong" parent hash — one whose epoch assigns a *different*
/// validator as the producer for the target height — the approval is
/// silently dropped at the `!is_block_producer` guard and never reaches
/// Doomslug.
///
/// This test verifies that `collect_block_approval` iterates all candidate
/// parent hashes and selects the one whose epoch makes the current node the
/// block producer, so the approval is not lost.
///
/// Setup
///   4 validators, epoch_length = 5.  We produce a chain long enough to
///   cross two epoch boundaries so the epoch-manager has epochs with
///   distinct `rng_seed`s (the genesis initializes both epoch 0 and 1 with
///   the same seed).  At the second boundary we pick two adjacent blocks
///   whose `get_epoch_id_from_prev_block` returns different epoch ids, and
///   find a target height where those epochs disagree on who the block
///   producer is.  We then overwrite `BlockPerHeight` at the boundary
///   height with the hashes of these two blocks.
///
/// Phase 1 – only the non-matching parent hash is stored at the boundary
///   height.  The node is not the producer under that epoch, so the
///   approval must be dropped (never reaches Doomslug).
///
/// Phase 2 – both hashes are stored.  The code must discover the matching
///   hash and select it as parent, so the approval reaches Doomslug.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_skip_approval_prefers_producer_matching_parent() {
    init_test_logger();

    let num_validators = 4;
    let accounts: Vec<near_primitives::types::AccountId> =
        (0..num_validators).map(|i| format!("test{i}").parse().unwrap()).collect();
    let mut genesis = Genesis::test(accounts, num_validators);
    genesis.config.epoch_length = 5;
    genesis.config.transaction_validity_period = 10;
    let mut env =
        TestEnv::builder_from_genesis(&genesis).validator_seats(num_validators as usize).build();

    // Produce a linear chain with a single client, temporarily swapping
    // the validator signer so the client can produce blocks at heights
    // assigned to any validator.
    let mut blocks = vec![];
    for height in 1..=12 {
        let head_hash = env.clients[0].chain.head().unwrap().last_block_hash;
        let epoch_id =
            env.clients[0].epoch_manager.get_epoch_id_from_prev_block(&head_hash).unwrap();
        let producer = env.clients[0].epoch_manager.get_block_producer(&epoch_id, height).unwrap();
        let signer = InMemoryValidatorSigner::from_seed(
            producer.clone(),
            KeyType::ED25519,
            producer.as_str(),
        );
        env.clients[0].validator_signer.update(Some(Arc::new(signer)));
        let block = env.clients[0].produce_block(height).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        blocks.push(block);
    }

    let epoch_manager = env.clients[0].epoch_manager.clone();
    let head_height = env.clients[0].chain.head().unwrap().height;

    // Find an epoch boundary pair where the two adjacent epochs assign
    // *different* block producers for some target_height. The genesis
    // initializes epoch 0 and epoch 1 with the same rng_seed, so we need
    // to go past the first boundary to reach epochs with distinct seeds.
    let boundary_indices: Vec<usize> = blocks
        .iter()
        .enumerate()
        .filter(|(_, b)| epoch_manager.is_next_block_epoch_start(b.hash()).unwrap())
        .map(|(i, _)| i)
        .collect();
    assert!(!boundary_indices.is_empty(), "should have at least one epoch boundary");

    let (boundary_block, pre_boundary_block, target_height, matching_producer) = boundary_indices
        .iter()
        .filter(|&&idx| idx > 0)
        .find_map(|&idx| {
            let bb = &blocks[idx];
            let pb = &blocks[idx - 1];
            let e_b = epoch_manager.get_epoch_id_from_prev_block(bb.hash()).ok()?;
            let e_p = epoch_manager.get_epoch_id_from_prev_block(pb.hash()).ok()?;
            if e_b == e_p {
                return None;
            }
            // Find a target_height where the two epochs disagree on
            // the block producer.
            (head_height + 1..head_height + 200).find_map(|h| {
                let p_b = epoch_manager.get_block_producer(&e_b, h).ok()?;
                let p_p = epoch_manager.get_block_producer(&e_p, h).ok()?;
                if p_b != p_p { Some((bb, pb, h, p_b)) } else { None }
            })
        })
        .expect("should find an epoch boundary pair with different producer assignments");

    let boundary_height = boundary_block.header().height();

    // Configure the node as the producer that matches the *boundary* epoch.
    // The boundary_block hash is the "right" parent and the pre_boundary_block
    // hash is the "wrong" one.
    let my_account = matching_producer;
    let signer = Arc::new(InMemoryValidatorSigner::from_seed(
        my_account.clone(),
        KeyType::ED25519,
        my_account.as_str(),
    ));
    env.clients[0].validator_signer.update(Some(signer.clone()));

    // Overwrites BlockPerHeight at boundary_height with exactly the given
    // set of (epoch_id, hash) pairs. This is raw DB surgery coupled to the
    // column's serialization format; if that format changes this will fail
    // at deserialization (get_all_block_hashes_by_height) rather than
    // silently producing wrong results.
    let write_block_hashes =
        |env: &mut TestEnv, hashes: &[(near_primitives::types::EpochId, CryptoHash)]| {
            use near_primitives::utils::index_to_bytes;
            use near_store::DBCol;
            use std::collections::{HashMap, HashSet};

            let store = env.clients[0].chain.chain_store().store();
            let mut store_update = store.store_update();
            let mut map: HashMap<near_primitives::types::EpochId, HashSet<CryptoHash>> =
                HashMap::new();
            for (epoch_id, hash) in hashes {
                map.entry(*epoch_id).or_default().insert(*hash);
            }
            store_update.set_ser(DBCol::BlockPerHeight, &index_to_bytes(boundary_height), &map);
            store_update.commit();
        };

    let make_approval =
        |target: u64| Approval::new(CryptoHash::default(), boundary_height, target, &*signer);

    // Phase 1: Only the non-matching parent hash (pre_boundary_block). Its
    // epoch makes a different validator the producer → approval is dropped.
    write_block_hashes(
        &mut env,
        &[(*pre_boundary_block.header().epoch_id(), *pre_boundary_block.hash())],
    );

    let approval1 = make_approval(target_height);
    assert!(matches!(approval1.inner, ApprovalInner::Skip(_)));
    env.clients[0].collect_block_approval(&approval1, ApprovalType::SelfApproval);
    assert!(
        env.clients[0].doomslug.approval_status_at_height(&target_height).approvals.is_empty(),
        "with only the non-matching parent hash, the approval should not reach doomslug"
    );

    // Phase 2: Both parent hashes present. The code should prefer the
    // boundary_block hash (whose epoch makes us the producer) → approval
    // reaches doomslug.
    write_block_hashes(
        &mut env,
        &[
            (*pre_boundary_block.header().epoch_id(), *pre_boundary_block.hash()),
            (*boundary_block.header().epoch_id(), *boundary_block.hash()),
        ],
    );

    let approval2 = make_approval(target_height);
    env.clients[0].collect_block_approval(&approval2, ApprovalType::SelfApproval);
    assert!(
        !env.clients[0].doomslug.approval_status_at_height(&target_height).approvals.is_empty(),
        "with both hashes present, the producer-matching parent should be selected"
    );
}
