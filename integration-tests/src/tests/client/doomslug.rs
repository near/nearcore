use crate::env::test_env::TestEnv;
use near_chain::Provenance;
use near_chain_configs::Genesis;
use near_crypto::KeyType;
use near_o11y::testonly::init_test_logger;
use near_primitives::block::{Approval, ApprovalInner, ApprovalType, Block};
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
/// height (e.g. forks around an epoch boundary), each fork block may
/// yield a different epoch via `get_epoch_id_from_prev_block` and thus
/// a different block-producer schedule for the target height.  If the
/// code picks the wrong parent, the approval is silently dropped.
///
/// This test creates real forks at epoch boundaries using two clients:
///   - Client 0 builds a continuous chain (good finalization →
///     `is_next_block_epoch_start` returns true at each boundary).
///   - Client 1 receives the same blocks except the last two before
///     each boundary, then produces a skip block at the boundary height.
///     The skip's stale finalization means `is_next_block_epoch_start`
///     returns false → different epoch than client 0's block.
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
    // Disable kickouts so all validators survive across epochs. Only
    // client 0 produces blocks/chunks, so without this, other validators
    // would be kicked for missing chunks after the first epoch.
    genesis.config.block_producer_kickout_threshold = 0;
    genesis.config.chunk_producer_kickout_threshold = 0;
    genesis.config.chunk_validator_only_kickout_threshold = 0;
    let mut env = TestEnv::builder_from_genesis(&genesis)
        .clients_count(2)
        .validator_seats(num_validators as usize)
        .build();

    let epoch_manager = env.clients[0].epoch_manager.clone();

    // Helper: set signer to the assigned block producer and produce.
    let produce_block = |env: &mut TestEnv, client: usize, h: u64| -> Arc<Block> {
        let head = env.clients[client].chain.head().unwrap().last_block_hash;
        let eid = env.clients[client].epoch_manager.get_epoch_id_from_prev_block(&head).unwrap();
        let producer = env.clients[client].epoch_manager.get_block_producer(&eid, h).unwrap();
        let signer = InMemoryValidatorSigner::from_seed(
            producer.clone(),
            KeyType::ED25519,
            producer.as_str(),
        );
        env.clients[client].validator_signer.update(Some(Arc::new(signer)));
        env.clients[client].produce_block(h).unwrap().unwrap()
    };

    // Build the chain incrementally. Both clients stay in sync via a
    // 2-block delayed buffer. At each epoch boundary (past the first
    // two), client 1 is 2 blocks behind and produces a fork skip block.
    let mut boundaries_seen = 0;
    let mut tested = 0;
    let mut client1_synced_to = 0u64;
    let mut buffer: Vec<Arc<Block>> = vec![];
    let mut h = 0u64;

    loop {
        h += 1;
        assert!(h <= 80, "ran out of blocks without finding 4 testable boundaries");
        let block = produce_block(&mut env, 0, h);
        env.process_block(0, block.clone(), Provenance::PRODUCED);

        // Send blocks to client 1 with a 2-block delay.
        if buffer.len() >= 2 {
            let old = buffer.remove(0);
            env.process_block(1, old.clone(), Provenance::NONE);
            client1_synced_to = old.header().height();
        }
        buffer.push(block.clone());

        if !epoch_manager.is_next_block_epoch_start(block.hash()).unwrap() {
            continue;
        }

        // block at height h is an epoch boundary.
        boundaries_seen += 1;
        if boundaries_seen <= 2 {
            // First two boundaries: flush buffer to client 1 and
            // continue (epochs 0/1 share the same rng_seed).
            for b in buffer.drain(..) {
                env.process_block(1, b.clone(), Provenance::NONE);
                client1_synced_to = b.header().height();
            }
            continue;
        }

        let boundary_height = h;
        let boundary_block = block;
        assert_eq!(client1_synced_to, boundary_height - 2);

        // Client 1 produces a skip block at boundary_height (skipping
        // boundary_height - 1). Its parent at boundary_height - 2 has
        // stale finalization → different epoch than the boundary block.
        let fork_block = produce_block(&mut env, 1, boundary_height);

        // Process the fork block on client 0.
        env.process_block(0, fork_block.clone(), Provenance::NONE);

        let epoch_boundary =
            epoch_manager.get_epoch_id_from_prev_block(boundary_block.hash()).unwrap();
        let epoch_fork = epoch_manager.get_epoch_id_from_prev_block(fork_block.hash()).unwrap();

        // Flush buffer to client 1 so it catches up for the next boundary.
        for b in buffer.drain(..) {
            env.process_block(1, b.clone(), Provenance::NONE);
            client1_synced_to = b.header().height();
        }

        if epoch_boundary == epoch_fork {
            continue;
        }

        // Produce a couple more blocks on client 0 so the new epoch's
        // info is registered in the epoch manager (it's created when
        // the first block of the new epoch is processed).
        for _extra in 1..=2 {
            h += 1;
            let b = produce_block(&mut env, 0, h);
            env.process_block(0, b.clone(), Provenance::PRODUCED);
            env.process_block(1, b, Provenance::NONE);
            client1_synced_to = h;
        }

        // Find a target_height where the two epochs disagree on producer.
        let head_height = env.clients[0].chain.head().unwrap().height;
        let Some((target_height, my_account)) =
            (head_height + 1..head_height + 200).find_map(|th| {
                let p_b = epoch_manager.get_block_producer(&epoch_boundary, th).ok()?;
                let p_f = epoch_manager.get_block_producer(&epoch_fork, th).ok()?;
                if p_b != p_f { Some((th, p_b)) } else { None }
            })
        else {
            continue;
        };

        // Configure the node as the boundary-epoch producer and test.
        let signer = Arc::new(InMemoryValidatorSigner::from_seed(
            my_account.clone(),
            KeyType::ED25519,
            my_account.as_str(),
        ));
        env.clients[0].validator_signer.update(Some(signer.clone()));

        let approval =
            Approval::new(CryptoHash::default(), boundary_height, target_height, &*signer);
        assert!(matches!(approval.inner, ApprovalInner::Skip(_)));
        env.clients[0].collect_block_approval(&approval, ApprovalType::SelfApproval);
        assert!(
            !env.clients[0].doomslug.approval_status_at_height(&target_height).approvals.is_empty(),
            "approval should reach doomslug at boundary height {boundary_height}"
        );

        tested += 1;
        // We repeat across 4 epoch boundaries.  Each trial independently
        // has ~50% chance of the right parent being first in HashMap iteration
        // order, so 4 trials give ~6% false-pass probability.
        if tested >= 4 {
            return;
        }
    }
}
