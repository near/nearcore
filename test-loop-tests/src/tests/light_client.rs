use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use near_async::messaging::Handler;
use near_async::time::Duration;
use near_client::{GetBlockProof, GetExecutionOutcome, GetNextLightClientBlock};
use near_o11y::testonly::init_test_logger;
use near_primitives::block_header::{
    Approval, ApprovalInner, compute_bp_hash_from_validator_stakes,
};
use near_primitives::gas::Gas;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{
    combine_hash, compute_root_from_path_and_item, verify_hash, verify_path,
};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{AccountId, Balance, TransactionOrReceiptId};
use near_primitives::views::{LightClientBlockLiteView, LightClientBlockView};
use std::collections::{HashMap, HashSet};

fn light_client_block_hash(block: &LightClientBlockView) -> CryptoHash {
    LightClientBlockLiteView {
        prev_block_hash: block.prev_block_hash,
        inner_rest_hash: block.inner_rest_hash,
        inner_lite: block.inner_lite.clone(),
    }
    .hash()
}

/// Validates a light client block against the previously known one, following NEP-25, exactly as
/// an external light client would: recompute the block hash, verify each approval signature against
/// the block producers of the block's epoch, check the >2/3 stake threshold, and on an epoch change
/// verify the next block producers hash. `block_producers` maps an epoch id to its ordered block
/// producers and is updated as new epochs are encountered.
fn validate_light_client_block(
    last_known_block: &LightClientBlockView,
    new_block: &LightClientBlockView,
    block_producers: &mut HashMap<CryptoHash, Vec<ValidatorStake>>,
) {
    let new_block_hash = light_client_block_hash(new_block);
    let next_block_hash = combine_hash(&new_block.next_block_inner_hash, &new_block_hash);

    assert!(
        new_block.inner_lite.epoch_id == last_known_block.inner_lite.epoch_id
            || new_block.inner_lite.epoch_id == last_known_block.inner_lite.next_epoch_id,
        "new block epoch must be the last known block's epoch or its next epoch",
    );

    let epoch_block_producers = &block_producers[&new_block.inner_lite.epoch_id];
    assert_eq!(new_block.approvals_after_next.len(), epoch_block_producers.len());

    let approval_message = Approval::get_data_for_sig(
        &ApprovalInner::Endorsement(next_block_hash),
        new_block.inner_lite.height + 2,
    );
    let mut total_stake = 0u128;
    let mut approved_stake = 0u128;
    for (approval, block_producer) in
        new_block.approvals_after_next.iter().zip(epoch_block_producers)
    {
        total_stake += block_producer.stake().as_yoctonear();
        let Some(signature) = approval else { continue };
        approved_stake += block_producer.stake().as_yoctonear();
        assert!(
            signature.verify(&approval_message, block_producer.public_key()),
            "approval signature must verify against the block producer key",
        );
    }
    assert!(approved_stake * 3 > total_stake * 2, "approved stake must exceed 2/3 of total stake");

    if new_block.inner_lite.epoch_id == last_known_block.inner_lite.next_epoch_id {
        let next_bps =
            new_block.next_bps.as_ref().expect("next_bps required when crossing into a new epoch");
        let next_stakes: Vec<ValidatorStake> = next_bps.iter().cloned().map(Into::into).collect();
        assert_eq!(
            compute_bp_hash_from_validator_stakes(&next_stakes, true),
            new_block.inner_lite.next_bp_hash,
            "next block producers hash must match",
        );
        block_producers.insert(new_block.inner_lite.next_epoch_id, next_stakes);
    }
}

/// Walks the `next_light_client_block` sequence across several epochs and checks that each
/// returned light client block describes the canonical chain and that the walk terminates.
#[test]
fn test_next_light_client_block() {
    init_test_logger();

    let epoch_length = 5;
    // Keep all blocks so the early seed block survives the multi-epoch walk.
    let mut env = TestLoopBuilder::new()
        .validators(4, 0)
        .enable_rpc()
        .epoch_length(epoch_length)
        .gc_num_epochs_to_keep(100)
        .build();

    // A block in an early epoch to seed the walk from.
    let start_hash = env.rpc_node().head().last_block_hash;
    env.rpc_runner().run_until_head_height(8 * epoch_length);
    let final_head_height = env.rpc_node().final_head().height;

    // Walk next_light_client_block forward, validating each block per NEP-25 as a light client
    // would. The first block is unvalidated (no prior block to validate against) and seeds the
    // block producers map from its next_bps; subsequent blocks are fully validated.
    let mut block_producers: HashMap<CryptoHash, Vec<ValidatorStake>> = HashMap::new();
    let mut last_known_block: Option<LightClientBlockView> = None;
    let mut last_block_hash = start_hash;
    // (height, epoch_id, recomputed_hash) per returned block, checked against the chain below.
    let mut steps: Vec<(u64, CryptoHash, CryptoHash)> = Vec::new();
    {
        let mut rpc = env.rpc_node_mut();
        let view_client = rpc.view_client_actor();
        // Safety cap; the walk must terminate well before this.
        for _ in 0..100 {
            let Some(block) =
                view_client.handle(GetNextLightClientBlock { last_block_hash }).unwrap()
            else {
                break;
            };
            match &last_known_block {
                Some(last_known_block) => {
                    validate_light_client_block(last_known_block, &block, &mut block_producers)
                }
                None => {
                    let next_bps = block.next_bps.clone().expect("first block must carry next_bps");
                    let next_stakes = next_bps.into_iter().map(Into::into).collect();
                    block_producers.insert(block.inner_lite.next_epoch_id, next_stakes);
                }
            }
            let recomputed_hash = light_client_block_hash(&block);
            steps.push((block.inner_lite.height, block.inner_lite.epoch_id, recomputed_hash));
            last_block_hash = recomputed_hash;
            last_known_block = Some(LightClientBlockView::clone(&block));
        }
    }

    assert!(steps.len() < 100, "light client walk did not terminate");

    let chain = &env.rpc_node().client().chain;
    let mut prev_height = 0;
    let mut distinct_epochs = HashSet::new();
    for (height, epoch_id, recomputed_hash) in &steps {
        assert!(*height > prev_height, "light client heights must strictly increase: {steps:?}");
        assert!(*height <= final_head_height, "light client block must be final: {steps:?}");
        assert_eq!(
            chain.get_block_hash_by_height(*height).unwrap(),
            *recomputed_hash,
            "recomputed light client block hash must match the canonical chain at height {height}",
        );
        prev_height = *height;
        distinct_epochs.insert(*epoch_id);
    }

    // Walking next_light_client_block should carry us across several epoch boundaries.
    assert!(distinct_epochs.len() >= 3, "expected to traverse several epochs, got {steps:?}");

    // Each step advances exactly one epoch: the walk never repeats or skips an epoch.
    assert_eq!(
        steps.len(),
        distinct_epochs.len(),
        "each light client step must be a new epoch: {steps:?}",
    );

    // The walk must climb all the way up to the head's final block, not terminate early.
    let last_step_height = steps.last().unwrap().0;
    assert!(
        final_head_height - last_step_height <= epoch_length,
        "light client walk stopped {} blocks below the final head: {steps:?}",
        final_head_height - last_step_height,
    );
}

/// The light client block tracks the last *final* block, so for a fixed last-known block it stays
/// in the previous epoch until the head's final block crosses into the new epoch.
#[test]
fn test_next_light_client_block_epoch_boundary() {
    init_test_logger();

    let epoch_length = 6;
    let mut env = TestLoopBuilder::new()
        .validators(4, 0)
        .enable_rpc()
        .epoch_length(epoch_length)
        .gc_num_epochs_to_keep(100)
        .build();

    // Use the first block of an epoch as the fixed last-known block, so it sits well below the
    // head's final block once we cross into the next epoch.
    env.rpc_runner().run_until_head_height(2 * epoch_length);
    let epoch_before = env.rpc_node().head().epoch_id;
    env.rpc_runner().run_until(|node| node.head().epoch_id != epoch_before, Duration::seconds(20));
    let last_known_head = env.rpc_node().head();
    let last_known_hash = last_known_head.last_block_hash;
    let last_known_epoch = last_known_head.epoch_id;

    // Advance until the head enters the next epoch; the head's final block still lags behind in the
    // previous epoch.
    env.rpc_runner()
        .run_until(|node| node.head().epoch_id != last_known_epoch, Duration::seconds(20));
    let next_epoch = env.rpc_node().head().epoch_id;

    let in_previous_epoch = env
        .rpc_node_mut()
        .view_client_actor()
        .handle(GetNextLightClientBlock { last_block_hash: last_known_hash })
        .unwrap()
        .unwrap();
    assert_eq!(
        in_previous_epoch.inner_lite.epoch_id, last_known_epoch.0,
        "light client block should still lag in the previous epoch right after the boundary",
    );

    // Advance one block at a time, staying within the new epoch, until the head's final block
    // crosses the boundary and the light client block follows it into the new epoch.
    let in_new_epoch = loop {
        env.rpc_runner().run_for_number_of_blocks(1);
        assert_eq!(
            env.rpc_node().head().epoch_id,
            next_epoch,
            "head must stay within the new epoch"
        );
        let block = env
            .rpc_node_mut()
            .view_client_actor()
            .handle(GetNextLightClientBlock { last_block_hash: last_known_hash })
            .unwrap()
            .unwrap();
        if block.inner_lite.epoch_id == next_epoch.0 {
            break block;
        }
        assert_eq!(
            block.inner_lite.epoch_id, last_known_epoch.0,
            "light client block must stay in the previous epoch until the final block crosses",
        );
    };

    // The invariant the light client head relies on: it shares the epoch of the block two heights
    // onward, so its approvals can be verified with that epoch's block producers.
    let two_ahead = env
        .rpc_node()
        .client()
        .chain
        .get_block_header_by_height(in_new_epoch.inner_lite.height + 2)
        .unwrap();
    assert_eq!(
        two_ahead.epoch_id().0,
        in_new_epoch.inner_lite.epoch_id,
        "light client block must share the epoch of the block two heights onward",
    );
}

/// Deploys a contract, calls it (once succeeding, once failing), and verifies the
/// `light_client_proof` merkle proofs for the transaction and each of its receipt outcomes.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_light_client_execution_outcome_proof() {
    init_test_logger();

    let user = create_account_id("user");
    let mut env = TestLoopBuilder::new()
        .validators(2, 0)
        .enable_rpc()
        .epoch_length(1000)
        .add_user_account(&user, Balance::from_near(10))
        .build();

    // A block in the same epoch as the head, used to seed the light client block lookup.
    let seed_hash = env.rpc_node().head().last_block_hash;

    let deploy_tx = env.rpc_node().tx_deploy_test_contract(&user);
    env.rpc_runner().run_tx(deploy_tx, Duration::seconds(5));

    // key = 42, value = 10, encoded as two little-endian u64s, matching `write_key_value`.
    let mut args = Vec::with_capacity(16);
    args.extend_from_slice(&42u64.to_le_bytes());
    args.extend_from_slice(&10u64.to_le_bytes());

    let success_tx = env.rpc_node().tx_call(
        &user,
        &user,
        "write_key_value",
        args.clone(),
        Balance::ZERO,
        Gas::from_teragas(300),
    );
    check_outcome_proofs(&mut env, &user, seed_hash, success_tx);

    let failing_tx = env.rpc_node().tx_call(
        &user,
        &user,
        "write_key_value",
        args,
        Balance::ZERO,
        Gas::from_gas(1000),
    );
    check_outcome_proofs(&mut env, &user, seed_hash, failing_tx);
}

fn check_outcome_proofs(
    env: &mut TestLoopEnv,
    account_id: &AccountId,
    seed_hash: CryptoHash,
    tx: SignedTransaction,
) {
    let outcome = env.rpc_runner().execute_tx(tx, Duration::seconds(5)).unwrap();
    // Advance so the outcome's block is final and included in later blocks' merkle roots.
    env.rpc_runner().run_for_number_of_blocks(4);

    let mut ids = vec![TransactionOrReceiptId::Transaction {
        transaction_hash: outcome.transaction_outcome.id,
        sender_id: account_id.clone(),
    }];
    for receipt_outcome in &outcome.receipts_outcome {
        ids.push(TransactionOrReceiptId::Receipt {
            receipt_id: receipt_outcome.id,
            receiver_id: account_id.clone(),
        });
    }

    let mut rpc = env.rpc_node_mut();
    let view_client = rpc.view_client_actor();

    // The light client head and the block merkle root all proofs are checked against.
    let light_client_block = view_client
        .handle(GetNextLightClientBlock { last_block_hash: seed_hash })
        .unwrap()
        .unwrap();
    let light_client_head = light_client_block_hash(&light_client_block);
    let block_merkle_root = light_client_block.inner_lite.block_merkle_root;

    for id in ids {
        let execution_outcome = view_client.handle(GetExecutionOutcome { id }).unwrap();
        let outcome_proof = execution_outcome.outcome_proof;
        let block_proof = view_client
            .handle(GetBlockProof {
                block_hash: outcome_proof.block_hash,
                head_block_hash: light_client_head,
            })
            .unwrap();

        // outcome -> chunk outcome root -> block outcome root.
        let chunk_outcome_root =
            compute_root_from_path_and_item(&outcome_proof.proof, &outcome_proof.to_hashes());
        assert!(verify_path(
            block_proof.block_header_lite.inner_lite.outcome_root,
            &execution_outcome.outcome_root_proof,
            &chunk_outcome_root,
        ));

        // The light block header recomputes to the proof's block hash.
        assert_eq!(block_proof.block_header_lite.hash(), outcome_proof.block_hash);

        // block hash -> light client head's block merkle root.
        assert!(verify_hash(block_merkle_root, &block_proof.proof, outcome_proof.block_hash));
    }
}
