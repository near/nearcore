use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use near_async::messaging::Handler as _;
use near_async::time::Duration;
use near_client::{GetExecutionBlockProof, GetExecutionOutcome, GetNextLightClientBlock};
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::merkle::{compute_root_from_path, compute_root_from_path_and_item};
use near_primitives::spice::commitment::{
    OutcomeCommitment, SpiceCommitmentProofView, StateCommitment, merklize_state_roots,
    verify_outcome_commitment, verify_state_commitment,
};
use near_primitives::transaction::PartialExecutionStatus;
use near_primitives::types::{Balance, TransactionOrReceiptId};
use near_primitives::views::{ExecutionOutcomeView, ExecutionStatusView, LightClientBlockLiteView};

fn outcome_view_to_hashes(outcome: &ExecutionOutcomeView) -> Vec<CryptoHash> {
    let status = match &outcome.status {
        ExecutionStatusView::Unknown => PartialExecutionStatus::Unknown,
        ExecutionStatusView::SuccessValue(s) => PartialExecutionStatus::SuccessValue(s.clone()),
        ExecutionStatusView::Failure(_) => PartialExecutionStatus::Failure,
        ExecutionStatusView::SuccessReceiptId(id) => PartialExecutionStatus::SuccessReceiptId(*id),
    };
    let mut result = vec![CryptoHash::hash_borsh((
        outcome.receipt_ids.clone(),
        outcome.gas_burnt,
        outcome.tokens_burnt,
        outcome.executor_id.clone(),
        status,
    ))];
    for log in &outcome.logs {
        result.push(hash(log.as_bytes()));
    }
    result
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_light_client_execution_and_state_proof() {
    init_test_logger();

    let sender = create_account_id("sender");
    let receiver = create_account_id("receiver");

    let mut env = TestLoopBuilder::new()
        .validators(2, 1)
        .enable_rpc()
        .add_user_account(&sender, Balance::from_near(10))
        .add_user_account(&receiver, Balance::from_near(0))
        .build();

    // The light client's initial trusted head: whatever the chain head is at the start.
    let initial_head_hash = env.rpc_node().head().last_block_hash;

    let tx = env.rpc_node().tx_send_money(&sender, &receiver, Balance::from_near(1));
    let tx_hash = tx.get_hash();
    let outcome = env.rpc_runner().execute_tx(tx, Duration::seconds(20)).unwrap();
    let tx_height = env
        .rpc_node()
        .client()
        .chain
        .get_block_header(&outcome.transaction_outcome.block_hash)
        .unwrap()
        .height();

    // Certify the block the outcome lands in, then run a few more so the anchor block
    // (the certifier's canonical child) exists and the trusted head is past it.
    env.rpc_runner().run_until_certified(tx_height);
    let head_after_certification = env.rpc_node().head().height;
    env.rpc_runner().run_until_final_head_height(head_after_certification + 3);

    let outcome_response = env
        .rpc_node_mut()
        .view_client_actor()
        .handle(GetExecutionOutcome {
            id: TransactionOrReceiptId::Transaction {
                transaction_hash: tx_hash,
                sender_id: sender,
            },
        })
        .unwrap();
    let outcome_block_hash = outcome_response.outcome_proof.block_hash;

    // A light client learns its trusted head and that head's block_merkle_root by
    // syncing a light client block from its initial head over RPC, not from the chain.
    let light_client_block = env
        .rpc_node_mut()
        .view_client_actor()
        .handle(GetNextLightClientBlock { last_block_hash: initial_head_hash })
        .unwrap()
        .unwrap();
    let trusted_head_lite = LightClientBlockLiteView {
        prev_block_hash: light_client_block.prev_block_hash,
        inner_rest_hash: light_client_block.inner_rest_hash,
        inner_lite: light_client_block.inner_lite.clone(),
    };
    let trusted_head_hash = trusted_head_lite.hash();
    let trusted_block_merkle_root = light_client_block.inner_lite.block_merkle_root;

    let block_proof_response = env
        .rpc_node_mut()
        .view_client_actor()
        .handle(GetExecutionBlockProof {
            block_hash: outcome_block_hash,
            head_block_hash: trusted_head_hash,
        })
        .unwrap();

    let anchor_block_lite = block_proof_response.block_header_lite;
    let SpiceCommitmentProofView {
        outcome_commitment_proof,
        state_commitment_proof,
        commitment_height,
    } = block_proof_response.spice_commitment_proof.unwrap();

    // outcome -> chunk_outcome_root (the certified block's outcome root for its shard).
    let mut outcome_with_id_to_hash = vec![outcome_response.outcome_proof.id];
    outcome_with_id_to_hash.extend(outcome_view_to_hashes(&outcome_response.outcome_proof.outcome));
    let chunk_outcome_root = compute_root_from_path_and_item(
        &outcome_response.outcome_proof.proof,
        &outcome_with_id_to_hash,
    );
    // chunk_outcome_root -> the block's certified outcome root over all shards.
    let outcome_root = compute_root_from_path(
        &outcome_response.outcome_root_proof,
        hash(chunk_outcome_root.as_ref()),
    );
    // rebuild the OutcomeCommitment leaf and prove it into the anchor block's outcome slot.
    let outcome_leaf = OutcomeCommitment {
        block_hash: outcome_block_hash,
        block_height: commitment_height,
        outcome_root,
    };
    assert!(verify_outcome_commitment(
        anchor_block_lite.inner_lite.outcome_root,
        &outcome_commitment_proof,
        &outcome_leaf,
    ));
    // the anchor block proves into the trusted head's block_merkle_root.
    let anchor_hash = anchor_block_lite.hash();
    assert_eq!(
        compute_root_from_path(&block_proof_response.block_proof, anchor_hash),
        trusted_block_merkle_root,
    );

    // The state side commits the block's certified state root into the anchor block's
    // `prev_state_root` slot; we read the root from the node rather than proving a value
    // up to it.
    // TODO(spice): prove a specific value (e.g. an account record) up to the state root
    // via a trie proof, instead of reading the state root from the node.
    let state_root = {
        let node = env.rpc_node();
        let chain = &node.client().chain;
        let outcome_block_header = chain.get_block_header(&outcome_block_hash).unwrap();
        let execution_results = chain
            .spice_core_reader
            .get_block_execution_results(&outcome_block_header)
            .unwrap()
            .unwrap();
        merklize_state_roots(&execution_results)
    };
    let state_leaf = StateCommitment {
        block_hash: outcome_block_hash,
        block_height: commitment_height,
        state_root,
    };
    assert!(verify_state_commitment(
        anchor_block_lite.inner_lite.prev_state_root,
        &state_commitment_proof,
        &state_leaf,
    ));
}
