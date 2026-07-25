use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use assert_matches::assert_matches;
use borsh::BorshDeserialize as _;
use near_async::messaging::Handler as _;
use near_async::time::Duration;
use near_client::{
    GetLightClientChunkExecutionProof, GetLightClientExecutionOutcomeProof,
    GetLightClientStateProof,
};
use near_client_primitives::types::GetLightClientProofError;
use near_o11y::testonly::init_test_logger;
use near_primitives::account::Account;
use near_primitives::block::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{
    Balance, ChunkExecutionRoots, ChunkExecutionRootsV1, Gas, SpiceChunkId, TransactionOrReceiptId,
};
use near_primitives::views::{
    ChunkExecutionProofView, ExecutionStatusView, LightClientBlockLiteView, StateProofTarget,
};
use near_store::spice_proof_verifier::{
    SpiceProofVerificationError, verify_chunk_execution_proof, verify_execution_outcome_proof,
    verify_state_proof,
};

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_light_client_chunk_execution_proofs() {
    init_test_logger();

    let contract_account = create_account_id("contract");
    let mut env = TestLoopBuilder::new()
        .validators(1, 0)
        .add_user_accounts([&contract_account], Balance::from_near(10))
        .build();

    let deploy_tx = env.validator().tx_deploy_test_contract(&contract_account);
    env.validator_runner().run_tx(deploy_tx, Duration::seconds(20));

    // rs_contract's write_key_value reads input as `key_bytes || value(u64 LE)` and
    // does storage_write(key, value), so this creates a provable ContractData entry.
    let storage_key = b"spice_key".to_vec();
    let storage_value: u64 = 42;
    let mut call_args = storage_key.clone();
    call_args.extend_from_slice(&storage_value.to_le_bytes());
    let call_tx = env.validator().tx_call(
        &contract_account,
        &contract_account,
        "write_key_value",
        call_args,
        Balance::ZERO,
        Gas::from_teragas(300),
    );
    let call_tx_hash = call_tx.get_hash();
    env.validator_runner().run_tx(call_tx, Duration::seconds(20));
    let receipt_id = env.validator().tx_receipt_id(call_tx_hash);

    // Certify the chain through the current tip, then let finality advance strictly
    // past it. The handler serves from the certifying-block index (written at
    // finality), and the head must be strictly after a chunk's certifying block for
    // the block merkle proof to recompute the head's block_merkle_root from it.
    let tip_height = env.validator().head().height;
    env.validator_runner().run_until_certified(tip_height);
    let certified_head_height = env.validator().head().height;
    env.validator_runner().run_until_final_head_height(certified_head_height + 1);

    let light_client_head = env.validator().final_head().last_block_hash;
    let head_header = env.validator().client().chain.get_block_header(&light_client_head).unwrap();
    let trusted_block_merkle_root = *head_header.block_merkle_root();

    // Outcome proof for the contract call's receipt. The handler resolves the chunk
    // it executed in; that chunk_id anchors the chunk-execution and state proofs.
    let outcome_response = env
        .validator_mut()
        .view_client_actor()
        .handle(GetLightClientExecutionOutcomeProof {
            id: TransactionOrReceiptId::Receipt {
                receipt_id,
                receiver_id: contract_account.clone(),
            },
            light_client_head,
        })
        .unwrap();

    verify_chunk_execution_proof(
        &outcome_response.chunk_execution_proof,
        &trusted_block_merkle_root,
    )
    .unwrap();
    verify_execution_outcome_proof(
        &outcome_response.outcome_proof,
        &outcome_response.chunk_execution_proof.roots,
    )
    .unwrap();

    // A server lying about the result must be rejected: changing the outcome status
    // changes the outcome hash, which then does not recompute the chunk's outcome_root.
    let mut tampered_outcome = outcome_response.outcome_proof.clone();
    tampered_outcome.outcome.status = ExecutionStatusView::SuccessValue(b"forged result".to_vec());
    assert_matches!(
        verify_execution_outcome_proof(
            &tampered_outcome,
            &outcome_response.chunk_execution_proof.roots,
        ),
        Err(SpiceProofVerificationError::InvalidOutcomeProof)
    );

    // The chunk the receipt executed in, resolved by the outcome handler; it anchors
    // the chunk-execution and state proofs below.
    let chunk_id = outcome_response.chunk_execution_proof.roots.chunk_id().clone();

    // Chunk-execution proof for the same chunk.
    let chunk_proof: ChunkExecutionProofView = env
        .validator_mut()
        .view_client_actor()
        .handle(GetLightClientChunkExecutionProof { chunk_id: chunk_id.clone(), light_client_head })
        .unwrap();
    verify_chunk_execution_proof(&chunk_proof, &trusted_block_merkle_root).unwrap();

    // Tampering a committed root no longer recomputes the chunk_execution_root.
    let ChunkExecutionRoots::V1(good_roots) = &chunk_proof.roots;
    let mut tampered_proof = chunk_proof.clone();
    tampered_proof.roots = ChunkExecutionRoots::V1(ChunkExecutionRootsV1 {
        state_root: CryptoHash::hash_bytes(b"tampered state root"),
        ..good_roots.clone()
    });
    assert_matches!(
        verify_chunk_execution_proof(&tampered_proof, &trusted_block_merkle_root),
        Err(SpiceProofVerificationError::InvalidRootsProof)
    );

    // A correct proof must not verify against a wrong trusted root.
    let wrong_root = CryptoHash::hash_bytes(b"not the head block merkle root");
    assert_matches!(
        verify_chunk_execution_proof(&chunk_proof, &wrong_root),
        Err(SpiceProofVerificationError::InvalidBlockProof)
    );

    // The contract account's record and the storage entry it wrote both live in this
    // chunk's shard at the certified state_root, so both are provable against it.

    // Account record proves the deployed contract and a gas-reduced balance.
    let account_target = StateProofTarget::Account { account_id: contract_account.clone() };
    let account_response = env
        .validator_mut()
        .view_client_actor()
        .handle(GetLightClientStateProof {
            chunk_id: chunk_id.clone(),
            target: account_target.clone(),
            light_client_head,
        })
        .unwrap();
    verify_state_proof(
        &account_target,
        account_response.value.as_ref(),
        &account_response.state_proof,
        &account_response.chunk_execution_proof.roots,
    )
    .unwrap();
    let account_bytes: Vec<u8> =
        account_response.value.clone().expect("contract account must be present").value.into();
    let account = Account::try_from_slice(&account_bytes).unwrap();
    assert_eq!(
        account.local_contract_hash(),
        Some(CryptoHash::hash_bytes(near_test_contracts::rs_contract()))
    );
    assert!(account.amount() > Balance::ZERO);
    assert!(account.amount() < Balance::from_near(10), "deploy and call gas should reduce balance");

    // The contract-data key/value we wrote is present and proves the exact value.
    let data_target = StateProofTarget::ContractData {
        account_id: contract_account.clone(),
        key: storage_key.clone().into(),
    };
    let data_response = env
        .validator_mut()
        .view_client_actor()
        .handle(GetLightClientStateProof {
            chunk_id: chunk_id.clone(),
            target: data_target.clone(),
            light_client_head,
        })
        .unwrap();
    let proved_value: Vec<u8> =
        data_response.value.clone().expect("contract data must be present").value.into();
    assert_eq!(proved_value, storage_value.to_le_bytes());
    verify_state_proof(
        &data_target,
        data_response.value.as_ref(),
        &data_response.state_proof,
        &data_response.chunk_execution_proof.roots,
    )
    .unwrap();

    // Tampering the claimed value must be rejected by the trie proof.
    let mut tampered_value = data_response.value.clone().unwrap();
    tampered_value.value = b"tampered contract data".to_vec().into();
    assert_matches!(
        verify_state_proof(
            &data_target,
            Some(&tampered_value),
            &data_response.state_proof,
            &data_response.chunk_execution_proof.roots,
        ),
        Err(SpiceProofVerificationError::InvalidStateProof)
    );

    // A not-yet-certified chunk (in the non-final head block) is not served.
    let head_block = env.validator().head_block();
    let uncertified_chunk_id =
        SpiceChunkId { block_hash: *head_block.hash(), shard_id: chunk_id.shard_id };
    let result =
        env.validator_mut().view_client_actor().handle(GetLightClientChunkExecutionProof {
            chunk_id: uncertified_chunk_id,
            light_client_head,
        });
    assert_matches!(result, Err(GetLightClientProofError::ChunkNotCertified { .. }));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_light_client_block_lite_view_hash() {
    init_test_logger();

    // `build()` warms up, so the chain already has produced a block.
    let env = TestLoopBuilder::new().validators(1, 0).build();

    // The verifier derives the certifying block's hash by calling
    // LightClientBlockLiteView::hash(), so that reconstruction (rebuilding the spice
    // header's inner-lite, including chunk_execution_root) must equal the real hash.
    let block = env.validator().head_block();
    assert_eq!(
        LightClientBlockLiteView::from(BlockHeader::clone(block.header())).hash(),
        *block.header().hash(),
    );
}
