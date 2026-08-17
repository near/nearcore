use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use assert_matches::assert_matches;
use near_async::messaging::Handler as _;
use near_async::time::Duration;
use near_chain::ChainStoreAccess as _;
use near_client::{GetLightClientChunkExecutionProof, GetLightClientExecutionOutcomeProof};
use near_client_primitives::types::GetLightClientProofError;
use near_epoch_manager::shard_assignment::account_id_to_shard_id;
use near_o11y::testonly::init_test_logger;
use near_primitives::block::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{
    Balance, ChunkExecutionRoots, ChunkExecutionRootsV1, Gas, SpiceChunkId, TransactionOrReceiptId,
};
use near_primitives::views::{
    ChunkExecutionProofView, ExecutionStatusView, LightClientBlockLiteView,
};
use near_store::spice_proof_verifier::{
    SpiceProofVerificationError, verify_chunk_execution_proof, verify_execution_outcome_proof,
};

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_light_client_chunk_execution_proof() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().validators(1, 0).build();

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

    // Any chunk certified by a block strictly below the head is servable. Walk back
    // from the head to the first block that carries execution results.
    let chain_store = &env.validator().client().chain.chain_store;
    let mut hash = *head_header.prev_hash();
    let (chunk_id, certifying_block_hash) = loop {
        let block = chain_store.get_block(&hash).unwrap();
        if let Some((chunk_id, _)) = block.spice_core_statements().iter_execution_results().next() {
            break (chunk_id.clone(), hash);
        }
        hash = *block.header().prev_hash();
        assert_ne!(hash, CryptoHash::default(), "no certified chunk below the light client head");
    };

    let chunk_proof: ChunkExecutionProofView = env
        .validator_mut()
        .view_client_actor()
        .handle(GetLightClientChunkExecutionProof { chunk_id: chunk_id.clone(), light_client_head })
        .unwrap();
    verify_chunk_execution_proof(&chunk_proof, &chunk_id, &trusted_block_merkle_root).unwrap();

    // Every merkle path in this proof is genuine, so only the chunk id check stops a
    // server from serving it as the answer for a chunk in another block.
    let chunk_id_in_another_block =
        SpiceChunkId { block_hash: light_client_head, shard_id: chunk_id.shard_id };
    assert_matches!(
        verify_chunk_execution_proof(
            &chunk_proof,
            &chunk_id_in_another_block,
            &trusted_block_merkle_root
        ),
        Err(SpiceProofVerificationError::UnexpectedChunkId { .. })
    );

    // Tampering a committed root no longer recomputes the chunk_execution_root.
    let ChunkExecutionRoots::V1(good_roots) = &chunk_proof.roots;
    let mut tampered_proof = chunk_proof.clone();
    tampered_proof.roots = ChunkExecutionRoots::V1(ChunkExecutionRootsV1 {
        state_root: CryptoHash::hash_bytes(b"tampered state root"),
        ..good_roots.clone()
    });
    assert_matches!(
        verify_chunk_execution_proof(&tampered_proof, &chunk_id, &trusted_block_merkle_root),
        Err(SpiceProofVerificationError::InvalidRootsProof)
    );

    // A correct proof must not verify against a wrong trusted root.
    let wrong_root = CryptoHash::hash_bytes(b"not the head block merkle root");
    assert_matches!(
        verify_chunk_execution_proof(&chunk_proof, &chunk_id, &wrong_root),
        Err(SpiceProofVerificationError::InvalidBlockProof)
    );

    // The certifying block's own block merkle root does not commit to itself, so a head
    // at exactly that height is rejected.
    let result =
        env.validator_mut().view_client_actor().handle(GetLightClientChunkExecutionProof {
            chunk_id: chunk_id.clone(),
            light_client_head: certifying_block_hash,
        });
    assert_matches!(result, Err(GetLightClientProofError::LightClientHeadTooOld { .. }));

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
fn test_spice_light_client_execution_outcome_proof() {
    init_test_logger();

    // `vault` sorts after every boundary account of the 4-shard layout, so the
    // target lives outside the first shard and the handler must resolve it.
    let contract_account = create_account_id("vault");
    let mut env = TestLoopBuilder::new()
        .validators(1, 0)
        .num_shards(4)
        .add_user_accounts([&contract_account], Balance::from_near(10))
        .build();

    let deploy_tx = env.validator().tx_deploy_test_contract(&contract_account);
    env.validator_runner().run_tx(deploy_tx, Duration::seconds(20));

    let call_tx = env.validator().tx_call(
        &contract_account,
        &contract_account,
        "log_something",
        Vec::new(),
        Balance::ZERO,
        Gas::from_teragas(300),
    );
    let call_tx_hash = call_tx.get_hash();
    env.validator_runner().run_tx(call_tx, Duration::seconds(20));
    let receipt_id = env.validator().tx_receipt_id(call_tx_hash);

    let tip_height = env.validator().head().height;
    env.validator_runner().run_until_certified(tip_height);
    let certified_head_height = env.validator().head().height;
    env.validator_runner().run_until_final_head_height(certified_head_height + 1);

    let light_client_head = env.validator().final_head().last_block_hash;
    let head_header = env.validator().client().chain.get_block_header(&light_client_head).unwrap();
    let trusted_block_merkle_root = *head_header.block_merkle_root();

    // The handler resolves which chunk executed the receipt, so the client learns the
    // chunk id from the served proof instead of naming it in the request.
    let response = env
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
    assert_eq!(response.outcome_proof.id, receipt_id);

    let chunk_id = response.chunk_execution_proof.roots.chunk_id().clone();
    let epoch_manager = env.validator().client().epoch_manager.clone();
    let epoch_id =
        *env.validator().client().chain.get_block_header(&chunk_id.block_hash).unwrap().epoch_id();
    let account_shard_id =
        account_id_to_shard_id(epoch_manager.as_ref(), &contract_account, &epoch_id).unwrap();
    assert_eq!(chunk_id.shard_id, account_shard_id);
    let first_shard_id =
        epoch_manager.get_shard_layout(&epoch_id).unwrap().shard_ids().next().unwrap();
    assert_ne!(account_shard_id, first_shard_id);

    verify_chunk_execution_proof(
        &response.chunk_execution_proof,
        &chunk_id,
        &trusted_block_merkle_root,
    )
    .unwrap();
    verify_execution_outcome_proof(
        &response.outcome_proof,
        &receipt_id,
        &response.chunk_execution_proof.roots,
    )
    .unwrap();

    // Only the id check stops a server from answering with another outcome of the same
    // chunk, whose merkle path is just as genuine.
    assert_matches!(
        verify_execution_outcome_proof(
            &response.outcome_proof,
            &call_tx_hash,
            &response.chunk_execution_proof.roots,
        ),
        Err(SpiceProofVerificationError::UnexpectedOutcomeId { .. })
    );

    // block_hash is not hashed into the outcome, so it is checked against the roots.
    let mut relabeled_outcome = response.outcome_proof.clone();
    relabeled_outcome.block_hash = CryptoHash::hash_bytes(b"not the executing block");
    assert_matches!(
        verify_execution_outcome_proof(
            &relabeled_outcome,
            &receipt_id,
            &response.chunk_execution_proof.roots,
        ),
        Err(SpiceProofVerificationError::UnexpectedOutcomeBlockHash { .. })
    );

    // A server lying about the result must be rejected: changing the outcome status
    // changes the outcome hash, which then does not recompute the chunk's outcome_root.
    let mut tampered_outcome = response.outcome_proof.clone();
    tampered_outcome.outcome.status = ExecutionStatusView::SuccessValue(b"forged result".to_vec());
    assert_matches!(
        verify_execution_outcome_proof(
            &tampered_outcome,
            &receipt_id,
            &response.chunk_execution_proof.roots
        ),
        Err(SpiceProofVerificationError::InvalidOutcomeProof)
    );

    // The same outcome must not verify against a different chunk's outcome_root.
    let ChunkExecutionRoots::V1(good_roots) = &response.chunk_execution_proof.roots;
    let tampered_roots = ChunkExecutionRoots::V1(ChunkExecutionRootsV1 {
        outcome_root: CryptoHash::hash_bytes(b"tampered outcome root"),
        ..good_roots.clone()
    });
    assert_matches!(
        verify_execution_outcome_proof(&response.outcome_proof, &receipt_id, &tampered_roots),
        Err(SpiceProofVerificationError::InvalidOutcomeProof)
    );

    // A transaction id resolves through the signer's shard, a separate branch from the
    // receipt one above, and lands in the chunk that converted the transaction.
    let transaction_response = env
        .validator_mut()
        .view_client_actor()
        .handle(GetLightClientExecutionOutcomeProof {
            id: TransactionOrReceiptId::Transaction {
                transaction_hash: call_tx_hash,
                sender_id: contract_account.clone(),
            },
            light_client_head,
        })
        .unwrap();
    assert_eq!(transaction_response.outcome_proof.id, call_tx_hash);
    let transaction_chunk_id = transaction_response.chunk_execution_proof.roots.chunk_id().clone();
    assert_eq!(transaction_chunk_id.shard_id, account_shard_id);
    verify_chunk_execution_proof(
        &transaction_response.chunk_execution_proof,
        &transaction_chunk_id,
        &trusted_block_merkle_root,
    )
    .unwrap();
    verify_execution_outcome_proof(
        &transaction_response.outcome_proof,
        &call_tx_hash,
        &transaction_response.chunk_execution_proof.roots,
    )
    .unwrap();

    // The request names an account only to find the outcome's shard cheaply. The
    // handler must ignore a wrong one and follow the outcome's executor instead.
    let misleading_response = env
        .validator_mut()
        .view_client_actor()
        .handle(GetLightClientExecutionOutcomeProof {
            id: TransactionOrReceiptId::Receipt {
                receipt_id,
                receiver_id: create_account_id("alice"),
            },
            light_client_head,
        })
        .unwrap();
    assert_eq!(
        misleading_response.chunk_execution_proof.roots.chunk_id(),
        &chunk_id,
        "a wrong account in the request must not move the proof to another chunk"
    );
    verify_execution_outcome_proof(
        &misleading_response.outcome_proof,
        &receipt_id,
        &misleading_response.chunk_execution_proof.roots,
    )
    .unwrap();

    let unknown_id = CryptoHash::hash_bytes(b"no such receipt");
    let result =
        env.validator_mut().view_client_actor().handle(GetLightClientExecutionOutcomeProof {
            id: TransactionOrReceiptId::Receipt {
                receipt_id: unknown_id,
                receiver_id: contract_account.clone(),
            },
            light_client_head,
        });
    assert_matches!(result, Err(GetLightClientProofError::UnknownTransactionOrReceipt { .. }));
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
