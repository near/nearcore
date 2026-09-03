use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::{create_account_id, create_account_ids};
use assert_matches::assert_matches;
use borsh::BorshDeserialize as _;
use near_async::messaging::Handler as _;
use near_async::time::Duration;
use near_chain::ChainStoreAccess as _;
use near_chain_configs::TrackedShardsConfig;
use near_client::{
    GetLightClientChunkExecutionProof, GetLightClientExecutionOutcomeProof,
    GetLightClientStateProof,
};
use near_client_primitives::types::GetLightClientProofError;
use near_epoch_manager::shard_assignment::account_id_to_shard_id;
use near_o11y::testonly::init_test_logger;
use near_primitives::account::{AccessKey, Account};
use near_primitives::block::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::types::{
    Balance, ChunkExecutionRoots, ChunkExecutionRootsV1, Gas, SpiceChunkId, TransactionOrReceiptId,
};
use near_primitives::views::{
    ChunkExecutionProofView, ExecutionStatusView, LightClientBlockLiteView, StateProofTarget,
    StateProofView,
};
use near_store::spice_proof_verifier::{
    SpiceProofVerificationError, StateProofOutcome, verify_chunk_execution_proof,
    verify_execution_outcome_proof, verify_state_proof,
};
use near_test_contracts::rs_contract;

/// Certifies the chain, then returns a final head strictly newer than the certifying block.
fn run_until_certified_light_client_head(env: &mut TestLoopEnv) -> CryptoHash {
    let tip_height = env.validator().head().height;
    env.validator_runner().run_until_certified(tip_height);
    let certified_head_height = env.validator().head().height;
    env.validator_runner().run_until_final_head_height(certified_head_height + 1);
    env.validator().final_head().last_block_hash
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_light_client_chunk_execution_proof() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().validators(1, 0).build();

    let light_client_head = run_until_certified_light_client_head(&mut env);
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

    let light_client_head = run_until_certified_light_client_head(&mut env);
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
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_light_client_outcome_proof_by_transaction_id() {
    init_test_logger();

    let [sender, receiver] = create_account_ids(["vault", "alice"]);
    let mut env = TestLoopBuilder::new()
        .validators(1, 0)
        .num_shards(4)
        .add_user_accounts([&sender, &receiver], Balance::from_near(10))
        .build();

    let transfer_tx = env.validator().tx_send_money(&sender, &receiver, Balance::from_near(1));
    let transfer_tx_hash = transfer_tx.get_hash();
    env.validator_runner().run_tx(transfer_tx, Duration::seconds(20));

    let light_client_head = run_until_certified_light_client_head(&mut env);
    let head_header = env.validator().client().chain.get_block_header(&light_client_head).unwrap();
    let trusted_block_merkle_root = *head_header.block_merkle_root();

    // A transaction is converted in the signer's shard, which the handler resolves
    // through a separate branch from the receipt one.
    let response = env
        .validator_mut()
        .view_client_actor()
        .handle(GetLightClientExecutionOutcomeProof {
            id: TransactionOrReceiptId::Transaction {
                transaction_hash: transfer_tx_hash,
                sender_id: sender.clone(),
            },
            light_client_head,
        })
        .unwrap();
    assert_eq!(response.outcome_proof.id, transfer_tx_hash);

    let chunk_id = response.chunk_execution_proof.roots.chunk_id().clone();
    let epoch_manager = env.validator().client().epoch_manager.clone();
    let epoch_id =
        *env.validator().client().chain.get_block_header(&chunk_id.block_hash).unwrap().epoch_id();
    assert_eq!(
        chunk_id.shard_id,
        account_id_to_shard_id(epoch_manager.as_ref(), &sender, &epoch_id).unwrap()
    );

    verify_chunk_execution_proof(
        &response.chunk_execution_proof,
        &chunk_id,
        &trusted_block_merkle_root,
    )
    .unwrap();
    verify_execution_outcome_proof(
        &response.outcome_proof,
        &transfer_tx_hash,
        &response.chunk_execution_proof.roots,
    )
    .unwrap();
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_light_client_outcome_proof_bad_request() {
    init_test_logger();

    let [sender, receiver] = create_account_ids(["vault", "alice"]);
    let mut env = TestLoopBuilder::new()
        .validators(1, 0)
        .num_shards(4)
        .add_user_accounts([&sender, &receiver], Balance::from_near(10))
        .build();

    let transfer_tx = env.validator().tx_send_money(&sender, &receiver, Balance::from_near(1));
    let transfer_tx_hash = transfer_tx.get_hash();
    env.validator_runner().run_tx(transfer_tx, Duration::seconds(20));
    let receipt_id = env.validator().tx_receipt_id(transfer_tx_hash);

    let light_client_head = run_until_certified_light_client_head(&mut env);

    let epoch_manager = env.validator().client().epoch_manager.clone();
    let epoch_id = env.validator().head().epoch_id;
    let sender_shard_id =
        account_id_to_shard_id(epoch_manager.as_ref(), &sender, &epoch_id).unwrap();
    let receiver_shard_id =
        account_id_to_shard_id(epoch_manager.as_ref(), &receiver, &epoch_id).unwrap();
    assert_ne!(sender_shard_id, receiver_shard_id, "the wrong hint must name another shard");

    // The transfer receipt executes in the receiver's shard.
    let response = env
        .validator_mut()
        .view_client_actor()
        .handle(GetLightClientExecutionOutcomeProof {
            id: TransactionOrReceiptId::Receipt { receipt_id, receiver_id: receiver.clone() },
            light_client_head,
        })
        .unwrap();
    let chunk_id = response.chunk_execution_proof.roots.chunk_id().clone();
    assert_eq!(chunk_id.shard_id, receiver_shard_id);

    // The request names an account only as a hint for the lookup. The handler must
    // ignore a wrong one and follow the outcome's executor instead.
    let misleading_response = env
        .validator_mut()
        .view_client_actor()
        .handle(GetLightClientExecutionOutcomeProof {
            id: TransactionOrReceiptId::Receipt { receipt_id, receiver_id: sender },
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
            id: TransactionOrReceiptId::Receipt { receipt_id: unknown_id, receiver_id: receiver },
            light_client_head,
        });
    assert_matches!(result, Err(GetLightClientProofError::UnknownTransactionOrReceipt { .. }));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_light_client_outcome_proof_untracked_shard() {
    init_test_logger();

    let [sender, receiver] = create_account_ids(["vault", "alice"]);
    let observer = create_account_id("observer");
    let shard_layout = ShardLayout::multi_shard(4, 1);
    let receiver_shard_id = shard_layout.account_id_to_shard_id(&receiver);
    let observed_shard_uid = shard_layout
        .shard_uids()
        .find(|shard_uid| shard_uid.shard_id() != receiver_shard_id)
        .unwrap();

    let mut env = TestLoopBuilder::new()
        .validators(1, 0)
        .shard_layout(shard_layout)
        .add_non_validator_client(&observer)
        .add_user_accounts([&sender, &receiver], Balance::from_near(10))
        .config_modifier(move |config, client_index| {
            if client_index == 1 {
                config.tracked_shards_config =
                    TrackedShardsConfig::Shards(vec![observed_shard_uid]);
            }
        })
        .build();

    let transfer_tx = env.validator().tx_send_money(&sender, &receiver, Balance::from_near(1));
    let transfer_tx_hash = transfer_tx.get_hash();
    env.validator_runner().run_tx(transfer_tx, Duration::seconds(20));
    let receipt_id = env.validator().tx_receipt_id(transfer_tx_hash);

    let light_client_head = run_until_certified_light_client_head(&mut env);
    let head_height =
        env.validator().client().chain.get_block_header(&light_client_head).unwrap().height();
    env.node_runner(1).run_until_final_head_height(head_height);

    let request = || GetLightClientExecutionOutcomeProof {
        id: TransactionOrReceiptId::Receipt { receipt_id, receiver_id: receiver.clone() },
        light_client_head,
    };

    // The validator tracks every shard, so the same request is servable there.
    env.validator_mut().view_client_actor().handle(request()).unwrap();

    let result = env.node_mut(1).view_client_actor().handle(request());
    assert_matches!(result, Err(GetLightClientProofError::UnavailableShard { .. }));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_light_client_state_proof() {
    init_test_logger();

    // See the outcome-proof test: this account is outside the first shard.
    let contract_account = create_account_id("vault");
    let mut env = TestLoopBuilder::new()
        .validators(1, 0)
        .num_shards(4)
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

    let tip_height = env.validator().head().height;
    env.validator_runner().run_until_certified(tip_height);
    let certified_head_height = env.validator().head().height;
    env.validator_runner().run_until_final_head_height(certified_head_height + 1);

    let light_client_head = env.validator().final_head().last_block_hash;

    // The chunk that executed the write; its certified state_root is what the state
    // proofs below are checked against.
    let write_block_hash = env.validator().execution_outcome_with_proof(receipt_id).block_hash;
    let epoch_manager = env.validator().client().epoch_manager.clone();
    let epoch_id =
        *env.validator().client().chain.get_block_header(&write_block_hash).unwrap().epoch_id();
    let account_shard_id =
        account_id_to_shard_id(epoch_manager.as_ref(), &contract_account, &epoch_id).unwrap();
    let chunk_id = SpiceChunkId { block_hash: write_block_hash, shard_id: account_shard_id };

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
    let StateProofOutcome::Present(account_value) = verify_state_proof(
        &account_target,
        &account_response.state_proof,
        &account_response.chunk_execution_proof.roots,
    )
    .unwrap() else {
        panic!("contract account must be present")
    };
    let account = Account::try_from_slice(account_value.as_slice()).unwrap();
    assert_eq!(account.local_contract_hash(), Some(CryptoHash::hash_bytes(rs_contract())));
    assert!(account.amount() < Balance::from_near(10), "deploy and call gas should reduce balance");

    // The contract-data key/value the call wrote is present and proves the exact value.
    let data_target = StateProofTarget::ContractData {
        account_id: contract_account.clone(),
        key: storage_key.into(),
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
    let StateProofOutcome::Present(proved_value) = verify_state_proof(
        &data_target,
        &data_response.state_proof,
        &data_response.chunk_execution_proof.roots,
    )
    .unwrap() else {
        panic!("contract data must be present")
    };
    assert_eq!(proved_value.as_slice(), storage_value.to_le_bytes().as_slice());

    // The chunk-execution proof in the response ties state_root to the trusted head.
    let head_header = env.validator().client().chain.get_block_header(&light_client_head).unwrap();
    verify_chunk_execution_proof(
        &data_response.chunk_execution_proof,
        &chunk_id,
        head_header.block_merkle_root(),
    )
    .unwrap();

    // Tampering the claimed value must be rejected by the trie proof.
    let tampered_state_proof = StateProofView {
        value: Some(b"tampered contract data".to_vec().into()),
        ..data_response.state_proof.clone()
    };
    assert_matches!(
        verify_state_proof(
            &data_target,
            &tampered_state_proof,
            &data_response.chunk_execution_proof.roots,
        ),
        Err(SpiceProofVerificationError::InvalidStateProof)
    );

    let other_shard_id = epoch_manager
        .get_shard_layout(&epoch_id)
        .unwrap()
        .shard_ids()
        .find(|shard_id| *shard_id != account_shard_id)
        .unwrap();
    let result = env.validator_mut().view_client_actor().handle(GetLightClientStateProof {
        chunk_id: SpiceChunkId { block_hash: write_block_hash, shard_id: other_shard_id },
        target: account_target,
        light_client_head,
    });
    assert_matches!(result, Err(GetLightClientProofError::TargetShardMismatch { .. }));

    // The deployed code itself is provable, and its bytes are the contract we sent.
    let code_target = StateProofTarget::LocalContractCode { account_id: contract_account.clone() };
    let code_response = env
        .validator_mut()
        .view_client_actor()
        .handle(GetLightClientStateProof {
            chunk_id: chunk_id.clone(),
            target: code_target.clone(),
            light_client_head,
        })
        .unwrap();
    let StateProofOutcome::Present(proved_code) = verify_state_proof(
        &code_target,
        &code_response.state_proof,
        &code_response.chunk_execution_proof.roots,
    )
    .unwrap() else {
        panic!("contract code must be present")
    };
    assert_eq!(proved_code.as_slice(), rs_contract());

    // The access key exercises the public-key to trie-handle encoding.
    let access_key_target = StateProofTarget::AccessKey {
        account_id: contract_account.clone(),
        public_key: create_user_test_signer(&contract_account).public_key(),
    };
    let access_key_response = env
        .validator_mut()
        .view_client_actor()
        .handle(GetLightClientStateProof {
            chunk_id: chunk_id.clone(),
            target: access_key_target.clone(),
            light_client_head,
        })
        .unwrap();
    let StateProofOutcome::Present(access_key_value) = verify_state_proof(
        &access_key_target,
        &access_key_response.state_proof,
        &access_key_response.chunk_execution_proof.roots,
    )
    .unwrap() else {
        panic!("access key must be present")
    };
    AccessKey::try_from_slice(access_key_value.as_slice()).unwrap();

    // A key the contract never wrote has no value, and that absence is provable.
    let absent_target = StateProofTarget::ContractData {
        account_id: contract_account,
        key: b"never_written".to_vec().into(),
    };
    let absent_response = env
        .validator_mut()
        .view_client_actor()
        .handle(GetLightClientStateProof {
            chunk_id,
            target: absent_target.clone(),
            light_client_head,
        })
        .unwrap();
    assert_matches!(
        verify_state_proof(
            &absent_target,
            &absent_response.state_proof,
            &absent_response.chunk_execution_proof.roots,
        ),
        Ok(StateProofOutcome::AbsentInShard)
    );
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
