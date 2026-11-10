//! Submitting ECCs to an RPC node: what it refuses, and where it sends what it accepts.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::setup::peer_manager_actor::HandlerResult;
use crate::utils::account::create_account_id;
use crate::utils::node::TestLoopNode;
use assert_matches::assert_matches;
use near_async::time::Duration;
use near_chain_configs::test_genesis::ValidatorsSpec;
use near_chain_configs::{ClientConfig, MutableConfigValue, TrackedShardsConfig};
use near_client::ProcessTxResponse;
use near_jsonrpc_primitives::types::light_client::RpcLightClientExecutionProofRequest;
use near_jsonrpc_primitives::types::receipts::{ReceiptReference, RpcReceiptToTxRequest};
use near_jsonrpc_primitives::types::transactions::{RpcTransactionStatusRequest, TransactionInfo};
use near_network::types::NetworkRequests;
use near_o11y::testonly::init_test_logger;
use near_parameters::{RuntimeConfig, RuntimeConfigStore};
use near_primitives::errors::InvalidTxError;
use near_primitives::hash::CryptoHash;
use near_primitives::num_rational::Rational32;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::sharding::ChunkHash;
use near_primitives::transaction::{
    ECC_METHOD_NAME, EccTransaction, ExecutionStatus, TransactionEnvelope,
};
use near_primitives::types::{AccountId, Balance, TransactionOrReceiptId};
use near_primitives::utils::create_ecc_outcome_id;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_primitives::views::{FinalExecutionStatus, TransactionEnvelopeView, TxExecutionStatus};
use near_primitives_core::serialize::to_base64;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

fn ecc(contract_id: &AccountId) -> EccTransaction {
    EccTransaction::new(contract_id.clone(), vec![1, 2, 3])
}

/// The first byte of the payload of the ECCs that `ecc_contract` refuses.
const REFUSED_TAG: u8 = 255;

/// A contract whose ECC method returns the payload, and refuses a call whose payload starts with
/// `REFUSED_TAG`.
fn ecc_contract() -> Vec<u8> {
    near_test_contracts::wat_contract(&format!(
        r#"(module
  (import "env" "input" (func $input (param i64)))
  (import "env" "register_len" (func $register_len (param i64) (result i64)))
  (import "env" "read_register" (func $read_register (param i64 i64)))
  (import "env" "value_return" (func $value_return (param i64 i64)))
  (memory (export "memory") 1)
  (func (export "{ECC_METHOD_NAME}")
    (call $input (i64.const 0))
    (call $read_register (i64.const 0) (i64.const 0))
    (if (i32.eq (i32.load8_u (i32.const 0)) (i32.const {REFUSED_TAG})) (then unreachable))
    (call $value_return (call $register_len (i64.const 0)) (i64.const 0))
  )
)"#
    ))
}

/// Gives the producers plenty of time to prepare a chunk, and so ECCs half of it. A producer runs
/// an ECC against the wall clock, and it drops an ECC that is late with all the time ECCs get, so
/// on a slow machine, or at the first run of a contract that compiles it, the default 100 ms could
/// lose an ECC that the test waits for.
fn give_eccs_time(client_config: &mut ClientConfig) {
    client_config.produce_chunk_add_transactions_time_limit = MutableConfigValue::new(
        Some(Duration::seconds(10)),
        "produce_chunk_add_transactions_time_limit",
    );
}

/// A chain with an RPC node that tracks every shard and produces no chunks, and a contract on it.
fn setup(protocol_version: u32, accounts: &[&AccountId], contract_id: &AccountId) -> TestLoopEnv {
    let mut builder = TestLoopBuilder::new()
        .enable_rpc()
        .protocol_version(protocol_version)
        .epoch_length(10)
        .track_all_shards()
        .config_modifier(|client_config, _| give_eccs_time(client_config));
    for account_id in accounts {
        builder = builder.add_user_account(account_id, Balance::from_near(1_000));
    }
    let mut env = builder.build();
    let deploy_tx = env.rpc_node().tx_deploy_contract(contract_id, ecc_contract());
    env.rpc_runner().run_tx(deploy_tx, Duration::seconds(10));
    env
}

#[test]
fn test_rpc_refuses_ecc_before_the_feature() {
    init_test_logger();
    // A chain cannot run above the binary's protocol version, so on a stable build this is
    // `PROTOCOL_VERSION` itself.
    let feature_version = ProtocolFeature::ExternalContractCalls.protocol_version();
    let protocol_version = PROTOCOL_VERSION.min(feature_version - 1);
    let contract_id = create_account_id("contract");
    let env = setup(protocol_version, &[&contract_id], &contract_id);

    let response = env.rpc_node().process_tx(ecc(&contract_id), false);
    assert_eq!(response, ProcessTxResponse::InvalidTx(InvalidTxError::InvalidTransactionVersion));
}

#[test]
fn test_rpc_refuses_ecc_to_account_without_contract() {
    init_test_logger();
    if !ProtocolFeature::ExternalContractCalls.enabled(PROTOCOL_VERSION) {
        tracing::info!("skipping: ECCs are not enabled in this build");
        return;
    }
    let contract_id = create_account_id("contract");
    let alice = create_account_id("alice");
    let nobody = create_account_id("nobody");
    let env = setup(PROTOCOL_VERSION, &[&contract_id, &alice], &contract_id);

    // An account that exists but holds no contract.
    let response = env.rpc_node().process_tx(ecc(&alice), false);
    assert_eq!(
        response,
        ProcessTxResponse::InvalidTx(InvalidTxError::ContractNotFound { contract_id: alice })
    );

    // An account that does not exist: the error is the one apply gives.
    let response = env.rpc_node().process_tx(ecc(&nobody), false);
    assert_eq!(
        response,
        ProcessTxResponse::InvalidTx(InvalidTxError::ContractNotFound { contract_id: nobody })
    );
}

/// `send_tx` with `NONE` hands an ECC to the node without waiting, and a refusal comes back as
/// an error.
#[test]
fn test_jsonrpc_send_tx_with_ecc() {
    init_test_logger();
    if !ProtocolFeature::ExternalContractCalls.enabled(PROTOCOL_VERSION) {
        tracing::info!("skipping: ECCs are not enabled in this build");
        return;
    }
    let contract_id = create_account_id("contract");
    let alice = create_account_id("alice");
    let mut env = setup(PROTOCOL_VERSION, &[&contract_id, &alice], &contract_id);

    let response = env
        .rpc_runner()
        .run_with_jsonrpc_client(
            |client| client.send_tx(ecc(&contract_id), TxExecutionStatus::None),
            Duration::seconds(5),
        )
        .unwrap();
    assert!(response.final_execution_outcome.is_none());
    assert_eq!(response.final_execution_status, TxExecutionStatus::None);

    let err = env
        .rpc_runner()
        .run_with_jsonrpc_client(
            |client| client.send_tx(ecc(&alice), TxExecutionStatus::Final),
            Duration::seconds(5),
        )
        .unwrap_err();
    assert!(format!("{err:?}").contains("ContractNotFound"), "unexpected error: {err:?}");
}

#[test]
fn test_rpc_refuses_oversized_ecc() {
    init_test_logger();
    if !ProtocolFeature::ExternalContractCalls.enabled(PROTOCOL_VERSION) {
        tracing::info!("skipping: ECCs are not enabled in this build");
        return;
    }
    let contract_id = create_account_id("contract");
    let env = setup(PROTOCOL_VERSION, &[&contract_id], &contract_id);

    let oversized = EccTransaction::new(contract_id.clone(), vec![0; 2 * 1024 * 1024]);
    let response = env.rpc_node().process_tx(oversized, false);
    assert_matches!(
        response,
        ProcessTxResponse::InvalidTx(InvalidTxError::TransactionSizeExceeded { .. })
    );

    // Past the limit of an ECC, though far below the limit of any transaction.
    let config = env.rpc_node().client().runtime_adapter.get_runtime_config(PROTOCOL_VERSION);
    let limit = config.external_calls_config.max_call_size;
    let payload_len = usize::try_from(limit).unwrap();
    let oversized = EccTransaction::new(contract_id, vec![0; payload_len]);
    let response = env.rpc_node().process_tx(oversized, false);
    assert_matches!(
        response,
        ProcessTxResponse::InvalidTx(InvalidTxError::TransactionSizeExceeded { limit: l, .. })
            if l == limit
    );
}

/// Two shards, split at "bb", a chunk producer for each of them, and a contract in each shard
/// (deployed through a chunk producer). The RPC node does not produce chunks, and tracks the
/// shards only if `rpc_tracks_shards`.
fn setup_two_shards(contracts: &[&AccountId], rpc_tracks_shards: bool) -> TestLoopEnv {
    let mut builder = TestLoopBuilder::new()
        .enable_rpc()
        .epoch_length(10)
        .validators_spec(ValidatorsSpec::desired_roles(&["cp0", "cp1"], &[]))
        .shard_layout(ShardLayout::multi_shard_custom(vec![create_account_id("bb")], 1))
        .track_all_shards()
        .config_modifier(move |client_config, idx| {
            give_eccs_time(client_config);
            // The RPC node is the client after the two chunk producers.
            if !rpc_tracks_shards && idx == 2 {
                client_config.tracked_shards_config = TrackedShardsConfig::NoShards;
            }
        });
    for contract_id in contracts {
        builder = builder.add_user_account(contract_id, Balance::from_near(1_000));
    }
    let mut env = builder.build();
    for contract_id in contracts {
        let deploy_tx = env.validator().tx_deploy_contract(contract_id, ecc_contract());
        env.validator_runner().run_tx(deploy_tx, Duration::seconds(10));
    }
    env
}

/// An ECC goes to the chunk producers of the contract's shard, whichever shard that is.
#[test]
fn test_rpc_forwards_ecc_to_producers_of_the_contracts_shard() {
    init_test_logger();
    if !ProtocolFeature::ExternalContractCalls.enabled(PROTOCOL_VERSION) {
        tracing::info!("skipping: ECCs are not enabled in this build");
        return;
    }
    let contracts = [create_account_id("a-contract"), create_account_id("contract")];
    let mut env = setup_two_shards(&[&contracts[0], &contracts[1]], true);

    let forwarded: Rc<RefCell<Vec<(AccountId, CryptoHash)>>> = Rc::default();
    let forwarded_clone = forwarded.clone();
    let rpc_data_idx = env.rpc_data_idx();
    env.node_datas[rpc_data_idx].register_override_handler(&mut env.test_loop.data, move |nr| {
        if let NetworkRequests::ForwardTx(target, tx) = &nr {
            forwarded_clone.borrow_mut().push((target.clone(), tx.get_hash()));
        }
        HandlerResult::Unhandled(nr)
    });

    let client = env.rpc_node().client();
    let head = env.rpc_node().head();
    let shard_layout = client.epoch_manager.get_shard_layout(&head.epoch_id).unwrap();
    let producers_of = |contract_id: &AccountId| {
        let shard_id = shard_layout.account_id_to_shard_id(contract_id);
        client.epoch_manager.get_epoch_chunk_producers_for_shard(&head.epoch_id, shard_id).unwrap()
    };
    let producers = contracts.each_ref().map(producers_of);
    // Otherwise a wrong shard would go unnoticed.
    assert!(
        producers[0].iter().all(|p| !producers[1].contains(p)),
        "the shards share producers: {producers:?}"
    );

    for (contract_id, producers) in contracts.iter().zip(&producers) {
        forwarded.borrow_mut().clear();
        let ecc = EccTransaction::new(contract_id.clone(), vec![1, 2, 3]);
        let response = env.rpc_node().process_tx(ecc.clone(), false);
        assert_eq!(response, ProcessTxResponse::RequestRouted);
        env.rpc_runner().run_for_number_of_blocks(1);

        let forwarded = forwarded.borrow();
        assert!(!forwarded.is_empty(), "the ECC to {contract_id} was not forwarded");
        for (target, hash) in forwarded.iter() {
            assert!(producers.contains(target), "{contract_id}: sent to {target}");
            assert_eq!(*hash, ecc.get_hash());
        }
    }
}

/// A node that does not track the contract's shard cannot tell whether the account holds a
/// contract, so it forwards the ECC as it would a signed transaction.
#[test]
fn test_rpc_without_the_shard_forwards_ecc_unchecked() {
    init_test_logger();
    if !ProtocolFeature::ExternalContractCalls.enabled(PROTOCOL_VERSION) {
        tracing::info!("skipping: ECCs are not enabled in this build");
        return;
    }
    let contract_id = create_account_id("contract");
    let nobody = create_account_id("nobody");
    let env = setup_two_shards(&[&contract_id], false);

    let client = env.rpc_node().client();
    let head = env.rpc_node().head();
    let shard_layout = client.epoch_manager.get_shard_layout(&head.epoch_id).unwrap();
    let shard_id = shard_layout.account_id_to_shard_id(&nobody);
    assert!(
        !client.shard_tracker.cares_about_shard_this_or_next_epoch(&head.last_block_hash, shard_id),
        "the RPC node tracks the shard"
    );

    let response = env.rpc_node().process_tx(EccTransaction::new(nobody, vec![]), false);
    assert_eq!(response, ProcessTxResponse::RequestRouted);
}

/// The chunks, from `from_height` on, that hold the ECC with the given hash. A chunk that a block
/// has again because its shard missed a chunk counts once.
fn chunks_with_ecc(
    node: &TestLoopNode,
    ecc_hash: CryptoHash,
    from_height: u64,
) -> HashSet<ChunkHash> {
    let chain = &node.client().chain;
    (from_height..=node.head().height)
        .filter_map(|height| chain.get_block_by_height(height).ok())
        .flat_map(|block| node.block_chunks(&block))
        .filter(|chunk| chunk.to_transactions().iter().any(|tx| tx.get_hash() == ecc_hash))
        .map(|chunk| chunk.chunk_hash().clone())
        .collect()
}

/// The id of the outcome of the ECC with the given hash, in the first chunk from `from_height` on
/// that holds it. Apply derives it from the hash and the height of that chunk.
fn ecc_outcome_id(node: &TestLoopNode, ecc_hash: CryptoHash, from_height: u64) -> CryptoHash {
    let chain = &node.client().chain;
    let height = (from_height..=node.head().height)
        .filter_map(|height| chain.get_block_by_height(height).ok())
        .flat_map(|block| node.block_chunks(&block))
        .find(|chunk| chunk.to_transactions().iter().any(|tx| tx.get_hash() == ecc_hash))
        .expect("the ECC should be in a chunk")
        .height_included();
    create_ecc_outcome_id(&ecc_hash, height)
}

/// RPC finds an external call by its unique id: `send_tx` waits for the copy that lands and returns
/// it, and `tx`, `light_client_proof` and `EXPERIMENTAL_receipt_to_tx` take that id.
#[test]
fn test_jsonrpc_finds_ecc_by_unique_id() {
    init_test_logger();
    // TODO(spice): the producer does not pack ECCs under Spice yet.
    if !ProtocolFeature::ExternalContractCalls.enabled(PROTOCOL_VERSION)
        || ProtocolFeature::Spice.enabled(PROTOCOL_VERSION)
    {
        tracing::info!("skipping: ECCs are not packed in this build");
        return;
    }
    const RPC_TIMEOUT: Duration = Duration::seconds(30);
    // Enough blocks for the outcome's block to be final under the light client head.
    const BLOCKS_TO_FINALITY: usize = 4;
    let echo_id = create_account_id("echo");
    let sender_id = create_account_id("sender");
    let receiver_id = create_account_id("receiver");
    let mut env = setup(PROTOCOL_VERSION, &[&echo_id, &sender_id, &receiver_id], &echo_id);
    let deploy_tx = env.rpc_node().tx_deploy_contract(&echo_id, ecc_contract());
    env.rpc_runner().run_tx(deploy_tx, RPC_TIMEOUT);
    let deploy_tx =
        env.rpc_node().tx_deploy_contract(&sender_id, ecc_transfer_contract(&receiver_id));
    env.rpc_runner().run_tx(deploy_tx, RPC_TIMEOUT);

    // `INCLUDED` answers with the copy that landed, under the unique id of its block's height.
    let included = EccTransaction::new(echo_id.clone(), vec![1]);
    let response = env
        .rpc_runner()
        .run_with_jsonrpc_client(
            |client| client.send_tx(included.clone(), TxExecutionStatus::Included),
            RPC_TIMEOUT,
        )
        .unwrap();
    let outcome = response.final_execution_outcome.unwrap().into_outcome();
    let TransactionEnvelopeView::Unsigned(view) = &outcome.transaction else {
        panic!("expected an external call, got {:?}", outcome.transaction);
    };
    assert_eq!(view.contract_id, echo_id);
    let block_hash = outcome.transaction_outcome.block_hash;
    let height = env.rpc_node().client().chain.get_block_header(&block_hash).unwrap().height();
    let transaction_id = create_ecc_outcome_id(&included.get_hash(), height);
    assert_eq!(view.transaction_id, transaction_id);
    assert_eq!(outcome.transaction_outcome.id, transaction_id);

    // `FINAL` also answers with the return value.
    let payload = vec![2, 3];
    let response = env
        .rpc_runner()
        .run_with_jsonrpc_client(
            |client| {
                let call = EccTransaction::new(echo_id.clone(), payload.clone());
                client.send_tx(call, TxExecutionStatus::Final)
            },
            RPC_TIMEOUT,
        )
        .unwrap();
    let outcome = response.final_execution_outcome.unwrap().into_outcome();
    assert_eq!(outcome.status, FinalExecutionStatus::SuccessValue(vec![2, 3]));

    // `tx` and `light_client_proof` take the unique id, with the contract as sender.
    let tx_status = env
        .rpc_runner()
        .run_with_jsonrpc_client(
            |client| {
                client.tx(RpcTransactionStatusRequest {
                    transaction_info: TransactionInfo::TransactionId {
                        tx_hash: transaction_id,
                        sender_account_id: echo_id.clone(),
                    },
                    wait_until: TxExecutionStatus::Final,
                })
            },
            RPC_TIMEOUT,
        )
        .unwrap();
    let found = tx_status.final_execution_outcome.unwrap().into_outcome();
    assert_eq!(found.transaction_outcome.id, transaction_id);
    assert_matches!(&found.transaction, TransactionEnvelopeView::Unsigned(found_view) if found_view == view);
    env.rpc_runner().run_for_number_of_blocks(BLOCKS_TO_FINALITY);
    // The proof is checked against a final block.
    let head_header = env.rpc_node().client().chain.head_header().unwrap();
    let light_client_head = *head_header.last_final_block();
    let proof = env
        .rpc_runner()
        .run_with_jsonrpc_client(
            |client| {
                client.light_client_proof(RpcLightClientExecutionProofRequest {
                    id: TransactionOrReceiptId::Transaction {
                        transaction_hash: transaction_id,
                        sender_id: echo_id.clone(),
                    },
                    light_client_head,
                })
            },
            RPC_TIMEOUT,
        )
        .unwrap();
    assert_eq!(proof.outcome_proof.id, transaction_id);
    // The proof names the block whose outcome root confirms the outcome, a later one.
    assert!(proof.block_header_lite.inner_lite.height > height);

    // `broadcast_tx_async` has no id to return, and `broadcast_tx_commit` returns the outcome.
    let async_call = to_base64(
        &borsh::to_vec(&TransactionEnvelope::from(EccTransaction::new(echo_id.clone(), vec![4])))
            .unwrap(),
    );
    let hash = env
        .rpc_runner()
        .run_with_jsonrpc_client(|client| client.broadcast_tx_async(async_call), RPC_TIMEOUT)
        .unwrap();
    assert_eq!(hash, None);
    let commit_call = to_base64(
        &borsh::to_vec(&TransactionEnvelope::from(EccTransaction::new(echo_id, vec![5]))).unwrap(),
    );
    let response = env
        .rpc_runner()
        .run_with_jsonrpc_client(|client| client.broadcast_tx_commit(commit_call), RPC_TIMEOUT)
        .unwrap();
    let outcome = response.final_execution_outcome.unwrap().into_outcome();
    assert_eq!(outcome.status, FinalExecutionStatus::SuccessValue(vec![5]));

    // `EXPERIMENTAL_receipt_to_tx` maps a call's receipt to the call's unique id and contract.
    let response = env
        .rpc_runner()
        .run_with_jsonrpc_client(
            |client| client.send_tx(ecc(&sender_id), TxExecutionStatus::Final),
            RPC_TIMEOUT,
        )
        .unwrap();
    let outcome = response.final_execution_outcome.unwrap().into_outcome();
    let [receipt_id] = outcome.transaction_outcome.outcome.receipt_ids.as_slice() else {
        panic!("expected one receipt, got {:?}", outcome.transaction_outcome);
    };
    let receipt_id = *receipt_id;
    let origin = env
        .rpc_runner()
        .run_with_jsonrpc_client(
            |client| {
                client.EXPERIMENTAL_receipt_to_tx(RpcReceiptToTxRequest {
                    receipt_reference: ReceiptReference { receipt_id },
                    block_height: None,
                    shard_id: None,
                    window: None,
                })
            },
            RPC_TIMEOUT,
        )
        .unwrap();
    assert_eq!(origin.transaction_hash, outcome.transaction_outcome.id);
    assert_eq!(origin.sender_account_id, sender_id);
}

/// An ECC sent to an RPC node that produces no chunks lands in a chunk of its contract's shard,
/// once, and runs there: the contract accepts it, and the outcome holds what its method returned.
#[test]
fn test_ecc_lands_in_a_chunk() {
    init_test_logger();
    // TODO(spice): the producer does not pack ECCs under Spice yet.
    if !ProtocolFeature::ExternalContractCalls.enabled(PROTOCOL_VERSION)
        || ProtocolFeature::Spice.enabled(PROTOCOL_VERSION)
    {
        tracing::info!("skipping: ECCs are not packed in this build");
        return;
    }
    let contract_id = create_account_id("contract");
    let mut env = setup(PROTOCOL_VERSION, &[&contract_id], &contract_id);

    let payload = vec![1, 2, 3];
    let ecc = EccTransaction::new(contract_id, payload.clone());
    let ecc_hash = ecc.get_hash();
    assert_eq!(env.rpc_node().process_tx(ecc, false), ProcessTxResponse::RequestRouted);

    let included_at = env.rpc_runner().run_until_included(&[ecc_hash]);
    let outcome_id = create_ecc_outcome_id(&ecc_hash, included_at);
    let outcome = env.rpc_runner().run_until_outcome_available(outcome_id, Duration::seconds(10));
    let outcome = outcome.outcome_with_id.outcome;
    assert_eq!(outcome.status, ExecutionStatus::SuccessValue(payload));

    // Nothing packs it again once a block has it.
    env.rpc_runner().run_for_number_of_blocks(5);
    assert_eq!(chunks_with_ecc(&env.rpc_node(), ecc_hash, included_at).len(), 1);
}

/// A contract gets one ECC per chunk, so the ECCs to it that wait go into the following chunks,
/// in the order they came.
#[test]
fn test_eccs_to_one_contract_land_in_separate_chunks_in_order() {
    init_test_logger();
    // TODO(spice): the producer does not pack ECCs under Spice yet.
    if !ProtocolFeature::ExternalContractCalls.enabled(PROTOCOL_VERSION)
        || ProtocolFeature::Spice.enabled(PROTOCOL_VERSION)
    {
        tracing::info!("skipping: ECCs are not packed in this build");
        return;
    }
    let contract_id = create_account_id("contract");
    let mut env = setup(PROTOCOL_VERSION, &[&contract_id], &contract_id);

    let eccs = [1, 2, 3].map(|tag| EccTransaction::new(contract_id.clone(), vec![tag]));
    let hashes = eccs.each_ref().map(EccTransaction::get_hash);
    for ecc in eccs {
        assert_eq!(env.rpc_node().process_tx(ecc, false), ProcessTxResponse::RequestRouted);
    }

    let heights = hashes.map(|hash| env.rpc_runner().run_until_included(&[hash]));

    assert!(heights[0] < heights[1] && heights[1] < heights[2], "heights: {heights:?}");
    env.rpc_runner().run_for_number_of_blocks(5);
    for hash in hashes {
        assert_eq!(chunks_with_ecc(&env.rpc_node(), hash, heights[0]).len(), 1);
    }
}

/// The producer packs no more ECCs into a chunk than their share of its transaction bytes
/// holds: six ECCs of the largest size to different contracts go into chunks of five at most,
/// each within the share, and every one of them gets an outcome.
#[test]
fn test_chunks_hold_eccs_within_their_size_share() {
    init_test_logger();
    // TODO(spice): the producer does not pack ECCs under Spice yet.
    if !ProtocolFeature::ExternalContractCalls.enabled(PROTOCOL_VERSION)
        || ProtocolFeature::Spice.enabled(PROTOCOL_VERSION)
    {
        tracing::info!("skipping: ECCs are not packed in this build");
        return;
    }
    let contract_ids = ["c1", "c2", "c3", "c4", "c5", "c6"].map(create_account_id);
    let mut builder = TestLoopBuilder::new()
        .enable_rpc()
        .epoch_length(10)
        .config_modifier(|client_config, _| give_eccs_time(client_config));
    for contract_id in &contract_ids {
        builder = builder.add_user_account(contract_id, Balance::from_near(1_000));
    }
    let mut env = builder.build();
    for contract_id in &contract_ids {
        let deploy_tx = env.rpc_node().tx_deploy_contract(contract_id, ecc_contract());
        env.rpc_runner().run_tx(deploy_tx, Duration::seconds(10));
    }
    let node = env.rpc_node();
    let config = node.client().runtime_adapter.get_runtime_config(PROTOCOL_VERSION);
    let max_call_size = config.external_calls_config.max_call_size;
    let size_share = config.external_calls_config.transactions_size_share(&config.witness_config);
    assert_eq!(size_share / max_call_size, 5);
    let first_height = node.head().height;

    let eccs = contract_ids.each_ref().map(|id| {
        let overhead = EccTransaction::new(id.clone(), vec![]).get_size();
        let payload_len = usize::try_from(max_call_size - overhead).unwrap();
        EccTransaction::new(id.clone(), vec![1; payload_len])
    });
    let hashes = eccs.each_ref().map(EccTransaction::get_hash);
    for ecc in eccs {
        assert_eq!(ecc.get_size(), max_call_size);
        assert_eq!(env.rpc_node().process_tx(ecc, false), ProcessTxResponse::RequestRouted);
    }
    env.rpc_runner().run_until(
        |node| hashes.iter().all(|hash| !chunks_with_ecc(node, *hash, first_height).is_empty()),
        Duration::seconds(30),
    );
    for hash in hashes {
        let outcome_id = ecc_outcome_id(&env.rpc_node(), hash, first_height);
        env.rpc_runner().run_until_outcome_available(outcome_id, Duration::seconds(10));
    }

    let node = env.rpc_node();
    let chain = &node.client().chain;
    let chunks = (first_height..=node.head().height)
        .filter_map(|height| chain.get_block_by_height(height).ok())
        .flat_map(|block| node.block_chunks(&block))
        .map(|chunk| (chunk.chunk_hash().clone(), chunk))
        .collect::<HashMap<_, _>>();
    let ecc_sizes_per_chunk = chunks
        .values()
        .map(|chunk| {
            let transactions = chunk.to_transactions();
            let eccs = transactions.iter().filter_map(|tx| tx.as_unsigned());
            eccs.map(EccTransaction::get_size).collect::<Vec<_>>()
        })
        .filter(|sizes| !sizes.is_empty())
        .collect::<Vec<_>>();
    let num_eccs = ecc_sizes_per_chunk.iter().map(Vec::len).sum::<usize>();
    assert_eq!(num_eccs, 6, "{ecc_sizes_per_chunk:?}");
    for sizes in &ecc_sizes_per_chunk {
        assert!(sizes.len() <= 5, "{ecc_sizes_per_chunk:?}");
        assert!(sizes.iter().sum::<u64>() <= size_share, "{ecc_sizes_per_chunk:?}");
    }
}

/// The producer runs each ECC before it packs it: one that the contract accepts lands with a
/// successful outcome, and one that it refuses never lands, and is not tried again.
#[test]
fn test_producer_packs_the_ecc_the_contract_accepts_and_drops_the_one_it_refuses() {
    init_test_logger();
    // TODO(spice): the producer does not pack ECCs under Spice yet.
    if !ProtocolFeature::ExternalContractCalls.enabled(PROTOCOL_VERSION)
        || ProtocolFeature::Spice.enabled(PROTOCOL_VERSION)
    {
        tracing::info!("skipping: ECCs are not packed in this build");
        return;
    }
    let contract_id = create_account_id("contract");
    let mut env = setup(PROTOCOL_VERSION, &[&contract_id], &contract_id);
    let first_height = env.rpc_node().head().height;

    // The refused call comes first, and the producer drops it without giving up the contract's
    // place in the chunk for the next one.
    let refused = EccTransaction::new(contract_id.clone(), vec![REFUSED_TAG]);
    let accepted = EccTransaction::new(contract_id, vec![1, 2]);
    let (refused_hash, accepted_hash) = (refused.get_hash(), accepted.get_hash());
    for ecc in [refused, accepted] {
        assert_eq!(env.rpc_node().process_tx(ecc, false), ProcessTxResponse::RequestRouted);
    }

    let included_at = env.rpc_runner().run_until_included(&[accepted_hash]);
    let outcome_id = create_ecc_outcome_id(&accepted_hash, included_at);
    let outcome = env.rpc_runner().run_until_outcome_available(outcome_id, Duration::seconds(10));
    assert_eq!(outcome.outcome_with_id.outcome.status, ExecutionStatus::SuccessValue(vec![1, 2]));

    env.rpc_runner().run_for_number_of_blocks(10);
    assert!(chunks_with_ecc(&env.rpc_node(), refused_hash, first_height).is_empty());
    assert_eq!(chunks_with_ecc(&env.rpc_node(), accepted_hash, first_height).len(), 1);
}

/// The producer stops packing ECCs once they use up their share of the chunk's compute, so apply
/// refuses none of the ones it packed. With a share that one ECC uses up, three ECCs to different
/// contracts, which wait in the pool together, go into three chunks, one each, and the contracts
/// accept all of them. Had the producer packed them into one chunk, apply would have refused the
/// last two for reaching the share.
#[test]
fn test_chunks_hold_eccs_within_their_compute_share() {
    init_test_logger();
    // TODO(spice): the producer does not pack ECCs under Spice yet.
    if !ProtocolFeature::ExternalContractCalls.enabled(PROTOCOL_VERSION)
        || ProtocolFeature::Spice.enabled(PROTOCOL_VERSION)
    {
        tracing::info!("skipping: ECCs are not packed in this build");
        return;
    }
    let contract_ids = ["c1", "c2", "c3"].map(create_account_id);
    let mut runtime_config = RuntimeConfig::test();
    runtime_config.external_calls_config.chunk_compute_share = Rational32::new(1, i32::MAX);
    let mut builder = TestLoopBuilder::new()
        .enable_rpc()
        .epoch_length(10)
        .runtime_config_store(RuntimeConfigStore::with_one_config(runtime_config))
        .config_modifier(|client_config, _| give_eccs_time(client_config));
    for contract_id in &contract_ids {
        builder = builder.add_user_account(contract_id, Balance::from_near(1_000));
    }
    let mut env = builder.build();
    for contract_id in &contract_ids {
        let deploy_tx = env.rpc_node().tx_deploy_contract(contract_id, ecc_contract());
        env.rpc_runner().run_tx(deploy_tx, Duration::seconds(10));
    }
    let first_height = env.rpc_node().head().height;

    let eccs = contract_ids.each_ref().map(ecc);
    let hashes = eccs.each_ref().map(EccTransaction::get_hash);
    for ecc in eccs {
        assert_eq!(env.rpc_node().process_tx(ecc, false), ProcessTxResponse::RequestRouted);
    }
    env.rpc_runner().run_until(
        |node| hashes.iter().all(|hash| !chunks_with_ecc(node, *hash, first_height).is_empty()),
        Duration::seconds(30),
    );

    for hash in hashes {
        let outcome_id = ecc_outcome_id(&env.rpc_node(), hash, first_height);
        let outcome =
            env.rpc_runner().run_until_outcome_available(outcome_id, Duration::seconds(10));
        let status = outcome.outcome_with_id.outcome.status;
        assert_eq!(status, ExecutionStatus::SuccessValue(vec![1, 2, 3]));
    }
    let chunks_of_hash = hashes.map(|hash| chunks_with_ecc(&env.rpc_node(), hash, first_height));
    let distinct_chunks = chunks_of_hash.iter().flatten().collect::<HashSet<_>>();
    assert_eq!(distinct_chunks.len(), 3, "{chunks_of_hash:?}");
}

/// What the contract from `ecc_transfer_contract` sends to its receiver on every ECC.
const ECC_TRANSFER_AMOUNT: Balance = Balance::from_near(1);

/// A contract whose ECC method transfers `ECC_TRANSFER_AMOUNT` to `receiver_id`.
fn ecc_transfer_contract(receiver_id: &AccountId) -> Vec<u8> {
    const RECEIVER_OFFSET: usize = 0;
    const AMOUNT_OFFSET: usize = 256;
    let receiver = receiver_id.as_str();
    let receiver_len = receiver.len();
    // WAT data strings take raw bytes as `\xx` escapes; the amount is a little-endian u128.
    let amount_bytes = ECC_TRANSFER_AMOUNT.as_yoctonear().to_le_bytes();
    let amount: String = amount_bytes.iter().map(|byte| format!("\\{byte:02x}")).collect();
    near_test_contracts::wat_contract(&format!(
        r#"(module
  (import "env" "promise_batch_create" (func $promise_batch_create (param i64 i64) (result i64)))
  (import "env" "promise_batch_action_transfer"
    (func $promise_batch_action_transfer (param i64 i64)))
  (memory (export "memory") 1)
  (data (i32.const {RECEIVER_OFFSET}) "{receiver}")
  (data (i32.const {AMOUNT_OFFSET}) "{amount}")
  (func (export "{ECC_METHOD_NAME}")
    (call $promise_batch_action_transfer
      (call $promise_batch_create (i64.const {receiver_len}) (i64.const {RECEIVER_OFFSET}))
      (i64.const {AMOUNT_OFFSET})))
)"#
    ))
}

/// An accepted ECC's promise becomes a receipt that runs on another shard: the receiver gets the
/// transfer, paid from the contract's balance.
#[test]
fn test_ecc_promise_transfers_to_another_shard() {
    init_test_logger();
    // TODO(spice): the producer does not pack ECCs under Spice yet.
    if !ProtocolFeature::ExternalContractCalls.enabled(PROTOCOL_VERSION)
        || ProtocolFeature::Spice.enabled(PROTOCOL_VERSION)
    {
        tracing::info!("skipping: ECCs are not packed in this build");
        return;
    }
    // Enough blocks for the receipts of a transaction, refunds included, to finish.
    const BLOCKS_TO_SETTLE: usize = 3;
    const OUTCOME_TIMEOUT: Duration = Duration::seconds(10);
    let contract_id = create_account_id("a-contract");
    let receiver_id = create_account_id("receiver");
    let mut env = setup_two_shards(&[&contract_id, &receiver_id], true);
    let head = env.rpc_node().head();
    let shard_layout =
        env.rpc_node().client().epoch_manager.get_shard_layout(&head.epoch_id).unwrap();
    assert_ne!(
        shard_layout.account_id_to_shard_id(&contract_id),
        shard_layout.account_id_to_shard_id(&receiver_id),
        "the contract and the receiver share a shard"
    );
    let deploy_tx =
        env.rpc_node().tx_deploy_contract(&contract_id, ecc_transfer_contract(&receiver_id));
    env.rpc_runner().run_tx(deploy_tx, OUTCOME_TIMEOUT);
    env.rpc_runner().run_for_number_of_blocks(BLOCKS_TO_SETTLE);
    let receiver_before = env.rpc_node().query_balance(&receiver_id);
    let contract_before = env.rpc_node().query_balance(&contract_id);

    let ecc = ecc(&contract_id);
    let ecc_hash = ecc.get_hash();
    assert_eq!(env.rpc_node().process_tx(ecc, false), ProcessTxResponse::RequestRouted);
    let included_at = env.rpc_runner().run_until_included(&[ecc_hash]);
    let outcome_id = create_ecc_outcome_id(&ecc_hash, included_at);
    let outcome = env.rpc_runner().run_until_outcome_available(outcome_id, OUTCOME_TIMEOUT);
    let outcome = outcome.outcome_with_id.outcome;
    let [receipt_id] = outcome.receipt_ids.as_slice() else {
        panic!("expected one receipt, got {outcome:?}");
    };
    let receipt_outcome =
        env.rpc_runner().run_until_outcome_available(*receipt_id, OUTCOME_TIMEOUT);
    let receipt_status = receipt_outcome.outcome_with_id.outcome.status;
    assert_matches!(receipt_status, ExecutionStatus::SuccessValue(_));
    env.rpc_runner().run_for_number_of_blocks(BLOCKS_TO_SETTLE);

    let receiver_after = env.rpc_node().query_balance(&receiver_id);
    assert_eq!(receiver_after, receiver_before.checked_add(ECC_TRANSFER_AMOUNT).unwrap());
    // The test chain burns gas at price 0 but buys the call's attached gas at the minimum purchase
    // price, and the transfer refunds all of it, so the contract pays exactly the transfer.
    let contract_after = env.rpc_node().query_balance(&contract_id);
    assert_eq!(contract_after, contract_before.checked_sub(ECC_TRANSFER_AMOUNT).unwrap());
}
