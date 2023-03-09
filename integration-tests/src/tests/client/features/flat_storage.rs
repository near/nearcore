use crate::tests::client::process_blocks::{deploy_test_contract, set_block_protocol_version};
use assert_matches::assert_matches;
use near_chain::{ChainGenesis, Provenance, RuntimeWithEpochManagerAdapter};
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_crypto::{InMemorySigner, KeyType, Signer};
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::test_utils::encode;
use near_primitives::transaction::{
    Action, ExecutionMetadata, ExecutionStatus, FunctionCallAction, SignedTransaction,
};
use near_primitives::version::ProtocolFeature;
use near_primitives::views::FinalExecutionStatus;
use near_primitives_core::config::ExtCosts;
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::{BlockHeightDelta, Gas, ProtocolVersion};
use near_store::test_utils::create_test_store;
use nearcore::config::GenesisExt;
use nearcore::TrackedConfig;
use std::path::Path;
use std::sync::Arc;

fn process_transaction(
    env: &mut TestEnv,
    signer: &dyn Signer,
    num_blocks: BlockHeightDelta,
    protocol_version: ProtocolVersion,
    actions: Vec<Action>,
) -> CryptoHash {
    let tip = env.clients[0].chain.head().unwrap();
    let epoch_id =
        env.clients[0].runtime_adapter.get_epoch_id_from_prev_block(&tip.last_block_hash).unwrap();
    let block_producer =
        env.clients[0].runtime_adapter.get_block_producer(&epoch_id, tip.height).unwrap();
    let last_block_hash = *env.clients[0].chain.get_block_by_height(tip.height).unwrap().hash();
    let next_height = tip.height + 1;
    let tx = SignedTransaction::from_actions(
        next_height,
        "test0".parse().unwrap(),
        "test0".parse().unwrap(),
        signer,
        actions,
        last_block_hash,
    );
    let tx_hash = tx.get_hash();
    env.clients[0].process_tx(tx, false, false);

    for i in next_height..next_height + num_blocks {
        let mut block = env.clients[0].produce_block(i).unwrap().unwrap();
        set_block_protocol_version(&mut block, block_producer.clone(), protocol_version);
        env.process_block(0, block.clone(), Provenance::PRODUCED);
    }
    tx_hash
}

#[test]
fn test_flat_storage_upgrade() {
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    let epoch_length = 12;
    let old_protocol_version = ProtocolFeature::FlatStorageReads.protocol_version() - 1;
    let new_protocol_version = old_protocol_version + 1;
    genesis.config.epoch_length = epoch_length;
    genesis.config.protocol_version = old_protocol_version;
    let chain_genesis = ChainGenesis::new(&genesis);
    let runtimes: Vec<Arc<dyn RuntimeWithEpochManagerAdapter>> =
        vec![Arc::new(nearcore::NightshadeRuntime::test_with_runtime_config_store(
            Path::new("../../../.."),
            create_test_store(),
            &genesis,
            TrackedConfig::new_empty(),
            RuntimeConfigStore::new(None),
        ))];
    let mut env = TestEnv::builder(chain_genesis).runtime_adapters(runtimes).build();

    deploy_test_contract(
        &mut env,
        "test0".parse().unwrap(),
        near_test_contracts::base_rs_contract(),
        epoch_length / 3,
        1,
    );

    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let gas = 20_000_000_000_000;
    let write_value_action = vec![Action::FunctionCall(FunctionCallAction {
        args: encode(&[1u64, 10u64]),
        method_name: "write_key_value".to_string(),
        gas,
        deposit: 0,
    })];
    let tx_hash = process_transaction(
        &mut env,
        &signer,
        epoch_length / 3,
        old_protocol_version,
        write_value_action,
    );
    let final_result = env.clients[0].chain.get_final_transaction_result(&tx_hash).unwrap();
    assert_matches!(final_result.status, FinalExecutionStatus::SuccessValue(_));
    let transaction_outcome = env.clients[0].chain.get_execution_outcome(&tx_hash).unwrap();
    let receipt_ids = transaction_outcome.outcome_with_id.outcome.receipt_ids;
    assert_eq!(receipt_ids.len(), 1);
    let receipt_execution_outcome =
        env.clients[0].chain.get_execution_outcome(&receipt_ids[0]).unwrap();
    assert_matches!(
        receipt_execution_outcome.outcome_with_id.outcome.status,
        ExecutionStatus::SuccessValue(_)
    );

    let touching_trie_node_cost: Gas = 16_101_955_926;

    let read_value_action = vec![Action::FunctionCall(FunctionCallAction {
        args: encode(&[1u64]),
        method_name: "read_value".to_string(),
        gas,
        deposit: 0,
    })];
    let tx_hash = process_transaction(
        &mut env,
        &signer,
        epoch_length * 2,
        new_protocol_version,
        read_value_action,
    );
    let final_result = env.clients[0].chain.get_final_transaction_result(&tx_hash).unwrap();
    assert_matches!(final_result.status, FinalExecutionStatus::SuccessValue(_));
    let transaction_outcome = env.clients[0].chain.get_execution_outcome(&tx_hash).unwrap();
    let receipt_ids = transaction_outcome.outcome_with_id.outcome.receipt_ids;
    assert_eq!(receipt_ids.len(), 1);
    let receipt_execution_outcome =
        env.clients[0].chain.get_execution_outcome(&receipt_ids[0]).unwrap();
    let metadata = receipt_execution_outcome.outcome_with_id.outcome.metadata;
    if let ExecutionMetadata::V3(profile_data) = metadata {
        let cost = profile_data.get_ext_cost(ExtCosts::touching_trie_node);
        assert_eq!(cost, touching_trie_node_cost * 4);
    } else {
        panic!("Too old version of metadata: {metadata:?}");
    }

    let read_value_action = vec![Action::FunctionCall(FunctionCallAction {
        args: encode(&[1u64]),
        method_name: "read_value".to_string(),
        gas,
        deposit: 0,
    })];
    let tx_hash = process_transaction(
        &mut env,
        &signer,
        epoch_length * 2,
        new_protocol_version,
        read_value_action,
    );
    let final_result = env.clients[0].chain.get_final_transaction_result(&tx_hash).unwrap();
    assert_matches!(final_result.status, FinalExecutionStatus::SuccessValue(_));
    let transaction_outcome = env.clients[0].chain.get_execution_outcome(&tx_hash).unwrap();
    let receipt_ids = transaction_outcome.outcome_with_id.outcome.receipt_ids;
    assert_eq!(receipt_ids.len(), 1);
    let receipt_execution_outcome =
        env.clients[0].chain.get_execution_outcome(&receipt_ids[0]).unwrap();
    let metadata = receipt_execution_outcome.outcome_with_id.outcome.metadata;
    if let ExecutionMetadata::V3(profile_data) = metadata {
        let cost = profile_data.get_ext_cost(ExtCosts::touching_trie_node);
        assert_eq!(cost, 0);
    } else {
        panic!("Too old version of metadata: {metadata:?}");
    }
}
