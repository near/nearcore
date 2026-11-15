use crate::env::nightshade_setup::TestEnvNightshadeSetupExt;
use crate::env::test_env::TestEnv;
use crate::utils::process_blocks::{deploy_test_contract, set_block_protocol_version};
use assert_matches::assert_matches;
use near_chain::Provenance;
use near_chain_configs::Genesis;
use near_client::ProcessTxResponse;
use near_crypto::{InMemorySigner, Signer};
use near_parameters::{ExtCosts, RuntimeConfigStore};
use near_primitives::hash::CryptoHash;
use near_primitives::test_utils::encode;
use near_primitives::transaction::{Action, ExecutionMetadata, SignedTransaction};
use near_primitives::types::{BlockHeightDelta, Gas};
use near_primitives::version::{PROTOCOL_VERSION, ProtocolVersion};
use near_primitives::views::FinalExecutionStatus;

fn process_transaction(
    env: &mut TestEnv,
    signer: &Signer,
    num_blocks: BlockHeightDelta,
    protocol_version: ProtocolVersion,
) -> CryptoHash {
    let tip = env.clients[0].chain.head().unwrap();
    let epoch_id =
        env.clients[0].epoch_manager.get_epoch_id_from_prev_block(&tip.last_block_hash).unwrap();
    let block_producer =
        env.clients[0].epoch_manager.get_block_producer(&epoch_id, tip.height).unwrap();
    let last_block_hash = *env.clients[0].chain.get_block_by_height(tip.height).unwrap().hash();
    let next_height = tip.height + 1;
    let gas = 20_000_000_000_000;
    let tx = SignedTransaction::from_actions(
        next_height,
        "test0".parse().unwrap(),
        "test0".parse().unwrap(),
        signer,
        vec![
            Action::function_call("write_key_value", encode(&[0u64, 10u64]), Gas::from_gas(gas)),
            Action::function_call("write_key_value", encode(&[1u64, 20u64]), Gas::from_gas(gas)),
        ],
        last_block_hash,
    );
    let tx_hash = tx.get_hash();
    assert_eq!(env.rpc_handlers[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);

    for i in next_height..next_height + num_blocks {
        let mut block = env.clients[0].produce_block(i).unwrap().unwrap();
        set_block_protocol_version(&mut block, block_producer.clone(), protocol_version);
        env.process_block(0, block.clone(), Provenance::PRODUCED);
    }
    tx_hash
}

/// We run the same transaction 4 times and compare resulting costs. This transaction writes two different key-value
/// pairs to the contract storage.
/// 1st run establishes the trie structure. For our needs, the structure is:
///
///                                                    --> (Leaf) -> (Value 1)
/// (Extension) -> (Branch) -> (Extension) -> (Branch) |
///                                                    --> (Leaf) -> (Value 2)
///
/// Later runs should count 12 nodes: 6 nodes per value, although the first 4 nodes are the same for both values.
#[test]
fn compare_node_counts() {
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    let epoch_length = 10;
    let num_blocks = 5;

    genesis.config.epoch_length = epoch_length;
    genesis.config.transaction_validity_period = epoch_length * 2;
    let mut env = TestEnv::builder(&genesis.config)
        .nightshade_runtimes_with_runtime_config_store(&genesis, vec![RuntimeConfigStore::new()])
        .build();

    deploy_test_contract(
        &mut env,
        "test0".parse().unwrap(),
        near_test_contracts::backwards_compatible_rs_contract(),
        num_blocks,
        1,
    );

    let signer = InMemorySigner::test_signer(&"test0".parse().unwrap());
    let tx_node_counts: Vec<u64> = (0..4)
        .map(|i| {
            let runtime_config =
                env.clients[0].runtime_adapter.get_runtime_config(PROTOCOL_VERSION);
            let touching_trie_node_cost: Gas =
                runtime_config.wasm_config.ext_costs.gas_cost(ExtCosts::touching_trie_node);
            let num_blocks = if i < 1 { num_blocks } else { 2 * epoch_length };
            let tx_hash = process_transaction(&mut env, &signer, num_blocks, PROTOCOL_VERSION);

            let final_result = env.clients[0].chain.get_final_transaction_result(&tx_hash).unwrap();
            assert_matches!(final_result.status, FinalExecutionStatus::SuccessValue(_));
            let transaction_outcome = env.clients[0].chain.get_execution_outcome(&tx_hash).unwrap();
            let receipt_ids = transaction_outcome.outcome_with_id.outcome.receipt_ids;
            assert_eq!(receipt_ids.len(), 1);
            let receipt_execution_outcome =
                env.clients[0].chain.get_execution_outcome(&receipt_ids[0]).unwrap();
            let metadata = receipt_execution_outcome.outcome_with_id.outcome.metadata;
            let profile_data = match metadata {
                ExecutionMetadata::V1 => panic!("ExecutionMetadata cannot be empty"),
                ExecutionMetadata::V2(_) => panic!("expected newest ExecutionMetadata"),
                ExecutionMetadata::V3(profile_data) => *profile_data,
                ExecutionMetadata::V4(v4) => v4.profile,
            };
            assert_eq!(profile_data.get_ext_cost(ExtCosts::read_cached_trie_node), Gas::ZERO);
            let cost = profile_data.get_ext_cost(ExtCosts::touching_trie_node);
            assert_eq!(cost.as_gas() % touching_trie_node_cost.as_gas(), 0);
            cost.checked_div(touching_trie_node_cost.as_gas()).unwrap().as_gas()
        })
        .collect();

    assert_eq!(tx_node_counts, [4, 12, 12, 12]);
}
