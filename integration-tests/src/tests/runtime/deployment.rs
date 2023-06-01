use crate::node::{Node, RuntimeNode};
use near_chain_configs::Genesis;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::transaction::{Action, DeployContractAction, SignedTransaction};
use near_primitives::types::AccountId;
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::FinalExecutionStatus;
use near_vm_runner::internal::VMKind;
use nearcore::config::GenesisExt;

const ONE_NEAR: u128 = 10u128.pow(24);

/// Tests if the maximum allowed contract can be deployed with current gas limits
#[test]
fn test_deploy_max_size_contract() {
    let account_id: AccountId = "alice.near".parse().unwrap();
    let test_contract_id: AccountId = "test_contract.alice.near".parse().unwrap();
    let runtime_config_store = RuntimeConfigStore::new(None);
    let config = runtime_config_store.get_config(PROTOCOL_VERSION);

    let genesis = Genesis::test(vec![account_id.clone()], 1);
    let node =
        RuntimeNode::new_from_genesis_and_config(&account_id, genesis, config.as_ref().clone());
    let node_user = node.user();

    // Compute size of a deployment transaction with an almost empty contract payload
    let block_hash = node_user.get_best_block_hash().unwrap_or_default();
    let signed_transaction = SignedTransaction::from_actions(
        node_user.get_access_key_nonce_for_signer(&account_id).unwrap_or_default() + 1,
        test_contract_id.clone(),
        test_contract_id.clone(),
        &*node_user.signer(),
        vec![Action::DeployContract(DeployContractAction { code: vec![0u8] })],
        block_hash,
    );
    let tx_overhead = signed_transaction.get_size();

    // Testable max contract size is limited by both `max_contract_size` and by `max_transaction_size`
    let max_contract_size = config.wasm_config.limit_config.max_contract_size;
    let max_transaction_size = config.wasm_config.limit_config.max_transaction_size;
    let contract_size = max_contract_size.min(max_transaction_size - tx_overhead);
    // Enough token to store contract + 1 NEAR for account
    let token_balance = config.storage_amount_per_byte() * contract_size as u128 + ONE_NEAR;

    // Create test account
    let transaction_result = node_user
        .create_account(
            account_id,
            test_contract_id.clone(),
            node.signer().public_key(),
            token_balance,
        )
        .unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(Vec::new()));
    assert_eq!(transaction_result.receipts_outcome.len(), 2);

    // Deploy contract
    let wasm_binary = near_test_contracts::sized_contract(contract_size as usize);
    // Run code through preparation for validation. (Deploying will succeed either way).
    near_vm_runner::prepare::prepare_contract(&wasm_binary, &config.wasm_config, VMKind::Wasmer2)
        .unwrap();
    let transaction_result =
        node_user.deploy_contract(test_contract_id, wasm_binary.to_vec()).unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(Vec::new()));
    assert_eq!(transaction_result.receipts_outcome.len(), 1);

    // Check total TX gas is in limit
    let tx_conversion_gas_burnt = transaction_result.transaction_outcome.outcome.gas_burnt;
    let deployment_gas_burnt = transaction_result.receipts_outcome[0].outcome.gas_burnt;
    let total_gas_burnt = tx_conversion_gas_burnt + deployment_gas_burnt;
    assert!(total_gas_burnt <= config.wasm_config.limit_config.max_gas_burnt,);
}
