use assert_matches::assert_matches;
use near_chain::{ChainGenesis, RuntimeAdapter};
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_crypto::{InMemorySigner, KeyType};
use near_primitives::config::VMConfig;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::transaction::{Action, DeployContractAction};
use near_primitives::version::ProtocolFeature;
use near_primitives::views::FinalExecutionStatus;
use near_store::test_utils::create_test_store;
use nearcore::config::GenesisExt;
use nearcore::TrackedConfig;
use std::path::Path;
use std::sync::Arc;

/// Tests if the cost of deployment is higher after the protocol update 53
#[test]
fn test_deploy_cost_increased() {
    let new_protocol_version = ProtocolFeature::IncreaseDeploymentCost.protocol_version();
    let old_protocol_version = new_protocol_version - 1;

    let contract_size = 1024 * 1024;
    let test_contract = near_test_contracts::sized_contract(contract_size);
    // Run code through preparation for validation. (Deploying will succeed either way).
    near_vm_runner::prepare::prepare_contract(&test_contract, &VMConfig::test()).unwrap();

    // Prepare TestEnv with a contract at the old protocol version.
    let epoch_length = 5;
    let mut env = {
        let mut genesis = Genesis::test(vec!["test0".parse().unwrap()], 1);
        genesis.config.epoch_length = epoch_length;
        genesis.config.protocol_version = old_protocol_version;
        let chain_genesis = ChainGenesis::new(&genesis);
        let runtimes: Vec<Arc<dyn RuntimeAdapter>> =
            vec![Arc::new(nearcore::NightshadeRuntime::test_with_runtime_config_store(
                Path::new("../../../.."),
                create_test_store(),
                &genesis,
                TrackedConfig::new_empty(),
                RuntimeConfigStore::new(None),
            ))];
        TestEnv::builder(chain_genesis).runtime_adapters(runtimes).build()
    };

    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let actions = vec![Action::DeployContract(DeployContractAction { code: test_contract })];

    let tx = env.tx_from_actions(actions.clone(), &signer, signer.account_id.clone());
    let old_outcome = env.execute_tx(tx);

    env.upgrade_protocol(new_protocol_version);

    let tx = env.tx_from_actions(actions, &signer, signer.account_id.clone());
    let new_outcome = env.execute_tx(tx);

    assert_matches!(old_outcome.status, FinalExecutionStatus::SuccessValue(_));
    assert_matches!(new_outcome.status, FinalExecutionStatus::SuccessValue(_));

    let old_deploy_gas = old_outcome.receipts_outcome[0].outcome.gas_burnt;
    let new_deploy_gas = new_outcome.receipts_outcome[0].outcome.gas_burnt;
    assert!(new_deploy_gas > old_deploy_gas);
    assert_eq!(new_deploy_gas - old_deploy_gas, contract_size as u64 * (64_572_944 - 6_812_999));
}
