use super::super::process_blocks::deploy_test_contract;
use crate::tests::client::utils::TestEnvNightshadeSetupExt;
use assert_matches::assert_matches;
use near_chain::ChainGenesis;
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_primitives::types::{AccountId, BlockHeight};
use near_primitives::views::FinalExecutionStatus;
use nearcore::config::GenesisExt;

/// Create a `TestEnv` with an account and a contract deployed to that account.
fn prepare_env_with_contract(
    epoch_length: u64,
    protocol_version: u32,
    account: AccountId,
    contract: Vec<u8>,
) -> TestEnv {
    let mut genesis = Genesis::test(vec![account.clone()], 1);
    genesis.config.epoch_length = epoch_length;
    genesis.config.protocol_version = protocol_version;
    let mut env = TestEnv::builder(ChainGenesis::new(&genesis))
        .real_epoch_managers(&genesis.config)
        .nightshade_runtimes(&genesis)
        .build();
    deploy_test_contract(&mut env, account, &contract, epoch_length, 1);
    env
}

/// Check that normal execution has the same gas cost after FixContractLoadingCost.
#[test]
fn unchanged_gas_cost() {
    let new_protocol_version =
        near_primitives::version::ProtocolFeature::FixContractLoadingCost.protocol_version();
    let old_protocol_version = new_protocol_version - 1;

    let contract_size = 4096;
    let contract = near_test_contracts::sized_contract(contract_size);

    let epoch_length: BlockHeight = 5;

    let account: AccountId = "test0".parse().unwrap();
    let mut env =
        prepare_env_with_contract(epoch_length, old_protocol_version, account.clone(), contract);

    let old_result = env.call_main(&account);
    let old_gas = old_result.receipts_outcome[0].outcome.gas_burnt;
    assert_matches!(old_result.status, FinalExecutionStatus::SuccessValue(_));

    env.upgrade_protocol(new_protocol_version);

    let new_result = env.call_main(&account);
    let new_gas = new_result.receipts_outcome[0].outcome.gas_burnt;
    assert_matches!(new_result.status, FinalExecutionStatus::SuccessValue(_));

    assert_eq!(old_gas, new_gas);
}

/// Check that execution that fails during contract preparation has the updated gas cost after the update.
#[test]
fn preparation_error_gas_cost() {
    let new_protocol_version =
        near_primitives::version::ProtocolFeature::FixContractLoadingCost.protocol_version();
    let old_protocol_version = new_protocol_version - 1;

    let bad_contract = b"not-a-contract".to_vec();
    let contract_size = bad_contract.len();

    let epoch_length: BlockHeight = 5;

    let account: AccountId = "test0".parse().unwrap();
    let mut env = prepare_env_with_contract(
        epoch_length,
        old_protocol_version,
        account.clone(),
        bad_contract,
    );

    let old_result = env.call_main(&account);
    let old_gas = old_result.receipts_outcome[0].outcome.gas_burnt;
    assert_matches!(old_result.status, FinalExecutionStatus::Failure(_));

    env.upgrade_protocol(new_protocol_version);

    let new_result = env.call_main(&account);
    let new_gas = new_result.receipts_outcome[0].outcome.gas_burnt;
    assert_matches!(new_result.status, FinalExecutionStatus::Failure(_));

    // Gas cost should be different because the upgrade pre-charges loading costs.
    assert_ne!(old_gas, new_gas);
    // Runtime parameter values for version of the protocol upgrade
    let loading_base = 35_445_963;
    let loading_byte = 216_750;
    let loading_cost = loading_base + contract_size as u64 * loading_byte;
    assert_eq!(old_gas + loading_cost, new_gas);
}
