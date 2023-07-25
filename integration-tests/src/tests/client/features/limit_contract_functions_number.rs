use crate::tests::client::process_blocks::deploy_test_contract;
use crate::tests::client::utils::TestEnvNightshadeSetupExt;
use assert_matches::assert_matches;
use near_chain::ChainGenesis;
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_primitives::errors::{
    ActionErrorKind, CompilationError, FunctionCallError, PrepareError, TxExecutionError,
};
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::version::ProtocolFeature;
use near_primitives::views::FinalExecutionStatus;
use nearcore::config::GenesisExt;

fn verify_contract_limits_upgrade(
    feature: ProtocolFeature,
    function_limit: u32,
    local_limit: u32,
    expected_prepare_err: PrepareError,
) {
    let old_protocol_version = feature.protocol_version() - 1;
    let new_protocol_version = feature.protocol_version();

    let epoch_length = 5;
    // Prepare TestEnv with a contract at the old protocol version.
    let mut env = {
        let mut genesis =
            Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
        genesis.config.epoch_length = epoch_length;
        genesis.config.protocol_version = old_protocol_version;
        let chain_genesis = ChainGenesis::new(&genesis);
        let mut env = TestEnv::builder(chain_genesis)
            .real_epoch_managers(&genesis.config)
            .nightshade_runtimes_with_runtime_config_store(
                &genesis,
                vec![RuntimeConfigStore::new(None)],
            )
            .build();

        deploy_test_contract(
            &mut env,
            "test0".parse().unwrap(),
            &near_test_contracts::LargeContract {
                functions: function_limit + 1,
                locals_per_function: local_limit + 1,
                ..Default::default()
            }
            .make(),
            epoch_length,
            1,
        );
        env
    };

    let account = "test0".parse().unwrap();
    let old_outcome = env.call_main(&account);

    env.upgrade_protocol(new_protocol_version);

    let new_outcome = env.call_main(&account);

    assert_matches!(old_outcome.status, FinalExecutionStatus::SuccessValue(_));
    let e = match new_outcome.status {
        FinalExecutionStatus::Failure(TxExecutionError::ActionError(e)) => e,
        status => panic!("expected transaction to fail, got {:?}", status),
    };
    match e.kind {
        ActionErrorKind::FunctionCallError(FunctionCallError::CompilationError(
            CompilationError::PrepareError(e),
        )) if e == expected_prepare_err => (),
        kind => panic!("got unexpected action error kind: {:?}", kind),
    }
}

// Check that we can't call a contract exceeding functions number limit after upgrade.
#[test]
fn test_function_limit_change() {
    verify_contract_limits_upgrade(
        ProtocolFeature::LimitContractFunctionsNumber,
        100_000,
        0,
        PrepareError::TooManyFunctions,
    );
}

// Check that we can't call a contract exceeding functions number limit after upgrade.
#[test]
fn test_local_limit_change() {
    verify_contract_limits_upgrade(
        ProtocolFeature::LimitContractLocals,
        64,
        15625,
        PrepareError::TooManyLocals,
    );
}
