use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use assert_matches::assert_matches;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_parameters::{ExtCosts, RuntimeConfigStore};
use near_primitives::errors::{
    ActionError, ActionErrorKind, CompilationError, FunctionCallError, PrepareError,
    TxExecutionError,
};
use near_primitives::types::{Balance, Gas};
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_primitives::views::FinalExecutionStatus;
use testlib::fees_utils::FeeHelper;

/// Exercise receipt preparation across the upgrade.
///
/// Successful execution keeps its cost, but even invalid Wasm must pay for loading.
/// Insufficient gas must take precedence over compilation errors after the upgrade.
#[test]
fn test_contract_loading_gas_protocol_upgrade() {
    init_test_logger();
    if !ProtocolFeature::FixContractLoadingCost.enabled(PROTOCOL_VERSION) {
        return;
    }
    let new_protocol = ProtocolFeature::FixContractLoadingCost.protocol_version();
    let old_protocol = new_protocol - 1;
    let valid = create_account_id("valid");
    let invalid = create_account_id("invalid");
    let bad_code = b"not-a-contract".to_vec();
    let configs = RuntimeConfigStore::new(None);
    let config = configs.get_config(new_protocol);
    let costs = &config.wasm_config.ext_costs;
    let loading_base = costs.gas_cost(ExtCosts::contract_loading_base);
    let loading_cost = loading_base
        .checked_add(
            costs
                .gas_cost(ExtCosts::contract_loading_bytes)
                .checked_mul(bad_code.len() as u64)
                .unwrap(),
        )
        .unwrap();

    // Leave enough time for all pre-upgrade calls before voting activates the feature.
    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .protocol_version(old_protocol)
        .protocol_upgrade_schedule(ProtocolUpgradeVotingSchedule::new_immediate(new_protocol))
        .epoch_length(50)
        .add_user_accounts([&valid, &invalid], Balance::from_near(1_000))
        .build();
    for (account, code) in
        [(&valid, near_test_contracts::sized_contract(4096)), (&invalid, bad_code)]
    {
        let tx = env.rpc_node().tx_deploy_contract(account, code);
        env.rpc_runner().run_tx(tx, Duration::seconds(10));
    }

    let mut successful_call_gas = None;
    for protocol in [old_protocol, new_protocol] {
        if protocol == new_protocol {
            env.rpc_runner().run_until(
                |node| node.protocol_version_at_head() == new_protocol,
                Duration::seconds(200),
            );
        }
        assert_eq!(env.rpc_node().protocol_version_at_head(), protocol);
        let overhead = FeeHelper::new(configs.get_config(protocol).as_ref().clone(), Balance::ZERO)
            .function_call_exec_gas("main".len() as u64);

        // Both base and byte precharge failures are tested on invalid Wasm: on the
        // old protocol compilation fails for free, on the new one it is never reached.
        for (account, gas) in [
            (&valid, Gas::from_teragas(10)),
            (&invalid, Gas::from_teragas(10)),
            (&invalid, loading_base.checked_sub(Gas::from_gas(1)).unwrap()),
            (&invalid, loading_cost.checked_sub(Gas::from_gas(1)).unwrap()),
        ] {
            let tx = env.rpc_node().tx_call(account, account, "main", vec![], Balance::ZERO, gas);
            let result = env.rpc_runner().execute_tx(tx, Duration::seconds(10)).unwrap();
            let burnt = result.receipts_outcome[0].outcome.gas_burnt;
            if account == &valid {
                assert_matches!(result.status, FinalExecutionStatus::SuccessValue(_));
                if let Some(old_gas) = successful_call_gas {
                    assert_eq!(burnt, old_gas);
                } else {
                    assert!(burnt > overhead);
                    successful_call_gas = Some(burnt);
                }
            } else {
                let out_of_gas = protocol == new_protocol && gas < loading_cost;
                let expected_error = if out_of_gas {
                    FunctionCallError::ExecutionError("Exceeded the prepaid gas.".into())
                } else {
                    FunctionCallError::CompilationError(CompilationError::PrepareError(
                        PrepareError::Deserialization,
                    ))
                };
                assert_eq!(
                    result.status,
                    FinalExecutionStatus::Failure(TxExecutionError::ActionError(ActionError {
                        index: Some(0),
                        kind: ActionErrorKind::FunctionCallError(expected_error),
                    }))
                );
                let expected_loading = if protocol == old_protocol {
                    Gas::ZERO
                } else if out_of_gas {
                    gas
                } else {
                    loading_cost
                };
                assert_eq!(burnt, overhead.checked_add(expected_loading).unwrap());
            }
        }
        // Ensure none of the baseline receipts accidentally executed after activation.
        assert_eq!(env.rpc_node().protocol_version_at_head(), protocol);
    }
}
