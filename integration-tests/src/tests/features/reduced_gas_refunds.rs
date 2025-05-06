use crate::node::{Node, RuntimeNode};
use near_chain_configs::Genesis;
use near_o11y::testonly::init_test_logger;
use near_parameters::{ExtCosts, ParameterCost, RuntimeConfig, RuntimeConfigStore};
use near_primitives::errors::{self, ActionErrorKind};
use near_primitives::types::{Balance, Gas};
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_primitives::views::{ExecutionOutcomeWithIdView, FinalExecutionStatus};
use std::sync::Arc;
use testlib::fees_utils::FeeHelper;
use testlib::runtime_utils::{add_test_contract, alice_account, bob_account};

const TGAS: Gas = 10u64.pow(12);

#[test]
fn test_burn_all_gas() {
    let attached_gas = 100 * TGAS;
    let burn_gas = attached_gas + 1;
    let deposit = 0;

    let refunds = generated_refunds_after_fn_call(attached_gas, burn_gas, deposit);

    if ProtocolFeature::ReducedGasRefunds.enabled(PROTOCOL_VERSION) {
        assert_eq!(refunds, vec![], "new version should have no refunds");
    } else {
        assert_eq!(refunds.len(), 1, "old version should have pessimistic gas price refund");
    }
}

#[test]
fn test_deposit_refund() {
    let attached_gas = 100 * TGAS;
    let burn_gas = attached_gas + 1;
    let deposit = 10;

    let refunds = generated_refunds_after_fn_call(attached_gas, burn_gas, deposit);

    if ProtocolFeature::ReducedGasRefunds.enabled(PROTOCOL_VERSION) {
        assert_eq!(refunds.len(), 1, "new version should only refund deposit");
    } else {
        assert_eq!(refunds.len(), 2, "old version should refund gas and deposit");
    }
}

#[test]
fn test_big_gas_refund() {
    let attached_gas = 100 * TGAS;
    let burn_gas = 10 * TGAS;
    let deposit = 0;

    let refunds = generated_refunds_after_fn_call(attached_gas, burn_gas, deposit);

    assert_eq!(refunds.len(), 1, "big gas refunds should happen on both versions");
}

#[test]
fn test_small_gas_refund() {
    let attached_gas = 10 * TGAS;
    let burn_gas = attached_gas - TGAS / 2;
    let deposit = 0;

    let refunds = generated_refunds_after_fn_call(attached_gas, burn_gas, deposit);

    if ProtocolFeature::ReducedGasRefunds.enabled(PROTOCOL_VERSION) {
        assert_eq!(refunds, vec![], "new version should not refund small amounts");
    } else {
        assert_eq!(refunds.len(), 1, "old version should refund");
    }
}

/// Run a simple NOOP contract function call and return the refund outcomes to
/// check if the match the expected output.
///
/// This method also checks if the difference between gas costs and balance
/// changes corresponds to the expected gas penalty.
fn generated_refunds_after_fn_call(
    attached_gas: Gas,
    burn_gas: Gas,
    deposit: Balance,
) -> Vec<ExecutionOutcomeWithIdView> {
    let (node, fee_helper) = setup_env(burn_gas);
    let node_user = node.user();
    let balance_before = node_user.view_balance(&alice_account()).unwrap();

    // Make a call to the noop method in the test contract. This executes
    // nothing but still charges the contract loading cost, which in this setup has
    // a cost matching the gas we want to burn.
    let method_name = "noop";
    let args = vec![];
    let bytes = method_name.as_bytes().len() as u64;

    let outcome = node_user
        .function_call(alice_account(), bob_account(), method_name, args, attached_gas, deposit)
        .expect("function call TX should succeed");

    // should either succeed or fail due to running out of gas
    match &outcome.status {
        FinalExecutionStatus::SuccessValue(_) => (),
        FinalExecutionStatus::Failure(errors::TxExecutionError::ActionError(action_error)) => {
            assert_eq!(
                action_error.kind,
                ActionErrorKind::FunctionCallError(errors::FunctionCallError::ExecutionError(
                    "Exceeded the prepaid gas.".to_owned()
                ))
            );
        }
        other => panic!("unexpected outcome: {other:?}"),
    }

    let balance_after = node_user.view_balance(&alice_account()).unwrap();
    let total_cost = balance_before - balance_after;

    // Make sure the total balances check out
    assert_eq!(outcome.tokens_burnt(), total_cost);

    // First outcome is the fn call,
    // everything after should be refunds
    let refunds = outcome.receipts_outcome[1..].to_vec();

    let actual_fn_call_gas_burnt: Gas = outcome.receipts_outcome[0]
        .outcome
        .metadata
        .gas_profile
        .as_deref()
        .unwrap()
        .iter()
        .map(|cost_entry| cost_entry.gas_used)
        .sum();

    let expected_cost = fee_helper.function_call_cost(bytes, actual_fn_call_gas_burnt);

    // Do a general check on the gas penalty.
    // Since gas price didn't change, the only difference must be the gas refund penalty.
    let penalty = total_cost - expected_cost;
    if ProtocolFeature::ReducedGasRefunds.enabled(PROTOCOL_VERSION) {
        let unspent_gas = attached_gas - actual_fn_call_gas_burnt;
        let max_gas_penalty = unspent_gas.max(
            unspent_gas * (*fee_helper.cfg().gas_refund_penalty.numer() as u64)
                / (*fee_helper.cfg().gas_refund_penalty.denom() as u64),
        );
        let min_gas_penalty = unspent_gas.min(fee_helper.cfg().min_gas_refund_penalty);

        assert!(penalty >= fee_helper.gas_to_balance(min_gas_penalty));
        assert!(penalty <= fee_helper.gas_to_balance(max_gas_penalty));
    } else {
        assert_eq!(penalty, 0, "there should be no gas penalty in this version");
    };

    // Let each test check refund receipts separately.
    refunds
}

/// Set up a test environment with a customized contract_loading_base cost.
fn setup_env(contract_load_gas: Gas) -> (RuntimeNode, FeeHelper) {
    init_test_logger();

    let mut genesis = Genesis::test(vec![alice_account(), bob_account()], 2);
    add_test_contract(&mut genesis, &alice_account());
    add_test_contract(&mut genesis, &bob_account());
    let runtime_config = runtime_config_with_contract_load_cost(contract_load_gas);

    let node =
        RuntimeNode::new_from_genesis_and_config(&alice_account(), genesis, runtime_config.clone());
    let fee_helper = FeeHelper::new(runtime_config, node.genesis().config.min_gas_price);
    (node, fee_helper)
}

fn runtime_config_with_contract_load_cost(contract_load_gas: u64) -> RuntimeConfig {
    let runtime_config_store = RuntimeConfigStore::new(None);
    let mut runtime_config =
        RuntimeConfig::clone(runtime_config_store.get_config(PROTOCOL_VERSION));

    let mut wasm_config = near_parameters::vm::Config::clone(&runtime_config.wasm_config);
    wasm_config.ext_costs.costs[ExtCosts::contract_loading_base] =
        ParameterCost { gas: contract_load_gas, compute: contract_load_gas };
    runtime_config.wasm_config = Arc::new(wasm_config);
    runtime_config
}
