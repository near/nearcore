use crate::node::{Node, RuntimeNode};
use near_chain_configs::Genesis;
use near_primitives::runtime::config::RuntimeConfig;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::serialize::to_base64;
use near_primitives::types::AccountId;
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::{CostGasUsed, ExecutionStatusView, FinalExecutionStatus};
use nearcore::config::GenesisExt;
use testlib::runtime_utils::{add_test_contract, alice_account, bob_account};

/// Initial balance used in tests.
const TESTING_INIT_BALANCE: u128 = 1_000_000_000 * NEAR_BASE;

/// One NEAR, divisible by 10^24.
const NEAR_BASE: u128 = 1_000_000_000_000_000_000_000_000;

/// Max prepaid amount of gas.
const MAX_GAS: u64 = 300_000_000_000_000;

fn test_contract_account() -> AccountId {
    "test_contract.alice.near".parse().unwrap()
}

fn setup_runtime_node_with_contract(wasm_binary: &[u8]) -> RuntimeNode {
    // Create a `RuntimeNode`. Load `RuntimeConfig` from `RuntimeConfigStore`
    // to ensure we are using the latest configuration.
    let mut genesis =
        Genesis::test(vec![alice_account(), bob_account(), "carol.near".parse().unwrap()], 3);
    add_test_contract(&mut genesis, &alice_account());
    add_test_contract(&mut genesis, &bob_account());
    let runtime_config_store = RuntimeConfigStore::new(None);
    let runtime_config = runtime_config_store.get_config(PROTOCOL_VERSION);
    let node = RuntimeNode::new_from_genesis_and_config(
        &alice_account(),
        genesis,
        RuntimeConfig::clone(runtime_config),
    );

    let account_id = node.account_id().unwrap();
    let node_user = node.user();
    let tx_result = node_user
        .create_account(
            account_id,
            test_contract_account(),
            node.signer().public_key(),
            TESTING_INIT_BALANCE / 2,
        )
        .unwrap();
    assert_eq!(tx_result.status, FinalExecutionStatus::SuccessValue(to_base64(&[])));
    assert_eq!(tx_result.receipts_outcome.len(), 2);

    let tx_result =
        node_user.deploy_contract(test_contract_account(), wasm_binary.to_vec()).unwrap();
    assert_eq!(tx_result.status, FinalExecutionStatus::SuccessValue(to_base64(&[])));
    assert_eq!(tx_result.receipts_outcome.len(), 1);

    node
}

/// Calls method `sanity_check` on `test-contract-rs` and verifies that the
/// resulting gas profile matches expectations.
///
/// This test intends to catch accidental configuration changes, see #4961.
#[test]
fn test_cost_sanity() {
    let test_contract = if cfg!(feature = "nightly_protocol") {
        near_test_contracts::nightly_rs_contract()
    } else {
        near_test_contracts::rs_contract()
    };
    let node = setup_runtime_node_with_contract(test_contract);
    let data = serde_json::json!({
        "contract_code": to_base64(near_test_contracts::trivial_contract()),
        "method_name": "main",
        "method_args": to_base64(&[]),
        "validator_id": bob_account().as_str(),
    });
    let res = node
        .user()
        .function_call(
            alice_account(),
            test_contract_account(),
            "sanity_check",
            serde_json::to_vec(&data).unwrap(),
            MAX_GAS,
            0,
        )
        .unwrap();
    assert_eq!(res.status, FinalExecutionStatus::SuccessValue(to_base64(&[])));
    assert_eq!(res.transaction_outcome.outcome.metadata.gas_profile, None);

    let receipts_status: Vec<&ExecutionStatusView> =
        res.receipts_outcome.iter().map(|outcome| &outcome.outcome.status).collect();
    insta::assert_yaml_snapshot!("receipts_status", receipts_status);

    let receipts_gas_profile: Vec<&Vec<CostGasUsed>> = res
        .receipts_outcome
        .iter()
        .map(|outcome| outcome.outcome.metadata.gas_profile.as_ref().unwrap())
        .collect();
    insta::assert_debug_snapshot!(
        if cfg!(feature = "nightly_protocol") {
            "receipts_gas_profile_nightly"
        } else {
            "receipts_gas_profile"
        },
        receipts_gas_profile
    );
}
