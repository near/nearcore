use crate::node::{Node, RuntimeNode};
use near_chain_configs::Genesis;
use near_primitives::errors::{ActionError, ActionErrorKind, TxExecutionError};
use near_primitives::runtime::config::RuntimeConfig;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::serialize::to_base64;
use near_primitives::types::AccountId;
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::{
    CostGasUsed, ExecutionOutcomeWithIdView, ExecutionStatusView, FinalExecutionStatus,
};
use near_vm_errors::FunctionCallErrorSer;
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
    assert_receipts_status(
        "sanity_check",
        res.receipts_outcome.clone(),
        expected_cost_sanity_receipts_statuses(),
    );
    assert_gas_profiles(
        "sanity_check",
        res.receipts_outcome.clone(),
        expected_cost_sanity_gas_profiles(),
    );
}

fn assert_receipts_status(
    method_name: &str,
    receipts_outcome: Vec<ExecutionOutcomeWithIdView>,
    expected_statuses: Vec<ExecutionStatusView>,
) {
    let actual_statuses: Vec<ExecutionStatusView> =
        receipts_outcome.iter().map(|outcome| outcome.outcome.status.clone()).collect();
    let mut diff_count = 0;
    for i in 0..std::cmp::max(actual_statuses.len(), expected_statuses.len()) {
        let actual = actual_statuses.get(i);
        let expected = expected_statuses.get(i);
        if actual != expected {
            diff_count += 1;
            println!("{}: receipts[{}] has unexpected status:", method_name, i);
            println!("\t+ {:?}", actual);
            println!("\t- {:?}", expected);
        }
    }
    if diff_count > 0 {
        panic!("encountered {} receipt(s) with unexptected status", diff_count);
    }
}

fn assert_gas_profiles(
    method_name: &str,
    receipts_outcome: Vec<ExecutionOutcomeWithIdView>,
    expected_gas_profiles: Vec<Vec<CostGasUsed>>,
) {
    let actual_gas_profiles = receipts_outcome
        .iter()
        .map(|outcome| outcome.outcome.metadata.gas_profile.as_ref().unwrap());
    assert_eq!(actual_gas_profiles.len(), expected_gas_profiles.len());
    let mut diff_count = 0;
    for (i, (actual_vec, expected_vec)) in
        actual_gas_profiles.zip(expected_gas_profiles).enumerate()
    {
        for j in 0..std::cmp::max(actual_vec.len(), expected_vec.len()) {
            let actual_cost = actual_vec.get(j);
            let expected_cost = expected_vec.get(j);
            if actual_cost != expected_cost {
                diff_count += 1;
                println!("{}: unexpected cost at receipts[{}] gas_profile[{}]:", method_name, i, j);
                println!("\t+ {:?}", actual_cost);
                println!("\t- {:?}", expected_cost);
            }
        }
    }
    if diff_count > 0 {
        panic!("encountered {} unexpected cost(s)", diff_count);
    }
}

fn expected_cost_sanity_receipts_statuses() -> Vec<ExecutionStatusView> {
    vec![
        // `sanity_check` calls `promise_return`
        ExecutionStatusView::SuccessReceiptId(
            "AwDkJs8BbhQMEvsvgPJ4SyvEYbPaiSEQEMf6sUxGTqoL".parse().unwrap(),
        ),
        // `sanity_check_panic`
        ExecutionStatusView::Failure(TxExecutionError::ActionError(ActionError {
            index: Some(0),
            kind: ActionErrorKind::FunctionCallError(FunctionCallErrorSer::ExecutionError(
                "Smart contract panicked: explicit guest panic".to_string(),
            )),
        })),
        ExecutionStatusView::SuccessValue(to_base64(&[])),
        // `sanity_check_panic_utf8`
        ExecutionStatusView::Failure(TxExecutionError::ActionError(ActionError {
            index: Some(0),
            kind: ActionErrorKind::FunctionCallError(FunctionCallErrorSer::ExecutionError(
                "Smart contract panicked: xyz".to_string(),
            )),
        })),
        ExecutionStatusView::SuccessValue(to_base64(&[])),
        ExecutionStatusView::SuccessValue(to_base64(&[])),
        ExecutionStatusView::SuccessValue(to_base64(&[])),
        ExecutionStatusView::SuccessValue(to_base64(&[])),
        ExecutionStatusView::SuccessValue(to_base64(&[])),
        ExecutionStatusView::SuccessValue(to_base64(&[])),
        ExecutionStatusView::SuccessValue(to_base64(&[])),
        ExecutionStatusView::SuccessValue(to_base64(&[])),
        ExecutionStatusView::SuccessValue(to_base64(&[])),
        ExecutionStatusView::SuccessValue(to_base64(&[])),
        ExecutionStatusView::SuccessValue(to_base64(&[])),
        ExecutionStatusView::SuccessValue(to_base64(&[])),
        ExecutionStatusView::SuccessValue(to_base64(&[])),
        ExecutionStatusView::SuccessValue(to_base64(&[])),
        ExecutionStatusView::SuccessValue(to_base64(&[])),
        ExecutionStatusView::SuccessValue(to_base64(&[])),
        ExecutionStatusView::SuccessValue(to_base64(&[])),
        ExecutionStatusView::SuccessValue(to_base64(&[])),
        ExecutionStatusView::SuccessValue(to_base64(&[])),
        ExecutionStatusView::SuccessValue(to_base64(&[])),
        ExecutionStatusView::SuccessValue(to_base64(&[])),
    ]
}

/// Generates a gas profile `Vec<CostGasUsed>`.
macro_rules! generate_gas_profile {
    ( $(($cost_category:literal, $cost:literal, $gas_used:literal)),*) => {
        vec![
            $(
                CostGasUsed{
                    cost_category: $cost_category.to_string(),
                    cost: $cost.to_string(),
                    gas_used: $gas_used,
                },
            )*
        ]
    };
}

/// Returns the expected gas profile of `test-contract-rs` method `noop`.
fn new_noop_gas_profile() -> Vec<CostGasUsed> {
    if cfg!(feature = "nightly_protocol") {
        generate_gas_profile!(
            ("WASM_HOST_COST", "CONTRACT_LOADING_BASE", 35445963),
            ("WASM_HOST_COST", "CONTRACT_LOADING_BYTES", 20693772750)
        )
    } else {
        generate_gas_profile!(
            ("WASM_HOST_COST", "CONTRACT_LOADING_BASE", 35445963),
            ("WASM_HOST_COST", "CONTRACT_LOADING_BYTES", 20506500750)
        )
    }
}

fn expected_cost_sanity_gas_profiles() -> Vec<Vec<CostGasUsed>> {
    if cfg!(feature = "nightly_protocol") {
        vec![
            generate_gas_profile!(
                ("ACTION_COST", "ADD_KEY", 101765125000),
                ("ACTION_COST", "CREATE_ACCOUNT", 199214750000),
                ("ACTION_COST", "DELETE_ACCOUNT", 147489000000),
                ("ACTION_COST", "DELETE_KEY", 94946625000),
                ("ACTION_COST", "DEPLOY_CONTRACT", 184997391966),
                ("ACTION_COST", "FUNCTION_CALL", 20981188693517),
                ("ACTION_COST", "NEW_RECEIPT", 1480548358496),
                ("ACTION_COST", "STAKE", 141715687500),
                ("ACTION_COST", "TRANSFER", 230246125000),
                ("WASM_HOST_COST", "ALT_BN128_G1_MULTIEXP_BASE", 713006929500),
                ("WASM_HOST_COST", "ALT_BN128_G1_MULTIEXP_ELEMENT", 3335092461),
                ("WASM_HOST_COST", "ALT_BN128_G1_SUM_BASE", 3175314375),
                ("WASM_HOST_COST", "ALT_BN128_G1_SUM_ELEMENT", 76218543),
                ("WASM_HOST_COST", "ALT_BN128_PAIRING_CHECK_BASE", 9685508901000),
                ("WASM_HOST_COST", "ALT_BN128_PAIRING_CHECK_ELEMENT", 26575188546),
                ("WASM_HOST_COST", "BASE", 17209927215),
                ("WASM_HOST_COST", "CONTRACT_LOADING_BASE", 35445963),
                ("WASM_HOST_COST", "CONTRACT_LOADING_BYTES", 20693772750),
                ("WASM_HOST_COST", "LOG_BASE", 7086626100),
                ("WASM_HOST_COST", "LOG_BYTE", 131987910),
                ("WASM_HOST_COST", "PROMISE_AND_BASE", 1465013400),
                ("WASM_HOST_COST", "PROMISE_AND_PER_PROMISE", 87234816),
                ("WASM_HOST_COST", "PROMISE_RETURN", 560152386),
                ("WASM_HOST_COST", "READ_CACHED_TRIE_NODE", 13680000000),
                ("WASM_HOST_COST", "READ_MEMORY_BASE", 182690424000),
                ("WASM_HOST_COST", "READ_MEMORY_BYTE", 4744063584),
                ("WASM_HOST_COST", "READ_REGISTER_BASE", 7551495558),
                ("WASM_HOST_COST", "READ_REGISTER_BYTE", 18628218),
                ("WASM_HOST_COST", "RIPEMD160_BASE", 853675086),
                ("WASM_HOST_COST", "RIPEMD160_BLOCK", 680107584),
                ("WASM_HOST_COST", "SHA256_BASE", 4540970250),
                ("WASM_HOST_COST", "SHA256_BYTE", 120586755),
                ("WASM_HOST_COST", "STORAGE_HAS_KEY_BASE", 108079793250),
                ("WASM_HOST_COST", "STORAGE_HAS_KEY_BYTE", 277117605),
                ("WASM_HOST_COST", "STORAGE_READ_BASE", 112713691500),
                ("WASM_HOST_COST", "STORAGE_READ_KEY_BYTE", 278572797),
                ("WASM_HOST_COST", "STORAGE_READ_VALUE_BYTE", 28055025),
                ("WASM_HOST_COST", "STORAGE_REMOVE_BASE", 106946061000),
                ("WASM_HOST_COST", "STORAGE_REMOVE_KEY_BYTE", 343983456),
                ("WASM_HOST_COST", "STORAGE_REMOVE_RET_VALUE_BYTE", 57657780),
                ("WASM_HOST_COST", "STORAGE_WRITE_BASE", 128393472000),
                ("WASM_HOST_COST", "STORAGE_WRITE_EVICTED_BYTE", 160586535),
                ("WASM_HOST_COST", "STORAGE_WRITE_KEY_BYTE", 281931468),
                ("WASM_HOST_COST", "STORAGE_WRITE_VALUE_BYTE", 310185390),
                ("WASM_HOST_COST", "TOUCHING_TRIE_NODE", 32203911852),
                ("WASM_HOST_COST", "UTF16_DECODING_BASE", 3543313050),
                ("WASM_HOST_COST", "UTF16_DECODING_BYTE", 1635774930),
                ("WASM_HOST_COST", "UTF8_DECODING_BASE", 46676685915),
                ("WASM_HOST_COST", "UTF8_DECODING_BYTE", 98262621423),
                ("WASM_HOST_COST", "VALIDATOR_STAKE_BASE", 911834726400),
                ("WASM_HOST_COST", "VALIDATOR_TOTAL_STAKE_BASE", 911834726400),
                ("WASM_HOST_COST", "WASM_INSTRUCTION", 25572901992),
                ("WASM_HOST_COST", "WRITE_MEMORY_BASE", 19626564027),
                ("WASM_HOST_COST", "WRITE_MEMORY_BYTE", 689114316),
                ("WASM_HOST_COST", "WRITE_REGISTER_BASE", 37251792318),
                ("WASM_HOST_COST", "WRITE_REGISTER_BYTE", 1657481904)
            ),
            generate_gas_profile!(
                ("WASM_HOST_COST", "BASE", 264768111),
                ("WASM_HOST_COST", "CONTRACT_LOADING_BASE", 35445963),
                ("WASM_HOST_COST", "CONTRACT_LOADING_BYTES", 20693772750),
                ("WASM_HOST_COST", "WASM_INSTRUCTION", 822756)
            ),
            generate_gas_profile!(),
            generate_gas_profile!(
                ("WASM_HOST_COST", "BASE", 264768111),
                ("WASM_HOST_COST", "CONTRACT_LOADING_BASE", 35445963),
                ("WASM_HOST_COST", "CONTRACT_LOADING_BYTES", 20693772750),
                ("WASM_HOST_COST", "READ_MEMORY_BASE", 2609863200),
                ("WASM_HOST_COST", "READ_MEMORY_BYTE", 11403999),
                ("WASM_HOST_COST", "UTF8_DECODING_BASE", 3111779061),
                ("WASM_HOST_COST", "UTF8_DECODING_BYTE", 874741437),
                ("WASM_HOST_COST", "WASM_INSTRUCTION", 3291024)
            ),
            generate_gas_profile!(),
            new_noop_gas_profile(),
            generate_gas_profile!(),
            new_noop_gas_profile(),
            generate_gas_profile!(),
            new_noop_gas_profile(),
            generate_gas_profile!(),
            generate_gas_profile!(),
            generate_gas_profile!(),
            generate_gas_profile!(),
            generate_gas_profile!(),
            generate_gas_profile!(
                ("WASM_HOST_COST", "CONTRACT_LOADING_BASE", 70891926),
                ("WASM_HOST_COST", "CONTRACT_LOADING_BYTES", 14739000)
            ),
            generate_gas_profile!(),
            generate_gas_profile!(),
            generate_gas_profile!(),
            generate_gas_profile!(),
            generate_gas_profile!(
                ("WASM_HOST_COST", "CONTRACT_LOADING_BASE", 35445963),
                ("WASM_HOST_COST", "CONTRACT_LOADING_BYTES", 20693772750)
            ),
            generate_gas_profile!(),
            generate_gas_profile!(
                ("WASM_HOST_COST", "BASE", 529536222),
                ("WASM_HOST_COST", "CONTRACT_LOADING_BASE", 35445963),
                ("WASM_HOST_COST", "CONTRACT_LOADING_BYTES", 20693772750),
                ("WASM_HOST_COST", "WASM_INSTRUCTION", 18923388),
                ("WASM_HOST_COST", "WRITE_REGISTER_BASE", 2865522486)
            ),
            generate_gas_profile!(),
            generate_gas_profile!(),
        ]
    } else {
        vec![
            generate_gas_profile!(
                ("ACTION_COST", "ADD_KEY", 101765125000),
                ("ACTION_COST", "CREATE_ACCOUNT", 199214750000),
                ("ACTION_COST", "DELETE_ACCOUNT", 147489000000),
                ("ACTION_COST", "DELETE_KEY", 94946625000),
                ("ACTION_COST", "DEPLOY_CONTRACT", 184997391966),
                ("ACTION_COST", "FUNCTION_CALL", 20981188693517),
                ("ACTION_COST", "NEW_RECEIPT", 1480548358496),
                ("ACTION_COST", "STAKE", 141715687500),
                ("ACTION_COST", "TRANSFER", 230246125000),
                ("WASM_HOST_COST", "BASE", 17209927215),
                ("WASM_HOST_COST", "CONTRACT_LOADING_BASE", 35445963),
                ("WASM_HOST_COST", "CONTRACT_LOADING_BYTES", 20506500750),
                ("WASM_HOST_COST", "LOG_BASE", 7086626100),
                ("WASM_HOST_COST", "LOG_BYTE", 131987910),
                ("WASM_HOST_COST", "PROMISE_AND_BASE", 1465013400),
                ("WASM_HOST_COST", "PROMISE_AND_PER_PROMISE", 87234816),
                ("WASM_HOST_COST", "PROMISE_RETURN", 560152386),
                ("WASM_HOST_COST", "READ_CACHED_TRIE_NODE", 13680000000),
                ("WASM_HOST_COST", "READ_MEMORY_BASE", 174860834400),
                ("WASM_HOST_COST", "READ_MEMORY_BYTE", 3402193035),
                ("WASM_HOST_COST", "READ_REGISTER_BASE", 7551495558),
                ("WASM_HOST_COST", "READ_REGISTER_BYTE", 18628218),
                ("WASM_HOST_COST", "RIPEMD160_BASE", 853675086),
                ("WASM_HOST_COST", "RIPEMD160_BLOCK", 680107584),
                ("WASM_HOST_COST", "SHA256_BASE", 4540970250),
                ("WASM_HOST_COST", "SHA256_BYTE", 120586755),
                ("WASM_HOST_COST", "STORAGE_HAS_KEY_BASE", 108079793250),
                ("WASM_HOST_COST", "STORAGE_HAS_KEY_BYTE", 277117605),
                ("WASM_HOST_COST", "STORAGE_READ_BASE", 112713691500),
                ("WASM_HOST_COST", "STORAGE_READ_KEY_BYTE", 278572797),
                ("WASM_HOST_COST", "STORAGE_READ_VALUE_BYTE", 28055025),
                ("WASM_HOST_COST", "STORAGE_REMOVE_BASE", 106946061000),
                ("WASM_HOST_COST", "STORAGE_REMOVE_KEY_BYTE", 343983456),
                ("WASM_HOST_COST", "STORAGE_REMOVE_RET_VALUE_BYTE", 57657780),
                ("WASM_HOST_COST", "STORAGE_WRITE_BASE", 128393472000),
                ("WASM_HOST_COST", "STORAGE_WRITE_EVICTED_BYTE", 160586535),
                ("WASM_HOST_COST", "STORAGE_WRITE_KEY_BYTE", 281931468),
                ("WASM_HOST_COST", "STORAGE_WRITE_VALUE_BYTE", 310185390),
                ("WASM_HOST_COST", "TOUCHING_TRIE_NODE", 32203911852),
                ("WASM_HOST_COST", "UTF16_DECODING_BASE", 3543313050),
                ("WASM_HOST_COST", "UTF16_DECODING_BYTE", 1635774930),
                ("WASM_HOST_COST", "UTF8_DECODING_BASE", 46676685915),
                ("WASM_HOST_COST", "UTF8_DECODING_BYTE", 98262621423),
                ("WASM_HOST_COST", "VALIDATOR_STAKE_BASE", 911834726400),
                ("WASM_HOST_COST", "VALIDATOR_TOTAL_STAKE_BASE", 911834726400),
                ("WASM_HOST_COST", "WASM_INSTRUCTION", 25446197568),
                ("WASM_HOST_COST", "WRITE_MEMORY_BASE", 19626564027),
                ("WASM_HOST_COST", "WRITE_MEMORY_BYTE", 689114316),
                ("WASM_HOST_COST", "WRITE_REGISTER_BASE", 31520747346),
                ("WASM_HOST_COST", "WRITE_REGISTER_BYTE", 1170881712)
            ),
            generate_gas_profile!(
                ("WASM_HOST_COST", "BASE", 264768111),
                ("WASM_HOST_COST", "CONTRACT_LOADING_BASE", 35445963),
                ("WASM_HOST_COST", "CONTRACT_LOADING_BYTES", 20506500750),
                ("WASM_HOST_COST", "WASM_INSTRUCTION", 822756)
            ),
            generate_gas_profile!(),
            generate_gas_profile!(
                ("WASM_HOST_COST", "BASE", 264768111),
                ("WASM_HOST_COST", "CONTRACT_LOADING_BASE", 35445963),
                ("WASM_HOST_COST", "CONTRACT_LOADING_BYTES", 20506500750),
                ("WASM_HOST_COST", "READ_MEMORY_BASE", 2609863200),
                ("WASM_HOST_COST", "READ_MEMORY_BYTE", 11403999),
                ("WASM_HOST_COST", "UTF8_DECODING_BASE", 3111779061),
                ("WASM_HOST_COST", "UTF8_DECODING_BYTE", 874741437),
                ("WASM_HOST_COST", "WASM_INSTRUCTION", 3291024)
            ),
            generate_gas_profile!(),
            new_noop_gas_profile(),
            generate_gas_profile!(),
            new_noop_gas_profile(),
            generate_gas_profile!(),
            new_noop_gas_profile(),
            generate_gas_profile!(),
            generate_gas_profile!(),
            generate_gas_profile!(),
            generate_gas_profile!(),
            generate_gas_profile!(),
            generate_gas_profile!(
                ("WASM_HOST_COST", "CONTRACT_LOADING_BASE", 70891926),
                ("WASM_HOST_COST", "CONTRACT_LOADING_BYTES", 14739000)
            ),
            generate_gas_profile!(),
            generate_gas_profile!(),
            generate_gas_profile!(),
            generate_gas_profile!(),
            generate_gas_profile!(
                ("WASM_HOST_COST", "CONTRACT_LOADING_BASE", 35445963),
                ("WASM_HOST_COST", "CONTRACT_LOADING_BYTES", 20506500750)
            ),
            generate_gas_profile!(),
            generate_gas_profile!(
                ("WASM_HOST_COST", "BASE", 529536222),
                ("WASM_HOST_COST", "CONTRACT_LOADING_BASE", 35445963),
                ("WASM_HOST_COST", "CONTRACT_LOADING_BYTES", 20506500750),
                ("WASM_HOST_COST", "WASM_INSTRUCTION", 18923388),
                ("WASM_HOST_COST", "WRITE_REGISTER_BASE", 2865522486)
            ),
            generate_gas_profile!(),
            generate_gas_profile!(),
        ]
    }
}
