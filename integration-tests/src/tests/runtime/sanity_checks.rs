use crate::node::{Node, RuntimeNode};
use near_chain_configs::Genesis;
use near_primitives::config::ExtCosts;
use near_primitives::runtime::config::RuntimeConfig;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::serialize::to_base64;
use near_primitives::types::AccountId;
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::{
    CostGasUsed, ExecutionOutcomeWithIdView, ExecutionStatusView, FinalExecutionStatus,
};
use nearcore::config::GenesisExt;
use std::collections::HashSet;
use std::mem::size_of;
use testlib::runtime_utils::{add_test_contract, alice_account, bob_account};

/// Initial balance used in tests.
const TESTING_INIT_BALANCE: u128 = 1_000_000_000 * NEAR_BASE;

/// One NEAR, divisible by 10^24.
const NEAR_BASE: u128 = 1_000_000_000_000_000_000_000_000;

/// Max prepaid amount of gas.
const MAX_GAS: u64 = 300_000_000_000_000;

/// Costs whose amount of `gas_used` may depend on environmental factors.
///
/// In particular, compiling our test contract with different versions of
/// compiler can lead to slightly different WASM output.
const NONDETERMINISTIC_COSTS: [&str; 2] = ["CONTRACT_LOADING_BYTES", "WASM_INSTRUCTION"];

fn is_nondeterministic_cost(cost: &str) -> bool {
    NONDETERMINISTIC_COSTS.iter().find(|&&ndt_cost| ndt_cost == cost).is_some()
}

fn test_contract_account() -> AccountId {
    format!("test-contract.{}", alice_account().as_str()).parse().unwrap()
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
    assert_eq!(tx_result.status, FinalExecutionStatus::SuccessValue(Vec::new()));
    assert_eq!(tx_result.receipts_outcome.len(), 2);

    let tx_result =
        node_user.deploy_contract(test_contract_account(), wasm_binary.to_vec()).unwrap();
    assert_eq!(tx_result.status, FinalExecutionStatus::SuccessValue(Vec::new()));
    assert_eq!(tx_result.receipts_outcome.len(), 1);

    node
}

fn get_receipts_status_with_clear_hash(
    outcomes: &[ExecutionOutcomeWithIdView],
) -> Vec<ExecutionStatusView> {
    outcomes
        .iter()
        .map(|outcome| {
            match outcome.outcome.status {
                ExecutionStatusView::SuccessReceiptId(_) => {
                    // We don’t control the hash of the receipt so clear it.
                    ExecutionStatusView::SuccessReceiptId(Default::default())
                }
                ref status => status.clone(),
            }
        })
        .collect::<Vec<_>>()
}

/// Calls method `sanity_check` on `test-contract-rs` and verifies that the
/// resulting gas profile matches expectations.
///
/// This test intends to catch accidental configuration changes, see #4961.
#[test]
fn test_cost_sanity() {
    let test_contract = if cfg!(feature = "nightly") {
        near_test_contracts::nightly_rs_contract()
    } else {
        near_test_contracts::rs_contract()
    };
    let node = setup_runtime_node_with_contract(test_contract);

    let args = format!(
        r#"{{
            "contract_code": {:?},
            "method_name": "main",
            "method_args": "",
            "validator_id": {:?}
        }}"#,
        to_base64(near_test_contracts::trivial_contract()),
        bob_account().as_str()
    );
    eprintln!("{args}");

    let res = node
        .user()
        .function_call(
            alice_account(),
            test_contract_account(),
            "sanity_check",
            args.into_bytes(),
            MAX_GAS,
            0,
        )
        .unwrap();
    assert_eq!(res.status, FinalExecutionStatus::SuccessValue(Vec::new()));
    assert_eq!(res.transaction_outcome.outcome.metadata.gas_profile, None);

    let receipts_status = get_receipts_status_with_clear_hash(&res.receipts_outcome);
    insta::assert_yaml_snapshot!("receipts_status", receipts_status);

    let receipts_gas_profile = res
        .receipts_outcome
        .iter()
        .map(|outcome| outcome.outcome.metadata.gas_profile.as_ref().unwrap())
        .map(|gas_profile| {
            gas_profile
                .iter()
                .cloned()
                .map(|cost| {
                    if is_nondeterministic_cost(&cost.cost) {
                        // Ignore `gas_used` of nondeterministic costs.
                        CostGasUsed { gas_used: 0, ..cost }
                    } else {
                        cost
                    }
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    insta::assert_debug_snapshot!(
        if cfg!(feature = "nightly") {
            "receipts_gas_profile_nightly"
        } else {
            "receipts_gas_profile"
        },
        receipts_gas_profile
    );
}

/// Verifies the sanity of nondeterministic costs using a trivial contract.
///
/// Some costs are nondeterministic since they depend on environmental factors,
/// see [`NONDETERMINISTIC_COSTS`]. For a trivial contract, however, there are
/// no such differences expected.
#[test]
fn test_cost_sanity_nondeterministic() {
    let contract = near_test_contracts::wat_contract(
        r#"(module (func (export "main") (i32.const 92) (drop)))"#,
    );
    let node = setup_runtime_node_with_contract(&contract);
    let res = node
        .user()
        .function_call(alice_account(), test_contract_account(), "main", vec![], MAX_GAS, 0)
        .unwrap();
    assert_eq!(res.status, FinalExecutionStatus::SuccessValue(Vec::new()));
    assert_eq!(res.transaction_outcome.outcome.metadata.gas_profile, None);

    let receipts_status = get_receipts_status_with_clear_hash(&res.receipts_outcome);
    insta::assert_yaml_snapshot!("receipts_status_nondeterministic", receipts_status);

    let receipts_gas_profile = res
        .receipts_outcome
        .iter()
        .map(|outcome| outcome.outcome.metadata.gas_profile.as_ref().unwrap())
        .collect::<Vec<_>>();

    insta::assert_debug_snapshot!(
        if cfg!(feature = "nightly") {
            "receipts_gas_profile_nondeterministic_nightly"
        } else {
            "receipts_gas_profile_nondeterministic"
        },
        receipts_gas_profile
    );

    // Verify that all nondeterministic costs are covered.
    let all_costs = {
        let all_costs = receipts_gas_profile
            .iter()
            .flat_map(|gas_profile| gas_profile.iter().map(|cost| cost.cost.as_str()));
        HashSet::from_iter(all_costs)
    };
    let ndt_costs = HashSet::from(NONDETERMINISTIC_COSTS);
    let missing_costs = ndt_costs.difference(&all_costs).collect::<Vec<_>>();
    assert!(
        missing_costs.is_empty(),
        "some nondeterministic costs are not covered: {:?}",
        missing_costs,
    );
}

/// Verifies the operation of host function `used_gas` according to
/// [gas instrumentation].
///
/// [gas instrumentation]: https://nomicon.io/RuntimeSpec/Preparation#gas-instrumentation
#[test]
fn test_sanity_used_gas() {
    let node = setup_runtime_node_with_contract(&contract_sanity_check_used_gas());
    let res = node
        .user()
        .function_call(alice_account(), test_contract_account(), "main", vec![], MAX_GAS, 0)
        .unwrap();

    let num_return_values = 4;
    let returned_bytes = match res.status {
        FinalExecutionStatus::SuccessValue(v) => v,
        _ => panic!("Unexpected status: {:?}", res.status),
    };
    assert_eq!(returned_bytes.len(), num_return_values * size_of::<u64>());

    let used_gas = stdx::as_chunks_exact::<{ size_of::<u64>() }, _>(&returned_bytes)
        .unwrap()
        .iter()
        .map(|bytes| u64::from_le_bytes(*bytes))
        .collect::<Vec<_>>();

    let runtime_config = node.client.read().unwrap().runtime_config.clone();
    let base_cost = runtime_config.wasm_config.ext_costs.gas_cost(ExtCosts::base);
    let op_cost = match runtime_config.wasm_config.limit_config.contract_prepare_version {
        // In old implementations of preparation, all of the contained instructions are paid
        // for upfront when entering a new metered block,
        //
        // It fails to be precise in that it does not consider calls to `used_gas` as
        // breaking up a metered block and so none of the gas cost to execute instructions between
        // calls to this function will be observable from within wasm code.
        //
        // In this test we account for this by setting `op_cost` to zero, but if future tests
        // change test WASM in significant ways, this approach may become incorrect.
        near_primitives::config::ContractPrepareVersion::V0
        | near_primitives::config::ContractPrepareVersion::V1 => 0,
        // Gas accounting is precise and instructions executed between calls to the side-effectful
        // `used_gas` host function calls will be observbable.
        near_primitives::config::ContractPrepareVersion::V2 => {
            u64::from(runtime_config.wasm_config.regular_op_cost)
        }
    };

    // Executing `used_gas` costs `base_cost` plus an instruction to execute the `call` itself.
    // When executing `used_gas` twice within a metered block, the returned values should differ by
    // that amount.
    assert_eq!(used_gas[1] - used_gas[0], base_cost + op_cost);
    // Between these two observations additional arithmetics have been executed.
    assert_eq!(used_gas[2] - used_gas[1], base_cost + 8 * op_cost);
    assert!(used_gas[3] - used_gas[2] > base_cost);
}

/// Returns a contract which calls host function `used_gas` multiple times, both
/// within the same [metered block] and in different metered blocks. The value
/// returned by the contract is an array of values returned by calls of
/// `used_gas`.
///
/// This contract is written in `wat` to avoid depending on the output generated
/// by a compiler (e.g. `rustc`).
///
/// [metered block]: https://nomicon.io/RuntimeSpec/Preparation#gas-instrumentation
fn contract_sanity_check_used_gas() -> Vec<u8> {
    wat::parse_str(
        r#"
          (module
            (type $t0 (func (result i64)))
            (type $t1 (func (param i64 i64)))
            (type $t2 (func))

            (import "env" "used_gas" (func $env.used_gas (type $t0)))
            (import "env" "value_return" (func $env.value_return (type $t1)))

            (memory 1)

            (func $main (export "main") (type $t2)
              (local $used_0 i64) (local $used_1 i64) (local $used_2 i64) (local $used_3 i64)

              ;; Call used_gas twice in metered block, without instructions in between.
              (call $env.used_gas)
              (call $env.used_gas)
              (local.set $used_1)
              (local.set $used_0)

              ;; In the same metered block, call used_gas again after executing other
              ;; instructions.
              (i64.add
                (local.get $used_0)
                (i64.const 1))
              drop
              nop

              (call $env.used_gas)
              (local.set $used_2)

              ;; Push a new metered block on the stack via br_if. The condition is false,
              ;; so we will _not_ branch out of the block.
              (block $b0
                (br_if $b0
                  (i64.eq
                    (local.get $used_0)
                    (i64.const 0)))
                  (local.set $used_3
                    call $env.used_gas)
                  nop ;; ensure there is more than used_gas to pay for in this block
              )

              ;; Prepare bytes passed to value_return.
              (i64.store
                (i32.const 0)
                (local.get $used_0))
              (i64.store
                (i32.const 8)
                (local.get $used_1))
              (i64.store
                (i32.const 16)
                (local.get $used_2))
              (i64.store
                (i32.const 24)
                (local.get $used_3))

              (call $env.value_return
                (i64.const 32)
                (i64.extend_i32_u
                  (i32.const 0)))
            )
          )
 "#,
    )
    .unwrap()
}
