use near_primitives::contract::ContractCode;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::test_utils::encode;
use near_primitives::transaction::{Action, FunctionCallAction};
use near_primitives::types::Balance;
use near_primitives::version::ProtocolFeature;
use near_vm_errors::{FunctionCallError, HostError, VMError, WasmTrap};
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::types::ReturnData;
use near_vm_logic::{ReceiptMetadata, VMConfig};
use std::mem::size_of;

use crate::tests::{
    create_context, with_vm_variants, CURRENT_ACCOUNT_ID, LATEST_PROTOCOL_VERSION,
    PREDECESSOR_ACCOUNT_ID, SIGNER_ACCOUNT_ID, SIGNER_ACCOUNT_PK,
};
use crate::vm_kind::VMKind;
use crate::VMResult;

fn test_contract() -> ContractCode {
    let code = if cfg!(feature = "protocol_feature_alt_bn128") {
        near_test_contracts::nightly_rs_contract()
    } else {
        near_test_contracts::rs_contract()
    };
    ContractCode::new(code.to_vec(), None)
}

fn assert_run_result(result: VMResult, expected_value: u64) {
    if let Some(_) = result.error() {
        panic!("Failed execution");
    }

    if let ReturnData::Value(value) = &result.outcome().return_data {
        let mut arr = [0u8; size_of::<u64>()];
        arr.copy_from_slice(&value);
        let res = u64::from_le_bytes(arr);
        assert_eq!(res, expected_value);
    } else {
        panic!("Value was not returned");
    }
}

#[test]
pub fn test_read_write() {
    with_vm_variants(|vm_kind: VMKind| {
        let code = test_contract();
        let mut fake_external = MockedExternal::new();

        let context = create_context(encode(&[10u64, 20u64]));
        let config = VMConfig::test();
        let fees = RuntimeFeesConfig::test();

        let promise_results = vec![];
        let gas_counter = context.new_gas_counter(&config);
        let runtime = vm_kind.runtime(config.clone()).expect("runtime has not been compiled");
        let result = runtime.run(
            &code,
            "write_key_value",
            &mut fake_external,
            context,
            &fees,
            gas_counter,
            &promise_results,
            LATEST_PROTOCOL_VERSION,
            None,
        );
        assert_run_result(result, 0);

        let context = create_context(encode(&[10u64]));
        let gas_counter = context.new_gas_counter(&config);
        let result = runtime.run(
            &code,
            "read_value",
            &mut fake_external,
            context,
            &fees,
            gas_counter,
            &promise_results,
            LATEST_PROTOCOL_VERSION,
            None,
        );
        assert_run_result(result, 20);
    });
}

#[test]
pub fn test_stablized_host_function() {
    with_vm_variants(|vm_kind: VMKind| {
        let code = test_contract();
        let mut fake_external = MockedExternal::new();

        let context = create_context(vec![]);
        let config = VMConfig::test();
        let fees = RuntimeFeesConfig::test();

        let promise_results = vec![];
        let runtime = vm_kind.runtime(config.clone()).expect("runtime has not been compiled");
        let result = runtime.run(
            &code,
            "do_ripemd",
            &mut fake_external,
            context.clone(),
            &fees,
            context.new_gas_counter(&config),
            &promise_results,
            LATEST_PROTOCOL_VERSION,
            None,
        );
        assert_eq!(result.error(), None);

        let gas_counter = context.new_gas_counter(&config);
        let result = runtime.run(
            &code,
            "do_ripemd",
            &mut fake_external,
            context,
            &fees,
            gas_counter,
            &promise_results,
            ProtocolFeature::MathExtension.protocol_version() - 1,
            None,
        );
        match result.error() {
            Some(&VMError::FunctionCallError(FunctionCallError::LinkError { msg: _ })) => {}
            _ => panic!("should return a link error due to missing import"),
        }
    });
}

macro_rules! def_test_ext {
    ($name:ident, $method:expr, $expected:expr, $input:expr, $validator:expr) => {
        #[test]
        pub fn $name() {
            with_vm_variants(|vm_kind: VMKind| {
                run_test_ext($method, $expected, $input, $validator, vm_kind)
            });
        }
    };
    ($name:ident, $method:expr, $expected:expr, $input:expr) => {
        #[test]
        pub fn $name() {
            with_vm_variants(|vm_kind: VMKind| {
                run_test_ext($method, $expected, $input, vec![], vm_kind)
            });
        }
    };
    ($name:ident, $method:expr, $expected:expr) => {
        #[test]
        pub fn $name() {
            with_vm_variants(|vm_kind: VMKind| {
                run_test_ext($method, $expected, &[], vec![], vm_kind)
            })
        }
    };
}

fn run_test_ext(
    method: &str,
    expected: &[u8],
    input: &[u8],
    validators: Vec<(&str, Balance)>,
    vm_kind: VMKind,
) {
    let code = test_contract();
    let mut fake_external = MockedExternal::new();
    fake_external.validators =
        validators.into_iter().map(|(s, b)| (s.parse().unwrap(), b)).collect();
    let config = VMConfig::test();
    let fees = RuntimeFeesConfig::test();
    let context = create_context(input.to_vec());
    let gas_counter = context.new_gas_counter(&config);
    let runtime = vm_kind.runtime(config).expect("runtime has not been compiled");

    let (outcome, err) = runtime
        .run(
            &code,
            method,
            &mut fake_external,
            context,
            &fees,
            gas_counter,
            &[],
            LATEST_PROTOCOL_VERSION,
            None,
        )
        .outcome_error();

    assert_eq!(outcome.profile.action_gas(), 0);

    if let Some(_) = err {
        panic!("Failed execution: {:?}", err);
    }

    if let ReturnData::Value(value) = outcome.return_data {
        assert_eq!(&value, &expected);
    } else {
        panic!("Value was not returned");
    }
}

def_test_ext!(ext_account_id, "ext_account_id", CURRENT_ACCOUNT_ID.as_bytes());

def_test_ext!(ext_signer_id, "ext_signer_id", SIGNER_ACCOUNT_ID.as_bytes());
def_test_ext!(
    ext_predecessor_account_id,
    "ext_predecessor_account_id",
    PREDECESSOR_ACCOUNT_ID.as_bytes(),
    &[]
);
def_test_ext!(ext_signer_pk, "ext_signer_pk", &SIGNER_ACCOUNT_PK);
def_test_ext!(ext_random_seed, "ext_random_seed", &[0, 1, 2]);

def_test_ext!(ext_prepaid_gas, "ext_prepaid_gas", &(10_u64.pow(14)).to_le_bytes());
def_test_ext!(ext_block_index, "ext_block_index", &10u64.to_le_bytes());
def_test_ext!(ext_block_timestamp, "ext_block_timestamp", &42u64.to_le_bytes());
def_test_ext!(ext_storage_usage, "ext_storage_usage", &12u64.to_le_bytes());
// Note, the used_gas is not a global used_gas at the beginning of method, but instead a diff
// in used_gas for computing fib(30) in a loop
def_test_ext!(ext_used_gas, "ext_used_gas", &[111, 10, 200, 15, 0, 0, 0, 0]);
def_test_ext!(
    ext_sha256,
    "ext_sha256",
    &[
        18, 176, 115, 156, 45, 100, 241, 132, 180, 134, 77, 42, 105, 111, 199, 127, 118, 112, 92,
        255, 88, 43, 83, 147, 122, 55, 26, 36, 42, 156, 160, 158,
    ],
    b"tesdsst"
);
// current_account_balance = context.account_balance + context.attached_deposit;
def_test_ext!(ext_account_balance, "ext_account_balance", &(2u128 + 2).to_le_bytes());
def_test_ext!(ext_attached_deposit, "ext_attached_deposit", &2u128.to_le_bytes());

def_test_ext!(
    ext_validator_stake_alice,
    "ext_validator_stake",
    &(100u128).to_le_bytes(),
    b"alice",
    vec![("alice", 100), ("bob", 1)]
);
def_test_ext!(
    ext_validator_stake_bob,
    "ext_validator_stake",
    &(1u128).to_le_bytes(),
    b"bob",
    vec![("alice", 100), ("bob", 1)]
);
def_test_ext!(
    ext_validator_stake_carol,
    "ext_validator_stake",
    &(0u128).to_le_bytes(),
    b"carol",
    vec![("alice", 100), ("bob", 1)]
);

def_test_ext!(
    ext_validator_total_stake,
    "ext_validator_total_stake",
    &(100u128 + 1).to_le_bytes(),
    &[],
    vec![("alice", 100), ("bob", 1)]
);

#[test]
pub fn test_out_of_memory() {
    with_vm_variants(|vm_kind: VMKind| {
        // TODO: currently we only run this test on Wasmer.
        match vm_kind {
            VMKind::Wasmtime => return,
            _ => {}
        }

        let code = test_contract();
        let mut fake_external = MockedExternal::new();

        let context = create_context(Vec::new());
        let config = VMConfig::free();
        let fees = RuntimeFeesConfig::free();
        let gas_counter = context.new_gas_counter(&config);
        let runtime = vm_kind.runtime(config).expect("runtime has not been compiled");

        let promise_results = vec![];
        let result = runtime.run(
            &code,
            "out_of_memory",
            &mut fake_external,
            context,
            &fees,
            gas_counter,
            &promise_results,
            LATEST_PROTOCOL_VERSION,
            None,
        );
        assert_eq!(
            result.error(),
            match vm_kind {
                VMKind::Wasmer0 | VMKind::Wasmer2 => Some(&VMError::FunctionCallError(
                    FunctionCallError::WasmTrap(WasmTrap::Unreachable)
                )),
                VMKind::Wasmtime => unreachable!(),
            }
        );
    })
}

fn function_call_weight_contract() -> ContractCode {
    ContractCode::new(near_test_contracts::rs_contract().to_vec(), None)
}

#[test]
fn attach_unspent_gas_but_burn_all_gas() {
    with_vm_variants(|vm_kind: VMKind| {
        let code = function_call_weight_contract();
        let mut external = MockedExternal::new();
        let mut config = VMConfig::test();
        let fees = RuntimeFeesConfig::test();
        let mut context = create_context(vec![]);

        let prepaid_gas = 100 * 10u64.pow(12);
        context.prepaid_gas = prepaid_gas;
        config.limit_config.max_gas_burnt = context.prepaid_gas / 3;
        let gas_counter = context.new_gas_counter(&config);
        let runtime = vm_kind.runtime(config).expect("runtime has not been compiled");

        let (outcome, err) = runtime
            .run(
                &code,
                "attach_unspent_gas_but_burn_all_gas",
                &mut external,
                context,
                &fees,
                gas_counter,
                &[],
                LATEST_PROTOCOL_VERSION,
                None,
            )
            .outcome_error();

        let err = err.unwrap();
        assert!(matches!(
            err,
            VMError::FunctionCallError(FunctionCallError::HostError(HostError::GasLimitExceeded))
        ));
        match &outcome.action_receipts.as_slice() {
            [(_, ReceiptMetadata { actions, .. })] => match actions.as_slice() {
                [Action::FunctionCall(FunctionCallAction { gas, .. })] => {
                    assert!(*gas > prepaid_gas / 3);
                }
                other => panic!("unexpected actions: {other:?}"),
            },
            other => panic!("unexpected receipts: {other:?}"),
        }
    });
}

#[test]
fn attach_unspent_gas_but_use_all_gas() {
    with_vm_variants(|vm_kind: VMKind| {
        let code = function_call_weight_contract();
        let mut external = MockedExternal::new();
        let mut config = VMConfig::test();
        let fees = RuntimeFeesConfig::test();
        let mut context = create_context(vec![]);

        context.prepaid_gas = 100 * 10u64.pow(12);
        config.limit_config.max_gas_burnt = context.prepaid_gas / 3;
        let gas_counter = context.new_gas_counter(&config);
        let runtime = vm_kind.runtime(config).expect("runtime has not been compiled");

        let (outcome, err) = runtime
            .run(
                &code,
                "attach_unspent_gas_but_use_all_gas",
                &mut external,
                context,
                &fees,
                gas_counter,
                &[],
                LATEST_PROTOCOL_VERSION,
                None,
            )
            .outcome_error();

        let err = err.unwrap();
        assert!(matches!(
            err,
            VMError::FunctionCallError(FunctionCallError::HostError(HostError::GasExceeded))
        ));

        match &outcome.action_receipts.as_slice() {
            [(_, ReceiptMetadata { actions, .. }), _] => match actions.as_slice() {
                [Action::FunctionCall(FunctionCallAction { gas, .. })] => {
                    assert_eq!(*gas, 0);
                }
                other => panic!("unexpected actions: {other:?}"),
            },
            other => panic!("unexpected receipts: {other:?}"),
        }
    });
}
