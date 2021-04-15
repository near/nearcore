use near_primitives::contract::ContractCode;
use near_primitives::profile::ProfileData;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::types::Balance;
use near_vm_errors::{FunctionCallError, VMError, WasmTrap};
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::{types::ReturnData, VMConfig, VMOutcome};
use std::mem::size_of;

use crate::tests::{
    create_context, with_vm_variants, CURRENT_ACCOUNT_ID, LATEST_PROTOCOL_VERSION,
    PREDECESSOR_ACCOUNT_ID, SIGNER_ACCOUNT_ID, SIGNER_ACCOUNT_PK,
};
use crate::{run_vm, VMKind};

fn test_contract() -> ContractCode {
    let code = if cfg!(feature = "protocol_feature_alt_bn128") {
        near_test_contracts::nightly_rs_contract()
    } else {
        near_test_contracts::rs_contract()
    };
    ContractCode::new(code.to_vec(), None)
}

fn assert_run_result((outcome, err): (Option<VMOutcome>, Option<VMError>), expected_value: u64) {
    if let Some(_) = err {
        panic!("Failed execution");
    }

    if let Some(VMOutcome { return_data, .. }) = outcome {
        if let ReturnData::Value(value) = return_data {
            let mut arr = [0u8; size_of::<u64>()];
            arr.copy_from_slice(&value);
            let res = u64::from_le_bytes(arr);
            assert_eq!(res, expected_value);
        } else {
            panic!("Value was not returned");
        }
    } else {
        panic!("Failed execution");
    }
}

fn arr_u64_to_u8(value: &[u64]) -> Vec<u8> {
    let mut res = vec![];
    for el in value {
        res.extend_from_slice(&el.to_le_bytes());
    }
    res
}

#[test]
pub fn test_read_write() {
    with_vm_variants(|vm_kind: VMKind| {
        let code = test_contract();
        let mut fake_external = MockedExternal::new();

        let context = create_context(arr_u64_to_u8(&[10u64, 20u64]));
        let config = VMConfig::default();
        let fees = RuntimeFeesConfig::default();

        let promise_results = vec![];
        let result = run_vm(
            &code,
            "write_key_value",
            &mut fake_external,
            context,
            &config,
            &fees,
            &promise_results,
            vm_kind.clone(),
            LATEST_PROTOCOL_VERSION,
            None,
            ProfileData::new_disabled(),
        );
        assert_run_result(result, 0);

        let context = create_context(arr_u64_to_u8(&[10u64]));
        let result = run_vm(
            &code,
            "read_value",
            &mut fake_external,
            context,
            &config,
            &fees,
            &promise_results,
            vm_kind,
            LATEST_PROTOCOL_VERSION,
            None,
            ProfileData::new_disabled(),
        );
        assert_run_result(result, 20);
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
    fake_external.validators = validators.into_iter().map(|(s, b)| (s.to_string(), b)).collect();
    let config = VMConfig::default();
    let fees = RuntimeFeesConfig::default();
    let context = create_context(input.to_vec());

    let profile = ProfileData::new_enabled();
    let (outcome, err) = run_vm(
        &code,
        &method,
        &mut fake_external,
        context,
        &config,
        &fees,
        &[],
        vm_kind,
        LATEST_PROTOCOL_VERSION,
        None,
        profile.clone(),
    );

    assert_eq!(profile.action_gas(), 0);

    if let Some(_) = err {
        panic!("Failed execution: {:?}", err);
    }

    if let Some(VMOutcome { return_data, .. }) = outcome {
        if let ReturnData::Value(value) = return_data {
            assert_eq!(&value, &expected);
        } else {
            panic!("Value was not returned");
        }
    } else {
        panic!("Failed execution");
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

#[cfg(feature = "protocol_feature_alt_bn128")]
def_test_ext!(
    ext_alt_bn128_pairing_check,
    "ext_alt_bn128_pairing_check",
    &[1],
    &base64::decode("AgAAAHUK2WNxTupDt1oaOshWw3squNVY4PgSyGwGtQYcEWMHJIY1c8C0A3FM466TMq5PSpfDrArT0hpcdfZB7ahoEAQBGgPbBg3Bc03mGw3y1sMJ1WOHDKDKcoevKnSsT+oaKdRvwIF8cDlrJvTm3vAkQe6FvBMrlDvNKKGzreRYqecdEUOjM6W7ZSz6GERlXIDLvjNVCSs6iES0XG65qGuBLR67FmQRS13YfRfUC7rHzAGMhQtSLEHeFBowGoTcGdVdGU+wBJWX8wuD/el5Jt4PdnXI1q/pgrXBp/+ZqfDP6xwfU0pFswaWSENKpoJTUnN7b9DdQCvt1brrBzj7s1/pnxdtrVVnCKXr4tpPSHis+xRTecmMYqr2edoTcyqHPO8eIDGqq8zExaCeqC8Xbot73t71Yn3QRiduupL+Qrl2A04gL7PFXU/wzE7shdWtdV4/mkRZ7IoA9/LU9SH5ACP26QB8VsaiyTYTGsRL/kdG7jMCF7mYi4ZBa4Fy9C/78FDBFw==").unwrap()
);

#[cfg(feature = "protocol_feature_alt_bn128")]
def_test_ext!(
    ext_alt_bn128_g1_sum,
    "ext_alt_bn128_g1_sum",
    &base64::decode("6I9NGC6Ikzk7Xw/CIippAtOEsTx4TodcXRjzzu5TLh4EIPsrWPsfnQMtqKfMMF+SHgSphZseRKyej9jTVCT8Aw==").unwrap(),
    &base64::decode("AgAAAADs00QWBTHQDDU1J1FtsDVGC5rDTICkFAtdvqNcFVO0EsRf4pf1kU9yNWyaj2ligWxqnoZGLtEEu3Ldp8+dgkQpAT+SS7pJZ4ql4b8tnwGv8W020cyHrmLCU15/Hp+LLCsDb34dEXKnY0BG4EoWCfaLdEAFcmAKKBbqXEqkAlbaTDA=").unwrap()
);

#[cfg(feature = "protocol_feature_alt_bn128")]
def_test_ext!(
    ext_alt_bn128_g1_multiexp,
    "ext_alt_bn128_g1_multiexp",
    &base64::decode("qoK67D1yppH5iP0qhCrD8Ms+idcZtEry4EegUtSpIylhCyZNbRQ0xVdRe9hQBxZIovzCMwFRMAdcZ5FB+QA6Lg==").unwrap(),
    &base64::decode("AgAAAOzTRBYFMdAMNTUnUW2wNUYLmsNMgKQUC12+o1wVU7QSxF/il/WRT3I1bJqPaWKBbGqehkYu0QS7ct2nz52CRCn3EXSIf0p4ORYJ7mRmZLWtUyGrqlKl/4DNx2kHDEUrET+SS7pJZ4ql4b8tnwGv8W020cyHrmLCU15/Hp+LLCsD2H5fx6TkvPtG6iZSiHT1Ih1TDyGsHTrOzFWN3hx0FwAaB2tgYeH+WuEKReDHNFmxyi8v597Ji5NP4PU8bZXkGQ==").unwrap()
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

        let promise_results = vec![];
        let result = run_vm(
            &code,
            "out_of_memory",
            &mut fake_external,
            context,
            &config,
            &fees,
            &promise_results,
            vm_kind,
            LATEST_PROTOCOL_VERSION,
            None,
            ProfileData::new_disabled(),
        );
        assert_eq!(
            result.1,
            match vm_kind {
                VMKind::Wasmer0 | VMKind::Wasmer1 => Some(VMError::FunctionCallError(
                    FunctionCallError::WasmTrap(WasmTrap::Unreachable)
                )),
                VMKind::Wasmtime => unreachable!(),
            }
        );
    })
}
