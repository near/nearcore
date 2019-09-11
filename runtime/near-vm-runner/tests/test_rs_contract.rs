use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::types::ReturnData;
use near_vm_logic::{Config, VMContext, VMOutcome};
use near_vm_runner::{run, VMError};
use std::fs;
use std::mem::size_of;
use std::path::PathBuf;

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

const CURRENT_ACCOUNT_ID: &str = "alice";
const SIGNER_ACCOUNT_ID: &str = "bob";
const SIGNER_ACCOUNT_PK: [u8; 3] = [0, 1, 2];
const PREDECESSOR_ACCOUNT_ID: &str = "carol";

fn arr_u64_to_u8(value: &[u64]) -> Vec<u8> {
    let mut res = vec![];
    for el in value {
        res.extend_from_slice(&el.to_le_bytes());
    }
    res
}

fn create_context(input: &[u8]) -> VMContext {
    VMContext {
        current_account_id: CURRENT_ACCOUNT_ID.to_owned(),
        signer_account_id: SIGNER_ACCOUNT_ID.to_owned(),
        signer_account_pk: Vec::from(&SIGNER_ACCOUNT_PK[..]),
        predecessor_account_id: PREDECESSOR_ACCOUNT_ID.to_owned(),
        input: input.to_owned(),
        block_index: 10,
        account_balance: 2u128,
        storage_usage: 12,
        attached_deposit: 2u128,
        prepaid_gas: 10_u64.pow(9),
        random_seed: vec![0, 1, 2],
        free_of_charge: false,
        output_data_receivers: vec![],
    }
}

#[test]
pub fn test_read_write() {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests/res/test_contract_rs.wasm");
    let code = fs::read(path).unwrap();
    let mut fake_external = MockedExternal::new();

    let context = create_context(&arr_u64_to_u8(&[10u64, 20u64]));
    let config = Config::default();

    let promise_results = vec![];
    let result = run(
        vec![],
        &code,
        b"write_key_value",
        &mut fake_external,
        context,
        &config,
        &promise_results,
    );
    assert_run_result(result, 0);

    let context = create_context(&arr_u64_to_u8(&[10u64]));
    let result =
        run(vec![], &code, b"read_value", &mut fake_external, context, &config, &promise_results);
    assert_run_result(result, 20);
}

macro_rules! def_test_ext {
    ($name:ident, $method:expr, $expected:expr, $input:expr) => {
        #[test]
        pub fn $name() {
            run_test_ext($method, $expected, $input)
        }
    };
    ($name:ident, $method:expr, $expected:expr) => {
        #[test]
        pub fn $name() {
            run_test_ext($method, $expected, &[])
        }
    };
}

fn run_test_ext(method: &[u8], expected: &[u8], input: &[u8]) {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests/res/test_contract_rs.wasm");
    let code = fs::read(path).unwrap();
    let mut fake_external = MockedExternal::new();
    let config = Config::default();
    let context = create_context(&input);

    let (outcome, err) =
        run(input.to_owned(), &code, &method, &mut fake_external, context, &config, &[]);

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

def_test_ext!(ext_account_id, b"ext_account_id", CURRENT_ACCOUNT_ID.as_bytes());

def_test_ext!(ext_signer_id, b"ext_signer_id", SIGNER_ACCOUNT_ID.as_bytes());
def_test_ext!(ext_predecessor_account_id, b"ext_predecessor_account_id", PREDECESSOR_ACCOUNT_ID.as_bytes(), &[]);
def_test_ext!(ext_signer_pk, b"ext_signer_pk", &SIGNER_ACCOUNT_PK);
def_test_ext!(ext_random_seed, b"ext_random_seed", &[0, 1, 2]);

def_test_ext!(ext_prepaid_gas, b"ext_prepaid_gas", &(10_u64.pow(9)).to_le_bytes());
def_test_ext!(ext_block_index, b"ext_block_index", &10u64.to_le_bytes());
def_test_ext!(ext_storage_usage, b"ext_storage_usage", &12u64.to_le_bytes());
// TODO: mock used_gas
def_test_ext!(ext_used_gas, b"ext_used_gas", &19u64.to_le_bytes());
def_test_ext!(ext_sha256,
    b"ext_sha256",
    &[
        18, 176, 115, 156, 45, 100, 241, 132, 180, 134, 77, 42, 105, 111, 199, 127, 118, 112,
        92, 255, 88, 43, 83, 147, 122, 55, 26, 36, 42, 156, 160, 158,
    ],
    b"tesdsst"
);
// current_account_balance = context.account_balance + context.attached_deposit;
def_test_ext!(ext_account_balance, b"ext_account_balance", &(2u128 + 2).to_le_bytes());
def_test_ext!(ext_attached_deposit, b"ext_attached_deposit", &2u128.to_le_bytes());
