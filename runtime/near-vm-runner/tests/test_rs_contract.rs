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

fn create_context(input: &[u64]) -> VMContext {
    let input = arr_u64_to_u8(input);
    VMContext {
        current_account_id: CURRENT_ACCOUNT_ID.to_owned(),
        signer_account_id: SIGNER_ACCOUNT_ID.to_owned(),
        signer_account_pk: Vec::from(&SIGNER_ACCOUNT_PK[..]),
        predecessor_account_id: PREDECESSOR_ACCOUNT_ID.to_owned(),
        input,
        block_index: 0,
        account_balance: 0,
        storage_usage: 0,
        attached_deposit: 0,
        prepaid_gas: 10u64.pow(9),
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

    let context = create_context(&[10u64, 20u64]);
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

    let context = create_context(&[10u64]);
    let result =
        run(vec![], &code, b"read_value", &mut fake_external, context, &config, &promise_results);
    assert_run_result(result, 20);
}

fn run_test_ext(method: &[u8], expected: &[u8]) {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests/res/test_contract_rs.wasm");
    let code = fs::read(path).unwrap();
    let mut fake_external = MockedExternal::new();
    let config = Config::default();
    let context = create_context(&[]);

    let (outcome, err) =
        run(vec![], &code, &method, &mut fake_external, context, &config, &[]);

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

#[test]
pub fn test_externals() {
    run_test_ext(b"ext_account_id", CURRENT_ACCOUNT_ID.as_bytes());
    run_test_ext(b"ext_signer_id", SIGNER_ACCOUNT_ID.as_bytes());
    run_test_ext(b"ext_predecessor_account_id", PREDECESSOR_ACCOUNT_ID.as_bytes());
    run_test_ext(b"ext_signer_pk", &SIGNER_ACCOUNT_PK);
}
