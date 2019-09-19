use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::types::ReturnData;
use near_vm_logic::{Config, External, VMContext};
use near_vm_runner::{run, VMError};
use std::fs;
use std::path::PathBuf;

const CURRENT_ACCOUNT_ID: &str = "alice";
const SIGNER_ACCOUNT_ID: &str = "bob";
const SIGNER_ACCOUNT_PK: [u8; 3] = [0, 1, 2];
const PREDECESSOR_ACCOUNT_ID: &str = "carol";

fn create_context(input: &[u8]) -> VMContext {
    VMContext {
        current_account_id: CURRENT_ACCOUNT_ID.to_owned(),
        signer_account_id: SIGNER_ACCOUNT_ID.to_owned(),
        signer_account_pk: Vec::from(&SIGNER_ACCOUNT_PK[..]),
        predecessor_account_id: PREDECESSOR_ACCOUNT_ID.to_owned(),
        input: Vec::from(input),
        block_index: 0,
        block_timestamp: 0,
        account_balance: 0,
        storage_usage: 0, // it's not actually 0 and storage_remove will overflow but we don't use it
        attached_deposit: 0,
        prepaid_gas: 10u64.pow(9),
        random_seed: vec![0, 1, 2],
        free_of_charge: false,
        output_data_receivers: vec![],
    }
}

#[test]
pub fn test_ts_contract() {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests/res/test_contract_ts.wasm");
    let code = fs::read(path).unwrap();
    let mut fake_external = MockedExternal::new();

    let context = create_context(&[]);
    let config = Config::default();

    // Call method that panics.
    let promise_results = vec![];
    let result =
        run(vec![], &code, b"try_panic", &mut fake_external, context, &config, &promise_results);
    assert_eq!(
        result.1,
        Some(VMError::WasmerCallError(
            "Smart contract has explicitly invoked `panic`.".to_string()
        ))
    );

    // Call method that writes something into storage.
    let context = create_context(b"foo bar");
    run(
        vec![],
        &code,
        b"try_storage_write",
        &mut fake_external,
        context,
        &config,
        &promise_results,
    )
    .0
    .unwrap();
    // Verify by looking directly into the storage of the host.
    let res = fake_external.storage_get(b"foo");
    let value = res.unwrap().unwrap();
    let value = String::from_utf8(value).unwrap();
    assert_eq!(value.as_str(), "bar");

    // Call method that reads the value from storage using registers.
    let context = create_context(b"foo");
    let result = run(
        vec![],
        &code,
        b"try_storage_read",
        &mut fake_external,
        context,
        &config,
        &promise_results,
    );

    if let ReturnData::Value(value) = result.0.unwrap().return_data {
        let value = String::from_utf8(value).unwrap();
        assert_eq!(value, "bar");
    } else {
        panic!("Value was not returned");
    }
}
