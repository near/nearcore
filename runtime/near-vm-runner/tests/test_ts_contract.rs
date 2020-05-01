use near_runtime_fees::RuntimeFeesConfig;
use near_vm_errors::FunctionCallError;
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::types::ReturnData;
use near_vm_logic::{External, HostError, VMConfig, VMContext};
use near_vm_runner::{run, VMError};

pub mod test_utils;

fn create_context(input: &[u8]) -> VMContext {
    let input = input.to_vec();
    test_utils::create_context(input)
}

const TEST_CONTRACT: &'static [u8] = include_bytes!("../tests/res/test_contract_ts.wasm");

#[test]
pub fn test_ts_contract() {
    let code = &TEST_CONTRACT;
    let mut fake_external = MockedExternal::new();

    let context = create_context(&[]);
    let config = VMConfig::default();
    let fees = RuntimeFeesConfig::default();

    // Call method that panics.
    let promise_results = vec![];
    let result = run(
        vec![],
        &code,
        b"try_panic",
        &mut fake_external,
        context,
        &config,
        &fees,
        &promise_results,
    );
    assert_eq!(
        result.1,
        Some(VMError::FunctionCallError(FunctionCallError::HostError(HostError::GuestPanic {
            panic_msg: "explicit guest panic".to_string()
        })))
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
        &fees,
        &promise_results,
    )
    .0
    .unwrap();
    // Verify by looking directly into the storage of the host.
    {
        let res = fake_external.storage_get(b"foo");
        let value_ptr = res.unwrap().unwrap();
        let value = value_ptr.deref().unwrap();
        let value = String::from_utf8(value).unwrap();
        assert_eq!(value.as_str(), "bar");
    }

    // Call method that reads the value from storage using registers.
    let context = create_context(b"foo");
    let result = run(
        vec![],
        &code,
        b"try_storage_read",
        &mut fake_external,
        context,
        &config,
        &fees,
        &promise_results,
    );

    if let ReturnData::Value(value) = result.0.unwrap().return_data {
        let value = String::from_utf8(value).unwrap();
        assert_eq!(value, "bar");
    } else {
        panic!("Value was not returned");
    }
}
