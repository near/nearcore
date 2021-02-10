use crate::test_utils::LATEST_PROTOCOL_VERSION;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_vm_errors::{FunctionCallError, HostError, VMError};
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::types::ReturnData;
use near_vm_logic::{External, VMConfig, VMContext, VMKind};
use near_vm_runner::{run_vm, with_vm_variants};

pub mod test_utils;

fn create_context(input: &[u8]) -> VMContext {
    let input = input.to_vec();
    test_utils::create_context(input)
}

const TEST_CONTRACT: &'static [u8] = include_bytes!("../tests/res/test_contract_ts.wasm");

#[test]
pub fn test_ts_contract() {
    with_vm_variants(|vm_kind: VMKind| {
        let code = &TEST_CONTRACT;
        let mut fake_external = MockedExternal::new();

        let context = create_context(&[]);
        let config = VMConfig::default();
        let fees = RuntimeFeesConfig::default();

        // Call method that panics.
        let promise_results = vec![];
        let result = run_vm(
            vec![],
            &code,
            b"try_panic",
            &mut fake_external,
            context,
            &config,
            &fees,
            &promise_results,
            vm_kind.clone(),
            LATEST_PROTOCOL_VERSION,
            None,
        );
        assert_eq!(
            result.1,
            Some(VMError::FunctionCallError(FunctionCallError::HostError(HostError::GuestPanic {
                panic_msg: "explicit guest panic".to_string()
            })))
        );

        // Call method that writes something into storage.
        let context = create_context(b"foo bar");
        run_vm(
            vec![],
            &code,
            b"try_storage_write",
            &mut fake_external,
            context,
            &config,
            &fees,
            &promise_results,
            vm_kind.clone(),
            LATEST_PROTOCOL_VERSION,
            None,
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
        let result = run_vm(
            vec![],
            &code,
            b"try_storage_read",
            &mut fake_external,
            context,
            &config,
            &fees,
            &promise_results,
            vm_kind,
            LATEST_PROTOCOL_VERSION,
            None,
        );

        if let ReturnData::Value(value) = result.0.unwrap().return_data {
            let value = String::from_utf8(value).unwrap();
            assert_eq!(value, "bar");
        } else {
            panic!("Value was not returned");
        }
    });
}
