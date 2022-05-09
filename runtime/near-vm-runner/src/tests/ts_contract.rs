use near_primitives::contract::ContractCode;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_vm_errors::{FunctionCallError, HostError, VMError};
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::types::ReturnData;
use near_vm_logic::{External, VMConfig};

use crate::tests::{create_context, with_vm_variants, LATEST_PROTOCOL_VERSION};
use crate::vm_kind::VMKind;

#[test]
pub fn test_ts_contract() {
    with_vm_variants(|vm_kind: VMKind| {
        let code = ContractCode::new(near_test_contracts::ts_contract().to_vec(), None);
        let mut fake_external = MockedExternal::new();

        let context = create_context(Vec::new());
        let config = VMConfig::test();
        let fees = RuntimeFeesConfig::test();

        // Call method that panics.
        let promise_results = vec![];
        let gas_counter = context.new_gas_counter(&config);
        let runtime = vm_kind.runtime(config.clone()).expect("runtime has not been compiled");
        let result = runtime.run(
            &code,
            "try_panic",
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
            Some(&VMError::FunctionCallError(FunctionCallError::HostError(
                HostError::GuestPanic { panic_msg: "explicit guest panic".to_string() }
            )))
        );

        // Call method that writes something into storage.
        let context = create_context(b"foo bar".to_vec());
        let gas_counter = context.new_gas_counter(&config);
        runtime.run(
            &code,
            "try_storage_write",
            &mut fake_external,
            context,
            &fees,
            gas_counter,
            &promise_results,
            LATEST_PROTOCOL_VERSION,
            None,
        );
        // Verify by looking directly into the storage of the host.
        {
            let res = fake_external.storage_get(b"foo");
            let value_ptr = res.unwrap().unwrap();
            let value = value_ptr.deref().unwrap();
            let value = String::from_utf8(value).unwrap();
            assert_eq!(value.as_str(), "bar");
        }

        // Call method that reads the value from storage using registers.
        let context = create_context(b"foo".to_vec());
        let gas_counter = context.new_gas_counter(&config);
        let result = runtime.run(
            &code,
            "try_storage_read",
            &mut fake_external,
            context,
            &fees,
            gas_counter,
            &promise_results,
            LATEST_PROTOCOL_VERSION,
            None,
        );

        if let ReturnData::Value(value) = result.outcome().return_data.clone() {
            let value = String::from_utf8(value).unwrap();
            assert_eq!(value, "bar");
        } else {
            panic!("Value was not returned");
        }
    });
}
