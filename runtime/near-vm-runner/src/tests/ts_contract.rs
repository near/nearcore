use near_primitives::contract::ContractCode;
use near_primitives::profile::ProfileData;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_vm_errors::{FunctionCallError, HostError, VMError};
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::types::ReturnData;
use near_vm_logic::{External, VMConfig};

use crate::tests::{create_context, with_vm_variants, LATEST_PROTOCOL_VERSION};
use crate::{run_vm, VMKind};

#[test]
pub fn test_ts_contract() {
    with_vm_variants(|vm_kind: VMKind| {
        let code = ContractCode::new(near_test_contracts::ts_contract().to_vec(), None);
        let mut fake_external = MockedExternal::new();

        let context = create_context(Vec::new());
        let config = VMConfig::default();
        let fees = RuntimeFeesConfig::default();

        // Call method that panics.
        let promise_results = vec![];
        let result = run_vm(
            &code,
            "try_panic",
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
        assert_eq!(
            result.1,
            Some(VMError::FunctionCallError(FunctionCallError::HostError(HostError::GuestPanic {
                panic_msg: "explicit guest panic".to_string()
            })))
        );

        // Call method that writes something into storage.
        let context = create_context(b"foo bar".to_vec());
        run_vm(
            &code,
            "try_storage_write",
            &mut fake_external,
            context,
            &config,
            &fees,
            &promise_results,
            vm_kind.clone(),
            LATEST_PROTOCOL_VERSION,
            None,
            ProfileData::new_disabled(),
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
        let context = create_context(b"foo".to_vec());
        let result = run_vm(
            &code,
            "try_storage_read",
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

        if let ReturnData::Value(value) = result.0.unwrap().return_data {
            let value = String::from_utf8(value).unwrap();
            assert_eq!(value, "bar");
        } else {
            panic!("Value was not returned");
        }
    });
}
