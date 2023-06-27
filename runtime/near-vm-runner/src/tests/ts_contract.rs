use crate::logic::errors::{FunctionCallError, HostError};
use crate::logic::mocks::mock_external::MockedExternal;
use crate::logic::types::ReturnData;
use crate::logic::{External, StorageGetMode, VMConfig};
use near_primitives_core::contract::ContractCode;
use near_primitives_core::runtime::fees::RuntimeFeesConfig;

use crate::tests::{create_context, with_vm_variants, LATEST_PROTOCOL_VERSION};
use crate::vm_kind::VMKind;

#[test]
pub fn test_ts_contract() {
    let config = VMConfig::test();
    with_vm_variants(&config, |vm_kind: VMKind| {
        let code = ContractCode::new(near_test_contracts::ts_contract().to_vec(), None);
        let mut fake_external = MockedExternal::new();

        let context = create_context(Vec::new());
        let fees = RuntimeFeesConfig::test();

        // Call method that panics.
        let promise_results = vec![];
        let runtime = vm_kind.runtime(config.clone()).expect("runtime has not been compiled");
        let result = runtime.run(
            &code,
            "try_panic",
            &mut fake_external,
            context,
            &fees,
            &promise_results,
            LATEST_PROTOCOL_VERSION,
            None,
        );
        let outcome = result.expect("execution failed");
        assert_eq!(
            outcome.aborted,
            Some(FunctionCallError::HostError(HostError::GuestPanic {
                panic_msg: "explicit guest panic".to_string()
            }))
        );

        // Call method that writes something into storage.
        let context = create_context(b"foo bar".to_vec());
        runtime
            .run(
                &code,
                "try_storage_write",
                &mut fake_external,
                context,
                &fees,
                &promise_results,
                LATEST_PROTOCOL_VERSION,
                None,
            )
            .expect("bad failure");
        // Verify by looking directly into the storage of the host.
        {
            let res = fake_external.storage_get(b"foo", StorageGetMode::Trie);
            let value_ptr = res.unwrap().unwrap();
            let value = value_ptr.deref().unwrap();
            let value = String::from_utf8(value).unwrap();
            assert_eq!(value.as_str(), "bar");
        }

        // Call method that reads the value from storage using registers.
        let context = create_context(b"foo".to_vec());
        let outcome = runtime
            .run(
                &code,
                "try_storage_read",
                &mut fake_external,
                context,
                &fees,
                &promise_results,
                LATEST_PROTOCOL_VERSION,
                None,
            )
            .expect("execution failed");

        if let ReturnData::Value(value) = outcome.return_data {
            let value = String::from_utf8(value).unwrap();
            assert_eq!(value, "bar");
        } else {
            panic!("Value was not returned");
        }
    });
}
