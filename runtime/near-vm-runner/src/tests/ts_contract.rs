use super::test_vm_config;
use crate::logic::errors::{FunctionCallError, HostError};
use crate::logic::mocks::mock_external::MockedExternal;
use crate::logic::types::ReturnData;
use crate::logic::{External, StorageGetMode};
use crate::runner::VMKindExt;
use crate::tests::{create_context, with_vm_variants};
use crate::ContractCode;
use near_parameters::vm::VMKind;
use near_parameters::RuntimeFeesConfig;

#[test]
pub fn test_ts_contract() {
    let config = test_vm_config();
    with_vm_variants(&config, |vm_kind: VMKind| {
        let code = ContractCode::new(near_test_contracts::ts_contract().to_vec(), None);
        let code_hash = code.hash();
        let mut fake_external = MockedExternal::with_code_hash(*code_hash);

        let context = create_context(Vec::new());
        let fees = RuntimeFeesConfig::test();

        // Call method that panics.
        let promise_results = vec![];
        let runtime = vm_kind.runtime(config.clone()).expect("runtime has not been compiled");
        let result = runtime.run(
            Some(&code),
            "try_panic",
            &mut fake_external,
            &context,
            &fees,
            &promise_results,
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
                Some(&code),
                "try_storage_write",
                &mut fake_external,
                &context,
                &fees,
                &promise_results,
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
                Some(&code),
                "try_storage_read",
                &mut fake_external,
                &context,
                &fees,
                &promise_results,
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
