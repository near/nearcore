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
use std::sync::Arc;

#[test]
pub fn test_ts_contract() {
    let config = Arc::new(test_vm_config());
    with_vm_variants(&config, |vm_kind: VMKind| {
        let code = ContractCode::new(near_test_contracts::ts_contract().to_vec(), None);
        let mut fake_external = MockedExternal::with_code(code);
        let context = create_context("try_panic", Vec::new());
        let fees = Arc::new(RuntimeFeesConfig::test());

        // Call method that panics.
        let runtime = vm_kind.runtime(config.clone()).expect("runtime has not been compiled");
        let result = runtime.prepare(&fake_external, &context, None).run(
            &mut fake_external,
            &context,
            Arc::clone(&fees),
        );
        let outcome = result.expect("execution failed");
        assert_eq!(
            outcome.aborted,
            Some(FunctionCallError::HostError(HostError::GuestPanic {
                panic_msg: "explicit guest panic".to_string()
            }))
        );

        // Call method that writes something into storage.
        let context = create_context("try_storage_write", b"foo bar".to_vec());
        let runtime = vm_kind.runtime(config.clone()).expect("runtime has not been compiled");
        runtime
            .prepare(&fake_external, &context, None)
            .run(&mut fake_external, &context, Arc::clone(&fees))
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
        let context = create_context("try_storage_read", b"foo".to_vec());
        let runtime = vm_kind.runtime(config.clone()).expect("runtime has not been compiled");
        let outcome = runtime
            .prepare(&fake_external, &context, None)
            .run(&mut fake_external, &context, Arc::clone(&fees))
            .expect("execution failed");

        if let ReturnData::Value(value) = outcome.return_data {
            let value = String::from_utf8(value).unwrap();
            assert_eq!(value, "bar");
        } else {
            panic!("Value was not returned");
        }
    });
}
