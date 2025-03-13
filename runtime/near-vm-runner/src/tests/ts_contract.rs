use super::test_vm_config;
use crate::ContractCode;
use crate::logic::External;
use crate::logic::errors::{FunctionCallError, HostError};
use crate::logic::gas_counter::FreeGasCounter;
use crate::logic::mocks::mock_external::MockedExternal;
use crate::logic::types::ReturnData;
use crate::runner::VMKindExt;
use crate::tests::{create_context, with_vm_variants};
use near_parameters::RuntimeFeesConfig;
use near_parameters::vm::VMKind;
use std::sync::Arc;

#[test]
pub fn test_ts_contract() {
    let config = Arc::new(test_vm_config());
    with_vm_variants(&config, |vm_kind: VMKind| {
        let code = ContractCode::new(near_test_contracts::ts_contract().to_vec(), None);
        let mut fake_external = MockedExternal::with_code(code);
        let context = create_context(Vec::new());
        let fees = Arc::new(RuntimeFeesConfig::test());

        // Call method that panics.
        let runtime = vm_kind.runtime(config.clone()).expect("runtime has not been compiled");
        let gas_counter = context.make_gas_counter(&config);
        let result = runtime.prepare(&fake_external, None, gas_counter, "try_panic").run(
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
        let context = create_context(b"foo bar".to_vec());
        let runtime = vm_kind.runtime(config.clone()).expect("runtime has not been compiled");
        let gas_counter = context.make_gas_counter(&config);
        runtime
            .prepare(&fake_external, None, gas_counter, "try_storage_write")
            .run(&mut fake_external, &context, Arc::clone(&fees))
            .expect("bad failure");
        // Verify by looking directly into the storage of the host.
        {
            let res = fake_external.storage_get(&mut FreeGasCounter, b"foo");
            let value_ptr = res.unwrap().unwrap();
            let value = value_ptr.deref(&mut FreeGasCounter).unwrap();
            let value = String::from_utf8(value).unwrap();
            assert_eq!(value.as_str(), "bar");
        }

        // Call method that reads the value from storage using registers.
        let context = create_context(b"foo".to_vec());
        let runtime = vm_kind.runtime(config.clone()).expect("runtime has not been compiled");
        let gas_counter = context.make_gas_counter(&config);
        let outcome = runtime
            .prepare(&fake_external, None, gas_counter, "try_storage_read")
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
