mod fixtures;

use crate::fixtures::get_context;
use near_runtime_fees::RuntimeFeesConfig;
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::mocks::mock_memory::MockedMemory;
use near_vm_logic::{HostError, VMConfig, VMLogic, VMLogicError};

#[test]
fn test_iterator_deprecated() {
    let mut ext = MockedExternal::default();
    let context = get_context(vec![], false);
    let config = VMConfig::default();
    let fees = RuntimeFeesConfig::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();
    let mut logic =
        VMLogic::new(&mut ext, context, &config, &fees, &promise_results, &mut memory, None);

    assert_eq!(
        Err(VMLogicError::HostError(HostError::Deprecated {
            method_name: "storage_iter_prefix".to_string()
        })),
        logic.storage_iter_prefix(1, b"a".as_ptr() as _)
    );
    assert_eq!(
        Err(VMLogicError::HostError(HostError::Deprecated {
            method_name: "storage_iter_range".to_string()
        })),
        logic.storage_iter_range(1, b"a".as_ptr() as _, 1, b"b".as_ptr() as _)
    );
    assert_eq!(
        Err(VMLogicError::HostError(HostError::Deprecated {
            method_name: "storage_iter_next".to_string()
        })),
        logic.storage_iter_next(0, 0, 1)
    );
}
