mod fixtures;
mod vm_logic_builder;

use crate::fixtures::get_context;
use near_vm_errors::{HostError, VMLogicError};
use vm_logic_builder::VMLogicBuilder;

#[test]
fn test_iterator_deprecated() {
    let context = get_context(vec![], false);
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(context);
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
