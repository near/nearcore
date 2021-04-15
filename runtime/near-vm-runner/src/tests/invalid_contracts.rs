use near_vm_errors::{CompilationError, FunctionCallError, PrepareError, VMError};

use crate::tests::{make_simple_contract_call_vm, wat2wasm_no_validate, with_vm_variants};
use crate::VMKind;

fn initializer_wrong_signature_contract() -> Vec<u8> {
    wat2wasm_no_validate(
        r#"
            (module
              (type (;0;) (func (param i32)))
              (func (;0;) (type 0))
              (start 0)
              (export "hello" (func 0))
            )"#,
    )
}

#[test]
fn test_initializer_wrong_signature_contract() {
    with_vm_variants(|vm_kind: VMKind| {
        assert_eq!(
            make_simple_contract_call_vm(&initializer_wrong_signature_contract(), "hello", vm_kind),
            (
                None,
                Some(VMError::FunctionCallError(FunctionCallError::CompilationError(
                    CompilationError::PrepareError(PrepareError::Deserialization)
                )))
            )
        );
    });
}

fn function_not_defined_contract() -> Vec<u8> {
    wat2wasm_no_validate(
        r#"
            (module
              (export "hello" (func 0))
            )"#,
    )
}

#[test]
/// StackHeightInstrumentation is weird but it's what we return for now
fn test_function_not_defined_contract() {
    with_vm_variants(|vm_kind: VMKind| {
        assert_eq!(
            make_simple_contract_call_vm(&function_not_defined_contract(), "hello", vm_kind),
            (
                None,
                Some(VMError::FunctionCallError(FunctionCallError::CompilationError(
                    CompilationError::PrepareError(PrepareError::Deserialization)
                )))
            )
        );
    });
}

fn function_type_not_defined_contract(bad_type: u64) -> Vec<u8> {
    wat2wasm_no_validate(&format!(
        r#"
            (module
              (func (;0;) (type {}))
              (export "hello" (func 0))
            )"#,
        bad_type
    ))
}

#[test]
fn test_function_type_not_defined_contract_1() {
    with_vm_variants(|vm_kind: VMKind| {
        assert_eq!(
            make_simple_contract_call_vm(&function_type_not_defined_contract(1), "hello", vm_kind),
            (
                None,
                Some(VMError::FunctionCallError(FunctionCallError::CompilationError(
                    CompilationError::PrepareError(PrepareError::Deserialization)
                )))
            )
        );
    });
}

#[test]
// Weird case. It's not valid wasm (wat2wasm validate will fail), but wasmer allows it.
fn test_function_type_not_defined_contract_2() {
    with_vm_variants(|vm_kind: VMKind| {
        assert_eq!(
            make_simple_contract_call_vm(&function_type_not_defined_contract(0), "hello", vm_kind),
            (
                None,
                Some(VMError::FunctionCallError(FunctionCallError::CompilationError(
                    CompilationError::PrepareError(PrepareError::Deserialization)
                )))
            )
        );
    });
}

#[test]
fn test_garbage_contract() {
    with_vm_variants(|vm_kind: VMKind| {
        assert_eq!(
            make_simple_contract_call_vm(&[], "hello", vm_kind),
            (
                None,
                Some(VMError::FunctionCallError(FunctionCallError::CompilationError(
                    CompilationError::PrepareError(PrepareError::Deserialization)
                )))
            )
        );
    });
}

fn evil_function_index() -> Vec<u8> {
    wat2wasm_no_validate(
        r#"
          (module
            (type (;0;) (func))
            (func (;0;) (type 0)
              call 4294967295)
            (export "abort_with_zero" (func 0))
          )"#,
    )
}

#[test]
fn test_evil_function_index() {
    with_vm_variants(|vm_kind: VMKind| {
        assert_eq!(
            make_simple_contract_call_vm(&evil_function_index(), "abort_with_zero", vm_kind),
            (
                None,
                Some(VMError::FunctionCallError(FunctionCallError::CompilationError(
                    CompilationError::PrepareError(PrepareError::Deserialization)
                )))
            )
        );
    });
}
