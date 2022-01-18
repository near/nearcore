use near_primitives::version::ProtocolFeature;
use near_vm_errors::{CompilationError, FunctionCallError, PrepareError, VMError, WasmTrap};

use assert_matches::assert_matches;

use crate::tests::{
    gas_and_error_match, make_simple_contract_call_vm,
    make_simple_contract_call_with_protocol_version_vm, with_vm_variants,
};
use crate::vm_kind::VMKind;

fn initializer_wrong_signature_contract() -> Vec<u8> {
    wat::parse_str(
        r#"
            (module
              (type (;0;) (func (param i32)))
              (func (;0;) (type 0))
              (start 0)
              (export "hello" (func 0))
            )"#,
    )
    .unwrap()
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
    wat::parse_str(
        r#"
            (module
              (export "hello" (func 0))
            )"#,
    )
    .unwrap()
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
    wat::parse_str(&format!(
        r#"
            (module
              (func (;0;) (type {}))
              (export "hello" (func 0))
            )"#,
        bad_type
    ))
    .unwrap()
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
    wat::parse_str(
        r#"
          (module
            (type (;0;) (func))
            (func (;0;) (type 0)
              call 4294967295)
            (export "abort_with_zero" (func 0))
          )"#,
    )
    .unwrap()
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

#[test]
fn test_limit_contract_functions_number() {
    with_vm_variants(|vm_kind: VMKind| {
        let old_protocol_version =
            ProtocolFeature::LimitContractFunctionsNumber.protocol_version() - 1;
        let new_protocol_version = old_protocol_version + 1;

        let functions_number_limit: u32 = 10_000;
        let method_name = "main";

        let code = near_test_contracts::many_functions_contract(functions_number_limit + 1);
        let (_, err) = make_simple_contract_call_with_protocol_version_vm(
            &code,
            method_name,
            old_protocol_version,
            vm_kind,
        );
        assert_eq!(err, None);

        let code = near_test_contracts::many_functions_contract(functions_number_limit);
        let (_, err) = make_simple_contract_call_with_protocol_version_vm(
            &code,
            method_name,
            new_protocol_version,
            vm_kind,
        );
        assert_eq!(err, None);

        let code = near_test_contracts::many_functions_contract(functions_number_limit + 1);
        let (_, err) = make_simple_contract_call_with_protocol_version_vm(
            &code,
            method_name,
            new_protocol_version,
            vm_kind,
        );
        assert_matches!(
            err,
            Some(VMError::FunctionCallError(FunctionCallError::CompilationError(
                CompilationError::PrepareError(PrepareError::TooManyFunctions)
            )))
        );
    });
}

fn many_locals(n_locals: usize) -> Vec<u8> {
    wat::parse_str(&format!(
        r#"
            (module
              (func $main (export "main")
                (local {})
                (call $main))
            )"#,
        "i32 ".repeat(n_locals)
    ))
    .unwrap()
}

#[test]
fn test_limit_locals() {
    with_vm_variants(|vm_kind| {
        match vm_kind {
            VMKind::Wasmer0 | VMKind::Wasmer2 => {}
            // All contracts leading to hardware traps can not run concurrently on Wasmtime and Wasmer,
            // Restore, once get rid of Wasmer 0.x.
            VMKind::Wasmtime => return,
        }

        let wasm_err = many_locals(50_001);
        let res = make_simple_contract_call_vm(&wasm_err, "main", vm_kind);
        gas_and_error_match(
            res,
            None,
            Some(VMError::FunctionCallError(FunctionCallError::CompilationError(
                CompilationError::PrepareError(PrepareError::Deserialization),
            ))),
        );

        let wasm_ok = many_locals(50_000);
        let res = make_simple_contract_call_vm(&wasm_ok, "main", vm_kind);
        gas_and_error_match(
            res,
            Some(47583963),
            Some(VMError::FunctionCallError(FunctionCallError::WasmTrap(WasmTrap::Unreachable))),
        );
    })
}
