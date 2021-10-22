#[cfg(feature = "protocol_feature_limit_contract_functions_number")]
use near_primitives::version::ProtocolFeature;
#[cfg(not(feature = "protocol_feature_limit_contract_functions_number"))]
use near_primitives::version::PROTOCOL_VERSION;
use near_vm_errors::{CompilationError, FunctionCallError, PrepareError, VMError};

#[cfg(feature = "protocol_feature_limit_contract_functions_number")]
use assert_matches::assert_matches;

use crate::tests::{
    make_simple_contract_call_vm, make_simple_contract_call_with_protocol_version_vm,
    with_vm_variants,
};
use crate::VMKind;

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
        #[cfg(feature = "protocol_feature_limit_contract_functions_number")]
        let old_protocol_version =
            ProtocolFeature::LimitContractFunctionsNumber.protocol_version() - 1;
        #[cfg(not(feature = "protocol_feature_limit_contract_functions_number"))]
        let old_protocol_version = PROTOCOL_VERSION - 1;

        let new_protocol_version = old_protocol_version + 1;

        let functions_number_limit: u32 = 10_000;
        let method_name = "main";

        let code = near_test_contracts::many_functions_contract(functions_number_limit + 10);
        let (_, err) = make_simple_contract_call_with_protocol_version_vm(
            &code,
            method_name,
            old_protocol_version,
            vm_kind,
        );
        assert_eq!(err, None);

        let code = near_test_contracts::many_functions_contract(functions_number_limit - 10);
        let (_, err) = make_simple_contract_call_with_protocol_version_vm(
            &code,
            method_name,
            new_protocol_version,
            vm_kind,
        );
        assert_eq!(err, None);

        let code = near_test_contracts::many_functions_contract(functions_number_limit + 10);
        let (_, err) = make_simple_contract_call_with_protocol_version_vm(
            &code,
            method_name,
            new_protocol_version,
            vm_kind,
        );
        #[cfg(not(feature = "protocol_feature_limit_contract_functions_number"))]
        assert_eq!(err, None);
        #[cfg(feature = "protocol_feature_limit_contract_functions_number")]
        assert_matches!(
            err,
            Some(VMError::FunctionCallError(FunctionCallError::CompilationError(
                CompilationError::PrepareError(PrepareError::TooManyFunctions)
            )))
        );
    });
}
