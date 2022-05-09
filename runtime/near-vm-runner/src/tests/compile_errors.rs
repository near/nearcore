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
            make_simple_contract_call_vm(&initializer_wrong_signature_contract(), "hello", vm_kind)
                .1,
            (Some(VMError::FunctionCallError(FunctionCallError::CompilationError(
                CompilationError::PrepareError(PrepareError::Deserialization)
            ))))
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
            make_simple_contract_call_vm(&function_not_defined_contract(), "hello", vm_kind).1,
            (Some(VMError::FunctionCallError(FunctionCallError::CompilationError(
                CompilationError::PrepareError(PrepareError::Deserialization)
            ))))
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
            make_simple_contract_call_vm(&function_type_not_defined_contract(1), "hello", vm_kind)
                .1,
            (Some(VMError::FunctionCallError(FunctionCallError::CompilationError(
                CompilationError::PrepareError(PrepareError::Deserialization)
            ))))
        );
    });
}

#[test]
// Weird case. It's not valid wasm (wat2wasm validate will fail), but wasmer allows it.
fn test_function_type_not_defined_contract_2() {
    with_vm_variants(|vm_kind: VMKind| {
        assert_eq!(
            make_simple_contract_call_vm(&function_type_not_defined_contract(0), "hello", vm_kind)
                .1,
            (Some(VMError::FunctionCallError(FunctionCallError::CompilationError(
                CompilationError::PrepareError(PrepareError::Deserialization)
            ))))
        );
    });
}

#[test]
fn test_garbage_contract() {
    with_vm_variants(|vm_kind: VMKind| {
        assert_eq!(
            make_simple_contract_call_vm(&[], "hello", vm_kind).1,
            (Some(VMError::FunctionCallError(FunctionCallError::CompilationError(
                CompilationError::PrepareError(PrepareError::Deserialization)
            ))))
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
            make_simple_contract_call_vm(&evil_function_index(), "abort_with_zero", vm_kind).1,
            (Some(VMError::FunctionCallError(FunctionCallError::CompilationError(
                CompilationError::PrepareError(PrepareError::Deserialization)
            ))))
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

        let code = near_test_contracts::LargeContract {
            functions: functions_number_limit + 1,
            ..Default::default()
        }
        .make();
        let (_, err) = make_simple_contract_call_with_protocol_version_vm(
            &code,
            method_name,
            old_protocol_version,
            vm_kind,
        );
        assert_eq!(err, None);

        let code = near_test_contracts::LargeContract {
            functions: functions_number_limit,
            ..Default::default()
        }
        .make();
        let (_, err) = make_simple_contract_call_with_protocol_version_vm(
            &code,
            method_name,
            new_protocol_version,
            vm_kind,
        );
        assert_eq!(err, None);

        let code = near_test_contracts::LargeContract {
            functions: functions_number_limit + 1,
            ..Default::default()
        }
        .make();
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

        let code = near_test_contracts::LargeContract {
            functions: functions_number_limit / 2,
            panic_imports: functions_number_limit / 2 + 1,
            ..Default::default()
        }
        .make();
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

        let code = near_test_contracts::LargeContract {
            functions: functions_number_limit,
            panic_imports: 1,
            ..Default::default()
        }
        .make();
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

#[test]
fn test_limit_locals() {
    with_vm_variants(|vm_kind| {
        match vm_kind {
            VMKind::Wasmer0 | VMKind::Wasmer2 => {}
            // All contracts leading to hardware traps can not run concurrently on Wasmtime and Wasmer,
            // Restore, once get rid of Wasmer 0.x.
            VMKind::Wasmtime => return,
        }

        let wasm_err = near_test_contracts::LargeContract {
            functions: 1,
            locals_per_function: 50_001,
            ..Default::default()
        }
        .make();
        let res = make_simple_contract_call_vm(&wasm_err, "main", vm_kind);
        let expected_gas = super::prepaid_loading_gas(wasm_err.len());
        gas_and_error_match(
            res,
            expected_gas,
            Some(VMError::FunctionCallError(FunctionCallError::CompilationError(
                CompilationError::PrepareError(PrepareError::Deserialization),
            ))),
        );

        let wasm_ok = near_test_contracts::LargeContract {
            functions: 1,
            locals_per_function: 50_000,
            ..Default::default()
        }
        .make();
        let res = make_simple_contract_call_vm(&wasm_ok, "main", vm_kind);
        gas_and_error_match(
            res,
            Some(43682463),
            Some(VMError::FunctionCallError(FunctionCallError::WasmTrap(WasmTrap::Unreachable))),
        );
    })
}

#[test]
fn test_limit_locals_global() {
    with_vm_variants(|vm_kind| {
        let new_protocol_version = ProtocolFeature::LimitContractLocals.protocol_version();
        let old_protocol_version = new_protocol_version - 1;
        let code_1000001_locals = near_test_contracts::LargeContract {
            functions: 101,
            locals_per_function: 9901,
            ..Default::default()
        }
        .make();
        match vm_kind {
            VMKind::Wasmer0 | VMKind::Wasmer2 => {}
            // All contracts leading to hardware traps can not run concurrently on Wasmtime and Wasmer,
            // Restore, once get rid of Wasmer 0.x.
            VMKind::Wasmtime => return,
        }

        let (_, err) = make_simple_contract_call_with_protocol_version_vm(
            &code_1000001_locals,
            "main",
            new_protocol_version,
            vm_kind,
        );
        assert_eq!(
            err,
            Some(VMError::FunctionCallError(FunctionCallError::CompilationError(
                CompilationError::PrepareError(PrepareError::TooManyLocals),
            ))),
        );

        let (_, err) = make_simple_contract_call_with_protocol_version_vm(
            &code_1000001_locals,
            "main",
            old_protocol_version,
            vm_kind,
        );
        assert_eq!(err, None);

        let (_, err) = make_simple_contract_call_with_protocol_version_vm(
            &near_test_contracts::LargeContract {
                functions: 64,
                locals_per_function: 15625,
                ..Default::default()
            }
            .make(),
            "main",
            new_protocol_version,
            vm_kind,
        );
        assert_eq!(err, None);
    })
}

fn call_sandbox_debug_log() -> Vec<u8> {
    wat::parse_str(
        r#"
            (module
                (import "env" "sandbox_debug_log" (func (;0;) (param i64 i64)))
                (func $main (export "main")
                    (call 0 (i64.const 0) (i64.const 1)))
            )"#,
    )
    .unwrap()
}

#[cfg(not(feature = "sandbox"))]
#[test]
fn test_sandbox_only_function() {
    with_vm_variants(|vm_kind| {
        let wasm = call_sandbox_debug_log();
        let res = make_simple_contract_call_vm(&wasm, "main", vm_kind);
        let error_msg = match vm_kind {
            VMKind::Wasmer0 => {
                "link error: Import not found, namespace: env, name: sandbox_debug_log"
            }
            VMKind::Wasmer2 => {
                "Error while importing \"env\".\"sandbox_debug_log\": unknown import. Expected Function(FunctionType { params: [I64, I64], results: [] })"
            }
            VMKind::Wasmtime => "\"unknown import: `env::sandbox_debug_log` has not been defined\"",
        };
        gas_and_error_match(
            res,
            Some(54519963),
            Some(VMError::FunctionCallError(FunctionCallError::LinkError {
                msg: error_msg.to_string(),
            })),
        );
    })
}

#[cfg(feature = "sandbox")]
#[test]
fn test_sandbox_only_function_when_sandbox_feature() {
    with_vm_variants(|vm_kind| {
        let wasm = call_sandbox_debug_log();
        let res = make_simple_contract_call_vm(&wasm, "main", vm_kind);
        gas_and_error_match(res, Some(66089076), None);
    })
}
