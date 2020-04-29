use near_vm_errors::{CompilationError, FunctionCallError, MethodResolveError, PrepareError};
use near_vm_logic::{HostError, ReturnData, VMOutcome};
use near_vm_runner::VMError;

pub mod test_utils;

use self::test_utils::{make_simple_contract_call, make_simple_contract_call_with_gas};

fn vm_outcome_with_gas(gas: u64) -> VMOutcome {
    VMOutcome {
        balance: 4,
        storage_usage: 12,
        return_data: ReturnData::None,
        burnt_gas: gas,
        used_gas: gas,
        logs: vec![],
    }
}

fn infinite_initializer_contract() -> Vec<u8> {
    wabt::wat2wasm(
        r#"
            (module
              (type (;0;) (func))
              (func (;0;) (type 0) (loop (br 0)))
              (func (;1;) (type 0))
              (start 0)
              (export "hello" (func 1))
            )"#,
    )
    .unwrap()
}

#[test]
fn test_infinite_initializer() {
    assert_eq!(
        make_simple_contract_call(&infinite_initializer_contract(), b"hello"),
        (
            Some(vm_outcome_with_gas(100000000000000)),
            Some(VMError::FunctionCallError(FunctionCallError::HostError(HostError::GasExceeded)))
        )
    );
}

#[test]
fn test_infinite_initializer_export_not_found() {
    assert_eq!(
        make_simple_contract_call(&infinite_initializer_contract(), b"hello2"),
        (
            None,
            Some(VMError::FunctionCallError(FunctionCallError::MethodResolveError(
                MethodResolveError::MethodNotFound
            )))
        )
    );
}

fn simple_contract() -> Vec<u8> {
    wabt::wat2wasm(
        r#"
            (module
              (type (;0;) (func))
              (func (;0;) (type 0))
              (export "hello" (func 0))
            )"#,
    )
    .unwrap()
}

#[test]
fn test_simple_contract() {
    assert_eq!(
        make_simple_contract_call(&simple_contract(), b"hello"),
        (Some(vm_outcome_with_gas(0)), None)
    );
}

#[test]
fn test_export_not_found() {
    assert_eq!(
        make_simple_contract_call(&simple_contract(), b"hello2"),
        (
            None,
            Some(VMError::FunctionCallError(FunctionCallError::MethodResolveError(
                MethodResolveError::MethodNotFound
            )))
        )
    );
}

#[test]
fn test_empty_method() {
    assert_eq!(
        make_simple_contract_call(&simple_contract(), b""),
        (
            None,
            Some(VMError::FunctionCallError(FunctionCallError::MethodResolveError(
                MethodResolveError::MethodEmptyName
            )))
        )
    );
}

#[test]
fn test_invalid_utf8() {
    assert_eq!(
        make_simple_contract_call(&simple_contract(), &[255u8]),
        (
            None,
            Some(VMError::FunctionCallError(FunctionCallError::MethodResolveError(
                MethodResolveError::MethodUTF8Error
            )))
        )
    );
}

fn trap_contract() -> Vec<u8> {
    wabt::wat2wasm(
        r#"
            (module
              (type (;0;) (func))
              (func (;0;) (type 0) (unreachable))
              (export "hello" (func 0))
            )"#,
    )
    .unwrap()
}

#[test]
fn test_trap_contract() {
    assert_eq!(
        make_simple_contract_call(&trap_contract(), b"hello"),
        (
            Some(vm_outcome_with_gas(3856371)),
            Some(VMError::FunctionCallError(FunctionCallError::WasmUnknownError))
        )
    );
}

fn trap_initializer() -> Vec<u8> {
    wabt::wat2wasm(
        r#"
            (module
              (type (;0;) (func))
              (func (;0;) (type 0) (unreachable))
              (start 0)
              (export "hello" (func 0))
            )"#,
    )
    .unwrap()
}

#[test]
fn test_trap_initializer() {
    assert_eq!(
        make_simple_contract_call(&trap_initializer(), b"hello"),
        (
            Some(vm_outcome_with_gas(3856371)),
            Some(VMError::FunctionCallError(FunctionCallError::WasmUnknownError))
        )
    );
}

fn wrong_signature_contract() -> Vec<u8> {
    wabt::wat2wasm(
        r#"
            (module
              (type (;0;) (func (param i32)))
              (func (;0;) (type 0))
              (export "hello" (func 0))
            )"#,
    )
    .unwrap()
}

#[test]
fn test_wrong_signature_contract() {
    assert_eq!(
        make_simple_contract_call(&wrong_signature_contract(), b"hello"),
        (
            None,
            Some(VMError::FunctionCallError(FunctionCallError::MethodResolveError(
                MethodResolveError::MethodInvalidSignature
            )))
        )
    );
}

fn export_wrong_type() -> Vec<u8> {
    wabt::wat2wasm(
        r#"
            (module
              (global (;0;) i32 (i32.const 123))
              (export "hello" (global 0))
            )"#,
    )
    .unwrap()
}

#[test]
fn test_export_wrong_type() {
    assert_eq!(
        make_simple_contract_call(&export_wrong_type(), b"hello"),
        (
            None,
            Some(VMError::FunctionCallError(FunctionCallError::MethodResolveError(
                MethodResolveError::MethodNotFound
            )))
        )
    );
}

fn guest_panic() -> Vec<u8> {
    wabt::wat2wasm(
        r#"
            (module
              (type (;0;) (func))
              (import "env" "panic" (func (;0;) (type 0)))
              (func (;1;) (type 0) (call 0))
              (export "hello" (func 1))
            )"#,
    )
    .unwrap()
}

#[test]
fn test_guest_panic() {
    assert_eq!(
        make_simple_contract_call(&guest_panic(), b"hello"),
        (
            Some(vm_outcome_with_gas(269143626)),
            Some(VMError::FunctionCallError(FunctionCallError::HostError(HostError::GuestPanic {
                panic_msg: "explicit guest panic".to_string()
            })))
        )
    );
}

fn stack_overflow() -> Vec<u8> {
    wabt::wat2wasm(
        r#"
            (module
              (type (;0;) (func))
              (func (;0;) (type 0) (call 0))
              (export "hello" (func 0))
            )"#,
    )
    .unwrap()
}

#[test]
fn test_stack_overflow() {
    assert_eq!(
        make_simple_contract_call(&stack_overflow(), b"hello"),
        (
            Some(vm_outcome_with_gas(63182782464)),
            Some(VMError::FunctionCallError(FunctionCallError::WasmUnknownError))
        )
    );
}

fn memory_grow() -> Vec<u8> {
    wabt::wat2wasm(
        r#"
            (module
              (type (;0;) (func))
              (func (;0;) (type 0)
                (loop
                  (memory.grow (i32.const 1))
                  drop
                  br 0
                )
              )
              (memory (;0;) 17 32)
              (export "hello" (func 0))
            )"#,
    )
    .unwrap()
}

#[test]
fn test_memory_grow() {
    assert_eq!(
        make_simple_contract_call(&memory_grow(), b"hello"),
        (
            Some(vm_outcome_with_gas(100000000000000)),
            Some(VMError::FunctionCallError(FunctionCallError::HostError(HostError::GasExceeded)))
        )
    );
}

fn bad_import_global(env: &str) -> Vec<u8> {
    wabt::wat2wasm(format!(
        r#"
            (module
              (type (;0;) (func))
              (import "{}" "input" (global (;0;) i32))
              (func (;0;) (type 0))
              (export "hello" (func 0))
            )"#,
        env
    ))
    .unwrap()
}

fn bad_import_func(env: &str) -> Vec<u8> {
    wabt::wat2wasm(format!(
        r#"
            (module
              (type (;0;) (func))
              (import "{}" "wtf" (func (;0;) (type 0)))
              (func (;0;) (type 0))
              (export "hello" (func 0))
            )"#,
        env
    ))
    .unwrap()
}

#[test]
// Weird behavior:
// Invalid import not from "env" -> PrepareError::Instantiate
// Invalid import from "env" -> LinkError
fn test_bad_import_1() {
    assert_eq!(
        make_simple_contract_call(&bad_import_global("wtf"), b"hello"),
        (
            None,
            Some(VMError::FunctionCallError(FunctionCallError::CompilationError(
                CompilationError::PrepareError(PrepareError::Instantiate)
            )))
        )
    );
}

#[test]
fn test_bad_import_2() {
    assert_eq!(
        make_simple_contract_call(&bad_import_func("wtf"), b"hello"),
        (
            None,
            Some(VMError::FunctionCallError(FunctionCallError::CompilationError(
                CompilationError::PrepareError(PrepareError::Instantiate)
            )))
        )
    );
}

#[test]
fn test_bad_import_3() {
    assert_eq!(
        make_simple_contract_call(&bad_import_global("env"), b"hello"),
        (
            Some(vm_outcome_with_gas(0)),
            Some(VMError::FunctionCallError(FunctionCallError::LinkError{
                msg: "link error: Incorrect import type, namespace: env, name: input, expected type: global, found type: function".to_string()
            }))
        )
    );
}

#[test]
fn test_bad_import_4() {
    assert_eq!(
        make_simple_contract_call(&bad_import_func("env"), b"hello"),
        (
            Some(vm_outcome_with_gas(0)),
            Some(VMError::FunctionCallError(FunctionCallError::LinkError {
                msg: "link error: Import not found, namespace: env, name: wtf".to_string()
            }))
        )
    );
}

fn some_initializer_contract() -> Vec<u8> {
    wabt::wat2wasm(
        r#"
            (module
              (type (;0;) (func))
              (func (;0;) (type 0) nop)
              (start 0)
              (export "hello" (func 0))
            )"#,
    )
    .unwrap()
}

#[test]
fn test_initializer_no_gas() {
    assert_eq!(
        make_simple_contract_call_with_gas(&some_initializer_contract(), b"hello", 0),
        (
            Some(vm_outcome_with_gas(0)),
            Some(VMError::FunctionCallError(FunctionCallError::HostError(HostError::GasExceeded)))
        )
    );
}

fn bad_many_imports() -> Vec<u8> {
    let mut imports = String::new();
    for i in 0..100 {
        imports.push_str(&format!(
            r#"
            (import "env" "wtf{}" (func (;{};) (type 0)))
         "#,
            i, i
        ));
    }
    wabt::wat2wasm(format!(
        r#"
            (module
              (type (;0;) (func))
              {}
              (export "hello" (func 0))
            )"#,
        imports
    ))
    .unwrap()
}

#[test]
fn test_bad_many_imports() {
    let result = make_simple_contract_call(&bad_many_imports(), b"hello");
    assert_eq!(result.0, Some(vm_outcome_with_gas(0)));
    if let Some(VMError::FunctionCallError(FunctionCallError::LinkError { msg })) = result.1 {
        eprintln!("{}", msg);
        assert!(msg.len() < 1000, format!("Huge error message: {}", msg.len()));
    } else {
        panic!(result.1);
    }
}
