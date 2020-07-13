use near_vm_errors::{CompilationError, FunctionCallError, MethodResolveError, PrepareError};
use near_vm_logic::{HostError, ReturnData, VMKind, VMOutcome};
use near_vm_runner::{with_vm_variants, VMError};

pub mod test_utils;

use self::test_utils::{make_simple_contract_call_vm, make_simple_contract_call_with_gas_vm};

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
    with_vm_variants(|vm_kind: VMKind| {
        assert_eq!(
            make_simple_contract_call_vm(&infinite_initializer_contract(), b"hello", vm_kind),
            (
                Some(vm_outcome_with_gas(100000000000000)),
                Some(VMError::FunctionCallError(FunctionCallError::HostError(
                    HostError::GasExceeded
                )))
            )
        );
    });
}

#[test]
fn test_infinite_initializer_export_not_found() {
    with_vm_variants(|vm_kind: VMKind| {
        assert_eq!(
            make_simple_contract_call_vm(&infinite_initializer_contract(), b"hello2", vm_kind),
            (
                None,
                Some(VMError::FunctionCallError(FunctionCallError::MethodResolveError(
                    MethodResolveError::MethodNotFound
                )))
            )
        );
    });
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
    with_vm_variants(|vm_kind: VMKind| {
        assert_eq!(
            make_simple_contract_call_vm(&simple_contract(), b"hello", vm_kind),
            (Some(vm_outcome_with_gas(43032213)), None),
        );
    });
}

#[test]
fn test_export_not_found() {
    with_vm_variants(|vm_kind: VMKind| {
        assert_eq!(
            make_simple_contract_call_vm(&simple_contract(), b"hello2", vm_kind),
            (
                None,
                Some(VMError::FunctionCallError(FunctionCallError::MethodResolveError(
                    MethodResolveError::MethodNotFound
                )))
            )
        );
    });
}

#[test]
fn test_empty_method() {
    with_vm_variants(|vm_kind: VMKind| {
        assert_eq!(
            make_simple_contract_call_vm(&simple_contract(), b"", vm_kind),
            (
                None,
                Some(VMError::FunctionCallError(FunctionCallError::MethodResolveError(
                    MethodResolveError::MethodEmptyName
                )))
            )
        );
    });
}

#[test]
fn test_invalid_utf8() {
    with_vm_variants(|vm_kind: VMKind| {
        assert_eq!(
            make_simple_contract_call_vm(&simple_contract(), &[255u8], vm_kind),
            (
                None,
                Some(VMError::FunctionCallError(FunctionCallError::MethodResolveError(
                    MethodResolveError::MethodUTF8Error
                )))
            )
        );
    });
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
    // See the comment is test_stack_overflow.
    assert_eq!(
        make_simple_contract_call_vm(&trap_contract(), b"hello", VMKind::Wasmer),
        (
            Some(vm_outcome_with_gas(47105334)),
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
    // See the comment is test_stack_overflow.
    assert_eq!(
        make_simple_contract_call_vm(&trap_initializer(), b"hello", VMKind::Wasmer),
        (
            Some(vm_outcome_with_gas(47755584)),
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
    with_vm_variants(|vm_kind: VMKind| {
        assert_eq!(
            make_simple_contract_call_vm(&wrong_signature_contract(), b"hello", vm_kind),
            (
                None,
                Some(VMError::FunctionCallError(FunctionCallError::MethodResolveError(
                    MethodResolveError::MethodInvalidSignature
                )))
            )
        );
    });
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
    with_vm_variants(|vm_kind: VMKind| {
        assert_eq!(
            make_simple_contract_call_vm(&export_wrong_type(), b"hello", vm_kind),
            (
                None,
                Some(VMError::FunctionCallError(FunctionCallError::MethodResolveError(
                    MethodResolveError::MethodNotFound
                )))
            )
        );
    });
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
    with_vm_variants(|vm_kind: VMKind| {
        assert_eq!(
            make_simple_contract_call_vm(&guest_panic(), b"hello", vm_kind),
            (
                Some(vm_outcome_with_gas(315341445)),
                Some(VMError::FunctionCallError(FunctionCallError::HostError(
                    HostError::GuestPanic { panic_msg: "explicit guest panic".to_string() }
                )))
            )
        );
    });
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
    // We only test trapping tests on Wasmer, as of version 0.17, when tests executed in parallel,
    // Wasmer signal handlers may catch signals thrown from the Wasmtime, and produce fake failing tests.
    assert_eq!(
        make_simple_contract_call_vm(&stack_overflow(), b"hello", VMKind::Wasmer),
        (
            Some(vm_outcome_with_gas(63226248177)),
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
    with_vm_variants(|vm_kind: VMKind| {
        assert_eq!(
            make_simple_contract_call_vm(&memory_grow(), b"hello", vm_kind),
            (
                Some(vm_outcome_with_gas(100000000000000)),
                Some(VMError::FunctionCallError(FunctionCallError::HostError(
                    HostError::GasExceeded
                )))
            )
        );
    });
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
    with_vm_variants(|vm_kind: VMKind| {
        assert_eq!(
            make_simple_contract_call_vm(&bad_import_global("wtf"), b"hello", vm_kind),
            (
                None,
                Some(VMError::FunctionCallError(FunctionCallError::CompilationError(
                    CompilationError::PrepareError(PrepareError::Instantiate)
                )))
            )
        );
    });
}

#[test]
fn test_bad_import_2() {
    with_vm_variants(|vm_kind: VMKind| {
        assert_eq!(
            make_simple_contract_call_vm(&bad_import_func("wtf"), b"hello", vm_kind),
            (
                None,
                Some(VMError::FunctionCallError(FunctionCallError::CompilationError(
                    CompilationError::PrepareError(PrepareError::Instantiate)
                )))
            )
        );
    });
}

#[test]
fn test_bad_import_3() {
    with_vm_variants(|vm_kind: VMKind| {
        let msg = match vm_kind {
            VMKind::Wasmer => "link error: Incorrect import type, namespace: env, name: input, expected type: global, found type: function",
            VMKind::Wasmtime => "\"incompatible import type for `env::input` specified\\ndesired signature was: Global(GlobalType { content: I32, mutability: Const })\\nsignatures available:\\n\\n  * Func(FuncType { params: [I64], results: [] })\\n\"",
        }.to_string();
        assert_eq!(
            make_simple_contract_call_vm(&bad_import_global("env"), b"hello", vm_kind),
            (
                Some(vm_outcome_with_gas(46500213)),
                Some(VMError::FunctionCallError(FunctionCallError::LinkError { msg: msg }))
            )
        );
    });
}

#[test]
fn test_bad_import_4() {
    with_vm_variants(|vm_kind: VMKind| {
        let msg = match vm_kind {
            VMKind::Wasmer => "link error: Import not found, namespace: env, name: wtf",
            VMKind::Wasmtime => "\"unknown import: `env::wtf` has not been defined\"",
        }
        .to_string();
        assert_eq!(
            make_simple_contract_call_vm(&bad_import_func("env"), b"hello", vm_kind),
            (
                Some(vm_outcome_with_gas(45849963)),
                Some(VMError::FunctionCallError(FunctionCallError::LinkError { msg: msg }))
            )
        );
    });
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
    with_vm_variants(|vm_kind: VMKind| {
        assert_eq!(
            make_simple_contract_call_with_gas_vm(
                &some_initializer_contract(),
                b"hello",
                0,
                vm_kind
            ),
            (
                Some(vm_outcome_with_gas(0)),
                Some(VMError::FunctionCallError(FunctionCallError::HostError(
                    HostError::GasExceeded
                )))
            )
        );
    });
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
    with_vm_variants(|vm_kind: VMKind| {
        let result = make_simple_contract_call_vm(&bad_many_imports(), b"hello", vm_kind);
        assert_eq!(result.0, Some(vm_outcome_with_gas(299664213)));
        if let Some(VMError::FunctionCallError(FunctionCallError::LinkError { msg })) = result.1 {
            eprintln!("{}", msg);
            assert!(msg.len() < 1000, format!("Huge error message: {}", msg.len()));
        } else {
            panic!(result.1);
        }
    });
}

fn external_call_contract() -> Vec<u8> {
    wabt::wat2wasm(
        r#"
            (module
              (import "env" "prepaid_gas" (func (;0;) (result i64)))
              (export "hello" (func 1))
              (func (;1;)
                  (drop (call 0))
                  )
            )"#,
    )
    .unwrap()
}

#[test]
fn test_external_call_ok() {
    with_vm_variants(|vm_kind: VMKind| {
        assert_eq!(
            make_simple_contract_call_vm(&external_call_contract(), b"hello", vm_kind),
            (Some(vm_outcome_with_gas(321582066)), None)
        );
    });
}

#[test]
fn test_external_call_error() {
    with_vm_variants(|vm_kind: VMKind| {
        assert_eq!(
            make_simple_contract_call_with_gas_vm(
                &external_call_contract(),
                b"hello",
                100,
                vm_kind
            ),
            (
                Some(vm_outcome_with_gas(100)),
                Some(VMError::FunctionCallError(FunctionCallError::HostError(
                    HostError::GasExceeded
                )))
            )
        );
    });
}
