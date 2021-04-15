use near_vm_errors::{
    CompilationError, FunctionCallError, HostError, MethodResolveError, PrepareError, VMError,
    WasmTrap,
};
use near_vm_logic::{ReturnData, VMOutcome};

use crate::cache::MockCompiledContractCache;
use crate::tests::{
    make_cached_contract_call_vm, make_simple_contract_call_vm,
    make_simple_contract_call_with_gas_vm, with_vm_variants,
};
use crate::VMKind;

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
            make_simple_contract_call_vm(&infinite_initializer_contract(), "hello", vm_kind),
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
            make_simple_contract_call_vm(&infinite_initializer_contract(), "hello2", vm_kind),
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
            make_simple_contract_call_vm(&simple_contract(), "hello", vm_kind),
            (Some(vm_outcome_with_gas(43032213)), None),
        );
    });
}

#[test]
fn test_export_not_found() {
    with_vm_variants(|vm_kind: VMKind| {
        assert_eq!(
            make_simple_contract_call_vm(&simple_contract(), "hello2", vm_kind),
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
            make_simple_contract_call_vm(&simple_contract(), "", vm_kind),
            (
                None,
                Some(VMError::FunctionCallError(FunctionCallError::MethodResolveError(
                    MethodResolveError::MethodEmptyName
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
    with_vm_variants(|vm_kind: VMKind| {
        match vm_kind {
            VMKind::Wasmer0 | VMKind::Wasmer1 => {}
            // All contracts leading to hardware traps can not run concurrently on Wasmtime and Wasmer,
            // Restore, once get rid of Wasmer 0.x.
            VMKind::Wasmtime => return,
        }
        assert_eq!(
            make_simple_contract_call_vm(&trap_contract(), "hello", vm_kind),
            (
                Some(vm_outcome_with_gas(47105334)),
                Some(VMError::FunctionCallError(FunctionCallError::WasmTrap(
                    WasmTrap::Unreachable
                )))
            )
        )
    })
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
    with_vm_variants(|vm_kind: VMKind| {
        match vm_kind {
            VMKind::Wasmer0 | VMKind::Wasmer1 => {}
            // All contracts leading to hardware traps can not run concurrently on Wasmtime and Wasmer,
            // Check if can restore, once get rid of Wasmer 0.x.
            VMKind::Wasmtime => return,
        }
        assert_eq!(
            make_simple_contract_call_vm(&trap_initializer(), "hello", vm_kind),
            (
                Some(vm_outcome_with_gas(47755584)),
                Some(VMError::FunctionCallError(FunctionCallError::WasmTrap(
                    WasmTrap::Unreachable
                )))
            )
        );
    });
}

fn div_by_zero_contract() -> Vec<u8> {
    wabt::wat2wasm(
        r#"
            (module
              (type (;0;) (func))
              (func (;0;) (type 0)
                i32.const 1
                i32.const 0
                i32.div_s
                return
              )
              (export "hello" (func 0))
            )"#,
    )
    .unwrap()
}

#[test]
fn test_div_by_zero_contract() {
    with_vm_variants(|vm_kind: VMKind| {
        match vm_kind {
            VMKind::Wasmer0 | VMKind::Wasmer1 => {}
            // All contracts leading to hardware traps can not run concurrently on Wasmtime and Wasmer,
            // Check if can restore, once get rid of Wasmer 0.x.
            VMKind::Wasmtime => return,
        }
        assert_eq!(
            make_simple_contract_call_vm(&div_by_zero_contract(), "hello", vm_kind),
            (
                Some(vm_outcome_with_gas(59758197)),
                Some(VMError::FunctionCallError(FunctionCallError::WasmTrap(
                    WasmTrap::IllegalArithmetic
                )))
            )
        )
    })
}

fn indirect_call_to_null_contract() -> Vec<u8> {
    wabt::wat2wasm(
        r#"
            (module
              (type (;0;) (func))
              (table (;0;) 2 funcref)
              (func (;0;) (type 0)
                i32.const 1
                call_indirect (type 0)
                return
              )
              (export "hello" (func 0))
            )"#,
    )
    .unwrap()
}

#[test]
fn test_indirect_call_to_null_contract() {
    with_vm_variants(|vm_kind: VMKind| {
        match vm_kind {
            VMKind::Wasmer1 => {}
            // Wasmer 0.x cannot distinguish indirect calls to null and calls with incorrect signature.
            VMKind::Wasmer0 => return,
            // All contracts leading to hardware traps can not run concurrently on Wasmtime and Wasmer,
            // Check if can restore, once get rid of Wasmer 0.x.
            VMKind::Wasmtime => return,
        }
        assert_eq!(
            make_simple_contract_call_vm(&indirect_call_to_null_contract(), "hello", vm_kind),
            (
                Some(vm_outcome_with_gas(57202326)),
                Some(VMError::FunctionCallError(FunctionCallError::WasmTrap(
                    WasmTrap::IndirectCallToNull
                )))
            )
        )
    })
}

fn indirect_call_to_wrong_signature_contract() -> Vec<u8> {
    wabt::wat2wasm(
        r#"
            (module
              (type (;0;) (func))
              (type (;1;) (func (result i32)))
              (func (;0;) (type 0)
                i32.const 1
                call_indirect (type 1)
                return
              )
              (func (;1;) (type 1)
                i32.const 0
                return
              )
              (table (;0;) 3 3 funcref)
              (elem (;0;) (i32.const 1) 0 1)
              (export "hello" (func 0))
            )"#,
    )
    .unwrap()
}

#[test]
fn test_indirect_call_to_wrong_signature_contract() {
    with_vm_variants(|vm_kind: VMKind| {
        match vm_kind {
            VMKind::Wasmer0 | VMKind::Wasmer1 => {}
            // All contracts leading to hardware traps can not run concurrently on Wasmtime and Wasmer,
            // Check if can restore, once get rid of Wasmer 0.x.
            VMKind::Wasmtime => return,
        }
        assert_eq!(
            make_simple_contract_call_vm(
                &indirect_call_to_wrong_signature_contract(),
                "hello",
                vm_kind
            ),
            (
                Some(vm_outcome_with_gas(61970826)),
                Some(VMError::FunctionCallError(FunctionCallError::WasmTrap(
                    WasmTrap::IncorrectCallIndirectSignature
                )))
            )
        )
    })
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
            make_simple_contract_call_vm(&wrong_signature_contract(), "hello", vm_kind),
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
            make_simple_contract_call_vm(&export_wrong_type(), "hello", vm_kind),
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
            make_simple_contract_call_vm(&guest_panic(), "hello", vm_kind),
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
    with_vm_variants(|vm_kind: VMKind| {
        // We only test trapping tests on Wasmer, as of version 0.17, when tests executed in parallel,
        // Wasmer signal handlers may catch signals thrown from the Wasmtime, and produce fake failing tests.
        match vm_kind {
            VMKind::Wasmer0 | VMKind::Wasmer1 => assert_eq!(
                make_simple_contract_call_vm(&stack_overflow(), "hello", vm_kind),
                (
                    Some(vm_outcome_with_gas(63226248177)),
                    Some(VMError::FunctionCallError(FunctionCallError::WasmTrap(
                        WasmTrap::Unreachable
                    )))
                )
            ),
            VMKind::Wasmtime => {}
        }
    });
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
            make_simple_contract_call_vm(&memory_grow(), "hello", vm_kind),
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
            make_simple_contract_call_vm(&bad_import_global("wtf"), "hello", vm_kind),
            (
                None,
                Some(VMError::FunctionCallError(FunctionCallError::CompilationError(
                    CompilationError::PrepareError(PrepareError::Instantiate)
                )))
            )
        )
    });
}

#[test]
fn test_bad_import_2() {
    with_vm_variants(|vm_kind: VMKind| {
        assert_eq!(
            make_simple_contract_call_vm(&bad_import_func("wtf"), "hello", vm_kind),
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
            VMKind::Wasmer0 => "link error: Incorrect import type, namespace: env, name: input, expected type: global, found type: function",
            VMKind::Wasmtime => "\"incompatible import type for `env::input` specified\\ndesired signature was: Global(GlobalType { content: I32, mutability: Const })\\nsignatures available:\\n\\n  * Func(FuncType { sig: WasmFuncType { params: [I64], returns: [] } })\\n\"",
            VMKind::Wasmer1 => "Error while importing \"env\".\"input\": incompatible import type. Expected Global(GlobalType { ty: I32, mutability: Const }) but received Function(FunctionType { params: [I64], results: [] })",
        }.to_string();
        assert_eq!(
            make_simple_contract_call_vm(&bad_import_global("env"), "hello", vm_kind),
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
            VMKind::Wasmer0 => "link error: Import not found, namespace: env, name: wtf",
            VMKind::Wasmtime => "\"unknown import: `env::wtf` has not been defined\"",
            VMKind::Wasmer1 => "Error while importing \"env\".\"wtf\": unknown import. Expected Function(FunctionType { params: [], results: [] })",
        }
        .to_string();
        assert_eq!(
            make_simple_contract_call_vm(&bad_import_func("env"), "hello", vm_kind),
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
                "hello",
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
        let result = make_simple_contract_call_vm(&bad_many_imports(), "hello", vm_kind);
        assert_eq!(result.0, Some(vm_outcome_with_gas(299664213)));
        if let Some(VMError::FunctionCallError(FunctionCallError::LinkError { msg })) = result.1 {
            eprintln!("{}", msg);
            assert!(msg.len() < 1000, "Huge error message: {}", msg.len());
        } else {
            panic!("{:?}", result.1);
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
            make_simple_contract_call_vm(&external_call_contract(), "hello", vm_kind),
            (Some(vm_outcome_with_gas(321582066)), None)
        );
    });
}

#[test]
fn test_external_call_error() {
    with_vm_variants(|vm_kind: VMKind| {
        assert_eq!(
            make_simple_contract_call_with_gas_vm(&external_call_contract(), "hello", 100, vm_kind),
            (
                Some(vm_outcome_with_gas(100)),
                Some(VMError::FunctionCallError(FunctionCallError::HostError(
                    HostError::GasExceeded
                )))
            )
        );
    });
}

#[test]
fn test_contract_error_caching() {
    with_vm_variants(|vm_kind: VMKind| {
        match vm_kind {
            VMKind::Wasmer0 | VMKind::Wasmer1 => {}
            VMKind::Wasmtime => return,
        }
        let mut cache = MockCompiledContractCache::default();
        let code = [42; 1000];
        let terragas = 1000000000000u64;
        assert_eq!(cache.len(), 0);
        let err1 =
            make_cached_contract_call_vm(&mut cache, &code, "method_name1", terragas, vm_kind);
        println!("{:?}", cache);
        assert_eq!(cache.len(), 1);
        let err2 =
            make_cached_contract_call_vm(&mut cache, &code, "method_name2", terragas, vm_kind);
        assert_eq!(err1, err2);
    })
}
