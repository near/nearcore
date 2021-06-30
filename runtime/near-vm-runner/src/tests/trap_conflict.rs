use near_vm_errors::{
    CompilationError, FunctionCallError, HostError, MethodResolveError, PrepareError, VMError,
    WasmTrap,
};
use near_vm_logic::{ReturnData, VMOutcome};

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
}`

pub fn trap_contract() -> Vec<u8> {
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
            VMKind::Wasmer0 | VMKind::Wasmer2 => {}
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
