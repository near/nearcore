//! This file is mainly to assure specific issues are working well
use anyhow::Result;
use near_vm_test_api::*;

#[derive(Clone)]
struct Env {
    memory: LazyInit<Memory>,
}

impl WasmerEnv for Env {}

/// Corruption of WasmerEnv when using call indirect.
///
/// Note: this one is specific to Singlepass, but we want to test in all
/// available compilers.
///
/// https://github.com/wasmerio/wasmer/issues/2329
#[compiler_test(issues)]
fn issue_2329(mut config: crate::Config) -> Result<()> {
    let store = config.store();

    fn read_memory(env: &Env, guest_ptr: u32) -> u32 {
        dbg!(env.memory.get_ref());
        dbg!(guest_ptr);
        0
    }

    let wat = r#"
    (module
        (type (;0;) (func (param i32) (result i32)))
        (type (;1;) (func))
        (type (;2;) (func (param i32 i32) (result i32)))
        (import "env" "__read_memory" (func $__read_memory (type 0)))
        (func $read_memory (type 1)
          (drop
            (call $_ZN5other8dispatch17h053cb34ef5d0d7b0E
              (i32.const 1)
              (i32.const 2)))
          (drop
            (call $__read_memory
              (i32.const 1))))
        (func $_ZN5other8dispatch17h053cb34ef5d0d7b0E (type 2) (param i32 i32) (result i32)
          (call_indirect (type 0)
            (local.get 1)
            (local.get 0)))
        (table (;0;) 2 2 funcref)
        (memory (;0;) 16)
        (global (;0;) (mut i32) (i32.const 1048576))
        (global (;1;) i32 (i32.const 1048576))
        (global (;2;) i32 (i32.const 1048576))
        (export "memory" (memory 0))
        (export "read_memory" (func $read_memory))
        (export "__data_end" (global 1))
        (export "__heap_base" (global 2))
        (elem (;0;) (i32.const 1) func $__read_memory))
    "#;
    let module = Module::new(&store, wat)?;
    let env = Env { memory: LazyInit::new() };
    let imports: ImportObject = imports! {
        "env" => {
            "__read_memory" => Function::new_native_with_env(
                &store,
                env,
                read_memory
            ),
        }
    };
    let instance =
        Instance::new_with_config(&module, InstanceConfig::with_stack_limit(1000000), &imports)?;
    instance.lookup_function("read_memory").unwrap().call(&[])?;
    Ok(())
}

/// Exhaustion of GPRs when calling a function with many floating point arguments
///
/// Note: this one is specific to Singlepass, but we want to test in all
/// available compilers.
#[compiler_test(issues)]
fn regression_gpr_exhaustion_for_calls(mut config: crate::Config) -> Result<()> {
    let store = config.store();
    let wat = r#"
        (module
          (type (;0;) (func (param f64) (result i32)))
          (type (;1;) (func (param f64 f64 f64 f64 f64 f64)))
          (func (;0;) (type 0) (param f64) (result i32)
            local.get 0
            local.get 0
            local.get 0
            local.get 0
            f64.const 0
            f64.const 0
            f64.const 0
            f64.const 0
            f64.const 0
            f64.const 0
            f64.const 0
            i32.const 0
            call_indirect (type 0)
            call_indirect (type 1)
            drop
            drop
            drop
            drop
            i32.const 0)
          (table (;0;) 1 1 funcref))
    "#;
    let module = Module::new(&store, wat)?;
    let imports: ImportObject = imports! {};
    let instance =
        Instance::new_with_config(&module, InstanceConfig::with_stack_limit(1000000), &imports)?;
    Ok(())
}
