//! Testing the imports with different provided functions.
//! This tests checks that the provided functions (both native and
//! dynamic ones) work properly.

use anyhow::Result;
use near_vm_engine::RuntimeError;
use near_vm_test_api::*;
use std::convert::Infallible;
use std::sync::atomic::AtomicBool;
use std::sync::{
    atomic::{AtomicUsize, Ordering::SeqCst},
    Arc,
};

fn get_module(store: &Store) -> Result<Module> {
    let wat = r#"
        (import "host" "0" (func))
        (import "host" "1" (func (param i32) (result i32)))
        (import "host" "2" (func (param i32) (param i64)))
        (import "host" "3" (func (param i32 i64 i32 f32 f64)))
        (memory $mem 1)
        (export "memory" (memory $mem))

        (func $foo
            call 0
            i32.const 0
            call 1
            i32.const 1
            i32.add
            i64.const 3
            call 2

            i32.const 100
            i64.const 200
            i32.const 300
            f32.const 400
            f64.const 500
            call 3
        )
        (start $foo)
    "#;

    let module = Module::new(&store, &wat)?;
    Ok(module)
}

#[compiler_test(imports)]
#[serial_test::serial(dynamic_function)]
fn dynamic_function(config: crate::Config) -> Result<()> {
    let store = config.store();
    let module = get_module(&store)?;
    static HITS: AtomicUsize = AtomicUsize::new(0);
    Instance::new_with_config(
        &module,
        InstanceConfig::with_stack_limit(1000000),
        &imports! {
            "host" => {
                "0" => Function::new(&store, FunctionType::new(vec![], vec![]), |_values| {
                    assert_eq!(HITS.fetch_add(1, SeqCst), 0);
                    Ok(vec![])
                }),
                "1" => Function::new(&store, FunctionType::new(vec![ValType::I32], vec![ValType::I32]), |values| {
                    assert_eq!(values[0], Value::I32(0));
                    assert_eq!(HITS.fetch_add(1, SeqCst), 1);
                    Ok(vec![Value::I32(1)])
                }),
                "2" => Function::new(&store, FunctionType::new(vec![ValType::I32, ValType::I64], vec![]), |values| {
                    assert_eq!(values[0], Value::I32(2));
                    assert_eq!(values[1], Value::I64(3));
                    assert_eq!(HITS.fetch_add(1, SeqCst), 2);
                    Ok(vec![])
                }),
                "3" => Function::new(&store, FunctionType::new(vec![ValType::I32, ValType::I64, ValType::I32, ValType::F32, ValType::F64], vec![]), |values| {
                    assert_eq!(values[0], Value::I32(100));
                    assert_eq!(values[1], Value::I64(200));
                    assert_eq!(values[2], Value::I32(300));
                    assert_eq!(values[3], Value::F32(400.0));
                    assert_eq!(values[4], Value::F64(500.0));
                    assert_eq!(HITS.fetch_add(1, SeqCst), 3);
                    Ok(vec![])
                }),
            },
        },
    )?;
    assert_eq!(HITS.swap(0, SeqCst), 4);
    Ok(())
}

#[compiler_test(imports)]
fn dynamic_function_with_env(config: crate::Config) -> Result<()> {
    let store = config.store();
    let module = get_module(&store)?;

    #[derive(Clone)]
    struct Env {
        counter: Arc<AtomicUsize>,
    }
    impl WasmerEnv for Env {}

    impl std::ops::Deref for Env {
        type Target = Arc<AtomicUsize>;
        fn deref(&self) -> &Self::Target {
            &self.counter
        }
    }

    let env: Env = Env { counter: Arc::new(AtomicUsize::new(0)) };
    Instance::new_with_config(
        &module,
        InstanceConfig::with_stack_limit(1000000),
        &imports! {
            "host" => {
                "0" => Function::new_with_env(&store, FunctionType::new(vec![], vec![]), env.clone(), |env, _values| {
                    assert_eq!(env.fetch_add(1, SeqCst), 0);
                    Ok(vec![])
                }),
                "1" => Function::new_with_env(&store, FunctionType::new(vec![ValType::I32], vec![ValType::I32]), env.clone(), |env, values| {
                    assert_eq!(values[0], Value::I32(0));
                    assert_eq!(env.fetch_add(1, SeqCst), 1);
                    Ok(vec![Value::I32(1)])
                }),
                "2" => Function::new_with_env(&store, FunctionType::new(vec![ValType::I32, ValType::I64], vec![]), env.clone(), |env, values| {
                    assert_eq!(values[0], Value::I32(2));
                    assert_eq!(values[1], Value::I64(3));
                    assert_eq!(env.fetch_add(1, SeqCst), 2);
                    Ok(vec![])
                }),
                "3" => Function::new_with_env(&store, FunctionType::new(vec![ValType::I32, ValType::I64, ValType::I32, ValType::F32, ValType::F64], vec![]), env.clone(), |env, values| {
                    assert_eq!(values[0], Value::I32(100));
                    assert_eq!(values[1], Value::I64(200));
                    assert_eq!(values[2], Value::I32(300));
                    assert_eq!(values[3], Value::F32(400.0));
                    assert_eq!(values[4], Value::F64(500.0));
                    assert_eq!(env.fetch_add(1, SeqCst), 3);
                    Ok(vec![])
                }),
            },
        },
    )?;
    assert_eq!(env.load(SeqCst), 4);
    Ok(())
}

#[compiler_test(imports)]
#[serial_test::serial(static_function)]
fn static_function(config: crate::Config) -> Result<()> {
    let store = config.store();
    let module = get_module(&store)?;

    static HITS: AtomicUsize = AtomicUsize::new(0);
    Instance::new_with_config(
        &module,
        InstanceConfig::with_stack_limit(1000000),
        &imports! {
            "host" => {
                "0" => Function::new_native(&store, || {
                    assert_eq!(HITS.fetch_add(1, SeqCst), 0);
                }),
                "1" => Function::new_native(&store, |x: i32| -> i32 {
                    assert_eq!(x, 0);
                    assert_eq!(HITS.fetch_add(1, SeqCst), 1);
                    1
                }),
                "2" => Function::new_native(&store, |x: i32, y: i64| {
                    assert_eq!(x, 2);
                    assert_eq!(y, 3);
                    assert_eq!(HITS.fetch_add(1, SeqCst), 2);
                }),
                "3" => Function::new_native(&store, |a: i32, b: i64, c: i32, d: f32, e: f64| {
                    assert_eq!(a, 100);
                    assert_eq!(b, 200);
                    assert_eq!(c, 300);
                    assert_eq!(d, 400.0);
                    assert_eq!(e, 500.0);
                    assert_eq!(HITS.fetch_add(1, SeqCst), 3);
                }),
            },
        },
    )?;
    assert_eq!(HITS.swap(0, SeqCst), 4);
    Ok(())
}

#[compiler_test(imports)]
#[serial_test::serial(static_function_with_results)]
fn static_function_with_results(config: crate::Config) -> Result<()> {
    let store = config.store();
    let module = get_module(&store)?;

    static HITS: AtomicUsize = AtomicUsize::new(0);
    Instance::new_with_config(
        &module,
        InstanceConfig::with_stack_limit(1000000),
        &imports! {
            "host" => {
                "0" => Function::new_native(&store, || {
                    assert_eq!(HITS.fetch_add(1, SeqCst), 0);
                }),
                "1" => Function::new_native(&store, |x: i32| -> Result<i32, Infallible> {
                    assert_eq!(x, 0);
                    assert_eq!(HITS.fetch_add(1, SeqCst), 1);
                    Ok(1)
                }),
                "2" => Function::new_native(&store, |x: i32, y: i64| {
                    assert_eq!(x, 2);
                    assert_eq!(y, 3);
                    assert_eq!(HITS.fetch_add(1, SeqCst), 2);
                }),
                "3" => Function::new_native(&store, |a: i32, b: i64, c: i32, d: f32, e: f64| {
                    assert_eq!(a, 100);
                    assert_eq!(b, 200);
                    assert_eq!(c, 300);
                    assert_eq!(d, 400.0);
                    assert_eq!(e, 500.0);
                    assert_eq!(HITS.fetch_add(1, SeqCst), 3);
                }),
            },
        },
    )?;
    assert_eq!(HITS.swap(0, SeqCst), 4);
    Ok(())
}

#[compiler_test(imports)]
fn static_function_with_env(config: crate::Config) -> Result<()> {
    let store = config.store();
    let module = get_module(&store)?;

    #[derive(Clone)]
    struct Env(Arc<AtomicUsize>);
    impl WasmerEnv for Env {}

    impl std::ops::Deref for Env {
        type Target = Arc<AtomicUsize>;
        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    let env: Env = Env(Arc::new(AtomicUsize::new(0)));
    Instance::new_with_config(
        &module,
        InstanceConfig::with_stack_limit(1000000),
        &imports! {
            "host" => {
                "0" => Function::new_native_with_env(&store, env.clone(), |env: &Env| {
                    assert_eq!(env.fetch_add(1, SeqCst), 0);
                }),
                "1" => Function::new_native_with_env(&store, env.clone(), |env: &Env, x: i32| -> i32 {
                    assert_eq!(x, 0);
                    assert_eq!(env.fetch_add(1, SeqCst), 1);
                    1
                }),
                "2" => Function::new_native_with_env(&store, env.clone(), |env: &Env, x: i32, y: i64| {
                    assert_eq!(x, 2);
                    assert_eq!(y, 3);
                    assert_eq!(env.fetch_add(1, SeqCst), 2);
                }),
                "3" => Function::new_native_with_env(&store, env.clone(), |env: &Env, a: i32, b: i64, c: i32, d: f32, e: f64| {
                    assert_eq!(a, 100);
                    assert_eq!(b, 200);
                    assert_eq!(c, 300);
                    assert_eq!(d, 400.0);
                    assert_eq!(e, 500.0);
                    assert_eq!(env.fetch_add(1, SeqCst), 3);
                }),
            },
        },
    )?;
    assert_eq!(env.load(SeqCst), 4);
    Ok(())
}

#[compiler_test(imports)]
fn static_function_that_fails(config: crate::Config) -> Result<()> {
    let store = config.store();
    let wat = r#"
        (import "host" "0" (func))

        (func $foo
            call 0
        )
        (start $foo)
    "#;

    let module = Module::new(&store, &wat)?;

    let result = Instance::new_with_config(
        &module,
        InstanceConfig::with_stack_limit(1000000),
        &imports! {
            "host" => {
                "0" => Function::new_native(&store, || -> Result<Infallible, RuntimeError> {
                    Err(RuntimeError::new("oops"))
                }),
            },
        },
    );

    assert!(result.is_err());

    match result {
        Err(InstantiationError::Start(runtime_error)) => {
            assert_eq!(runtime_error.message(), "oops")
        }
        _ => assert!(false),
    }

    Ok(())
}

fn get_module2(store: &Store) -> Result<Module> {
    let wat = r#"
        (import "host" "fn" (func))
        (memory $mem 1)
        (export "memory" (memory $mem))
        (export "main" (func $main))
        (func $main (param) (result)
          (call 0))
    "#;

    let module = Module::new(&store, &wat)?;
    Ok(module)
}

#[compiler_test(imports)]
fn dynamic_function_with_env_wasmer_env_init_works(config: crate::Config) -> Result<()> {
    let store = config.store();
    let module = get_module2(&store)?;

    #[allow(dead_code)]
    #[derive(Clone)]
    struct Env {
        memory: Memory,
    }
    impl WasmerEnv for Env {}

    let env: Env = Env {
        memory: Memory::new(
            &store,
            MemoryType { minimum: 0.into(), maximum: None, shared: false },
        )?,
    };
    let function_fn =
        Function::new_with_env(&store, FunctionType::new(vec![], vec![]), env, |env, _values| {
            Ok(vec![])
        });
    let instance = Instance::new_with_config(
        &module,
        InstanceConfig::with_stack_limit(1000000),
        &imports! {
            "host" => {
                "fn" => function_fn,
            },
        },
    )?;
    let f: NativeFunc<(), ()> = instance.get_native_function("main")?;
    f.call()?;
    Ok(())
}

static REGRESSION_IMPORT_TRAMPOLINES: &str = r#"(module
  (type (;0;) (func))
  (type (;1;) (func (param i32)))
  (import "env" "panic" (func (;0;) (type 0)))
  (import "env" "gas" (func (;1;) (type 1)))
  (export "panic" (func 0))
  (export "gas" (func 1))
)"#;

#[compiler_test(imports)]
fn regression_import_trampolines(config: crate::Config) -> Result<()> {
    let store = config.store();
    let module = Module::new(&store, &REGRESSION_IMPORT_TRAMPOLINES)?;
    let panic = Function::new_native(&store, || ());
    static GAS_CALLED: AtomicBool = AtomicBool::new(false);
    let gas = Function::new_native(&store, |p: i32| {
        GAS_CALLED.store(true, SeqCst);
        assert_eq!(p, 42)
    });
    let imports = imports! {
        "env" => {
            "panic" => panic,
            "gas" => gas,
        }
    };
    let instance =
        Instance::new_with_config(&module, InstanceConfig::with_stack_limit(1000000), &imports)?;
    let panic = instance.lookup_function("panic").unwrap();
    panic.call(&[])?;
    let gas = instance.lookup_function("gas").unwrap();
    gas.call(&[Value::I32(42)])?;
    assert_eq!(GAS_CALLED.load(SeqCst), true);
    Ok(())
}

// TODO(0-copy): no longer possible to get references to exported entities other than functions
//               (we don't need that functionality)
// #[compiler_test(imports)]
// fn multi_use_host_fn_manages_memory_correctly(config: crate::Config) -> Result<()> {
//     let store = config.store();
//     let module = get_module2(&store)?;
//
//     #[allow(dead_code)]
//     #[derive(Clone)]
//     struct Env {
//         memory: LazyInit<Memory>,
//     }
//
//     impl WasmerEnv for Env {
//         fn init_with_instance(&mut self, instance: &Instance) -> Result<(), HostEnvInitError> {
//             let memory = instance.exports.get_memory("memory")?.clone();
//             self.memory.initialize(memory);
//             Ok(())
//         }
//     }
//
//     let env: Env = Env {
//         memory: LazyInit::default(),
//     };
//     fn host_fn(env: &Env) {
//         assert!(env.memory.get_ref().is_some());
//         println!("Hello, world!");
//     }
//
//     let imports = imports! {
//         "host" => {
//             "fn" => Function::new_native_with_env(&store, env.clone(), host_fn),
//         },
//     };
//     let instance1 = Instance::new_with_config(&module, InstanceConfig::with_stack_limit(1000000), &imports)?;
//     let instance2 = Instance::new_with_config(&module, InstanceConfig::with_stack_limit(1000000), &imports)?;
//     {
//         let f1: NativeFunc<(), ()> = instance1.get_native_function("main")?;
//         f1.call()?;
//     }
//     drop(instance1);
//     {
//         let f2: NativeFunc<(), ()> = instance2.get_native_function("main")?;
//         f2.call()?;
//     }
//     drop(instance2);
//     Ok(())
// }
//
// #[compiler_test(imports)]
// fn instance_local_memory_lifetime(config: crate::Config) -> Result<()> {
//     let store = config.store();
//
//     let memory: Memory = {
//         let wat = r#"(module
//     (memory $mem 1)
//     (export "memory" (memory $mem))
// )"#;
//         let module = Module::new(&store, wat)?;
//         let instance = Instance::new_with_config(&module, InstanceConfig::with_stack_limit(1000000), &imports! {})?;
//         instance.exports.get_memory("memory")?.clone()
//     };
//
//     let wat = r#"(module
//     (import "env" "memory" (memory $mem 1) )
//     (func $get_at (type $get_at_t) (param $idx i32) (result i32)
//       (i32.load (local.get $idx)))
//     (type $get_at_t (func (param i32) (result i32)))
//     (type $set_at_t (func (param i32) (param i32)))
//     (func $set_at (type $set_at_t) (param $idx i32) (param $val i32)
//       (i32.store (local.get $idx) (local.get $val)))
//     (export "get_at" (func $get_at))
//     (export "set_at" (func $set_at))
// )"#;
//     let module = Module::new(&store, wat)?;
//     let imports = imports! {
//         "env" => {
//             "memory" => memory,
//         },
//     };
//     let instance = Instance::new_with_config(&module, InstanceConfig::with_stack_limit(1000000), &imports)?;
//     let set_at: NativeFunc<(i32, i32), ()> = instance.get_native_function("set_at")?;
//     let get_at: NativeFunc<i32, i32> = instance.get_native_function("get_at")?;
//     set_at.call(200, 123)?;
//     assert_eq!(get_at.call(200)?, 123);
//
//     Ok(())
// }
