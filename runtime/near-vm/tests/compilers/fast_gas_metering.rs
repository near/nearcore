use std::ptr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use wasmer::*;
use wasmer_compiler_singlepass::Singlepass;
use wasmer_engine_universal::Universal;
use wasmer_types::{FastGasCounter, InstanceConfig};

fn get_module_with_start(store: &Store) -> Module {
    let wat = r#"
        (import "host" "func" (func))
        (import "host" "gas" (func (param i32)))
        (memory $mem 1)
        (export "memory" (memory $mem))
        (export "bar" (func $bar))
        (func $foo
            call 0
            i32.const 42
            call 1
            call 0
            i32.const 100
            call 1
            call 0
        )
        (func $bar
            call 0
            i32.const 100
            call 1
        )
        (start $foo)
    "#;

    Module::new(&store, &wat).unwrap()
}

fn get_module(store: &Store) -> Module {
    let wat = r#"
        (import "host" "func" (func))
        (import "host" "has" (func (param i32)))
        (import "host" "gas" (func (param i32)))
        (memory $mem 1)
        (export "memory" (memory $mem))
        (func (export "foo")
            call 0
            i32.const 442
            call 1
            i32.const 42
            call 2
            call 0
            i32.const 100
            call 2
            call 0
        )
        (func (export "bar") (local i32 i32)
            call 0
            i32.const 100
            call 2
        )
        (func (export "zoo")
            loop
                i32.const 1_000_000_000
                call 2
                br 0
            end
        )
    "#;

    Module::new(&store, &wat).unwrap()
}

fn get_module_tricky_arg(store: &Store) -> Module {
    let wat = r#"
        (import "host" "func" (func))
        (import "host" "gas" (func (param i32)))
        (memory $mem 1)
        (export "memory" (memory $mem))
        (func $get_gas (param i32) (result i32)
         i32.const 1
         get_local 0
         i32.add)
        (func (export "foo")
            i32.const 1000000000
            call $get_gas
            call 1
        )
        (func (export "zoo")
            i32.const -2
            call 1
        )
    "#;

    Module::new(&store, &wat).unwrap()
}

fn get_store(regular_op_cost: u64) -> Store {
    let compiler = Singlepass::default();
    let engine = Universal::new(compiler).engine();
    let mut tunables = BaseTunables::for_target(engine.target());
    tunables.set_regular_op_cost(regular_op_cost);
    let store = Store::new_with_tunables(&engine, tunables);
    store
}

#[test]
fn test_gas_intrinsic_in_start() {
    let store = get_store(0);
    let mut gas_counter = FastGasCounter::new(100);
    let module = get_module_with_start(&store);
    static HITS: AtomicUsize = AtomicUsize::new(0);
    let result = Instance::new_with_config(
        &module,
        unsafe { InstanceConfig::default().with_counter(ptr::addr_of_mut!(gas_counter)) },
        &imports! {
            "host" => {
                "func" => Function::new(&store, FunctionType::new(vec![], vec![]), |_values| {
                    HITS.fetch_add(1, SeqCst);
                    Ok(vec![])
                }),
                "gas" => Function::new(&store, FunctionType::new(vec![ValType::I32], vec![]), |_| {
                    // It shall be never called, as call is intrinsified.
                    assert!(false);
                    Ok(vec![])
                }),
            },
        },
    );
    assert!(result.is_err());
    match result {
        Err(InstantiationError::Start(runtime_error)) => {
            assert_eq!(runtime_error.message(), "gas limit exceeded")
        }
        _ => assert!(false),
    }
    // Ensure "func" was called twice.
    assert_eq!(HITS.swap(0, SeqCst), 2);
    // Ensure gas was partially spent.
    assert_eq!(gas_counter.burnt(), 142);
    assert_eq!(gas_counter.gas_limit, 100);
}

fn test_gas_regular(opcode_cost: u64) {
    let store = get_store(opcode_cost);
    let mut gas_counter = FastGasCounter::new(200 + 11 * opcode_cost);
    let module = get_module(&store);
    let hits = std::sync::Arc::new(AtomicUsize::new(0));
    let instance = Instance::new_with_config(
        &module,
        unsafe { InstanceConfig::default().with_counter(ptr::addr_of_mut!(gas_counter)) },
        &imports! {
            "host" => {
                "func" => Function::new(&store, FunctionType::new(vec![], vec![]), {
                    let hits = hits.clone();
                    move |_values| {
                        hits.fetch_add(1, SeqCst);
                        Ok(vec![])
                    }
                }),
                "has" => Function::new(&store, FunctionType::new(vec![ValType::I32], vec![]), {
                    let hits = hits.clone();
                    move |_| {
                        hits.fetch_add(1, SeqCst);
                        Ok(vec![])
                    }
                }),
                "gas" => Function::new(&store, FunctionType::new(vec![ValType::I32], vec![]), |_| {
                    // It shall be never called, as call is intrinsified.
                    assert!(false);
                    Ok(vec![])
                }),
            },
        },
    );
    assert!(instance.is_ok());
    let instance = instance.unwrap();
    let foo_func = instance
        .lookup_function("foo")
        .expect("expected function foo");
    let bar_func = instance
        .lookup_function("bar")
        .expect("expected function bar");
    let zoo_func = instance
        .lookup_function("zoo")
        .expect("expected function zoo");
    // Ensure "func" was not called.
    assert_eq!(hits.load(SeqCst), 0);
    let e = bar_func.call(&[]);
    assert!(e.is_ok());
    // Ensure "func" was called.
    assert_eq!(hits.load(SeqCst), 1);
    assert_eq!(gas_counter.burnt(), 100 + 3 * opcode_cost);
    let _e = foo_func.call(&[]).err().expect("error calling function");
    // Ensure "func" and "has" was called again.
    assert_eq!(hits.load(SeqCst), 4);
    assert_eq!(gas_counter.burnt(), 242 + 11 * opcode_cost);
    // Finally try to exhaust rather large limit
    if opcode_cost == 0 {
        gas_counter.gas_limit = 1_000_000_000_000_000;
        let _e = zoo_func.call(&[]).err().expect("error calling function");
        assert_eq!(gas_counter.burnt(), 1_000_000_000_000_242);
    }
}

#[test]
fn test_gas_intrinsic_regular() {
    test_gas_regular(0);
}

#[test]
fn test_gas_accounting_regular() {
    test_gas_regular(3);
}

#[test]
fn test_gas_intrinsic_default() {
    let store = get_store(0);
    let module = get_module(&store);
    static HITS: AtomicUsize = AtomicUsize::new(0);
    let instance = Instance::new(
        &module,
        &imports! {
            "host" => {
                "func" => Function::new(&store, FunctionType::new(vec![], vec![]), |_values| {
                    HITS.fetch_add(1, SeqCst);
                    Ok(vec![])
                }),
                "has" => Function::new(&store, FunctionType::new(vec![ValType::I32], vec![]), |_| {
                    HITS.fetch_add(1, SeqCst);
                    Ok(vec![])
                }),
                "gas" => Function::new(&store, FunctionType::new(vec![ValType::I32], vec![]), |_| {
                    // It shall be never called, as call is intrinsified.
                    assert!(false);
                    Ok(vec![])
                }),
            },
        },
    );
    assert!(instance.is_ok());
    let instance = instance.unwrap();
    let foo_func = instance
        .lookup_function("foo")
        .expect("expected function foo");
    let bar_func = instance
        .lookup_function("bar")
        .expect("expected function bar");
    // Ensure "func" was called.
    assert_eq!(HITS.load(SeqCst), 0);
    let e = bar_func.call(&[]);
    assert!(e.is_ok());
    // Ensure "func" was called.
    assert_eq!(HITS.load(SeqCst), 1);
    let _e = foo_func.call(&[]);
    // Ensure "func" and "has" was called.
    assert_eq!(HITS.load(SeqCst), 5);
}

#[test]
fn test_gas_intrinsic_tricky() {
    let store = get_store(0);
    let module = get_module_tricky_arg(&store);
    static BURNT_GAS: AtomicUsize = AtomicUsize::new(0);
    static HITS: AtomicUsize = AtomicUsize::new(0);
    let instance = Instance::new(
        &module,
        &imports! {
            "host" => {
                "func" => Function::new(&store, FunctionType::new(vec![], vec![]), |_values| {
                    HITS.fetch_add(1, SeqCst);
                    Ok(vec![])
                }),
                "gas" => Function::new(&store, FunctionType::new(vec![ValType::I32], vec![]), |arg| {
                    // It shall be called, as tricky call is not intrinsified.
                    HITS.fetch_add(1, SeqCst);
                    match arg[0] {
                        Value::I32(arg) => {
                            BURNT_GAS.fetch_add(arg as usize, SeqCst);
                        },
                        _ => {
                            assert!(false)
                        }
                    }
                    Ok(vec![])
                }),
            },
        },
    );
    assert!(instance.is_ok());
    let instance = instance.unwrap();
    let foo_func = instance
        .lookup_function("foo")
        .expect("expected function foo");

    let _e = foo_func.call(&[]);

    assert_eq!(BURNT_GAS.load(SeqCst), 1000000001);
    // Ensure "gas" was called.
    assert_eq!(HITS.load(SeqCst), 1);

    let zoo_func = instance
        .lookup_function("zoo")
        .expect("expected function zoo");

    let _e = zoo_func.call(&[]);
    // We decremented gas by two.
    assert_eq!(BURNT_GAS.load(SeqCst), 999999999);
    // Ensure "gas" was called.
    assert_eq!(HITS.load(SeqCst), 2);
}
