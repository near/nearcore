//! A Wasm module can import entities, like functions, memories,
//! globals and tables.
//!
//! In this example, we'll create a system for getting and adjusting a counter value. However, host
//! functions are not limited to storing data outside of Wasm, they're normal host functions and
//! can do anything that the host can do.
//!
//!   1. There will be a `get_counter` function that will return an i32 of
//!      the current global counter,
//!   2. There will be an `add_to_counter` function will add the passed
//!      i32 value to the counter, and return an i32 of the current
//!      global counter.
//!
//! You can run the example directly by executing in Wasmer root:
//!
//! ```shell
//! cargo run --example imported-function-env --release --features "singlepass"
//! ```
//!
//! Ready?

use std::sync::{Arc, Mutex};
use wasmer::{imports, wat2wasm, Function, Instance, Module, Store, WasmerEnv};
use wasmer_compiler_singlepass::Singlepass;
use wasmer_engine_universal::Universal;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Let's declare the Wasm module with the text representation.
    let wasm_bytes = wat2wasm(
        br#"
(module
  (func $get_counter (import "env" "get_counter") (result i32))
  (func $add_to_counter (import "env" "add_to_counter") (param i32) (result i32))

  (type $increment_t (func (param i32) (result i32)))
  (func $increment_f (type $increment_t) (param $x i32) (result i32)
    (block
      (loop
        (call $add_to_counter (i32.const 1))
        (set_local $x (i32.sub (get_local $x) (i32.const 1)))
        (br_if 1 (i32.eq (get_local $x) (i32.const 0)))
        (br 0)))
    call $get_counter)
  (export "increment_counter_loop" (func $increment_f)))
"#,
    )?;

    // Create a Store.
    // Note that we don't need to specify the engine/compiler if we want to use
    // the default provided by Wasmer.
    // You can use `Store::default()` for that.
    let store = Store::new(&Universal::new(Singlepass::default()).engine());

    println!("Compiling module...");
    // Let's compile the Wasm module.
    let module = Module::new(&store, wasm_bytes)?;

    // We create some shared data here, `Arc` is required because we may
    // move our WebAssembly instance to another thread to run it. Mutex
    // lets us get shared mutabilty which is fine because we know we won't
    // run host calls concurrently.  If concurrency is a possibilty, we'd have
    // to use a `Mutex`.
    let shared_counter: Arc<Mutex<i32>> = Arc::new(Mutex::new(0));

    // Once we have our counter we'll wrap it inside en `Env` which we'll pass
    // to our imported functions.
    //
    // This struct may have been anything. The only constraint is it must be
    // possible to know the size of the `Env` at compile time (i.e it has to
    // implement the `Sized` trait) and that it implement the `WasmerEnv` trait.
    // We derive a default implementation of `WasmerEnv` here.
    #[derive(Clone)]
    struct Env {
        counter: Arc<Mutex<i32>>,
    }
    impl WasmerEnv for Env {}

    // Create the functions
    fn get_counter(env: &Env) -> i32 {
        *env.counter.lock().unwrap()
    }
    fn add_to_counter(env: &Env, add: i32) -> i32 {
        let mut counter_ref = env.counter.lock().unwrap();

        *counter_ref += add;
        *counter_ref
    }

    // Create an import object.
    let import_object = imports! {
        "env" => {
            "get_counter" => Function::new_native_with_env(&store, Env { counter: shared_counter.clone() }, get_counter),
            "add_to_counter" => Function::new_native_with_env(&store, Env { counter: shared_counter.clone() }, add_to_counter),
        }
    };

    println!("Instantiating module...");
    // Let's instantiate the Wasm module.
    let instance = Instance::new_with_config(&module, InstanceConfig::with_stack_limit(1000000), &import_object)?;

    // Here we go.
    //
    // The Wasm module exports a function called `increment_counter_loop`. Let's get it.
    let increment_counter_loop = instance
        .lookup_function("increment_counter_loop")
        .ok_or("could not find `increment_counter_loop` export")?
        .native::<i32, i32>()?;

    let counter_value: i32 = *shared_counter.lock().unwrap();
    println!("Initial ounter value: {:?}", counter_value);

    println!("Calling `increment_counter_loop` function...");
    // Let's call the `increment_counter_loop` exported function.
    //
    // It will loop five times thus incrementing our counter five times.
    let result = increment_counter_loop.call(5)?;

    let counter_value: i32 = *shared_counter.lock().unwrap();
    println!("New counter value (host): {:?}", counter_value);
    assert_eq!(counter_value, 5);

    println!("New counter value (guest): {:?}", result);
    assert_eq!(result, 5);

    Ok(())
}

#[test]
fn test_imported_function_env() -> Result<(), Box<dyn std::error::Error>> {
    main()
}
