//! A Wasm module can sometimes be invalid or trigger traps, and in those case we will get
//! an error back from the API.
//!
//! In this example we'll see how to handle such errors in the most
//! basic way. To do that we'll use a Wasm module that we know will
//! produce an error.
//!
//! You can run the example directly by executing in Wasmer root:
//!
//! ```shell
//! cargo run --example errors --release --features "singlepass"
//! ```
//!
//! Ready?

use wasmer::{imports, wat2wasm, Instance, Module, Store};
use wasmer_compiler_singlepass::Singlepass;
use wasmer_engine_universal::Universal;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Let's declare the Wasm module with the text representation.
    let wasm_bytes = wat2wasm(
        br#"
(module
  (type $do_div_by_zero_t (func (result i32)))
  (func $do_div_by_zero_f (type $do_div_by_zero_t) (result i32)
    i32.const 4
    i32.const 0
    i32.div_s)

  (type $div_by_zero_t (func (result i32)))
  (func $div_by_zero_f (type $div_by_zero_t) (result i32)
    call $do_div_by_zero_f)
  (export "div_by_zero" (func $div_by_zero_f)))
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

    // Create an import object.
    let import_object = imports! {};

    println!("Instantiating module...");
    // Let's instantiate the Wasm module.
    let instance = Instance::new_with_config(&module, InstanceConfig::with_stack_limit(1000000), &import_object)?;

    // Here we go.
    //
    // The Wasm module exports a function called `div_by_zero`. As its name
    // implies, this function will try to do a division by zero and thus
    // produce an error.
    //
    // Let's get it.
    let div_by_zero = instance
        .lookup_function("div_by_zero")
        .ok_or("could not find `div_by_zero` export")?
        .native::<(), i32>()?;

    println!("Calling `div_by_zero` function...");
    // Let's call the `div_by_zero` exported function.
    let result = div_by_zero.call();

    // When we call a function it can either succeed or fail. We expect it to fail.
    match result {
        Ok(_) => {
            // This should have thrown an error, return an error
            panic!("div_by_zero did not error");
        }
        Err(e) => {
            // Log the error
            println!("Error caught from `div_by_zero`: {}", e.message());

            // Errors come with a trace we can inspect to get more
            // information on the execution flow.
            let frames = e.trace();
            let frames_len = frames.len();

            for i in 0..frames_len {
                println!(
                    "  Frame #{}: {:?}::{:?}",
                    frames_len - i,
                    frames[i].module_name(),
                    frames[i].function_name().or(Some("<func>")).unwrap()
                );
            }
        }
    }

    Ok(())
}

#[test]
fn test_exported_function() -> Result<(), Box<dyn std::error::Error>> {
    main()
}
