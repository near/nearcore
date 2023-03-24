//! A Wasm module can export entities, like functions, memories,
//! globals and tables.
//!
//! This example illustrates how to use exported globals. They come
//! in 2 flavors:
//!
//!   1. Immutable globals (const),
//!   2. Mutable globals.
//!
//! You can run the example directly by executing in Wasmer root:
//!
//! ```shell
//! cargo run --example exported-global --release --features "singlepass"
//! ```
//!
//! Ready?

use wasmer::{imports, wat2wasm, Instance, Module, Mutability, Store, Type, Value};
use wasmer_compiler_singlepass::Singlepass;
use wasmer_engine_universal::Universal;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Let's declare the Wasm module with the text representation.
    let wasm_bytes = wat2wasm(
        br#"
(module
  (global $one (export "one") f32 (f32.const 1))
  (global $some (export "some") (mut f32) (f32.const 0))

  (func (export "get_one") (result f32) (global.get $one))
  (func (export "get_some") (result f32) (global.get $some))

  (func (export "set_some") (param f32) (global.set $some (local.get 0))))
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

    // Create an empty import object.
    let import_object = imports! {};

    println!("Instantiating module...");
    // Let's instantiate the Wasm module.
    let instance = Instance::new(&module, &import_object)?;

    // Here we go.
    //
    // The Wasm module exports some globals. Let's get them.
    // Note that
    //
    //     ```
    //     get_global(name)
    //     ```
    //
    // is just an alias to
    //
    //     ```
    //     get::<Global>(name)`.
    //     ```
    let one = match instance.lookup("one") {
        Some(wasmer::Export::Global(g)) => g,
        _ => return Err("could not find `one` export as a global".into()),
    };
    let some = match instance.lookup("some") {
        Some(wasmer::Export::Global(g)) => g,
        _ => return Err("could not find `some` export as a global".into()),
    };

    println!("Getting globals types information...");
    // Let's get the globals types. The results are `GlobalType`s.
    let one_type = one.from.ty();
    let some_type = some.from.ty();

    println!("`one` type: {:?} {:?}", one_type.mutability, one_type.ty);
    assert_eq!(one_type.mutability, Mutability::Const);
    assert_eq!(one_type.ty, Type::F32);

    println!("`some` type: {:?} {:?}", some_type.mutability, some_type.ty);
    assert_eq!(some_type.mutability, Mutability::Var);
    assert_eq!(some_type.ty, Type::F32);

    println!("Getting global values...");
    // Getting the values of globals can be done in two ways:
    //   1. Through an exported function,
    //   2. Using the Global API directly.
    //
    // We will use an exported function for the `one` global
    // and the Global API for `some`.
    let get_one = instance
        .lookup_function("get_one")
        .ok_or("could not find `get_one` export")?
        .native::<(), f32>()?;

    let one_value = get_one.call()?;
    let some_value = some.from.get(&store);

    println!("`one` value: {:?}", one_value);
    assert_eq!(one_value, 1.0);

    println!("`some` value: {:?}", some_value);
    assert_eq!(some_value, Value::F32(0.0));

    println!("Setting global values...");
    // Trying to set the value of a immutable global (`const`)
    // will result in a `RuntimeError`.
    let result = unsafe { one.from.set(Value::F32(42.0)) };
    assert_eq!(
        result.expect_err("Expected an error").to_string(),
        "Attempted to set an immutable global"
    );

    let one_result = one.from.get(&store);
    println!("`one` value after `set`: {:?}", one_result);
    assert_eq!(one_result, Value::F32(1.0));

    // Setting the values of globals can be done in two ways:
    //   1. Through an exported function,
    //   2. Using the Global API directly.
    //
    // We will use both for the `some` global.
    let set_some = instance
        .lookup_function("set_some")
        .ok_or("could not find `set_some` export")?
        .native::<f32, ()>()?;
    set_some.call(21.0)?;
    let some_result = some.from.get(&store);
    println!("`some` value after `set_some`: {:?}", some_result);
    assert_eq!(some_result, Value::F32(21.0));

    unsafe {
        some.from.set(Value::F32(42.0))?;
    }
    let some_result = some.from.get(&store);
    println!("`some` value after `set`: {:?}", some_result);
    assert_eq!(some_result, Value::F32(42.0));

    Ok(())
}

#[test]
fn test_exported_global() -> Result<(), Box<dyn std::error::Error>> {
    main()
}
