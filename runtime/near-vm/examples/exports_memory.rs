//! A Wasm module can export entities, like functions, memories,
//! globals and tables.
//!
//! This example illustrates how to use exported memories
//!
//! You can run the example directly by executing in Wasmer root:
//!
//! ```shell
//! cargo run --example exported-memory --release --features "singlepass"
//! ```
//!
//! Ready?

use wasmer::{imports, wat2wasm, Array, Instance, Module, Store, WasmPtr};
use wasmer_compiler_singlepass::Singlepass;
use wasmer_engine_universal::Universal;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Let's declare the Wasm module with the text representation.
    let wasm_bytes = wat2wasm(
        br#"
(module
  (memory (export "mem") 1)

  (global $offset i32 (i32.const 42))
  (global $length (mut i32) (i32.const 13))

  (func (export "load_offset") (result i32)
    global.get $offset)
  (func (export "load_length") (result i32)
    global.get $length)

  (data (global.get $offset) "Hello, World!"))
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
    let instance = Instance::new_with_config(&module, InstanceConfig::with_stack_limit(1000000), &import_object)?;

    let load_offset = instance
        .lookup_function("load_offset")
        .ok_or("could not find `load_offset` export")?
        .native::<(), WasmPtr<u8, Array>>()?;
    let load_length = instance
        .lookup_function("load_length")
        .ok_or("could not find `load_length` export")?
        .native::<(), i32>()?;

    // Here we go.
    //
    // The Wasm module exports a memory under "mem". Let's get it.
    let memory = match instance.lookup("mem") {
        Some(wasmer::Export::Memory(m)) => m,
        _ => return Err("could not find `mem` as an exported memory".into()),
    };
    let memory = wasmer::Memory::from_vmmemory(&store, memory);

    // Now that we have the exported memory, let's get some
    // information about it.
    //
    // The first thing we might be intersted in is the size of the memory.
    // Let's get it!
    println!("Memory size (pages) {:?}", memory.size());
    println!("Memory size (bytes) {:?}", memory.data_size());

    // Next, we'll want to read the contents of the memory.
    //
    // To do so, we have to get a `View` of the memory.
    //let view = memory.view::<u8>();

    // Oh! Wait, before reading the contents, we need to know
    // where to find what we are looking for.
    //
    // Fortunately, the Wasm module exports a `load` function
    // which will tell us the offset and length of the string.
    let ptr = load_offset.call()?;
    println!("String offset: {:?}", ptr.offset());
    let length = load_length.call()?;
    println!("String length: {:?}", length);

    // We now know where to fin our string, let's read it.
    //
    // We will get bytes out of the memory so we need to
    // decode them into a string.
    let str = ptr.get_utf8_string(&memory, length as u32).unwrap();
    println!("Memory contents: {:?}", str);

    // What about changing the contents of the memory with a more
    // appropriate string?
    //
    // To do that, we'll dereference our pointer and change the content
    // of each `Cell`
    let new_str = b"Hello, Wasmer!";
    let values = ptr.deref(&memory, 0, new_str.len() as u32).unwrap();
    for i in 0..new_str.len() {
        values[i].set(new_str[i]);
    }

    // And now, let's see the result.
    //
    // Since the new strings is bigger than the older one, we
    // query the length again. The offset remains the same as
    // before.
    println!("New string length: {:?}", new_str.len());

    let str = ptr.get_utf8_string(&memory, new_str.len() as u32).unwrap();
    println!("New memory contents: {:?}", str);

    // Much better, don't you think?

    Ok(())
}

#[test]
fn test_exported_memory() -> Result<(), Box<dyn std::error::Error>> {
    main()
}
