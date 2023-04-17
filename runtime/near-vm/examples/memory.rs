//! With Wasmer you'll be able to interact with guest module memory.
//!
//! This example illustrates the basics of interacting with Wasm module memory.:
//!
//!   1. How to load a Wasm modules as bytes
//!   2. How to compile the module
//!   3. How to create an instance of the module
//!
//! You can run the example directly by executing in Wasmer root:
//!
//! ```shell
//! cargo run --example memory --release --features "singlepass"
//! ```
//!
//! Ready?

use std::mem;
use wasmer::{imports, wat2wasm, Bytes, Instance, Module, NativeFunc, Pages, Store};
use wasmer_compiler_singlepass::Singlepass;
use wasmer_engine_universal::Universal;

// this example is a work in progress:
// TODO: clean it up and comment it https://github.com/wasmerio/wasmer/issues/1749

fn main() -> anyhow::Result<()> {
    // Let's declare the Wasm module.
    //
    // We are using the text representation of the module here but you can also load `.wasm`
    // files using the `include_bytes!` macro.
    let wasm_bytes = wat2wasm(
        r#"
(module
  (type $mem_size_t (func (result i32)))
  (type $get_at_t (func (param i32) (result i32)))
  (type $set_at_t (func (param i32) (param i32)))

  (memory $mem 1)

  (func $get_at (type $get_at_t) (param $idx i32) (result i32)
    (i32.load (local.get $idx)))

  (func $set_at (type $set_at_t) (param $idx i32) (param $val i32)
    (i32.store (local.get $idx) (local.get $val)))

  (func $mem_size (type $mem_size_t) (result i32)
    (memory.size))

  (export "get_at" (func $get_at))
  (export "set_at" (func $set_at))
  (export "mem_size" (func $mem_size))
  (export "memory" (memory $mem)))
"#
        .as_bytes(),
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

    // The module exports some utility functions, let's get them.
    //
    // These function will be used later in this example.
    let mem_size: NativeFunc<(), i32> = instance
        .lookup_function("mem_size")
        .ok_or(anyhow::anyhow!("could not find `mem_size` export"))?
        .native()?;
    let get_at: NativeFunc<i32, i32> = instance
        .lookup_function("get_at")
        .ok_or(anyhow::anyhow!("could not find `get_at` export"))?
        .native()?;
    let set_at: NativeFunc<(i32, i32), ()> = instance
        .lookup_function("set_at")
        .ok_or(anyhow::anyhow!("could not find `set_at` export"))?
        .native()?;
    let memory = match instance.lookup("memory") {
        Some(wasmer::Export::Memory(m)) => m,
        _ => anyhow::bail!("could not find `memory` as an exported memory"),
    };

    // We now have an instance ready to be used.
    //
    // We will start by querying the most intersting information
    // about the memory: its size. There are mainly two ways of getting
    // this:
    // * the size as a number of `Page`s
    // * the size as a number of bytes
    //
    // The size in bytes can be found either by querying its pages or by
    // querying the memory directly.
    println!("Querying memory size...");
    assert_eq!(memory.from.size(), Pages::from(1));
    assert_eq!(memory.from.size().bytes(), Bytes::from(65536 as usize));
    unsafe {
        assert_eq!(memory.from.vmmemory().as_ref().current_length, 65536);
    }

    // Sometimes, the guest module may also export a function to let you
    // query the memory. Here we have a `mem_size` function, let's try it:
    let result = mem_size.call()?;
    println!("Memory size: {:?}", result);
    assert_eq!(Pages::from(result as u32), memory.from.size());

    // Now that we know the size of our memory, it's time to see how wa
    // can change this.
    //
    // A memory can be grown to allow storing more things into it. Let's
    // see how we can do that:
    println!("Growing memory...");
    // Here we are requesting two more pages for our memory.
    memory.from.grow(Pages::from(2))?;
    assert_eq!(memory.from.size(), Pages::from(3));
    unsafe {
        assert_eq!(memory.from.vmmemory().as_ref().current_length, 65536 * 3);
    }

    // Now that we know how to query and adjust the size of the memory,
    // let's see how wa can write to it or read from it.
    //
    // We'll only focus on how to do this using exported functions, the goal
    // is to show how to work with memory addresses. Here we'll use absolute
    // addresses to write and read a value.
    let mem_addr = 0x2220;
    let val = 0xFEFEFFE;
    set_at.call(mem_addr, val)?;

    let result = get_at.call(mem_addr)?;
    println!("Value at {:#x?}: {:?}", mem_addr, result);
    assert_eq!(result, val);

    // Now instead of using hard coded memory addresses, let's try to write
    // something at the end of the second memory page and read it.
    let page_size = 0x1_0000;
    let mem_addr = (page_size * 2) - mem::size_of_val(&val) as i32;
    let val = 0xFEA09;
    set_at.call(mem_addr, val)?;

    let result = get_at.call(mem_addr)?;
    println!("Value at {:#x?}: {:?}", mem_addr, result);
    assert_eq!(result, val);

    Ok(())
}
