# `near-vm`

This crate is a fork of `wasmer`. A significant number of things
changed, but the documentation is not up-to-date yet.

[`Wasmer`](https://wasmer.io/) is the most popular
[WebAssembly](https://webassembly.org/) runtime for Rust. It supports
JIT (Just In Time) and AOT (Ahead Of Time) compilation as well as
pluggable compilers suited to your needs.

It's designed to be safe and secure, and runnable in any kind of environment.

## Usage

Here is a small example of using Wasmer to run a WebAssembly module
written with its WAT format (textual format):

```rust
use near_vm_test_api::{Store, Module, Instance, Value, imports};

fn main() -> anyhow::Result<()> {
    let module_wat = r#"
    (module
    (type $t0 (func (param i32) (result i32)))
    (func $add_one (export "add_one") (type $t0) (param $p0 i32) (result i32)
        get_local $p0
        i32.const 1
        i32.add))
    "#;

    let store = Store::default();
    let module = Module::new(&store, &module_wat)?;
    // The module doesn't import anything, so we create an empty import object.
    let import_object = imports! {};
    let instance = Instance::new_with_config(&module, InstanceConfig::with_stack_limit(1000000), &import_object)?;

    let add_one = instance.exports.get_function("add_one")?;
    let result = add_one.call(&[Value::I32(42)])?;
    assert_eq!(result[0], Value::I32(43));

    Ok(())
}
```

[Discover the full collection of examples](https://github.com/wasmerio/wasmer/tree/master/examples).

## Features

Wasmer is not only fast, but also designed to be *highly customizable*:

* **Pluggable engines** — An engine is responsible to drive the
  compilation process and to store the generated executable code
  somewhere, either:
  * in-memory (with [`wasmer-engine-universal`]),
  * in a native shared object file (with [`wasmer-engine-dylib`],
    `.dylib`, `.so`, `.dll`), then load it with `dlopen`,
  * in a native static object file (with [`wasmer-engine-staticlib`]),
    in addition to emitting a C header file, which both can be linked
    against a sandboxed WebAssembly runtime environment for the
    compiled module with no need for runtime compilation.

* **Pluggable compilers** — A compiler is used by an engine to
  transform WebAssembly into executable code:
  * [`wasmer-compiler-singlepass`] provides a fast compilation-time
    but an unoptimized runtime speed,

* **Headless mode** — Once a WebAssembly module has been compiled, it
  is possible to serialize it in a file for example, and later execute
  it with Wasmer with headless mode turned on. Headless Wasmer has no
  compiler, which makes it more portable and faster to load. It's
  ideal for constrainted environments.

* **Cross-compilation** — Most compilers support cross-compilation. It
  means it possible to pre-compile a WebAssembly module targetting a
  different architecture or platform and serialize it, to then run it
  on the targetted architecture and platform later.

* **Run Wasmer in a JavaScript environment** — With the `js` Cargo
  feature, it is possible to compile a Rust program using Wasmer to
  WebAssembly. In this context, the resulting WebAssembly module will
  expect to run in a JavaScript environment, like a browser, Node.js,
  Deno and so on. In this specific scenario, there is no engines or
  compilers available, it's the one available in the JavaScript
  environment that will be used.

Wasmer ships by default with the Cranelift compiler as its great for
development purposes.  However, we strongly encourage to use the LLVM
compiler in production as it performs about 50% faster, achieving
near-native speeds.

Note: if one wants to use multiple compilers at the same time, it's
also possible! One will need to import them directly via each of the
compiler crates.

Read [the documentation to learn
more](https://wasmerio.github.io/wasmer/crates/doc/wasmer/).

---

Made with ❤️ by the Wasmer team, for the community

[`wasmer-engine-universal`]: https://github.com/wasmerio/wasmer/tree/master/lib/engine-universal
[`wasmer-engine-dylib`]: https://github.com/wasmerio/wasmer/tree/master/lib/engine-dylib
[`wasmer-engine-staticlib`]: https://github.com/wasmerio/wasmer/tree/master/lib/engine-staticlib
[`wasmer-compiler-singlepass`]: https://github.com/wasmerio/wasmer/tree/master/lib/compiler-singlepass
