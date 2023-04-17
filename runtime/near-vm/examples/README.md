# Wasmer Examples

This directory contains a collection of examples. This isn't an
exhaustive collection though, if one example is missing, please ask,
we will be happy to fulfill your needs!

## Handy Diagrams

As a quick introduction to Wasmer's main workflows, here are three
diagrams. We hope it provides an overview of how the crates assemble
together.

1. **Module compilation**, illustrates how WebAssembly bytes are
   validated, parsed, and compiled, with the help of the
   `wasmer::Module`, `wasmer_engine::Engine`, and
   `wasmer_compiler::Compiler` API.

   ![Module compilation](../assets/diagrams/Diagram_module_compilation.png)

2. **Module serialization**, illustrates how a module can be
   serialized and deserialized, with the help of
   `wasmer::Module::serialize` and `wasmer::Module::deserialize`. The
   important part is that the engine can changed between those two
   steps, and thus how a headless engine can be used for the
   deserialization.

   ![Module serialization](../assets/diagrams/Diagram_module_serialization.png)

3. **Module instantiation**, illustrates what happens when
   `wasmer::Instance::new_with_config` is called.

   ![Module instantiation](../assets/diagrams/Diagram_module_instantiation.png)

## Examples

The examples are written in a difficulty/discovery order. Concepts that
are explained in an example is not necessarily re-explained in a next
example.

### Basics

1. [**Hello World**][hello-world], explains the core concepts of the Wasmer
   API for compiling and executing WebAssembly.

   _Keywords_: introduction, instance, module.

   <details>
    <summary><em>Execute the example</em></summary>

    ```shell
    $ cargo run --example hello-world --release --features "singlepass"
    ```

   </details>

2. [**Instantiating a module**][instance], explains the basics of using Wasmer
   and how to create an instance out of a Wasm module.

   _Keywords_: instance, module.

   <details>
    <summary><em>Execute the example</em></summary>

    ```shell
    $ cargo run --example instance --release --features "singlepass"
    ```

   </details>

3. [**Handling errors**][errors], explains the basics of interacting with
   Wasm module memory.

   _Keywords_: instance, error.

   <details>
    <summary><em>Execute the example</em></summary>

    ```shell
    $ cargo run --example errors --release --features "singlepass"
    ```

   </details>

4. [**Interacting with memory**][memory], explains the basics of interacting with
   Wasm module memory.

   _Keywords_: memory, module.

   <details>
    <summary><em>Execute the example</em></summary>

    ```shell
    $ cargo run --example memory --release --features "singlepass"
    ```

   </details>

### Exports

1. [**Exported global**][exported-global], explains how to work with
   exported globals: get/set their value, have information about their
   type.

   _Keywords_: export, global.

   <details>
   <summary><em>Execute the example</em></summary>

   ```shell
   $ cargo run --example exported-global --release --features "singlepass"
   ```

   </details>

2. [**Exported function**][exported-function], explains how to get and
   how to call an exported function. They come in 2 flavors: dynamic,
   and “static”/native. The pros and cons are discussed briefly.

   _Keywords_: export, function, dynamic, static, native.

   <details>
   <summary><em>Execute the example</em></summary>

   ```shell
   $ cargo run --example exported-function --release --features "singlepass"
   ```

   </details>


3. [**Exported memory**][exported-memory], explains how to read from
    and write to exported memory.

   _Keywords_: export, memory.

   <details>
   <summary><em>Execute the example</em></summary>

   ```shell
   $ cargo run --example exported-memory --release --features "singlepass"
   ```

   </details>

### Imports

1. [**Imported global**][imported-global], explains how to work with
   imported globals: create globals, import them, get/set their value.

   _Keywords_: import, global.

   <details>
   <summary><em>Execute the example</em></summary>

   ```shell
   $ cargo run --example imported-global --release --features "singlepass"
   ```

   </details>

2. [**Imported function**][imported-function], explains how to define
   an imported function. They come in 2 flavors: dynamic,
   and “static”/native.

   _Keywords_: import, function, dynamic, static, native.

   <details>
   <summary><em>Execute the example</em></summary>

   ```shell
   $ cargo run --example imported-function --release --features "singlepass"
   ```

   </details>

### Externs

1. [**Table**][table], explains how to use Wasm Tables from the Wasmer API.

   _Keywords_: basic, table, call_indirect

   <details>
   <summary><em>Execute the example</em></summary>

   ```shell
   $ cargo run --example table --release --features "singlepass"
   ```

   </details>

2. [**Memory**][memory], explains how to use Wasm Memories from
   the Wasmer API.  Memory example is a work in progress.

   _Keywords_: basic, memory

   <details>
   <summary><em>Execute the example</em></summary>

   ```shell
   $ cargo run --example memory --release --features "singlepass"
   ```

   </details>

### Tunables

1. [**Limit memory**][tunables-limit-memory], explains how to use Tunables to limit the
   size of an exported Wasm memory

   _Keywords_: basic, tunables, memory

   <details>
   <summary><em>Execute the example</em></summary>

   ```shell
   $ cargo run --example tunables-limit-memory --release --features "singlepass"
   ```

   </details>

### Engines

1. [**Universal engine**][engine-universal], explains what an engine is, what the
   Universal engine is, and how to set it up. The example completes itself
   with the compilation of the Wasm module, its instantiation, and
   finally, by calling an exported function.

   _Keywords_: Universal, engine, in-memory, executable code.

   <details>
   <summary><em>Execute the example</em></summary>

   ```shell
   $ cargo run --example engine-universal --release --features "singlepass"
   ```

   </details>

2. [**Dylib engine**][engine-dylib], explains what a Dylib engine
   is, and how to set it up. The example completes itself with the
   compilation of the Wasm module, its instantiation, and finally, by
   calling an exported function.

   _Keywords_: native, engine, shared library, dynamic library,
   executable code.

   <details>
   <summary><em>Execute the example</em></summary>

   ```shell
   $ cargo run --example engine-dylib --release --features "singlepass"
   ```

   </details>

3. [**Headless engines**][engine-headless], explains what a headless
   engine is, what problem it does solve, and what are the benefits of
   it. The example completes itself with the instantiation of a
   pre-compiled Wasm module, and finally, by calling an exported
   function.

   _Keywords_: native, engine, constrained environment, ahead-of-time
   compilation, cross-compilation, executable code, serialization.

   <details>
   <summary><em>Execute the example</em></summary>

   ```shell
   $ cargo run --example engine-headless --release --features "singlepass"
   ```

   </details>

4. [**Cross-compilation**][cross-compilation], illustrates the power
   of the abstraction over the engines and the compilers, such as it
   is possible to cross-compile a Wasm module for a custom target.

   _Keywords_: engine, compiler, cross-compilation.

   <details>
   <summary><em>Execute the example</em></summary>

   ```shell
   $ cargo run --example cross-compilation --release --features "singlepass"
   ```

   </details>

5. [**Features**][features], illustrates how to enable WebAssembly
   features that aren't yet stable.

   _Keywords_: engine, features.

   <details>
   <summary><em>Execute the example</em></summary>

   ```shell
   $ cargo run --example features --release --features "singlepass"
   ```

   </details>

### Compilers

1. [**Singlepass compiler**][compiler-singlepass], explains how to use
   the [`wasmer-compiler-singlepass`] compiler.

   _Keywords_: compiler, singlepass.

   <details>
   <summary><em>Execute the example</em></summary>

   ```shell
   $ cargo run --example compiler-singlepass --release --features "singlepass"
   ```

   </details>

### Integrations

1. [**WASI**][wasi], explains how to use the [WebAssembly System
   Interface][WASI] (WASI), i.e. the [`wasmer-wasi`] crate.

   _Keywords_: wasi, system, interface

   <details>
   <summary><em>Execute the example</em></summary>

   ```shell
   $ cargo run --example wasi --release --features "singlepass,wasi"
   ```

   </details>

2. [**WASI Pipes**][wasi-pipes], builds on the WASI example to show off
   stdio piping in Wasmer.

   _Keywords_: wasi, system, interface

   <details>
   <summary><em>Execute the example</em></summary>

   ```shell
   $ cargo run --example wasi-pipes --release --features "singlepass,wasi"
   ```

   </details>

[hello-world]: ./hello_world.rs
[engine-universal]: ./engine_universal.rs
[engine-dylib]: ./engine_dylib.rs
[engine-headless]: ./engine_headless.rs
[compiler-singlepass]: ./compiler_singlepass.rs
[cross-compilation]: ./engine_cross_compilation.rs
[exported-global]: ./exports_global.rs
[exported-function]: ./exports_function.rs
[exported-memory]: ./exports_memory.rs
[imported-global]: ./imports_global.rs
[imported-function]: ./imports_function.rs
[instance]: ./instance.rs
[wasi]: ./wasi.rs
[wasi-pipes]: ./wasi_pipes.rs
[table]: ./table.rs
[memory]: ./memory.rs
[errors]: ./errors.rs
[tunables-limit-memory]: ./tunables_limit_memory.rs
[features]: ./features.rs
[`wasmer-compiler-singlepass`]: https://github.com/wasmerio/wasmer/tree/master/lib/compiler-singlepass
[`wasmer-wasi`]: https://github.com/wasmerio/wasmer/tree/master/lib/wasi
[WASI]: https://github.com/WebAssembly/WASI
