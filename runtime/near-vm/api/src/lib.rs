#![doc(
    html_logo_url = "https://github.com/wasmerio.png?size=200",
    html_favicon_url = "https://wasmer.io/images/icons/favicon-32x32.png"
)]
#![deny(missing_docs, trivial_numeric_casts, unused_extern_crates, rustdoc::broken_intra_doc_links)]
#![warn(unused_import_braces)]
#![cfg_attr(
    feature = "cargo-clippy",
    allow(clippy::new_without_default, clippy::vtable_address_comparisons)
)]
#![cfg_attr(
    feature = "cargo-clippy",
    warn(
        clippy::float_arithmetic,
        clippy::mut_mut,
        clippy::nonminimal_bool,
        clippy::map_unwrap_or,
        clippy::print_stdout,
        clippy::unicode_not_nfc,
        clippy::use_self
    )
)]
//! [`Wasmer`](https://wasmer.io/) is the most popular
//! [WebAssembly](https://webassembly.org/) runtime for Rust. It supports
//! JIT (Just In Time) and AOT (Ahead Of Time) compilation as well as
//! pluggable compilers suited to your needs.
//!
//! It's designed to be safe and secure, and runnable in any kind of environment.
//!
//! # Usage
//!
//! Here is a small example of using Wasmer to run a WebAssembly module
//! written with its WAT format (textual format):
//!
//! ```rust
//! use near_vm::{Store, Module, Instance, InstanceConfig, Value, Export, imports};
//!
//! fn main() -> anyhow::Result<()> {
//!     let module_wat = r#"
//!     (module
//!       (type $t0 (func (param i32) (result i32)))
//!       (func $add_one (export "add_one") (type $t0) (param $p0 i32) (result i32)
//!         get_local $p0
//!         i32.const 1
//!         i32.add))
//!     "#;
//!
//!     let store = Store::default();
//!     let module = Module::new(&store, &module_wat)?;
//!     // The module doesn't import anything, so we create an empty import object.
//!     let import_object = imports! {};
//!     let instance = Instance::new_with_config(&module, InstanceConfig::with_stack_limit(1000000), &import_object)?;
//!
//!     let add_one = instance.lookup_function("add_one").unwrap();
//!     let result = add_one.call(&[Value::I32(42)])?;
//!     assert_eq!(result[0], Value::I32(43));
//!
//!     Ok(())
//! }
//! ```
//!
//! [Discover the full collection of examples](https://github.com/wasmerio/wasmer/tree/master/examples).
//!
//! # Overview of the Features
//!
//! Wasmer is not only fast, but also designed to be *highly customizable*:
//!
//! * **Pluggable engines** — An engine is responsible to drive the
//!   compilation process and to store the generated executable code
//!   somewhere, either:
//!   * in-memory (with [`wasmer-engine-universal`]),
//!   * in a native shared object file (with [`wasmer-engine-dylib`],
//!     `.dylib`, `.so`, `.dll`), then load it with `dlopen`,
//!   * in a native static object file (with [`wasmer-engine-staticlib`]),
//!     in addition to emitting a C header file, which both can be linked
//!     against a sandboxed WebAssembly runtime environment for the
//!     compiled module with no need for runtime compilation.
//!
//! * **Pluggable compilers** — A compiler is used by an engine to
//!   transform WebAssembly into executable code:
//!   * [`wasmer-compiler-singlepass`] provides a fast compilation-time
//!     but an unoptimized runtime speed,
//!
//! * **Headless mode** — Once a WebAssembly module has been compiled, it
//!   is possible to serialize it in a file for example, and later execute
//!   it with Wasmer with headless mode turned on. Headless Wasmer has no
//!   compiler, which makes it more portable and faster to load. It's
//!   ideal for constrainted environments.
//!
//! * **Cross-compilation** — Most compilers support cross-compilation. It
//!   means it possible to pre-compile a WebAssembly module targetting a
//!   different architecture or platform and serialize it, to then run it
//!   on the targetted architecture and platform later.
//!
//! * **Run Wasmer in a JavaScript environment** — With the `js` Cargo
//!   feature, it is possible to compile a Rust program using Wasmer to
//!   WebAssembly. In this context, the resulting WebAssembly module will
//!   expect to run in a JavaScript environment, like a browser, Node.js,
//!   Deno and so on. In this specific scenario, there is no engines or
//!   compilers available, it's the one available in the JavaScript
//!   environment that will be used.
//!
//! Wasmer ships by default with the Cranelift compiler as its great for
//! development purposes.  However, we strongly encourage to use the LLVM
//! compiler in production as it performs about 50% faster, achieving
//! near-native speeds.
//!
//! Note: if one wants to use multiple compilers at the same time, it's
//! also possible! One will need to import them directly via each of the
//! compiler crates.
//!
//! # Table of Contents
//!
//! - [WebAssembly Primitives](#webassembly-primitives)
//!   - [Externs](#externs)
//!     - [Functions](#functions)
//!     - [Memories](#memories)
//!     - [Globals](#globals)
//!     - [Tables](#tables)
//! - [Project Layout](#project-layout)
//!   - [Engines](#engines)
//!   - [Compilers](#compilers)
//! - [Cargo Features](#cargo-features)
//! - [Using Wasmer in a JavaScript environment](#using-wasmer-in-a-javascript-environment)
//!
//!
//! # WebAssembly Primitives
//!
//! In order to make use of the power of the `wasmer` API, it's important
//! to understand the primitives around which the API is built.
//!
//! Wasm only deals with a small number of core data types, these data
//! types can be found in the [`Value`] type.
//!
//! In addition to the core Wasm types, the core types of the API are
//! referred to as "externs".
//!
//! ## Externs
//!
//! An [`Extern`] is a type that can be imported or exported from a Wasm
//! module.
//!
//! To import an extern, simply give it a namespace and a name with the
//! [`imports`] macro:
//!
//! ```
//! # use near_vm::{imports, Function, Memory, MemoryType, Store, ImportObject};
//! # fn imports_example(store: &Store) -> ImportObject {
//! let memory = Memory::new(&store, MemoryType::new(1, None, false)).unwrap();
//! imports! {
//!     "env" => {
//!          "my_function" => Function::new_native(store, || println!("Hello")),
//!          "memory" => memory,
//!     }
//! }
//! # }
//! ```
//!
//! And to access an exported extern, see the [`Exports`] API, accessible
//! from any instance via `instance.exports`:
//!
//! ```
//! # use near_vm::{imports, Instance, Function, Memory, NativeFunc};
//! # fn exports_example(instance: &Instance) -> anyhow::Result<()> {
//! let memory = instance.lookup("memory").unwrap();
//! let memory = instance.lookup("some_other_memory").unwrap();
//! let add: NativeFunc<(i32, i32), i32> = instance.get_native_function("add").unwrap();
//! let result = add.call(5, 37)?;
//! assert_eq!(result, 42);
//! # Ok(())
//! # }
//! ```
//!
//! These are the primary types that the `wasmer` API uses.
//!
//! ### Functions
//!
//! There are 2 types of functions in `wasmer`:
//! 1. Wasm functions,
//! 2. Host functions.
//!
//! A Wasm function is a function defined in a WebAssembly module that can
//! only perform computation without side effects and call other functions.
//!
//! Wasm functions take 0 or more arguments and return 0 or more results.
//! Wasm functions can only deal with the primitive types defined in
//! [`Value`].
//!
//! A Host function is any function implemented on the host, in this case in
//! Rust.
//!
//! Host functions can optionally be created with an environment that
//! implements [`WasmerEnv`]. This environment is useful for maintaining
//! host state (for example the filesystem in WASI).
//!
//! Thus WebAssembly modules by themselves cannot do anything but computation
//! on the core types in [`Value`]. In order to make them more useful we
//! give them access to the outside world with [`imports`].
//!
//! If you're looking for a sandboxed, POSIX-like environment to execute Wasm
//! in, check out the [`wasmer-wasi`] crate for our implementation of WASI,
//! the WebAssembly System Interface.
//!
//! In the `wasmer` API we support functions which take their arguments and
//! return their results dynamically, [`Function`], and functions which
//! take their arguments and return their results statically, [`NativeFunc`].
//!
//! ### Memories
//!
//! Memories store data.
//!
//! In most Wasm programs, nearly all data will live in a [`Memory`].
//!
//! This data can be shared between the host and guest to allow for more
//! interesting programs.
//!
//! ### Globals
//!
//! A [`Global`] is a type that may be either mutable or immutable, and
//! contains one of the core Wasm types defined in [`Value`].
//!
//! ### Tables
//!
//! A [`Table`] is an indexed list of items.
//!
//! # Project Layout
//!
//! The Wasmer project is divided into a number of crates, below is a dependency
//! graph with transitive dependencies removed.
//!
//! <div>
//! <img src="https://raw.githubusercontent.com/wasmerio/wasmer/master/docs/deps_dedup.svg" />
//! </div>
//!
//! While this crate is the top level API, we also publish crates built
//! on top of this API that you may be interested in using, including:
//!
//! - [`wasmer-cache`] for caching compiled Wasm modules,
//! - [`wasmer-emscripten`] for running Wasm modules compiled to the
//!   Emscripten ABI,
//! - [`wasmer-wasi`] for running Wasm modules compiled to the WASI ABI.
//!
//! The Wasmer project has two major abstractions:
//! 1. [Engines][wasmer-engine],
//! 2. [Compilers][wasmer-compiler].
//!
//! These two abstractions have multiple options that can be enabled
//! with features.
//!
//! ## Engines
//!
//! An engine is a system that uses a compiler to make a WebAssembly
//! module executable.
//!
//! ## Compilers
//!
//! A compiler is a system that handles the details of making a Wasm
//! module executable. For example, by generating native machine code
//! for each Wasm function.
//!
//! # Cargo Features
//!
//! This crate comes in 1 flavor:
//!
//! 1. `sys`
//!    where `wasmer` will be compiled to a native executable
//!    which provides compilers, engines, a full VM etc.
#![cfg_attr(feature = "sys", doc = "## Features for the `sys` feature group (enabled)")]
#![cfg_attr(not(feature = "sys"), doc = "## Features for the `sys` feature group (disabled)")]
//!
//! The default features can be enabled with the `sys-default` feature.
//!
//! The features for the `sys` feature group can be broken down into 2
//! kinds: features that enable new functionality and features that
//! set defaults.
//!
//! The features that enable new functionality are:
//! - `singlepass`
#![cfg_attr(feature = "singlepass", doc = "(enabled),")]
#![cfg_attr(not(feature = "singlepass"), doc = "(disabled),")]
//!   enables Wasmer's [Singlepass compiler][wasmer-compiler-singlepass],
//! - `wat`
#![cfg_attr(feature = "wat", doc = "(enabled),")]
#![cfg_attr(not(feature = "wat"), doc = "(disabled),")]
//!   enables `wasmer` to parse the WebAssembly text format,
//! - `universal`
#![cfg_attr(feature = "universal", doc = "(enabled),")]
#![cfg_attr(not(feature = "universal"), doc = "(disabled),")]
//!   enables [the Universal engine][`wasmer-engine-universal`],
//! - `dylib`
#![cfg_attr(feature = "dylib", doc = "(enabled),")]
#![cfg_attr(not(feature = "dylib"), doc = "(disabled),")]
//!   enables [the Dylib engine][`wasmer-engine-dylib`].
//!
//! The features that set defaults come in sets that are mutually exclusive.
//!
//! The first set is the default compiler set:
//! - `default-singlepass`
#![cfg_attr(feature = "default-singlepass", doc = "(enabled),")]
#![cfg_attr(not(feature = "default-singlepass"), doc = "(disabled),")]
//!   set Wasmer's Singlepass compiler as the default.
//!
//! The next set is the default engine set:
//! - `default-universal`
#![cfg_attr(feature = "default-universal", doc = "(enabled),")]
#![cfg_attr(not(feature = "default-universal"), doc = "(disabled),")]
//!   set the Universal engine as the default,
//! - `default-dylib`
#![cfg_attr(feature = "default-dylib", doc = "(enabled),")]
#![cfg_attr(not(feature = "default-dylib"), doc = "(disabled),")]
//!   set the Dylib engine as the default.
//!
//! - `wat`
#![cfg_attr(feature = "wat", doc = "(enabled),")]
#![cfg_attr(not(feature = "wat"), doc = "(disabled),")]
//!  allows to read a Wasm file in its text format. This feature is
//!  normally used only in development environments. It will add
//!  around 650kb to the Wasm bundle (120Kb gzipped).
//!
//! [wasm]: https://webassembly.org/
//! [wasmer-examples]: https://github.com/wasmerio/wasmer/tree/master/examples
//! [`wasmer-cache`]: https://docs.rs/wasmer-cache/
//! [wasmer-compiler]: https://docs.rs/wasmer-compiler/
//! [`wasmer-emscripten`]: https://docs.rs/wasmer-emscripten/
//! [wasmer-engine]: https://docs.rs/wasmer-engine/
//! [`wasmer-engine-universal`]: https://docs.rs/wasmer-engine-universal/
//! [`wasmer-engine-dylib`]: https://docs.rs/wasmer-engine-dylib/
//! [`wasmer-engine-staticlib`]: https://docs.rs/wasmer-engine-staticlib/
//! [`wasmer-compiler-singlepass`]: https://docs.rs/wasmer-compiler-singlepass/
//! [`wasmer-wasi`]: https://docs.rs/wasmer-wasi/
//! [`wasm-pack`]: https://github.com/rustwasm/wasm-pack/
//! [`wasm-bindgen`]: https://github.com/rustwasm/wasm-bindgen

#[cfg(not(feature = "sys"))]
compile_error!("At least the `sys` or the `js` feature must be enabled. Please, pick one.");

#[cfg(all(feature = "sys", target_arch = "wasm32"))]
compile_error!("The `sys` feature must be enabled only for non-`wasm32` target.");

#[cfg(feature = "sys")]
mod sys;

#[cfg(feature = "sys")]
pub use sys::*;
