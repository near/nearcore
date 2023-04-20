# `near-vm-types`

This crate is a fork of `wasmer-types`. A significant number of things changed, but the documentation is not up-to-date yet.

This library provides all the types and traits necessary to use
WebAssembly easily anywhere.

Among other things, it defines the following _types_:

* `units` like `Pages` or `Bytes`
* `types` and `values` like `I32`, `I64`, `F32`, `F64`, `ExternRef`,
  `FuncRef`, `V128`, value conversions, `ExternType`, `FunctionType`
  etc.
* `native` contains a set of trait and implementations to deal with
  WebAssembly types that have a direct representation on the host,
* `memory_view`, an API to read/write memories when bytes are
  interpreted as particular types (`i8`, `i16`, `i32` etc.)
* `indexes` contains all the possible WebAssembly module indexes for
  various types
* `initializers` for tables, data etc.
* `features` to enable or disable some WebAssembly features inside the
  Wasmer runtime


### Acknowledgments

This project borrowed some of the code for the entity structure from [cranelift-entity](https://crates.io/crates/cranelift-entity).
We decided to move it here to help on serialization/deserialization and also to ease the integration with other tools like `loupe`.

Please check [Wasmer ATTRIBUTIONS](https://github.com/wasmerio/wasmer/blob/master/ATTRIBUTIONS.md) to further see licenses and other attributions of the project. 
