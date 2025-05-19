# `near-vm-compiler-singlepass`

This crate is a fork of `wasmer-compiler-singlepass`. A significant number of things changed, but the documentation is not up-to-date yet.

This crate contains a compiler implementation based on the Singlepass linear compiler.

## Usage

```rust
use near_vm_test_api::{Store, Universal};
use near_vm_compiler_singlepass::Singlepass;

let compiler = Singlepass::new();
// Put it into an engine and add it to the store
let store = Store::new(&Universal::new(compiler).engine());
```

*Note: you can find a [full working example using Singlepass compiler
here][example].*

## When to use Singlepass

Singlepass is designed to emit compiled code at linear time, as such
is not prone to JIT bombs and also offers great compilation
performance, however with a bit slower runtime speed.

The fact that singlepass is not prone to JIT bombs and offers a very
predictable compilation speed makes it ideal for **blockchains** and other
systems where fast and consistent compilation times are very critical.


[example]: https://github.com/wasmerio/wasmer/blob/master/examples/compiler_singlepass.rs
