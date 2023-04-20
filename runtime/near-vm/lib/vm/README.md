# `near-vm-vm`

This crate is a fork of `wasmer-vm`. A significant number of things changed, but the documentation is not up-to-date yet.

This crate contains the Wasmer VM runtime library, supporting the Wasm ABI used by any [`wasmer-engine`] implementation.

The Wasmer runtime is modular by design, and provides several
libraries where each of them provides a specific set of features. This
`wasmer-vm` library contains the low-level foundation for the runtime
itself.

It provides all the APIs the
[`wasmer-engine`](https://crates.io/crates/wasmer-engine) needs to operate,
from the `instance`, to `memory`, `probestack`, signature registry, `trap`,
`table`, `VMContext`, `libcalls` etc.

It is very unlikely that a user will need to deal with `wasmer-vm`
directly. The `wasmer` crate provides types that embed types from
`wasmer-vm` with a higher-level API.


[`wasmer-engine`]: https://crates.io/crates/wasmer-engine

### Acknowledgments

This project borrowed some of the code for the VM structure and trapping from the [wasmtime-runtime](https://crates.io/crates/wasmtime-runtime).

Please check [Wasmer ATTRIBUTIONS](https://github.com/wasmerio/wasmer/blob/master/ATTRIBUTIONS.md) to further see licenses and other attributions of the project. 
