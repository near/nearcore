# `near-vm-engine`

This crate is a fork of `wasmer-engine`. A significant number of things changed, but the documentation is not up-to-date yet.

This crate is the general abstraction for creating Engines in Wasmer.

Wasmer Engines are mainly responsible for two things:
* Transform the compilation code (from any Wasmer Compiler) to
  **create** an `Artifact`,
* **Load** an`Artifact` so it can be used by the user (normally,
  pushing the code into executable memory and so on).

### Acknowledgments

This project borrowed some of the code of the trap implementation from
the [`wasmtime-api`], the code since then has evolved significantly.

Please check [Wasmer `ATTRIBUTIONS`] to further see licenses and other
attributions of the project.

[`wasmtime-api`]: https://crates.io/crates/wasmtime
[Wasmer `ATTRIBUTIONS`]: https://github.com/wasmerio/wasmer/blob/2.3.0/ATTRIBUTIONS.md
