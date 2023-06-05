# `near-vm-compiler`

This crate is a fork of `wasmer-compiler`. A significant number of things
changed, but the documentation is not up-to-date yet.

This crate is the base for Compiler implementations.

It performs the translation from a Wasm module into a basic
`ModuleInfo`, but leaves the Wasm function bytecode translation to the
compiler implementor.

Here are some of the Compilers provided by Wasmer:

* [Singlepass](https://github.com/wasmerio/wasmer/tree/master/lib/compiler-singlepass),

## How to create a compiler

To create a compiler, one needs to implement two traits:

1. `CompilerConfig`, that configures and creates a new compiler,
2. `Compiler`, the compiler itself that will compile a module.

```rust
/// The compiler configuration options.
pub trait CompilerConfig {
    /// Gets the custom compiler config
    fn compiler(&self) -> Box<dyn Compiler>;
}

/// An implementation of a compiler from parsed WebAssembly module to compiled native code.
pub trait Compiler {
    /// Compiles a parsed module.
    ///
    /// It returns the [`Compilation`] or a [`CompileError`].
    fn compile_module<'data, 'module>(
        &self,
        target: &Target,
        compile_info: &'module CompileModuleInfo,
        // The list of function bodies
        function_body_inputs: PrimaryMap<LocalFunctionIndex, FunctionBodyData<'data>>,
        instrumentation: &finite_wasm::Module,
    ) -> Result<Compilation, CompileError>;
}
```

## Acknowledgments

This project borrowed some of the code strucutre from the
[`cranelift-wasm`] crate, however it's been adapted to not depend on
any specific IR and be abstract of any compiler.

Please check [Wasmer `ATTRIBUTIONS`] to further see licenses and other
attributions of the project.


[`cranelift-wasm`]: https://crates.io/crates/cranelift-wasm
[Wasmer `ATTRIBUTIONS`]: https://github.com/wasmerio/wasmer/blob/master/ATTRIBUTIONS.md
