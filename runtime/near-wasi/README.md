# NEAR-WASI

NEAR-WASI implements necessary interface specified by [WebAssembly System Interface](https://github.com/WebAssembly/WASI)
(WASI for short) with stubs and near-vm-logic functions. It's an important foundation to build C/C++ programs to run on 
NEAR. 

## Motivation of this library 
Unlike Rust, where programmer can still use std and many of libraries with wasm32-unknown target, in C/C++ even libc
cannot be used. User of wasm32-unknown target need to cherry-pick implementations of `malloc`, `strcpy` etc. and port
any library with the custom implementations. 

Instead, C/C++ programmers usually use wasm32-wasi target. WASI specifies a set of syscalls. Wasm-libc provides libc
implementations if WASI is implemented by host. This makes wasm32-wasi target more practical for any non-trivial C/C++
program. However, original NEAR contract Runtime intentionally excludes Wasmer-WASI because 1) NEAR runtime must be a
sandboxed environment and contract should not have access to host I/O; 2) operations like system random, system time
would make the execution non-deterministic. To make libc and libraries built on top of libc available to NEAR contract
runtime, we made this NEAR-WASI library. It addresses the problem by:

1. Provide most of I/O access, such as socks, files, etc. as stub;
2. Provide some operation, such as print to stdout, random seed and system time by the deterministic ones provided by near-vm-logic;
3. Provide a Rust API to build import for vm runners, so these WASI functions can be imported before run a NEAR contract. 

## Roadmap
- [x] Implement all as stub
- [x] Rust API to build import for Wasmer 2
- [x] QuickJS running on Wasmer 2
- [x] Implement all possible WASI functions with near-vm-logic
- [ ] Add tests to non-stub WASI functions
- [ ] Rust API to build import for Wasmer 0 and Wasmtime