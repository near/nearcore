//! A WebAssembly `Compiler` implementation using Singlepass.
//!
//! Singlepass is a super-fast assembly generator that generates
//! assembly code in just one pass. This is useful for different applications
//! including Blockchains and Edge computing where quick compilation
//! times are a must, and JIT bombs should never happen.
//!
//! Compared to Cranelift and LLVM, Singlepass compiles much faster but has worse
//! runtime performance.

mod address_map;
mod codegen_x64;
mod compiler;
mod config;
mod emitter_x64;
mod machine;
mod x64_decl;

pub use crate::compiler::SinglepassCompiler;
pub use crate::config::Singlepass;
