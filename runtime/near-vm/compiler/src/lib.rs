//! The `near_vm-compiler` crate provides the necessary abstractions
//! to create a compiler.
//!
//! It provides an universal way of parsing a module via `wasmparser`,
//! while giving the responsibility of compiling specific function
//! WebAssembly bodies to the `Compiler` implementation.

#![deny(missing_docs, trivial_numeric_casts, unused_extern_crates)]
#![warn(unused_import_braces)]
#![deny(unstable_features)]
#![cfg_attr(feature = "cargo-clippy", allow(clippy::new_without_default))]
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

mod lib {
    pub mod std {
        pub use std::{borrow, boxed, collections, fmt, str, string, sync, vec};
    }
}

mod address_map;
mod compiler;
mod error;
mod function;
mod jump_table;
mod module;
mod relocation;
mod target;
mod trap;
#[macro_use]
mod translator;
mod section;
mod sourceloc;

pub use crate::address_map::{FunctionAddressMap, InstructionAddressMap};
pub use crate::compiler::{Compiler, CompilerConfig, Symbol, SymbolRegistry};
pub use crate::error::{
    CompileError, MiddlewareError, ParseCpuFeatureError, WasmError, WasmResult,
};
pub use crate::function::{
    Compilation, CompiledFunction, CompiledFunctionFrameInfo, CustomSections, Dwarf, FunctionBody,
    FunctionBodyRef, Functions, TrampolinesSection,
};
pub use crate::jump_table::{JumpTable, JumpTableOffsets};
pub use crate::module::CompileModuleInfo;
pub use crate::relocation::{Relocation, RelocationKind, RelocationTarget, Relocations};
pub use crate::section::{
    CustomSection, CustomSectionProtection, CustomSectionRef, SectionBody, SectionIndex,
};
pub use crate::sourceloc::SourceLoc;
pub use crate::target::{
    Architecture, BinaryFormat, CallingConvention, CpuFeature, Endianness, OperatingSystem,
    PointerWidth, Target, Triple,
};
pub use crate::translator::{
    translate_module, wptype_to_type, FunctionBodyData, FunctionReader, ModuleEnvironment,
    ModuleTranslationState,
};
pub use crate::trap::TrapInformation;
pub use near_vm_types::Features;

/// wasmparser is exported as a module to slim compiler dependencies
pub use wasmparser;

/// Offset in bytes from the beginning of the function.
pub type CodeOffset = u32;

/// Addend to add to the symbol value.
pub type Addend = i64;

/// Version number of this crate.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
