//! This are the common types and utility tools for using WebAssembly
//! in a Rust environment.
//!
//! This crate provides common structures such as `Type` or `Value`, type indexes
//! and native function wrappers with `Func`.

#![deny(missing_docs, unused_extern_crates)]
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

/// The `lib` module defines a `std` module that is identical whether
/// the `core` or the `std` feature is enabled.
pub mod lib {
    /// Custom `std` module.
    pub mod std {
        pub use std::{
            any, borrow, boxed, cell, cmp, convert, fmt, format, hash, iter, marker, mem, ops, ptr,
            rc, slice, string, sync, u32, vec,
        };
    }
}

mod archives;
mod extern_ref;
mod features;
mod indexes;
mod initializers;
mod memory_view;
mod module;
mod native;
pub mod partial_sum_map;
mod types;
mod units;
mod values;

/// The entity module, with common helpers for Rust structures
pub mod entity;
pub use crate::extern_ref::{ExternRef, VMExternRef};
pub use crate::features::Features;
pub use crate::indexes::{
    CustomSectionIndex, DataIndex, ElemIndex, ExportIndex, FunctionIndex, GlobalIndex, ImportIndex,
    LocalFunctionIndex, LocalGlobalIndex, LocalMemoryIndex, LocalTableIndex, MemoryIndex,
    SignatureIndex, TableIndex,
};
pub use crate::initializers::{
    DataInitializer, DataInitializerLocation, OwnedDataInitializer, OwnedTableInitializer,
};
pub use crate::memory_view::{Atomically, MemoryView};
pub use crate::module::{ImportCounts, ModuleInfo};
pub use crate::native::{NativeWasmType, ValueType};
pub use crate::units::{
    Bytes, PageCountOutOfRange, Pages, WASM_MAX_PAGES, WASM_MIN_PAGES, WASM_PAGE_SIZE,
};
pub use crate::values::{Value, WasmValueType};
pub use types::{
    ExportType, ExternType, FastGasCounter, FunctionType, FunctionTypeRef, GlobalInit, GlobalType,
    Import, InstanceConfig, MemoryType, Mutability, TableType, Type, V128,
};

pub use archives::ArchivableIndexMap;

/// Version number of this crate.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
