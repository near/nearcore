//! Runtime library support for Wasmer.

#![deny(missing_docs, trivial_numeric_casts, unused_extern_crates)]
#![deny(trivial_numeric_casts, unused_extern_crates)]
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

mod artifact;
mod export;
mod func_data_registry;
mod global;
mod imports;
mod instance;
mod memory;
mod mmap;
mod probestack;
mod resolver;
mod sig_registry;
mod table;
mod trap;
mod tunables;
mod vmcontext;
mod vmoffsets;

pub mod libcalls;

pub use crate::artifact::{Artifact, Instantiatable};
pub use crate::export::*;
pub use crate::func_data_registry::{FuncDataRegistry, VMFuncRef};
pub use crate::global::*;
pub use crate::imports::{Imports, VMImport, VMImportType};
pub use crate::instance::{
    initialize_host_envs, ImportFunctionEnv, ImportInitializerFuncPtr, InstanceAllocator,
    InstanceHandle, WeakOrStrongInstanceRef,
};
pub use crate::memory::{LinearMemory, Memory, MemoryError, MemoryStyle};
pub use crate::mmap::Mmap;
pub use crate::probestack::PROBESTACK;
pub use crate::resolver::{
    ChainableNamedResolver, Export, ExportFunction, ExportFunctionMetadata, NamedResolver,
    NamedResolverChain, NullResolver, Resolver,
};
pub use crate::sig_registry::{SignatureRegistry, VMSharedSignatureIndex};
pub use crate::table::{LinearTable, Table, TableElement, TableStyle};
pub use crate::trap::*;
pub use crate::tunables::{TestTunables, Tunables};
pub use crate::vmcontext::{
    FunctionBodyPtr, FunctionExtent, SectionBodyPtr, VMBuiltinFunctionIndex,
    VMCallerCheckedAnyfunc, VMContext, VMDynamicFunctionContext, VMFunctionBody,
    VMFunctionEnvironment, VMFunctionImport, VMFunctionKind, VMGlobalDefinition, VMGlobalImport,
    VMLocalFunction, VMMemoryDefinition, VMMemoryImport, VMTableDefinition, VMTableImport,
    VMTrampoline,
};
pub use crate::vmoffsets::{TargetSharedSignatureIndex, VMOffsets};
#[deprecated(
    since = "2.1.0",
    note = "ModuleInfo, ExportsIterator, ImportsIterator should be imported from near_vm_types."
)]
pub use near_vm_types::ModuleInfo;
pub use near_vm_types::VMExternRef;

/// Version number of this crate.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
