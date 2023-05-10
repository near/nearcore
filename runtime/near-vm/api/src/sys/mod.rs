mod cell;
mod env;
mod exports;
mod externals;
mod import_object;
mod instance;
mod module;
mod native;
mod ptr;
mod store;
mod tunables;
mod types;

#[doc(hidden)]
pub mod internals {
    //! We use the internals module for exporting types that are only
    //! intended to use in internal crates such as the compatibility crate
    //! `near_vm-vm`. Please don't use any of this types directly, as
    //! they might change frequently or be removed in the future.

    pub use crate::sys::externals::{WithEnv, WithoutEnv};
}

pub use crate::sys::cell::WasmCell;
pub use crate::sys::env::{HostEnvInitError, LazyInit, WasmerEnv};
pub use crate::sys::exports::{ExportError, Exportable, Exports};
pub use crate::sys::externals::{
    Extern, FromToNativeWasmType, Function, Global, HostFunction, Memory, Table, WasmTypeList,
};
pub use crate::sys::import_object::{ImportObject, ImportObjectIterator, LikeNamespace};
pub use crate::sys::instance::{Instance, InstanceConfig, InstantiationError};
pub use crate::sys::module::Module;
pub use crate::sys::native::NativeFunc;
pub use crate::sys::ptr::{Array, Item, WasmPtr};
pub use crate::sys::store::{Store, StoreObject};
pub use crate::sys::tunables::BaseTunables;
pub use crate::sys::types::{
    ExportType, ExternType, FunctionType, GlobalType, MemoryType, Mutability, TableType, Val,
    ValType,
};
pub use crate::sys::types::{Val as Value, ValType as Type};
#[cfg(feature = "compiler")]
pub use near_vm_compiler::{wasmparser, CompilerConfig};
pub use near_vm_compiler::{
    CompileError, CpuFeature, Features, ParseCpuFeatureError, Target, WasmError, WasmResult,
};
pub use near_vm_engine::{DeserializeError, Engine, FrameInfo, LinkError, RuntimeError};
pub use near_vm_types::{
    Atomically, Bytes, ExportIndex, ExternRef, GlobalInit, LocalFunctionIndex, MemoryView, Pages,
    ValueType, WASM_MAX_PAGES, WASM_MIN_PAGES, WASM_PAGE_SIZE,
};
pub use near_vm_vm::{
    ChainableNamedResolver, Export, NamedResolver, NamedResolverChain, Resolver, Tunables,
};
pub use target_lexicon::{Architecture, CallingConvention, OperatingSystem, Triple, HOST};

// TODO: should those be moved into near_vm::vm as well?
pub use near_vm_vm::{raise_user_trap, MemoryError};
pub mod vm {
    //! The `vm` module re-exports near_vm-vm types.

    pub use near_vm_vm::{
        Memory, MemoryError, MemoryStyle, Table, TableStyle, VMExtern, VMMemoryDefinition,
        VMTableDefinition,
    };
}

#[cfg(feature = "wat")]
pub use wat::parse_bytes as wat2wasm;

#[cfg(feature = "singlepass")]
pub use near_vm_compiler_singlepass::Singlepass;

#[cfg(feature = "universal")]
pub use near_vm_engine_universal::{Universal, UniversalArtifact, UniversalEngine};

#[cfg(feature = "dylib")]
pub use near_vm_engine_dylib::{Dylib, DylibArtifact, DylibEngine};

/// Version number of this crate.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The Deprecated JIT Engine (please use `Universal` instead)
#[cfg(feature = "jit")]
#[deprecated(since = "2.0.0", note = "Please use the `universal` feature instead")]
pub type JIT = Universal;

/// The Deprecated Native Engine (please use `Dylib` instead)
#[cfg(feature = "native")]
#[deprecated(since = "2.0.0", note = "Please use the `native` feature instead")]
pub type Native = Dylib;
