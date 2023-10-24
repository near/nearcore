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

pub use crate::sys::env::{LazyInit, WasmerEnv};
pub use crate::sys::exports::Exports;
pub use crate::sys::externals::{Function, Global, Memory, Table};
pub use crate::sys::import_object::{ImportObject, LikeNamespace};
pub use crate::sys::instance::{Instance, InstanceConfig, InstantiationError};
pub use crate::sys::module::Module;
pub use crate::sys::native::NativeFunc;
pub use crate::sys::store::Store;
pub use crate::sys::tunables::BaseTunables;
pub use crate::sys::types::{FunctionType, MemoryType, TableType, Val, ValType};
pub use crate::sys::types::{Val as Value, ValType as Type};
pub use near_vm_types::ExternRef;
pub use near_vm_vm::{Export, NamedResolver};

#[cfg(feature = "wat")]
pub use wat::parse_bytes as wat2wasm;

#[cfg(feature = "singlepass")]
pub use near_vm_compiler_singlepass::Singlepass;

#[cfg(feature = "universal")]
pub use near_vm_engine::universal::{Universal, UniversalArtifact, UniversalEngine};
