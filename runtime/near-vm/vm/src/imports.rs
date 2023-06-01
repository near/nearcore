// This file contains code from external sources.
// Attributions: https://github.com/wasmerio/wasmer/blob/master/ATTRIBUTIONS.md

use crate::instance::ImportFunctionEnv;
use crate::vmcontext::{VMFunctionImport, VMGlobalImport, VMMemoryImport, VMTableImport};
use crate::{VMSharedSignatureIndex, VMTrampoline};
use near_vm_types::entity::{BoxedSlice, PrimaryMap};
use near_vm_types::{FunctionIndex, GlobalIndex, MemoryIndex, TableIndex};

/// Type of the import.
pub enum VMImportType {
    /// A function import.
    Function {
        /// Signature for the function import.
        sig: VMSharedSignatureIndex,
        /// Trampoline to use for functions that use [`VMFunctionKind::Static`].
        static_trampoline: VMTrampoline,
    },
    /// A global.
    Global(near_vm_types::GlobalType),
    /// A table.
    Table(near_vm_types::TableType),
    /// Some memory.
    Memory(near_vm_types::MemoryType, crate::MemoryStyle),
}

/// A module import.
pub struct VMImport {
    /// This is passed to the `resolve` method.
    ///
    /// This index is shared between different import types.
    pub import_no: u32,
    /// The module name.
    pub module: String,
    /// The field name.
    pub field: String,
    /// Type of the import.
    pub ty: VMImportType,
}

/// Resolved import pointers.
#[derive(Clone)]
pub struct Imports {
    /// Resolved addresses for imported functions.
    pub functions: BoxedSlice<FunctionIndex, VMFunctionImport>,

    /// Initializers for host function environments. This is split out from `functions`
    /// because the generated code never needs to touch this and the extra wasted
    /// space may affect Wasm runtime performance due to increased cache pressure.
    ///
    /// We make it optional so that we can free the data after use.
    ///
    /// We move this data in `get_imported_function_envs` because there's
    /// no value to keeping it around; host functions must be initialized
    /// exactly once so we save some memory and improve correctness by
    /// moving this data.
    pub host_function_env_initializers: Option<BoxedSlice<FunctionIndex, ImportFunctionEnv>>,

    /// Resolved addresses for imported tables.
    pub tables: BoxedSlice<TableIndex, VMTableImport>,

    /// Resolved addresses for imported memories.
    pub memories: BoxedSlice<MemoryIndex, VMMemoryImport>,

    /// Resolved addresses for imported globals.
    pub globals: BoxedSlice<GlobalIndex, VMGlobalImport>,
}

impl Imports {
    /// Construct a new `Imports` instance.
    pub fn new(
        function_imports: PrimaryMap<FunctionIndex, VMFunctionImport>,
        host_function_env_initializers: PrimaryMap<FunctionIndex, ImportFunctionEnv>,
        table_imports: PrimaryMap<TableIndex, VMTableImport>,
        memory_imports: PrimaryMap<MemoryIndex, VMMemoryImport>,
        global_imports: PrimaryMap<GlobalIndex, VMGlobalImport>,
    ) -> Self {
        Self {
            functions: function_imports.into_boxed_slice(),
            host_function_env_initializers: Some(host_function_env_initializers.into_boxed_slice()),
            tables: table_imports.into_boxed_slice(),
            memories: memory_imports.into_boxed_slice(),
            globals: global_imports.into_boxed_slice(),
        }
    }

    /// Construct a new `Imports` instance with no imports.
    pub fn none() -> Self {
        Self {
            functions: PrimaryMap::new().into_boxed_slice(),
            host_function_env_initializers: None,
            tables: PrimaryMap::new().into_boxed_slice(),
            memories: PrimaryMap::new().into_boxed_slice(),
            globals: PrimaryMap::new().into_boxed_slice(),
        }
    }

    /// Get the `WasmerEnv::init_with_instance` function pointers and the pointers
    /// to the envs to call it on.
    ///
    /// This function can only be called once, it deletes the data it returns after
    /// returning it to ensure that it's not called more than once.
    pub fn get_imported_function_envs(&mut self) -> BoxedSlice<FunctionIndex, ImportFunctionEnv> {
        self.host_function_env_initializers
            .take()
            .unwrap_or_else(|| PrimaryMap::new().into_boxed_slice())
    }
}
