//! Define `UniversalArtifact` to allow compiling and instantiating to be
//! done as separate steps.

use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::sync::Arc;
use wasmer_engine::InstantiationError;
use wasmer_types::entity::{BoxedSlice, EntityRef, PrimaryMap};
use wasmer_types::{
    DataIndex, ElemIndex, FunctionIndex, GlobalInit, GlobalType, ImportCounts, LocalFunctionIndex,
    LocalGlobalIndex, MemoryType, OwnedDataInitializer, OwnedTableInitializer, SignatureIndex,
    TableType,
};
use wasmer_vm::{
    Artifact, FunctionBodyPtr, FunctionExtent, InstanceHandle, Instantiatable, MemoryStyle,
    Resolver, TableStyle, Tunables, VMImport, VMImportType, VMLocalFunction, VMOffsets,
    VMSharedSignatureIndex,
};

/// A compiled wasm module, containing everything necessary for instantiation.
pub struct UniversalArtifact {
    // TODO: figure out how to allocate fewer distinct structures onto heap. Maybe have an arena…?
    pub(crate) engine: crate::UniversalEngine,
    pub(crate) import_counts: ImportCounts,
    pub(crate) start_function: Option<FunctionIndex>,
    pub(crate) vmoffsets: VMOffsets,
    pub(crate) imports: Vec<VMImport>,
    pub(crate) dynamic_function_trampolines: BoxedSlice<FunctionIndex, FunctionBodyPtr>,
    pub(crate) functions: BoxedSlice<LocalFunctionIndex, VMLocalFunction>,
    pub(crate) exports: BTreeMap<String, wasmer_types::ExportIndex>,
    pub(crate) signatures: BoxedSlice<SignatureIndex, VMSharedSignatureIndex>,
    pub(crate) local_memories: Vec<(MemoryType, MemoryStyle)>,
    pub(crate) data_segments: Vec<OwnedDataInitializer>,
    pub(crate) passive_data: BTreeMap<DataIndex, Arc<[u8]>>,
    pub(crate) local_tables: Vec<(TableType, TableStyle)>,
    pub(crate) element_segments: Vec<OwnedTableInitializer>,
    // TODO: does this need to be a BTreeMap? Can it be a plain vector?
    pub(crate) passive_elements: BTreeMap<ElemIndex, Box<[FunctionIndex]>>,
    pub(crate) local_globals: Vec<(GlobalType, GlobalInit)>,
}

impl UniversalArtifact {
    /// Return the extents of the specified local function.
    pub fn function_extent(&self, index: LocalFunctionIndex) -> Option<FunctionExtent> {
        let func = self.functions.get(index)?;
        Some(FunctionExtent {
            address: func.body,
            length: usize::try_from(func.length).unwrap(),
        })
    }

    /// Return the engine instance this artifact is loaded into.
    pub fn engine(&self) -> &crate::UniversalEngine {
        &self.engine
    }
}

impl Instantiatable for UniversalArtifact {
    type Error = InstantiationError;

    unsafe fn instantiate(
        self: Arc<Self>,
        tunables: &dyn Tunables,
        resolver: &dyn Resolver,
        host_state: Box<dyn std::any::Any>,
        config: wasmer_types::InstanceConfig,
    ) -> Result<InstanceHandle, Self::Error> {
        let (imports, import_function_envs) = {
            let mut imports = wasmer_engine::resolve_imports(
                &self.engine,
                resolver,
                &self.import_counts,
                &self.imports,
                &self.dynamic_function_trampolines,
            )
            .map_err(InstantiationError::Link)?;

            // Get the `WasmerEnv::init_with_instance` function pointers and the pointers
            // to the envs to call it on.
            let import_function_envs = imports.get_imported_function_envs();

            (imports, import_function_envs)
        };

        let (allocator, memory_definition_locations, table_definition_locations) =
            wasmer_vm::InstanceAllocator::new(self.vmoffsets.clone());

        // Memories
        let mut memories: PrimaryMap<wasmer_types::LocalMemoryIndex, _> =
            PrimaryMap::with_capacity(self.local_memories.len());
        for (idx, (ty, style)) in (self.import_counts.memories..).zip(self.local_memories.iter()) {
            let memory = tunables
                .create_vm_memory(&ty, &style, memory_definition_locations[idx as usize])
                .map_err(|e| {
                    InstantiationError::Link(wasmer_engine::LinkError::Resource(format!(
                        "Failed to create memory: {}",
                        e
                    )))
                })?;
            memories.push(memory);
        }

        // Tables
        let mut tables: PrimaryMap<wasmer_types::LocalTableIndex, _> =
            PrimaryMap::with_capacity(self.local_tables.len());
        for (idx, (ty, style)) in (self.import_counts.tables..).zip(self.local_tables.iter()) {
            let table = tunables
                .create_vm_table(ty, style, table_definition_locations[idx as usize])
                .map_err(|e| InstantiationError::Link(wasmer_engine::LinkError::Resource(e)))?;
            tables.push(table);
        }

        // Globals
        let mut globals =
            PrimaryMap::<LocalGlobalIndex, _>::with_capacity(self.local_globals.len());
        for (ty, _) in self.local_globals.iter() {
            globals.push(Arc::new(wasmer_vm::Global::new(*ty)));
        }

        let passive_data = self.passive_data.clone();
        Ok(InstanceHandle::new(
            self,
            allocator,
            memories.into_boxed_slice(),
            tables.into_boxed_slice(),
            globals.into_boxed_slice(),
            imports,
            passive_data,
            host_state,
            import_function_envs,
            config,
        ))
    }
}

impl Artifact for UniversalArtifact {
    fn offsets(&self) -> &wasmer_vm::VMOffsets {
        &self.vmoffsets
    }

    fn import_counts(&self) -> &ImportCounts {
        &self.import_counts
    }

    fn functions(&self) -> &BoxedSlice<LocalFunctionIndex, VMLocalFunction> {
        &self.functions
    }

    fn passive_elements(&self) -> &BTreeMap<ElemIndex, Box<[FunctionIndex]>> {
        &self.passive_elements
    }

    fn element_segments(&self) -> &[OwnedTableInitializer] {
        &self.element_segments[..]
    }

    fn data_segments(&self) -> &[OwnedDataInitializer] {
        &self.data_segments[..]
    }

    fn globals(&self) -> &[(GlobalType, GlobalInit)] {
        &self.local_globals[..]
    }

    fn start_function(&self) -> Option<FunctionIndex> {
        self.start_function
    }

    fn export_field(&self, name: &str) -> Option<wasmer_types::ExportIndex> {
        self.exports.get(name).cloned()
    }

    fn signatures(&self) -> &[wasmer_vm::VMSharedSignatureIndex] {
        self.signatures.values().as_slice()
    }

    fn function_signature(&self, index: FunctionIndex) -> Option<VMSharedSignatureIndex> {
        match self.import_counts().local_function_index(index) {
            Ok(local) => Some(self.functions[local].signature),
            Err(import) => self
                .imports
                .iter()
                .filter_map(|im| {
                    if let VMImportType::Function { sig, .. } = im.ty {
                        Some(sig)
                    } else {
                        None
                    }
                })
                .nth(import.index()),
        }
    }
}
