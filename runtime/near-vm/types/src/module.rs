// This file contains code from external sources.
// Attributions: https://github.com/wasmerio/wasmer/blob/master/ATTRIBUTIONS.md

//! Data structure for representing WebAssembly modules in a
//! `wasmer::Module`.

use crate::entity::{EntityRef, PrimaryMap};
use crate::ArchivableIndexMap;
use crate::{
    CustomSectionIndex, DataIndex, ElemIndex, ExportIndex, FunctionIndex, FunctionType,
    GlobalIndex, GlobalInit, GlobalType, ImportIndex, LocalFunctionIndex, LocalGlobalIndex,
    LocalMemoryIndex, LocalTableIndex, MemoryIndex, MemoryType, OwnedTableInitializer,
    SignatureIndex, TableIndex, TableType,
};
use indexmap::IndexMap;
use rkyv::{
    de::SharedDeserializeRegistry, ser::ScratchSpace, ser::Serializer,
    ser::SharedSerializeRegistry, Archive, Archived, Fallible,
};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::Arc;

#[derive(Debug, Clone, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
pub struct ModuleId {
    id: usize,
}

impl ModuleId {
    pub fn id(&self) -> String {
        format!("{}", &self.id)
    }
}

impl Default for ModuleId {
    fn default() -> Self {
        static NEXT_ID: AtomicUsize = AtomicUsize::new(0);
        Self { id: NEXT_ID.fetch_add(1, SeqCst) }
    }
}

/// The counts of imported entities in a WebAssembly module.
#[derive(
    Debug, Copy, Clone, Default, PartialEq, Eq, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive,
)]
#[archive(as = "Self")]
pub struct ImportCounts {
    /// Number of imported functions in the module.
    pub functions: u32,

    /// Number of imported tables in the module.
    pub tables: u32,

    /// Number of imported memories in the module.
    pub memories: u32,

    /// Number of imported globals in the module.
    pub globals: u32,
}

impl ImportCounts {
    fn make_local<R: EntityRef, I: EntityRef>(idx: I, imports: u32) -> Result<R, I> {
        EntityRef::index(idx).checked_sub(imports as _).map(R::new).ok_or(idx)
    }

    /// Convert the `FunctionIndex` to a `LocalFunctionIndex`.
    pub fn local_function_index(
        &self,
        idx: FunctionIndex,
    ) -> Result<LocalFunctionIndex, FunctionIndex> {
        Self::make_local(idx, self.functions)
    }

    /// Convert the `TableIndex` to a `LocalTableIndex`.
    pub fn local_table_index(&self, idx: TableIndex) -> Result<LocalTableIndex, TableIndex> {
        Self::make_local(idx, self.tables)
    }

    /// Convert the `MemoryIndex` to a `LocalMemoryIndex`.
    pub fn local_memory_index(&self, idx: MemoryIndex) -> Result<LocalMemoryIndex, MemoryIndex> {
        Self::make_local(idx, self.memories)
    }

    /// Convert the `GlobalIndex` to a `LocalGlobalIndex`.
    pub fn local_global_index(&self, idx: GlobalIndex) -> Result<LocalGlobalIndex, GlobalIndex> {
        Self::make_local(idx, self.globals)
    }

    fn make_index<R: EntityRef, I: EntityRef>(idx: I, imports: u32) -> R {
        let imports = imports as usize;
        R::new(idx.index() + imports)
    }

    /// Convert the `LocalFunctionIndex` to a `FunctionIndex`.
    pub fn function_index(&self, idx: LocalFunctionIndex) -> FunctionIndex {
        Self::make_index(idx, self.functions)
    }

    /// Convert the `LocalTableIndex` to a `TableIndex`.
    pub fn table_index(&self, idx: LocalTableIndex) -> TableIndex {
        Self::make_index(idx, self.tables)
    }

    /// Convert the `LocalMemoryIndex` to a `MemoryIndex`.
    pub fn memory_index(&self, idx: LocalMemoryIndex) -> MemoryIndex {
        Self::make_index(idx, self.memories)
    }

    /// Convert the `LocalGlobalIndex` to a `GlobalIndex`.
    pub fn global_index(&self, idx: LocalGlobalIndex) -> GlobalIndex {
        Self::make_index(idx, self.globals)
    }
}

/// A translated WebAssembly module, excluding the function bodies and
/// memory initializers.
#[derive(Debug, Clone, Default)]
pub struct ModuleInfo {
    /// A unique identifier (within this process) for this module.
    ///
    /// We skip serialization/deserialization of this field, as it
    /// should be computed by the process.
    ///
    /// It's not skipped in rkyv, but that is okay, because even though it's skipped in
    /// bincode/serde it's still deserialized back as a garbage number, and later override from
    /// computed by the process
    pub id: ModuleId,

    /// The name of this wasm module, often found in the wasm file.
    pub name: Option<String>,

    /// Imported entities with the (module, field, index_of_the_import)
    ///
    /// Keeping the `index_of_the_import` is important, as there can be
    /// two same references to the same import, and we don't want to confuse
    /// them.
    pub imports: IndexMap<(String, String, u32), ImportIndex>,

    /// Exported entities.
    pub exports: IndexMap<String, ExportIndex>,

    /// The module "start" function, if present.
    pub start_function: Option<FunctionIndex>,

    /// WebAssembly table initializers.
    pub table_initializers: Vec<OwnedTableInitializer>,

    /// WebAssembly passive elements.
    pub passive_elements: BTreeMap<ElemIndex, Box<[FunctionIndex]>>,

    /// WebAssembly passive data segments.
    pub passive_data: BTreeMap<DataIndex, Arc<[u8]>>,

    /// WebAssembly global initializers.
    pub global_initializers: PrimaryMap<LocalGlobalIndex, GlobalInit>,

    /// WebAssembly function names.
    pub function_names: HashMap<FunctionIndex, String>,

    /// WebAssembly function signatures.
    pub signatures: PrimaryMap<SignatureIndex, FunctionType>,

    /// WebAssembly functions (imported and local).
    pub functions: PrimaryMap<FunctionIndex, SignatureIndex>,

    /// WebAssembly tables (imported and local).
    pub tables: PrimaryMap<TableIndex, TableType>,

    /// WebAssembly linear memories (imported and local).
    pub memories: PrimaryMap<MemoryIndex, MemoryType>,

    /// WebAssembly global variables (imported and local).
    pub globals: PrimaryMap<GlobalIndex, GlobalType>,

    /// Custom sections in the module.
    pub custom_sections: IndexMap<String, CustomSectionIndex>,

    /// The data for each CustomSection in the module.
    pub custom_sections_data: PrimaryMap<CustomSectionIndex, Arc<[u8]>>,

    /// The counts of imported entities.
    pub import_counts: ImportCounts,
}

/// Mirror version of ModuleInfo that can derive rkyv traits
#[derive(rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
pub struct ArchivableModuleInfo {
    pub name: Option<String>,
    pub imports: ArchivableIndexMap<(String, String, u32), ImportIndex>,
    pub exports: ArchivableIndexMap<String, ExportIndex>,
    pub start_function: Option<FunctionIndex>,
    pub table_initializers: Vec<OwnedTableInitializer>,
    pub passive_elements: BTreeMap<ElemIndex, Box<[FunctionIndex]>>,
    pub passive_data: BTreeMap<DataIndex, Arc<[u8]>>,
    pub global_initializers: PrimaryMap<LocalGlobalIndex, GlobalInit>,
    pub function_names: BTreeMap<FunctionIndex, String>,
    pub signatures: PrimaryMap<SignatureIndex, FunctionType>,
    pub functions: PrimaryMap<FunctionIndex, SignatureIndex>,
    pub tables: PrimaryMap<TableIndex, TableType>,
    pub memories: PrimaryMap<MemoryIndex, MemoryType>,
    pub globals: PrimaryMap<GlobalIndex, GlobalType>,
    pub custom_sections: ArchivableIndexMap<String, CustomSectionIndex>,
    pub custom_sections_data: PrimaryMap<CustomSectionIndex, Arc<[u8]>>,
    pub import_counts: ImportCounts,
}

impl From<ModuleInfo> for ArchivableModuleInfo {
    fn from(it: ModuleInfo) -> Self {
        Self {
            name: it.name,
            imports: ArchivableIndexMap::from(it.imports),
            exports: ArchivableIndexMap::from(it.exports),
            start_function: it.start_function,
            table_initializers: it.table_initializers,
            passive_elements: it.passive_elements.into_iter().collect(),
            passive_data: it.passive_data.into_iter().collect(),
            global_initializers: it.global_initializers,
            function_names: it.function_names.into_iter().collect(),
            signatures: it.signatures,
            functions: it.functions,
            tables: it.tables,
            memories: it.memories,
            globals: it.globals,
            custom_sections: ArchivableIndexMap::from(it.custom_sections),
            custom_sections_data: it.custom_sections_data,
            import_counts: it.import_counts,
        }
    }
}

impl From<ArchivableModuleInfo> for ModuleInfo {
    fn from(it: ArchivableModuleInfo) -> Self {
        Self {
            id: Default::default(),
            name: it.name,
            imports: it.imports.into(),
            exports: it.exports.into(),
            start_function: it.start_function,
            table_initializers: it.table_initializers,
            passive_elements: it.passive_elements.into_iter().collect(),
            passive_data: it.passive_data.into_iter().collect(),
            global_initializers: it.global_initializers,
            function_names: it.function_names.into_iter().collect(),
            signatures: it.signatures,
            functions: it.functions,
            tables: it.tables,
            memories: it.memories,
            globals: it.globals,
            custom_sections: it.custom_sections.into(),
            custom_sections_data: it.custom_sections_data,
            import_counts: it.import_counts,
        }
    }
}

impl From<&ModuleInfo> for ArchivableModuleInfo {
    fn from(it: &ModuleInfo) -> Self {
        Self::from(it.clone())
    }
}

impl Archive for ModuleInfo {
    type Archived = <ArchivableModuleInfo as Archive>::Archived;
    type Resolver = <ArchivableModuleInfo as Archive>::Resolver;

    unsafe fn resolve(&self, pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
        ArchivableModuleInfo::from(self).resolve(pos, resolver, out)
    }
}

impl<S: Serializer + SharedSerializeRegistry + ScratchSpace + ?Sized> rkyv::Serialize<S>
    for ModuleInfo
{
    fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        ArchivableModuleInfo::from(self).serialize(serializer)
    }
}

impl<D: Fallible + ?Sized + SharedDeserializeRegistry> rkyv::Deserialize<ModuleInfo, D>
    for Archived<ModuleInfo>
{
    fn deserialize(&self, deserializer: &mut D) -> Result<ModuleInfo, D::Error> {
        let r: ArchivableModuleInfo =
            rkyv::Deserialize::<ArchivableModuleInfo, D>::deserialize(self, deserializer)?;
        Ok(ModuleInfo::from(r))
    }
}

// For test serialization correctness, everything except module id should be same
impl PartialEq for ModuleInfo {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.imports == other.imports
            && self.exports == other.exports
            && self.start_function == other.start_function
            && self.table_initializers == other.table_initializers
            && self.passive_elements == other.passive_elements
            && self.passive_data == other.passive_data
            && self.global_initializers == other.global_initializers
            && self.function_names == other.function_names
            && self.signatures == other.signatures
            && self.functions == other.functions
            && self.tables == other.tables
            && self.memories == other.memories
            && self.globals == other.globals
            && self.custom_sections == other.custom_sections
            && self.custom_sections_data == other.custom_sections_data
            && self.import_counts == other.import_counts
    }
}

impl Eq for ModuleInfo {}

impl ModuleInfo {
    /// Allocates the module data structures.
    pub fn new() -> Self {
        Default::default()
    }

    /// Get the given passive element, if it exists.
    pub fn get_passive_element(&self, index: ElemIndex) -> Option<&[FunctionIndex]> {
        self.passive_elements.get(&index).map(|es| &**es)
    }

    /// Get the exported signatures of the module
    pub fn exported_signatures(&self) -> Vec<FunctionType> {
        self.exports
            .iter()
            .filter_map(|(_name, export_index)| match export_index {
                ExportIndex::Function(i) => {
                    let signature = self.functions.get(*i).unwrap();
                    let func_type = self.signatures.get(*signature).unwrap();
                    Some(func_type.clone())
                }
                _ => None,
            })
            .collect::<Vec<FunctionType>>()
    }

    /// Get the custom sections of the module given a `name`.
    pub fn custom_sections<'a>(&'a self, name: &'a str) -> impl Iterator<Item = Arc<[u8]>> + 'a {
        self.custom_sections.iter().filter_map(move |(section_name, section_index)| {
            if name != section_name {
                return None;
            }
            Some(self.custom_sections_data[*section_index].clone())
        })
    }

    /// Convert a `LocalFunctionIndex` into a `FunctionIndex`.
    pub fn func_index(&self, local_func: LocalFunctionIndex) -> FunctionIndex {
        self.import_counts.function_index(local_func)
    }

    /// Convert a `FunctionIndex` into a `LocalFunctionIndex`. Returns None if the
    /// index is an imported function.
    pub fn local_func_index(&self, func: FunctionIndex) -> Option<LocalFunctionIndex> {
        self.import_counts.local_function_index(func).ok()
    }

    /// Test whether the given function index is for an imported function.
    pub fn is_imported_function(&self, index: FunctionIndex) -> bool {
        self.local_func_index(index).is_none()
    }

    /// Convert a `LocalTableIndex` into a `TableIndex`.
    pub fn table_index(&self, local_table: LocalTableIndex) -> TableIndex {
        self.import_counts.table_index(local_table)
    }

    /// Convert a `TableIndex` into a `LocalTableIndex`. Returns None if the
    /// index is an imported table.
    pub fn local_table_index(&self, table: TableIndex) -> Option<LocalTableIndex> {
        self.import_counts.local_table_index(table).ok()
    }

    /// Test whether the given table index is for an imported table.
    pub fn is_imported_table(&self, index: TableIndex) -> bool {
        self.local_table_index(index).is_none()
    }

    /// Convert a `LocalMemoryIndex` into a `MemoryIndex`.
    pub fn memory_index(&self, local_memory: LocalMemoryIndex) -> MemoryIndex {
        self.import_counts.memory_index(local_memory)
    }

    /// Convert a `MemoryIndex` into a `LocalMemoryIndex`. Returns None if the
    /// index is an imported memory.
    pub fn local_memory_index(&self, memory: MemoryIndex) -> Option<LocalMemoryIndex> {
        self.import_counts.local_memory_index(memory).ok()
    }

    /// Test whether the given memory index is for an imported memory.
    pub fn is_imported_memory(&self, index: MemoryIndex) -> bool {
        self.local_memory_index(index).is_none()
    }

    /// Convert a `LocalGlobalIndex` into a `GlobalIndex`.
    pub fn global_index(&self, local_global: LocalGlobalIndex) -> GlobalIndex {
        self.import_counts.global_index(local_global)
    }

    /// Convert a `GlobalIndex` into a `LocalGlobalIndex`. Returns None if the
    /// index is an imported global.
    pub fn local_global_index(&self, global: GlobalIndex) -> Option<LocalGlobalIndex> {
        self.import_counts.local_global_index(global).ok()
    }

    /// Test whether the given global index is for an imported global.
    pub fn is_imported_global(&self, index: GlobalIndex) -> bool {
        self.local_global_index(index).is_none()
    }

    /// Get the Module name
    pub fn name(&self) -> String {
        match self.name {
            Some(ref name) => name.to_string(),
            None => "<module>".to_string(),
        }
    }

    /// Get the imported function types of the module.
    pub fn imported_function_types<'a>(&'a self) -> impl Iterator<Item = FunctionType> + 'a {
        self.functions
            .values()
            .take(self.import_counts.functions as usize)
            .map(move |sig_index| self.signatures[*sig_index].clone())
    }
}

impl fmt::Display for ModuleInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}
