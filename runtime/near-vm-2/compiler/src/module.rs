use crate::lib::std::sync::Arc;
use near_vm_types::entity::PrimaryMap;
use near_vm_types::{Features, MemoryIndex, ModuleInfo, TableIndex};
use near_vm_vm::{MemoryStyle, TableStyle};

/// The required info for compiling a module.
///
/// This differs from [`ModuleInfo`] because it have extra info only
/// possible after translation (such as the features used for compiling,
/// or the `MemoryStyle` and `TableStyle`).
#[derive(Debug, PartialEq, Eq, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
pub struct CompileModuleInfo {
    /// The features used for compiling the module
    pub features: Features,
    /// The module information
    pub module: Arc<ModuleInfo>,
    /// The memory styles used for compiling.
    ///
    /// The compiler will emit the most optimal code based
    /// on the memory style (static or dynamic) chosen.
    pub memory_styles: PrimaryMap<MemoryIndex, MemoryStyle>,
    /// The table plans used for compiling.
    pub table_styles: PrimaryMap<TableIndex, TableStyle>,
}
