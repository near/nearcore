use crate::MemoryError;
use crate::{Memory, Table};
use crate::{MemoryStyle, TableStyle};
use crate::{VMMemoryDefinition, VMTableDefinition};
use near_vm_types::{MemoryType, TableType};
use std::ptr::NonNull;
use std::sync::Arc;

/// An engine delegates the creation of memories, tables, and globals
/// to a foreign implementor of this trait.
pub trait Tunables: Sync {
    /// Construct a `MemoryStyle` for the provided `MemoryType`
    fn memory_style(&self, memory: &MemoryType) -> MemoryStyle;

    /// Construct a `TableStyle` for the provided `TableType`
    fn table_style(&self, table: &TableType) -> TableStyle;

    /// Create a memory owned by the host given a [`MemoryType`] and a [`MemoryStyle`].
    fn create_host_memory(
        &self,
        ty: &MemoryType,
        style: &MemoryStyle,
    ) -> Result<Arc<dyn Memory>, MemoryError>;

    /// Create a memory owned by the VM given a [`MemoryType`] and a [`MemoryStyle`].
    ///
    /// # Safety
    /// - `vm_definition_location` must point to a valid location in VM memory.
    unsafe fn create_vm_memory(
        &self,
        ty: &MemoryType,
        style: &MemoryStyle,
        vm_definition_location: NonNull<VMMemoryDefinition>,
    ) -> Result<Arc<dyn Memory>, MemoryError>;

    /// Create a table owned by the host given a [`TableType`] and a [`TableStyle`].
    fn create_host_table(
        &self,
        ty: &TableType,
        style: &TableStyle,
    ) -> Result<Arc<dyn Table>, String>;

    /// Create a table owned by the VM given a [`TableType`] and a [`TableStyle`].
    ///
    /// # Safety
    /// - `vm_definition_location` must point to a valid location in VM memory.
    unsafe fn create_vm_table(
        &self,
        ty: &TableType,
        style: &TableStyle,
        vm_definition_location: NonNull<VMTableDefinition>,
    ) -> Result<Arc<dyn Table>, String>;

    /// Instrumentation configuration: stack limiter config
    fn stack_limiter_cfg(&self) -> Box<dyn finite_wasm::max_stack::SizeConfig>;

    /// Instrumentation configuration: gas accounting config
    fn gas_cfg(&self) -> Box<dyn finite_wasm::wasmparser::VisitOperator<Output = u64>>;

    /// Cost for initializing a stack frame
    fn stack_init_gas_cost(&self, frame_size: u64) -> u64;
}

#[doc(hidden)]
pub struct TestTunables;

impl Tunables for TestTunables {
    fn memory_style(&self, _memory: &MemoryType) -> MemoryStyle {
        unimplemented!()
    }

    fn table_style(&self, _table: &TableType) -> TableStyle {
        unimplemented!()
    }

    fn create_host_memory(
        &self,
        _ty: &MemoryType,
        _style: &MemoryStyle,
    ) -> Result<Arc<dyn Memory>, MemoryError> {
        unimplemented!()
    }

    unsafe fn create_vm_memory(
        &self,
        _ty: &MemoryType,
        _style: &MemoryStyle,
        _vm_definition_location: NonNull<VMMemoryDefinition>,
    ) -> Result<Arc<dyn Memory>, MemoryError> {
        unimplemented!()
    }

    fn create_host_table(
        &self,
        _ty: &TableType,
        _style: &TableStyle,
    ) -> Result<Arc<dyn Table>, String> {
        unimplemented!()
    }

    unsafe fn create_vm_table(
        &self,
        _ty: &TableType,
        _style: &TableStyle,
        _vm_definition_location: NonNull<VMTableDefinition>,
    ) -> Result<Arc<dyn Table>, String> {
        unimplemented!()
    }

    fn stack_limiter_cfg(&self) -> Box<dyn finite_wasm::max_stack::SizeConfig> {
        unimplemented!()
    }

    fn gas_cfg(&self) -> Box<dyn finite_wasm::wasmparser::VisitOperator<Output = u64>> {
        unimplemented!()
    }

    fn stack_init_gas_cost(&self, _frame_size: u64) -> u64 {
        unimplemented!()
    }
}
