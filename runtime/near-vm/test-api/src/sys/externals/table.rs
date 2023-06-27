// This is test code...
#![allow(clippy::vtable_address_comparisons)]

use super::super::exports::Exportable;
use super::super::store::Store;
use super::super::types::TableType;
use super::super::types::{Val, ValFuncRef};
use near_vm_engine::RuntimeError;
use near_vm_vm::{Export, Table as RuntimeTable, TableElement, VMTable};
use std::sync::Arc;

/// A WebAssembly `table` instance.
///
/// The `Table` struct is an array-like structure representing a WebAssembly Table,
/// which stores function references.
///
/// A table created by the host or in WebAssembly code will be accessible and
/// mutable from both host and WebAssembly.
///
/// Spec: <https://webassembly.github.io/spec/core/exec/runtime.html#table-instances>
pub struct Table {
    store: Store,
    vm_table: VMTable,
}

fn set_table_item(
    table: &dyn RuntimeTable,
    item_index: u32,
    item: TableElement,
) -> Result<(), RuntimeError> {
    table.set(item_index, item).map_err(|e| e.into())
}

impl Table {
    /// Creates a new `Table` with the provided [`TableType`] definition.
    ///
    /// All the elements in the table will be set to the `init` value.
    ///
    /// This function will construct the `Table` using the store
    /// [`BaseTunables`][crate::sys::BaseTunables].
    pub fn new(store: &Store, ty: TableType, init: Val) -> Result<Self, RuntimeError> {
        let item = init.into_table_reference(store)?;
        let tunables = store.tunables();
        let style = tunables.table_style(&ty);
        let table = tunables.create_host_table(&ty, &style).map_err(RuntimeError::new)?;

        let num_elements = table.size();
        for i in 0..num_elements {
            set_table_item(table.as_ref(), i, item.clone())?;
        }

        Ok(Self { store: store.clone(), vm_table: VMTable { from: table, instance_ref: None } })
    }

    /// Returns the [`TableType`] of the `Table`.
    pub fn ty(&self) -> &TableType {
        self.vm_table.from.ty()
    }

    /// Returns the [`Store`] where the `Table` belongs.
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// Retrieves the size of the `Table` (in elements)
    pub fn size(&self) -> u32 {
        self.vm_table.from.size()
    }

    pub(crate) fn from_vm_export(store: &Store, vm_table: VMTable) -> Self {
        Self { store: store.clone(), vm_table }
    }

    /// Returns whether or not these two tables refer to the same data.
    pub fn same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.vm_table.from, &other.vm_table.from)
    }

    /// Get access to the backing VM value for this extern. This function is for
    /// tests it should not be called by users of the Wasmer API.
    ///
    /// # Safety
    /// This function is unsafe to call outside of tests for the near-vm crate
    /// because there is no stability guarantee for the returned type and we may
    /// make breaking changes to it at any time or remove this method.
    #[doc(hidden)]
    pub unsafe fn get_vm_table(&self) -> &VMTable {
        &self.vm_table
    }
}

impl Clone for Table {
    fn clone(&self) -> Self {
        let mut vm_table = self.vm_table.clone();
        vm_table.upgrade_instance_ref().unwrap();

        Self { store: self.store.clone(), vm_table }
    }
}

impl<'a> Exportable<'a> for Table {
    fn to_export(&self) -> Export {
        self.vm_table.clone().into()
    }
}
