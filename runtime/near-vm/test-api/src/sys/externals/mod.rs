pub(crate) mod function;
mod global;
mod memory;
mod table;

pub use self::function::{
    FromToNativeWasmType, Function, HostFunction, WasmTypeList, WithEnv, WithoutEnv,
};

pub use self::global::Global;
pub use self::memory::Memory;
pub use self::table::Table;

use super::exports::Exportable;
use super::store::{Store, StoreObject};
use near_vm_vm::Export;
use std::fmt;

/// An `Extern` is the runtime representation of an entity that
/// can be imported or exported.
///
/// Spec: <https://webassembly.github.io/spec/core/exec/runtime.html#external-values>
#[derive(Clone)]
pub enum Extern {
    /// A external [`Function`].
    Function(Function),
    /// A external [`Global`].
    Global(Global),
    /// A external [`Table`].
    Table(Table),
    /// A external [`Memory`].
    Memory(Memory),
}

impl Extern {
    /// Create an `Extern` from an `near_vm_engine::Export`.
    pub fn from_vm_export(store: &Store, export: Export) -> Self {
        match export {
            Export::Function(f) => Self::Function(Function::from_vm_export(store, f)),
            Export::Memory(m) => Self::Memory(Memory::from_vm_export(store, m)),
            Export::Global(g) => Self::Global(Global::from_vm_export(store, g)),
            Export::Table(t) => Self::Table(Table::from_vm_export(store, t)),
        }
    }
}

impl<'a> Exportable<'a> for Extern {
    fn to_export(&self) -> Export {
        match self {
            Self::Function(f) => f.to_export(),
            Self::Global(g) => g.to_export(),
            Self::Memory(m) => m.to_export(),
            Self::Table(t) => t.to_export(),
        }
    }
}

impl StoreObject for Extern {
    fn comes_from_same_store(&self, store: &Store) -> bool {
        let my_store = match self {
            Self::Function(f) => f.store(),
            Self::Global(g) => g.store(),
            Self::Memory(m) => m.store(),
            Self::Table(t) => t.store(),
        };
        Store::same(my_store, store)
    }
}

impl fmt::Debug for Extern {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Function(_) => "Function(...)",
                Self::Global(_) => "Global(...)",
                Self::Memory(_) => "Memory(...)",
                Self::Table(_) => "Table(...)",
            }
        )
    }
}

impl From<Function> for Extern {
    fn from(r: Function) -> Self {
        Self::Function(r)
    }
}

impl From<Global> for Extern {
    fn from(r: Global) -> Self {
        Self::Global(r)
    }
}

impl From<Memory> for Extern {
    fn from(r: Memory) -> Self {
        Self::Memory(r)
    }
}

impl From<Table> for Extern {
    fn from(r: Table) -> Self {
        Self::Table(r)
    }
}
