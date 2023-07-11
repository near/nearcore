use super::externals::Function;
use super::store::{Store, StoreObject};
use near_vm_engine::RuntimeError;
use near_vm_types::Value;
pub use near_vm_types::{
    ExportType, ExternType, FunctionType, GlobalType, MemoryType, Mutability, TableType,
    Type as ValType,
};
use near_vm_vm::VMFuncRef;

/// WebAssembly computations manipulate values of basic value types:
/// * Integers (32 or 64 bit width)
/// * Floating-point (32 or 64 bit width)
/// * Vectors (128 bits, with 32 or 64 bit lanes)
///
/// Spec: <https://webassembly.github.io/spec/core/exec/runtime.html#values>
pub type Val = Value<Function>;

impl StoreObject for Val {
    fn comes_from_same_store(&self, store: &Store) -> bool {
        match self {
            Self::FuncRef(None) => true,
            Self::FuncRef(Some(f)) => Store::same(store, f.store()),
            // `ExternRef`s are not tied to specific stores
            Self::ExternRef(_) => true,
            Self::I32(_) | Self::I64(_) | Self::F32(_) | Self::F64(_) | Self::V128(_) => true,
        }
    }
}

impl From<Function> for Val {
    fn from(val: Function) -> Self {
        Self::FuncRef(Some(val))
    }
}

/// It provides useful functions for converting back and forth
/// from [`Val`] into `FuncRef`.
pub trait ValFuncRef {
    fn into_vm_funcref(&self, store: &Store) -> Result<VMFuncRef, RuntimeError>;

    unsafe fn from_vm_funcref(item: VMFuncRef, store: &Store) -> Self;

    fn into_table_reference(&self, store: &Store)
        -> Result<near_vm_vm::TableElement, RuntimeError>;

    unsafe fn from_table_reference(item: near_vm_vm::TableElement, store: &Store) -> Self;
}

impl ValFuncRef for Val {
    fn into_vm_funcref(&self, store: &Store) -> Result<VMFuncRef, RuntimeError> {
        if !self.comes_from_same_store(store) {
            return Err(RuntimeError::new("cross-`Store` values are not supported"));
        }
        Ok(match self {
            Self::FuncRef(None) => VMFuncRef::null(),
            Self::FuncRef(Some(f)) => f.vm_funcref(),
            _ => return Err(RuntimeError::new("val is not func ref")),
        })
    }

    /// # Safety
    ///
    /// The returned `Val` must outlive the containing instance.
    unsafe fn from_vm_funcref(func_ref: VMFuncRef, store: &Store) -> Self {
        Self::FuncRef(Function::from_vm_funcref(store, func_ref))
    }

    fn into_table_reference(
        &self,
        store: &Store,
    ) -> Result<near_vm_vm::TableElement, RuntimeError> {
        if !self.comes_from_same_store(store) {
            return Err(RuntimeError::new("cross-`Store` values are not supported"));
        }
        Ok(match self {
            // TODO(reftypes): review this clone
            Self::ExternRef(extern_ref) => {
                near_vm_vm::TableElement::ExternRef(extern_ref.clone().into())
            }
            Self::FuncRef(None) => near_vm_vm::TableElement::FuncRef(VMFuncRef::null()),
            Self::FuncRef(Some(f)) => near_vm_vm::TableElement::FuncRef(f.vm_funcref()),
            _ => return Err(RuntimeError::new("val is not reference")),
        })
    }

    /// # Safety
    ///
    /// The returned `Val` may not outlive the containing instance.
    unsafe fn from_table_reference(item: near_vm_vm::TableElement, store: &Store) -> Self {
        match item {
            near_vm_vm::TableElement::FuncRef(f) => Self::from_vm_funcref(f, store),
            near_vm_vm::TableElement::ExternRef(extern_ref) => Self::ExternRef(extern_ref.into()),
        }
    }
}
