use super::super::exports::Exportable;
use super::super::store::{Store, StoreObject};
use super::super::types::GlobalType;
use super::super::types::Mutability;
use super::super::types::Val;
use near_vm_engine::RuntimeError;
use near_vm_vm::{Export, Global as RuntimeGlobal, VMGlobal};
use std::fmt;
use std::sync::Arc;

/// A WebAssembly `global` instance.
///
/// A global instance is the runtime representation of a global variable.
/// It consists of an individual value and a flag indicating whether it is mutable.
///
/// Spec: <https://webassembly.github.io/spec/core/exec/runtime.html#global-instances>
pub struct Global {
    store: Store,
    vm_global: VMGlobal,
}

impl Global {
    /// Create a new `Global` with the initial value [`Val`].
    pub fn new(store: &Store, val: Val) -> Self {
        Self::from_value(store, val, Mutability::Const).unwrap()
    }

    /// Create a mutable `Global` with the initial value [`Val`].
    pub fn new_mut(store: &Store, val: Val) -> Self {
        Self::from_value(store, val, Mutability::Var).unwrap()
    }

    /// Create a `Global` with the initial value [`Val`] and the provided [`Mutability`].
    fn from_value(store: &Store, val: Val, mutability: Mutability) -> Result<Self, RuntimeError> {
        if !val.comes_from_same_store(store) {
            return Err(RuntimeError::new("cross-`Store` globals are not supported"));
        }
        let global = RuntimeGlobal::new(GlobalType { mutability, ty: val.ty() });
        unsafe {
            global
                .set_unchecked(val.clone())
                .map_err(|e| RuntimeError::new(format!("create global for {:?}: {}", val, e)))?;
        };

        Ok(Self {
            store: store.clone(),
            vm_global: VMGlobal { from: Arc::new(global), instance_ref: None },
        })
    }

    /// Returns the [`GlobalType`] of the `Global`.
    pub fn ty(&self) -> &GlobalType {
        self.vm_global.from.ty()
    }

    /// Returns the [`Store`] where the `Global` belongs.
    ///
    /// # Example
    ///
    /// ```
    /// # use near_vm_test_api::{Global, Store, Value};
    /// # let store = Store::default();
    /// #
    /// let g = Global::new(&store, Value::I32(1));
    ///
    /// assert_eq!(g.store(), &store);
    /// ```
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// Retrieves the current value [`Val`] that the Global has.
    ///
    /// # Example
    ///
    /// ```
    /// # use near_vm_test_api::{Global, Store, Value};
    /// # let store = Store::default();
    /// #
    /// let g = Global::new(&store, Value::I32(1));
    ///
    /// assert_eq!(g.get(), Value::I32(1));
    /// ```
    pub fn get(&self) -> Val {
        self.vm_global.from.get(&self.store)
    }

    /// Sets a custom value [`Val`] to the runtime Global.
    ///
    /// # Example
    ///
    /// ```
    /// # use near_vm_test_api::{Global, Store, Value};
    /// # let store = Store::default();
    /// #
    /// let g = Global::new_mut(&store, Value::I32(1));
    ///
    /// assert_eq!(g.get(), Value::I32(1));
    ///
    /// g.set(Value::I32(2));
    ///
    /// assert_eq!(g.get(), Value::I32(2));
    /// ```
    ///
    /// # Errors
    ///
    /// Trying to mutate a immutable global will raise an error:
    ///
    /// ```should_panic
    /// # use near_vm_test_api::{Global, Store, Value};
    /// # let store = Store::default();
    /// #
    /// let g = Global::new(&store, Value::I32(1));
    ///
    /// g.set(Value::I32(2)).unwrap();
    /// ```
    ///
    /// Trying to set a value of a incompatible type will raise an error:
    ///
    /// ```should_panic
    /// # use near_vm_test_api::{Global, Store, Value};
    /// # let store = Store::default();
    /// #
    /// let g = Global::new(&store, Value::I32(1));
    ///
    /// // This results in an error: `RuntimeError`.
    /// g.set(Value::I64(2)).unwrap();
    /// ```
    pub fn set(&self, val: Val) -> Result<(), RuntimeError> {
        if !val.comes_from_same_store(&self.store) {
            return Err(RuntimeError::new("cross-`Store` values are not supported"));
        }
        unsafe {
            self.vm_global.from.set(val).map_err(|e| RuntimeError::new(format!("{}", e)))?;
        }
        Ok(())
    }

    pub(crate) fn from_vm_export(store: &Store, vm_global: VMGlobal) -> Self {
        Self { store: store.clone(), vm_global }
    }
}

impl Clone for Global {
    fn clone(&self) -> Self {
        let mut vm_global = self.vm_global.clone();
        vm_global.upgrade_instance_ref().unwrap();

        Self { store: self.store.clone(), vm_global }
    }
}

impl fmt::Debug for Global {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("Global")
            .field("ty", &self.ty())
            .field("value", &self.get())
            .finish()
    }
}

impl<'a> Exportable<'a> for Global {
    fn to_export(&self) -> Export {
        self.vm_global.clone().into()
    }
}
