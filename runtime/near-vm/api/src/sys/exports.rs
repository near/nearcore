use crate::sys::externals::Extern;
use crate::sys::import_object::LikeNamespace;
use indexmap::IndexMap;
use near_vm_vm::Export;
use std::sync::Arc;
use thiserror::Error;

/// The `ExportError` can happen when trying to get a specific
/// export [`Extern`] from the [`Instance`] exports.
///
/// [`Instance`]: crate::Instance
///
/// # Examples
///
/// ## Incompatible export type
///
/// ```should_panic
/// # use near_vm::{imports, wat2wasm, Function, Instance, InstanceConfig, Module, Store, Type, Value, ExportError};
/// # let store = Store::default();
/// # let wasm_bytes = wat2wasm(r#"
/// # (module
/// #   (global $one (export "glob") f32 (f32.const 1)))
/// # "#.as_bytes()).unwrap();
/// # let module = Module::new(&store, wasm_bytes).unwrap();
/// # let import_object = imports! {};
/// # let instance = Instance::new_with_config(&module, InstanceConfig::with_stack_limit(1000000), &import_object).unwrap();
/// #
/// // This results in an error.
/// let export = instance.lookup_function("glob").unwrap();
/// ```
///
/// ## Missing export
///
/// ```should_panic
/// # use near_vm::{imports, wat2wasm, Function, Instance, InstanceConfig, Module, Store, Type, Value, ExportError};
/// # let store = Store::default();
/// # let wasm_bytes = wat2wasm("(module)".as_bytes()).unwrap();
/// # let module = Module::new(&store, wasm_bytes).unwrap();
/// # let import_object = imports! {};
/// # let instance = Instance::new_with_config(&module, InstanceConfig::with_stack_limit(1000000), &import_object).unwrap();
/// #
/// // This results with an error: `ExportError::Missing`.
/// let export = instance.lookup("unknown").unwrap();
/// ```
#[derive(Error, Debug)]
pub enum ExportError {
    /// An error than occurs when the exported type and the expected type
    /// are incompatible.
    #[error("Incompatible Export Type")]
    IncompatibleType,
    /// This error arises when an export is missing
    #[error("Missing export {0}")]
    Missing(String),
}

/// Exports is a special kind of map that allows easily unwrapping
/// the types of instances.
///
/// TODO: add examples of using exports
#[derive(Clone, Default)]
pub struct Exports {
    map: Arc<IndexMap<String, Extern>>,
}

impl Exports {
    /// Creates a new `Exports`.
    pub fn new() -> Self {
        Default::default()
    }

    /// Insert a new export into this `Exports` map.
    pub fn insert<S, E>(&mut self, name: S, value: E)
    where
        S: Into<String>,
        E: Into<Extern>,
    {
        Arc::get_mut(&mut self.map).unwrap().insert(name.into(), value.into());
    }
}

impl LikeNamespace for Exports {
    fn get_namespace_export(&self, name: &str) -> Option<Export> {
        self.map.get(name).map(|is_export| is_export.to_export())
    }

    fn get_namespace_exports(&self) -> Vec<(String, Export)> {
        self.map.iter().map(|(k, v)| (k.clone(), v.to_export())).collect()
    }
}

/// This trait is used to mark types as gettable from an [`Instance`].
///
/// [`Instance`]: crate::Instance
pub trait Exportable<'a>: Sized {
    /// This function is used when providedd the [`Extern`] as exportable, so it
    /// can be used while instantiating the [`Module`].
    ///
    /// [`Module`]: crate::Module
    fn to_export(&self) -> Export;
}
