use super::env::HostEnvInitError;
use super::exports::ExportError;
use super::externals::WasmTypeList;
use super::module::Module;
use super::native::NativeFunc;
use near_vm_engine::{LinkError, RuntimeError};
use near_vm_vm::{Export, InstanceHandle, Resolver};
use std::sync::{Arc, Mutex};
use thiserror::Error;

pub use near_vm_types::InstanceConfig;

/// A WebAssembly Instance is a stateful, executable
/// instance of a WebAssembly [`Module`].
///
/// Instance objects contain all the exported WebAssembly
/// functions, memories, tables and globals that allow
/// interacting with WebAssembly.
///
/// Spec: <https://webassembly.github.io/spec/core/exec/runtime.html#module-instances>
#[derive(Clone)]
pub struct Instance {
    handle: Arc<Mutex<InstanceHandle>>,
    module: Module,
}

/// An error while instantiating a module.
///
/// This is not a common WebAssembly error, however
/// we need to differentiate from a `LinkError` (an error
/// that happens while linking, on instantiation), a
/// Trap that occurs when calling the WebAssembly module
/// start function, and an error when initializing the user's
/// host environments.
#[derive(Error, Debug)]
pub enum InstantiationError {
    /// A linking ocurred during instantiation.
    #[error(transparent)]
    Link(LinkError),

    /// A runtime error occured while invoking the start function
    #[error("could not invoke the start function: {0}")]
    Start(RuntimeError),

    /// The module was compiled with a CPU feature that is not available on
    /// the current host.
    #[error("missing requires CPU features: {0:?}")]
    CpuFeature(String),

    /// Error occurred when initializing the host environment.
    #[error(transparent)]
    HostEnvInitialization(HostEnvInitError),
}

impl From<near_vm_engine::InstantiationError> for InstantiationError {
    fn from(other: near_vm_engine::InstantiationError) -> Self {
        match other {
            near_vm_engine::InstantiationError::Link(e) => Self::Link(e),
            near_vm_engine::InstantiationError::Start(e) => Self::Start(e),
            near_vm_engine::InstantiationError::CpuFeature(e) => Self::CpuFeature(e),
        }
    }
}

impl From<HostEnvInitError> for InstantiationError {
    fn from(other: HostEnvInitError) -> Self {
        Self::HostEnvInitialization(other)
    }
}

impl Instance {
    /// Creates a new `Instance` from a WebAssembly [`Module`] and a
    /// set of imports resolved by the [`Resolver`].
    ///
    /// The resolver can be anything that implements the [`Resolver`] trait,
    /// so you can plug custom resolution for the imports, if you wish not
    /// to use [`ImportObject`].
    ///
    /// The [`ImportObject`] is the easiest way to provide imports to the instance.
    ///
    /// [`ImportObject`]: crate::ImportObject
    ///
    /// ```
    /// # use near_vm_test_api::{imports, Store, Module, Global, Value, Instance, InstanceConfig};
    /// # fn main() -> anyhow::Result<()> {
    /// let store = Store::default();
    /// let module = Module::new(&store, "(module)")?;
    /// let imports = imports!{
    ///   "host" => {
    ///     "var" => Global::new(&store, Value::I32(2))
    ///   }
    /// };
    /// let instance = Instance::new_with_config(&module, InstanceConfig::with_stack_limit(1000), &imports)?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## Errors
    ///
    /// The function can return [`InstantiationError`]s.
    ///
    /// Those are, as defined by the spec:
    ///  * Link errors that happen when plugging the imports into the instance
    ///  * Runtime errors that happen when running the module `start` function.
    #[tracing::instrument(skip_all)]
    pub fn new_with_config(
        module: &Module,
        config: InstanceConfig,
        resolver: &dyn Resolver,
    ) -> Result<Self, InstantiationError> {
        let handle = module.instantiate(resolver, config)?;
        let instance = Self { handle: Arc::new(Mutex::new(handle)), module: module.clone() };

        // # Safety
        // `initialize_host_envs` should be called after instantiation but before
        // returning an `Instance` to the user. We set up the host environments
        // via `WasmerEnv::init_with_instance`.
        //
        // This usage is correct because we pass a valid pointer to `instance` and the
        // correct error type returned by `WasmerEnv::init_with_instance` as a generic
        // parameter.
        unsafe {
            near_vm_vm::initialize_host_envs::<HostEnvInitError>(
                &*instance.handle,
                &instance as *const _ as *const _,
            )?;
        }

        Ok(instance)
    }

    /// Lookup an exported entity by its name.
    pub fn lookup(&self, field: &str) -> Option<Export> {
        let vmextern = self.handle.lock().unwrap().lookup(field)?;
        Some(vmextern.into())
    }

    /// Lookup an exported function by its name.
    pub fn lookup_function(&self, field: &str) -> Option<super::externals::Function> {
        if let Export::Function(f) = self.lookup(field)? {
            Some(super::externals::Function::from_vm_export(self.module.store(), f))
        } else {
            None
        }
    }

    /// Get an export as a `NativeFunc`.
    pub fn get_native_function<Args, Rets>(
        &self,
        name: &str,
    ) -> Result<NativeFunc<Args, Rets>, ExportError>
    where
        Args: WasmTypeList,
        Rets: WasmTypeList,
    {
        match self.lookup(name) {
            Some(Export::Function(f)) => {
                super::externals::Function::from_vm_export(self.module.store(), f)
                    .native()
                    .map_err(|_| ExportError::IncompatibleType)
            }
            Some(_) => Err(ExportError::IncompatibleType),
            None => Err(ExportError::Missing("not found".into())),
        }
    }

    // Used internally by wast only
    #[doc(hidden)]
    pub fn handle(&self) -> std::sync::MutexGuard<'_, InstanceHandle> {
        self.handle.lock().unwrap()
    }
}
