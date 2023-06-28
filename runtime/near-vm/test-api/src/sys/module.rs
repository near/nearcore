use super::instance::InstantiationError;
use super::store::Store;
use near_vm_compiler::CompileError;
#[cfg(feature = "wat")]
use near_vm_compiler::WasmError;
use near_vm_engine::RuntimeError;
use near_vm_types::InstanceConfig;
use near_vm_vm::{InstanceHandle, Instantiatable, Resolver};
use std::fmt;
use std::io;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum IoCompileError {
    /// An IO error
    #[error(transparent)]
    Io(#[from] io::Error),
    /// A compilation error
    #[error(transparent)]
    Compile(#[from] CompileError),
}

/// A WebAssembly Module contains stateless WebAssembly
/// code that has already been compiled and can be instantiated
/// multiple times.
///
/// ## Cloning a module
///
/// Cloning a module is cheap: it does a shallow copy of the compiled
/// contents rather than a deep copy.
#[derive(Clone)]
pub struct Module {
    store: Store,
    artifact: Arc<near_vm_engine::universal::UniversalArtifact>,
}

impl Module {
    /// Creates a new WebAssembly Module given the configuration
    /// in the store.
    ///
    /// If the provided bytes are not WebAssembly-like (start with `b"\0asm"`),
    /// and the "wat" feature is enabled for this crate, this function will try to
    /// to convert the bytes assuming they correspond to the WebAssembly text
    /// format.
    ///
    /// ## Security
    ///
    /// Before the code is compiled, it will be validated using the store
    /// features.
    ///
    /// ## Errors
    ///
    /// Creating a WebAssembly module from bytecode can result in a
    /// [`CompileError`] since this operation requires to transorm the Wasm
    /// bytecode into code the machine can easily execute.
    ///
    /// ## Example
    ///
    /// Reading from a WAT file.
    ///
    /// ```
    /// use near_vm_test_api::*;
    /// # fn main() -> anyhow::Result<()> {
    /// # let store = Store::default();
    /// let wat = "(module)";
    /// let module = Module::new(&store, wat)?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Reading from bytes:
    ///
    /// ```
    /// use near_vm_test_api::*;
    /// # fn main() -> anyhow::Result<()> {
    /// # let store = Store::default();
    /// // The following is the same as:
    /// // (module
    /// //   (type $t0 (func (param i32) (result i32)))
    /// //   (func $add_one (export "add_one") (type $t0) (param $p0 i32) (result i32)
    /// //     get_local $p0
    /// //     i32.const 1
    /// //     i32.add)
    /// // )
    /// let bytes: Vec<u8> = vec![
    ///     0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x06, 0x01, 0x60,
    ///     0x01, 0x7f, 0x01, 0x7f, 0x03, 0x02, 0x01, 0x00, 0x07, 0x0b, 0x01, 0x07,
    ///     0x61, 0x64, 0x64, 0x5f, 0x6f, 0x6e, 0x65, 0x00, 0x00, 0x0a, 0x09, 0x01,
    ///     0x07, 0x00, 0x20, 0x00, 0x41, 0x01, 0x6a, 0x0b, 0x00, 0x1a, 0x04, 0x6e,
    ///     0x61, 0x6d, 0x65, 0x01, 0x0a, 0x01, 0x00, 0x07, 0x61, 0x64, 0x64, 0x5f,
    ///     0x6f, 0x6e, 0x65, 0x02, 0x07, 0x01, 0x00, 0x01, 0x00, 0x02, 0x70, 0x30,
    /// ];
    /// let module = Module::new(&store, bytes)?;
    /// # Ok(())
    /// # }
    /// ```
    #[allow(unreachable_code)]
    #[tracing::instrument(skip_all)]
    pub fn new(store: &Store, bytes: impl AsRef<[u8]>) -> Result<Self, CompileError> {
        #[cfg(feature = "wat")]
        let bytes = wat::parse_bytes(bytes.as_ref()).map_err(|e| {
            CompileError::Wasm(WasmError::Generic(format!("Error when converting wat: {}", e)))
        })?;

        Self::from_binary(store, bytes.as_ref())
    }

    /// Creates a new WebAssembly module from a binary.
    ///
    /// Opposed to [`Module::new`], this function is not compatible with
    /// the WebAssembly text format (if the "wat" feature is enabled for
    /// this crate).
    #[tracing::instrument(skip_all)]
    pub(crate) fn from_binary(store: &Store, binary: &[u8]) -> Result<Self, CompileError> {
        let engine = store.engine();
        engine.validate(binary)?;
        let executable = engine.compile_universal(binary, store.tunables())?;
        let artifact = engine.load_universal_executable(&executable)?;
        Ok(Self { store: store.clone(), artifact: Arc::new(artifact) })
    }

    pub(crate) fn instantiate(
        &self,
        resolver: &dyn Resolver,
        config: InstanceConfig,
    ) -> Result<InstanceHandle, InstantiationError> {
        unsafe {
            let instance_handle = Arc::clone(&self.artifact).instantiate(
                self.store.tunables(),
                resolver,
                Box::new((self.store.clone(), Arc::clone(&self.artifact))),
                config,
            )?;

            // After the instance handle is created, we need to initialize
            // the data, call the start function and so. However, if any
            // of this steps traps, we still need to keep the instance alive
            // as some of the Instance elements may have placed in other
            // instance tables.
            instance_handle
                .finish_instantiation()
                .map_err(|t| InstantiationError::Start(RuntimeError::from_trap(t)))?;

            Ok(instance_handle)
        }
    }

    /// Returns the [`Store`] where the `Instance` belongs.
    pub fn store(&self) -> &Store {
        &self.store
    }
}

impl fmt::Debug for Module {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Module").finish()
    }
}
