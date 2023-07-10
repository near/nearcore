use crate::sys::tunables::BaseTunables;
#[cfg(all(feature = "compiler", feature = "engine"))]
use near_vm_compiler::CompilerConfig;
use near_vm_engine::universal::UniversalEngine;
use near_vm_vm::Tunables;
use std::fmt;
use std::sync::Arc;

/// The store represents all global state that can be manipulated by
/// WebAssembly programs. It consists of the runtime representation
/// of all instances of functions, tables, memories, and globals that
/// have been allocated during the lifetime of the abstract machine.
///
/// The `Store` holds the engine (that is —amongst many things— used to compile
/// the Wasm bytes into a valid module artifact), in addition to the
/// [`Tunables`] (that are used to create the memories, tables and globals).
///
/// Spec: <https://webassembly.github.io/spec/core/exec/runtime.html#store>
#[derive(Clone)]
pub struct Store {
    engine: Arc<UniversalEngine>,
    tunables: Arc<dyn Tunables + Send + Sync>,
}

impl Store {
    /// Creates a new `Store` with a specific [`Engine`].
    pub fn new(engine: Arc<UniversalEngine>) -> Self {
        Self::new_with_tunables(Arc::clone(&engine), BaseTunables::for_target(engine.target()))
    }

    /// Creates a new `Store` with a specific [`Engine`] and [`Tunables`].
    pub fn new_with_tunables(
        engine: Arc<UniversalEngine>,
        tunables: impl Tunables + Send + Sync + 'static,
    ) -> Self {
        Self { engine, tunables: Arc::new(tunables) }
    }

    /// Returns the [`Tunables`].
    pub fn tunables(&self) -> &dyn Tunables {
        self.tunables.as_ref()
    }

    /// Returns the [`Engine`].
    pub fn engine(&self) -> Arc<UniversalEngine> {
        Arc::clone(&self.engine)
    }

    /// Checks whether two stores are identical. A store is considered
    /// equal to another store if both have the same engine. The
    /// tunables are excluded from the logic.
    pub fn same(a: &Self, b: &Self) -> bool {
        a.engine.id() == b.engine.id()
    }
}

impl PartialEq for Store {
    fn eq(&self, other: &Self) -> bool {
        Self::same(self, other)
    }
}

unsafe impl Send for Store {}
unsafe impl Sync for Store {}

// We only implement default if we have assigned a default compiler and engine
#[cfg(all(feature = "default-compiler", feature = "default-engine"))]
impl Default for Store {
    fn default() -> Self {
        // We store them on a function that returns to make
        // sure this function doesn't emit a compile error even if
        // more than one compiler is enabled.
        #[allow(unreachable_code)]
        fn get_config() -> impl CompilerConfig + 'static {
            cfg_if::cfg_if! {
                if #[cfg(feature = "default-singlepass")] {
                    near_vm_compiler_singlepass::Singlepass::default()
                } else {
                    compile_error!("No default compiler chosen")
                }
            }
        }

        #[allow(unreachable_code, unused_mut)]
        fn get_engine(mut config: impl CompilerConfig + 'static) -> UniversalEngine {
            cfg_if::cfg_if! {
                if #[cfg(feature = "default-universal")] {
                    let pool = near_vm_engine::universal::LimitedMemoryPool::new(1, 0x10000).unwrap();
                    near_vm_engine::universal::Universal::new(config)
                        .code_memory_pool(pool)
                        .engine()
                } else if #[cfg(feature = "default-dylib")] {
                    near_vm_engine_dylib::Dylib::new(config)
                        .engine()
                } else {
                    compile_error!("No default engine chosen")
                }
            }
        }

        let config = get_config();
        let engine = get_engine(config);
        let tunables = BaseTunables::for_target(engine.target());
        Self::new_with_tunables(engine.into(), tunables)
    }
}

impl fmt::Debug for Store {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Store").finish()
    }
}

/// A trait represinting any object that lives in the `Store`.
pub trait StoreObject {
    /// Return true if the object `Store` is the same as the provided `Store`.
    fn comes_from_same_store(&self, store: &Store) -> bool;
}
