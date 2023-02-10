use crate::Engine;
use enumset::EnumSet;
use wasmer_compiler::{CompileError, CpuFeature, Features};
use wasmer_types::FunctionIndex;
use wasmer_vm::Artifact;

mod private {
    pub struct Internal(pub(super) ());
}

/// A WASM module built by some [`Engine`](crate::Engine).
///
/// Types implementing this trait are ready to be saved (to e.g. disk) for later use or loaded with
/// the `Engine` to in order to produce an [`Artifact`](crate::Artifact).
pub trait Executable {
    /// Load this executable with the specified engine.
    ///
    /// TODO(0-copy): change error type here.
    fn load(
        &self,
        engine: &(dyn Engine + 'static),
    ) -> Result<std::sync::Arc<dyn Artifact>, CompileError>;

    /// The features with which this `Executable` was built.
    fn features(&self) -> Features;

    /// The CPU features this `Executable` requires.
    fn cpu_features(&self) -> EnumSet<CpuFeature>;

    /// Serializes the artifact into bytes
    fn serialize(&self) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>;

    /// Obtain a best effort description for the function at the given function index.
    ///
    /// Implementations are not required to maintain symbol names, so this may always return None.
    fn function_name(&self, index: FunctionIndex) -> Option<&str>;

    /// Internal: support for downcasting `Executable`s.
    #[doc(hidden)]
    fn type_id(&self, _: private::Internal) -> std::any::TypeId
    where
        Self: 'static,
    {
        std::any::TypeId::of::<Self>()
    }
}

impl dyn Executable {
    /// Downcast a dynamic Executable object to a concrete implementation of the trait.
    pub fn downcast_ref<T: Executable + 'static>(&self) -> Option<&T> {
        if std::any::TypeId::of::<T>() == self.type_id(private::Internal(())) {
            unsafe { Some(&*(self as *const dyn Executable as *const T)) }
        } else {
            None
        }
    }
}
