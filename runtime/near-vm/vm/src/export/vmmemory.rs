use crate::instance::WeakOrStrongInstanceRef;
use crate::memory::{Memory, MemoryStyle};
use near_vm_types::MemoryType;
use std::sync::Arc;

/// A memory export value.
#[derive(Debug, Clone)]
pub struct VMMemory {
    /// Pointer to the containing `Memory`.
    from: Arc<dyn Memory>,

    /// A “reference” to the instance through the
    /// `InstanceRef`. `None` if it is a host memory.
    pub instance_ref: Option<WeakOrStrongInstanceRef>,
}

/// # Safety
/// This is correct because there is no non-threadsafe logic directly in this type;
/// correct use of the raw memory from multiple threads via `definition` requires `unsafe`
/// and is the responsibility of the user of this type.
unsafe impl Send for VMMemory {}

/// # Safety
/// This is correct because the values directly in `definition` should be considered immutable
/// and the type is both `Send` and `Clone` (thus marking it `Sync` adds no new behavior, it
/// only makes this type easier to use)
unsafe impl Sync for VMMemory {}

impl VMMemory {
    /// Create a new `VMMemory`
    pub fn new(from: Arc<dyn Memory>, instance_ref: Option<WeakOrStrongInstanceRef>) -> Self {
        Self { from, instance_ref }
    }

    /// Retrieve the memory this export is having
    pub fn from(&self) -> &Arc<dyn Memory> {
        &self.from
    }

    /// Get the type for this exported memory
    pub fn ty(&self) -> MemoryType {
        self.from.ty()
    }

    /// Get the style for this exported memory
    pub fn style(&self) -> &MemoryStyle {
        self.from.style()
    }

    /// Returns whether or not the two `VMMemory`s refer to the same Memory.
    pub fn same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.from, &other.from)
    }

    /// Converts the stored instance ref into a strong `InstanceRef` if it is weak.
    /// Returns None if it cannot be upgraded.
    pub fn upgrade_instance_ref(&mut self) -> Option<()> {
        if let Some(ref mut ir) = self.instance_ref {
            *ir = ir.upgrade()?;
        }
        Some(())
    }
}
