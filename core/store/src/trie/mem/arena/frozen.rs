use std::sync::Arc;

use super::single_thread::STArenaMemory;
use super::{Arena, ArenaMemory, ArenaPos};

/// FrozenArenaMemory holds a cloneable read-only shared memory instance.
/// This can later be converted to HybridArenaMemory.
#[derive(Clone)]
pub struct FrozenArenaMemory {
    pub(super) shared_memory: Arc<STArenaMemory>,
}

/// We only implement the ArenaMemory interface for FrozenArena, as it is read-only.
/// ArenaMemoryMut is not implemented.
impl ArenaMemory for FrozenArenaMemory {
    fn raw_slice(&self, pos: ArenaPos, len: usize) -> &[u8] {
        self.shared_memory.raw_slice(pos, len)
    }
}

/// FrozenArena is a read-only arena that is cloneable and can be shared between threads.
#[derive(Clone)]
pub struct FrozenArena {
    /// The memory of the arena.
    pub(super) memory: FrozenArenaMemory,

    /// active_allocs_bytes and active_allocs_count are used while initializing
    /// allocator for HybridArena.
    pub(super) active_allocs_bytes: usize,
    pub(super) active_allocs_count: usize,
}

/// We only implement the Arena interface for FrozenArena, as it is read-only.
/// ArenaMut and ArenaWithDealloc are not implemented.
impl Arena for FrozenArena {
    type Memory = FrozenArenaMemory;

    fn memory(&self) -> &FrozenArenaMemory {
        &self.memory
    }
}
