use super::single_thread::STArenaMemory;
use super::{Arena, ArenaMemory, ArenaPos};
use smallvec::SmallVec;
use std::sync::Arc;

/// FrozenArenaMemory holds cloneable read-only shared memory as one or more
/// layers. Each layer is an `Arc<STArenaMemory>` that can be shared across
/// multiple arenas (e.g. parent and child shards after resharding).
///
/// Chunks are addressed contiguously across layers: layer 0 owns chunks
/// `0..N0`, layer 1 owns `N0..N0+N1`, etc.
///
/// A precomputed `chunk_lookup` table maps each global chunk index to its
/// owning layer and local offset, giving O(1) access in `raw_slice`.
#[derive(Clone)]
pub struct FrozenArenaMemory {
    pub(super) shared_layers: SmallVec<[Arc<STArenaMemory>; 2]>,
    /// Maps global chunk index -> (layer, offset) for O(1) lookup.
    chunk_lookup: Vec<(u32, u32)>,
}

impl FrozenArenaMemory {
    /// Creates an empty `FrozenArenaMemory` with no shared layers.
    pub(super) fn empty() -> Self {
        Self { shared_layers: SmallVec::new(), chunk_lookup: Vec::new() }
    }

    /// Total number of chunks across all shared layers.
    #[inline]
    pub(super) fn total_chunk_count(&self) -> u32 {
        self.chunk_lookup.len() as u32
    }

    /// Appends a new layer on top of the existing shared layers.
    pub(super) fn push_layer(&mut self, layer: Arc<STArenaMemory>) {
        let layer_index = self.shared_layers.len() as u32;
        let layer_chunks = layer.chunks.len() as u32;
        self.chunk_lookup.extend((0..layer_chunks).map(|offset| (layer_index, offset)));
        self.shared_layers.push(layer);
    }
}

/// We only implement the ArenaMemory interface for FrozenArena, as it is read-only.
/// ArenaMemoryMut is not implemented.
impl ArenaMemory for FrozenArenaMemory {
    fn raw_slice(&self, pos: ArenaPos, len: usize) -> &[u8] {
        let (layer_index, local_chunk) = self.chunk_lookup[pos.chunk as usize];
        let adjusted = ArenaPos { chunk: local_chunk, pos: pos.pos };
        self.shared_layers[layer_index as usize].raw_slice(adjusted, len)
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

impl FrozenArena {
    /// Number of active allocations (alloc calls minus dealloc calls).
    #[cfg(test)]
    pub fn num_active_allocs(&self) -> usize {
        self.active_allocs_count
    }

    #[cfg(test)]
    pub fn active_allocs_bytes(&self) -> usize {
        self.active_allocs_bytes
    }
}
