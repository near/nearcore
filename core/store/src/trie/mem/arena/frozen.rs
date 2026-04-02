use super::single_thread::STArenaMemory;
use super::{Arena, ArenaMemory, ArenaPos};
use std::sync::Arc;

/// FrozenArenaMemory holds cloneable read-only shared memory as one or more
/// layers. Each layer is an `Arc<STArenaMemory>` that can be shared across
/// multiple arenas (e.g. parent and child shards after resharding).
///
/// Chunks are addressed contiguously across layers: layer 0 owns chunks
/// `0..N0`, layer 1 owns `N0..N0+N1`, etc. The total number of chunks is
/// precomputed in `total_chunk_count` for faster offset calculations.
#[derive(Clone)]
pub struct FrozenArenaMemory {
    pub(super) shared_layers: Vec<Arc<STArenaMemory>>,
    pub(super) total_chunk_count: u32,
}

impl FrozenArenaMemory {
    /// Creates an empty `FrozenArenaMemory` with no shared layers.
    pub(super) fn empty() -> Self {
        Self { shared_layers: Vec::new(), total_chunk_count: 0 }
    }

    /// Total number of chunks across all shared layers.
    #[inline]
    pub(super) fn total_chunk_count(&self) -> u32 {
        self.total_chunk_count
    }

    /// Appends a new layer on top of the existing shared layers.
    pub(super) fn push_layer(&mut self, layer: Arc<STArenaMemory>) {
        self.total_chunk_count += layer.chunks.len() as u32;
        self.shared_layers.push(layer);
    }
}

/// We only implement the ArenaMemory interface for FrozenArena, as it is read-only.
/// ArenaMemoryMut is not implemented.
impl ArenaMemory for FrozenArenaMemory {
    fn raw_slice(&self, pos: ArenaPos, len: usize) -> &[u8] {
        let mut remaining_chunk = pos.chunk;
        for layer in &self.shared_layers {
            let layer_chunks = layer.chunks.len() as u32;
            if remaining_chunk < layer_chunks {
                let adjusted = ArenaPos { chunk: remaining_chunk, pos: pos.pos };
                return layer.raw_slice(adjusted, len);
            }
            remaining_chunk -= layer_chunks;
        }
        panic!(
            "FrozenArenaMemory::raw_slice: chunk {} out of bounds (total {})",
            pos.chunk, self.total_chunk_count
        );
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
