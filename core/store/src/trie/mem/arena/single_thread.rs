use super::alloc::Allocator;
use super::{
    Arena, ArenaMemory, ArenaMemoryMut, ArenaMut, ArenaPos, ArenaSliceMut, ArenaWithDealloc,
};

/// `ArenaMemory` implementation for `STArena` (single-threaded arena). Stores the in-memory trie
/// data as large byte arrays called "chunks".
#[derive(Default)]
pub struct STArenaMemory {
    pub(super) chunks: Vec<Vec<u8>>,
}

impl ArenaMemory for STArenaMemory {
    fn raw_slice(&self, pos: ArenaPos, len: usize) -> &[u8] {
        &self.chunks[pos.chunk()][pos.pos()..pos.pos() + len]
    }
}

impl ArenaMemoryMut for STArenaMemory {
    fn raw_slice_mut(&mut self, pos: ArenaPos, len: usize) -> &mut [u8] {
        &mut self.chunks[pos.chunk()][pos.pos()..pos.pos() + len]
    }
}

/// Single-threaded `Arena` to store in-memory trie nodes.
/// Includes a bump allocator that can free nodes.
///
/// To allocate, deallocate, or mutate any allocated memory, a mutable
/// reference to the `STArena` is needed.
pub struct STArena {
    pub(super) memory: STArenaMemory,
    pub(super) allocator: Allocator,
}

impl STArena {
    /// Creates a new memory region of the given size to store trie nodes.
    /// The `max_size_in_bytes` can be conservatively large as long as it
    /// can fit into virtual memory (which there are terabytes of). The actual
    /// memory usage will only be as much as is needed.
    pub fn new(name: String) -> Self {
        Self { memory: Default::default(), allocator: Allocator::new(name) }
    }

    pub(crate) fn new_from_existing_chunks(
        name: String,
        chunks: Vec<Vec<u8>>,
        active_allocs_bytes: usize,
        active_allocs_count: usize,
    ) -> Self {
        let arena = Self {
            memory: STArenaMemory { chunks },
            allocator: Allocator::new_with_initial_stats(
                name,
                active_allocs_bytes,
                active_allocs_count,
            ),
        };
        arena.allocator.update_memory_usage_gauge(&arena.memory);
        arena
    }

    /// Number of active allocations (alloc calls minus dealloc calls).
    #[cfg(test)]
    pub fn num_active_allocs(&self) -> usize {
        self.allocator.num_active_allocs()
    }

    #[cfg(test)]
    pub fn active_allocs_bytes(&self) -> usize {
        self.allocator.active_allocs_bytes()
    }
}

impl Arena for STArena {
    type Memory = STArenaMemory;

    fn memory(&self) -> &STArenaMemory {
        &self.memory
    }
}

impl ArenaMut for STArena {
    type MemoryMut = STArenaMemory;

    fn memory_mut(&mut self) -> &mut STArenaMemory {
        &mut self.memory
    }

    fn alloc(&mut self, size: usize) -> ArenaSliceMut<Self::Memory> {
        self.allocator.allocate(&mut self.memory, size)
    }
}

impl ArenaWithDealloc for STArena {
    fn dealloc(&mut self, pos: ArenaPos, len: usize) {
        self.allocator.deallocate(&mut self.memory, pos, len);
    }
}

#[cfg(test)]
mod tests {
    use crate::trie::mem::arena::single_thread::STArenaMemory;
    use crate::trie::mem::arena::{ArenaMemory, ArenaMemoryMut, ArenaPos};

    #[test]
    fn test_arena_ptr_and_slice() {
        let mut arena = STArenaMemory::default();
        arena.chunks.push(vec![0; 1000]);
        arena.chunks.push(vec![0; 1000]);

        let chunk1 = ArenaPos { chunk: 1, pos: 0 };

        arena.ptr_mut(chunk1.offset_by(8)).slice_mut(4, 16).write_pos_at(6, chunk1.offset_by(123));
        assert_eq!(
            arena.ptr(chunk1.offset_by(8)).slice(4, 16).read_ptr_at(6).raw_pos(),
            chunk1.offset_by(123)
        );
        assert_eq!(
            arena.slice(chunk1.offset_by(18), 8).read_ptr_at(0).raw_pos(),
            chunk1.offset_by(123)
        );

        arena
            .slice_mut(chunk1.offset_by(10), 20)
            .subslice_mut(1, 8)
            .write_pos_at(0, chunk1.offset_by(234));
        assert_eq!(
            arena.slice(chunk1.offset_by(10), 20).subslice(1, 8).read_ptr_at(0).raw_pos(),
            chunk1.offset_by(234)
        );
        assert_eq!(
            arena.slice(chunk1.offset_by(11), 8).read_ptr_at(0).raw_pos(),
            chunk1.offset_by(234)
        );
    }
}
