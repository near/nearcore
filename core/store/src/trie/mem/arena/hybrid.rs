use std::convert::From;
use std::sync::Arc;

use super::alloc::Allocator;
use super::frozen::{FrozenArena, FrozenArenaMemory};
use super::single_thread::{STArena, STArenaMemory};
use super::{
    Arena, ArenaMemory, ArenaMemoryMut, ArenaMut, ArenaPos, ArenaSliceMut, ArenaWithDealloc,
};

/// HybridArenaMemory represents a combination of owned and shared memory.
///
/// Access to owned_memory and shared_memory can be thought of as a layered memory model.
/// On the top layer, we have the owned_memory which is mutable and can be mutated by the owning arena.
/// On the bottom layer, we have the shared_memory which is read-only and can be shared between threads.
///
/// Memory position or ArenaPos { chunk, pos } references and positions to the shared memory remain valid.
/// All new allocations to the owned memory have a `pos` with chunk offset by shared_memory.chunks.len().
/// Since the shared memory is read-only, shared_memory.chunks.len() never changes.
///
/// For `pos` with chunk value >= shared_memory.chunks.len(), the memory is read from owned_memory.
/// For `pos` with chunk value < shared_memory.chunks.len(), the memory is read from shared_memory.
///
/// More information about HybridArena in section below.
pub struct HybridArenaMemory {
    owned_memory: STArenaMemory,
    shared_memory: Arc<STArenaMemory>,
}

/// Conversion from FrozenArenaMemory to HybridArenaMemory.
/// This creates a new instance of owned memory while sharing the shared memory.
impl From<FrozenArenaMemory> for HybridArenaMemory {
    fn from(frozen_memory: FrozenArenaMemory) -> Self {
        Self { owned_memory: Default::default(), shared_memory: frozen_memory.shared_memory }
    }
}

impl HybridArenaMemory {
    #[inline]
    fn chunks_offset(&self) -> u32 {
        self.shared_memory.chunks.len() as u32
    }
}

impl ArenaMemory for HybridArenaMemory {
    fn raw_slice(&self, mut pos: ArenaPos, len: usize) -> &[u8] {
        debug_assert!(!pos.is_invalid());
        if pos.chunk >= self.chunks_offset() {
            pos.chunk -= self.chunks_offset();
            self.owned_memory.raw_slice(pos, len)
        } else {
            self.shared_memory.raw_slice(pos, len)
        }
    }
}

impl ArenaMemoryMut for HybridArenaMemory {
    fn is_mutable(&self, pos: ArenaPos) -> bool {
        pos.chunk >= self.chunks_offset()
    }

    fn raw_slice_mut(&mut self, mut pos: ArenaPos, len: usize) -> &mut [u8] {
        debug_assert!(!pos.is_invalid());
        assert!(pos.chunk >= self.chunks_offset(), "Cannot mutate shared memory");
        pos.chunk -= self.chunks_offset();
        self.owned_memory.raw_slice_mut(pos, len)
    }
}

/// HybridArena represents Arena with a combination of owned and shared memory.
/// The shared memory is read-only and can be shared between threads and owned by multiple arenas.
/// The owned memory is mutable and can be mutated by the owning arena.
///
/// Note that while the pos for the shared memory remains valid, shared memory cannot be mutated.
/// or deallocated. All allocations and deallocations are done on the owned memory only.
///
/// It's possible to represent STArenaMemory as HybridArenaMemory by setting shared memory as empty.
/// This is useful for converting STArena to HybridArena to unify the interface.
///
/// For typical MemTries usage, most of the time shared memory will be empty. The only time we use
/// shared memory is during resharding when the child shards need access to the parent shard's memory.
pub struct HybridArena {
    memory: HybridArenaMemory,
    allocator: Allocator,
}

/// Conversion from STArena to HybridArena. We set shared memory as empty.
impl From<STArena> for HybridArena {
    fn from(arena: STArena) -> Self {
        Self {
            memory: HybridArenaMemory {
                owned_memory: arena.memory,
                shared_memory: Arc::new(Default::default()),
            },
            allocator: arena.allocator,
        }
    }
}

impl HybridArena {
    /// Function to create a new HybridArena from an existing instance of shared memory in FrozenArena.
    #[allow(dead_code)]
    pub fn from_frozen(name: String, frozen_arena: FrozenArena) -> Self {
        let allocator = Allocator::new_with_initial_stats(
            name,
            frozen_arena.active_allocs_bytes,
            frozen_arena.active_allocs_count,
        );
        Self { memory: frozen_arena.memory.into(), allocator }
    }

    /// HybridArena with an empty shared memory can be frozen. Freezing HybridArena with shared memory will panic.
    /// This effectively converts the owned_memory in the Arena to shared_memory.
    ///
    /// Instances of FrozenArena are cloneable and can be used to create new instances of HybridArena with
    /// shared memory from FrozenArena.
    #[allow(dead_code)]
    pub fn freeze(self) -> FrozenArena {
        assert!(!self.has_shared_memory(), "Cannot freeze arena with shared memory");
        FrozenArena {
            memory: FrozenArenaMemory { shared_memory: Arc::new(self.memory.owned_memory) },
            active_allocs_bytes: self.allocator.active_allocs_bytes(),
            active_allocs_count: self.allocator.num_active_allocs(),
        }
    }

    #[inline]
    pub fn has_shared_memory(&self) -> bool {
        self.memory.chunks_offset() > 0
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

impl Arena for HybridArena {
    type Memory = HybridArenaMemory;

    fn memory(&self) -> &Self::Memory {
        &self.memory
    }
}

impl ArenaMut for HybridArena {
    type MemoryMut = HybridArenaMemory;

    fn memory_mut(&mut self) -> &mut Self::Memory {
        &mut self.memory
    }

    fn alloc(&mut self, size: usize) -> ArenaSliceMut<Self::Memory> {
        let ArenaSliceMut { mut pos, len, .. } =
            self.allocator.allocate(&mut self.memory.owned_memory, size);
        pos.chunk = pos.chunk + self.memory.chunks_offset();
        ArenaSliceMut::new(&mut self.memory, pos, len)
    }
}

impl ArenaWithDealloc for HybridArena {
    fn dealloc(&mut self, mut pos: ArenaPos, len: usize) {
        assert!(pos.chunk >= self.memory.chunks_offset(), "Cannot deallocate shared memory");
        pos.chunk = pos.chunk - self.memory.chunks_offset();
        self.allocator.deallocate(&mut self.memory.owned_memory, pos, len);
    }
}

#[cfg(test)]
mod tests {
    use crate::trie::mem::arena::single_thread::STArena;
    use crate::trie::mem::arena::{Arena, ArenaMemory, ArenaMemoryMut, ArenaMut, ArenaPos};

    use super::HybridArena;

    #[test]
    fn test_hybrid_arena() {
        let size = 25;
        let pos0 = ArenaPos { chunk: 1, pos: 420 };

        // Create and populate STArena with 2 chunks
        let chunks = vec![vec![0; 1000], vec![0; 1000]];
        let mut st_arena = STArena::new_from_existing_chunks("test".to_string(), chunks, 0, 0);
        let slice = st_arena.memory_mut().raw_slice_mut(pos0, size);
        for i in 0..size {
            slice[i] = i as u8;
        }

        // Create two HybridArena instances from frozen arena
        let frozen_arena = HybridArena::from(st_arena).freeze();
        let mut hybrid_arena1 = HybridArena::from_frozen("test1".to_string(), frozen_arena.clone());
        let mut hybrid_arena2 = HybridArena::from_frozen("test2".to_string(), frozen_arena.clone());

        // Populate both hybrid arena
        hybrid_arena1.alloc(50); // random allocation
        let mut slice1 = hybrid_arena1.alloc(size);
        let mut slice2 = hybrid_arena2.alloc(size);
        for i in 0..size {
            slice1.raw_slice_mut()[i] = (size + i) as u8;
            slice2.raw_slice_mut()[i] = (2 * size + i) as u8;
        }

        // Verify pos2 allocated memory has chunk >= 2
        assert_eq!(slice1.raw_pos(), ArenaPos { chunk: 2, pos: 56 });
        assert_eq!(slice2.raw_pos(), ArenaPos { chunk: 2, pos: 0 });

        // Verify written values to frozen arena
        for i in 0..size {
            let val = frozen_arena.memory.raw_slice(pos0, size)[i];
            assert_eq!(val, i as u8);
        }

        // Verify shared and owned memory written values
        let shared_slice1 = hybrid_arena1.memory().raw_slice(pos0, size);
        let shared_slice2 = hybrid_arena2.memory().raw_slice(pos0, size);
        let slice1 = hybrid_arena1.memory().raw_slice(ArenaPos { chunk: 2, pos: 56 }, size);
        let slice2 = hybrid_arena2.memory().raw_slice(ArenaPos { chunk: 2, pos: 0 }, size);
        for i in 0..size {
            assert_eq!(shared_slice1[i], i as u8);
            assert_eq!(shared_slice2[i], i as u8);
            assert_eq!(slice1[i], (size + i) as u8);
            assert_eq!(slice2[i], (2 * size + i) as u8);
        }
    }

    #[test]
    #[should_panic(expected = "Cannot mutate shared memory")]
    fn test_hybrid_arena_panic_on_mut_access_shared_memory() {
        let chunks = vec![vec![0; 1000], vec![0; 1000]];
        let st_arena = STArena::new_from_existing_chunks("test".to_string(), chunks, 0, 0);
        let frozen_arena = HybridArena::from(st_arena).freeze();
        let mut hybrid_arena = HybridArena::from_frozen("test".to_string(), frozen_arena);
        let _slice = hybrid_arena.memory_mut().raw_slice_mut(ArenaPos { chunk: 1, pos: 25 }, 50);
    }
}
