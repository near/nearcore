use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::alloc::{CHUNK_SIZE, allocation_class, allocation_size};
use super::single_thread::STArena;
use super::{Arena, ArenaMemory, ArenaMemoryMut, ArenaMut, ArenaPos, ArenaSliceMut};

/// Arena that can be allocated on from multiple threads, but still allowing conversion to a
/// single-threaded `STArena` afterwards.
///
/// The `ConcurrentArena` cannot be directly used; rather, for each thread wishing to use it,
/// `for_thread` must be called to get a `ConcurrentArenaForThread`, which then acts like a
/// normal arena, except that deallocation is not supported.
///
/// The only synchronization they need is the chunk counter. The purpose is so that after
/// multiple threads allocate on their own arenas, the resulting memory can still be combined
/// into a single arena while allowing the pointers (ArenaPos) to still be valid. This is what
/// allows a memtrie to be loaded in parallel.
pub struct ConcurrentArena {
    /// Chunks allocated by each `ConcurrentArenaForThread` share the same "logical" memory
    /// space. This counter is used to ensure that each `ConcurrentArenaForThread` gets unique
    /// chunk positions, so that the allocations made by different threads do not conflict in
    /// their positions.
    ///
    /// The goal here is so that allocations coming from multiple threads can be merged into a
    /// single arena at the end, without having to alter any arena pointers (ArenaPos).
    next_chunk_pos: Arc<AtomicUsize>,
}

impl ConcurrentArena {
    pub fn new() -> Self {
        Self { next_chunk_pos: Arc::new(AtomicUsize::new(0)) }
    }

    /// Returns an arena that can be used for one thread.
    pub fn for_thread(&self) -> ConcurrentArenaForThread {
        ConcurrentArenaForThread::new(self.next_chunk_pos.clone())
    }

    /// Converts the arena to a single-threaded arena. All returned values of `for_thread` must be
    /// passed in.
    ///
    /// There is a caveat that may be fixed in the future if desired: the returned arena will have
    /// some memory wasted. This is because the last chunk of each thread may not be full, and
    /// the single-threaded arena is unable to make use of multiple partially filled chunks. The
    /// maximum memory wasted is 4MB * number of threads; the average is 2MB * number of threads.
    /// The wasted memory will be reclaimed when the memtrie shard is unloaded.
    pub fn to_single_threaded(
        self,
        name: String,
        threads: Vec<ConcurrentArenaForThread>,
    ) -> STArena {
        let mut chunks = vec![Vec::new(); self.next_chunk_pos.load(Ordering::Relaxed)];
        let mut active_allocs_bytes = 0;
        let mut active_allocs_count = 0;
        for thread in threads {
            let memory = thread.memory;
            for (pos, chunk) in memory.chunks {
                assert!(
                    chunks[pos].is_empty(),
                    "Arena threads from the same ConcurrentArena passed in"
                );
                chunks[pos] = chunk;
            }
            active_allocs_bytes += thread.allocator.active_allocs_bytes;
            active_allocs_count += thread.allocator.active_allocs_count;
        }
        for chunk in &chunks {
            assert!(!chunks.is_empty(), "Not all arena threads are passed in");
            assert_eq!(chunk.len(), CHUNK_SIZE); // may as well check this
        }
        STArena::new_from_existing_chunks(name, chunks, active_allocs_bytes, active_allocs_count)
    }
}

/// Arena to be used for a single thread.
pub struct ConcurrentArenaForThread {
    memory: ConcurrentArenaMemory,
    allocator: ConcurrentArenaAllocator,
}

pub struct ConcurrentArenaMemory {
    /// Chunks of memory allocated for this thread. The usize is the global chunk position.
    chunks: Vec<(usize, Vec<u8>)>,
    /// Index is global chunk position, value is local chunk position.
    /// For a chunk position that does not belong to the thread, the value is `usize::MAX`.
    /// This vector is as large as needed to contain the largest global chunk position used
    /// by this thread, but might not be as large as the total number of chunks allocated
    /// globally.
    chunk_pos_global_to_local: Vec<usize>,
}

impl ConcurrentArenaMemory {
    pub fn new() -> Self {
        Self { chunks: Vec::new(), chunk_pos_global_to_local: Vec::new() }
    }

    pub fn add_chunk(&mut self, pos: usize) {
        while self.chunk_pos_global_to_local.len() <= pos {
            self.chunk_pos_global_to_local.push(usize::MAX);
        }
        self.chunk_pos_global_to_local[pos] = self.chunks.len();
        self.chunks.push((pos, vec![0; CHUNK_SIZE]));
    }

    pub fn chunk(&self, pos: usize) -> &[u8] {
        let index = self.chunk_pos_global_to_local[pos];
        &self.chunks[index].1
    }

    pub fn chunk_mut(&mut self, pos: usize) -> &mut [u8] {
        let index = self.chunk_pos_global_to_local[pos];
        &mut self.chunks[index].1
    }
}

impl ArenaMemory for ConcurrentArenaMemory {
    fn raw_slice(&self, pos: ArenaPos, len: usize) -> &[u8] {
        &self.chunk(pos.chunk())[pos.pos()..pos.pos() + len]
    }
}

impl ArenaMemoryMut for ConcurrentArenaMemory {
    fn is_mutable(&self, _pos: ArenaPos) -> bool {
        true
    }

    fn raw_slice_mut(&mut self, pos: ArenaPos, len: usize) -> &mut [u8] {
        &mut self.chunk_mut(pos.chunk())[pos.pos()..pos.pos() + len]
    }
}

/// Allocator for a single thread. Unlike the allocator for `STArena`, this one only supports
/// allocation and not deallocation, so it is substantially simpler.
pub struct ConcurrentArenaAllocator {
    next_chunk_pos: Arc<AtomicUsize>,
    next_pos: ArenaPos,

    // Stats that will be transferred to the single-threaded arena.
    active_allocs_bytes: usize,
    active_allocs_count: usize,
}

impl ConcurrentArenaAllocator {
    fn new(next_chunk_pos: Arc<AtomicUsize>) -> Self {
        Self {
            next_chunk_pos,
            next_pos: ArenaPos::invalid(),
            active_allocs_bytes: 0,
            active_allocs_count: 0,
        }
    }

    pub fn allocate<'a>(
        &mut self,
        arena: &'a mut ConcurrentArenaMemory,
        size: usize,
    ) -> ArenaSliceMut<'a, ConcurrentArenaMemory> {
        // We must allocate in the same kind of sizes as the single-threaded arena,
        // so that after converting to `STArena`, these allocations can be properly
        // reused.
        let size_class = allocation_class(size);
        let allocation_size = allocation_size(size_class);
        if self.next_pos.is_invalid() || self.next_pos.pos() + allocation_size > CHUNK_SIZE {
            let next_chunk_pos = self.next_chunk_pos.fetch_add(1, Ordering::Relaxed);
            self.next_pos = ArenaPos { chunk: next_chunk_pos as u32, pos: 0 };
            arena.add_chunk(next_chunk_pos);
        }
        let pos = self.next_pos;
        self.next_pos = pos.offset_by(allocation_size);
        self.active_allocs_bytes += allocation_size;
        self.active_allocs_count += 1;
        ArenaSliceMut::new(arena, pos, size)
    }
}

impl ConcurrentArenaForThread {
    fn new(next_chunk_pos: Arc<AtomicUsize>) -> Self {
        Self {
            memory: ConcurrentArenaMemory::new(),
            allocator: ConcurrentArenaAllocator::new(next_chunk_pos),
        }
    }
}

impl Arena for ConcurrentArenaForThread {
    type Memory = ConcurrentArenaMemory;

    fn memory(&self) -> &Self::Memory {
        &self.memory
    }
}

impl ArenaMut for ConcurrentArenaForThread {
    type MemoryMut = ConcurrentArenaMemory;

    fn memory_mut(&mut self) -> &mut Self::Memory {
        &mut self.memory
    }

    fn alloc(&mut self, size: usize) -> ArenaSliceMut<Self::Memory> {
        self.allocator.allocate(&mut self.memory, size)
    }
}

#[cfg(test)]
mod tests {
    use super::ConcurrentArena;
    use crate::trie::mem::arena::alloc::CHUNK_SIZE;
    use crate::trie::mem::arena::metrics::MEMTRIE_ARENA_MEMORY_USAGE_BYTES;
    use crate::trie::mem::arena::{Arena, ArenaMemory, ArenaMut, ArenaWithDealloc};

    #[test]
    fn test_concurrent_arena() {
        // cspell:words starena
        let arena = ConcurrentArena::new();
        let mut thread1 = arena.for_thread();
        let mut thread2 = arena.for_thread();
        let mut thread3 = arena.for_thread();

        let mut alloc1 = thread1.alloc(17);
        let mut alloc2 = thread2.alloc(25);
        let mut alloc3 = thread3.alloc(40);
        alloc1.raw_slice_mut().copy_from_slice(&[1; 17]);
        alloc2.raw_slice_mut().copy_from_slice(&[2; 25]);
        alloc3.raw_slice_mut().copy_from_slice(&[3; 40]);
        let ptr1 = alloc1.raw_pos();
        let ptr2 = alloc2.raw_pos();
        let ptr3 = alloc3.raw_pos();

        let name = rand::random::<u64>().to_string();
        let mut starena = arena.to_single_threaded(name.clone(), vec![thread1, thread2, thread3]);

        assert_eq!(starena.num_active_allocs(), 3);
        assert_eq!(starena.active_allocs_bytes(), 24 + 32 + 40);
        assert_eq!(
            MEMTRIE_ARENA_MEMORY_USAGE_BYTES.get_metric_with_label_values(&[&name]).unwrap().get(),
            3 * CHUNK_SIZE as i64
        );

        let mut alloc4 = starena.alloc(17);
        alloc4.raw_slice_mut().copy_from_slice(&[4; 17]);
        let ptr4 = alloc4.raw_pos();

        assert_eq!(starena.memory().raw_slice(ptr1, 17), &[1; 17]);
        assert_eq!(starena.memory().raw_slice(ptr2, 25), &[2; 25]);
        assert_eq!(starena.memory().raw_slice(ptr3, 40), &[3; 40]);
        assert_eq!(starena.memory().raw_slice(ptr4, 17), &[4; 17]);

        // Allocations from the concurrent arena can be deallocated and reused in the converted STArena.
        // Allocations of the same size class are reusable.
        starena.dealloc(ptr1, 17);
        let mut alloc5 = starena.alloc(23);
        assert_eq!(alloc5.raw_pos(), ptr1);
        alloc5.raw_slice_mut().copy_from_slice(&[5; 23]);
        starena.dealloc(ptr2, 25);
        let mut alloc6 = starena.alloc(32);
        assert_eq!(alloc6.raw_pos(), ptr2);
        alloc6.raw_slice_mut().copy_from_slice(&[6; 32]);
        starena.dealloc(ptr3, 40);
        let mut alloc7 = starena.alloc(37);
        assert_eq!(alloc7.raw_pos(), ptr3);
        alloc7.raw_slice_mut().copy_from_slice(&[7; 37]);
        starena.dealloc(ptr4, 17);
        let mut alloc8 = starena.alloc(24);
        assert_eq!(alloc8.raw_pos(), ptr4);
        alloc8.raw_slice_mut().copy_from_slice(&[8; 24]);

        assert_eq!(starena.memory().raw_slice(ptr1, 23), &[5; 23]);
        assert_eq!(starena.memory().raw_slice(ptr2, 32), &[6; 32]);
        assert_eq!(starena.memory().raw_slice(ptr3, 37), &[7; 37]);
        assert_eq!(starena.memory().raw_slice(ptr4, 24), &[8; 24]);
    }
}
