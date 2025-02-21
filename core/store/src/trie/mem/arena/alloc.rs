use super::metrics::MEMTRIE_ARENA_ACTIVE_ALLOCS_COUNT;
use super::single_thread::STArenaMemory;
use super::{ArenaMemory, ArenaPos, ArenaSliceMut};
use crate::trie::mem::arena::ArenaMemoryMut;
use crate::trie::mem::arena::metrics::{
    MEMTRIE_ARENA_ACTIVE_ALLOCS_BYTES, MEMTRIE_ARENA_MEMORY_USAGE_BYTES,
};
use crate::trie::mem::flexible_data::encoding::BorshFixedSize;
use near_o11y::metrics::IntGauge;

/// Simple bump allocator with freelists.
///
/// Allocations are rounded up to its allocation class, so that deallocated
/// memory can be reused by a similarly sized allocation. Each allocation
/// class maintains a separate freelist.
///
/// Allocations are first done by popping from a freelist, if available. If not,
/// we allocate a new region by bump `next_alloc_pos` forward. Deallocated
/// regions are added to their corresponding freelist.
///
/// As a result, the memory usage of this allocator never decreases. In
/// practice, for in-memory tries, there is very little memory usage creep even
/// when tested over weeks of uptime.
pub struct Allocator {
    /// The head of a linked list of freed allocations; one for each allocation
    /// class.
    /// The "next" pointer of a freed allocation is stored in the first
    /// 8 bytes of the allocated memory itself. An empty freelist is represented
    /// using `ArenaPos::invalid()`.
    freelists: [ArenaPos; NUM_ALLOCATION_CLASSES],
    /// The next position in the arena that a new allocation (which cannot be
    /// satisfied by popping one from a freelist) will be allocated at.
    /// This position would only ever move forward. De-allocating an allocation
    /// does not affect this position; it only adds an entry to a freelist.
    next_alloc_pos: ArenaPos,

    // Stats. Note that keep the bytes and count locally too because the
    // gauges are process-wide, so stats-keeping directly with those may not be
    // accurate in case multiple instances of the allocator share the same name.
    active_allocs_bytes: usize,
    active_allocs_count: usize,
    active_allocs_bytes_gauge: IntGauge,
    active_allocs_count_gauge: IntGauge,
    memory_usage_gauge: IntGauge,
}

const MAX_ALLOC_SIZE: usize = 16 * 1024;
const ROUND_UP_TO_8_BYTES_UNDER: usize = 256;
const ROUND_UP_TO_64_BYTES_UNDER: usize = 1024;
pub(crate) const CHUNK_SIZE: usize = 4 * 1024 * 1024;

/// Calculates the allocation class (an index from 0 to NUM_ALLOCATION_CLASSES)
/// for the given size that we wish to allocate.
pub(crate) const fn allocation_class(size: usize) -> usize {
    if size <= ROUND_UP_TO_8_BYTES_UNDER {
        (size + 7) / 8 - 1
    } else if size <= ROUND_UP_TO_64_BYTES_UNDER {
        (size - ROUND_UP_TO_8_BYTES_UNDER + 63) / 64 + allocation_class(ROUND_UP_TO_8_BYTES_UNDER)
    } else {
        ((ROUND_UP_TO_64_BYTES_UNDER - 1).leading_zeros() - (size - 1).leading_zeros()) as usize
            + allocation_class(ROUND_UP_TO_64_BYTES_UNDER)
    }
}

/// Calculates the size of the actual allocation for the given size class.
pub(crate) const fn allocation_size(size_class: usize) -> usize {
    if size_class <= allocation_class(ROUND_UP_TO_8_BYTES_UNDER) {
        (size_class + 1) * 8
    } else if size_class <= allocation_class(ROUND_UP_TO_64_BYTES_UNDER) {
        (size_class - allocation_class(ROUND_UP_TO_8_BYTES_UNDER)) * 64 + ROUND_UP_TO_8_BYTES_UNDER
    } else {
        ROUND_UP_TO_64_BYTES_UNDER << (size_class - allocation_class(ROUND_UP_TO_64_BYTES_UNDER))
    }
}

const NUM_ALLOCATION_CLASSES: usize = allocation_class(MAX_ALLOC_SIZE) + 1;

impl Allocator {
    pub fn new(name: String) -> Self {
        Self {
            freelists: [ArenaPos::invalid(); NUM_ALLOCATION_CLASSES],
            next_alloc_pos: ArenaPos::invalid(),
            active_allocs_bytes: 0,
            active_allocs_count: 0,
            active_allocs_bytes_gauge: MEMTRIE_ARENA_ACTIVE_ALLOCS_BYTES
                .with_label_values(&[&name]),
            active_allocs_count_gauge: MEMTRIE_ARENA_ACTIVE_ALLOCS_COUNT
                .with_label_values(&[&name]),
            memory_usage_gauge: MEMTRIE_ARENA_MEMORY_USAGE_BYTES.with_label_values(&[&name]),
        }
    }

    pub fn new_with_initial_stats(
        name: String,
        active_allocs_bytes: usize,
        active_allocs_count: usize,
    ) -> Self {
        let mut allocator = Self::new(name);
        allocator.active_allocs_bytes = active_allocs_bytes;
        allocator.active_allocs_count = active_allocs_count;
        allocator.active_allocs_bytes_gauge.set(active_allocs_bytes as i64);
        allocator.active_allocs_count_gauge.set(active_allocs_count as i64);
        allocator
    }

    pub fn update_memory_usage_gauge(&self, memory: &STArenaMemory) {
        self.memory_usage_gauge.set(memory.chunks.len() as i64 * CHUNK_SIZE as i64);
    }

    /// Adds a new chunk to the arena, and updates the next_alloc_pos to the beginning of
    /// the new chunk.
    fn new_chunk(&mut self, memory: &mut STArenaMemory) {
        memory.chunks.push(vec![0; CHUNK_SIZE]);
        self.next_alloc_pos =
            ArenaPos { chunk: u32::try_from(memory.chunks.len() - 1).unwrap(), pos: 0 };
        self.update_memory_usage_gauge(memory);
    }

    /// Allocates a slice of the given size in the arena.
    pub fn allocate<'a>(
        &mut self,
        memory: &'a mut STArenaMemory,
        size: usize,
    ) -> ArenaSliceMut<'a, STArenaMemory> {
        assert!(size <= MAX_ALLOC_SIZE, "Cannot allocate {} bytes", size);
        self.active_allocs_bytes += size;
        self.active_allocs_count += 1;
        self.active_allocs_bytes_gauge.set(self.active_allocs_bytes as i64);
        self.active_allocs_count_gauge.set(self.active_allocs_count as i64);
        let size_class = allocation_class(size);
        let allocation_size = allocation_size(size_class);
        if self.freelists[size_class].is_invalid() {
            if self.next_alloc_pos.is_invalid()
                || memory.chunks[self.next_alloc_pos.chunk()].len()
                    <= self.next_alloc_pos.pos() + allocation_size
            {
                self.new_chunk(memory);
            }
            let ptr = self.next_alloc_pos;
            self.next_alloc_pos = self.next_alloc_pos.offset_by(allocation_size);
            memory.slice_mut(ptr, size)
        } else {
            let pos = self.freelists[size_class];
            self.freelists[size_class] = memory.ptr(pos).read_pos();
            memory.slice_mut(pos, size)
        }
    }

    /// Deallocates the given slice from the arena; the slice's `pos` and `len`
    /// must be the same as an allocation that was returned earlier.
    pub fn deallocate(&mut self, memory: &mut STArenaMemory, pos: ArenaPos, len: usize) {
        self.active_allocs_bytes -= len;
        self.active_allocs_count -= 1;
        self.active_allocs_bytes_gauge.set(self.active_allocs_bytes as i64);
        self.active_allocs_count_gauge.set(self.active_allocs_count as i64);
        let size_class = allocation_class(len);
        memory
            .slice_mut(pos, ArenaPos::SERIALIZED_SIZE)
            .write_pos_at(0, self.freelists[size_class]);
        self.freelists[size_class] = pos;
    }

    pub(super) fn num_active_allocs(&self) -> usize {
        self.active_allocs_count
    }

    pub(super) fn active_allocs_bytes(&self) -> usize {
        self.active_allocs_bytes
    }
}

#[cfg(test)]
mod test {
    use super::MAX_ALLOC_SIZE;
    use crate::trie::mem::arena::alloc::CHUNK_SIZE;
    use crate::trie::mem::arena::hybrid::HybridArena;
    use crate::trie::mem::arena::single_thread::STArena;
    use crate::trie::mem::arena::{Arena, ArenaMut, ArenaSliceMut, ArenaWithDealloc};
    use std::mem::size_of;

    #[test]
    fn test_allocate_deallocate() {
        let mut arena = STArena::new("".to_owned());
        // Repeatedly allocate and deallocate.
        for i in 0..10 {
            let mut slices = Vec::new();
            for size in (1..=16384).step_by(3) {
                let mut alloc = arena.alloc(size);
                // Check that the allocated length is large enough.
                assert!(alloc.len >= size);
                let region = alloc.raw_slice_mut();
                // Try writing some arbitrary bytes into the allocated space.
                for j in 0..size {
                    region[j] = ((i + j) % 256) as u8;
                }
                slices.push((alloc.pos, alloc.len));
            }
            slices.sort_by_key(|(pos, _)| *pos);
            // Check that the allocated intervals don't overlap.
            for i in 1..slices.len() {
                assert!(slices[i - 1].0.offset_by(slices[i - 1].1) <= slices[i].0);
            }
            // Check that each allocated interval is valid.
            for (pos, len) in &slices {
                assert!((pos.chunk()) < arena.memory().chunks.len());
                assert!(pos.offset_by(*len).pos() <= CHUNK_SIZE);
            }
            for (pos, len) in slices {
                arena.dealloc(pos, len);
            }
        }
    }

    #[test]
    fn test_allocation_reuse() {
        let mut arena = STArena::new("".to_owned());
        // Repeatedly allocate and deallocate. New allocations should reuse
        // old deallocated memory.
        for _ in 0..10 {
            let mut slices = Vec::new();
            for _ in 0..2000 {
                let alloc = arena.alloc(8192);
                slices.push((alloc.pos, alloc.len));
            }
            for (pos, len) in slices {
                arena.dealloc(pos, len);
            }
        }
        assert_eq!(arena.num_active_allocs(), 0);
        // 8192 * 2000 <= 16MB, so we should have allocated only 4 chunks.
        assert_eq!(arena.memory().chunks.len(), 4);
    }

    #[test]
    fn test_size_classes() {
        for i in 1..=MAX_ALLOC_SIZE {
            let size_class = super::allocation_class(i);
            assert!(size_class < super::NUM_ALLOCATION_CLASSES);
            let size = super::allocation_size(size_class);
            assert!(size >= i); // alloc must be large enough
            assert!(size >= size_of::<usize>()); // needed for freelist pointers
            if size_class > 0 {
                // Allocation must be as small as possible to fit the size.
                assert!(super::allocation_size(size_class - 1) < i);
            }
        }
    }

    #[test]
    #[should_panic(expected = "Cannot deallocate shared memory")]
    fn test_hybrid_arena_panic_on_dealloc_shared_memory() {
        let mut arena = STArena::new("test_arena".to_owned());
        let ArenaSliceMut { pos, len, .. } = arena.alloc(50);
        let frozen_arena = HybridArena::from(arena).freeze();
        let mut hybrid_arena = HybridArena::from_frozen("hybrid_arena".to_string(), frozen_arena);

        // Call to deallocate should panic because the pos is from shared memory.
        hybrid_arena.dealloc(pos, len)
    }
}
