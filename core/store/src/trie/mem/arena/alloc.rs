use near_o11y::metrics::IntGauge;

use super::metrics::MEM_TRIE_ARENA_ACTIVE_ALLOCS_COUNT;
use super::{ArenaMemory, ArenaSliceMut};
use crate::trie::mem::arena::metrics::{
    MEM_TRIE_ARENA_ACTIVE_ALLOCS_BYTES, MEM_TRIE_ARENA_MEMORY_USAGE_BYTES,
};

/// Simple bump allocator with freelists. Allocations are rounded up to its
/// allocation class, so that deallocated memory can be reused by a similarly
/// sized allocation.
pub struct Allocator {
    freelists: [usize; NUM_ALLOCATION_CLASSES],
    next_ptr: usize,

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

/// Calculates the allocation class (an index from 0 to NUM_ALLOCATION_CLASSES)
/// for the given size that we wish to allocate.
const fn allocation_class(size: usize) -> usize {
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
const fn allocation_size(size_class: usize) -> usize {
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
            freelists: [usize::MAX; NUM_ALLOCATION_CLASSES],
            next_ptr: 0,
            active_allocs_bytes: 0,
            active_allocs_count: 0,
            active_allocs_bytes_gauge: MEM_TRIE_ARENA_ACTIVE_ALLOCS_BYTES
                .with_label_values(&[&name]),
            active_allocs_count_gauge: MEM_TRIE_ARENA_ACTIVE_ALLOCS_COUNT
                .with_label_values(&[&name]),
            memory_usage_gauge: MEM_TRIE_ARENA_MEMORY_USAGE_BYTES.with_label_values(&[&name]),
        }
    }

    /// Allocates a slice of the given size in the arena.
    pub fn allocate<'a>(&mut self, arena: &'a mut ArenaMemory, size: usize) -> ArenaSliceMut<'a> {
        assert!(size <= MAX_ALLOC_SIZE, "Cannot allocate {} bytes", size);
        self.active_allocs_bytes += size;
        self.active_allocs_count += 1;
        self.active_allocs_bytes_gauge.set(self.active_allocs_bytes as i64);
        self.active_allocs_count_gauge.set(self.active_allocs_count as i64);
        let size_class = allocation_class(size);
        let allocation_size = allocation_size(size_class);
        if self.freelists[size_class] == usize::MAX {
            if arena.mmap.len() < self.next_ptr + allocation_size {
                panic!(
                    "In-memory trie Arena out of memory; configured as {} bytes maximum,
                    tried to allocate {} when {} bytes already used",
                    arena.mmap.len(),
                    allocation_size,
                    self.next_ptr
                );
            }
            let ptr = self.next_ptr;
            self.next_ptr += allocation_size;
            self.memory_usage_gauge.set(self.next_ptr as i64);
            arena.slice_mut(ptr, size)
        } else {
            let pos = self.freelists[size_class];
            self.freelists[size_class] = arena.ptr(pos).read_usize();
            arena.slice_mut(pos, size)
        }
    }

    /// Deallocates the given slice from the arena; the slice's `pos` and `len`
    /// must be the same as an allocation that was returned earlier.
    pub fn deallocate(&mut self, arena: &mut ArenaMemory, pos: usize, len: usize) {
        self.active_allocs_bytes -= len;
        self.active_allocs_count -= 1;
        self.active_allocs_bytes_gauge.set(self.active_allocs_bytes as i64);
        self.active_allocs_count_gauge.set(self.active_allocs_count as i64);
        let size_class = allocation_class(len);
        arena
            .slice_mut(pos, allocation_size(size_class))
            .write_usize_at(0, self.freelists[size_class]);
        self.freelists[size_class] = pos;
    }

    #[cfg(test)]
    pub fn num_active_allocs(&self) -> usize {
        self.active_allocs_count
    }
}

#[cfg(test)]
mod test {
    use super::MAX_ALLOC_SIZE;
    use crate::trie::mem::arena::Arena;
    use std::mem::size_of;

    #[test]
    fn test_allocate_deallocate() {
        let mut arena = Arena::new(10000, "".to_owned());
        // Repeatedly allocate and deallocate; we should not run out of memory.
        for i in 0..1000 {
            let mut slices = Vec::new();
            for size in 1..=100 {
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
                assert!(slices[i - 1].0 + slices[i - 1].1 <= slices[i].0);
            }
            for (pos, len) in slices {
                arena.dealloc(pos, len);
            }
        }
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
}
