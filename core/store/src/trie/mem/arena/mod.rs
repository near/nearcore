mod alloc;
mod metrics;
use self::alloc::Allocator;
use borsh::{BorshDeserialize, BorshSerialize};
use memmap2::{MmapMut, MmapOptions};
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::mem::size_of;

/// Arena to store in-memory trie nodes.
/// Includes a bump allocator that can free nodes.
///
/// To allocate, deallocate, or mutate any allocated memory, a mutable
/// reference to the `Arena` is needed.
pub struct Arena {
    memory: ArenaMemory,
    allocator: Allocator,
}

/// Mmap-ed memory to host the in-memory trie nodes.
/// A mutable reference to `ArenaMemory` can be used to mutate allocated
/// memory, but not to allocate or deallocate memory.
///
/// From an `ArenaMemory` one can obtain an `ArenaPtr` (single location)
/// or `ArenaSlice` (range of bytes) to read the actual memory, and the
/// mutable versions `ArenaPtrMut` and `ArenaSliceMut` to write memory.
pub struct ArenaMemory {
    mmap: MmapMut,
}

impl ArenaMemory {
    fn new(max_size_in_bytes: usize) -> Self {
        let mmap = MmapOptions::new().len(max_size_in_bytes).map_anon().expect("mmap failed");
        Self { mmap }
    }

    fn raw_slice(&self, pos: usize, len: usize) -> &[u8] {
        &self.mmap[pos..pos + len]
    }

    fn raw_slice_mut(&mut self, pos: usize, len: usize) -> &mut [u8] {
        &mut self.mmap[pos..pos + len]
    }

    /// Provides read access to a region of memory in the arena.
    pub fn slice<'a>(&'a self, pos: usize, len: usize) -> ArenaSlice<'a> {
        ArenaSlice { arena: self, pos, len }
    }

    /// Provides write access to a region of memory in the arena.
    pub fn slice_mut<'a>(&'a mut self, pos: usize, len: usize) -> ArenaSliceMut<'a> {
        ArenaSliceMut { arena: self, pos, len }
    }

    /// Represents some position in the arena but without a known length.
    pub fn ptr<'a>(self: &'a Self, pos: usize) -> ArenaPtr<'a> {
        ArenaPtr { arena: self, pos }
    }

    /// Like `ptr` but with write access.
    pub fn ptr_mut<'a>(self: &'a mut Self, pos: usize) -> ArenaPtrMut<'a> {
        ArenaPtrMut { arena: self, pos }
    }
}

impl Arena {
    /// Creates a new memory region of the given size to store trie nodes.
    /// The `max_size_in_bytes` can be conservatively large as long as it
    /// can fit into virtual memory (which there are terabytes of). The actual
    /// memory usage will only be as much as is needed.
    pub fn new(max_size_in_bytes: usize, name: String) -> Self {
        Self { memory: ArenaMemory::new(max_size_in_bytes), allocator: Allocator::new(name) }
    }

    /// Allocates a slice of the given size in the arena.
    pub fn alloc<'a>(&'a mut self, size: usize) -> ArenaSliceMut<'a> {
        self.allocator.allocate(&mut self.memory, size)
    }

    /// Deallocates the given slice from the arena; the slice's `pos` and `len`
    /// must be the same as an allocation that was returned earlier.
    pub fn dealloc(&mut self, pos: usize, len: usize) {
        self.allocator.deallocate(&mut self.memory, pos, len);
    }

    /// Number of active allocations (alloc calls minus dealloc calls).
    #[cfg(test)]
    pub fn num_active_allocs(&self) -> usize {
        self.allocator.num_active_allocs()
    }

    pub fn memory(&self) -> &ArenaMemory {
        &self.memory
    }

    pub fn memory_mut(&mut self) -> &mut ArenaMemory {
        &mut self.memory
    }
}

/// Represents some position in the arena but without a known length.
#[derive(Clone, Copy)]
pub struct ArenaPtr<'a> {
    arena: &'a ArenaMemory,
    pos: usize,
}

impl<'a> Debug for ArenaPtr<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "arena[{:x}]", self.pos)
    }
}

impl<'a> Hash for ArenaPtr<'a> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.pos.hash(state);
    }
}

impl<'a> PartialEq for ArenaPtr<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.arena as *const ArenaMemory == other.arena as *const ArenaMemory
            && self.pos == other.pos
    }
}

impl<'a> Eq for ArenaPtr<'a> {}

impl<'a> ArenaPtr<'a> {
    /// Returns a slice relative to this pointer.
    pub fn slice(&self, offset: usize, len: usize) -> ArenaSlice<'a> {
        ArenaSlice { arena: self.arena, pos: self.pos + offset, len }
    }

    /// Returns the offset to the beginning of the arena.
    pub fn raw_offset(&self) -> usize {
        self.pos
    }

    pub fn arena(&self) -> &'a ArenaMemory {
        self.arena
    }

    /// Reads a usize at the memory pointed to by this pointer.
    pub fn read_usize(&self) -> usize {
        usize::try_from_slice(&self.arena.raw_slice(self.pos, size_of::<usize>())).unwrap()
    }
}

/// Represents a slice of memory in the arena.
#[derive(Clone)]
pub struct ArenaSlice<'a> {
    arena: &'a ArenaMemory,
    pos: usize,
    len: usize,
}

impl<'a> Debug for ArenaSlice<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "arena[{:x}..{:x}]", self.pos, self.pos + self.len)
    }
}

impl<'a> ArenaSlice<'a> {
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns the pointer pointing to the beginning of this slice.
    pub fn ptr(&self) -> ArenaPtr<'a> {
        ArenaPtr { arena: self.arena, pos: self.pos }
    }

    /// Provides direct access to the memory.
    pub fn raw_slice(&self) -> &'a [u8] {
        &self.arena.raw_slice(self.pos, self.len)
    }

    /// Reads a usize at the given offset in the slice (bounds checked),
    /// and returns it as a pointer.
    pub fn read_ptr_at(&self, pos: usize) -> ArenaPtr<'a> {
        let pos = usize::try_from_slice(&self.raw_slice()[pos..][..size_of::<usize>()]).unwrap();
        ArenaPtr { arena: self.arena, pos }
    }

    /// Returns a slice within this slice. This checks bounds.
    pub fn subslice(&self, start: usize, len: usize) -> ArenaSlice<'a> {
        assert!(start + len <= self.len);
        ArenaSlice { arena: self.arena, pos: self.pos + start, len }
    }
}

/// Like `ArenaPtr` but allows writing to the memory.
pub struct ArenaPtrMut<'a> {
    arena: &'a mut ArenaMemory,
    pos: usize,
}

impl<'a> ArenaPtrMut<'a> {
    /// Makes a const copy of the pointer, which can only be used while also
    /// holding a reference to the original pointer.
    pub fn ptr<'b>(&'b self) -> ArenaPtr<'b> {
        ArenaPtr { arena: self.arena, pos: self.pos }
    }

    /// Makes a mutable copy of the pointer, which can only be used while also
    /// holding a mutable reference to the original pointer.
    pub fn ptr_mut<'b>(&'b mut self) -> ArenaPtrMut<'b> {
        ArenaPtrMut { arena: self.arena, pos: self.pos }
    }

    /// Makes a slice relative to the pointer, which can only be used while
    /// also holding a mutable reference to the original pointer.
    pub fn slice<'b>(&'b self, offset: usize, len: usize) -> ArenaSlice<'b> {
        ArenaSlice { arena: self.arena, pos: self.pos + offset, len }
    }

    /// Like `slice` but makes a mutable slice.
    pub fn slice_mut<'b>(&'b mut self, offset: usize, len: usize) -> ArenaSliceMut<'b> {
        ArenaSliceMut { arena: self.arena, pos: self.pos + offset, len }
    }

    /// Provides mutable access to the whole memory, while holding a mutable
    /// reference to the pointer.
    pub fn arena_mut<'b>(&'b mut self) -> &'b mut ArenaMemory {
        &mut self.arena
    }
}

/// Represents a mutable slice of memory in the arena.
pub struct ArenaSliceMut<'a> {
    arena: &'a mut ArenaMemory,
    pos: usize,
    len: usize,
}

impl<'a> ArenaSliceMut<'a> {
    pub fn raw_offset(&self) -> usize {
        self.pos
    }

    pub fn len(&self) -> usize {
        self.len
    }

    /// Writes a usize at the given offset in the slice (bounds checked).
    pub fn write_usize_at(&mut self, pos: usize, ptr: usize) {
        assert!(pos + size_of::<usize>() <= self.len);
        ptr.serialize(&mut &mut self.arena.raw_slice_mut(self.pos + pos, size_of::<usize>()))
            .unwrap();
    }

    /// Provides mutable raw memory access. It is only possible while holding
    /// a mutable reference to the original slice.
    pub fn raw_slice_mut<'b>(&'b mut self) -> &'b mut [u8] {
        self.arena.raw_slice_mut(self.pos, self.len)
    }

    /// Provides a subslice that is also mutable, which is only possible while
    /// holding a mutable reference to the original slice.
    pub fn subslice_mut<'b>(&'b mut self, start: usize, len: usize) -> ArenaSliceMut<'b> {
        assert!(start + len <= self.len);
        ArenaSliceMut { arena: self.arena, pos: self.pos + start, len }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_arena_mmap() {
        let mut arena1 = super::ArenaMemory::new(1);
        let mut arena2 = super::ArenaMemory::new(1000);
        let mut arena3 = super::ArenaMemory::new(10000);
        let mut arena4 = super::ArenaMemory::new(100000);
        let size_100gb = 100 * 1024 * 1024 * 1024;
        // 100GB is a lot, but it's all virtual memory so it's fine in 64-bit.
        let mut arena5 = super::ArenaMemory::new(size_100gb);
        arena1.raw_slice_mut(0, 1).fill(1);
        arena2.raw_slice_mut(0, 1000).fill(2);
        arena3.raw_slice_mut(0, 10000).fill(3);
        arena4.raw_slice_mut(0, 100000).fill(4);
        arena5.raw_slice_mut(size_100gb - 100, 100).fill(5);
        assert!(arena1.raw_slice(0, 1).iter().all(|x| *x == 1));
        assert!(arena2.raw_slice(0, 1000).iter().all(|x| *x == 2));
        assert!(arena3.raw_slice(0, 10000).iter().all(|x| *x == 3));
        assert!(arena4.raw_slice(0, 100000).iter().all(|x| *x == 4));
        assert!(arena5.raw_slice(size_100gb - 100, 100).iter().all(|x| *x == 5));
    }

    #[test]
    fn test_arena_ptr_and_slice() {
        let mut arena = super::ArenaMemory::new(10 * 4096);

        arena.ptr_mut(8).slice_mut(4, 16).write_usize_at(6, 123456);
        assert_eq!(arena.ptr(8).slice(4, 16).read_ptr_at(6).raw_offset(), 123456);
        assert_eq!(arena.slice(18, 8).read_ptr_at(0).raw_offset(), 123456);

        arena.slice_mut(10, 20).subslice_mut(1, 8).write_usize_at(0, 234567);
        assert_eq!(arena.slice(10, 20).subslice(1, 8).read_ptr_at(0).raw_offset(), 234567);
        assert_eq!(arena.slice(11, 8).read_ptr_at(0).raw_offset(), 234567);
    }
}
