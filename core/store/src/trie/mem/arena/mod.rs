mod alloc;
use self::alloc::Allocator;
use borsh::{BorshDeserialize, BorshSerialize};
use mmap_rs::{MmapFlags, MmapMut, MmapOptions};
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
pub struct ArenaMemory {
    mmap: MmapMut,
}

impl ArenaMemory {
    fn new(max_size_in_bytes: usize) -> Self {
        let mmap = MmapOptions::new(max_size_in_bytes)
            .unwrap()
            // We need NO_RESERVE so that the memory region doesn't actually
            // take up space before the memory is used.
            .with_flags(MmapFlags::NO_RESERVE)
            .map_mut()
            .expect("mmap failed");
        Self { mmap }
    }

    fn raw_slice<'a>(&'a self, pos: usize, len: usize) -> &'a [u8] {
        &self.mmap.as_slice()[pos..pos + len]
    }

    fn raw_slice_mut<'a>(&'a mut self, pos: usize, len: usize) -> &'a mut [u8] {
        &mut self.mmap.as_mut_slice()[pos..pos + len]
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
    pub fn new(max_size_in_bytes: usize) -> Self {
        Self { memory: ArenaMemory::new(max_size_in_bytes), allocator: Allocator::new() }
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
        usize::try_from_slice(&self.arena.raw_slice(self.pos, 8)).unwrap()
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
        assert!(pos + 8 <= self.len);
        ptr.serialize(&mut &mut self.arena.raw_slice_mut(self.pos + pos, 8)).unwrap();
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
