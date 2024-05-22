mod alloc;
mod metrics;

use self::alloc::Allocator;
use super::flexible_data::encoding::BorshFixedSize;
use borsh::{BorshDeserialize, BorshSerialize};
use derive_where::derive_where;
use std::fmt::{Debug, Display, Formatter};
use std::hash::Hash;
use std::mem::size_of;

/// Single-threaded `Arena` to store in-memory trie nodes.
/// Includes a bump allocator that can free nodes.
///
/// To allocate, deallocate, or mutate any allocated memory, a mutable
/// reference to the `STArena` is needed.
pub struct STArena {
    memory: STArenaMemory,
    allocator: Allocator,
}

/// An abstraction of an arena that also allows being implemented differently,
/// specifically in the case of a multi-threaded arena where each arena instance
/// behaves like an `STArena` but they share memory chunk allocations.
pub trait Arena: Sized + 'static {
    type Memory: ArenaMemory;
    fn memory(&self) -> &Self::Memory;
    fn memory_mut(&mut self) -> &mut Self::Memory;
    /// Allocates a slice of the given size in the arena.
    fn alloc(&mut self, size: usize) -> ArenaSliceMut<Self::Memory>;
}

/// The deallocation part of the arena interface; separated from the `Arena`
/// trait because the multi-threaded implementation does not support deallocation.
pub trait ArenaWithDealloc: Arena {
    /// Deallocates the given slice from the arena; the slice's `pos` and `len`
    /// must be the same as an allocation that was returned earlier.
    fn dealloc(&mut self, pos: ArenaPos, len: usize);
}

/// Represents the entire memory used to host in-memory trie nodes.
///
/// A mutable reference to `ArenaMemory` can be used to mutate allocated
/// memory, but not to allocate or deallocate memory.
///
/// From an `ArenaMemory` one can obtain an `ArenaPtr` (single location)
/// or `ArenaSlice` (range of bytes) to read the actual memory, and the
/// mutable versions `ArenaPtrMut` and `ArenaSliceMut` to write memory.
pub trait ArenaMemory: Sized + 'static {
    fn raw_slice(&self, pos: ArenaPos, len: usize) -> &[u8];
    fn raw_slice_mut(&mut self, pos: ArenaPos, len: usize) -> &mut [u8];

    /// Provides read access to a region of memory in the arena.
    fn slice(&self, pos: ArenaPos, len: usize) -> ArenaSlice<Self> {
        ArenaSlice { arena: self, pos, len }
    }

    /// Provides write access to a region of memory in the arena.
    fn slice_mut(&mut self, pos: ArenaPos, len: usize) -> ArenaSliceMut<Self> {
        ArenaSliceMut { arena: self, pos, len }
    }

    /// Represents some position in the arena but without a known length.
    fn ptr(self: &Self, pos: ArenaPos) -> ArenaPtr<Self> {
        ArenaPtr { arena: self, pos }
    }

    /// Like `ptr` but with write access.
    fn ptr_mut(self: &mut Self, pos: ArenaPos) -> ArenaPtrMut<Self> {
        ArenaPtrMut { arena: self, pos }
    }
}

/// `ArenaMemory` implementation for `STArena` (single-threaded arena). Stores the in-memory trie
/// data as large byte arrays called "chunks".
pub struct STArenaMemory {
    chunks: Vec<Vec<u8>>,
}

/// A position of memory in an `ArenaMemory`.
#[derive(
    Copy,
    Clone,
    Debug,
    Hash,
    PartialEq,
    Eq,
    Default,
    PartialOrd,
    Ord,
    BorshSerialize,
    BorshDeserialize,
)]
pub struct ArenaPos {
    chunk: u32,
    pos: u32,
}

impl BorshFixedSize for ArenaPos {
    const SERIALIZED_SIZE: usize = 8;
}

impl Display for ArenaPos {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "c{:X}:p{:X}", self.chunk, self.pos)
    }
}

impl ArenaPos {
    pub fn pos(&self) -> usize {
        self.pos as usize
    }

    pub fn chunk(&self) -> usize {
        self.chunk as usize
    }

    pub fn offset_by(self, offset: usize) -> Self {
        Self { chunk: self.chunk, pos: self.pos + u32::try_from(offset).unwrap() }
    }

    pub(crate) const fn invalid() -> Self {
        Self { chunk: u32::MAX, pos: u32::MAX }
    }

    pub(crate) const fn is_invalid(&self) -> bool {
        self.chunk == u32::MAX && self.pos == u32::MAX
    }
}

impl STArenaMemory {
    fn new() -> Self {
        Self { chunks: Vec::new() }
    }
}

impl ArenaMemory for STArenaMemory {
    fn raw_slice(&self, pos: ArenaPos, len: usize) -> &[u8] {
        &self.chunks[pos.chunk()][pos.pos()..pos.pos() + len]
    }

    fn raw_slice_mut(&mut self, pos: ArenaPos, len: usize) -> &mut [u8] {
        &mut self.chunks[pos.chunk()][pos.pos()..pos.pos() + len]
    }
}

impl STArena {
    /// Creates a new memory region of the given size to store trie nodes.
    /// The `max_size_in_bytes` can be conservatively large as long as it
    /// can fit into virtual memory (which there are terabytes of). The actual
    /// memory usage will only be as much as is needed.
    pub fn new(name: String) -> Self {
        Self { memory: STArenaMemory::new(), allocator: Allocator::new(name) }
    }

    /// Number of active allocations (alloc calls minus dealloc calls).
    #[cfg(test)]
    pub fn num_active_allocs(&self) -> usize {
        self.allocator.num_active_allocs()
    }
}

impl Arena for STArena {
    type Memory = STArenaMemory;

    fn memory(&self) -> &STArenaMemory {
        &self.memory
    }

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

/// Represents some position in the arena but without a known length.
#[derive_where(Clone, Copy)]
pub struct ArenaPtr<'a, Memory: ArenaMemory> {
    arena: &'a Memory,
    pos: ArenaPos,
}

impl<'a, Memory: ArenaMemory> Debug for ArenaPtr<'a, Memory> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "arena[{}]", self.pos)
    }
}

impl<'a, Memory: ArenaMemory> Hash for ArenaPtr<'a, Memory> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.pos.hash(state);
    }
}

impl<'a, Memory: ArenaMemory> PartialEq for ArenaPtr<'a, Memory> {
    fn eq(&self, other: &Self) -> bool {
        self.arena as *const Memory == other.arena as *const Memory && self.pos == other.pos
    }
}

impl<'a, Memory: ArenaMemory> Eq for ArenaPtr<'a, Memory> {}

impl<'a, Memory: ArenaMemory> ArenaPtr<'a, Memory> {
    /// Returns a slice relative to this pointer.
    pub fn slice(&self, offset: usize, len: usize) -> ArenaSlice<'a, Memory> {
        ArenaSlice { arena: self.arena, pos: self.pos.offset_by(offset), len }
    }

    /// Returns the raw position in the arena.
    pub fn raw_pos(&self) -> ArenaPos {
        self.pos
    }

    pub fn arena(&self) -> &'a Memory {
        self.arena
    }

    /// Reads an ArenaPos at the memory pointed to by this pointer.
    pub fn read_pos(&self) -> ArenaPos {
        ArenaPos::try_from_slice(&self.arena.raw_slice(self.pos, size_of::<usize>())).unwrap()
    }
}

/// Represents a slice of memory in the arena.
#[derive_where(Clone)]
pub struct ArenaSlice<'a, Memory: ArenaMemory> {
    arena: &'a Memory,
    pos: ArenaPos,
    len: usize,
}

impl<'a, Memory: ArenaMemory> Debug for ArenaSlice<'a, Memory> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "arena[{}..{:x}]", self.pos, self.pos.pos() + self.len)
    }
}

impl<'a, Memory: ArenaMemory> ArenaSlice<'a, Memory> {
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns the pointer pointing to the beginning of this slice.
    pub fn ptr(&self) -> ArenaPtr<'a, Memory> {
        ArenaPtr { arena: self.arena, pos: self.pos }
    }

    /// Provides direct access to the memory.
    pub fn raw_slice(&self) -> &'a [u8] {
        &self.arena.raw_slice(self.pos, self.len)
    }

    /// Reads an usize at the given offset in the slice (bounds checked),
    /// and returns it as a pointer.
    pub fn read_ptr_at(&self, pos: usize) -> ArenaPtr<'a, Memory> {
        let pos = ArenaPos::try_from_slice(&self.raw_slice()[pos..][..size_of::<usize>()]).unwrap();
        ArenaPtr { arena: self.arena, pos }
    }

    /// Returns a slice within this slice. This checks bounds.
    pub fn subslice(&self, start: usize, len: usize) -> ArenaSlice<'a, Memory> {
        assert!(start + len <= self.len);
        ArenaSlice { arena: self.arena, pos: self.pos.offset_by(start), len }
    }
}

/// Like `ArenaPtr` but allows writing to the memory.
pub struct ArenaPtrMut<'a, Memory: ArenaMemory> {
    arena: &'a mut Memory,
    pos: ArenaPos,
}

impl<'a, Memory: ArenaMemory> ArenaPtrMut<'a, Memory> {
    /// Makes a const copy of the pointer, which can only be used while also
    /// holding a reference to the original pointer.
    pub fn ptr(&self) -> ArenaPtr<Memory> {
        ArenaPtr { arena: self.arena, pos: self.pos }
    }

    /// Makes a mutable copy of the pointer, which can only be used while also
    /// holding a mutable reference to the original pointer.
    pub fn ptr_mut(&mut self) -> ArenaPtrMut<Memory> {
        ArenaPtrMut { arena: self.arena, pos: self.pos }
    }

    /// Makes a slice relative to the pointer, which can only be used while
    /// also holding a mutable reference to the original pointer.
    pub fn slice(&self, offset: usize, len: usize) -> ArenaSlice<Memory> {
        ArenaSlice { arena: self.arena, pos: self.pos.offset_by(offset), len }
    }

    /// Like `slice` but makes a mutable slice.
    pub fn slice_mut(&mut self, offset: usize, len: usize) -> ArenaSliceMut<Memory> {
        ArenaSliceMut { arena: self.arena, pos: self.pos.offset_by(offset), len }
    }

    /// Provides mutable access to the whole memory, while holding a mutable
    /// reference to the pointer.
    pub fn arena_mut(&mut self) -> &mut Memory {
        &mut self.arena
    }
}

/// Represents a mutable slice of memory in the arena.
pub struct ArenaSliceMut<'a, Memory: ArenaMemory> {
    arena: &'a mut Memory,
    pos: ArenaPos,
    len: usize,
}

impl<'a, Memory: ArenaMemory> ArenaSliceMut<'a, Memory> {
    pub fn new(arena: &'a mut Memory, pos: ArenaPos, len: usize) -> Self {
        Self { arena, pos, len }
    }

    pub fn raw_pos(&self) -> ArenaPos {
        self.pos
    }

    pub fn len(&self) -> usize {
        self.len
    }

    /// Writes a ArenaPos at the given offset in the slice (bounds checked).
    pub fn write_pos_at(&mut self, offset: usize, to_write: ArenaPos) {
        assert!(offset + ArenaPos::SERIALIZED_SIZE <= self.len);
        to_write
            .serialize(
                &mut &mut self
                    .arena
                    .raw_slice_mut(self.pos.offset_by(offset), ArenaPos::SERIALIZED_SIZE),
            )
            .unwrap();
    }

    /// Provides mutable raw memory access.
    pub fn raw_slice_mut(&mut self) -> &mut [u8] {
        self.arena.raw_slice_mut(self.pos, self.len)
    }

    /// Provides a subslice that is also mutable, which is only possible while
    /// holding a mutable reference to the original slice.
    pub fn subslice_mut(&mut self, start: usize, len: usize) -> ArenaSliceMut<Memory> {
        assert!(start + len <= self.len);
        ArenaSliceMut { arena: self.arena, pos: self.pos.offset_by(start), len }
    }
}

#[cfg(test)]
mod tests {
    use crate::trie::mem::arena::{ArenaMemory, ArenaPos};

    #[test]
    fn test_arena_ptr_and_slice() {
        let mut arena = super::STArenaMemory::new();
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
