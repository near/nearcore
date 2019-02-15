use std::sync::Arc;

use kvdb_memorydb::InMemory;

use crate::TOTAL_COLUMNS;
use crate::Trie;

pub type MemoryStorage = InMemory;

pub fn create_memory_db() -> MemoryStorage {
    kvdb_memorydb::create(TOTAL_COLUMNS.unwrap())
}

pub fn create_trie() -> Trie {
    let storage = Arc::new(create_memory_db());
    Trie::new(storage)
}
