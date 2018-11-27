use std::sync::Arc;

use kvdb_memorydb::InMemory;

use {StateDb, TOTAL_COLUMNS};

pub type MemoryStorage = InMemory;

pub fn create_memory_db() -> MemoryStorage {
    kvdb_memorydb::create(TOTAL_COLUMNS.unwrap())
}

pub fn create_state_db() -> StateDb {
    let storage = Arc::new(create_memory_db());
    StateDb::new(storage)
}
