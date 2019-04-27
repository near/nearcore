use std::sync::Arc;

use crate::{Store, NUM_COLS};

pub fn create_test_store() -> Store {
    let db = Arc::new(kvdb_memorydb::create(NUM_COLS));
    Store::new(db)
}
