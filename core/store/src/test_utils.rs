use std::sync::Arc;

use crate::{Store, NUM_COLS};

pub fn create_test_store() -> Arc<Store> {
    let db = Arc::new(kvdb_memorydb::create(NUM_COLS));
    Arc::new(Store::new(db))
}
