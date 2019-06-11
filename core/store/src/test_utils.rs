use std::sync::Arc;

use crate::trie::Trie;
use crate::{Store, NUM_COLS};

/// Creates an in-memory database.
pub fn create_test_store() -> Arc<Store> {
    let db = Arc::new(kvdb_memorydb::create(NUM_COLS));
    Arc::new(Store::new(db))
}

/// Creates a Trie using an in-memory database.
pub fn create_trie() -> Arc<Trie> {
    let store = create_test_store();
    Arc::new(Trie::new(store))
}
