use std::sync::Arc;

use crate::db::TestDB;
use crate::trie::Trie;
use crate::Store;

/// Creates an in-memory database.
pub fn create_test_store() -> Arc<Store> {
    let db = Arc::new(TestDB::new());
    Arc::new(Store::new(db))
}

/// Creates a Trie using an in-memory database.
pub fn create_trie() -> Arc<Trie> {
    let store = create_test_store();
    Arc::new(Trie::new(store))
}
