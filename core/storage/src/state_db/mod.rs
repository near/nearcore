use crate::state_db::trie::DBChanges;
use crate::COL_STATE;
use kvdb::KeyValueDB;
use std::sync::Arc;

pub mod trie;
pub mod update;

pub struct StateDb {
    trie: trie::Trie,
    storage: Arc<KeyValueDB>,
}

impl StateDb {
    pub fn new(storage: Arc<KeyValueDB>) -> Self {
        StateDb { trie: trie::Trie::new(storage.clone(), COL_STATE), storage }
    }

    pub fn commit(&self, transaction: DBChanges) -> std::io::Result<()> {
        trie::apply_changes(&self.storage, COL_STATE, transaction)
    }
}
