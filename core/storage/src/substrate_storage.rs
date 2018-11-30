use hash256_std_hasher::Hash256StdHasher;
use hash_db::Hasher;
use kvdb::DBValue;
use primitives::hash::CryptoHash;
use std::sync::Arc;
pub use substrate_state_machine::{
    Backend, Externalities, OverlayedChanges, Storage,
};
use substrate_state_machine::InMemoryChangesTrieStorage;

#[derive(Debug)]
pub struct CryptoHasher;

impl Hasher for CryptoHasher {
    type Out = CryptoHash;
    type StdHasher = Hash256StdHasher;
    const LENGTH: usize = 32;
    fn hash(x: &[u8]) -> Self::Out {
        primitives::hash::hash(x)
    }
}

pub type MemoryDB = memory_db::MemoryDB<CryptoHasher, DBValue>;
pub type TrieBackend =
    substrate_state_machine::TrieBackend<Arc<Storage<CryptoHasher>>, CryptoHasher>;
pub type TrieBackendTransaction = MemoryDB;
pub type ChangesTrieStorage = InMemoryChangesTrieStorage<CryptoHasher>;
pub type StateExt<'a> =
    substrate_state_machine::Ext<'a, CryptoHasher, TrieBackend, ChangesTrieStorage>;

#[cfg(test)]
mod tests {
    use hash_db::HashDB;
    use std::ops::Deref;
    use super::*;
    use super::super::{COL_STATE, StateDb};

    fn state_transition(
        backend: &TrieBackend,
        overlay: &mut OverlayedChanges,
    ) -> (TrieBackendTransaction, CryptoHash) {
        let root = *backend.root();
        println!("root before changes is {:?}", root);

        let mut ext = StateExt::new(overlay, &backend, None);
        assert_eq!(root, ext.storage_root());

        // all changes are applied to the overlay
        ext.place_storage(b"dog".to_vec(), Some(b"puppy".to_vec()));
        ext.place_storage(b"dog2".to_vec(), Some(b"puppy2".to_vec()));
        ext.place_storage(b"dog3".to_vec(), Some(b"puppy3".to_vec()));

        // storage_root() returns trie root after all changes
        let root_after = ext.storage_root();

        println!("root after changes is {:?}", root_after);

        // consume Ext and return a transaction with changes
        let (storage_transaction, _changes_trie_transaction) = ext.transaction();
        println!("{:?}", storage_transaction.keys().len());
        (storage_transaction, root_after)
    }

    #[test]
    fn externalities() {
        let kvdb = kvdb_memorydb::create(1 /*numColumns*/);
        let root = CryptoHash::default();
        let storage_db = Arc::new(StateDb::new(Arc::new(kvdb)));
        let storage_db_copy = Arc::clone(&storage_db);

        let backend: TrieBackend = TrieBackend::new(storage_db_copy, root);
        let mut overlay = Default::default();

        assert!(backend.storage(&b"dog".to_vec()).unwrap().is_none());
        // transaction to apply to db with changes for the block
        let (mut storage_transaction, root_after) = state_transition(&backend, &mut overlay);
        overlay.commit_prospective();

        // TODO: test rollback:
        // overlay.discard_prospective() and don't apply transaction
        assert!(backend.storage(&b"dog".to_vec()).unwrap().is_none());

        // Apply changes to the storage

        let mdb = storage_db.deref().storage.deref();

        let mut transaction = mdb.transaction();

        for x in mdb.iter(COL_STATE) {
            println!("Entry in db before: {:?}", x);
        }

        for (k, (v, rc)) in storage_transaction.drain() {
            if rc > 0 {
                transaction.put(COL_STATE, k.as_ref(), &v.to_vec());
            } else if rc < 0 {
                transaction.delete(COL_STATE, k.as_ref());
            }
        }

        for (k, v) in overlay.into_committed() {
            println!("overlay entry: {:?} {:?}", k, v);
        }

        mdb.write(transaction).expect("write to db");
        for x in mdb.iter(COL_STATE) {
            println!("Entry in db after: {:?}", x);
        }

        let storage_db_copy = Arc::clone(&storage_db);
        let backend: TrieBackend = TrieBackend::new(storage_db_copy, root_after);

        let puppy =
            backend.storage(&b"dog3".to_vec()).unwrap().expect("the key should be in the backend");
        assert_eq!(b"puppy3".to_vec(), puppy);
    }
}
