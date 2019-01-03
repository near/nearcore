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
    use kvdb::KeyValueDB;
    use kvdb::DBTransaction;

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


    // Bug#1: trie node codec produces equivalent output if two nodes have equivalent subtrees.
    // build_db_transaction should treat the node storage as a multiset of key/value pairs.
    // The bug is hard to hit if trie keys are hashes.
    #[test]
    #[should_panic]
    fn substrate_bug_kvdb_multiset() {
        let kvdb = kvdb_memorydb::create(1 /*numColumns*/);
        let mut root = CryptoHash::default();
        let storage_db = Arc::new(StateDb::new(Arc::new(kvdb)));

        let mut prefix1 = String::from("dog");
        let mut prefix2 = String::from("cat");
        let value = String::from("puppypuppypuppypuppypuppypuppypuppypuppypuppypuppypuppypuppypuppy");

        for _block in 0..4 {
            let storage_db_copy = Arc::clone(&storage_db);
            let backend: TrieBackend = TrieBackend::new(storage_db_copy, root);
            let mut overlay = OverlayedChanges::default();

            {
                let mut ext = StateExt::new(&mut overlay, &backend, None);
                match _block {
                    0 => {
                        // Write dogaa and dogbb
                        //
                        ext.place_storage((prefix1.clone() + "aa").into_bytes(), Some(value.clone().into_bytes()));
                        ext.place_storage((prefix1.clone() + "bb").into_bytes(), Some(value.clone().into_bytes()));
                    }
                    1 => {
                        // Write cataa and catbb
                        //
                        ext.place_storage((prefix2.clone() + "aa").into_bytes(), Some(value.clone().into_bytes()));
                        ext.place_storage((prefix2.clone() + "bb").into_bytes(), Some(value.clone().into_bytes()));
                    }
                    2 => {
                        // Write dogac which deletes the old branching node.
                        // The same node had another identical copy which also gets deleted (bug).
                        //
                        ext.place_storage((prefix1.clone() + "ac").into_bytes(), Some(value.clone().into_bytes()));
                        ext.place_storage((prefix1.clone() + "bc").into_bytes(), Some(value.clone().into_bytes()));
                    }
                    3 => {
                        // Crash trying to read the incorrectly deleted node.
                        //
                        ext.storage(&(prefix2.clone() + "aa").into_bytes()[..]);
                    }
                    _ => {}
                }
            }
            overlay.commit_prospective();


            let mdb = storage_db.deref().storage.deref();

            let (root_after, transaction) = {
                let mut ext = StateExt::new(&mut overlay, &backend, None);
                let root_after = ext.storage_root();
                let (mut storage_transaction, _changes_trie_transaction) = ext.transaction();
                let transaction = build_db_transaction(&mut storage_transaction, mdb);
                (root_after, transaction)
            };

            mdb.write(transaction).expect("write to db");
            root = root_after;
            println!("{}: root after changes is {:?}", _block, root_after);
        }
    }

    // Bug#2: trie node codec does not work when keys can have long common prefixes (~190 bytes).
    // The bug is hard to hit if trie keys are hashes.
    #[test]
    #[should_panic]
    fn substrate_bug_long_extension() {
        let kvdb = kvdb_memorydb::create(1 /*numColumns*/);
        let mut root = CryptoHash::default();
        let storage_db = Arc::new(StateDb::new(Arc::new(kvdb)));

        let mut prefix1 = String::new();
        for _i in 0..190 {
            prefix1 += "x";
        }
        let value = String::from("puppy");

        for _block in 0..2 {
            let storage_db_copy = Arc::clone(&storage_db);
            let backend: TrieBackend = TrieBackend::new(storage_db_copy, root);
            let mut overlay = OverlayedChanges::default();

            {
                let mut ext = StateExt::new(&mut overlay, &backend, None);
                match _block {
                    0 => {
                        // Trie root is an extension node with ~380 nibbles.
                        // Substrate node codec breaks on extension nodes with more than ~(256+128).
                        //
                        ext.place_storage((prefix1.clone() + "aa").into_bytes(), Some(value.clone().into_bytes()));
                        ext.place_storage((prefix1.clone() + "bb").into_bytes(), Some(value.clone().into_bytes()));
                    }
                    1 => {
                        // Crash attempting to decode the root node.
                        //
                        ext.storage(&(prefix1.clone() + "aa").into_bytes()[..]);
                    }
                    _ => {}
                }
            }
            overlay.commit_prospective();


            let mdb = storage_db.deref().storage.deref();

            let (root_after, transaction) = {
                let mut ext = StateExt::new(&mut overlay, &backend, None);
                let root_after = ext.storage_root();
                let (mut storage_transaction, _changes_trie_transaction) = ext.transaction();
                let transaction = build_db_transaction(&mut storage_transaction, mdb);
                (root_after, transaction)
            };

            mdb.write(transaction).expect("write to db");
            root = root_after;
            println!("{}: root after changes is {:?}", _block, root_after);
        }
    }

    fn build_db_transaction(storage_transaction: &mut MemoryDB, mdb: &KeyValueDB) -> DBTransaction {
        let mut transaction = mdb.transaction();
        for (k, (v, rc)) in storage_transaction.drain() {
            if rc > 0 {
                transaction.put(COL_STATE, k.as_ref(), &v.to_vec());
            } else if rc < 0 {
                transaction.delete(COL_STATE, k.as_ref());
            }
        }
        transaction
    }
}
