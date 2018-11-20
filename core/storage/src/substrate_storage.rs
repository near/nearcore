use substrate_primitives::{hash::H256};
use substrate_trie::NodeCodec;

use primitives::hash::CryptoHash;
use hash_db::Hasher;
use hash256_std_hasher::Hash256StdHasher;

/// Concrete implementation of Hasher using Blake2b 256-bit hashes
#[derive(Debug)]
pub struct Sha256Hasher;

impl Hasher for Sha256Hasher {
    type Out = CryptoHash;
    type StdHasher = Hash256StdHasher;
    const LENGTH: usize = 32;
    fn hash(x: &[u8]) -> Self::Out {
        primitives::hash::hash(x)
        //H256::from((primitives::hash::hash(x).0).0)
    }
}

#[cfg(test)]
use trie_db::TrieMut;

pub use substrate_state_machine::{
    Backend, Ext, Externalities, InMemoryChangesTrieStorage, OverlayedChanges,
    TrieBackend,
};

pub type MemoryDB = memory_db::MemoryDB<Sha256Hasher, trie_db::DBValue>;

// TODO: backend should not own MemoryDB
pub type TestBackend = TrieBackend<MemoryDB, Sha256Hasher>;
pub type TestBackendTransaction = MemoryDB;
pub type TestChangesTrieStorage = InMemoryChangesTrieStorage<Sha256Hasher>;
pub type TestExt<'a> = Ext<'a, Sha256Hasher, TestBackend, TestChangesTrieStorage>;

#[cfg(test)]
mod tests {
    type TrieDBMut<'a, H> = trie_db::TrieDBMut<'a, H, NodeCodec<H>>;
    use super::*;

    fn test_db() -> (MemoryDB, CryptoHash) {
        let mut root = CryptoHash::default();
        let mut mdb = MemoryDB::default(); // TODO: use new() to be more correct
        {
            let mut trie = TrieDBMut::new(&mut mdb, &mut root);
            trie.insert(b"key", b"value").expect("insert failed");
            trie.insert(b"value1", &[42]).expect("insert failed");
            trie.insert(b"value2", &[24]).expect("insert failed");
            trie.insert(b":code", b"return 42").expect("insert failed");
            for i in 128u8..255u8 {
                trie.insert(&[i], &[i]).unwrap();
            }
        }
        (mdb, root)
    }

    fn state_transition(
        backend: &TestBackend,
        overlay: &mut OverlayedChanges,
    ) -> (TestBackendTransaction, CryptoHash) {
        let root = *backend.root();
        println!("root before changes is {:?}", root);

        let mut ext = TestExt::new(overlay, &backend, None);
        assert_eq!(root, ext.storage_root());

        // all changes are applied to the overlay
        ext.place_storage(b"dog".to_vec(), Some(b"puppy".to_vec()));

        // storage_root() returns trie root after all changes
        let root_after = ext.storage_root();

        println!("root after changes is {:?}", root_after);

        // consume Ext and return a transaction with changes
        let (storage_transaction, _changes_trie_transaction) = ext.transaction();
        (storage_transaction, root_after)
    }

    #[test]
    fn externalities() {
        // MemoryDB with some trie stored in it
        let (mdb, root) = test_db();

        let backend = TrieBackend::new(mdb, root);
        let mut overlay = Default::default();

        // transaction to apply to MemoryDB with changes for the block
        let (storage_transaction, root_after) = state_transition(&backend, &mut overlay);

        // TODO: test rollback:
        // overlay.discard_prospective() and don't apply transaction
        assert!(backend.storage(&b"dog".to_vec()).unwrap().is_none());

        // Apply changes to the storage
        let mut mdb = backend.into_storage();
        mdb.consolidate(storage_transaction);

        let backend = TrieBackend::new(mdb, root_after);
        let puppy = backend
            .storage(&b"dog".to_vec())
            .unwrap()
            .expect("the key should be in the backend");
        assert_eq!(b"puppy".to_vec(), puppy);
    }
}