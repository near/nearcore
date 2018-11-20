use substrate_primitives::hash::H256;
use substrate_primitives::Blake2Hasher;
use substrate_trie::NodeCodec;
use trie_db::TrieMut;

use substrate_state_machine::{Externalities, TestExternalities};

use hex_literal::*;

type MemoryDB<H> = memory_db::MemoryDB<H, trie_db::DBValue>;
type TrieDBMut<'a, H> = trie_db::TrieDBMut<'a, H, NodeCodec<H>>;

fn test_db() -> (MemoryDB<Blake2Hasher>, H256) {
    let mut root = H256::default();
    let mut mdb = MemoryDB::<Blake2Hasher>::default(); // TODO: use new() to be more correct
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

#[test]
fn create_triedb() {
    let (mut memdb, mut root) = test_db();
    println!("trie root is {:?}", root);
    let _ = TrieDBMut::<Blake2Hasher>::from_existing(&mut memdb, &mut root);
}

#[test]
fn externalities_usage() {
    let mut ext = TestExternalities::<Blake2Hasher>::default();
    ext.set_storage(b"doe".to_vec(), b"reindeer".to_vec());
    ext.set_storage(b"dog".to_vec(), b"puppy".to_vec());
    ext.set_storage(b"dogglesworth".to_vec(), b"cat".to_vec());
    const ROOT: [u8; 32] = hex!("0b41e488cccbd67d1f1089592c2c235f5c5399b053f7fe9152dd4b5f279914cd");
    assert_eq!(ext.storage_root(), H256::from(ROOT));
}
