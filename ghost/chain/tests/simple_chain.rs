use std::sync::Arc;

use chrono::Utc;

use near_chain::{Block, BlockHeader, Chain, Provenance, RuntimeAdapter};
use near_store::{Store, StoreUpdate, test_utils::create_test_store};
use primitives::test_utils::init_test_logger;
use primitives::types::MerkleHash;

struct KeyValueRuntime {
    store: Arc<Store>,
    root: MerkleHash,
}

impl KeyValueRuntime {
    pub fn new(store: Arc<Store>) -> Self {
        KeyValueRuntime { store, root: MerkleHash::default() }
    }

    pub fn get_root(&self) -> MerkleHash {
        self.root
    }
}

impl RuntimeAdapter for KeyValueRuntime {
    fn genesis_state(&self) -> (StoreUpdate, MerkleHash) {
        (self.store.store_update(), MerkleHash::default())
    }
}

fn setup() -> (Chain, Arc<KeyValueRuntime>) {
    init_test_logger();
    let store = create_test_store();
    let runtime = Arc::new(KeyValueRuntime::new(store.clone()));
    let chain = Chain::new(store, runtime.clone(), Utc::now()).unwrap();
    (chain, runtime)
}

#[test]
fn empty_chain() {
    let (chain, _) = setup();
    assert_eq!(chain.store().head().unwrap().height, 0);
}

#[test]
fn build_chain() {
    let (mut chain, runtime) = setup();
    for i in 0..4 {
        let prev = chain.store().head_header().unwrap();
        let block = Block::produce(&prev, runtime.get_root(), vec![]);
        let tip = chain.process_block(block, Provenance::PRODUCED, |_, _, _| {}).unwrap();
        assert_eq!(tip.unwrap().height, i + 1);
    }
    assert_eq!(chain.store().head().unwrap().height, 4);
}
