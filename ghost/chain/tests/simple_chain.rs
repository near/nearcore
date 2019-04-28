use std::sync::Arc;

use chrono::Utc;
use near_chain::{Block, BlockHeader, Chain, RuntimeAdapter};
use near_store::{test_utils::create_test_store, Store, StoreUpdate};
use primitives::test_utils::init_test_logger;
use primitives::types::MerkleHash;

struct KeyValueRuntime {
    store: Arc<Store>,
}

impl KeyValueRuntime {
    pub fn new(store: Arc<Store>) -> Self {
        KeyValueRuntime { store }
    }
}

impl RuntimeAdapter for KeyValueRuntime {
    fn genesis_state(&self) -> (StoreUpdate, MerkleHash) {
        (self.store.store_update(), MerkleHash::default())
    }
}

fn setup() -> Chain {
    init_test_logger();
    let store = create_test_store();
    let runtime = Arc::new(KeyValueRuntime::new(store.clone()));
    Chain::new(store, runtime, Utc::now()).unwrap()
}

#[test]
fn empty_chain() {
    let chain = setup();
    assert_eq!(chain.store().head().unwrap().height, 0);
}
