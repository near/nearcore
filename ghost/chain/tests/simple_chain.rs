use std::sync::Arc;

use chrono::Utc;

use near_chain::test_utils::KeyValueRuntime;
use near_chain::{Block, BlockHeader, Chain, ErrorKind, Provenance, RuntimeAdapter};
use near_store::{test_utils::create_test_store, Store, StoreUpdate};
use primitives::test_utils::init_test_logger;
use primitives::types::MerkleHash;
use primitives::crypto::signer::InMemorySigner;

fn setup() -> (Chain, Arc<KeyValueRuntime>, Arc<InMemorySigner>) {
    init_test_logger();
    let store = create_test_store();
    let runtime = Arc::new(KeyValueRuntime::new(store.clone()));
    let chain = Chain::new(store, runtime.clone(), Utc::now()).unwrap();
    let signer = Arc::new(InMemorySigner::from_seed("test", "test"));
    (chain, runtime, signer)
}

#[test]
fn empty_chain() {
    let (chain, _, _) = setup();
    assert_eq!(chain.store().head().unwrap().height, 0);
}

#[test]
fn build_chain() {
    let (mut chain, runtime, signer) = setup();
    for i in 0..4 {
        let prev = chain.store().head_header().unwrap();
        let block = Block::produce(&prev, runtime.get_root(), vec![], signer.clone());
        let tip = chain.process_block(block, Provenance::PRODUCED, |_, _, _| {}).unwrap();
        assert_eq!(tip.unwrap().height, i + 1);
    }
    assert_eq!(chain.store().head().unwrap().height, 4);
}

#[test]
fn build_chain_with_orhpans() {
    let (mut chain, _, signer) = setup();
    let mut blocks = vec![chain.store().get_block(&chain.genesis().hash()).unwrap()];
    for i in 1..4 {
        let block = Block::produce(&blocks[i - 1].header, MerkleHash::default(), vec![], signer.clone());
        blocks.push(block);
    }
    assert_eq!(
        chain
            .process_block(blocks.pop().unwrap(), Provenance::PRODUCED, |_, _, _| {})
            .unwrap_err()
            .kind(),
        ErrorKind::Orphan
    );
    assert_eq!(
        chain
            .process_block(blocks.pop().unwrap(), Provenance::PRODUCED, |_, _, _| {})
            .unwrap_err()
            .kind(),
        ErrorKind::Orphan
    );
    let res = chain.process_block(blocks.pop().unwrap(), Provenance::PRODUCED, |_, _, _| {});
    assert_eq!(res.unwrap().unwrap().height, 3);
    assert_eq!(
        chain
            .process_block(blocks.pop().unwrap(), Provenance::PRODUCED, |_, _, _| {})
            .unwrap_err()
            .kind(),
        ErrorKind::Unfit("already known in store".to_string())
    );
}
