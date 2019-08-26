use std::collections::HashMap;
use std::sync::Arc;

use tempdir::TempDir;

use near::{get_store_path, GenesisConfig, NightshadeRuntime};
use near_chain::{Block, Chain, Provenance};
use near_crypto::{InMemorySigner, KeyType};
use near_primitives::hash::CryptoHash;
use near_primitives::test_utils::init_test_logger;
use near_primitives::transaction::SignedTransaction;
use near_store::create_store;

#[test]
fn runtime_hanldle_fork() {
    init_test_logger();

    let tmp_dir = TempDir::new("handle_fork").unwrap();
    let store = create_store(&get_store_path(tmp_dir.path()));
    let genesis_config = GenesisConfig::testing_spec(2, 1);
    let signer = Arc::new(InMemorySigner::from_seed("near.0", KeyType::ED25519, "near.0"));
    let runtime =
        Arc::new(NightshadeRuntime::new(tmp_dir.path(), store.clone(), genesis_config.clone()));

    let mut chain = Chain::new(store, runtime, genesis_config.genesis_time).unwrap();

    let tx1 = SignedTransaction::send_money(
        1,
        "near.0".to_string(),
        "near.1".to_string(),
        signer.clone(),
        100,
    );
    let tx2 = SignedTransaction::send_money(
        1,
        "near.0".to_string(),
        "near.1".to_string(),
        signer.clone(),
        500,
    );
    let tx3 = SignedTransaction::send_money(
        2,
        "near.0".to_string(),
        "near.1".to_string(),
        signer.clone(),
        100,
    );
    let state_root = chain.get_post_state_root(&chain.genesis().hash()).unwrap().clone();
    let b1 = Block::produce(
        chain.genesis(),
        1,
        state_root,
        CryptoHash::default(),
        vec![tx1],
        HashMap::default(),
        vec![],
        signer.clone(),
    );
    chain.process_block(b1.clone(), Provenance::NONE, |_, _, _| {}).unwrap();
    let b2 = Block::produce(
        chain.genesis(),
        2,
        state_root,
        CryptoHash::default(),
        vec![tx2],
        HashMap::default(),
        vec![],
        signer.clone(),
    );
    chain.process_block(b2, Provenance::NONE, |_, _, _| {}).unwrap();
    let state_root3 = chain.get_post_state_root(&b1.hash()).unwrap().clone();
    let b3 = Block::produce(
        &b1.header,
        3,
        state_root3,
        CryptoHash::default(),
        vec![tx3],
        HashMap::default(),
        vec![],
        signer.clone(),
    );
    chain.process_block(b3, Provenance::NONE, |_, _, _| {}).unwrap();
}
