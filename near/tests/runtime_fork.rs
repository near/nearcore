use std::collections::HashMap;
use std::sync::Arc;

use tempdir::TempDir;

use near::{get_store_path, GenesisConfig, NightshadeRuntime};
use near_chain::types::RuntimeAdapter;
use near_chain::{Block, Chain, Provenance};
use near_primitives::crypto::signer::InMemorySigner;
use near_primitives::test_utils::init_test_logger;
use near_primitives::transaction::TransactionBody;
use near_store::create_store;

#[test]
fn runtime_hanldle_fork() {
    init_test_logger();

    let tmp_dir = TempDir::new("handle_fork").unwrap();
    let store = create_store(&get_store_path(tmp_dir.path()));
    let genesis_config = GenesisConfig::testing_spec(2, 1);
    let signer = Arc::new(InMemorySigner::from_seed("near.0", "near.0"));
    let runtime =
        Arc::new(NightshadeRuntime::new(tmp_dir.path(), store.clone(), genesis_config.clone()));
    let genesis_chunks = Block::genesis_chunks(runtime.genesis_state().1, runtime.num_shards());

    let mut chain = Chain::new(store, runtime, genesis_config.genesis_time).unwrap();

    let tx1 = TransactionBody::send_money(1, "near.0", "near.1", 100).sign(&*signer);
    let tx2 = TransactionBody::send_money(1, "near.0", "near.1", 500).sign(&*signer);
    let tx3 = TransactionBody::send_money(2, "near.0", "near.1", 100).sign(&*signer);
    let b1 = Block::produce(
        chain.genesis(),
        1,
        genesis_chunks.clone(),
        vec![tx1],
        HashMap::default(),
        vec![],
        signer.clone(),
    );
    chain.process_block(&None, b1.clone(), Provenance::NONE, |_, _, _| {}).unwrap();
    let b2 = Block::produce(
        chain.genesis(),
        2,
        genesis_chunks.clone(),
        vec![tx2],
        HashMap::default(),
        vec![],
        signer.clone(),
    );
    chain.process_block(&None, b2, Provenance::NONE, |_, _, _| {}).unwrap();
    let b3 = Block::produce(
        &b1.header,
        3,
        genesis_chunks.clone(),
        vec![tx3],
        HashMap::default(),
        vec![],
        signer.clone(),
    );
    chain.process_block(&None, b3, Provenance::NONE, |_, _, _| {}).unwrap();
}
