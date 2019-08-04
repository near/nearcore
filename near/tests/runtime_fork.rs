use std::collections::HashMap;
use std::sync::Arc;

use tempdir::TempDir;

use near::{get_store_path, GenesisConfig, NightshadeRuntime};
use near_chain::types::RuntimeAdapter;
use near_chain::{Block, Chain, ChainGenesis, Provenance};
use near_primitives::crypto::signer::InMemorySigner;
use near_primitives::test_utils::init_test_logger;
use near_primitives::transaction::TransactionBody;
use near_primitives::types::EpochId;
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
    let genesis_chunks = Block::genesis_chunks(
        runtime.genesis_state().1,
        runtime.num_shards(),
        genesis_config.gas_limit,
    );

    let mut chain = Chain::new(
        store,
        runtime,
        &ChainGenesis::new(
            genesis_config.genesis_time,
            genesis_config.gas_limit,
            genesis_config.gas_price,
            genesis_config.total_supply,
            genesis_config.max_inflation_rate,
            genesis_config.gas_price_adjustment_rate,
        ),
    )
    .unwrap();

    let tx1 = TransactionBody::send_money(1, "near.0", "near.1", 100).sign(&*signer);
    let tx2 = TransactionBody::send_money(1, "near.0", "near.1", 500).sign(&*signer);
    let tx3 = TransactionBody::send_money(2, "near.0", "near.1", 100).sign(&*signer);
    let b1 = Block::produce(
        chain.genesis(),
        1,
        genesis_chunks.clone(),
        EpochId::default(),
        vec![tx1],
        HashMap::default(),
        genesis_config.gas_price_adjustment_rate,
        genesis_config.max_inflation_rate,
        signer.clone(),
    );
    chain.process_block(&None, b1.clone(), Provenance::NONE, |_, _, _| {}, |_| {}).unwrap();
    let b2 = Block::produce(
        chain.genesis(),
        2,
        genesis_chunks.clone(),
        EpochId::default(),
        vec![tx2],
        HashMap::default(),
        genesis_config.gas_price_adjustment_rate,
        genesis_config.max_inflation_rate,
        signer.clone(),
    );
    chain.process_block(&None, b2, Provenance::NONE, |_, _, _| {}, |_| {}).unwrap();
    let b3 = Block::produce(
        &b1.header,
        3,
        genesis_chunks.clone(),
        EpochId::default(),
        vec![tx3],
        HashMap::default(),
        genesis_config.gas_price_adjustment_rate,
        genesis_config.max_inflation_rate,
        signer.clone(),
    );
    chain.process_block(&None, b3, Provenance::NONE, |_, _, _| {}, |_| {}).unwrap();
}
