use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::env::test_env::TestEnv;
use near_chain::Provenance;
use near_chain_configs::Genesis;
use near_chain_configs::test_utils::TESTING_INIT_STAKE;
use near_client::ProcessTxResponse;
use near_crypto::InMemorySigner;
use near_epoch_manager::EpochManager;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{BlockHeight, BlockHeightDelta, NumBlocks, ShardId};
use near_state_viewer::apply_chain_range;
use near_state_viewer::cli::{ApplyRangeMode, StorageSource};
use near_store::Store;
use near_store::genesis::initialize_genesis_state;
use near_store::test_utils::create_test_store;
use nearcore::NightshadeRuntime;

fn setup(epoch_length: NumBlocks) -> (Store, Genesis, TestEnv) {
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.num_block_producer_seats = 2;
    genesis.config.num_block_producer_seats_per_shard = vec![2];
    genesis.config.epoch_length = epoch_length;
    let store = create_test_store();
    initialize_genesis_state(store.clone(), &genesis, None);
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config, None);
    let nightshade_runtime = NightshadeRuntime::test(
        Path::new("."),
        store.clone(),
        &genesis.config,
        epoch_manager.clone(),
    );
    let env = TestEnv::builder(&genesis.config)
        .validator_seats(2)
        .stores(vec![store.clone()])
        .epoch_managers(vec![epoch_manager])
        .runtimes(vec![nightshade_runtime])
        .build();
    (store, genesis, env)
}

/// Produces blocks, avoiding the potential failure where the client is not the
/// block producer for each subsequent height (this can happen when a new validator
/// is staked since they will also have heights where they should produce the block instead).
fn safe_produce_blocks(
    env: &mut TestEnv,
    initial_height: BlockHeight,
    num_blocks: BlockHeightDelta,
    block_without_chunks: Option<BlockHeight>,
) {
    let mut h = initial_height;
    let mut blocks = vec![];
    for _ in 1..=num_blocks {
        let mut block = None;
        // `env.clients[0]` may not be the block producer at `h`,
        // loop until we find a height env.clients[0] should produce.
        while block.is_none() {
            block = env.clients[0].produce_block(h).unwrap();
            h += 1;
        }
        let mut block = block.unwrap();
        if let Some(block_without_chunks) = block_without_chunks {
            if block_without_chunks == h {
                assert!(!blocks.is_empty());
                testlib::process_blocks::set_no_chunk_in_block(&mut block, blocks.last().unwrap())
            }
        }
        blocks.push(block.clone());
        env.process_block(0, block, Provenance::PRODUCED);
    }
}

#[test]
fn test_apply_chain_range() {
    let epoch_length = 4;
    let (store, genesis, mut env) = setup(epoch_length);
    let genesis_hash = *env.clients[0].chain.genesis().hash();
    let signer = InMemorySigner::test_signer(&"test1".parse().unwrap());
    let tx = SignedTransaction::stake(
        1,
        "test1".parse().unwrap(),
        &signer,
        TESTING_INIT_STAKE,
        signer.public_key(),
        genesis_hash,
    );
    assert_eq!(env.rpc_handlers[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);

    safe_produce_blocks(&mut env, 1, epoch_length * 2 + 1, None);

    initialize_genesis_state(store.clone(), &genesis, None);
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config, None);
    let runtime = NightshadeRuntime::test(
        Path::new("."),
        store.clone(),
        &genesis.config,
        epoch_manager.clone(),
    );
    apply_chain_range(
        ApplyRangeMode::Parallel,
        store,
        None,
        &genesis,
        None,
        None,
        ShardId::new(0),
        epoch_manager.as_ref(),
        runtime,
        true,
        None,
        false,
        StorageSource::Trie,
    );
}

#[test]
fn test_apply_chain_range_no_chunks() {
    let epoch_length = 4;
    let (store, genesis, mut env) = setup(epoch_length);
    let genesis_hash = *env.clients[0].chain.genesis().hash();
    let signer = InMemorySigner::test_signer(&"test1".parse().unwrap());
    let tx = SignedTransaction::stake(
        1,
        "test1".parse().unwrap(),
        &signer,
        TESTING_INIT_STAKE,
        signer.public_key(),
        genesis_hash,
    );
    assert_eq!(env.rpc_handlers[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);

    safe_produce_blocks(&mut env, 1, epoch_length * 2 + 1, Some(5));

    initialize_genesis_state(store.clone(), &genesis, None);
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config, None);
    let runtime = NightshadeRuntime::test(
        Path::new("."),
        store.clone(),
        &genesis.config,
        epoch_manager.clone(),
    );
    let mut file = tempfile::NamedTempFile::new().unwrap();
    apply_chain_range(
        ApplyRangeMode::Parallel,
        store,
        None,
        &genesis,
        None,
        None,
        ShardId::new(0),
        epoch_manager.as_ref(),
        runtime,
        true,
        Some(file.as_file_mut()),
        false,
        StorageSource::Trie,
    );
    let mut csv = String::new();
    file.as_file_mut().seek(SeekFrom::Start(0)).unwrap();
    file.as_file_mut().read_to_string(&mut csv).unwrap();
    let lines: Vec<&str> = csv.split("\n").collect();
    assert!(lines[0].contains("Height"));
    let mut has_tx = 0;
    let mut no_tx = 0;
    for line in &lines {
        if line.contains(",test0,1,0,") {
            has_tx += 1;
        }
        if line.contains(",test0,0,0,") {
            no_tx += 1;
        }
    }
    assert_eq!(has_tx, 1, "{:#?}", lines);
    assert_eq!(no_tx, 8, "{:#?}", lines);
}
