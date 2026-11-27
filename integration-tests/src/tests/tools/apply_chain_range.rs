use crate::env::test_env::TestEnv;
use itertools::Itertools;
use near_chain::{ChainStore, ChainStoreAccess, Provenance};
use near_chain_configs::Genesis;
use near_chain_configs::test_utils::TESTING_INIT_STAKE;
use near_client::ProcessTxResponse;
use near_crypto::InMemorySigner;
use near_epoch_manager::EpochManager;
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{ECC_METHOD_NAME, EccTransaction, SignedTransaction};
use near_primitives::types::{
    AccountId, Balance, BlockHeight, BlockHeightDelta, NumBlocks, ShardId,
};
use near_state_viewer::apply_chain_range;
use near_state_viewer::cli::{ApplyRangeMode, StorageSource};
use near_store::Store;
use near_store::genesis::initialize_genesis_state;
use near_store::test_utils::create_test_store;
use nearcore::NightshadeRuntime;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

fn setup(epoch_length: NumBlocks) -> (Store, Genesis, TestEnv) {
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.num_block_producer_seats = 2;
    genesis.config.epoch_length = epoch_length;
    genesis.config.transaction_validity_period = epoch_length * 2;
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
    let mut blocks: Vec<std::sync::Arc<_>> = vec![];
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
                testlib::process_blocks::set_no_chunk_in_block(
                    std::sync::Arc::make_mut(&mut block),
                    blocks.last().unwrap(),
                )
            }
        }
        blocks.push(block.clone());
        env.process_block(0, block, Provenance::PRODUCED);
    }
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
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
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
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
    // The send_money tx produces an extra refund receipt at some height, turning one
    // "no_tx, no_receipt" line into a receipt-bearing line.
    assert_eq!(no_tx, 7, "{:#?}", lines);
}

/// Produces `num_blocks` blocks on top of the head.
fn produce_blocks(env: &mut TestEnv, num_blocks: BlockHeightDelta) {
    let next_height = env.clients[0].chain.head().unwrap().height + 1;
    safe_produce_blocks(env, next_height, num_blocks, None);
}

/// The height of the block whose new chunk includes the transaction with `tx_hash`.
fn inclusion_height(chain_store: &ChainStore, tx_hash: &CryptoHash) -> BlockHeight {
    let head_height = chain_store.head().unwrap().height;
    (1..=head_height)
        .find(|&height| {
            let Ok(block_hash) = chain_store.get_block_hash_by_height(height) else { return false };
            let block = chain_store.get_block(&block_hash).unwrap();
            block.chunks().iter_raw().filter(|chunk| chunk.is_new_chunk(height)).any(|chunk| {
                let chunk = chain_store.get_chunk(&chunk.chunk_hash()).unwrap();
                chunk.to_transactions().iter().any(|tx| &tx.get_hash() == tx_hash)
            })
        })
        .expect("transaction is not included in any block")
}

/// With `only_contracts`, a chunk is applied when it has a transaction that deploys or calls a
/// contract, which includes an ECC, and skipped when it has just a transfer or nothing.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_apply_chain_range_only_contracts_applies_ecc() {
    let epoch_length = 10;
    let (store, genesis, mut env) = setup(epoch_length);
    let genesis_hash = *env.clients[0].chain.genesis().hash();
    let contract_id: AccountId = "test1".parse().unwrap();
    let signer = InMemorySigner::test_signer(&contract_id);

    // A contract that accepts every ECC.
    let code = near_test_contracts::wat_contract(&format!(
        r#"(module (func (export "{ECC_METHOD_NAME}")))"#
    ));
    let deploy = SignedTransaction::deploy_contract(1, &contract_id, code, &signer, genesis_hash);
    let deploy_hash = deploy.get_hash();
    assert_eq!(env.rpc_handlers[0].process_tx(deploy, false, false), ProcessTxResponse::ValidTx);
    produce_blocks(&mut env, 3);

    let ecc = EccTransaction::new(contract_id.clone(), vec![1]);
    let ecc_hash = ecc.get_hash();
    assert_eq!(env.rpc_handlers[0].process_tx(ecc, false, false), ProcessTxResponse::ValidTx);
    produce_blocks(&mut env, 3);

    let transfer = SignedTransaction::send_money(
        2,
        contract_id,
        "test0".parse().unwrap(),
        &signer,
        Balance::from_yoctonear(100),
        genesis_hash,
    );
    let transfer_hash = transfer.get_hash();
    assert_eq!(env.rpc_handlers[0].process_tx(transfer, false, false), ProcessTxResponse::ValidTx);
    produce_blocks(&mut env, 6);

    initialize_genesis_state(store.clone(), &genesis, None);
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config, None);
    let runtime = NightshadeRuntime::test(
        Path::new("."),
        store.clone(),
        &genesis.config,
        epoch_manager.clone(),
    );
    let chain_store =
        ChainStore::new(store.clone(), false, genesis.config.transaction_validity_period);
    let deploy_height = inclusion_height(&chain_store, &deploy_hash);
    let ecc_height = inclusion_height(&chain_store, &ecc_hash);
    let transfer_height = inclusion_height(&chain_store, &transfer_hash);
    assert!(deploy_height < ecc_height && ecc_height < transfer_height);
    // The range applied is up to the final head.
    assert!(chain_store.final_head().unwrap().height > transfer_height);

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
        false,
        Some(file.as_file_mut()),
        true,
        StorageSource::Trie,
    );

    // The rows of the chunks applied with transactions, as (height, number of transactions). A
    // chunk missing at its height is applied with none, whatever `only_contracts` says.
    let mut csv = String::new();
    file.as_file_mut().seek(SeekFrom::Start(0)).unwrap();
    file.as_file_mut().read_to_string(&mut csv).unwrap();
    let applied = csv
        .lines()
        .skip(1)
        .map(|line| {
            let fields = line.split(',').collect_vec();
            (fields[0].parse::<BlockHeight>().unwrap(), fields[3].parse::<usize>().unwrap())
        })
        .filter(|&(_, num_tx)| num_tx > 0)
        .sorted()
        .collect_vec();
    assert_eq!(applied, vec![(deploy_height, 1), (ecc_height, 1)]);
}
