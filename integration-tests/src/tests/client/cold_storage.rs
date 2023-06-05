use borsh::BorshDeserialize;
use near_chain::{ChainGenesis, Provenance};
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_client::ProcessTxResponse;
use near_crypto::{InMemorySigner, KeyType};
use near_epoch_manager::EpochManager;
use near_o11y::testonly::init_test_logger;
use near_primitives::block::Tip;
use near_primitives::sharding::{PartialEncodedChunk, ShardChunk};
use near_primitives::transaction::{
    Action, DeployContractAction, FunctionCallAction, SignedTransaction,
};
use near_primitives_core::types::AccountId;
use near_store::cold_storage::{
    copy_all_data_to_cold, test_cold_genesis_update, test_get_store_initial_writes,
    test_get_store_reads, update_cold_db, update_cold_head,
};
use near_store::metadata::DbKind;
use near_store::metadata::DB_VERSION;
use near_store::test_utils::create_test_node_storage_with_cold;
use near_store::{DBCol, Store, COLD_HEAD_KEY, HEAD_KEY};
use nearcore::config::GenesisExt;
use nearcore::{cold_storage::spawn_cold_store_loop, NearConfig};
use std::collections::HashSet;
use std::str::FromStr;
use strum::IntoEnumIterator;

use super::utils::TestEnvNightshadeSetupExt;

fn check_key(first_store: &Store, second_store: &Store, col: DBCol, key: &[u8]) {
    let pretty_key = near_fmt::StorageKey(key);
    tracing::debug!("Checking {:?} {:?}", col, pretty_key);

    let first_res = first_store.get(col, key);
    let second_res = second_store.get(col, key);

    if col == DBCol::PartialChunks {
        tracing::debug!("{:?}", first_store.get_ser::<PartialEncodedChunk>(col, key));
    }

    assert_eq!(first_res.unwrap(), second_res.unwrap(), "col: {:?} key: {:?}", col, pretty_key);
}

fn check_iter(
    first_store: &Store,
    second_store: &Store,
    col: DBCol,
    no_check_rules: &Vec<Box<dyn Fn(DBCol, &Box<[u8]>, &Box<[u8]>) -> bool>>,
) -> u64 {
    let mut num_checks = 0;
    for (key, value) in first_store.iter(col).map(Result::unwrap) {
        let mut check = true;
        for no_check in no_check_rules {
            if no_check(col, &key, &value) {
                check = false;
            }
        }
        if check {
            check_key(first_store, second_store, col, &key);
            num_checks += 1;
        }
    }
    num_checks
}

/// Deploying test contract and calling write_random_value 5 times every block for 4 epochs.
/// Also doing 5 send transactions every block.
/// 4 epochs, because this test does not cover gc behaviour.
/// After every block updating a separate database with data from client's storage.
/// After 4 epochs we check that everything, that exists in cold columns
/// of the storage of the client also exists in the database to which we were writing.
#[test]
fn test_storage_after_commit_of_cold_update() {
    init_test_logger();

    let epoch_length = 5;
    let max_height = epoch_length * 4;

    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);

    genesis.config.epoch_length = epoch_length;
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.epoch_length = epoch_length;
    let mut env = TestEnv::builder(chain_genesis)
        .real_epoch_managers(&genesis.config)
        .nightshade_runtimes(&genesis)
        .build();

    let (store, ..) = create_test_node_storage_with_cold(DB_VERSION, DbKind::Hot);

    let mut last_hash = *env.clients[0].chain.genesis().hash();

    test_cold_genesis_update(&*store.cold_db().unwrap(), &env.clients[0].runtime_adapter.store())
        .unwrap();

    let state_reads = test_get_store_reads(DBCol::State);
    let state_changes_reads = test_get_store_reads(DBCol::StateChanges);

    for h in 1..max_height {
        let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
        if h == 1 {
            let tx = SignedTransaction::from_actions(
                h,
                "test0".parse().unwrap(),
                "test0".parse().unwrap(),
                &signer,
                vec![Action::DeployContract(DeployContractAction {
                    code: near_test_contracts::rs_contract().to_vec(),
                })],
                last_hash,
            );
            assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
        }
        // Don't send transactions in last two blocks. Because on last block production a chunk from
        // the next block will be produced and information about these transactions will be written
        // into db. And it is a PAIN to filter it out, especially for Receipts.
        if h + 2 < max_height {
            for i in 0..5 {
                let tx = SignedTransaction::from_actions(
                    h * 10 + i,
                    "test0".parse().unwrap(),
                    "test0".parse().unwrap(),
                    &signer,
                    vec![Action::FunctionCall(FunctionCallAction {
                        method_name: "write_random_value".to_string(),
                        args: vec![],
                        gas: 100_000_000_000_000,
                        deposit: 0,
                    })],
                    last_hash,
                );
                assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
            }
            for i in 0..5 {
                let tx = SignedTransaction::send_money(
                    h * 10 + i,
                    "test0".parse().unwrap(),
                    "test1".parse().unwrap(),
                    &signer,
                    1,
                    last_hash,
                );
                assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
            }
        }

        let block = env.clients[0].produce_block(h).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);

        update_cold_db(
            &*store.cold_db().unwrap(),
            &env.clients[0].runtime_adapter.store(),
            &env.clients[0]
                .epoch_manager
                .get_shard_layout(
                    &env.clients[0].epoch_manager.get_epoch_id_from_prev_block(&last_hash).unwrap(),
                )
                .unwrap(),
            &h,
        )
        .unwrap();

        last_hash = *block.hash();
    }

    // assert that we don't read State from db, but from TrieChanges
    assert_eq!(state_reads, test_get_store_reads(DBCol::State));
    // assert that we don't read StateChanges from db again after iter_prefix
    assert_eq!(state_changes_reads, test_get_store_reads(DBCol::StateChanges));

    // We still need to filter out one chunk
    let mut no_check_rules: Vec<Box<dyn Fn(DBCol, &Box<[u8]>, &Box<[u8]>) -> bool>> = vec![];
    no_check_rules.push(Box::new(move |col, _key, value| -> bool {
        if col == DBCol::Chunks {
            let chunk = ShardChunk::try_from_slice(&*value).unwrap();
            if *chunk.prev_block() == last_hash {
                return true;
            }
        }
        false
    }));
    no_check_rules.push(Box::new(move |col, _key, value| -> bool {
        if col == DBCol::PartialChunks {
            let chunk = PartialEncodedChunk::try_from_slice(&*value).unwrap();
            if *chunk.prev_block() == last_hash {
                return true;
            }
        }
        false
    }));
    no_check_rules.push(Box::new(move |col, key, _value| -> bool {
        if col == DBCol::ChunkHashesByHeight {
            let height = u64::from_le_bytes(key[0..8].try_into().unwrap());
            if height == max_height {
                return true;
            }
        }
        false
    }));

    for col in DBCol::iter() {
        if col.is_cold() {
            let num_checks = check_iter(
                &env.clients[0].runtime_adapter.store(),
                &store.get_cold_store().unwrap(),
                col,
                &no_check_rules,
            );
            // assert that this test actually checks something
            // apart from StateChangesForSplitStates and StateHeaders, that are empty
            assert!(
                col == DBCol::StateChangesForSplitStates
                    || col == DBCol::StateHeaders
                    || num_checks > 0
            );
        }
    }
}

/// Producing 10 * 5 blocks and updating HEAD of cold storage after each one.
/// After every update checking that HEAD in cold db, COLD_HEAD in hot db and HEAD in hot store are equal.
#[test]
fn test_cold_db_head_update() {
    init_test_logger();

    let epoch_length = 5;
    let max_height = epoch_length * 10;

    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);

    genesis.config.epoch_length = epoch_length;
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.epoch_length = epoch_length;
    let (store, ..) = create_test_node_storage_with_cold(DB_VERSION, DbKind::Hot);
    let hot_store = &store.get_hot_store();
    let cold_store = &store.get_cold_store().unwrap();
    let mut env = TestEnv::builder(chain_genesis)
        .stores(vec![hot_store.clone()])
        .real_epoch_managers(&genesis.config)
        .nightshade_runtimes(&genesis)
        .build();

    for h in 1..max_height {
        env.produce_block(0, h);
        update_cold_head(&*store.cold_db().unwrap(), &env.clients[0].runtime_adapter.store(), &h)
            .unwrap();

        let head = &env.clients[0]
            .runtime_adapter
            .store()
            .get_ser::<Tip>(DBCol::BlockMisc, HEAD_KEY)
            .unwrap();
        let cold_head_in_hot = hot_store.get_ser::<Tip>(DBCol::BlockMisc, COLD_HEAD_KEY).unwrap();
        let cold_head_in_cold = cold_store.get_ser::<Tip>(DBCol::BlockMisc, HEAD_KEY).unwrap();

        assert_eq!(head, &cold_head_in_cold);
        assert_eq!(head, &cold_head_in_hot);
    }
}

/// Very similar to `test_storage_after_commit_of_cold_update`, but has less transactions,
/// and more importantly SKIPS.
/// Here we are testing that `update_cold_db` handles itself correctly
/// if some heights are not present in blockchain.
#[test]
fn test_cold_db_copy_with_height_skips() {
    init_test_logger();

    let epoch_length = 5;
    let max_height = epoch_length * 4;

    let skips = HashSet::from([1, 4, 5, 7, 11, 14, 16, 19]);

    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);

    genesis.config.epoch_length = epoch_length;
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.epoch_length = epoch_length;
    let mut env = TestEnv::builder(chain_genesis)
        .real_epoch_managers(&genesis.config)
        .nightshade_runtimes(&genesis)
        .build();

    let (storage, ..) = create_test_node_storage_with_cold(DB_VERSION, DbKind::Hot);

    let mut last_hash = *env.clients[0].chain.genesis().hash();

    test_cold_genesis_update(&*storage.cold_db().unwrap(), &env.clients[0].runtime_adapter.store())
        .unwrap();

    for h in 1..max_height {
        let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
        // It is still painful to filter out transactions in last two blocks.
        // So, as block 19 is skipped, blocks 17 and 18 shouldn't contain any transactions.
        // So, we shouldn't send any transactions between block 17 and the previous block.
        // And as block 16 is skipped, the previous block to 17 is 15.
        // Therefore, no transactions after block 15.
        if h < 16 {
            for i in 0..5 {
                let tx = SignedTransaction::send_money(
                    h * 10 + i,
                    "test0".parse().unwrap(),
                    "test1".parse().unwrap(),
                    &signer,
                    1,
                    last_hash,
                );
                assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
            }
        }

        let block = {
            if !skips.contains(&h) {
                let block = env.clients[0].produce_block(h).unwrap().unwrap();
                env.process_block(0, block.clone(), Provenance::PRODUCED);
                Some(block)
            } else {
                None
            }
        };

        update_cold_db(
            &*storage.cold_db().unwrap(),
            &env.clients[0].runtime_adapter.store(),
            &env.clients[0]
                .epoch_manager
                .get_shard_layout(
                    &env.clients[0].epoch_manager.get_epoch_id_from_prev_block(&last_hash).unwrap(),
                )
                .unwrap(),
            &h,
        )
        .unwrap();

        if block.is_some() {
            last_hash = *block.unwrap().hash();
        }
    }

    // We still need to filter out one chunk
    let mut no_check_rules: Vec<Box<dyn Fn(DBCol, &Box<[u8]>, &Box<[u8]>) -> bool>> = vec![];
    no_check_rules.push(Box::new(move |col, _key, value| -> bool {
        if col == DBCol::Chunks {
            let chunk = ShardChunk::try_from_slice(&*value).unwrap();
            if *chunk.prev_block() == last_hash {
                return true;
            }
        }
        false
    }));
    no_check_rules.push(Box::new(move |col, _key, value| -> bool {
        if col == DBCol::PartialChunks {
            let chunk = PartialEncodedChunk::try_from_slice(&*value).unwrap();
            if *chunk.prev_block() == last_hash {
                return true;
            }
        }
        false
    }));

    for col in DBCol::iter() {
        if col.is_cold() && col != DBCol::ChunkHashesByHeight {
            let num_checks = check_iter(
                &env.clients[0].runtime_adapter.store(),
                &storage.get_cold_store().unwrap(),
                col,
                &no_check_rules,
            );
            // assert that this test actually checks something
            // apart from StateChangesForSplitStates and StateHeaders, that are empty
            assert!(
                col == DBCol::StateChangesForSplitStates
                    || col == DBCol::StateHeaders
                    || num_checks > 0
            );
        }
    }
}

/// Producing 4 epochs of blocks with some transactions.
/// Call copying full contents of cold columns to cold storage in batches of specified max_size.
/// Checks COLD_STORE_MIGRATION_BATCH_WRITE_COUNT metric for some batch_sizes:
/// - If batch_size = 0, check that every value was copied in a separate batch.
/// - If batch_size = usize::MAX, check that everything was copied in one batch.
/// Most importantly, checking that everything from cold columns was indeed copied into cold storage.
fn test_initial_copy_to_cold(batch_size: usize) {
    init_test_logger();

    let epoch_length = 5;
    let max_height = epoch_length * 4;

    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);

    genesis.config.epoch_length = epoch_length;
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.epoch_length = epoch_length;
    let mut env = TestEnv::builder(chain_genesis)
        .real_epoch_managers(&genesis.config)
        .nightshade_runtimes(&genesis)
        .build();

    let (store, ..) = create_test_node_storage_with_cold(DB_VERSION, DbKind::Archive);

    let mut last_hash = *env.clients[0].chain.genesis().hash();

    for h in 1..max_height {
        let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
        for i in 0..5 {
            let tx = SignedTransaction::send_money(
                h * 10 + i,
                "test0".parse().unwrap(),
                "test1".parse().unwrap(),
                &signer,
                1,
                last_hash,
            );
            assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
        }

        let block = env.clients[0].produce_block(h).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        last_hash = *block.hash();
    }

    let keep_going = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));

    copy_all_data_to_cold(
        (*store.cold_db().unwrap()).clone(),
        &env.clients[0].runtime_adapter.store(),
        batch_size,
        &keep_going,
    )
    .unwrap();

    for col in DBCol::iter() {
        if !col.is_cold() {
            continue;
        }
        let num_checks = check_iter(
            &env.clients[0].runtime_adapter.store(),
            &store.get_cold_store().unwrap(),
            col,
            &vec![],
        );
        // StateChangesForSplitStates and StateHeaders are empty
        if col == DBCol::StateChangesForSplitStates || col == DBCol::StateHeaders {
            continue;
        }
        // assert that this test actually checks something
        assert!(num_checks > 0);
        if batch_size == 0 {
            assert_eq!(num_checks, test_get_store_initial_writes(col));
        } else if batch_size == usize::MAX {
            assert_eq!(1, test_get_store_initial_writes(col));
        }
    }
}

#[test]
fn test_initial_copy_to_cold_small_batch() {
    test_initial_copy_to_cold(0);
}

#[test]
fn test_initial_copy_to_cold_huge_batch() {
    test_initial_copy_to_cold(usize::MAX);
}

#[test]
fn test_initial_copy_to_cold_medium_batch() {
    test_initial_copy_to_cold(5000);
}

/// This test checks that garbage collection does not remove data needed for cold storage migration prematurely.
/// Test flow:
/// - Produce a lot of blocks.
/// - Manually perform initial migration.
/// - Produce a lot more blocks for hot tail to reach its boundary.
/// - Spawn a cold store loop (just like we do in neard).
/// - Wait 10 seconds.
/// - Check that cold head progressed.
#[test]
fn test_cold_loop_on_gc_boundary() {
    init_test_logger();

    let epoch_length = 5;

    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);

    genesis.config.epoch_length = epoch_length;
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.epoch_length = epoch_length;

    let (store, ..) = create_test_node_storage_with_cold(DB_VERSION, DbKind::Hot);
    let hot_store = &store.get_hot_store();
    let cold_store = &store.get_cold_store().unwrap();
    let mut env = TestEnv::builder(chain_genesis)
        .archive(true)
        .save_trie_changes(true)
        .stores(vec![hot_store.clone()])
        .real_epoch_managers(&genesis.config)
        .nightshade_runtimes(&genesis)
        .build();

    let height_delta = env.clients[0].config.gc.gc_num_epochs_to_keep * epoch_length * 2;

    let mut last_hash = *env.clients[0].chain.genesis().hash();

    for h in 1..height_delta {
        let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
        for i in 0..5 {
            let tx = SignedTransaction::send_money(
                h * 10 + i,
                "test0".parse().unwrap(),
                "test1".parse().unwrap(),
                &signer,
                1,
                last_hash,
            );
            assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
        }

        let block = env.clients[0].produce_block(h).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        last_hash = *block.hash();
    }

    let keep_going = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));

    copy_all_data_to_cold((*store.cold_db().unwrap()).clone(), &hot_store, 1000000, &keep_going)
        .unwrap();

    update_cold_head(&*store.cold_db().unwrap(), &hot_store, &(height_delta - 1)).unwrap();

    for h in height_delta..height_delta * 2 {
        let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
        for i in 0..5 {
            let tx = SignedTransaction::send_money(
                h * 10 + i,
                "test0".parse().unwrap(),
                "test1".parse().unwrap(),
                &signer,
                1,
                last_hash,
            );
            assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
        }

        let block = env.clients[0].produce_block(h).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        last_hash = *block.hash();
    }

    let start_cold_head =
        cold_store.get_ser::<Tip>(DBCol::BlockMisc, COLD_HEAD_KEY).unwrap().unwrap().height;

    let signer =
        InMemorySigner::from_random(AccountId::from_str("test").unwrap(), KeyType::ED25519);

    let mut near_config = NearConfig::new(
        nearcore::config::Config::default(),
        genesis.clone(),
        near_crypto::KeyFile {
            account_id: signer.account_id,
            public_key: signer.public_key,
            secret_key: signer.secret_key,
        },
        None,
    )
    .unwrap();
    near_config.client_config = env.clients[0].config.clone();
    near_config.config.save_trie_changes = Some(true);

    let epoch_manager = EpochManager::new_arc_handle(store.get_hot_store(), &genesis.config);
    spawn_cold_store_loop(&near_config, &store, epoch_manager).unwrap();
    std::thread::sleep(std::time::Duration::from_secs(1));

    let end_cold_head =
        cold_store.get_ser::<Tip>(DBCol::BlockMisc, COLD_HEAD_KEY).unwrap().unwrap().height;

    assert!(
        end_cold_head > start_cold_head,
        "Start cold head is {}, end cold head is {}",
        start_cold_head,
        end_cold_head
    );
}
