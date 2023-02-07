use crate::tests::client::process_blocks::create_nightshade_runtime_with_store;
use crate::tests::client::process_blocks::create_nightshade_runtimes;
use borsh::BorshDeserialize;
use near_chain::{ChainGenesis, Provenance};
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_crypto::{InMemorySigner, KeyType};
use near_o11y::testonly::init_test_logger;
use near_primitives::block::Tip;
use near_primitives::sharding::ShardChunk;
use near_primitives::transaction::{
    Action, DeployContractAction, FunctionCallAction, SignedTransaction,
};
use near_store::cold_storage::{
    test_cold_genesis_update, test_get_store_reads, update_cold_db, update_cold_head,
};
use near_store::metadata::DbKind;
use near_store::metadata::DB_VERSION;
use near_store::test_utils::create_test_node_storage_with_cold;
use near_store::{DBCol, Store, Temperature, COLD_HEAD_KEY, HEAD_KEY};
use nearcore::config::GenesisExt;
use std::collections::HashSet;
use strum::IntoEnumIterator;

fn check_key(first_store: &Store, second_store: &Store, col: DBCol, key: &[u8]) {
    tracing::debug!("Checking {:?} {:?}", col, key);

    let first_res = first_store.get(col, key);
    let second_res = second_store.get(col, key);

    assert_eq!(first_res.unwrap(), second_res.unwrap());
}

fn check_iter(
    first_store: &Store,
    second_store: &Store,
    col: DBCol,
    no_check_rules: &Vec<Box<dyn Fn(DBCol, &Box<[u8]>) -> bool>>,
) -> u64 {
    let mut num_checks = 0;
    for (key, value) in first_store.iter(col).map(Result::unwrap) {
        let mut check = true;
        for no_check in no_check_rules {
            if no_check(col, &value) {
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
        .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
        .build();

    let store = create_test_node_storage_with_cold(DB_VERSION, DbKind::Hot);

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
            env.clients[0].process_tx(tx, false, false);
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
                env.clients[0].process_tx(tx, false, false);
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
                env.clients[0].process_tx(tx, false, false);
            }
        }

        let block = env.clients[0].produce_block(h).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);

        update_cold_db(
            &*store.cold_db().unwrap(),
            &env.clients[0].runtime_adapter.store(),
            &env.clients[0]
                .runtime_adapter
                .get_shard_layout(
                    &env.clients[0]
                        .runtime_adapter
                        .get_epoch_id_from_prev_block(&last_hash)
                        .unwrap(),
                )
                .unwrap(),
            &h,
        )
        .unwrap();

        last_hash = block.hash().clone();
    }

    // assert that we don't read State from db, but from TrieChanges
    assert_eq!(state_reads, test_get_store_reads(DBCol::State));
    // assert that we don't read StateChanges from db again after iter_prefix
    assert_eq!(state_changes_reads, test_get_store_reads(DBCol::StateChanges));

    // We still need to filter out one chunk
    let mut no_check_rules: Vec<Box<dyn Fn(DBCol, &Box<[u8]>) -> bool>> = vec![];
    no_check_rules.push(Box::new(move |col, value| -> bool {
        if col == DBCol::Chunks {
            let chunk = ShardChunk::try_from_slice(&*value).unwrap();
            if *chunk.prev_block() == last_hash {
                return true;
            }
        }
        false
    }));

    for col in DBCol::iter() {
        if col.is_cold() {
            let num_checks = check_iter(
                &env.clients[0].runtime_adapter.store(),
                &store.get_store(Temperature::Cold),
                col,
                &no_check_rules,
            );
            // assert that this test actually checks something
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
    let store = create_test_node_storage_with_cold(DB_VERSION, DbKind::Hot);
    let hot_store = &store.get_store(Temperature::Hot);
    let cold_store = &store.get_store(Temperature::Cold);
    let runtime_adapter = create_nightshade_runtime_with_store(&genesis, &hot_store);
    let mut env = TestEnv::builder(chain_genesis).runtime_adapters(vec![runtime_adapter]).build();

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
        .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
        .build();

    let store = create_test_node_storage_with_cold(DB_VERSION, DbKind::Hot);

    let mut last_hash = *env.clients[0].chain.genesis().hash();

    test_cold_genesis_update(&*store.cold_db().unwrap(), &env.clients[0].runtime_adapter.store())
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
                env.clients[0].process_tx(tx, false, false);
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
            &*store.cold_db().unwrap(),
            &env.clients[0].runtime_adapter.store(),
            &env.clients[0]
                .runtime_adapter
                .get_shard_layout(
                    &env.clients[0]
                        .runtime_adapter
                        .get_epoch_id_from_prev_block(&last_hash)
                        .unwrap(),
                )
                .unwrap(),
            &h,
        )
        .unwrap();

        if block.is_some() {
            last_hash = block.unwrap().hash().clone();
        }
    }

    // We still need to filter out one chunk
    let mut no_check_rules: Vec<Box<dyn Fn(DBCol, &Box<[u8]>) -> bool>> = vec![];
    no_check_rules.push(Box::new(move |col, value| -> bool {
        if col == DBCol::Chunks {
            let chunk = ShardChunk::try_from_slice(&*value).unwrap();
            if *chunk.prev_block() == last_hash {
                return true;
            }
        }
        false
    }));

    for col in DBCol::iter() {
        if col.is_cold() {
            let num_checks = check_iter(
                &env.clients[0].runtime_adapter.store(),
                &store.get_store(Temperature::Cold),
                col,
                &no_check_rules,
            );
            // assert that this test actually checks something
            assert!(
                col == DBCol::StateChangesForSplitStates
                    || col == DBCol::StateHeaders
                    || num_checks > 0
            );
        }
    }
}
