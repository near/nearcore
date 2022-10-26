use crate::tests::client::process_blocks::create_nightshade_runtimes;
use near_chain::{ChainGenesis, Provenance};
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_crypto::{InMemorySigner, KeyType};
use near_o11y::testonly::init_test_logger;
use near_primitives::transaction::{
    Action, DeployContractAction, FunctionCallAction, SignedTransaction,
};
use near_store::cold_storage::{test_cold_genesis_update, update_cold_db};
use near_store::db::TestDB;
use near_store::{DBCol, NodeStorage, Store, Temperature};
use nearcore::config::GenesisExt;
use strum::IntoEnumIterator;

fn check_key(first_store: &Store, second_store: &Store, col: DBCol, key: &[u8]) {
    tracing::debug!("Checking {:?} {:?}", col, key);

    let first_res = first_store.get(col, key);
    let second_res = second_store.get(col, key);

    assert_eq!(first_res.unwrap(), second_res.unwrap());
}

fn check_iter(first_store: &Store, second_store: &Store, col: DBCol) {
    for (key, _) in first_store.iter(col).map(Result::unwrap) {
        check_key(first_store, second_store, col, &key);
    }
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

    let cold_db = TestDB::new();

    let epoch_length = 5;
    let max_height = epoch_length * 4;

    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);

    genesis.config.epoch_length = epoch_length;
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.epoch_length = epoch_length;
    let mut env = TestEnv::builder(chain_genesis)
        .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
        .build();

    let mut last_hash = *env.clients[0].chain.genesis().hash();

    test_cold_genesis_update(&*cold_db, &env.clients[0].runtime_adapter.store()).unwrap();

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
                "test0".parse().unwrap(),
                &signer,
                1,
                last_hash,
            );
            env.clients[0].process_tx(tx, false, false);
        }

        let block = env.clients[0].produce_block(h).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);

        last_hash = block.hash().clone();

        update_cold_db(&*cold_db, &env.clients[0].runtime_adapter.store(), &h).unwrap();
    }

    let cold_store = NodeStorage::new(cold_db).get_store(Temperature::Hot);

    for col in DBCol::iter() {
        if col.is_cold() {
            check_iter(&env.clients[0].runtime_adapter.store(), &cold_store, col);
        }
    }
}
