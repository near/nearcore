use crate::tests::client::process_blocks::create_nightshade_runtimes;
use near_chain::{ChainGenesis, Provenance};
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_crypto::{InMemorySigner, KeyType};
use near_o11y::testonly::init_test_logger;
use near_primitives::transaction::{
    Action, DeployContractAction, FunctionCallAction, SignedTransaction,
};
use near_store::test_utils::create_test_store;
use near_store::{DBCol, Store};
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
/// 4 epochs, because this test does not cover gc behaviour.
/// After every block StoreUpdate for cold storage is created and committed to a separate storage.
/// After 4 epochs we check that everything, that exists in cold columns
/// of the storage of the second (non-validating) client
/// also exists in the separate storage to which StoreUpdate s were committed.
#[test]
fn test_storage_after_commit_of_cold_update() {
    init_test_logger();

    let cold_store = create_test_store();

    let epoch_length = 5;
    let max_height = epoch_length * 4;

    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);

    genesis.config.epoch_length = epoch_length;
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.epoch_length = epoch_length;
    let mut env = TestEnv::builder(chain_genesis)
        .clients_count(2)
        .runtime_adapters(create_nightshade_runtimes(&genesis, 2))
        .build();

    let mut last_hash = *env.clients[0].chain.genesis().hash();

    {
        let mut cold_update = cold_store.store_update();
        cold_update.test_genesis_update(&env.clients[0].runtime_adapter.get_store()).unwrap();
        cold_update.commit().unwrap();
    }

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
                h * epoch_length + i,
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

        let block = env.clients[0].produce_block(h).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        env.process_block(1, block.clone(), Provenance::NONE);

        last_hash = block.hash().clone();

        let mut cold_update = cold_store.store_update();
        cold_update.add_cold_update(&env.clients[0].runtime_adapter.get_store(), &h).unwrap();
        cold_update.commit().unwrap();
    }

    for col in DBCol::iter() {
        if col.is_cold() {
            check_iter(&env.clients[1].runtime_adapter.get_store(), &cold_store, col);
        }
    }
}
