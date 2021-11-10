use std::path::Path;
use std::sync::Arc;

use near_chain::{ChainGenesis, Provenance, RuntimeAdapter};
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_crypto::{InMemorySigner, KeyType};
use near_primitives::account::Account;
use near_primitives::serialize::{from_base64, to_base64};
use near_primitives::state_record::StateRecord;
use near_primitives::transaction::{
    Action, DeployContractAction, FunctionCallAction, SignedTransaction,
};
use near_primitives::types::{AccountId, BlockHeight, Nonce};
use near_store::test_utils::create_test_store;
use nearcore::config::GenesisExt;

fn test_setup() -> (TestEnv, InMemorySigner) {
    let epoch_length = 5;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    let mut env = TestEnv::builder(ChainGenesis::test())
        .runtime_adapters(vec![Arc::new(nearcore::NightshadeRuntime::test(
            Path::new("."),
            create_test_store(),
            &genesis,
        )) as Arc<dyn RuntimeAdapter>])
        .build();
    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    send_tx(
        &mut env,
        1,
        "test0".parse().unwrap(),
        "test0".parse().unwrap(),
        &signer,
        vec![Action::DeployContract(DeployContractAction {
            code: near_test_contracts::rs_contract().to_vec(),
        })],
    );
    do_blocks(&mut env, 1, 3);

    send_tx(
        &mut env,
        2,
        "test0".parse().unwrap(),
        "test0".parse().unwrap(),
        &signer,
        vec![Action::FunctionCall(FunctionCallAction {
            method_name: "write_random_value".to_string(),
            args: vec![],
            gas: 100000000000000,
            deposit: 0,
        })],
    );
    do_blocks(&mut env, 3, 9);
    (env, signer)
}

fn do_blocks(env: &mut TestEnv, start: BlockHeight, end: BlockHeight) {
    for i in start..end {
        let last_block = env.clients[0].produce_block(i).unwrap().unwrap();
        env.process_block(0, last_block.clone(), Provenance::PRODUCED);
    }
}

fn send_tx(
    env: &mut TestEnv,
    nonce: Nonce,
    signer_id: AccountId,
    receiver_id: AccountId,
    signer: &InMemorySigner,
    actions: Vec<Action>,
) {
    let hash = env.clients[0].chain.head().unwrap().last_block_hash;
    let tx = SignedTransaction::from_actions(nonce, signer_id, receiver_id, signer, actions, hash);
    env.clients[0].process_tx(tx, false, false);
}

#[test]
#[cfg(feature = "sandbox")]
fn test_patch_state() {
    let (mut env, _signer) = test_setup();

    let state = env.query_state("test0".parse().unwrap());
    env.clients[0].chain.patch_state(vec![StateRecord::Data {
        account_id: "test0".parse().unwrap(),
        data_key: from_base64(&state[0].key).unwrap(),
        value: b"world".to_vec(),
    }]);

    do_blocks(&mut env, 9, 20);
    let state2 = env.query_state("test0".parse().unwrap());
    assert_eq!(state2.len(), 1);
    assert_eq!(state2[0].value, to_base64(b"world"));
}

#[test]
#[cfg(feature = "sandbox")]
fn test_patch_account() {
    let (mut env, _signer) = test_setup();
    let mut test1: Account = env.query_account("test1".parse().unwrap()).into();
    test1.set_amount(10);

    env.clients[0].chain.patch_state(vec![StateRecord::Account {
        account_id: "test1".parse().unwrap(),
        account: test1,
    }]);
    do_blocks(&mut env, 9, 20);
    let test1_after = env.query_account("test1".parse().unwrap());
    assert_eq!(test1_after.amount, 10);
}
