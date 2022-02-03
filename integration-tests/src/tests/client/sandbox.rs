use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use actix::System;

use near_actix_test_utils::run_actix;
use near_chain::{ChainGenesis, Provenance, RuntimeAdapter};
use near_chain_configs::Genesis;
use near_client::test_utils::{setup_mock, TestEnv, MAX_BLOCK_PROD_TIME, MIN_BLOCK_PROD_TIME};
use near_crypto::{InMemorySigner, KeyType};
use near_logger_utils::init_test_logger;
use near_network::types::{
    NetworkClientMessages, NetworkRequests, NetworkResponses, PeerManagerMessageResponse,
};
use near_network_primitives::types::NetworkSandboxMessage;
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
            Path::new("../../../.."),
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

#[test]
fn test_fast_forward() {
    const BLOCKS_TO_FASTFORWARD: BlockHeight = 10_000;
    const BLOCKS_TO_PRODUCE: u64 = 20;

    // the 1_000_000 is to convert milliseconds to nanoseconds, where MIN/MAX_BLOCK_PROD_TIME are in milliseconds:
    const FAST_FORWARD_DELTA: u64 = (BLOCKS_TO_FASTFORWARD + BLOCKS_TO_PRODUCE) * 1_000_000;

    init_test_logger();
    run_actix(async {
        let count = Arc::new(AtomicUsize::new(0));
        let first_block_timestamp = Arc::new(AtomicU64::new(0));

        // Produce 20 blocks
        let (_client, _view_client) = setup_mock(
            vec!["test".parse().unwrap()],
            "test".parse().unwrap(),
            true,
            false,
            Box::new(move |msg, _ctx, client| {
                if let NetworkRequests::Block { block } = msg.as_network_requests_ref() {
                    let height = block.header().height();
                    let timestamp = block.header().raw_timestamp();

                    count.fetch_add(1, Ordering::Relaxed);
                    let at = count.load(Ordering::Relaxed);

                    // Fast forward by 10,000 blocks after the first block produced:
                    if at == 1 {
                        first_block_timestamp.store(timestamp, Ordering::Relaxed);
                        client.do_send(NetworkClientMessages::Sandbox(
                            NetworkSandboxMessage::SandboxFastForward(BLOCKS_TO_FASTFORWARD),
                        ));
                    }

                    // Check at final block if timestamp and block height matches up:
                    if at >= BLOCKS_TO_PRODUCE as usize {
                        let before_forward = first_block_timestamp.load(Ordering::Relaxed);
                        let min_forwarded_time =
                            before_forward + FAST_FORWARD_DELTA * MIN_BLOCK_PROD_TIME;
                        let max_forwarded_time =
                            before_forward + FAST_FORWARD_DELTA * MAX_BLOCK_PROD_TIME;
                        assert!(
                            height >= BLOCKS_TO_FASTFORWARD,
                            "Was not able to fast forward. Current height: {}",
                            height
                        );

                        // Check if final block timestamp is within fast forwarded time min and max:
                        assert!(
                            (min_forwarded_time..max_forwarded_time).contains(&timestamp),
                            "Forwarded timestamp {} is not within expected range ({},{})",
                            timestamp,
                            min_forwarded_time,
                            max_forwarded_time
                        );

                        System::current().stop();
                    }
                }
                PeerManagerMessageResponse::NetworkResponses(NetworkResponses::NoResponse)
            }),
        );

        near_network::test_utils::wait_or_panic(5000);
    });
}
