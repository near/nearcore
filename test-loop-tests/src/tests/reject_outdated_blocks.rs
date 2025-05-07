use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::ONE_NEAR;
use itertools::Itertools;
use near_async::time::Duration;
use near_chain::{Block, Error, Provenance};
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_chain_configs::test_genesis::ValidatorsSpec;
use near_crypto::InMemorySigner;
use near_crypto::KeyType;
use near_o11y::testonly::init_test_logger;
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::test_utils::create_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::AccountId;
use near_primitives::types::AccountInfo;
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives_core::num_rational::Rational32;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(12345);
fn pseudo_rand() -> u64 {
    let _ = NEXT.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
        Some(n.wrapping_mul(6364136223846793005).wrapping_add(1))
    });
    NEXT.load(Ordering::Relaxed)
}

fn create_tx(latest_block: &Block, origin: &AccountId, receiver: &AccountId) -> SignedTransaction {
    let nonce = pseudo_rand();
    let signer = InMemorySigner::from_seed(origin.clone(), KeyType::ED25519, origin.as_str());
    SignedTransaction::send_money(
        nonce,
        origin.clone(),
        receiver.clone(),
        &signer.into(),
        10,
        *latest_block.hash(),
    )
}

#[test]
fn slow_test_reject_blocks_with_outdated_protocol_version() {
    init_test_logger();

    let test_loop_builder = TestLoopBuilder::new();
    let epoch_config_store = EpochConfigStore::for_chain_id("mainnet", None).unwrap();
    let epoch_length = 10;

    let initial_balance = 1_000_000 * ONE_NEAR;
    let accounts =
        (0..5).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    let clients = accounts.iter().cloned().collect_vec();
    let validators = vec![AccountInfo {
        account_id: accounts[0].clone(),
        public_key: create_test_signer(accounts[0].as_str()).public_key(),
        amount: 62_500 * ONE_NEAR,
    }];

    let genesis = TestGenesisBuilder::new()
        .genesis_time_from_clock(&test_loop_builder.clock())
        .shard_layout(epoch_config_store.get_config(PROTOCOL_VERSION).shard_layout.clone())
        .epoch_length(epoch_length)
        .validators_spec(ValidatorsSpec::raw(validators, 3, 3, 3))
        .max_inflation_rate(Rational32::new(0, 1))
        .add_user_accounts_simple(&accounts, initial_balance)
        .build();

    let TestLoopEnv { mut test_loop, node_datas, shared_state } = test_loop_builder
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .build()
        .warmup();

    let client = &test_loop.data.get(&node_datas[0].client_sender.actor_handle()).client;
    let rpc_handler = &test_loop.data.get(&node_datas[0].rpc_handler_sender.actor_handle());

    let height = client.chain.head().unwrap().height;
    let latest_block = client.chain.get_block_by_height(height).unwrap();
    let tx = create_tx(&latest_block, &accounts[0], &accounts[1]);
    let _ = rpc_handler.process_tx(tx, false, false);

    // check if block is rejected due to the outdated version
    let client = &mut test_loop.data.get_mut(&node_datas[0].client_sender.actor_handle()).client;
    let mut old_version_block = client.produce_block(height + 1).unwrap().unwrap();
    old_version_block.mut_header().set_latest_protocol_version(PROTOCOL_VERSION - 1);
    let res = client.process_block_test(old_version_block.clone().into(), Provenance::NONE);
    assert!(matches!(res, Err(Error::InvalidProtocolVersion)));

    TestLoopEnv { test_loop, node_datas, shared_state }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}
