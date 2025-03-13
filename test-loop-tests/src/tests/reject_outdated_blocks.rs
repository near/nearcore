use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::ONE_NEAR;
use itertools::Itertools;
use near_async::test_loop::data::TestLoopData;
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
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives_core::num_rational::Rational32;
use near_primitives_core::version::ProtocolFeature;
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

// TODO(resharding) - Implement this test for the stable protocol version. The
// original implementation was not compatible with the two shard layout changes
// in between the RejectBlocksWithOutdatedProtocolVersions and stable.
#[test]
fn slow_test_reject_blocks_with_outdated_protocol_version_protocol_upgrade() {
    init_test_logger();

    let mut protocol_version =
        ProtocolFeature::RejectBlocksWithOutdatedProtocolVersions.protocol_version() - 1;
    let target_protocol_version =
        ProtocolFeature::RejectBlocksWithOutdatedProtocolVersions.protocol_version();
    let protocol_upgrade_schedule =
        ProtocolUpgradeVotingSchedule::new_immediate(target_protocol_version);

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
        .protocol_version(protocol_version)
        .genesis_time_from_clock(&test_loop_builder.clock())
        .shard_layout(epoch_config_store.get_config(protocol_version).shard_layout.clone())
        .epoch_length(epoch_length)
        .validators_spec(ValidatorsSpec::raw(validators, 3, 3, 3))
        .max_inflation_rate(Rational32::new(0, 1))
        .add_user_accounts_simple(&accounts, initial_balance)
        .build();

    let TestLoopEnv { mut test_loop, node_datas, shared_state } = test_loop_builder
        .genesis(genesis)
        .protocol_upgrade_schedule(protocol_upgrade_schedule)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .build()
        .warmup();

    let sender = node_datas[0].client_sender.clone();
    let handle = sender.actor_handle();

    let tx_sender = node_datas[0].tx_processor_sender.clone();
    let tx_handle = tx_sender.actor_handle();

    // produce a valid block then tamper it with outdated protocol version
    // then check if block is NOT rejected because of outdated protocol version
    let (height, latest_block) = {
        let client = &test_loop.data.get_mut(&handle).client;
        let height = client.chain.head().unwrap().height;
        (height, client.chain.get_block_by_height(height).unwrap())
    };

    {
        let tx = create_tx(&latest_block, &accounts[0], &accounts[1]);
        let tx_processor = &test_loop.data.get(&tx_handle);
        let _ = tx_processor.process_tx(tx, false, false);
    }

    {
        let client = &mut test_loop.data.get_mut(&handle).client;
        let mut old_version_block = client.produce_block(height + 1).unwrap().unwrap();
        old_version_block.mut_header().set_latest_protocol_version(
            ProtocolFeature::RejectBlocksWithOutdatedProtocolVersions.protocol_version() - 2,
        );

        let epoch_id = client
            .epoch_manager
            .get_epoch_id_from_prev_block(&client.chain.head().unwrap().last_block_hash)
            .unwrap();
        protocol_version = client.epoch_manager.get_epoch_protocol_version(&epoch_id).unwrap();
        assert!(
            protocol_version
                < ProtocolFeature::RejectBlocksWithOutdatedProtocolVersions.protocol_version()
        );
        assert!(old_version_block.header().latest_protocol_version() < protocol_version);
        let res = client.process_block_test(old_version_block.clone().into(), Provenance::NONE);
        assert!(!matches!(res, Err(Error::InvalidProtocolVersion)));
    }

    // wait for protocol version to advance
    test_loop.run_until(
        |test_loop_data: &mut TestLoopData| {
            let client = &test_loop_data.get(&handle).client;
            let head = client.chain.head().unwrap();
            let epoch_height = client
                .epoch_manager
                .get_epoch_height_from_prev_block(&head.prev_block_hash)
                .unwrap();
            // ensure loop exists because condition is met rather than timeout
            assert!(epoch_height < 3);

            let epoch_id =
                client.epoch_manager.get_epoch_id_from_prev_block(&head.last_block_hash).unwrap();
            protocol_version = client.epoch_manager.get_epoch_protocol_version(&epoch_id).unwrap();
            protocol_version
                >= ProtocolFeature::RejectBlocksWithOutdatedProtocolVersions.protocol_version()
        },
        Duration::seconds(4 * epoch_length as i64),
    );

    // produce another block with outdated protocol version
    // then check if block is rejected due to the outdated version
    let (height, latest_block) = {
        let client = &mut test_loop.data.get_mut(&handle).client;
        let height = client.chain.head().unwrap().height;
        (height, client.chain.get_block_by_height(height).unwrap())
    };

    let tx_processor = test_loop.data.get(&tx_handle);
    let _ =
        tx_processor.process_tx(create_tx(&latest_block, &accounts[0], &accounts[1]), false, false);

    let client = &mut test_loop.data.get_mut(&handle).client;
    let mut old_version_block = client.produce_block(height + 1).unwrap().unwrap();
    old_version_block.mut_header().set_latest_protocol_version(protocol_version - 1);
    let res = client.process_block_test(old_version_block.clone().into(), Provenance::NONE);
    assert!(matches!(res, Err(Error::InvalidProtocolVersion)));

    TestLoopEnv { test_loop, node_datas, shared_state }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}
