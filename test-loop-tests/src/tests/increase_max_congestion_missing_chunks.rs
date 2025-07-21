use assert_matches::assert_matches;
use itertools::Itertools;
use near_async::test_loop::data::TestLoopData;
use near_async::time::Duration;
use near_chain::BlockHeader;
use near_chain_configs::test_genesis::{
    TestEpochConfigBuilder, TestGenesisBuilder, ValidatorsSpec,
};
use near_o11y::testonly::init_test_logger;
use near_primitives::epoch_manager::{EpochConfig, EpochConfigStore};
use near_primitives::errors::InvalidTxError;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockHeightDelta, ShardIndex};
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::version::ProtocolFeature;
use std::cell::Cell;
use std::collections::{BTreeMap, HashMap};
use std::ops::Deref;
use std::sync::Arc;

use crate::setup::builder::TestLoopBuilder;
use crate::setup::drop_condition::DropCondition;
use crate::setup::env::TestLoopEnv;
use crate::utils::ONE_NEAR;
use crate::utils::transactions::{TransactionRunner, execute_tx, get_shared_block_hash};

/// Check that when some shard misses 5 chunks in a row, we reject txs where
/// receiver belongs to that shard due to congestion control before protocol
/// upgrade. But after protocol upgrade we accept them, even if it misses 10
/// chunks in a row.
/// The real threshold for missing chunks is 100, but it would make test very
/// slow.
#[test]
fn slow_test_tx_inclusion() {
    init_test_logger();

    let new_protocol = ProtocolFeature::IncreaseMaxCongestionMissedChunks.protocol_version();
    let old_protocol = new_protocol - 1;
    // Enough to have correct epoch with 15 missing chunks.
    let epoch_length: BlockHeightDelta = 20;
    let old_chunk_range_to_drop: HashMap<ShardIndex, std::ops::Range<i64>> =
        HashMap::from_iter([(1, -(epoch_length as i64)..-(epoch_length as i64) + 15)].into_iter());
    let new_chunk_range_to_drop: HashMap<ShardIndex, std::ops::Range<i64>> =
        HashMap::from_iter([(1, 0..15)].into_iter());

    // 2 producers, 2 validators, 1 rpc node, 4 shards, 20 accounts (account{i}) with 10k NEAR each.
    // Taken from standard_setup_1()
    let num_clients = 5;
    let num_producers = 2;
    let num_validators = 2;
    let num_rpc = 1;
    let accounts =
        (0..20).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    let initial_balance = 10000 * ONE_NEAR;
    let clients = accounts.iter().take(num_clients).cloned().collect_vec();

    let boundary_accounts = ["account3", "account5", "account7"];
    let boundary_accounts = boundary_accounts.iter().map(|a| a.parse().unwrap()).collect();
    let shard_layout = ShardLayout::multi_shard_custom(boundary_accounts, 1);

    // split the clients into producers, validators, and rpc nodes
    let tmp = clients.clone();
    let (producers, tmp) = tmp.split_at(num_producers);
    let (validators, tmp) = tmp.split_at(num_validators);
    let (rpcs, tmp) = tmp.split_at(num_rpc);
    assert!(tmp.is_empty());

    let producers = producers.iter().map(|account| account.as_str()).collect_vec();
    let validators = validators.iter().map(|account| account.as_str()).collect_vec();
    let validators_spec = ValidatorsSpec::desired_roles(&producers, &validators);
    let [rpc_id] = rpcs else { panic!("Expected exactly one rpc node") };

    let builder = TestLoopBuilder::new();
    let genesis = TestGenesisBuilder::new()
        .protocol_version(old_protocol)
        .genesis_time_from_clock(&builder.clock())
        .genesis_height(10000)
        .shard_layout(shard_layout.clone())
        .epoch_length(epoch_length)
        .validators_spec(validators_spec.clone())
        .add_user_accounts_simple(&accounts, initial_balance)
        .build();
    let genesis_epoch_info = TestEpochConfigBuilder::new()
        .epoch_length(epoch_length)
        .shard_layout(shard_layout.clone())
        .validators_spec(validators_spec)
        .build();

    let mainnet_epoch_config_store = EpochConfigStore::for_chain_id("mainnet", None).unwrap();
    let mut old_epoch_config = mainnet_epoch_config_store.get_config(old_protocol).deref().clone();
    let mut new_epoch_config = mainnet_epoch_config_store.get_config(new_protocol).deref().clone();

    // Adjust the epoch configs for the test
    let adjust_epoch_config = |config: &mut EpochConfig| {
        config.epoch_length = epoch_length;
        config.shard_layout = shard_layout.clone();
        config.num_block_producer_seats = genesis_epoch_info.num_block_producer_seats;
        config.num_chunk_producer_seats = genesis_epoch_info.num_chunk_producer_seats;
        config.num_chunk_validator_seats = genesis_epoch_info.num_chunk_validator_seats;
        config.block_producer_kickout_threshold = 0;
        config.chunk_producer_kickout_threshold = 0;
        config.chunk_validator_only_kickout_threshold = 0;
    };
    adjust_epoch_config(&mut old_epoch_config);
    adjust_epoch_config(&mut new_epoch_config);

    let epoch_config_store = EpochConfigStore::test(BTreeMap::from_iter(vec![
        (old_protocol, Arc::new(old_epoch_config)),
        (new_protocol, Arc::new(new_epoch_config)),
    ]));

    // Immediately start voting for the new protocol version
    let protocol_upgrade_schedule = ProtocolUpgradeVotingSchedule::new_immediate(new_protocol);

    let TestLoopEnv { mut test_loop, node_datas, shared_state } = builder
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .protocol_upgrade_schedule(protocol_upgrade_schedule)
        .clients(clients)
        .build()
        .drop(DropCondition::ProtocolUpgradeChunkRange(new_protocol, old_chunk_range_to_drop))
        .drop(DropCondition::ProtocolUpgradeChunkRange(new_protocol, new_chunk_range_to_drop))
        .warmup();

    let client_handle = node_datas[0].client_sender.actor_handle();
    let last_observed_height = Cell::new(0);

    let is_new_height = |block_header: &BlockHeader| -> bool {
        if last_observed_height.get() == block_header.height() {
            return false;
        }

        // There should be no missing blocks
        if last_observed_height.get() != 0 {
            assert_eq!(last_observed_height.get() + 1, block_header.height());
        }
        tracing::info!(target: "test", "Observed new block at height {}, chunk_mask: {:?}", block_header.height(), block_header.chunk_mask());
        last_observed_height.set(block_header.height());
        true
    };

    // Wait for the last epoch with old protocol version, until we see 5
    // blocks with missing chunks in it.
    let last_epoch_old_version_first_height = Cell::new(0);
    let success_condition = |test_loop_data: &mut TestLoopData| -> bool {
        let client = &test_loop_data.get(&client_handle).client;
        let tip = client.chain.head().unwrap();

        // Validate the block
        let block_header = client.chain.get_block_header(&tip.last_block_hash).unwrap();
        if !is_new_height(&block_header) {
            return false;
        }

        // Stop if the next epoch is the one with the new protocol version
        let next_epoch_info = client.epoch_manager.get_epoch_info(&tip.next_epoch_id).unwrap();
        if next_epoch_info.protocol_version() != new_protocol {
            return false;
        }

        if last_epoch_old_version_first_height.get() == 0 {
            last_epoch_old_version_first_height.set(block_header.height());
        }

        block_header.height() >= last_epoch_old_version_first_height.get() + 5
    };

    test_loop.run_until(success_condition, Duration::seconds((4 * epoch_length) as i64));

    // Check that tx is rejected with ShardStuck error.
    let account0: AccountId = "account0".parse().unwrap();
    let account4: AccountId = "account4".parse().unwrap();
    let account0_signer = &create_user_test_signer(&account0).into();
    let tx = SignedTransaction::send_money(
        100,
        account0.clone(),
        account4.clone(),
        &account0_signer,
        1000 * ONE_NEAR,
        get_shared_block_hash(&node_datas, &test_loop.data),
    );
    let tx_exec_res = execute_tx(
        &mut test_loop,
        &rpc_id,
        TransactionRunner::new(tx, false),
        &node_datas,
        Duration::seconds(20),
    );
    assert_matches!(tx_exec_res, Err(InvalidTxError::ShardStuck { .. }));

    // Wait for the first epoch with new protocol version, until we see 10
    // blocks with missing chunks in it.
    let new_version_first_height = Cell::new(0);
    let success_condition = |test_loop_data: &mut TestLoopData| -> bool {
        let client = &test_loop_data.get(&client_handle).client;
        let tip = client.chain.head().unwrap();

        // Validate the block
        let block_header = client.chain.get_block_header(&tip.last_block_hash).unwrap();
        if !is_new_height(&block_header) {
            return false;
        }

        // Stop if the next epoch is the one with the new protocol version
        let epoch_info = client.epoch_manager.get_epoch_info(&tip.epoch_id).unwrap();
        if epoch_info.protocol_version() != new_protocol {
            return false;
        }

        if new_version_first_height.get() == 0 {
            new_version_first_height.set(block_header.height());
        }

        block_header.height() >= new_version_first_height.get() + 10
    };

    test_loop.run_until(success_condition, Duration::seconds((2 * epoch_length) as i64));

    // Check that tx is accepted with new protocol version.
    let tx = SignedTransaction::send_money(
        200,
        account0,
        account4,
        &account0_signer,
        1000 * ONE_NEAR,
        get_shared_block_hash(&node_datas, &test_loop.data),
    );
    let tx_exec_res = execute_tx(
        &mut test_loop,
        &rpc_id,
        TransactionRunner::new(tx, false),
        &node_datas,
        Duration::seconds(20),
    );
    assert_matches!(tx_exec_res, Ok(_));

    TestLoopEnv { test_loop, node_datas, shared_state }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}
