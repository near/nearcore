use itertools::Itertools;
use near_async::test_loop::data::TestLoopData;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{
    TestEpochConfigBuilder, TestGenesisBuilder, ValidatorsSpec,
};
use near_o11y::testonly::init_test_logger;
use near_primitives::epoch_manager::{EpochConfig, EpochConfigStore};
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, BlockHeight, ShardId, ShardIndex};
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::version::PROTOCOL_VERSION;
use near_vm_runner::logic::ProtocolVersion;

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Deref;
use std::sync::Arc;

use crate::setup::builder::TestLoopBuilder;
use crate::setup::drop_condition::DropCondition;
use crate::setup::env::TestLoopEnv;
use crate::utils::ONE_NEAR;

/// Test upgrading the blockchain to another protocol version.
/// Optionally make some chunks around epoch boundary missing.
/// Uses a hardcoded shard layout, it doesn't change during the test.
pub(crate) fn test_protocol_upgrade(
    old_protocol: ProtocolVersion,
    new_protocol: ProtocolVersion,
    chunk_ranges_to_drop: HashMap<ShardIndex, std::ops::Range<i64>>,
) {
    init_test_logger();

    // 2 producers, 2 validators, 1 rpc node, 4 shards, 20 accounts (account{i}) with 10k NEAR each.
    // Taken from standard_setup_1()
    let num_clients = 5;
    let num_producers = 2;
    let num_validators = 2;
    let num_rpc = 1;
    let epoch_length = 10;
    let accounts =
        (0..20).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    let initial_balance = 10000 * ONE_NEAR;
    let clients = accounts.iter().take(num_clients).cloned().collect_vec();

    // TODO - support different shard layouts, is there a way to make missing chunks generic?
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
    let [_rpc_id] = rpcs else { panic!("Expected exactly one rpc node") };

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
    let mut old_epoch_config: EpochConfig =
        mainnet_epoch_config_store.get_config(old_protocol).deref().clone();
    let mut new_epoch_config: EpochConfig =
        mainnet_epoch_config_store.get_config(new_protocol).deref().clone();

    // Adjust the epoch configs for the test
    let adjust_epoch_config = |config: &mut EpochConfig| {
        config.epoch_length = epoch_length;
        config.shard_layout = shard_layout.clone();
        config.num_block_producer_seats = genesis_epoch_info.num_block_producer_seats;
        config.num_chunk_producer_seats = genesis_epoch_info.num_chunk_producer_seats;
        config.num_chunk_validator_seats = genesis_epoch_info.num_chunk_validator_seats;

        if !chunk_ranges_to_drop.is_empty() {
            config.block_producer_kickout_threshold = 0;
            config.chunk_producer_kickout_threshold = 0;
            config.chunk_validator_only_kickout_threshold = 0;
        }
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
        .drop(DropCondition::ProtocolUpgradeChunkRange(new_protocol, chunk_ranges_to_drop.clone()))
        .warmup();

    let client_handle = node_datas[0].client_sender.actor_handle();
    let epoch_ids_with_old_protocol = RefCell::new(BTreeSet::new());
    let epoch_ids_with_new_protocol = RefCell::new(BTreeSet::new());
    let first_new_protocol_height = Cell::new(None);
    let observed_missing_chunks: RefCell<BTreeMap<ShardId, Vec<BlockHeight>>> =
        RefCell::new(BTreeMap::new());
    let last_observed_height = Cell::new(0);
    let success_condition = |test_loop_data: &mut TestLoopData| -> bool {
        let client = &test_loop_data.get(&client_handle).client;
        let tip = client.chain.head().unwrap();

        // Validate the block
        let block_header = client.chain.get_block_header(&tip.last_block_hash).unwrap();
        if last_observed_height.get() != block_header.height() {
            if last_observed_height.get() != 0 {
                // There should be no missing blocks
                assert_eq!(last_observed_height.get() + 1, block_header.height());
            }
            tracing::info!(target: "test", "Observed new block at height {}, chunk_mask: {:?}", block_header.height(), block_header.chunk_mask());
            last_observed_height.set(block_header.height());

            // Record observed missing chunks
            let shard_layout = client.epoch_manager.get_shard_layout(&tip.epoch_id).unwrap();
            for shard_info in shard_layout.shard_infos() {
                let shard_index = shard_info.shard_index();
                let shard_id = shard_info.shard_id();
                if !block_header.chunk_mask()[shard_index] {
                    observed_missing_chunks
                        .borrow_mut()
                        .entry(shard_id)
                        .or_default()
                        .push(block_header.height());
                }
            }
        }

        // Record current epoch
        let cur_epoch_info = client.epoch_manager.get_epoch_info(&tip.epoch_id).unwrap();
        if cur_epoch_info.protocol_version() == old_protocol {
            epoch_ids_with_old_protocol.borrow_mut().insert(tip.epoch_id);
        } else if cur_epoch_info.protocol_version() == new_protocol {
            epoch_ids_with_new_protocol.borrow_mut().insert(tip.epoch_id);
            if first_new_protocol_height.get().is_none() {
                first_new_protocol_height.set(Some(block_header.height()));
            }
        } else {
            panic!(
                "Unexpected protocol version: {}. old version = {}, new version = {}",
                cur_epoch_info.protocol_version(),
                old_protocol,
                new_protocol
            );
        }

        // Stop the test after observing two epochs with the old version and two epochs with the new version
        epoch_ids_with_old_protocol.borrow().len() >= 2
            && epoch_ids_with_new_protocol.borrow().len() >= 2
    };

    test_loop.run_until(success_condition, Duration::seconds((7 * epoch_length) as i64));

    // Validate that the correct chunks were missing
    let upgraded_epoch_start = first_new_protocol_height.get().unwrap();
    let mut expected_missing_chunks = BTreeMap::new();
    for (shard_index, missing_range) in &chunk_ranges_to_drop {
        let shard_id = shard_layout.get_shard_id(*shard_index).unwrap();
        let missing_heights: Vec<BlockHeight> = missing_range
            .clone()
            .map(|delta| (upgraded_epoch_start as i64 + delta).try_into().unwrap())
            .collect();
        if !missing_heights.is_empty() {
            expected_missing_chunks.insert(shard_id, missing_heights);
        }
    }
    assert_eq!(&*observed_missing_chunks.borrow(), &expected_missing_chunks);

    TestLoopEnv { test_loop, node_datas, shared_state }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}

#[test]
fn slow_test_protocol_upgrade_no_missing_chunks() {
    test_protocol_upgrade(PROTOCOL_VERSION - 1, PROTOCOL_VERSION, HashMap::new());
}

#[test]
fn slow_test_protocol_upgrade_with_missing_chunk_one() {
    test_protocol_upgrade(
        PROTOCOL_VERSION - 1,
        PROTOCOL_VERSION,
        HashMap::from_iter([(0, 0..0), (1, -1..0), (2, 0..1), (3, -1..1)].into_iter()),
    );
}

#[test]
fn slow_test_protocol_upgrade_with_missing_chunks_two() {
    test_protocol_upgrade(
        PROTOCOL_VERSION - 1,
        PROTOCOL_VERSION,
        HashMap::from_iter([(0, 0..0), (1, -2..0), (2, 0..2), (3, -2..2)].into_iter()),
    );
}

/// Test protocol upgrade to a version that isn't the latest version.
/// There was a bug which caused `test_protocol_upgrade` to always upgrade to `PROTOCOL_VERSION`,
/// this test ensures that the bug is fixed and it upgrades to the desired version, not the latest one.
#[test]
fn slow_test_protocol_upgrade_not_latest() {
    test_protocol_upgrade(PROTOCOL_VERSION - 2, PROTOCOL_VERSION - 1, HashMap::new());
}
