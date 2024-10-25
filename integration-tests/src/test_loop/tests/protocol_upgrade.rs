use aurora_engine_types::BTreeSet;
use itertools::Itertools;
use near_async::test_loop::data::TestLoopData;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_o11y::testonly::init_test_logger;
use near_primitives::epoch_manager::{EpochConfig, EpochConfigStore};
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, BlockHeight, ShardId};
use near_primitives::version::PROTOCOL_VERSION;
use near_store::ShardUId;
use near_vm_runner::logic::ProtocolVersion;

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap};
use std::ops::Deref;
use std::sync::Arc;

use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::TestLoopEnv;
use crate::test_loop::utils::ONE_NEAR;

/// The test works only with this shard layout:
/// shard_ids: [0, 1, 2, 3]
/// version: 1
fn assert_shard_layout(shard_layout: &ShardLayout) {
    let version = 1;
    let expected_uids: Vec<ShardUId> =
        (0..4).map(|id| ShardUId { version, shard_id: id }).collect();
    let layout_uids: Vec<ShardUId> = shard_layout.shard_uids().collect();
    assert_eq!(expected_uids, layout_uids);
}

/// Test upgrading the blockchain to another protocol version.
/// Optionally make some chunks around epoch boundary missing.
/// Uses a hardcoded shard layout, it doesn't change during the test.
pub(crate) fn test_protocol_upgrade(
    old_protocol: ProtocolVersion,
    new_protocol: ProtocolVersion,
    missing_chunk_ranges: HashMap<ShardId, std::ops::Range<i64>>,
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
    let shard_layout = ShardLayout::v1(
        ["account3", "account5", "account7"].iter().map(|a| a.parse().unwrap()).collect(),
        None,
        1,
    );

    // split the clients into producers, validators, and rpc nodes
    let tmp = clients.clone();
    let (producers, tmp) = tmp.split_at(num_producers);
    let (validators, tmp) = tmp.split_at(num_validators);
    let (rpcs, tmp) = tmp.split_at(num_rpc);
    assert!(tmp.is_empty());

    let producers = producers.iter().map(|account| account.as_str()).collect_vec();
    let validators = validators.iter().map(|account| account.as_str()).collect_vec();
    let [_rpc_id] = rpcs else { panic!("Expected exactly one rpc node") };

    let builder = TestLoopBuilder::new();
    let mut genesis_builder = TestGenesisBuilder::new();
    genesis_builder
        .protocol_version(old_protocol)
        .genesis_time_from_clock(&builder.clock())
        .genesis_height(10000)
        .shard_layout(shard_layout.clone())
        .epoch_length(epoch_length)
        .validators_desired_roles(&producers, &validators);
    for account in accounts {
        genesis_builder.add_user_account_simple(account.clone(), initial_balance);
    }
    let (genesis, genesis_epoch_config_store) = genesis_builder.build();
    let genesis_epoch_info = genesis_epoch_config_store.get_config(old_protocol);

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
        config.validator_selection_config.num_chunk_producer_seats =
            genesis_epoch_info.validator_selection_config.num_chunk_producer_seats;
        config.validator_selection_config.num_chunk_validator_seats =
            genesis_epoch_info.validator_selection_config.num_chunk_validator_seats;

        if !missing_chunk_ranges.is_empty() {
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

    // Translate shard ids to shard uids
    let chunk_ranges_to_drop: HashMap<ShardUId, std::ops::Range<i64>> = missing_chunk_ranges
        .iter()
        .map(|(shard_id, range)| {
            let shard_uid = ShardUId { version: 1, shard_id: shard_id.get().try_into().unwrap() };
            (shard_uid, range.clone())
        })
        .collect();

    let TestLoopEnv { mut test_loop, datas: node_datas, tempdir } = builder
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .drop_protocol_upgrade_chunks(new_protocol, chunk_ranges_to_drop)
        .clients(clients)
        .build();

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

            let shard_layout = client.epoch_manager.get_shard_layout(&tip.epoch_id).unwrap();
            assert_shard_layout(&shard_layout);

            // Record observed missing chunks
            for (shard_index, shard_id) in shard_layout.shard_ids().enumerate() {
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
    for (shard_id, missing_range) in &missing_chunk_ranges {
        let missing_heights: Vec<BlockHeight> = missing_range
            .clone()
            .map(|delta| (upgraded_epoch_start as i64 + delta).try_into().unwrap())
            .collect();
        if !missing_heights.is_empty() {
            expected_missing_chunks.insert(*shard_id, missing_heights);
        }
    }
    assert_eq!(&*observed_missing_chunks.borrow(), &expected_missing_chunks);

    TestLoopEnv { test_loop, datas: node_datas, tempdir }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}

#[test]
fn test_protocol_upgrade_no_missing_chunks() {
    test_protocol_upgrade(PROTOCOL_VERSION - 1, PROTOCOL_VERSION, HashMap::new());
}

#[test]
fn test_protocol_upgrade_with_missing_chunk_one() {
    test_protocol_upgrade(
        PROTOCOL_VERSION - 1,
        PROTOCOL_VERSION,
        HashMap::from_iter(
            [
                (ShardId::new(0), 0..0),
                (ShardId::new(1), -1..0),
                (ShardId::new(2), 0..1),
                (ShardId::new(3), -1..1),
            ]
            .into_iter(),
        ),
    );
}

#[test]
fn test_protocol_upgrade_with_missing_chunks_two() {
    test_protocol_upgrade(
        PROTOCOL_VERSION - 1,
        PROTOCOL_VERSION,
        HashMap::from_iter(
            [
                (ShardId::new(0), 0..0),
                (ShardId::new(1), -2..0),
                (ShardId::new(2), 0..2),
                (ShardId::new(3), -2..2),
            ]
            .into_iter(),
        ),
    );
}
