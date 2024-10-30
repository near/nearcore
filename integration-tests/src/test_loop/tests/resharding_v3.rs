use borsh::BorshDeserialize;
use itertools::Itertools;
use near_async::test_loop::data::TestLoopData;
use near_async::time::Duration;
use near_chain::ChainStoreAccess;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_client::Client;
use near_o11y::testonly::init_test_logger;
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{account_id_to_shard_uid, ShardLayout};
use near_primitives::state_record::StateRecord;
use near_primitives::types::{AccountId, ShardId};
use near_primitives::version::{ProtocolFeature, PROTOCOL_VERSION};
use near_store::adapter::StoreAdapter;
use near_store::db::refcount::decode_value_with_rc;
use near_store::{DBCol, ShardUId};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::TestLoopEnv;
use crate::test_loop::utils::ONE_NEAR;

fn create_new_shard_layout(base_shard_layout: &ShardLayout, boundary_account: &str) -> ShardLayout {
    let mut boundary_accounts = base_shard_layout.boundary_accounts().clone();
    let mut shard_ids: Vec<_> = base_shard_layout.shard_ids().collect();
    let max_shard_id = *shard_ids.iter().max().unwrap();
    let last_shard_id = shard_ids.pop().unwrap();
    let mut shards_split_map: BTreeMap<ShardId, Vec<ShardId>> =
        shard_ids.iter().map(|shard_id| (*shard_id, vec![*shard_id])).collect();
    let new_shards = vec![max_shard_id + 1, max_shard_id + 2];
    shard_ids.extend(new_shards.clone());
    shards_split_map.insert(last_shard_id, new_shards);
    boundary_accounts.push(boundary_account.parse().unwrap());
    ShardLayout::v2(boundary_accounts, shard_ids.clone(), Some(shards_split_map))
}

fn print_and_assert_shard_accounts(client: &Client) {
    let tip = client.chain.head().unwrap();
    let epoch_id = tip.epoch_id;
    let epoch_config = client.epoch_manager.get_epoch_config(&epoch_id).unwrap();
    for shard_uid in epoch_config.shard_layout.shard_uids() {
        let chunk_extra = client.chain.get_chunk_extra(&tip.prev_block_hash, &shard_uid).unwrap();
        let trie = client
            .runtime_adapter
            .get_trie_for_shard(
                shard_uid.shard_id(),
                &tip.prev_block_hash,
                *chunk_extra.state_root(),
                false,
            )
            .unwrap();
        let mut shard_accounts = vec![];
        for item in trie.lock_for_iter().iter().unwrap() {
            let (key, value) = item.unwrap();
            let state_record = StateRecord::from_raw_key_value(key, value);
            if let Some(StateRecord::Account { account_id, .. }) = state_record {
                shard_accounts.push(account_id.to_string());
            }
        }
        println!("accounts for shard {}: {:?}", shard_uid, shard_accounts);
        assert!(!shard_accounts.is_empty());
    }
}

/// Asserts that all parent shard State is accessible via parent and children shards.
fn check_state_shard_uid_mapping_after_resharding(client: &Client, parent_shard_uid: ShardUId) {
    let tip = client.chain.head().unwrap();
    let epoch_id = tip.epoch_id;
    let epoch_config = client.epoch_manager.get_epoch_config(&epoch_id).unwrap();
    let children_shard_uids =
        epoch_config.shard_layout.get_children_shards_uids(parent_shard_uid.shard_id()).unwrap();
    assert_eq!(children_shard_uids.len(), 2);

    let store = client.chain.chain_store.store().trie_store();
    for kv in store.store().iter_raw_bytes(DBCol::State) {
        let (key, value) = kv.unwrap();
        let shard_uid = ShardUId::try_from_slice(&key[0..8]).unwrap();
        // Just after resharding, no State data must be keyed using children ShardUIds.
        assert!(!children_shard_uids.contains(&shard_uid));
        if shard_uid != parent_shard_uid {
            continue;
        }
        let node_hash = CryptoHash::try_from_slice(&key[8..]).unwrap();
        let (value, _) = decode_value_with_rc(&value);
        let parent_value = store.get(parent_shard_uid, &node_hash);
        // Parent shard data must still be accessible using parent ShardUId.
        assert_eq!(&parent_value.unwrap()[..], value.unwrap());
        // All parent shard data is available via both children shards.
        for child_shard_uid in &children_shard_uids {
            let child_value = store.get(*child_shard_uid, &node_hash);
            assert_eq!(&child_value.unwrap()[..], value.unwrap());
        }
    }
}

/// Base setup to check sanity of Resharding V3.
/// TODO(#11881): add the following scenarios:
/// - Nodes must not track all shards. State sync must succeed.
/// - Set up chunk validator-only nodes. State witness must pass validation.
/// - Consistent tx load. All txs must succeed.
/// - Delayed receipts, congestion control computation.
/// - Cross-shard receipts of all kinds, crossing resharding boundary.
/// - Shard layout v2 -> v2 transition.
/// - Shard layout can be taken from mainnet.
fn test_resharding_v3_base(chunk_ranges_to_drop: HashMap<ShardUId, std::ops::Range<i64>>) {
    if !ProtocolFeature::SimpleNightshadeV4.enabled(PROTOCOL_VERSION) {
        return;
    }

    init_test_logger();
    let builder = TestLoopBuilder::new();

    let initial_balance = 1_000_000 * ONE_NEAR;
    let epoch_length = 6;
    let accounts =
        (0..8).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    // #12195 prevents number of BPs bigger than `epoch_length`.
    let clients = vec![accounts[0].clone(), accounts[3].clone(), accounts[6].clone()];
    let block_and_chunk_producers =
        clients.iter().map(|account: &AccountId| account.as_str()).collect_vec();

    // Prepare shard split configuration.
    let base_epoch_config_store = EpochConfigStore::for_chain_id("mainnet", None).unwrap();
    let base_protocol_version = ProtocolFeature::SimpleNightshadeV4.protocol_version() - 1;
    let mut base_epoch_config =
        base_epoch_config_store.get_config(base_protocol_version).as_ref().clone();
    base_epoch_config.validator_selection_config.shuffle_shard_assignment_for_chunk_producers =
        false;
    if !chunk_ranges_to_drop.is_empty() {
        base_epoch_config.block_producer_kickout_threshold = 0;
        base_epoch_config.chunk_producer_kickout_threshold = 0;
        base_epoch_config.chunk_validator_only_kickout_threshold = 0;
    }
    base_epoch_config.shard_layout = ShardLayout::v1(vec!["account3".parse().unwrap()], None, 3);
    let base_shard_layout = base_epoch_config.shard_layout.clone();
    let mut epoch_config = base_epoch_config.clone();
    let boundary_account = "account6";
    epoch_config.shard_layout = create_new_shard_layout(&base_shard_layout, boundary_account);
    let expected_num_shards = epoch_config.shard_layout.shard_ids().count();
    let epoch_config_store = EpochConfigStore::test(BTreeMap::from_iter(vec![
        (base_protocol_version, Arc::new(base_epoch_config)),
        (base_protocol_version + 1, Arc::new(epoch_config)),
    ]));

    let mut genesis_builder = TestGenesisBuilder::new();
    genesis_builder
        .genesis_time_from_clock(&builder.clock())
        .shard_layout(base_shard_layout.clone())
        .protocol_version(base_protocol_version)
        .epoch_length(epoch_length)
        .validators_desired_roles(&block_and_chunk_producers, &[]);
    for account in &accounts {
        genesis_builder.add_user_account_simple(account.clone(), initial_balance);
    }
    let (genesis, _) = genesis_builder.build();

    let TestLoopEnv { mut test_loop, datas: node_datas, tempdir } = builder
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .drop_protocol_upgrade_chunks(base_protocol_version + 1, chunk_ranges_to_drop.clone())
        .track_all_shards()
        .build();

    let boundary_account_id: AccountId = boundary_account.parse().unwrap();
    let parent_shard_uid = account_id_to_shard_uid(&boundary_account_id, &base_shard_layout);
    let client_handle = node_datas[0].client_sender.actor_handle();
    let latest_block_height = std::cell::Cell::new(0u64);
    let success_condition = |test_loop_data: &mut TestLoopData| -> bool {
        let client = &test_loop_data.get(&client_handle).client;
        let tip = client.chain.head().unwrap();

        // Check that all chunks are included.
        let block_header = client.chain.get_block_header(&tip.last_block_hash).unwrap();
        if latest_block_height.get() < tip.height {
            if latest_block_height.get() == 0 {
                println!("State before resharding:");
                print_and_assert_shard_accounts(client);
            }
            latest_block_height.set(tip.height);
            println!("block: {} chunks: {:?}", tip.height, block_header.chunk_mask());
            if chunk_ranges_to_drop.is_empty() {
                assert!(block_header.chunk_mask().iter().all(|chunk_bit| *chunk_bit));
            }
        }

        // Return true if we passed an epoch with increased number of shards.
        let epoch_height =
            client.epoch_manager.get_epoch_height_from_prev_block(&tip.prev_block_hash).unwrap();
        assert!(epoch_height < 6);
        let prev_epoch_id =
            client.epoch_manager.get_prev_epoch_id_from_prev_block(&tip.prev_block_hash).unwrap();
        let epoch_config = client.epoch_manager.get_epoch_config(&prev_epoch_id).unwrap();
        if epoch_config.shard_layout.shard_ids().count() != expected_num_shards {
            return false;
        }

        println!("State after resharding:");
        print_and_assert_shard_accounts(client);
        check_state_shard_uid_mapping_after_resharding(&client, parent_shard_uid);
        return true;
    };

    test_loop.run_until(
        success_condition,
        // Give enough time to produce ~7 epochs.
        Duration::seconds((7 * epoch_length) as i64),
    );

    TestLoopEnv { test_loop, datas: node_datas, tempdir }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}

#[test]
fn test_resharding_v3() {
    test_resharding_v3_base(HashMap::new());
}

#[test]
fn test_resharding_v3_drop_chunks_before() {
    let chunk_ranges_to_drop = HashMap::from([(ShardUId { shard_id: 1, version: 3 }, -2..0)]);
    test_resharding_v3_base(chunk_ranges_to_drop);
}

#[test]
fn test_resharding_v3_drop_chunks_after() {
    let chunk_ranges_to_drop = HashMap::from([(ShardUId { shard_id: 2, version: 3 }, 0..2)]);
    test_resharding_v3_base(chunk_ranges_to_drop);
}

#[test]
fn test_resharding_v3_drop_chunks_before_and_after() {
    let chunk_ranges_to_drop = HashMap::from([(ShardUId { shard_id: 0, version: 3 }, -2..2)]);
    test_resharding_v3_base(chunk_ranges_to_drop);
}

#[test]
fn test_resharding_v3_drop_chunks_all() {
    let chunk_ranges_to_drop = HashMap::from([
        (ShardUId { shard_id: 0, version: 3 }, -1..2),
        (ShardUId { shard_id: 1, version: 3 }, -3..0),
        (ShardUId { shard_id: 2, version: 3 }, 0..3),
        (ShardUId { shard_id: 3, version: 3 }, 0..1),
    ]);
    test_resharding_v3_base(chunk_ranges_to_drop);
}
