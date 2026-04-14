use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::{create_validators_spec, validators_spec_clients};
use crate::utils::setups::derive_new_epoch_config_from_boundary;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestEpochConfigBuilder;
use near_o11y::testonly::init_test_logger;
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{ShardLayout, get_block_shard_uid};
use near_primitives::types::{AccountId, Balance};
use near_primitives::version::PROTOCOL_VERSION;
use near_store::adapter::StoreAdapter;
use near_store::adapter::trie_store::get_shard_uid_mapping;
use near_store::archive::cold_storage::join_two_keys;
use near_store::db::Database;
use near_store::{DBCol, ShardUId, TrieChanges};
use std::collections::BTreeMap;
use std::sync::Arc;

const EPOCH_LENGTH: u64 = 6;
const GC_NUM_EPOCHS_TO_KEEP: u64 = 3;

/// Verifies that resharding trie nodes are copied to cold storage.
///
/// During resharding, `retain_split_shard` creates intermediate trie nodes stored
/// under the parent shard's prefix. Without persisting TrieChanges for the child
/// shards, the cold store copy loop has no way to discover these nodes.
///
/// Eventually `gc_parent_shard_after_resharding` range-deletes the parent prefix
/// from hot store, and the nodes are lost from hot. Cold store uses a permanent
/// `shard_uid_mapping` (child -> parent prefix), so if the nodes were never
/// copied there, historical queries eventually fail.
///
/// This test:
/// 1. Runs resharding and captures the child shard TrieChanges (before GC).
/// 2. Waits for the cold store loop to process past the resharding boundary.
/// 3. Verifies the cold store's State column contains all resharding trie nodes.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_resharding_trie_nodes_copied_to_cold_store() {
    init_test_logger();

    // --- 1. Configure shard layouts and epoch configs for resharding ---
    let version = 3;
    let base_shard_layout = ShardLayout::multi_shard(3, version);
    let boundary_account: AccountId = "boundary".parse().unwrap();

    let validators_spec = create_validators_spec(1, 0);
    let mut clients = validators_spec_clients(&validators_spec);
    let archival_id: AccountId = "archival".parse().unwrap();
    clients.push(archival_id.clone());

    let user_account: AccountId = "user0".parse().unwrap();

    let genesis = TestLoopBuilder::new_genesis_builder()
        .protocol_version(PROTOCOL_VERSION - 1)
        .validators_spec(validators_spec)
        .shard_layout(base_shard_layout.clone())
        .epoch_length(EPOCH_LENGTH)
        .add_user_accounts_simple(&[user_account], Balance::from_near(1_000_000))
        .build();

    let base_epoch_config = TestEpochConfigBuilder::from_genesis(&genesis).build();
    let (new_epoch_config, new_shard_layout) =
        derive_new_epoch_config_from_boundary(&base_epoch_config, &boundary_account);

    let epoch_configs = vec![
        (genesis.config.protocol_version, Arc::new(base_epoch_config)),
        (genesis.config.protocol_version + 1, Arc::new(new_epoch_config)),
    ];
    let epoch_config_store = EpochConfigStore::test(BTreeMap::from_iter(epoch_configs));

    // --- 2. Build the test environment with cold storage archival node ---
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .clients(clients)
        .epoch_config_store(epoch_config_store)
        .cold_storage_archival_clients(vec![archival_id])
        .gc_num_epochs_to_keep(GC_NUM_EPOCHS_TO_KEEP)
        .build();

    // --- 3. Wait for resharding to complete ---
    let archival_handle = env.node_datas.last().unwrap().client_sender.actor_handle();
    let epoch_manager = env.test_loop.data.get(&archival_handle).client.epoch_manager.clone();

    env.test_loop.run_until(
        |test_loop_data| {
            let head = &test_loop_data.get(&archival_handle).client.chain.head().unwrap();
            epoch_manager.get_shard_layout(&head.epoch_id).unwrap() == new_shard_layout
        },
        Duration::seconds((4 * EPOCH_LENGTH) as i64),
    );

    // --- 4. Find resharding block and capture TrieChanges BEFORE GC cleans them up ---
    let client = &env.test_loop.data.get(&archival_handle).client;
    let head = client.chain.head().unwrap();

    let mut block_hash = head.last_block_hash;
    let resharding_block_hash = loop {
        let header = client.chain.get_block_header(&block_hash).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(header.epoch_id()).unwrap();
        if shard_layout == base_shard_layout {
            break *header.hash();
        }
        block_hash = *header.prev_hash();
    };

    let old_shard_uids: std::collections::HashSet<ShardUId> =
        base_shard_layout.shard_uids().collect();
    let child_shard_uids: Vec<ShardUId> =
        new_shard_layout.shard_uids().filter(|uid| !old_shard_uids.contains(uid)).collect();
    assert!(!child_shard_uids.is_empty());

    // Read TrieChanges now, before GC removes them from hot store.
    let hot_store = client.chain.chain_store.store().store();
    let mut all_insertions: Vec<(ShardUId, CryptoHash)> = vec![];
    for child_shard_uid in &child_shard_uids {
        let key = get_block_shard_uid(&resharding_block_hash, child_shard_uid);
        let trie_changes: Option<TrieChanges> = hot_store.get_ser(DBCol::TrieChanges, &key);
        let trie_changes = trie_changes.unwrap();
        assert!(trie_changes.deletions().is_empty(),);
        for op in trie_changes.insertions() {
            all_insertions.push((*child_shard_uid, *op.hash()));
        }
    }
    assert!(!all_insertions.is_empty());

    // --- 5. Wait for cold store loop to process past the resharding boundary ---
    let post_resharding_height = head.height;
    let target_height = post_resharding_height + EPOCH_LENGTH * (GC_NUM_EPOCHS_TO_KEEP + 2);
    env.test_loop.run_until(
        |test_loop_data| {
            let head = &test_loop_data.get(&archival_handle).client.chain.head().unwrap();
            head.height >= target_height
        },
        Duration::seconds((target_height + 10) as i64),
    );

    // --- 6. Verify cold store has the resharding trie nodes ---
    let cold_store_sender = env.node_datas.last().unwrap().cold_store_sender.as_ref().unwrap();
    let cold_store_actor = env.test_loop.data.get(&cold_store_sender.actor_handle());
    let cold_db = cold_store_actor.get_cold_db();
    let cold_store = cold_db.as_store();

    for (child_shard_uid, node_hash) in &all_insertions {
        // Cold store uses permanent shard_uid_mapping (child → parent prefix).
        let mapped_shard_uid = get_shard_uid_mapping(&cold_store, *child_shard_uid);
        let state_key = join_two_keys(&mapped_shard_uid.to_bytes(), node_hash.as_bytes());
        let value = cold_db.get_raw_bytes(DBCol::State, &state_key);
        assert!(value.is_some());
    }
}
