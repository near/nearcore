use crate::setup::builder::TestLoopBuilder;
use crate::tests::sync::util::{
    TEST_EPOCH_SYNC_HORIZON, far_horizon_height, run_until_synced, throttle_header_sync,
};
use crate::utils::account::create_account_id;
use near_async::time::Duration;
use near_chain::ChainStoreAccess;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_store::DBCol;
use near_store::adapter::StoreAdapter;
use nearcore::migrations::{AGGREGATOR_KEY, test_only_migrate_53_to_54};
use std::collections::HashMap;

/// The 53 -> 54 migration rebuilds the per-block aggregator rows that block recording writes.
///
/// Records a chain partway into its third epoch, then rebuilds the pre-54 state: no
/// `DBCol::EpochInfoAggregators` rows and a legacy `AGGREGATOR_KEY` row. The migration must
/// write the same rows block recording wrote, for the final head's epoch from its start and
/// every block above the final head, and delete the legacy row. The chain then keeps
/// advancing on the migrated rows.
#[test]
fn test_migration_53_to_54_rebuilds_epoch_info_aggregator_rows() {
    init_test_logger();
    let mut env = TestLoopBuilder::new().epoch_length(10).build();
    env.validator_runner().run_until_new_epoch();
    env.validator_runner().run_until_new_epoch();
    env.validator_runner().run_for_number_of_blocks(5);

    let store = env.validator().store();
    let recorded_rows: HashMap<Vec<u8>, Vec<u8>> = store
        .iter(DBCol::EpochInfoAggregators)
        .map(|(key, value)| (key.to_vec(), value.to_vec()))
        .collect();

    let mut store_update = store.store_update();
    store_update.delete_all(DBCol::EpochInfoAggregators);
    store_update.set_raw_bytes(DBCol::EpochInfo, AGGREGATOR_KEY, b"legacy aggregator");
    store_update.commit();

    test_only_migrate_53_to_54(
        &store,
        &env.shared_state.genesis.config,
        env.shared_state.epoch_config_store.clone(),
    )
    .unwrap();

    assert!(store.get(DBCol::EpochInfo, AGGREGATOR_KEY).is_none(), "legacy row must be deleted");
    for (key, value) in store.iter(DBCol::EpochInfoAggregators) {
        assert_eq!(
            recorded_rows.get(key.as_ref()).map(Vec::as_slice),
            Some(value.as_ref()),
            "migrated row for block {} must match the recorded row",
            CryptoHash::try_from(key.as_ref()).unwrap(),
        );
    }

    // Every block from the head back to the final head's epoch start has a row.
    let chain_store = store.chain_store();
    let final_head = chain_store.final_head().unwrap();
    let epoch_manager = env.validator().client().epoch_manager.clone();
    let epoch_start =
        *epoch_manager.get_block_info(&final_head.last_block_hash).unwrap().epoch_first_block();
    let mut block_hash = chain_store.head().unwrap().last_block_hash;
    loop {
        assert!(
            store.exists(DBCol::EpochInfoAggregators, block_hash.as_ref()),
            "block {block_hash} must have a migrated row",
        );
        if block_hash == epoch_start {
            break;
        }
        block_hash = *epoch_manager.get_block_info(&block_hash).unwrap().prev_hash();
    }

    env.validator_runner().run_until_new_epoch();
}

/// After the 53 -> 54 migration, blocks whose last final block is in the previous epoch
/// still record.
///
/// Runs the migration when the final head is the first block of an epoch, so its last
/// final block is in the previous epoch and has no row. A block recorded later computes the
/// chunk producer blacklist at its own last final block, which can be that block. The
/// blacklist is empty by rule for such a block and must not read its row.
#[test]
fn test_migration_53_to_54_blacklist_with_last_final_block_in_previous_epoch() {
    init_test_logger();
    let mut env = TestLoopBuilder::new().epoch_length(10).build();
    env.validator_runner().run_until_new_epoch();
    env.validator_runner().run_until_new_epoch();
    let store = env.validator().store();
    let epoch_manager = env.validator().client().epoch_manager.clone();
    let head_hash = env.validator().head().last_block_hash;
    let epoch_start_height = epoch_manager.get_epoch_start_height(&head_hash).unwrap();
    env.validator_runner().run_until_final_head_height(epoch_start_height);

    let chain_store = store.chain_store();
    let final_head = chain_store.final_head().unwrap();
    let final_head_info = epoch_manager.get_block_info(&final_head.last_block_hash).unwrap();
    let last_final_hash = *final_head_info.last_final_block_hash();
    let last_final_info = epoch_manager.get_block_info(&last_final_hash).unwrap();
    assert_eq!(final_head_info.epoch_first_block(), final_head_info.hash());
    assert_ne!(last_final_info.epoch_id(), final_head_info.epoch_id());

    let mut store_update = store.store_update();
    store_update.delete_all(DBCol::EpochInfoAggregators);
    store_update.commit();
    test_only_migrate_53_to_54(
        &store,
        &env.shared_state.genesis.config,
        env.shared_state.epoch_config_store.clone(),
    )
    .unwrap();

    assert!(!store.exists(DBCol::EpochInfoAggregators, last_final_hash.as_ref()));
    // A fork block with the same last final block computes the same blacklist.
    let blacklist =
        epoch_manager.get_chunk_producer_blacklist(&final_head.last_block_hash).unwrap();
    assert!(blacklist.is_empty());

    env.validator_runner().run_until_new_epoch();
}

/// After the 53 -> 54 migration, a node stopped in header sync after epoch sync catches up.
///
/// Epoch sync stores the synced epoch's first block with its `BlockInfo` and leaves the
/// final head at genesis until block sync starts. The node stops after header sync has
/// recorded a few blocks past that first block, and its rows are deleted to rebuild the
/// pre-54 state. The migration must write the first block's row and the rows of the
/// header-synced blocks after it, so header sync can record the next block on restart.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_migration_53_to_54_during_header_sync_after_epoch_sync() {
    init_test_logger();
    let epoch_length = 10;
    let mut env = TestLoopBuilder::new().validators(4, 0).epoch_length(epoch_length).build();
    env.node_runner(0).run_until_head_height(far_horizon_height(epoch_length));

    let node_state = env
        .node_state_builder()
        .account_id(&create_account_id("new_node"))
        .config_modifier(|config| {
            config.epoch_sync.epoch_sync_horizon_num_epochs = TEST_EPOCH_SYNC_HORIZON;
            // Stay in header sync until the header head is near the tip.
            config.block_header_fetch_horizon = 2;
        })
        .build();
    let new_node_idx = env.add_node("new_node", node_state);
    // Few headers per response, so the node stays in header sync long enough to stop it.
    throttle_header_sync(&mut env.test_loop, &env.shared_state, &env.node_datas[new_node_idx], 5);
    // On restart, the node resumes header sync only with its header head within the epoch
    // sync horizon of the tip. Otherwise it epoch syncs again and resets its data.
    let horizon = TEST_EPOCH_SYNC_HORIZON * epoch_length;
    let new_node = env.node_datas[new_node_idx].client_sender.actor_handle();
    let validator = env.node_datas[0].client_sender.actor_handle();
    env.test_loop.run_until(
        |data| {
            let chain = &data.get(&new_node).client.chain;
            let tip_height = data.get(&validator).client.chain.head().unwrap().height;
            chain.chain_store.store().epoch_store().get_epoch_sync_proof().unwrap().is_some()
                && chain.header_head().unwrap().height + horizon / 2 >= tip_height
        },
        Duration::seconds(20),
    );

    let node_state = env.kill_node("new_node");
    let store = node_state.storage.hot_store.clone();
    let chain_store = store.chain_store();
    assert_eq!(chain_store.final_head().unwrap().height, chain_store.get_genesis_height());
    let first_block_hash = *store
        .epoch_store()
        .get_epoch_sync_proof()
        .unwrap()
        .unwrap()
        .current_epoch
        .first_block_header_in_epoch
        .hash();
    let header_head_hash = chain_store.header_head().unwrap().last_block_hash;

    let mut store_update = store.store_update();
    store_update.delete_all(DBCol::EpochInfoAggregators);
    store_update.commit();
    test_only_migrate_53_to_54(
        &store,
        &env.shared_state.genesis.config,
        env.shared_state.epoch_config_store.clone(),
    )
    .unwrap();
    assert!(store.exists(DBCol::EpochInfoAggregators, first_block_hash.as_ref()));
    assert!(store.exists(DBCol::EpochInfoAggregators, header_head_hash.as_ref()));

    let restarted_idx = env.restart_node("new_node-restart", node_state);
    run_until_synced(&mut env.test_loop, &env.node_datas, restarted_idx, 0);
    env.node_runner(restarted_idx).run_for_number_of_blocks(2 * epoch_length as usize);
}
