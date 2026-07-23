//! Tests for `sync_checkpoint: "earliest_available"` / `get_earliest_block_hash`
//! resolution: it must return a block whose state the node can actually serve.
//!
//! `get_earliest_block_hash` anchors on `max(gc_stop_height, tail)`. Two regimes
//! break the naive single-marker choices:
//!
//! * Resharding (`slow_test_reshard_earliest_available_after_gc`): the GC `tail`
//!   comes to rest on the resharding boundary block `H_r`, whose split-parent shard
//!   state has already been wiped, so resolving `account -> shard` via `H_r`'s
//!   pre-split layout hit a missing shard and failed with `MissingTrieValue`.
//!   Anchoring on `gc_stop_height` (= `H_r + 1`, the first child-epoch block) avoids
//!   it.
//! * State sync (`slow_test_earliest_available_servable_after_state_sync`): a
//!   freshly-synced node has `tail` (its sync point) *above* `gc_stop_height`, which
//!   is computed from `head_epoch - keep` and can point at epochs the node never
//!   downloaded. Flooring at `tail` keeps the result servable.

use super::sync::util::{
    TEST_EPOCH_SYNC_HORIZON, far_horizon_height, run_until_synced, track_sync_status,
};
use crate::setup::builder::{MIN_BLOCK_PROD_TIME, TestLoopBuilder};
use crate::utils::account::create_account_id;
use crate::utils::setups::derive_new_epoch_config_from_boundary;
use crate::utils::transactions::{execute_money_transfers, make_accounts};
use near_async::messaging::Handler;
use near_async::time::Duration;
use near_chain_configs::TrackedShardsConfig;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::{GetBlock, Query, QueryError};
use near_o11y::testonly::init_test_logger;
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::types::{
    AccountId, Balance, BlockId, BlockReference, EpochId, Finality, SyncCheckpoint,
};
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::QueryRequest;
use std::collections::BTreeMap;
use std::slice;
use std::sync::Arc;

// Resharding test: keep GC stepping at least as often as blocks are produced so the
// tail promptly reaches `H_r`.
const GC_STEP_PERIOD: Duration = Duration::milliseconds(MIN_BLOCK_PROD_TIME as i64);

// State-sync test parameters.
const EPOCH_LENGTH: u64 = 10;
const GC_NUM_EPOCHS_TO_KEEP: u64 = 3;

// The GC tail comes to rest on a resharding boundary block whose split-parent shard
// state has been wiped; `earliest_available` must skip it and resolve to the first
// block the node still serves state for (the child-layout block), not fail with
// `MissingTrieValue`.
#[test]
// Spice uses a separate view/query path; resharding under spice is not covered here.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_reshard_earliest_available_after_gc() {
    init_test_logger();

    let epoch_length = 5;
    let gc_num_epochs_to_keep = 3;
    let base_shard_layout = ShardLayout::multi_shard(3, 3);

    // `multi_shard(3, 3)` splits at boundary accounts ["test1", "test2"], so the last shard
    // covers `[test2, +inf)`. `zzz` lives there and gets state in genesis; `test3` is the
    // resharding boundary that splits that same last shard. At a pre-split block `zzz` therefore
    // resolves to the parent (last) shard; post-split it resolves to the new right child.
    let query_account: AccountId = "zzz".parse().unwrap();
    let boundary_account: AccountId = "test3".parse().unwrap();

    let chunk_producer = "cp0";
    let validators_spec = ValidatorsSpec::desired_roles(&[chunk_producer], &[]);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .protocol_version(PROTOCOL_VERSION - 1)
        .validators_spec(validators_spec)
        .shard_layout(base_shard_layout.clone())
        .add_user_accounts_simple(slice::from_ref(&query_account), Balance::from_near(1_000_000))
        .epoch_length(epoch_length)
        .build();

    let base_epoch_config = TestEpochConfigBuilder::from_genesis(&genesis).build();
    let (new_epoch_config, new_shard_layout) =
        derive_new_epoch_config_from_boundary(&base_epoch_config, &boundary_account);

    // Sanity: the query account really is in the shard that splits.
    let parent_shard_id = base_shard_layout.account_id_to_shard_id(&query_account);
    let child_shard_id = new_shard_layout.account_id_to_shard_id(&query_account);
    assert_ne!(
        parent_shard_id, child_shard_id,
        "query account's shard must actually split for this repro to be meaningful"
    );
    let parent_shard_uid = ShardUId::from_shard_id_and_layout(parent_shard_id, &base_shard_layout);
    assert!(
        new_shard_layout.get_split_parent_shard_uids().contains(&parent_shard_uid),
        "parent shard {parent_shard_uid:?} should be a split parent in the new layout"
    );

    let epoch_config_store = EpochConfigStore::test(BTreeMap::from_iter(vec![
        (genesis.config.protocol_version, Arc::new(base_epoch_config)),
        (genesis.config.protocol_version + 1, Arc::new(new_epoch_config)),
    ]));

    let client: AccountId = chunk_producer.parse().unwrap();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .clients(vec![client])
        .epoch_config_store(epoch_config_store)
        .gc_num_epochs_to_keep(gc_num_epochs_to_keep)
        .config_modifier(move |config, _client_index| {
            config.gc.gc_step_period = GC_STEP_PERIOD;
        })
        .build();

    let client_handle = env.node_datas[0].client_sender.actor_handle();
    let view_handle = env.node_datas[0].view_client_sender.actor_handle();
    let chain_store = env.test_loop.data.get(&client_handle).client.chain.chain_store.clone();
    let epoch_manager = env.test_loop.data.get(&client_handle).client.epoch_manager.clone();

    // 1. Run until the resharding split has happened (the new shard layout is active at head).
    env.test_loop.run_until(
        |_| {
            let head = chain_store.head().unwrap();
            epoch_manager.get_shard_layout(&head.epoch_id).unwrap() == new_shard_layout
        },
        Duration::seconds((5 * epoch_length) as i64),
    );

    // `H_r` = the resharding boundary block = last block of the epoch before the new-layout epoch.
    let head = chain_store.head().unwrap();
    let resharding_epoch_start =
        epoch_manager.get_epoch_start_height(&head.last_block_hash).unwrap();
    let boundary_height = resharding_epoch_start - 1;

    // 2. Run until GC has processed `H_r`, wiping the split-parent state prefix. The tail comes
    //    to rest on `H_r`, but may advance further if GC/head race ahead, so assert on the
    //    kept-state window (`gc_stop_height`) rather than an exact tail value.
    env.test_loop.run_until(
        |_| chain_store.tail() >= boundary_height,
        Duration::seconds(((gc_num_epochs_to_keep + 6) * epoch_length) as i64),
    );
    let gc_stop_height = chain_store.gc_stop_height();
    assert!(
        gc_stop_height > boundary_height,
        "kept-state window should start past the wiped boundary block \
         (gc_stop_height={gc_stop_height}, boundary_height={boundary_height})"
    );

    // 3. earliest_available must resolve to the first block the node still serves state for -
    //    `gc_stop_height`, in the child layout - NOT the wiped pre-split boundary block `H_r`.
    let earliest_block = {
        let view_client = env.test_loop.data.get_mut(&view_handle);
        view_client
            .handle(GetBlock(BlockReference::SyncCheckpoint(SyncCheckpoint::EarliestAvailable)))
            .expect("earliest_available block should be retrievable")
    };
    assert_eq!(
        earliest_block.header.height, gc_stop_height,
        "earliest_available should resolve to gc_stop_height, not the wiped boundary block"
    );
    let earliest_layout =
        epoch_manager.get_shard_layout(&EpochId(earliest_block.header.epoch_id)).unwrap();
    assert_eq!(
        earliest_layout, new_shard_layout,
        "earliest_available should resolve to a post-split (child-layout) block"
    );

    // 4. The account query at earliest_available now succeeds (before the fix: MissingTrieValue).
    let earliest_result = {
        let view_client = env.test_loop.data.get_mut(&view_handle);
        view_client.handle(Query::new(
            BlockReference::SyncCheckpoint(SyncCheckpoint::EarliestAvailable),
            QueryRequest::ViewAccount { account_id: query_account.clone() },
        ))
    };
    assert!(
        earliest_result.is_ok(),
        "query at earliest_available should succeed after the fix, got: {earliest_result:?}"
    );

    // 5. Sanity: the same query at finality:final (head / child layout) succeeds too.
    let final_result = {
        let view_client = env.test_loop.data.get_mut(&view_handle);
        view_client.handle(Query::new(
            BlockReference::Finality(Finality::Final),
            QueryRequest::ViewAccount { account_id: query_account.clone() },
        ))
    };
    assert!(final_result.is_ok(), "query at finality:final should succeed, got: {final_result:?}");

    // 6. The boundary block `H_r` itself is genuinely garbage collected (its parent-shard state
    //    was wiped), so a query pinned to it returns a clean GarbageCollectedBlock. This confirms
    //    the fix skips unavailable state rather than relying on the boundary block staying readable.
    let boundary_result = {
        let view_client = env.test_loop.data.get_mut(&view_handle);
        view_client.handle(Query::new(
            BlockReference::BlockId(BlockId::Height(boundary_height)),
            QueryRequest::ViewAccount { account_id: query_account },
        ))
    };
    assert!(
        matches!(boundary_result, Err(QueryError::GarbageCollectedBlock { .. })),
        "query pinned to the wiped boundary block should be GarbageCollectedBlock, got: {boundary_result:?}"
    );
}

// After a state sync `gc_stop_height` can fall below `tail` (the sync point), so
// `earliest_available` must be floored at `tail` - otherwise it points at blocks the
// node never downloaded and the query errors. This drives a fresh node through
// far-horizon sync and walks the post-sync catch-up window (where `gc_stop_height <
// tail`), asserting `earliest_available` stays servable throughout.
#[test]
// Spice uses a separate view/query path; sync under spice is not covered here.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_earliest_available_servable_after_state_sync() {
    init_test_logger();

    let accounts = make_accounts(100);
    let mut env = TestLoopBuilder::new()
        .validators(4, 0)
        .num_shards(4)
        .epoch_length(EPOCH_LENGTH)
        .add_user_accounts(&accounts, Balance::from_near(1_000_000))
        .build();

    // Non-trivial state so state sync has something to transfer.
    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_until_head_height(far_horizon_height(EPOCH_LENGTH));

    // Fresh node that far-horizon syncs (EpochSync -> HeaderSync -> StateSync ->
    // BlockSync). Track all shards so account queries don't fail for an unrelated
    // reason, and keep a short GC window so `gc_stop_height` sits below the sync
    // point (the inverted `gc_stop_height < tail` window under test).
    let new_account = create_account_id("new_node");
    let node_state = env
        .node_state_builder()
        .account_id(&new_account)
        .config_modifier(|config| {
            config.tracked_shards_config = TrackedShardsConfig::AllShards;
            config.epoch_sync.epoch_sync_horizon_num_epochs = TEST_EPOCH_SYNC_HORIZON;
            config.gc.gc_num_epochs_to_keep = GC_NUM_EPOCHS_TO_KEEP;
        })
        .build();
    env.add_node("new_node", node_state);
    let new_node_idx = env.node_datas.len() - 1;
    let view_handle = env.node_datas[new_node_idx].view_client_sender.actor_handle();

    let _sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, new_node_idx);
    run_until_synced(&mut env.test_loop, &env.node_datas, new_node_idx, 0);

    // Walk the post-sync catch-up window. Right after sync `gc_stop_height` is
    // below `tail` (blocks below the sync point were never downloaded); as the
    // node advances GC eventually restores the usual `tail == gc_stop_height - 1`.
    // Throughout, `earliest_available` must resolve to a block the node can serve.
    let account = accounts[0].clone();
    let mut observed_inverted_window = false;
    for _ in 0..(GC_NUM_EPOCHS_TO_KEEP + 2) {
        let (head_height, tail, gc_stop_height) = {
            let node = env.node(new_node_idx);
            let chain = &node.client().chain;
            (
                chain.head().unwrap().height,
                chain.chain_store.tail(),
                chain.chain_store.gc_stop_height(),
            )
        };
        observed_inverted_window |= gc_stop_height < tail;

        let earliest_query = {
            let view_client = env.test_loop.data.get_mut(&view_handle);
            view_client.handle(Query::new(
                BlockReference::SyncCheckpoint(SyncCheckpoint::EarliestAvailable),
                QueryRequest::ViewAccount { account_id: account.clone() },
            ))
        };
        let earliest_block = {
            let view_client = env.test_loop.data.get_mut(&view_handle);
            view_client
                .handle(GetBlock(BlockReference::SyncCheckpoint(SyncCheckpoint::EarliestAvailable)))
        };

        let ctx = format!("head={head_height}, tail={tail}, gc_stop_height={gc_stop_height}");
        assert!(
            earliest_query.is_ok(),
            "ViewAccount at earliest_available should succeed ({ctx}), got: {earliest_query:?}"
        );
        assert!(
            earliest_block.is_ok(),
            "GetBlock at earliest_available should succeed ({ctx}), got: {earliest_block:?}"
        );

        env.node_runner(new_node_idx).run_for_number_of_blocks(EPOCH_LENGTH as usize);
    }

    assert!(
        observed_inverted_window,
        "test never entered the post-sync window where gc_stop_height < tail; \
         the regression scenario was not exercised"
    );
}
