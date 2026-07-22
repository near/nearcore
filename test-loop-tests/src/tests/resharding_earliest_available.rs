//! Regression test for the SWEAT / FastAuth incident: a view query with
//! `sync_checkpoint: "earliest_available"` returned
//! `MissingTrieValue(MissingTrieValue { context: TrieStorage, .. })` for about an
//! epoch after a resharding split, then "resolved by itself".
//!
//! Root cause: `earliest_available` used to resolve to the GC `tail` block, which
//! sits one block *below* the state-kept window `[gc_stop_height, head]` and is
//! retained only as a boundary marker. When the GC tail comes to rest on the
//! resharding boundary block `H_r` (the last block of the pre-split epoch `E`),
//! `gc_parent_shard_after_resharding` has already deleted the split-parent shard's
//! state, so a query resolving `account -> shard` via `H_r`'s (pre-split) layout
//! hit the wiped parent shard and failed with `MissingTrieValue`.
//!
//! Fix: `get_earliest_block_hash` anchors on `gc_stop_height` (the first block the
//! node still serves state for) instead of the tail. Past a resharding boundary
//! that is `H_r + 1`, the first block of the child epoch, whose state is intact.
//!
//! This test drives a single split, lets GC advance the tail onto `H_r` (wiping
//! the parent shard), and asserts:
//!   * `earliest_available` resolves to `H_r + 1` (child layout), not `H_r`;
//!   * a `ViewAccount` there for an account in the split shard succeeds;
//!   * the boundary block `H_r` is genuinely gone (explicit query -> GarbageCollectedBlock),
//!     so the fix is skipping unavailable state rather than getting lucky.
//! Before the fix the `earliest_available` query failed with `MissingTrieValue`.

use crate::setup::builder::{MIN_BLOCK_PROD_TIME, TestLoopBuilder};
use crate::utils::setups::derive_new_epoch_config_from_boundary;
use near_async::messaging::Handler;
use near_async::time::Duration;
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
use std::sync::Arc;

// Keep GC stepping at least as often as blocks are produced so the tail promptly reaches `H_r`.
const GC_STEP_PERIOD: Duration = Duration::milliseconds(MIN_BLOCK_PROD_TIME as i64);

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
        .add_user_accounts_simple(&[query_account.clone()], Balance::from_near(1_000_000))
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

    // 2. Run until GC has processed `H_r`: the tail comes to rest exactly on it, and the
    //    split-parent state prefix has been wiped. `gc_stop_height` is now `H_r + 1`.
    env.test_loop.run_until(
        |_| chain_store.tail() >= boundary_height,
        Duration::seconds(((gc_num_epochs_to_keep + 6) * epoch_length) as i64),
    );
    assert_eq!(
        chain_store.tail(),
        boundary_height,
        "tail should rest on the resharding boundary block"
    );

    // 3. earliest_available must resolve to the first block the node still serves state for -
    //    `H_r + 1`, in the child layout - NOT the wiped pre-split boundary block `H_r`.
    let earliest_block = {
        let view_client = env.test_loop.data.get_mut(&view_handle);
        Handler::handle(
            view_client,
            GetBlock(BlockReference::SyncCheckpoint(SyncCheckpoint::EarliestAvailable)),
        )
        .expect("earliest_available block should be retrievable")
    };
    assert_eq!(
        earliest_block.header.height, resharding_epoch_start,
        "earliest_available should skip the wiped boundary block and resolve to gc_stop_height"
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
        Handler::handle(
            view_client,
            Query::new(
                BlockReference::SyncCheckpoint(SyncCheckpoint::EarliestAvailable),
                QueryRequest::ViewAccount { account_id: query_account.clone() },
            ),
        )
    };
    assert!(
        earliest_result.is_ok(),
        "query at earliest_available should succeed after the fix, got: {earliest_result:?}"
    );

    // 5. Sanity: the same query at finality:final (head / child layout) succeeds too.
    let final_result = {
        let view_client = env.test_loop.data.get_mut(&view_handle);
        Handler::handle(
            view_client,
            Query::new(
                BlockReference::Finality(Finality::Final),
                QueryRequest::ViewAccount { account_id: query_account.clone() },
            ),
        )
    };
    assert!(final_result.is_ok(), "query at finality:final should succeed, got: {final_result:?}");

    // 6. The boundary block `H_r` itself is genuinely garbage collected (its parent-shard state
    //    was wiped), so a query pinned to it returns a clean GarbageCollectedBlock. This confirms
    //    the fix skips unavailable state rather than relying on the boundary block staying readable.
    let boundary_result = {
        let view_client = env.test_loop.data.get_mut(&view_handle);
        Handler::handle(
            view_client,
            Query::new(
                BlockReference::BlockId(BlockId::Height(boundary_height)),
                QueryRequest::ViewAccount { account_id: query_account.clone() },
            ),
        )
    };
    assert!(
        matches!(boundary_result, Err(QueryError::GarbageCollectedBlock { .. })),
        "query pinned to the wiped boundary block should be GarbageCollectedBlock, got: {boundary_result:?}"
    );
}
