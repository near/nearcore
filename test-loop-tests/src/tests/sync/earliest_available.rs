//! Regression test for PR #16100 review feedback (Trisfald): `earliest_available`
//! must resolve to a block whose state the node can actually serve, including
//! right after a state sync.
//!
//! `get_earliest_block_hash` anchors on `gc_stop_height`, but a freshly
//! state-synced node has its `tail` (the sync point / oldest block it actually
//! downloaded) *above* `gc_stop_height` — which is computed from
//! `head_epoch - gc_num_epochs_to_keep` and can point at epochs the node never
//! downloaded. Anchoring purely on `gc_stop_height` would hand out a block the
//! node never fetched, so a `ViewAccount` at `earliest_available` errors. The fix
//! takes `max(gc_stop_height, tail)`.
//!
//! This drives a fresh node through far-horizon sync, then walks the post-sync
//! catch-up window (where `gc_stop_height < tail`) and asserts `earliest_available`
//! stays servable throughout.

use super::util::{
    TEST_EPOCH_SYNC_HORIZON, far_horizon_height, run_until_synced, track_sync_status,
};
use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use crate::utils::transactions::{execute_money_transfers, make_accounts};
use near_async::messaging::Handler;
use near_chain_configs::TrackedShardsConfig;
use near_client::{GetBlock, Query};
use near_o11y::testonly::init_test_logger;
use near_primitives::types::{Balance, BlockReference, SyncCheckpoint};
use near_primitives::views::QueryRequest;

const EPOCH_LENGTH: u64 = 10;
const GC_NUM_EPOCHS_TO_KEEP: u64 = 3;

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
