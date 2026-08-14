//! Sync × EarlyKickout integration coverage: nodes joining via the sync pipeline must
//! hold the `DBCol::ChunkProducers` rows their catch-up reads depend on, and those rows
//! must name the right producer.
//!
//! The oracle lives in `tests::early_kickout_probe`. The blacklist is empty throughout all
//! three cases — `EARLY_KICKOUT_EPOCH_GRACE_BLOCKS` is 1000 against an epoch of 10, so no
//! anchor clears the grace window — which every case asserts rather than assumes. The offset
//! half of the oracle bites only where a shard has more than one chunk producer, since the
//! sampler ignores the height when the settlement holds a single entry; cases A and C run
//! more validators than shards for that reason.
//!
//! Adjacent coverage: the epoch-manager unit tests own "seeding writes rows" and
//! the error on a miss (`chain/chain/src/tests/chunk_producers.rs`,
//! `test_resolution_errors_on_anchor_db_miss`); `tests::early_kickout_e2e` owns sync
//! under an *active* kickout, where rows deviate from the plain schedule.
//!
//! Nightly-only (gated at `sync/mod.rs`): EarlyKickout only enters `PROTOCOL_VERSION`
//! on nightly.

use super::state_sync::{
    assert_shard_shuffling_happened, assert_state_synced_for_reassigned_shard,
    get_boundary_accounts,
};
use super::util::{
    TEST_EPOCH_SYNC_HORIZON, assert_far_horizon_sync_sequence, assert_near_horizon_sync_sequence,
    far_horizon_height, restrict_to_single_peer, run_until_synced, track_sync_status,
    verify_balances_on_synced_node,
};
use crate::setup::builder::TestLoopBuilder;
use crate::tests::early_kickout_probe::{
    AnchorWalk, assert_blacklist_read_everywhere, assert_walk_window,
    lowest_epoch_start_in_headers, probe_block_region, walk_anchor_rows,
};
use crate::utils::account::{create_account_id, create_validators_spec, validators_spec_clients};
use crate::utils::transactions::{execute_money_transfers, make_accounts};
use near_chain_configs::TrackedShardsConfig;
use near_chain_configs::test_genesis::TestEpochConfigBuilder;
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::Balance;
use near_primitives::version::{MIN_SUPPORTED_PROTOCOL_VERSION, ProtocolFeature};

/// A row written under an active blacklist here means the grace logic regressed: every anchor
/// these cases touch is inside the grace window. `tests::early_kickout_e2e` owns the other
/// side, where the blacklist is live.
fn assert_inside_grace_window(walk: &AnchorWalk, label: &str) {
    assert_eq!(
        walk.blacklisted_rows(),
        0,
        "{label}: rows written under an active blacklist, but every anchor here is inside \
         the start-of-epoch grace window ({walk:?})"
    );
}

// Case A — far horizon (epoch → header → state → block sync), observer node.
//
// Mirrors `test_far_horizon_full_pipeline`. An observer verifies chunk-header
// signatures via the anchored read during catch-up, before any validator-role gating,
// so a missing same-epoch row rejects the block and stalls `run_until_synced`. The
// probes turn that into a positive assertion.
#[test]
// TODO(spice-test): mirrors a sync scenario spice marks incompatible; assess and fix for spice.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_early_kickout_far_horizon_observer() {
    init_test_logger();

    let epoch_length = 10;
    let accounts = make_accounts(100);
    // Two shards against four validators: two producers per shard, so the canonical
    // schedule rotates with height and the oracle can detect a wrong anchor offset.
    let mut env = TestLoopBuilder::new()
        .validators(4, 0)
        .num_shards(2)
        .epoch_length(epoch_length)
        .add_user_accounts(&accounts, Balance::from_near(1_000_000))
        .build();

    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_until_head_height(far_horizon_height(epoch_length));

    let new_account = create_account_id("new_node");
    let node_state = env
        .node_state_builder()
        .account_id(&new_account)
        .config_modifier(|config| {
            // Track all shards so verify_balances_on_synced_node can query every account.
            config.tracked_shards_config = TrackedShardsConfig::AllShards;
            config.epoch_sync.epoch_sync_horizon_num_epochs = TEST_EPOCH_SYNC_HORIZON;
        })
        .build();
    env.add_node("new_node", node_state);
    let new_node_idx = env.node_datas.len() - 1;

    // One source peer, so the sync sequence below describes one peer's data rather than
    // a race between four.
    restrict_to_single_peer(&env.shared_state, &env.node_datas, new_node_idx, 0);
    let sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, new_node_idx);
    run_until_synced(&mut env.test_loop, &env.node_datas, new_node_idx, 0);

    // Probe H — header-only region, above the state-sync tail down to the synced epoch
    // start. Reachable only by a header walk, and where the epoch-sync-seeded rows live.
    //
    // Runs before the extra epochs below: the rows survive them today
    // (`DEFAULT_GC_NUM_EPOCHS_TO_KEEP` is 5, margin about one epoch), but that is a
    // tuning accident. Starting at `tail + 2` keeps the anchors covered here contiguous
    // with the block probe's floor of `tail + 3`.
    let probe_h = {
        let node = env.node(new_node_idx);
        let tail = node.tail();
        let start = node.client().chain.get_block_hash_by_height(tail + 2).unwrap();
        let low = lowest_epoch_start_in_headers(&node, start);
        let walk = walk_anchor_rows(&node, start, low);
        assert_walk_window(&walk, epoch_length / 2, "header-only probe");
        assert_eq!(
            walk.lowest_height, low,
            "header-only probe stopped above the synced epoch start ({walk:?})"
        );
        assert!(
            walk.lowest_height < tail,
            "header-only probe never reached below the tail {tail} ({walk:?}); \
             epoch sync did not leave a header-only region"
        );
        assert!(walk.same_epoch_rows > 0, "header-only probe checked no anchor row ({walk:?})");
        // Reaching the synced epoch's opening heights is what exercises the sampler
        // fallthrough on an epoch this node never processed blocks for.
        assert!(
            walk.cross_epoch_heights > 0,
            "header-only probe never reached the synced epoch's opening heights ({walk:?})"
        );
        assert_inside_grace_window(&walk, "header-only probe");
        // The one region where the accessor cannot always run: the headers around the
        // epoch-sync point arrive without a `BlockInfo` (`apply_validated_proof` writes one
        // for its own boundary blocks only). Measured: 2. Bounded by the proof's
        // boundary-header count rather than pinned to that, so losing `BlockInfo` for
        // header-synced heights trips it while proof-shape drift does not.
        assert!(
            walk.blacklist_unavailable <= 3,
            "header-only probe: blacklist unreadable for {} anchors, more than the epoch-sync \
             proof's boundary headers can explain ({walk:?})",
            walk.blacklist_unavailable
        );
        walk
    };

    env.node_runner(new_node_idx).run_for_number_of_blocks(3 * epoch_length as usize);

    // Probe B — block region above the tail. Runs after the extra epochs: right after
    // sync the tail sits only a few heights below the head.
    let probe_b = probe_block_region(&env.node(new_node_idx), epoch_length);
    assert_walk_window(&probe_b, epoch_length, "block-region probe");
    assert!(probe_b.same_epoch_rows > 0, "block-region probe checked no anchor row ({probe_b:?})");
    assert_inside_grace_window(&probe_b, "block-region probe");
    assert_blacklist_read_everywhere(&probe_b, "block-region probe");

    tracing::info!(target: "test", ?probe_h, ?probe_b, "far-horizon observer probes complete");

    assert_far_horizon_sync_sequence(&sync_history.borrow());
    verify_balances_on_synced_node(&env.test_loop.data, &env.node_datas, new_node_idx, &accounts);
}

// Case B — validator state sync via shard shuffling (true producer catchup).
//
// Mirrors `test_state_sync_simple_two_node`. Shuffling forces a validator to state-sync
// a newly assigned shard and then produce chunks for it, running the V2 partial-witness
// path; with one producer per shard any failure stalls the chain.
//
// That single producer per shard also means the schedule does not rotate with height
// here, so this case cannot detect a wrong anchor offset. Cases A and C carry that.
#[test]
// TODO(spice-test): mirrors a sync scenario spice marks incompatible; assess and fix for spice.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_early_kickout_state_sync_shuffling() {
    init_test_logger();

    let epoch_length = 10;
    let validators_spec = create_validators_spec(2, 0);
    let clients = validators_spec_clients(&validators_spec);
    let accounts = make_accounts(10);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .shard_layout(ShardLayout::multi_shard_custom(get_boundary_accounts(2), 1))
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, Balance::from_near(10_000))
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build_store_for_genesis_protocol_version();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients.clone())
        .build();

    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_for_number_of_blocks(40);

    // Reaching 40 blocks already implies state sync worked; these make it explicit.
    assert_shard_shuffling_happened(&env, &clients);
    let state_synced = assert_state_synced_for_reassigned_shard(&env, &clients);
    tracing::info!(target: "test", ?state_synced, "validators that state-synced a reassigned shard");

    for idx in 0..clients.len() {
        let label = format!("block-region probe on node {idx}");
        let walk = probe_block_region(&env.node(idx), epoch_length);
        assert_walk_window(&walk, epoch_length / 2, &label);
        assert!(walk.same_epoch_rows > 0, "node {idx} probe checked no anchor row ({walk:?})");
        assert_inside_grace_window(&walk, &label);
        assert_blacklist_read_everywhere(&walk, &label);
    }
}

// Case C — the 151 -> 152 activation edge, crossed by a node that block-synced from
// genesis: the early epochs resolve through the canonical sampler with no rows at all,
// then the vote lands the client version and rows become mandatory.
//
// Near-horizon rather than far-horizon on purpose. The vote jumps straight to the
// client version, so activation lands around height 21, while far-horizon sync needs a
// head of at least `far_horizon_height` and a block-probe floor near 20 — the boundary
// would sit one height inside the window and drift out on any timing change, leaving
// the pre-activation assertions vacuous.
#[test]
// TODO(spice-test): mirrors a sync scenario spice marks incompatible; assess and fix for spice.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_early_kickout_activation_edge_block_sync() {
    init_test_logger();

    // Not `PROTOCOL_VERSION - 1`: that sits well above EarlyKickout on nightly and
    // would give no activation edge.
    let genesis_protocol_version = ProtocolFeature::EarlyKickout.protocol_version() - 1;
    assert!(
        genesis_protocol_version >= MIN_SUPPORTED_PROTOCOL_VERSION,
        "genesis version {genesis_protocol_version} is below the minimum supported \
         {MIN_SUPPORTED_PROTOCOL_VERSION}"
    );

    let epoch_length = 10;
    // Four validators over two shards, for the rotating schedule the oracle needs.
    let validators_spec = create_validators_spec(4, 0);
    let clients = validators_spec_clients(&validators_spec);
    let accounts = make_accounts(10);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .protocol_version(genesis_protocol_version)
        .shard_layout(ShardLayout::multi_shard_custom(get_boundary_accounts(2), 1))
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, Balance::from_near(10_000))
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build_store_for_genesis_protocol_version();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients.clone())
        .build();

    // No `execute_money_transfers`: it would push the head past the epoch sync horizon,
    // and the joiner would far-horizon sync straight over the pre-activation heights
    // this test is about.
    env.node_runner(0).run_until_head_height(TEST_EPOCH_SYNC_HORIZON * epoch_length - 1);

    let new_account = create_account_id("new_node");
    let node_state = env
        .node_state_builder()
        .account_id(&new_account)
        .config_modifier(|config| {
            config.epoch_sync.epoch_sync_horizon_num_epochs = TEST_EPOCH_SYNC_HORIZON;
        })
        .build();
    env.add_node("new_node", node_state);
    let new_node_idx = env.node_datas.len() - 1;

    let sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, new_node_idx);
    run_until_synced(&mut env.test_loop, &env.node_datas, new_node_idx, 0);
    assert_near_horizon_sync_sequence(&sync_history.borrow());

    // Getting past the boundary at all is the no-stall assertion.
    env.node_runner(new_node_idx).run_for_number_of_blocks(2 * epoch_length as usize);

    let node = env.node(new_node_idx);
    let head = node.head();
    let version_at_head =
        node.client().epoch_manager.get_epoch_protocol_version(&head.epoch_id).unwrap();
    assert!(
        ProtocolFeature::EarlyKickout.enabled(version_at_head),
        "head epoch is still at version {version_at_head}; the network never upgraded"
    );

    let walk = probe_block_region(&node, epoch_length);
    assert_walk_window(&walk, epoch_length / 2, "activation-edge probe");

    // Without this the three assertions below pass vacuously on a window sitting
    // entirely on one side of activation.
    let activation_height = walk
        .first_kickout_height
        .unwrap_or_else(|| panic!("walk never reached an EarlyKickout epoch ({walk:?})"));
    assert!(
        activation_height > walk.lowest_height,
        "activation at {activation_height} is at or below the bottom of the walked window \
         ({walk:?}); the pre-activation assertions would be vacuous"
    );
    assert!(walk.pre_activation_heights > 0, "no pre-activation height walked ({walk:?})");
    assert!(
        walk.cross_epoch_heights > 0,
        "no height took the cross-epoch arm; the epoch boundary was not covered ({walk:?})"
    );
    assert!(
        walk.same_epoch_rows > 0,
        "no same-epoch anchor row checked after activation ({walk:?})"
    );
    assert_inside_grace_window(&walk, "activation-edge probe");
    assert_blacklist_read_everywhere(&walk, "activation-edge probe");

    assert_shard_shuffling_happened(&env, &clients);
    tracing::info!(target: "test", activation_height, ?walk, "activation edge probe complete");
}
