//! Sync × EarlyKickout integration coverage: nodes joining via the sync pipeline must
//! hold the `DBCol::ChunkProducers` rows their block-validation reads while syncing
//! depend on, and those rows must name the right producer.
//!
//! The oracle lives in `tests::early_kickout_probe`. The blacklist is empty throughout all
//! four cases — `EARLY_KICKOUT_EPOCH_GRACE_BLOCKS` is 1000 against an epoch of 10, so no
//! anchor clears the grace window — which every case asserts rather than assumes. The offset
//! half of the oracle bites only where a shard has more than one chunk producer, since the
//! sampler ignores the height when the settlement holds a single entry; cases A, C and D run
//! more validators than shards for that reason.
//!
//! Cases C and D cross EarlyKickout activation over block sync and epoch sync.
//! Stable isolates the adjacent 86-to-87 edge. Nightly crosses every feature from 87 through
//! `PROTOCOL_VERSION`.
//!
//! Adjacent coverage: the epoch-manager unit tests own "seeding writes rows" and
//! the error on a miss (`chain/chain/src/tests/chunk_producers.rs`,
//! `test_resolution_errors_on_anchor_db_miss`); `tests::early_kickout_e2e` owns sync
//! under an *active* kickout, where rows deviate from the plain schedule.

use super::state_sync::{
    assert_shard_shuffling_happened, assert_state_synced_for_reassigned_shard,
    get_boundary_accounts,
};
use super::util::{
    TEST_EPOCH_SYNC_HORIZON, assert_far_horizon_sync_sequence, assert_near_horizon_sync_sequence,
    collect_distinct_epoch_ids, far_horizon_height, restrict_to_single_peer, run_until_synced,
    track_sync_status, verify_balances_on_synced_node,
};
use crate::setup::builder::TestLoopBuilder;
use crate::tests::early_kickout_probe::{
    AnchorWalk, assert_blacklist_read_everywhere, assert_walk_window, probe_block_region,
    walk_anchor_rows,
};
use crate::utils::account::{create_account_id, create_validators_spec, validators_spec_clients};
use crate::utils::node::TestLoopNode;
use crate::utils::transactions::{execute_money_transfers, make_accounts};
use borsh::BorshDeserialize;
use near_async::time::Duration;
use near_chain_configs::TrackedShardsConfig;
use near_chain_configs::test_genesis::TestEpochConfigBuilder;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{Balance, BlockHeight, EpochId};
use near_primitives::version::{MIN_SUPPORTED_PROTOCOL_VERSION, ProtocolFeature};
use near_store::DBCol;

/// How far the epoch-sync target lags the head, in epochs, on Case D's chain.
///
/// `ContinuousEpochSync` is active on every profile, so a peer serves the *stored* proof rather
/// than deriving one per request. On a linear chain, processing the first block of epoch T advances
/// that stored proof to T-2 (`update_epoch_sync_proof`, called from `ChainUpdate` at each boundary).
/// Not an invariant to lean on beyond this fixture: the derivation reuses an existing proof that
/// already reaches the epoch, writes nothing at the first boundaries, and also runs on fork blocks.
/// Case D's straddle assertion, not this arithmetic, is what catches the target drifting.
///
/// With activation measured at height 32 and an epoch of 10, this puts the source's stop-epoch
/// start at 52, and the source heights that both clear epoch sync's "too recent" gate and still
/// sit inside that epoch are 53..=61. Case D stops mid-epoch, at 57, to sit in the middle of that
/// window rather than on either edge.
const TARGET_LAG_EPOCHS: u64 = 2;

/// Epoch-start height of `node`'s head epoch, from its own `DBCol::EpochStart` rows.
fn source_epoch_start(node: &TestLoopNode) -> Option<BlockHeight> {
    node.store().get_ser(DBCol::EpochStart, node.head().epoch_id.as_ref())
}

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

/// Probe H — the header-only region a sync leaves behind: from `tail + 2` down to the synced
/// epoch's start, reachable only by a header walk and exactly where the epoch-sync seeder's rows
/// live. Returns the walk and the synced epoch's id, so a caller can say more about that epoch.
///
/// Starting at `tail + 2` keeps the anchors covered here contiguous with `probe_block_region`'s
/// floor of `tail + 3`. Callers run it before any further block production: the rows survive a few
/// more epochs today (`DEFAULT_GC_NUM_EPOCHS_TO_KEEP` is 5, margin about one epoch), but that is a
/// tuning accident.
fn probe_header_region(
    node: &TestLoopNode,
    source: &TestLoopNode,
    epoch_length: u64,
) -> (AnchorWalk, EpochId) {
    let tail = node.tail();
    let start = node.client().chain.get_block_hash_by_height(tail + 2).unwrap();
    // Floor of the walk: the synced epoch's start, from the follower's own `DBCol::EpochStart`
    // rows — independent of the header traversal under test. A fresh node has no row before epoch
    // sync, `apply_validated_proof` writes exactly one, and header sync only adds rows for later
    // epochs, so the min non-genesis row is the synced epoch's. Cross-checked against the source
    // so a wrong row cannot silently move the floor.
    let (low_epoch_id, low) = node
        .store()
        .iter(DBCol::EpochStart)
        .map(|(key, value)| {
            let epoch_id = EpochId(CryptoHash::try_from(&key[..]).unwrap());
            let height = BlockHeight::try_from_slice(&value).unwrap();
            (epoch_id, height)
        })
        .filter(|(_, height)| *height > 0)
        .min_by_key(|(_, height)| *height)
        .expect("epoch sync wrote no EpochStart row");
    let source_low: Option<BlockHeight> =
        source.store().get_ser(DBCol::EpochStart, low_epoch_id.as_ref());
    assert_eq!(
        source_low,
        Some(low),
        "seeded EpochStart height disagrees with the source for epoch {low_epoch_id:?}"
    );
    let walk = walk_anchor_rows(node, start, low).unwrap_or_else(|err| {
        panic!("header-only probe failed before reaching the floor {low}: {err:?}")
    });
    // The walk's own postcondition, re-asserted so a walker regression that returns `Ok` early
    // cannot silently narrow the probe.
    assert_eq!(
        walk.lowest_height, low,
        "header-only probe stopped above the synced epoch start ({walk:?})"
    );
    assert_walk_window(&walk, epoch_length / 2, "header-only probe");
    assert!(
        walk.lowest_height < tail,
        "header-only probe never reached below the tail {tail} ({walk:?}); \
         epoch sync did not leave a header-only region"
    );
    assert!(walk.same_epoch_rows > 0, "header-only probe checked no anchor row ({walk:?})");
    // Reaching the synced epoch's opening heights is what exercises the sampler fallthrough on an
    // epoch this node never processed blocks for.
    assert!(
        walk.cross_epoch_heights > 0,
        "header-only probe never reached the synced epoch's opening heights ({walk:?})"
    );
    assert_inside_grace_window(&walk, "header-only probe");
    // The one region where the accessor cannot always run: the headers around the epoch-sync point
    // arrive without a `BlockInfo` (`apply_validated_proof` writes one for its own boundary blocks
    // only). Measured: 2. Bounded by the proof's boundary-header count rather than pinned to that,
    // so losing `BlockInfo` for header-synced heights trips it while proof-shape drift does not.
    assert!(
        walk.blacklist_unavailable <= 3,
        "header-only probe: blacklist unreadable for {} anchors, more than the epoch-sync \
         proof's boundary headers can explain ({walk:?})",
        walk.blacklist_unavailable
    );
    (walk, low_epoch_id)
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

    // Probe H — the header-only region epoch sync left behind. Runs before the extra epochs
    // below; see `probe_header_region` for why.
    let (probe_h, _synced_epoch_id) =
        probe_header_region(&env.node(new_node_idx), &env.node(0), epoch_length);

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
// here, so this case cannot detect a wrong anchor offset. Cases A, C and D carry that.
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
    let acquisitions = assert_state_synced_for_reassigned_shard(&env, &clients);
    tracing::info!(target: "test", ?acquisitions, "state acquisitions for reassigned shards");

    for idx in 0..clients.len() {
        let label = format!("block-region probe on node {idx}");
        let walk = probe_block_region(&env.node(idx), epoch_length);
        assert_walk_window(&walk, epoch_length / 2, &label);
        assert!(walk.same_epoch_rows > 0, "node {idx} probe checked no anchor row ({walk:?})");
        assert_inside_grace_window(&walk, &label);
        assert_blacklist_read_everywhere(&walk, &label);
    }
}

// Case C — the EarlyKickout activation edge, crossed by a node that block-synced from
// genesis: the early epochs resolve through the canonical sampler with no rows at all,
// then the vote lands the client version and rows become mandatory.
//
// Near-horizon rather than far-horizon on purpose. The vote jumps straight to the client
// version, so activation lands at height 32 (measured, both profiles), while far-horizon sync
// needs a head of at least `far_horizon_height` — 50 at this epoch length — and floors its
// block probe at `tail + 3`. That tail tracks the state-sync point a couple of epochs below
// such a head, so the boundary would sit at or under the floor and the pre-activation
// assertions would go vacuous.
#[test]
// TODO(spice-test): mirrors a sync scenario spice marks incompatible; assess and fix for spice.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_early_kickout_activation_edge_block_sync() {
    init_test_logger();

    // Not `PROTOCOL_VERSION - 1`: on a nightly build that sits well above EarlyKickout
    // and would give no activation edge.
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

// Case D — the EarlyKickout activation edge crossed by a node that joined through epoch sync.
//
// Epoch sync bypasses `record_block_info` for the synced epoch and seeds its first block through
// `seed_chunk_producers_after_epoch_sync`. Case A and `early_kickout_e2e`'s epoch-sync bootstrap
// reach that writer too, but both run chains that have been at the EarlyKickout version since
// genesis. Here the target epoch is the *first* one at that version, so the proof's earlier
// boundary blocks are pre-activation and carry no rows.
//
// What that buys over Case A, stated as the mutation it kills: gate the writer's call site on the
// predecessor epoch's protocol version instead of the seeded epoch's, and Case A still passes while
// this case fails on a missing same-epoch anchor row. The walk statistics do not show it —
// `cross_epoch_heights` counts anchors in a different epoch, not a pre-activation one, and reports
// the same value in both cases.
//
// The chain is run to a computed height rather than a fixed one because extra blocks slide the
// target forward off the activation epoch. The complementary writer arm, a synced epoch still below
// activation, belongs to `epoch_sync_seeder_writes_no_rows_below_activation`.
#[test]
// TODO(spice-test): mirrors a sync scenario spice marks incompatible; assess and fix for spice.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_early_kickout_activation_edge_epoch_sync() {
    init_test_logger();

    // Not `PROTOCOL_VERSION - 1`: on a nightly build that sits well above EarlyKickout and
    // would give no activation edge.
    let genesis_protocol_version = ProtocolFeature::EarlyKickout.protocol_version() - 1;
    assert!(
        genesis_protocol_version >= MIN_SUPPORTED_PROTOCOL_VERSION,
        "genesis version {genesis_protocol_version} is below the minimum supported \
         {MIN_SUPPORTED_PROTOCOL_VERSION}"
    );

    let epoch_length = 10;
    // Four validators over two shards, so each shard has more than one chunk producer and the
    // canonical schedule rotates with height; a single-producer shard would make the oracle's
    // anchor-offset half blind.
    let validators_spec = create_validators_spec(4, 0);
    let clients = validators_spec_clients(&validators_spec);
    let accounts = make_accounts(100);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .protocol_version(genesis_protocol_version)
        .shard_layout(ShardLayout::multi_shard_custom(get_boundary_accounts(2), 1))
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, Balance::from_near(1_000_000))
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build_store_for_genesis_protocol_version();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .build();

    // Where activation actually landed, read off the chain rather than assumed from the vote
    // schedule. The margin against the far-horizon threshold below is only a couple of blocks, so
    // a vote landing an epoch earlier trips that assert rather than being absorbed here.
    let run_timeout = Duration::seconds((15 * epoch_length) as i64);
    env.node_runner(0).run_until(
        |node| {
            let head = node.head();
            let version =
                node.client().epoch_manager.get_epoch_protocol_version(&head.epoch_id).unwrap();
            ProtocolFeature::EarlyKickout.enabled(version)
        },
        run_timeout,
    );
    let activation_start = source_epoch_start(&env.node(0))
        .expect("no `EpochStart` row for the first EarlyKickout epoch");

    // Stop `TARGET_LAG_EPOCHS` epochs past activation, so the target is the activation epoch.
    // Running further slides it forward and the probes below degrade into a re-run of Case A.
    //
    // Stopping mid-epoch rather than on the boundary: the boundary is where both margins are
    // thinnest at once — the far-horizon floor below, and epoch sync's own "source too recent"
    // gate (`chain/client/src/sync/epoch.rs`), which needs the source a few blocks past the
    // target's epoch start. Half an epoch of extra head buys both without moving the target.
    let stop_at_epoch_start = activation_start + TARGET_LAG_EPOCHS * epoch_length;
    let stop_at_height = stop_at_epoch_start + epoch_length / 2;
    env.node_runner(0).run_until(|node| node.head().height >= stop_at_height, run_timeout);
    // Waiting on an absolute height, so re-assert what the wait used to imply: the source is still
    // inside the intended epoch, which is what keeps the stored epoch-sync proof pointed at the
    // activation epoch.
    assert_eq!(
        source_epoch_start(&env.node(0)),
        Some(stop_at_epoch_start),
        "source left epoch {stop_at_epoch_start} before height {stop_at_height}; the stored \
         proof no longer targets the activation epoch"
    );
    // `far_horizon_height` is this suite's conservative setup floor, not the production phase
    // decision, so a shorter chain is not necessarily impossible to sync — it is just outside what
    // this fixture is set up for. `assert_far_horizon_sync_sequence` below is the exact statement
    // about which path actually ran.
    let head_height = env.node(0).head().height;
    assert!(
        head_height >= far_horizon_height(epoch_length),
        "activation landed too early for this fixture: stopping {TARGET_LAG_EPOCHS} epochs past it \
         leaves head {head_height}, under the far-horizon setup floor {}",
        far_horizon_height(epoch_length)
    );

    let new_account = create_account_id("new_node");
    let node_state = env
        .node_state_builder()
        .account_id(&new_account)
        .config_modifier(|config| {
            // Track all shards so every shard's anchored read runs on the joiner.
            config.tracked_shards_config = TrackedShardsConfig::AllShards;
            config.epoch_sync.epoch_sync_horizon_num_epochs = TEST_EPOCH_SYNC_HORIZON;
        })
        .build();
    env.add_node("new_node", node_state);
    let new_node_idx = env.node_datas.len() - 1;

    // One source peer, so the sync sequence below describes one peer's data rather than a race
    // between four.
    restrict_to_single_peer(&env.shared_state, &env.node_datas, new_node_idx, 0);
    let sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, new_node_idx);
    run_until_synced(&mut env.test_loop, &env.node_datas, new_node_idx, 0);
    // Asserted before the probes: they are only about epoch sync if epoch sync actually ran.
    assert_far_horizon_sync_sequence(&sync_history.borrow());

    // Probe H — the header-only region above the state-sync tail down to the synced epoch start,
    // which is exactly where `apply_validated_proof` left its rows.
    let (probe_h, synced_epoch_id) =
        probe_header_region(&env.node(new_node_idx), &env.node(0), epoch_length);

    // The whole point of this case: the synced epoch is the FIRST EarlyKickout epoch, so its
    // predecessor is still pre-activation and the proof straddles the edge. Asserting only the
    // enabled side would let the target slide forward and leave the probe above re-running Case A
    // on a single-version proof.
    //
    // Read from the joiner and cross-checked against the source. The joiner's copy is the one that
    // matters: `init_after_epoch_sync` installs the predecessor's `EpochInfo` from the proof, and
    // that pre-activation entry is the state this case has and Case A does not.
    {
        let source = env.node(0);
        let source_epoch_manager = &source.client().epoch_manager;
        let epoch_ids = collect_distinct_epoch_ids(source.client());
        let position = epoch_ids
            .iter()
            .position(|id| id == &synced_epoch_id)
            .expect("synced epoch is not on the source's canonical chain");
        let prev_epoch_id = position
            .checked_sub(1)
            .map(|index| epoch_ids[index])
            .expect("synced epoch is the source's first epoch, so nothing precedes it");

        let node = env.node(new_node_idx);
        let joiner_epoch_manager = &node.client().epoch_manager;
        let synced_version =
            joiner_epoch_manager.get_epoch_protocol_version(&synced_epoch_id).unwrap();
        let prev_version = joiner_epoch_manager.get_epoch_protocol_version(&prev_epoch_id).unwrap();
        assert_eq!(
            (synced_version, prev_version),
            (
                source_epoch_manager.get_epoch_protocol_version(&synced_epoch_id).unwrap(),
                source_epoch_manager.get_epoch_protocol_version(&prev_epoch_id).unwrap(),
            ),
            "joiner disagrees with the source about the versions either side of the boundary"
        );
        assert!(
            ProtocolFeature::EarlyKickout.enabled(synced_version),
            "synced epoch is at version {synced_version}, below activation; the seeder \
             writes nothing there and every row assertion above would be vacuous"
        );
        assert!(
            !ProtocolFeature::EarlyKickout.enabled(prev_version),
            "the epoch before the synced one is already at version {prev_version}; the \
             target slid past activation and the proof no longer straddles the edge"
        );
    }

    env.node_runner(new_node_idx).run_for_number_of_blocks(3 * epoch_length as usize);

    // Probe B — the block region above the tail, built by the joiner's own block processing
    // after the sync. Getting here at all is the no-stall assertion: a missing same-epoch
    // anchor row rejects the block with `ChunkProducerNotInDB` and the node never advances.
    let probe_b = probe_block_region(&env.node(new_node_idx), epoch_length);
    assert_walk_window(&probe_b, epoch_length, "block-region probe");
    assert!(probe_b.same_epoch_rows > 0, "block-region probe checked no anchor row ({probe_b:?})");
    assert_inside_grace_window(&probe_b, "block-region probe");
    assert_blacklist_read_everywhere(&probe_b, "block-region probe");

    tracing::info!(target: "test", ?probe_h, ?probe_b, "activation-edge epoch-sync probes complete");
}
