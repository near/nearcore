//! End-to-end tests for early (mid-epoch) chunk producer kickout.
//!
//! `ProtocolFeature::EarlyKickout` (nightly) tracks each chunk producer's
//! production stats within an epoch and, once a producer crosses the
//! mid-epoch thresholds, excludes it from the `DBCol::ChunkProducers`
//! assignment for the chunks anchored at later blocks. The excluded slot is
//! reassigned to a healthy replacement. These tests drive the whole pipeline
//! over a live test-loop chain:
//!
//! * `test_early_kickout_reassignment` — induce a real production miss with the
//!   adversarial chunk-skip message and assert the offending slot is reassigned
//!   while the shard keeps producing chunks (liveness).
//! * `slow_test_early_kickout_epoch_sync_bootstrap` — a fresh node epoch-syncs
//!   into a network that ALREADY has an active reassignment and must resolve
//!   chunk producers consistently with the full validators, with no
//!   `ChunkProducerNotInDB` errors.
//! * `slow_test_early_kickout_state_sync_under_active_kickout` — one scenario, two
//!   independently verified properties: a validator state-syncs a newly assigned
//!   shard while a blacklist is active, and the rows it holds deviate from the
//!   plain schedule in exactly the slots they should.
//!
//! All three require `nightly` (feature gate) and `test_features` (adversarial
//! messages, plus the threshold override below).
//!
//! Production trips the blacklist at 100 misses accumulated past a 1000-block
//! start-of-epoch grace, which is ~1100 blocks — far more than a test-loop chain
//! can run. All three therefore shrink both thresholds through
//! `set_early_kickout_thresholds_for_testing` so the gate trips in tens of
//! blocks. The overrides are thread-local with production-constant defaults, so
//! nothing outside these tests is affected; the exact production values
//! (100 / 80% / 1000) stay covered by the epoch-manager unit tests, which never
//! install an override. What these tests prove is the end-to-end wiring, which
//! the thresholds do not affect.

use crate::setup::builder::TestLoopBuilder;
use crate::tests::early_kickout_probe::{
    assert_blacklist_read_everywhere, assert_walk_window, probe_block_region,
};
use crate::tests::sync::state_sync::{
    assert_shard_shuffling_happened, assert_state_synced_for_reassigned_shard,
    get_boundary_accounts,
};
use crate::tests::sync::util::{TEST_EPOCH_SYNC_HORIZON, far_horizon_height};
use crate::utils::account::{
    create_account_id, create_validator_id, create_validators_spec, validators_spec_clients,
};
use crate::utils::node::NodeRunner;
use crate::utils::transactions::{execute_money_transfers, make_accounts};
use borsh::BorshDeserialize;
use near_async::time::Duration;
use near_chain_configs::TrackedShardsConfig;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::NetworkAdversarialMessage;
use near_client::client_actor::AdvProduceChunksMode;
use near_epoch_manager::{
    EarlyKickoutThresholdGuard, EpochManagerAdapter, set_early_kickout_thresholds_for_testing,
};
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{
    AccountId, AccountInfo, Balance, BlockHeight, EpochId, ValidatorInfoIdentifier,
};
use near_primitives::utils::get_block_shard_id;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_store::DBCol;
use std::collections::HashSet;
use std::sync::Arc;

/// Grace window used by every test here, in blocks into the epoch. Small enough that a
/// test-loop chain clears it in seconds, large enough that the first blocks of an
/// epoch still cannot blacklist anyone.
const TEST_EPOCH_GRACE_BLOCKS: u64 = 20;
/// Miss floor used by every test here. A target holding ~half its shard's slots reaches
/// this within ~10 heights of leaving the grace window.
const TEST_MIN_MISSES: u64 = 5;

/// Asserts `EarlyKickout` is enabled for the genesis protocol version, then shrinks the
/// early-kickout gate for the calling thread — which is the thread the whole test-loop
/// chain runs on. Hold the returned guard for the whole test; it restores the production
/// values when dropped, panic or not.
fn shrink_early_kickout_gate(
    min_misses: u64,
    epoch_grace_blocks: u64,
) -> EarlyKickoutThresholdGuard {
    assert!(
        ProtocolFeature::EarlyKickout.enabled(PROTOCOL_VERSION),
        "test requires EarlyKickout enabled for the genesis protocol version"
    );
    set_early_kickout_thresholds_for_testing(Some(min_misses), Some(epoch_grace_blocks))
}

/// A chunk producer on a shard that holds at least one other producer, so blacklisting it
/// leaves a clean replacement. Keeps the all-blacklisted safety valve out of the picture; it
/// would suppress the reassignment these tests are about.
fn pick_target_with_replacement(
    epoch_manager: &Arc<dyn EpochManagerAdapter>,
    epoch_id: &EpochId,
) -> AccountId {
    let epoch_info = epoch_manager.get_epoch_info(epoch_id).unwrap();
    let shard_layout = epoch_manager.get_shard_layout(epoch_id).unwrap();
    let target_id = shard_layout
        .shard_ids()
        .find_map(|shard_id| {
            let index = shard_layout.get_shard_index(shard_id).unwrap();
            let producers = &epoch_info.chunk_producers_settlement()[index];
            (producers.len() >= 2).then(|| producers[0])
        })
        .expect("need a shard with >= 2 chunk producers for a clean replacement");
    epoch_info.get_validator(target_id).account_id().clone()
}

/// Runs the node until `target` is blacklisted on some shard at the node's final head,
/// per `epoch_manager`'s aggregator view. With `pin_epoch`, final heads outside that
/// epoch are ignored, so the wait cannot be satisfied by a stale blacklist from a prior
/// epoch.
fn run_until_target_blacklisted(
    runner: &mut NodeRunner<'_>,
    epoch_manager: &Arc<dyn EpochManagerAdapter>,
    target: &AccountId,
    pin_epoch: Option<EpochId>,
) {
    let epoch_manager = epoch_manager.clone();
    let target = target.clone();
    runner.run_until(
        move |node| {
            let final_head = node.final_head();
            if pin_epoch.is_some_and(|pin| final_head.epoch_id != pin) {
                return false;
            }
            let Ok(epoch_info) = epoch_manager.get_epoch_info(&final_head.epoch_id) else {
                return false;
            };
            let Some(&target_id) = epoch_info.get_validator_id(&target) else {
                return false;
            };
            let Ok(blacklist) =
                epoch_manager.get_chunk_producer_blacklist(&final_head.last_block_hash)
            else {
                return false;
            };
            blacklist.values().any(|excluded| excluded.contains(&target_id))
        },
        Duration::seconds(300),
    );
}

/// Flagship reassignment test.
///
/// Setup: 4 block+chunk producers over 2 shards (balance-shards puts 2
/// producers on each shard, so blacklisting one always leaves a clean
/// replacement and never trips the all-blacklisted safety valve),
/// `kickouts_standard_80_percent`, and an epoch long enough that the induced
/// miss crosses the mid-epoch thresholds well before the epoch boundary.
///
/// We stop one producer's chunk production with the adversarial message; its
/// assigned chunks are missed. Once the anchor is past the grace window and the
/// target has missed at least `TEST_MIN_MISSES` chunks at under 80% production,
/// it is blacklisted on its shard and the chunks it would have produced are
/// reassigned. We assert (a) the miss-induced reassignment (the stored producer
/// for the target's own scheduled slots is a different validator) and (b)
/// liveness (the shard keeps producing chunks after the reassignment).
#[test]
fn test_early_kickout_reassignment() {
    init_test_logger();

    let _thresholds = shrink_early_kickout_gate(TEST_MIN_MISSES, TEST_EPOCH_GRACE_BLOCKS);

    // A 50%-share producer clears the grace and the miss floor within ~40 heights.
    // A 200-block epoch leaves comfortable margin so the reassignment lands
    // mid-epoch, before the standard end-of-epoch kickout.
    let epoch_length = 200;
    let validators_spec = create_validators_spec(4, 0);
    let clients = validators_spec_clients(&validators_spec);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .shard_layout(ShardLayout::multi_shard(2, 1))
        .validators_spec(validators_spec)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .kickouts_standard_80_percent()
        .build_store_for_genesis_protocol_version();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .build();

    let epoch_manager = env.node(0).client().epoch_manager.clone();

    let target_account = {
        let head = env.node(0).head();
        pick_target_with_replacement(&epoch_manager, &head.epoch_id)
    };

    // Stop the target's chunk production. Its assigned chunks start being missed
    // so its production ratio falls below the early-kickout threshold.
    env.runner_for_account(&target_account).send_adversarial_message(
        NetworkAdversarialMessage::AdvProduceChunks(AdvProduceChunksMode::StopProduce),
    );

    // Condition-based wait (no fixed heights): run until the mid-epoch kickout
    // math blacklists the target on some shard, which requires the anchor to be
    // past the grace window with at least `TEST_MIN_MISSES` misses at under 80%.
    run_until_target_blacklisted(&mut env.node_runner(0), &epoch_manager, &target_account, None);

    let trigger_head = env.node(0).head().height;
    let trigger_epoch_id = env.node(0).final_head().epoch_id;

    // The reassignment only materializes in chunks whose grandparent anchor was
    // produced after the blacklist formed. Advance further (still within the same
    // long epoch) so those chunks exist, then assert the reassignment + liveness.
    env.node_runner(0).run_for_number_of_blocks(20);

    let observe = env.node(0);
    assert_eq!(
        observe.head().epoch_id,
        trigger_epoch_id,
        "reassignment window must stay in the triggering epoch"
    );
    let final_head = observe.final_head();
    let epoch_id = final_head.epoch_id;
    let epoch_info = epoch_manager.get_epoch_info(&epoch_id).unwrap();
    let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();
    let target_id = *epoch_info
        .get_validator_id(&target_account)
        .expect("target must still be a validator in the triggering epoch");

    // Locate the target's shard and confirm a replacement remains.
    let target_shard = shard_layout
        .shard_ids()
        .find(|&shard_id| {
            let index = shard_layout.get_shard_index(shard_id).unwrap();
            epoch_info.chunk_producers_settlement()[index].contains(&target_id)
        })
        .expect("target must be a chunk producer");
    let target_shard_index = shard_layout.get_shard_index(target_shard).unwrap();
    assert!(
        epoch_info.chunk_producers_settlement()[target_shard_index].len() >= 2,
        "target shard must retain a non-blacklisted producer (safety-valve guard)"
    );

    let blacklist =
        epoch_manager.get_chunk_producer_blacklist(&final_head.last_block_hash).unwrap();
    assert!(
        blacklist.get(&target_shard).is_some_and(|excluded| excluded.contains(&target_id)),
        "target must be blacklisted on shard {target_shard}"
    );

    let epoch_start = epoch_manager.get_epoch_start_height(&final_head.last_block_hash).unwrap();
    let chain = &observe.client().chain;

    // For the target's own scheduled slots (where the plain schedule picks it),
    // the DB-backed resolver must return a DIFFERENT validator once the
    // grandparent anchor has the target blacklisted. Mirrors the epoch-manager
    // anti-flap unit test, end-to-end over the real chain.
    let mut reassigned_slots = 0u32;
    let mut height = final_head.height;
    while height > epoch_start + 2 && reassigned_slots < 2 {
        let is_target_slot = epoch_info.sample_chunk_producer(&shard_layout, target_shard, height)
            == Some(target_id);
        if is_target_slot {
            if let (Ok(anchor_hash), Ok(prev_hash)) = (
                chain.get_block_hash_by_height(height - 2),
                chain.get_block_hash_by_height(height - 1),
            ) {
                let anchor_blacklist =
                    epoch_manager.get_chunk_producer_blacklist(&anchor_hash).unwrap();
                let anchor_blacklists_target = anchor_blacklist
                    .get(&target_shard)
                    .is_some_and(|excluded| excluded.contains(&target_id));
                if anchor_blacklists_target {
                    let resolved = epoch_manager
                        .get_chunk_producer_info_from_prev_block(&prev_hash, target_shard)
                        .unwrap();
                    assert_ne!(
                        resolved.account_id(),
                        &target_account,
                        "chunk at height {height} on shard {target_shard} must be \
                         reassigned away from the blacklisted producer"
                    );
                    reassigned_slots += 1;
                }
            }
        }
        height -= 1;
    }
    assert!(
        reassigned_slots >= 1,
        "expected at least one miss-induced reassignment of the target's slots"
    );

    // Liveness: after the reassignment the shard keeps producing chunks (does not
    // stall). Every block in a post-reassignment window (whose anchor blacklists
    // the target) must carry the offending shard's chunk. Tolerate skipped block
    // heights (only chunk production is stopped, but stay robust to scheduling
    // changes) while requiring enough blocks for the window to stay meaningful.
    let mut liveness_blocks = 0u32;
    for height in (trigger_head + 3)..=(trigger_head + 15) {
        let Ok(block) = chain.get_block_by_height(height) else {
            continue;
        };
        liveness_blocks += 1;
        assert!(
            block.header().chunk_mask()[target_shard_index],
            "shard chunk missing at height {height} after reassignment (shard stalled)"
        );
    }
    assert!(liveness_blocks >= 10, "liveness window too thin: only {liveness_blocks} blocks found");
}

/// Epoch-sync bootstrap into a network with an ACTIVE reassignment.
///
/// A fresh node bootstraps via epoch sync into a running EarlyKickout network in
/// which a chunk producer is ALREADY blacklisted (its slot reassigned) before the
/// node joins. This exercises the bootstrap path against a network with active
/// kickout and checks that the synced node resolves chunk producers consistently
/// with the full validators, with no `ChunkProducerNotInDB` errors and no
/// divergence.
///
/// Setup uses skewed stakes so one producer dominates its shard's chunk sampling
/// (accumulating misses quickly) while still sharing the shard with a healthy
/// replacement. Standard kickout thresholds are left at 0 so the target is never
/// removed at an epoch boundary; the early-kickout math keeps it blacklisted
/// mid-epoch, so the reassignment recurs every epoch.
///
/// The bootstrap property is checked three ways: (0) immediately after the
/// epoch-sync proof applies, the `EpochStart` row it writes for the synced epoch
/// exists and matches the source — without the fix the test dies here, in
/// seconds, instead of timing out on catch-up; (1) after catch-up, walking the
/// canonical chain from the head back to the epoch-sync-seeded first block,
/// every `ChunkProducers` row matches the source — across the synced epoch this
/// pins the blacklist-activation height and the reassigned producer per shard —
/// and the synced epoch's validator stats (which feed `next_bp_hash`) match the
/// source; and (2) across a post-sync window with the reassignment active the
/// node resolves every shard with no `ChunkProducerNotInDB`, agrees with the
/// source, and reproduces >= 1 real reassignment.
///
/// The 151->152 activation edge is intentionally not exercised here; it is
/// covered by the epoch-manager unit tests and the cold-storage boundary test.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_early_kickout_epoch_sync_bootstrap() {
    init_test_logger();

    let _thresholds = shrink_early_kickout_gate(TEST_MIN_MISSES, TEST_EPOCH_GRACE_BLOCKS);

    let epoch_length = 100;
    let target_account = create_validator_id(0);

    // Skewed stakes: validator0 dominates chunk-producer sampling on its shard
    // (~90% of slots), so it clears the grace and the miss floor within ~30
    // heights of each epoch start, while the shard still holds a healthy
    // replacement.
    let stakes = [1_000_000u128, 100_000, 100_000, 100_000];
    let validators: Vec<AccountInfo> = (0..4)
        .map(|i| {
            let account_id = create_validator_id(i);
            AccountInfo {
                public_key: create_test_signer(account_id.as_str()).public_key(),
                account_id,
                amount: Balance::from_near(stakes[i]),
            }
        })
        .collect();
    let validators_spec = ValidatorsSpec::raw(validators, 4, 4, 0);
    let clients = validators_spec_clients(&validators_spec);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .shard_layout(ShardLayout::multi_shard(2, 1))
        .validators_spec(validators_spec)
        .build();
    // `from_genesis` leaves all kickout thresholds at 0 (no standard kickout).
    let epoch_config_store =
        TestEpochConfigBuilder::from_genesis(&genesis).build_store_for_genesis_protocol_version();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .build();

    let source_em = env.node(0).client().epoch_manager.clone();

    // induce the reassignment BEFORE the fresh node joins, so it syncs into a
    // network that already has an active kickout. StopProduce is permanent, so the
    // target keeps missing and (kickout thresholds at 0 -> never standard-kicked)
    // stays blacklisted in every epoch it accumulates enough misses.
    env.runner_for_account(&target_account).send_adversarial_message(
        NetworkAdversarialMessage::AdvProduceChunks(AdvProduceChunksMode::StopProduce),
    );

    // Run several epochs so (a) a fresh node is beyond the epoch-sync horizon,
    // forcing the far-horizon path, and (b) the target is blacklisted in the current
    // epoch. `far_horizon_height` is the same depth the sync tests use; anything
    // shallower makes the epoch-sync proof itself invalid ("need at least two epochs
    // in all_epochs") and silently degrades this into a header-sync-from-genesis
    // test. The condition wait then guarantees the blacklist has actually formed.
    env.node_runner(0).run_until_head_height(far_horizon_height(epoch_length));
    run_until_target_blacklisted(&mut env.node_runner(0), &source_em, &target_account, None);

    // Add a fresh non-validator node that must bootstrap from genesis while the
    // reassignment is active on the network.
    let new_account = create_account_id("ek_sync_node");
    let node_state = env
        .node_state_builder()
        .account_id(&new_account)
        .config_modifier(|config| {
            config.tracked_shards_config = TrackedShardsConfig::AllShards;
            config.epoch_sync.epoch_sync_horizon_num_epochs = TEST_EPOCH_SYNC_HORIZON;
        })
        .build();
    env.add_node("ek_sync_node", node_state);
    let new_node_idx = env.node_datas.len() - 1;
    let synced_em = env.node(new_node_idx).client().epoch_manager.clone();

    // Wait for the epoch-sync proof to apply: the follower's header head jumps from
    // genesis into the synced epoch (or past it — header sync applies whole batches
    // per event) in the same event that commits the proof's store update.
    {
        let synced_handle = env.node_datas[new_node_idx].client_sender.actor_handle();
        env.test_loop.run_until(
            |data| {
                data.get(&synced_handle).client.chain.header_head().unwrap().height > epoch_length
            },
            Duration::seconds(200),
        );
    }

    // Assertion 0: the synced epoch's `EpochStart` row. It is the min-height row on
    // the follower: a fresh node has none before epoch sync (genesis writes none),
    // `apply_validated_proof` writes exactly one, and header sync only adds rows for
    // later epochs. The `height > 0` filter keeps the derivation correct even if a
    // genesis row ever appears. Without the fix there is no row at all and the test
    // fails HERE, in seconds, instead of timing out on catch-up.
    let (synced_epoch_id, synced_epoch_start) = {
        let synced_store = env.node(new_node_idx).store();
        let (epoch_id, height) = synced_store
            .iter(DBCol::EpochStart)
            .map(|(key, value)| {
                let epoch_id = EpochId(CryptoHash::try_from(&key[..]).unwrap());
                let height = BlockHeight::try_from_slice(&value).unwrap();
                (epoch_id, height)
            })
            .filter(|(_, height)| *height > 0)
            .min_by_key(|(_, height)| *height)
            .expect("epoch sync wrote no EpochStart row for the synced epoch");
        let source_height: Option<BlockHeight> =
            env.node(0).store().get_ser(DBCol::EpochStart, epoch_id.as_ref());
        assert_eq!(
            source_height,
            Some(height),
            "seeded EpochStart height disagrees with the source for epoch {epoch_id:?}"
        );
        (epoch_id, height)
    };
    // If epoch sync silently degraded into header sync from genesis, the min-height row
    // would be an early epoch's row (written by header processing) and the assertions
    // below would target the wrong epoch. Fail fast here instead.
    assert!(
        synced_epoch_start > epoch_length,
        "seeded epoch starts at {synced_epoch_start} - epoch sync did not run"
    );

    // Bring the fresh node to the network tip (epoch sync -> header -> state ->
    // block). The network keeps advancing (and the target keeps missing) while it
    // bootstraps, so the node syncs into a live, kicking network.
    {
        let synced_handle = env.node_datas[new_node_idx].client_sender.actor_handle();
        let source_handle = env.node_datas[0].client_sender.actor_handle();
        env.test_loop.run_until(
            |data| {
                data.get(&synced_handle).client.chain.head().unwrap().height
                    == data.get(&source_handle).client.chain.head().unwrap().height
            },
            Duration::seconds(200),
        );
    }

    // Confirm it really epoch-synced (skipped old epochs) rather than
    // block-syncing every block from genesis.
    let synced_tail = env.node(new_node_idx).tail();
    assert!(
        synced_tail > epoch_length,
        "synced node tail {synced_tail} should be past the first epoch (epoch sync skips blocks)"
    );

    // Assertion 1: walk the canonical chain from the catch-up head back to the
    // epoch-sync-seeded first block, comparing every `ChunkProducers` row against
    // the source. "Node joined" alone is too weak: attribution can diverge
    // (crediting the kicked-out producer for chunks its replacement made) well
    // before a wrong `next_bp_hash` surfaces. Across the synced epoch, row parity
    // pins the blacklist-activation height and the reassigned producer per shard;
    // later epochs cover each re-activation up to the head. The walk uses headers
    // (not the height index): header-synced heights have no canonical-index entry.
    // This must stay the FIRST assertion after catch-up — retention GC deletes
    // header-only hashes' rows once their heights leave the GC window, and the
    // synced epoch has about one epoch of margin here.
    {
        let synced = env.node(new_node_idx);
        let source = env.node(0);
        let final_head = synced.final_head();
        let synced_chain = &synced.client().chain;
        let synced_epoch_info = synced_em.get_epoch_info(&synced_epoch_id).unwrap();

        // Heights walked inside the synced epoch, for the offset check below.
        let mut synced_epoch_heights = HashSet::new();
        let mut reassigned_rows = 0u32;
        let mut hash = final_head.last_block_hash;
        loop {
            let header = synced_chain.get_block_header(&hash).unwrap_or_else(|e| {
                panic!("header walk broke at {hash} before the synced epoch start: {e:?}")
            });
            let height = header.height();
            assert!(
                height >= synced_epoch_start,
                "walk stepped below the synced epoch start {synced_epoch_start} without \
                 landing on it (reached {height}); wrong fork or missing seeded block"
            );
            let in_synced_epoch = header.epoch_id() == &synced_epoch_id;
            let shard_layout = synced_em.get_shard_layout(header.epoch_id()).unwrap();
            for shard_id in shard_layout.shard_ids() {
                let key = get_block_shard_id(&hash, shard_id);
                let synced_row: Option<ValidatorStake> =
                    synced.store().get_ser(DBCol::ChunkProducers, &key);
                let source_row: Option<ValidatorStake> =
                    source.store().get_ser(DBCol::ChunkProducers, &key);
                assert!(
                    synced_row.is_some(),
                    "ChunkProducers row missing on the synced node at height {height} shard \
                     {shard_id} (a GC'd range means this assertion ran too late in the test)"
                );
                assert_eq!(
                    synced_row, source_row,
                    "ChunkProducers row diverged from the source at height {height} shard \
                     {shard_id}"
                );
                // Reassignment pin: the TARGET's own slots deviating from the plain
                // schedule proves the blacklist was active for that anchor; only the
                // target misses chunks, so healthy producers' slots must never deviate
                // (negative path). Rows sample the producer at anchor height + 2, so
                // only anchors whose sample height stays inside the synced epoch count
                // (the walk visits height + 2 before height).
                if in_synced_epoch && synced_epoch_heights.contains(&(height + 2)) {
                    let planned = synced_epoch_info
                        .sample_chunk_producer(&shard_layout, shard_id, height + 2)
                        .map(|id| synced_epoch_info.get_validator(id).account_id().clone());
                    let stored = synced_row.as_ref().map(|stake| stake.account_id());
                    if planned.as_ref() == Some(&target_account) {
                        if stored != planned.as_ref() {
                            reassigned_rows += 1;
                        }
                    } else {
                        assert_eq!(
                            stored,
                            planned.as_ref(),
                            "healthy producer's slot reassigned at height {} shard {shard_id}",
                            height + 2
                        );
                    }
                }
            }
            if in_synced_epoch {
                synced_epoch_heights.insert(height);
            }
            if height == synced_epoch_start {
                assert!(
                    in_synced_epoch,
                    "block at the synced epoch start height belongs to {:?}, expected {:?}",
                    header.epoch_id(),
                    synced_epoch_id
                );
                break;
            }
            hash = *header.prev_hash();
        }
        assert!(
            synced_epoch_heights.len() as u64 >= epoch_length / 2,
            "synced-epoch coverage too thin: {} headers walked",
            synced_epoch_heights.len()
        );
        assert!(
            reassigned_rows >= 1,
            "no blacklist-driven reassignment found in the synced epoch's rows \
             ({} headers walked)",
            synced_epoch_heights.len()
        );

        // Aggregator-derived validator stats for the SYNCED epoch: the
        // produced/expected stats feed rewards and thus `next_bp_hash`. On the
        // follower, `get_validator_info(EpochId)` resolves the epoch start through
        // the `EpochStart` row that only exists because epoch sync now writes it.
        let synced_info =
            synced_em.get_validator_info(ValidatorInfoIdentifier::EpochId(synced_epoch_id)).expect(
                "synced node must compute validator info for the synced epoch \
                 (EpochOutOfBounds means the seeded EpochStart row is missing)",
            );
        let source_info = source_em
            .get_validator_info(ValidatorInfoIdentifier::EpochId(synced_epoch_id))
            .unwrap();
        assert_eq!(
            synced_info.current_validators, source_info.current_validators,
            "synced-epoch validator stats diverged between synced node and source"
        );
        // Raw epoch-summary parity: `current_validators` flattens the summary; the
        // raw row also covers kickouts, proposals and stats wholesale.
        let synced_summary = synced
            .store()
            .get(DBCol::EpochValidatorInfo, synced_epoch_id.as_ref())
            .map(|slice| slice.to_vec());
        let source_summary = source
            .store()
            .get(DBCol::EpochValidatorInfo, synced_epoch_id.as_ref())
            .map(|slice| slice.to_vec());
        assert!(synced_summary.is_some(), "synced node has no epoch summary for the synced epoch");
        assert_eq!(
            synced_summary, source_summary,
            "EpochValidatorInfo summary diverged for the synced epoch"
        );
        // Live-aggregator parity at the catch-up head. The head epoch's `EpochStart`
        // row comes from normal header processing, not the fix; this complements the
        // synced-epoch summary check above.
        let head_info = synced_em
            .get_validator_info(ValidatorInfoIdentifier::BlockHash(final_head.last_block_hash))
            .unwrap();
        let source_head_info = source_em
            .get_validator_info(ValidatorInfoIdentifier::BlockHash(final_head.last_block_hash))
            .unwrap();
        assert_eq!(
            head_info.current_validators, source_head_info.current_validators,
            "aggregator-derived validator stats diverged at the catch-up head"
        );
    }

    // Let the follower live through a FRESH epoch from its start, so its aggregator
    // covers that whole epoch (matching the source's; avoids the partial
    // mid-epoch-sync aggregator artifact). Capture that epoch, then wait for the
    // persistent miss to re-blacklist the target WITHIN it (finalized). Pinning to
    // `observe_epoch` is essential: the trailing final_head still sits in the prior
    // epoch (where the target was already blacklisted) right after the boundary, so
    // an unpinned wait would return immediately, before this epoch clears its own
    // grace window. Blacklist observed on the full-aggregator source.
    env.node_runner(new_node_idx).run_until_new_epoch();
    let observe_epoch = env.node(new_node_idx).head().epoch_id;
    run_until_target_blacklisted(
        &mut env.node_runner(new_node_idx),
        &source_em,
        &target_account,
        Some(observe_epoch),
    );
    // Post-blacklist runway so many of the target's own slots have a grandparent
    // anchor that blacklists it (still within the same epoch: ~30 + 20 < 100).
    env.node_runner(new_node_idx).run_for_number_of_blocks(20);

    // Assertion 2: scan the current (fully-processed) epoch on the FOLLOWER for the
    // target's own scheduled slots whose grandparent anchor blacklists it (blacklist
    // observed on the full-aggregator source, never the follower's partial one). For
    // each, the follower's DB-backed resolver must (a) agree with the source and (b)
    // return a DIFFERENT validator than the blacklisted target. Mirrors Test A's
    // proven scan, cross-checked follower-vs-source.
    let synced = env.node(new_node_idx);
    let synced_head = synced.head();
    let epoch_id = synced_head.epoch_id;
    let epoch_start = synced_em.get_epoch_start_height(&synced_head.last_block_hash).unwrap();
    let shard_layout = synced_em.get_shard_layout(&epoch_id).unwrap();
    let source_epoch_info = source_em.get_epoch_info(&epoch_id).unwrap();
    let target_id = *source_epoch_info
        .get_validator_id(&target_account)
        .expect("target must be a validator in the observed epoch");
    let target_shard = shard_layout
        .shard_ids()
        .find(|&shard_id| {
            let index = shard_layout.get_shard_index(shard_id).unwrap();
            source_epoch_info.chunk_producers_settlement()[index].contains(&target_id)
        })
        .expect("target must be a chunk producer");
    let synced_chain = &synced.client().chain;

    let mut reassigned = 0u32;
    let mut target_slots = 0u32;
    let mut blacklisting_anchors = 0u32;
    let mut height = synced_head.height;
    while height > epoch_start + 2 && reassigned < 2 {
        let is_target_slot =
            source_epoch_info.sample_chunk_producer(&shard_layout, target_shard, height)
                == Some(target_id);
        if is_target_slot {
            target_slots += 1;
            if let (Ok(anchor_hash), Ok(prev_hash)) = (
                synced_chain.get_block_hash_by_height(height - 2),
                synced_chain.get_block_hash_by_height(height - 1),
            ) {
                let anchor_blacklist =
                    source_em.get_chunk_producer_blacklist(&anchor_hash).unwrap();
                if anchor_blacklist
                    .get(&target_shard)
                    .is_some_and(|excluded| excluded.contains(&target_id))
                {
                    blacklisting_anchors += 1;
                    let synced_resolved = synced_em
                        .get_chunk_producer_info_from_prev_block(&prev_hash, target_shard)
                        .unwrap_or_else(|e| {
                            panic!(
                                "synced node failed to resolve chunk producer \
                                 (shard {target_shard}, height {height}): {e:?}"
                            )
                        });
                    let source_resolved = source_em
                        .get_chunk_producer_info_from_prev_block(&prev_hash, target_shard)
                        .unwrap();
                    assert_eq!(
                        synced_resolved.account_id(),
                        source_resolved.account_id(),
                        "synced node diverged from source at height {height} shard {target_shard}"
                    );
                    assert_ne!(
                        synced_resolved.account_id(),
                        &target_account,
                        "chunk at height {height} shard {target_shard} must be reassigned \
                         away from the blacklisted target"
                    );
                    reassigned += 1;
                }
            }
        }
        height -= 1;
    }
    assert!(
        reassigned >= 1,
        "synced node must reproduce at least one blacklist-driven reassignment \
         (epoch_start={epoch_start}, head={}, target_slots={target_slots}, \
          blacklisting_anchors={blacklisting_anchors})",
        synced_head.height
    );
}

/// State sync under an ACTIVE early kickout.
///
/// Two independently verified properties in one scenario: shard shuffling forces a validator
/// to state-sync a newly assigned shard while a producer is blacklisted, and the probe checks
/// the `DBCol::ChunkProducers` rows that validator holds.
///
/// This is the half `tests::sync::early_kickout_sync` cannot reach: there every anchor is
/// inside the grace window, so the blacklist is always empty and the reassignment write path
/// never runs. Here the grace is shrunk, and the blacklist has to rebuild from each epoch's own
/// stats since it resets at every boundary.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_early_kickout_state_sync_under_active_kickout() {
    init_test_logger();

    // Tighter than the other two tests: this one needs many boundaries (see below), so epochs
    // must be short, and every epoch has to clear its own grace window.
    const GRACE_BLOCKS: u64 = 5;
    const MIN_MISSES: u64 = 3;
    let _thresholds = shrink_early_kickout_gate(MIN_MISSES, GRACE_BLOCKS);

    // Fits grace, miss accumulation and a post-blacklist runway in one epoch.
    let epoch_length = 30;
    // 2 producers per shard, needed twice over: the stopped target always has a healthy
    // replacement, so the safety valve never fires and the shard never stalls; and the schedule
    // rotates with height, so the anchor-offset half of the oracle can bite.
    let validators_spec = create_validators_spec(6, 0);
    let clients = validators_spec_clients(&validators_spec);
    let accounts = make_accounts(10);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .shard_layout(ShardLayout::multi_shard_custom(get_boundary_accounts(3), 1))
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, Balance::from_near(10_000))
        .build();
    // Deliberately no `kickouts_standard_80_percent()`: `from_genesis` leaves the end-of-epoch
    // thresholds at 0, so the stopped target survives each boundary and keeps missing chunks.
    // With standard kickout on it would be removed after the first epoch, leaving nobody to
    // rebuild the blacklist. The mid-epoch math ignores those thresholds anyway.
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build_store_for_genesis_protocol_version();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients.clone())
        .build();

    // Create nontrivial user state before the multi-epoch scenario.
    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();

    let epoch_manager = env.node(0).client().epoch_manager.clone();
    let target_account = {
        let head = env.node(0).head();
        pick_target_with_replacement(&epoch_manager, &head.epoch_id)
    };
    env.runner_for_account(&target_account).send_adversarial_message(
        NetworkAdversarialMessage::AdvProduceChunks(AdvProduceChunksMode::StopProduce),
    );

    run_until_target_blacklisted(&mut env.node_runner(0), &epoch_manager, &target_account, None);

    // One boundary would be enough for the state sync, but four are needed for the shuffle to
    // provably move a shard: the genesis and first epochs carry an all-zero `rng_seed` so the
    // earliest boundaries do not redraw at all, and a redraw over a few producer groups can
    // return the identity permutation, which reads as "shuffling didn't happen" (measured).
    let mut probe_epoch = env.node(0).head().epoch_id;
    for boundary in 1..=4 {
        assert_eq!(
            env.node(0).final_head().epoch_id,
            probe_epoch,
            "boundary {boundary}: head and final head disagree on the epoch, so \
             `run_until_new_epoch` would skip a whole epoch instead of crossing the next \
             boundary"
        );
        env.node_runner(0).run_until_new_epoch();
        probe_epoch = env.node(0).head().epoch_id;
        // Pinning to the new epoch is essential: right after the boundary the trailing final
        // head still sits in the previous one, where the target is already blacklisted, so an
        // unpinned wait would return before this epoch clears its own grace window.
        run_until_target_blacklisted(
            &mut env.node_runner(0),
            &epoch_manager,
            &target_account,
            Some(probe_epoch),
        );
    }

    // Runway, so rows anchored after this epoch's blacklist formed exist to probe. Still inside
    // the same epoch (5 grace + a few misses + 10 < 30).
    env.node_runner(0).run_for_number_of_blocks(10);
    assert_eq!(
        env.node(0).head().epoch_id,
        probe_epoch,
        "probe window left the active-blacklist epoch"
    );

    assert_shard_shuffling_happened(&env, &clients);
    let state_synced = assert_state_synced_for_reassigned_shard(&env, &clients);
    tracing::info!(target: "test", ?state_synced, "validators that state-synced a reassigned shard");

    for idx in state_synced {
        let label = format!("active-kickout probe on node {idx}");
        let walk = probe_block_region(&env.node(idx), epoch_length);
        assert_walk_window(&walk, epoch_length / 2, &label);
        assert_blacklist_read_everywhere(&walk, &label);
        // Both directions: reassigned rows prove this node exercised the active-blacklist
        // row-seeding path, plain rows prove the probe is not blanket-accepting deviation. Each
        // side is recomputed from this node's own accessor, never from another node's copy of
        // the row.
        assert!(
            walk.reassigned_rows > 0,
            "{label}: no row was reassigned away from a blacklisted plain pick, so the \
             active blacklist was never exercised ({walk:?})"
        );
        assert!(
            walk.plain_rows > 0,
            "{label}: no row followed the plain schedule, so the probe cannot tell a \
             reassignment from a blanket deviation ({walk:?})"
        );
    }
}
