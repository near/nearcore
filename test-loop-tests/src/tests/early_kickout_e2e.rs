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
//! * `test_early_kickout_epoch_sync_bootstrap` — a fresh node epoch-syncs into a
//!   network that ALREADY has an active reassignment and must resolve chunk
//!   producers consistently with the full validators, with no
//!   `ChunkProducerNotInDB` errors.
//!
//! Both require `nightly` (feature gate) and `test_features` (adversarial
//! messages).

use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::{
    create_account_id, create_validator_id, create_validators_spec, validators_spec_clients,
};
use near_async::time::Duration;
use near_chain_configs::TrackedShardsConfig;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::NetworkAdversarialMessage;
use near_client::client_actor::AdvProduceChunksMode;
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{AccountInfo, Balance};
use near_primitives::utils::get_block_shard_id;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_store::DBCol;

/// Test A — flagship reassignment test.
///
/// Setup: 4 block+chunk producers over 2 shards (balance-shards puts 2
/// producers on each shard, so blacklisting one always leaves a clean
/// replacement and never trips the all-blacklisted safety valve),
/// `kickouts_standard_80_percent`, and a long epoch so the induced miss crosses
/// the mid-epoch thresholds well before the epoch boundary.
///
/// We stop one producer's chunk production with the adversarial message; its
/// assigned chunks are missed. Once it crosses
/// `EARLY_KICKOUT_MINIMUM_OBSERVED_BLOCKS` (50) and `EARLY_KICKOUT_MIN_MISSES`
/// (20) it is blacklisted on its shard, and the chunks it would have produced
/// are reassigned. We assert (a) the miss-induced reassignment (the stored
/// producer for the target's own scheduled slots is a different validator) and
/// (b) liveness (the shard keeps producing chunks after the reassignment).
#[test]
fn test_early_kickout_reassignment() {
    init_test_logger();

    assert!(
        ProtocolFeature::EarlyKickout.enabled(PROTOCOL_VERSION),
        "test requires EarlyKickout enabled for the genesis protocol version"
    );

    // Long epoch: a 50%-share producer needs ~100 heights to reach the
    // observed-blocks floor. A 200-block epoch leaves comfortable margin so the
    // reassignment lands mid-epoch, before the standard end-of-epoch kickout.
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

    // Pick a chunk producer on a shard that has at least one other producer, so
    // blacklisting it leaves a clean replacement (avoids the all-blacklisted
    // safety valve, which would suppress reassignment).
    let target_account = {
        let head = env.node(0).head();
        let epoch_info = epoch_manager.get_epoch_info(&head.epoch_id).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(&head.epoch_id).unwrap();
        let target_id = shard_layout
            .shard_ids()
            .find_map(|shard_id| {
                let index = shard_layout.get_shard_index(shard_id).unwrap();
                let producers = &epoch_info.chunk_producers_settlement()[index];
                (producers.len() >= 2).then(|| producers[0])
            })
            .expect("need a shard with >= 2 chunk producers for a clean replacement");
        epoch_info.get_validator(target_id).account_id().clone()
    };

    // Stop the target's chunk production. Its assigned chunks start being missed
    // so its production ratio falls below the early-kickout threshold.
    env.runner_for_account(&target_account).send_adversarial_message(
        NetworkAdversarialMessage::AdvProduceChunks(AdvProduceChunksMode::StopProduce),
    );

    // Condition-based wait (no fixed heights): run until the mid-epoch kickout
    // math blacklists the target on some shard, which requires crossing both
    // EARLY_KICKOUT_MINIMUM_OBSERVED_BLOCKS (50) and EARLY_KICKOUT_MIN_MISSES (20).
    {
        let em = epoch_manager.clone();
        let target = target_account.clone();
        env.node_runner(0).run_until(
            move |node| {
                let final_head = node.final_head();
                let Ok(epoch_info) = em.get_epoch_info(&final_head.epoch_id) else {
                    return false;
                };
                let Some(&target_id) = epoch_info.get_validator_id(&target) else {
                    return false;
                };
                let Ok(blacklist) = em.get_chunk_producer_blacklist(&final_head.last_block_hash)
                else {
                    return false;
                };
                blacklist.values().any(|excluded| excluded.contains(&target_id))
            },
            Duration::seconds(300),
        );
    }

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
    // stall). Every height in a post-reassignment window (whose anchor blacklists
    // the target) must carry the offending shard's chunk.
    for height in (trigger_head + 3)..=(trigger_head + 15) {
        let block = chain.get_block_by_height(height).unwrap();
        assert!(
            block.header().chunk_mask()[target_shard_index],
            "shard chunk missing at height {height} after reassignment (shard stalled)"
        );
    }
}

/// Test C — epoch-sync bootstrap into a network with an ACTIVE reassignment.
///
/// A fresh node bootstraps via epoch sync into a running EarlyKickout network in
/// which a chunk producer is ALREADY blacklisted (its slot reassigned) before the
/// node joins. This exercises the bootstrap path against a network with active
/// kickout and checks that the synced node resolves chunk producers consistently
/// with the full validators, with no `ChunkProducerNotInDB` errors and no
/// divergence.
///
/// Setup uses skewed stakes so one producer dominates its shard's chunk sampling
/// (crossing the observed-blocks floor quickly) while still sharing the shard with
/// a healthy replacement. Standard kickout thresholds are left at 0 so the target
/// is never removed at an epoch boundary; the early-kickout math keeps it
/// blacklisted mid-epoch, so the reassignment recurs every epoch.
///
/// Note: the epoch-sync-seeded row is always the synced epoch's FIRST block, whose
/// blacklist is provably empty (a first-of-epoch anchor cannot reach the
/// observed-blocks floor), so that seeded row equals the plain schedule by design.
/// The meaningful bootstrap property is checked two ways: (1) the seeded
/// first-block rows exist and match the source (no missing row), and (2) after the
/// node catches up, across a post-sync window with the reassignment active it
/// resolves every shard with no `ChunkProducerNotInDB`, agrees with the source,
/// and reproduces >= 1 real reassignment.
///
/// The 151->152 activation edge is intentionally not exercised here (see Test A
/// rationale); it is covered by the epoch-manager unit tests and the cold-storage
/// boundary test.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_early_kickout_epoch_sync_bootstrap() {
    init_test_logger();

    assert!(
        ProtocolFeature::EarlyKickout.enabled(PROTOCOL_VERSION),
        "test requires EarlyKickout enabled for the genesis protocol version"
    );

    let epoch_length = 100;
    let target_account = create_validator_id(0);

    // Skewed stakes: validator0 dominates chunk-producer sampling on its shard
    // (~90% of slots), so it crosses the observed-blocks floor in ~55 heights,
    // while the shard still holds a healthy replacement.
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

    // Run several epochs so (a) a fresh node is beyond the epoch-sync horizon (2
    // epochs), forcing the far-horizon path, and (b) the target is blacklisted in
    // the current epoch. The head-height advance gets us past 3 epochs; the
    // condition wait then guarantees the blacklist has actually formed.
    env.node_runner(0).run_until_head_height(3 * epoch_length);
    {
        let em = source_em.clone();
        let target = target_account.clone();
        env.node_runner(0).run_until(
            move |node| {
                let final_head = node.final_head();
                let Ok(epoch_info) = em.get_epoch_info(&final_head.epoch_id) else {
                    return false;
                };
                let Some(&target_id) = epoch_info.get_validator_id(&target) else {
                    return false;
                };
                let Ok(blacklist) = em.get_chunk_producer_blacklist(&final_head.last_block_hash)
                else {
                    return false;
                };
                blacklist.values().any(|excluded| excluded.contains(&target_id))
            },
            Duration::seconds(300),
        );
    }

    // Add a fresh non-validator node that must bootstrap from genesis while the
    // reassignment is active on the network.
    let new_account = create_account_id("ek_sync_node");
    let node_state = env
        .node_state_builder()
        .account_id(&new_account)
        .config_modifier(|config| {
            config.tracked_shards_config = TrackedShardsConfig::AllShards;
            config.epoch_sync.epoch_sync_horizon_num_epochs = 2;
        })
        .build();
    env.add_node("ek_sync_node", node_state);
    let new_node_idx = env.node_datas.len() - 1;
    let synced_em = env.node(new_node_idx).client().epoch_manager.clone();

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

    // Assertion 1: the rows seeded by `seed_chunk_producers_after_epoch_sync` for
    // the synced epoch's first block exist on the fresh node and match the source
    // validator (no missing row). The first block of any epoch always has an empty
    // blacklist, so this row equals the plain schedule.
    {
        let synced = env.node(new_node_idx);
        let source = env.node(0);
        let synced_head = synced.head();
        let epoch_start = synced_em.get_epoch_start_height(&synced_head.last_block_hash).unwrap();
        let shard_layout = synced_em.get_shard_layout(&synced_head.epoch_id).unwrap();
        let first_block_hash = synced.client().chain.get_block_hash_by_height(epoch_start).unwrap();
        for shard_id in shard_layout.shard_ids() {
            let key = get_block_shard_id(&first_block_hash, shard_id);
            let synced_row: Option<ValidatorStake> =
                synced.store().get_ser(DBCol::ChunkProducers, &key);
            let source_row: Option<ValidatorStake> =
                source.store().get_ser(DBCol::ChunkProducers, &key);
            let synced_row = synced_row.unwrap_or_else(|| {
                panic!(
                    "epoch-sync-seeded ChunkProducers row missing on synced node for shard {shard_id}"
                )
            });
            assert_eq!(
                Some(&synced_row),
                source_row.as_ref(),
                "epoch-sync-seeded row for the first block mismatches the source on shard {shard_id}"
            );
        }
    }

    // Let the follower live through a FRESH epoch from its start, so its aggregator
    // covers that whole epoch (matching the source's; avoids the partial
    // mid-epoch-sync aggregator artifact). Capture that epoch, then wait for the
    // persistent miss to re-blacklist the target WITHIN it (finalized). Pinning to
    // `observe_epoch` is essential: the trailing final_head still sits in the prior
    // epoch (where the target was already blacklisted) right after the boundary, so
    // an unpinned wait would return immediately, before this epoch crosses the
    // observed-blocks floor. Blacklist observed on the full-aggregator source.
    env.node_runner(new_node_idx).run_until_new_epoch();
    let observe_epoch = env.node(new_node_idx).head().epoch_id;
    {
        let em = source_em.clone();
        let target = target_account.clone();
        env.node_runner(new_node_idx).run_until(
            move |node| {
                let final_head = node.final_head();
                if final_head.epoch_id != observe_epoch {
                    return false;
                }
                let Ok(epoch_info) = em.get_epoch_info(&final_head.epoch_id) else {
                    return false;
                };
                let Some(&target_id) = epoch_info.get_validator_id(&target) else {
                    return false;
                };
                let Ok(blacklist) = em.get_chunk_producer_blacklist(&final_head.last_block_hash)
                else {
                    return false;
                };
                blacklist.values().any(|excluded| excluded.contains(&target_id))
            },
            Duration::seconds(300),
        );
    }
    // Post-blacklist runway so many of the target's own slots have a grandparent
    // anchor that blacklists it (still within the same epoch: floor ~55 + 20 < 100).
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
