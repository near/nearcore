//! Tests for the early-kickout blacklist math (`compute_chunk_producer_blacklist`)
//! and the gated `get_chunk_producer_blacklist` accessor. The math is pure with no
//! production callers; these tests exercise the math directly and the accessor
//! end-to-end (gate + boundary reset + enabled path).

use crate::reward_calculator::NUM_NS_IN_SECOND;
use crate::test_utils::{DEFAULT_TOTAL_SUPPLY, record_block, setup_default_epoch_manager};
use crate::{
    EpochManager, EpochManagerAdapter, EpochManagerHandle, compute_chunk_producer_blacklist,
};
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::epoch_info::EpochInfo;
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::shard_layout::ShardLayout;
use near_primitives::stateless_validation::chunk_endorsements_bitmap::ChunkEndorsementsBitmap;
use near_primitives::types::{Balance, ChunkStats, ShardId, ValidatorId};
use near_primitives::version::PROTOCOL_VERSION;
use std::collections::{HashMap, HashSet};

#[cfg(feature = "nightly")]
use crate::metrics::EARLY_KICKOUT_SLOT_REASSIGNED;

const STAKE: Balance = Balance::from_yoctonear(1_000_000);

/// Builds a single-shard `EpochInfo` with `num_producers` chunk producers (ids
/// `0..num_producers`) and returns it alongside the layout and the shard id.
fn single_shard_epoch(num_producers: u64) -> (EpochInfo, ShardLayout, ShardId) {
    let accounts: Vec<_> =
        (0..num_producers).map(|i| (format!("test{i}").parse().unwrap(), STAKE)).collect();
    let settlement: Vec<ValidatorId> = (0..num_producers).collect();
    let shard_layout = ShardLayout::single_shard();
    let shard_id = shard_layout.shard_ids().next().unwrap();
    let epoch_info = crate::test_utils::epoch_info(
        0,
        accounts,
        settlement.clone(),
        vec![settlement],
        PROTOCOL_VERSION,
        shard_layout.clone(),
    );
    (epoch_info, shard_layout, shard_id)
}

/// Convenience: builds a `shard_tracker` with a single shard from `(validator_id,
/// produced, expected)` triples.
fn tracker(
    shard_id: ShardId,
    stats: &[(ValidatorId, u64, u64)],
) -> HashMap<ShardId, HashMap<ValidatorId, ChunkStats>> {
    let inner: HashMap<ValidatorId, ChunkStats> = stats
        .iter()
        .map(|&(id, produced, expected)| (id, ChunkStats::new_with_production(produced, expected)))
        .collect();
    HashMap::from([(shard_id, inner)])
}

// 1. produced/expected < 80%, missed >= 20, expected >= 50 -> blacklisted.
#[test]
fn blacklist_below_threshold() {
    let (epoch_info, layout, shard_id) = single_shard_epoch(4);
    // id 0: 50/100 = 50% < 80%, missed 50; others healthy.
    let st = tracker(shard_id, &[(0, 50, 100), (1, 100, 100), (2, 100, 100), (3, 100, 100)]);
    let bl = compute_chunk_producer_blacklist(&st, &epoch_info, &layout);
    assert_eq!(bl, HashMap::from([(shard_id, HashSet::from([0]))]));
}

// 2. produced*100 == expected*80 -> NOT blacklisted (strict `<`), missed >= 20.
#[test]
fn blacklist_exactly_at_threshold() {
    let (epoch_info, layout, shard_id) = single_shard_epoch(4);
    // id 0: 320/400 = exactly 80%, missed 80 (>= 20). Strict `<` must exclude it.
    let st = tracker(shard_id, &[(0, 320, 400), (1, 400, 400), (2, 400, 400), (3, 400, 400)]);
    let bl = compute_chunk_producer_blacklist(&st, &epoch_info, &layout);
    assert!(bl.is_empty());
}

// 3. missed < 20 -> not blacklisted regardless of ratio.
#[test]
fn blacklist_under_min_misses() {
    let (epoch_info, layout, shard_id) = single_shard_epoch(4);
    // id 0: 31/50 = 62% < 80% but missed only 19 (< 20); expected >= 50.
    let st = tracker(shard_id, &[(0, 31, 50), (1, 100, 100), (2, 100, 100), (3, 100, 100)]);
    let bl = compute_chunk_producer_blacklist(&st, &epoch_info, &layout);
    assert!(bl.is_empty());
}

// 4. every producer would be blacklisted -> shard omitted (safety valve).
#[test]
fn blacklist_safety_valve_all_producers() {
    let (epoch_info, layout, shard_id) = single_shard_epoch(2);
    let st = tracker(shard_id, &[(0, 0, 100), (1, 0, 100)]);
    let bl = compute_chunk_producer_blacklist(&st, &epoch_info, &layout);
    assert!(bl.is_empty());
}

// 5. lone producer would be blacklisted -> omitted (safety valve).
#[test]
fn blacklist_single_producer_shard() {
    let (epoch_info, layout, shard_id) = single_shard_epoch(1);
    let st = tracker(shard_id, &[(0, 0, 100)]);
    let bl = compute_chunk_producer_blacklist(&st, &epoch_info, &layout);
    assert!(bl.is_empty());
}

// 6. expected < 50 at 0% production -> not blacklisted (sample-size guard).
#[test]
fn blacklist_minimum_observed_blocks() {
    let (epoch_info, layout, shard_id) = single_shard_epoch(2);
    // id 0: 0/49, below the observed-blocks floor.
    let st = tracker(shard_id, &[(0, 0, 49), (1, 100, 100)]);
    let bl = compute_chunk_producer_blacklist(&st, &epoch_info, &layout);
    assert!(bl.is_empty());
}

// 7. endorsement-only entries are ignored; producers judged on production only.
#[test]
fn blacklist_ignores_endorsement_only_entries() {
    let (epoch_info, layout, shard_id) = single_shard_epoch(3);
    let mut inner = HashMap::new();
    // (a) endorser-only validator NOT in settlement (id 3): high endorsement, zero
    // production. Must never be a candidate.
    inner.insert(3, ChunkStats::new(0, 0, 1000, 1000));
    // (b) settlement producer (id 0) with high endorsement but failing production.
    inner.insert(0, ChunkStats::new(50, 100, 1000, 1000));
    inner.insert(1, ChunkStats::new_with_production(100, 100));
    inner.insert(2, ChunkStats::new_with_production(100, 100));
    let st = HashMap::from([(shard_id, inner)]);
    let bl = compute_chunk_producer_blacklist(&st, &epoch_info, &layout);
    assert_eq!(bl, HashMap::from([(shard_id, HashSet::from([0]))]));
}

// 8. all producers above threshold -> empty map.
#[test]
fn blacklist_empty_when_healthy() {
    let (epoch_info, layout, shard_id) = single_shard_epoch(3);
    let st = tracker(shard_id, &[(0, 100, 100), (1, 96, 100), (2, 100, 100)]);
    let bl = compute_chunk_producer_blacklist(&st, &epoch_info, &layout);
    assert!(bl.is_empty());
}

// 9. two shards, independent blacklists.
#[test]
fn blacklist_multi_shard_independent() {
    let num_producers = 3u64;
    let accounts: Vec<_> =
        (0..num_producers).map(|i| (format!("test{i}").parse().unwrap(), STAKE)).collect();
    let settlement: Vec<ValidatorId> = (0..num_producers).collect();
    let shard_layout = ShardLayout::multi_shard(2, 0);
    let shard_ids: Vec<ShardId> = shard_layout.shard_ids().collect();
    let epoch_info = crate::test_utils::epoch_info(
        0,
        accounts,
        settlement.clone(),
        vec![settlement.clone(), settlement],
        PROTOCOL_VERSION,
        shard_layout.clone(),
    );
    // shard 0: id 0 fails. shard 1: all healthy.
    let mut st = HashMap::new();
    st.insert(
        shard_ids[0],
        HashMap::from([
            (0u64, ChunkStats::new_with_production(0, 100)),
            (1, ChunkStats::new_with_production(100, 100)),
            (2, ChunkStats::new_with_production(100, 100)),
        ]),
    );
    st.insert(
        shard_ids[1],
        HashMap::from([
            (0u64, ChunkStats::new_with_production(100, 100)),
            (1, ChunkStats::new_with_production(100, 100)),
            (2, ChunkStats::new_with_production(100, 100)),
        ]),
    );
    let bl = compute_chunk_producer_blacklist(&st, &epoch_info, &shard_layout);
    assert_eq!(bl, HashMap::from([(shard_ids[0], HashSet::from([0]))]));
}

// --- Accessor tests (end-to-end through EpochManagerHandle) ---

/// Records a block at `cur` with an explicit per-shard `chunk_mask` (true =
/// produced, false = missed). Mirrors `record_block_with_version` but lets the
/// caller control the chunk mask so we can synthesize miss-heavy stats.
fn record_block_with_mask(
    em: &mut EpochManager,
    prev: CryptoHash,
    cur: CryptoHash,
    height: u64,
    chunk_mask: Vec<bool>,
) {
    let epoch_id = em.get_epoch_id(&prev).unwrap();
    let shard_layout = em.get_shard_layout(&epoch_id).unwrap();
    // A missed chunk (mask == false) must carry an EMPTY endorsement bitmap for that
    // shard; only produced chunks include endorsements.
    let chunk_endorsements = ChunkEndorsementsBitmap::from_endorsements(
        shard_layout
            .shard_ids()
            .enumerate()
            .map(|(shard_index, shard_id)| {
                if !chunk_mask[shard_index] {
                    return vec![];
                }
                let assignments =
                    em.get_chunk_validator_assignments(&epoch_id, shard_id, height).unwrap();
                vec![true; assignments.assignments().iter().len()]
            })
            .collect(),
    );
    em.record_block_info(
        BlockInfo::new(
            cur,
            height,
            height.saturating_sub(2),
            prev,
            prev,
            vec![],
            chunk_mask,
            DEFAULT_TOTAL_SUPPLY,
            PROTOCOL_VERSION,
            PROTOCOL_VERSION,
            height * NUM_NS_IN_SECOND,
            chunk_endorsements,
            None,
        ),
        [0; 32],
    )
    .unwrap()
    .commit();
}

/// Drives `count` blocks in epoch 0 where the single shard's chunk is missed
/// exactly on the heights where `target` is the scheduled producer. The result:
/// `target` accumulates 0 produced / many expected (blacklist candidate) while the
/// other producer stays at 100%. Returns the recorded block hashes (index = height).
fn drive_targeted_misses(
    handle: &EpochManagerHandle,
    count: u64,
    target: ValidatorId,
) -> Vec<CryptoHash> {
    let h: Vec<CryptoHash> = (0..=count).map(|i| hash(&i.to_le_bytes())).collect();
    record_block(&mut handle.write(), CryptoHash::default(), h[0], 0, vec![]);
    let epoch_id = handle.get_epoch_id(&h[0]).unwrap();
    let layout = handle.get_shard_layout(&epoch_id).unwrap();
    let shard_id = layout.shard_ids().next().unwrap();
    let epoch_info = handle.get_epoch_info(&epoch_id).unwrap();
    let mut prev = h[0];
    for height in 1..=count {
        let scheduled = epoch_info.sample_chunk_producer(&layout, shard_id, height).unwrap();
        let produced = scheduled != target;
        record_block_with_mask(
            &mut handle.write(),
            prev,
            h[height as usize],
            height,
            vec![produced],
        );
        prev = h[height as usize];
    }
    h
}

// 10. pre-v152 protocol + miss-heavy stats -> accessor returns empty (gate proves
//     no production leak). Only meaningful on stable (PROTOCOL_VERSION < 152).
#[cfg(not(feature = "nightly"))]
#[test]
fn get_chunk_producer_blacklist_empty_when_feature_disabled() {
    let validators = vec![("test0".parse().unwrap(), STAKE), ("test1".parse().unwrap(), STAKE)];
    let handle = setup_default_epoch_manager(validators, 10_000, 1, 3, 90, 60).into_handle();
    let h = drive_targeted_misses(&handle, 160, 0);
    let bl = handle.get_chunk_producer_blacklist(h.last().unwrap()).unwrap();
    assert!(bl.is_empty(), "feature disabled must yield empty blacklist, got {bl:?}");
}

// Enabled-path end-to-end: v152+ protocol + miss-heavy stats -> the target is
// blacklisted on its shard (proves the accessor wires aggregator -> compute).
#[cfg(feature = "nightly")]
#[test]
fn get_chunk_producer_blacklist_blacklists_miss_heavy_producer() {
    let validators = vec![("test0".parse().unwrap(), STAKE), ("test1".parse().unwrap(), STAKE)];
    let handle = setup_default_epoch_manager(validators, 10_000, 1, 3, 90, 60).into_handle();
    let h = drive_targeted_misses(&handle, 160, 0);
    let prev = *h.last().unwrap();
    let epoch_id = handle.get_epoch_id_from_prev_block(&prev).unwrap();
    let shard_id = handle.get_shard_layout(&epoch_id).unwrap().shard_ids().next().unwrap();
    let bl = handle.get_chunk_producer_blacklist(&prev).unwrap();
    assert_eq!(bl, HashMap::from([(shard_id, HashSet::from([0]))]));
}

// 11. v152+ protocol: at an epoch boundary the aggregator belongs to the previous
//     epoch, so the accessor returns empty even though epoch 0 stats are miss-heavy.
#[cfg(feature = "nightly")]
#[test]
fn get_chunk_producer_blacklist_resets_on_epoch_boundary() {
    let validators = vec![("test0".parse().unwrap(), STAKE), ("test1".parse().unwrap(), STAKE)];
    // Epoch length 160: epoch 0 accumulates miss-heavy stats, then we cross into
    // epoch 1 so the last epoch-0 block is an epoch boundary.
    let handle = setup_default_epoch_manager(validators, 160, 1, 3, 90, 60).into_handle();
    let h = drive_targeted_misses(&handle, 160, 0);
    // Find the last recorded block that is the end of its epoch (next block starts a
    // new epoch). The accessor keyed on it must reset to empty.
    let boundary = h
        .iter()
        .rev()
        .find(|hash| handle.is_next_block_epoch_start(hash).unwrap())
        .expect("expected an epoch boundary among recorded blocks");
    let bl = handle.get_chunk_producer_blacklist(boundary).unwrap();
    assert!(bl.is_empty(), "epoch boundary must reset blacklist, got {bl:?}");
}

// --- Writer tests: `seed_chunk_producers` stores the blacklist-adjusted producer,
// resolved end-to-end through the DB-backed `get_chunk_producer_info_from_prev_block`. ---

/// Drives `count` blocks, deciding each chunk's produced/missed flag from the
/// producer that consensus actually resolves (the seeded `DBCol::ChunkProducers`
/// row via the grandparent anchor), not the plain sampler. A chunk is missed iff
/// the resolved producer is `target`, so only `target` accrues misses; once the
/// writer starts excluding `target`, the replacement is resolved and produces,
/// which freezes `target`'s stats (the anti-flap dynamic under test).
#[cfg(feature = "nightly")]
fn drive_anchored_misses(
    handle: &EpochManagerHandle,
    count: u64,
    target: ValidatorId,
) -> Vec<CryptoHash> {
    let h: Vec<CryptoHash> = (0..=count).map(|i| hash(&i.to_le_bytes())).collect();
    record_block(&mut handle.write(), CryptoHash::default(), h[0], 0, vec![]);
    let epoch_id = handle.get_epoch_id(&h[0]).unwrap();
    let layout = handle.get_shard_layout(&epoch_id).unwrap();
    let shard_id = layout.shard_ids().next().unwrap();
    let target_account =
        handle.get_epoch_info(&epoch_id).unwrap().get_validator(target).account_id().clone();
    let mut prev = h[0];
    for height in 1..=count {
        let resolved = handle.get_chunk_producer_info_from_prev_block(&prev, shard_id).unwrap();
        let produced = resolved.account_id() != &target_account;
        record_block_with_mask(
            &mut handle.write(),
            prev,
            h[height as usize],
            height,
            vec![produced],
        );
        prev = h[height as usize];
    }
    h
}

// 12. Anti-flap: once `target` is blacklisted, the seeded row (read back through the
//     resolver) excludes it in favour of the replacement, and because the aggregator
//     credits the replacement, `target` accrues no new expected and stays out on the
//     next anchor (does not flap back in).
#[cfg(feature = "nightly")]
#[test]
fn seeded_producer_excludes_blacklisted_and_does_not_flap_back() {
    let validators = vec![("test0".parse().unwrap(), STAKE), ("test1".parse().unwrap(), STAKE)];
    let handle = setup_default_epoch_manager(validators, 10_000, 1, 3, 90, 60).into_handle();
    let count = 160u64;
    let h = drive_anchored_misses(&handle, count, 0);

    let epoch_id = handle.get_epoch_id(&h[0]).unwrap();
    let layout = handle.get_shard_layout(&epoch_id).unwrap();
    let shard_id = layout.shard_ids().next().unwrap();
    let epoch_info = handle.get_epoch_info(&epoch_id).unwrap();
    let target_account = epoch_info.get_validator(0).account_id().clone();

    // Target 0 is blacklisted by the end of the run.
    let bl = handle.get_chunk_producer_blacklist(&h[count as usize]).unwrap();
    assert_eq!(
        bl.get(&shard_id),
        Some(&HashSet::from([0])),
        "target must be blacklisted at the tip, got {bl:?}"
    );

    // The two latest chunk heights whose PLAIN producer is the blacklisted target.
    // For both, the resolver (reading the seeded row) must pick the replacement, and
    // it must stay excluded across both anchors -- i.e. it does not flap back in.
    let mut target_heights: Vec<u64> = (3..=count)
        .filter(|&hh| epoch_info.sample_chunk_producer(&layout, shard_id, hh) == Some(0))
        .collect();
    assert!(target_heights.len() >= 2, "need >=2 target slots, got {target_heights:?}");
    let last_two = target_heights.split_off(target_heights.len() - 2);

    for hh in last_two {
        // Plain sampler would pick the blacklisted target here...
        assert_eq!(epoch_info.sample_chunk_producer(&layout, shard_id, hh), Some(0));
        // ...and the anchor seeding this chunk (grandparent = h[hh-2]) has it blacklisted.
        let anchor_bl = handle.get_chunk_producer_blacklist(&h[(hh - 2) as usize]).unwrap();
        assert_eq!(anchor_bl.get(&shard_id), Some(&HashSet::from([0])));
        // ...so the stored (resolved) producer is the replacement, not the target.
        let resolved = handle
            .get_chunk_producer_info_from_prev_block(&h[(hh - 1) as usize], shard_id)
            .unwrap();
        assert_ne!(
            resolved.account_id(),
            &target_account,
            "blacklisted producer flapped back in at height {hh}"
        );
    }

    // Anti-flap invariant: once excluded, the target accrues no new expected. Its
    // aggregator `expected` is frozen across the late tail because the aggregator
    // credits the seeded replacement instead of the (never-resolved) target.
    let expected_of_target = |anchor: &CryptoHash| -> u64 {
        handle
            .read()
            .get_epoch_info_aggregator_upto_last(anchor)
            .unwrap()
            .shard_tracker
            .get(&shard_id)
            .and_then(|m| m.get(&0))
            .map(|s| s.expected())
            .unwrap_or(0)
    };
    let early = count - 20;
    // Precondition: the target is already blacklisted at `early`...
    assert!(
        !handle.get_chunk_producer_blacklist(&h[early as usize]).unwrap().is_empty(),
        "target must already be blacklisted at the earlier anchor"
    );
    // ...so its expected does not grow between `early` and the tip.
    assert_eq!(
        expected_of_target(&h[early as usize]),
        expected_of_target(&h[count as usize]),
        "blacklisted target must accrue no new expected"
    );
}

// 13. Empty-blacklist path end-to-end (nightly, feature ON): with no misses nothing is
//     blacklisted, so the seeded/resolved producer equals the plain sampler at every
//     height. Proves the writer never excludes spuriously.
#[cfg(feature = "nightly")]
#[test]
fn seeded_producer_matches_plain_sampler_without_kickouts() {
    let validators = vec![("test0".parse().unwrap(), STAKE), ("test1".parse().unwrap(), STAKE)];
    let handle = setup_default_epoch_manager(validators, 10_000, 1, 3, 90, 60).into_handle();
    let count = 40u64;
    let h: Vec<CryptoHash> = (0..=count).map(|i| hash(&i.to_le_bytes())).collect();
    record_block(&mut handle.write(), CryptoHash::default(), h[0], 0, vec![]);
    let mut prev = h[0];
    for height in 1..=count {
        record_block_with_mask(&mut handle.write(), prev, h[height as usize], height, vec![true]);
        prev = h[height as usize];
    }

    let epoch_id = handle.get_epoch_id(&h[0]).unwrap();
    let layout = handle.get_shard_layout(&epoch_id).unwrap();
    let shard_id = layout.shard_ids().next().unwrap();
    let epoch_info = handle.get_epoch_info(&epoch_id).unwrap();
    // No blacklist accrued.
    assert!(handle.get_chunk_producer_blacklist(&h[count as usize]).unwrap().is_empty());
    for height in 2..=count {
        let plain = epoch_info.sample_chunk_producer(&layout, shard_id, height).unwrap();
        let resolved = handle
            .get_chunk_producer_info_from_prev_block(&h[(height - 1) as usize], shard_id)
            .unwrap();
        assert_eq!(
            resolved.account_id(),
            epoch_info.get_validator(plain).account_id(),
            "empty-blacklist resolver must match plain sampler at height {height}"
        );
    }
}

// 14. Feature OFF (stable, pre-v152): even with a miss-heavy target the resolver never
//     excludes -- the resolved producer equals the plain sampler at every height.
#[cfg(not(feature = "nightly"))]
#[test]
fn feature_off_resolved_producer_matches_plain_sampler() {
    let validators = vec![("test0".parse().unwrap(), STAKE), ("test1".parse().unwrap(), STAKE)];
    let handle = setup_default_epoch_manager(validators, 10_000, 1, 3, 90, 60).into_handle();
    let count = 160u64;
    let h = drive_targeted_misses(&handle, count, 0);
    let epoch_id = handle.get_epoch_id(&h[0]).unwrap();
    let layout = handle.get_shard_layout(&epoch_id).unwrap();
    let shard_id = layout.shard_ids().next().unwrap();
    let epoch_info = handle.get_epoch_info(&epoch_id).unwrap();
    for height in 2..=count {
        let plain = epoch_info.sample_chunk_producer(&layout, shard_id, height).unwrap();
        let resolved = handle
            .get_chunk_producer_info_from_prev_block(&h[(height - 1) as usize], shard_id)
            .unwrap();
        assert_eq!(
            resolved.account_id(),
            epoch_info.get_validator(plain).account_id(),
            "feature-off resolver must match plain sampler at height {height}"
        );
    }
}

// --- Metric test: `near_early_kickout_slot_reassigned_total` counts seeded writes the
// blacklist actually moved, once per write, bounded to the `shard_id` label. ---

/// Reads the process-global reassignment counter for one shard.
#[cfg(feature = "nightly")]
fn slot_reassigned_total(shard_id: ShardId) -> u64 {
    EARLY_KICKOUT_SLOT_REASSIGNED.with_label_values(&[&shard_id.to_string()]).get()
}

/// Multi-shard variant of `drive_anchored_misses`: records heights `start..=end`,
/// missing only `target_shard`'s chunk and only when its *resolved* (seeded)
/// producer is `target`; every other shard always produces. So `target` accrues
/// misses solely on `target_shard`, which is the only shard that blacklists. The
/// aggregator credits the resolved producer, so once `target` is excluded its stats
/// freeze (anti-flap), keeping the blacklist stable. `hashes[i]` is the block at
/// height `i`; genesis (`hashes[0]`) must already be recorded.
#[cfg(feature = "nightly")]
fn drive_anchored_misses_on_shard(
    handle: &EpochManagerHandle,
    hashes: &[CryptoHash],
    start: u64,
    end: u64,
    target: ValidatorId,
    target_shard: ShardId,
) {
    let epoch_id = handle.get_epoch_id(&hashes[0]).unwrap();
    let layout = handle.get_shard_layout(&epoch_id).unwrap();
    let target_account =
        handle.get_epoch_info(&epoch_id).unwrap().get_validator(target).account_id().clone();
    for height in start..=end {
        let prev = hashes[(height - 1) as usize];
        let mask: Vec<bool> = layout
            .shard_ids()
            .map(|sid| {
                if sid != target_shard {
                    return true;
                }
                let resolved = handle.get_chunk_producer_info_from_prev_block(&prev, sid).unwrap();
                resolved.account_id() != &target_account
            })
            .collect();
        record_block_with_mask(&mut handle.write(), prev, hashes[height as usize], height, mask);
    }
}

// 15. Rollout metric: with a producer blacklisted on one shard, the counter for that
//     shard increments exactly once per seeded write that the blacklist actually
//     moved (stored producer != plain sampler), and a healthy shard never increments.
//     Isolated on non-zero shard labels so the process-global counter is not shared
//     with the single-shard writer tests above (which only touch shard 0).
#[cfg(feature = "nightly")]
#[test]
fn slot_reassigned_metric_counts_reassignments() {
    let validators: Vec<_> = (0..6).map(|i| (format!("test{i}").parse().unwrap(), STAKE)).collect();
    let handle = setup_default_epoch_manager(validators, 10_000, 3, 3, 90, 60).into_handle();
    let count = 160u64;

    let h: Vec<CryptoHash> = (0..=count).map(|i| hash(&i.to_le_bytes())).collect();
    record_block(&mut handle.write(), CryptoHash::default(), h[0], 0, vec![]);

    let epoch_id = handle.get_epoch_id(&h[0]).unwrap();
    let layout = handle.get_shard_layout(&epoch_id).unwrap();
    let epoch_info = handle.get_epoch_info(&epoch_id).unwrap();

    // Pick a shard to reassign and a shard to keep healthy, both non-zero so their
    // counters are never shared with the single-shard tests. The target shard needs
    // >= 2 distinct producers (else the safety valve vetoes blacklisting); the target
    // validator is one of its producers so it accrues misses there.
    let distinct = |s: ShardId| -> HashSet<ValidatorId> {
        let idx = layout.get_shard_index(s).unwrap();
        epoch_info.chunk_producers_settlement()[idx].iter().copied().collect()
    };
    let non_zero: Vec<ShardId> = layout.shard_ids().filter(|s| *s != ShardId::new(0)).collect();
    let target_shard = *non_zero
        .iter()
        .find(|&&s| distinct(s).len() >= 2)
        .expect("need a non-zero shard with >=2 producers");
    let target: ValidatorId = *distinct(target_shard).iter().min().unwrap();
    let healthy_shard =
        *non_zero.iter().find(|&&s| s != target_shard).expect("need a second non-zero shard");

    // Captured after genesis: genesis seeds chunk height 2 with an empty blacklist, so
    // it can never reassign and is excluded from both the window and the oracle below.
    let before_target = slot_reassigned_total(target_shard);
    let before_healthy = slot_reassigned_total(healthy_shard);

    // Phase 1 records h[1..=count-2], seeding chunk heights 3..=count. The window
    // `mid - before` therefore matches the seeded rows we can read back (each needs its
    // child block, recorded in phase 2).
    drive_anchored_misses_on_shard(&handle, &h, 1, count - 2, target, target_shard);
    let mid_target = slot_reassigned_total(target_shard);
    let mid_healthy = slot_reassigned_total(healthy_shard);
    // Phase 2 records the tail so anchor h[count-2]'s row is readable via child h[count-1].
    drive_anchored_misses_on_shard(&handle, &h, count - 1, count, target, target_shard);

    // The target is blacklisted on its shard; the healthy shard never is.
    let bl = handle.get_chunk_producer_blacklist(&h[(count - 2) as usize]).unwrap();
    assert_eq!(
        bl.get(&target_shard),
        Some(&HashSet::from([target])),
        "target must be blacklisted on the target shard, got {bl:?}"
    );
    assert!(bl.get(&healthy_shard).is_none(), "healthy shard must not be blacklisted, got {bl:?}");

    // Independent oracle over the readable seed writes (chunk heights 3..=count, anchors
    // h[1..=count-2]): a write reassigned iff the actually-stored producer differs from
    // the plain sampler. The healthy shard must equal the plain sampler everywhere.
    let mut reassigned = 0u64;
    for hh in 3..=count {
        let child = &h[(hh - 1) as usize];

        let plain = epoch_info.sample_chunk_producer(&layout, target_shard, hh).unwrap();
        let stored = handle.get_chunk_producer_info_from_prev_block(child, target_shard).unwrap();
        if stored.account_id() != epoch_info.get_validator(plain).account_id() {
            reassigned += 1;
        }

        let plain_h = epoch_info.sample_chunk_producer(&layout, healthy_shard, hh).unwrap();
        let stored_h =
            handle.get_chunk_producer_info_from_prev_block(child, healthy_shard).unwrap();
        assert_eq!(
            stored_h.account_id(),
            epoch_info.get_validator(plain_h).account_id(),
            "healthy shard seed must match plain sampler at height {hh}"
        );
    }

    assert!(reassigned >= 1, "scenario must drive at least one reassignment");
    assert_eq!(
        mid_target - before_target,
        reassigned,
        "reassigned counter must equal the moved seed writes on the target shard"
    );
    assert_eq!(
        mid_healthy - before_healthy,
        0,
        "healthy shard counter must not increment when nothing is reassigned"
    );
}
