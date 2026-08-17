//! Tests for the early-kickout blacklist math (`compute_chunk_producer_blacklist`)
//! and the gated `get_chunk_producer_blacklist` accessor: the math directly, and the
//! accessor end-to-end (gate + boundary reset + enabled path).

#[cfg(feature = "nightly")]
use crate::CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET;
#[cfg(feature = "nightly")]
use crate::EARLY_KICKOUT_EPOCH_GRACE_BLOCKS;
#[cfg(feature = "nightly")]
use crate::epoch_info_aggregator::EpochInfoAggregator;
use crate::reward_calculator::NUM_NS_IN_SECOND;
#[cfg(all(feature = "nightly", feature = "test_features"))]
use crate::set_early_kickout_thresholds_for_testing;
use crate::test_utils::DEFAULT_TOTAL_SUPPLY;
use crate::test_utils::{
    epoch_info, record_block, record_block_with_final_and_mask, setup_default_epoch_manager,
};
use crate::{
    ChunkProducerBlacklist, EpochManager, EpochManagerAdapter, EpochManagerHandle,
    compute_chunk_producer_blacklist,
};
#[cfg(feature = "nightly")]
use crate::{SampleEpoch, SeedAnchor};
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::epoch_info::EpochInfo;
#[cfg(feature = "nightly")]
use near_primitives::errors::EpochError;
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::shard_layout::ShardLayout;
#[cfg(feature = "nightly")]
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::stateless_validation::chunk_endorsements_bitmap::ChunkEndorsementsBitmap;
#[cfg(feature = "nightly")]
use near_primitives::types::EpochId;
#[cfg(feature = "nightly")]
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{AccountId, Balance, ChunkStats, ShardId, ValidatorId};
#[cfg(feature = "nightly")]
use near_primitives::utils::get_block_shard_id;
use near_primitives::version::PROTOCOL_VERSION;
#[cfg(feature = "nightly")]
use near_store::DBCol;
#[cfg(feature = "nightly")]
use near_store::adapter::StoreAdapter;
use std::collections::{HashMap, HashSet};

const STAKE: Balance = Balance::from_yoctonear(1_000_000);

/// Builds an `EpochInfo` over `layout` with `num_producers` accounts (ids
/// `0..num_producers`) and one explicit chunk-producer `settlement` per shard, in
/// shard-index order (`layout.shard_infos()`). `num_producers` is passed explicitly
/// rather than inferred: it sets the account count that `stake_weights` indexes, and
/// per-shard settlements no longer determine it.
fn epoch_info_for_layout(
    layout: &ShardLayout,
    settlements: Vec<Vec<ValidatorId>>,
    num_producers: u64,
) -> EpochInfo {
    assert_eq!(
        settlements.len(),
        layout.num_shards() as usize,
        "one chunk-producer settlement per shard",
    );
    let accounts: Vec<_> =
        (0..num_producers).map(|i| (format!("test{i}").parse().unwrap(), STAKE)).collect();
    let block_producers: Vec<ValidatorId> = (0..num_producers).collect();
    epoch_info(0, accounts, block_producers, settlements, PROTOCOL_VERSION, layout.clone())
}

/// Builds a single-shard `EpochInfo` with `num_producers` chunk producers (ids
/// `0..num_producers`) and returns it alongside the layout and the shard id.
fn single_shard_epoch(num_producers: u64) -> (EpochInfo, ShardLayout, ShardId) {
    let shard_layout = ShardLayout::single_shard();
    let shard_id = shard_layout.shard_ids().next().unwrap();
    let settlement: Vec<ValidatorId> = (0..num_producers).collect();
    let epoch_info = epoch_info_for_layout(&shard_layout, vec![settlement], num_producers);
    (epoch_info, shard_layout, shard_id)
}

/// Projects a `ChunkProducerBlacklist`'s per-shard `shard_stats` into a comparable
/// map (`raw_candidate_count`, `kept`). `BlacklistShardStats` has no `PartialEq`, so
/// tests read it field by field rather than adding a derive to the production struct.
fn shard_stats_projection(
    result: &ChunkProducerBlacklist,
) -> HashMap<ShardId, (usize, Option<ValidatorId>)> {
    result
        .shard_stats
        .iter()
        .map(|(shard_id, stats)| (*shard_id, (stats.raw_candidate_count, stats.kept)))
        .collect()
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

/// Runs the math, returns the applied blacklist map.
fn blacklist(
    st: &HashMap<ShardId, HashMap<ValidatorId, ChunkStats>>,
    epoch_info: &EpochInfo,
    layout: &ShardLayout,
) -> HashMap<ShardId, HashSet<ValidatorId>> {
    compute_chunk_producer_blacklist(st, epoch_info, layout).blacklist
}

/// On an all-bad shard the keep-one valve leaves exactly one survivor. Returns the
/// id of that survivor (the least-bad producer that keeps its slot).
fn kept_survivor(
    st: &HashMap<ShardId, HashMap<ValidatorId, ChunkStats>>,
    epoch_info: &EpochInfo,
    layout: &ShardLayout,
    shard_id: ShardId,
    producers: &[ValidatorId],
) -> ValidatorId {
    let bl = blacklist(st, epoch_info, layout);
    let excluded = bl.get(&shard_id).cloned().unwrap_or_default();
    let survivors: Vec<ValidatorId> =
        producers.iter().copied().filter(|id| !excluded.contains(id)).collect();
    assert_eq!(survivors.len(), 1, "keep-one must leave exactly one survivor, got {survivors:?}");
    survivors[0]
}

// 1. produced/expected < 80%, missed >= 100 -> blacklisted.
#[test]
fn blacklist_below_threshold() {
    let (epoch_info, layout, shard_id) = single_shard_epoch(4);
    // id 0: 0/200 = 0% < 80%, missed 200; others healthy.
    let st = tracker(shard_id, &[(0, 0, 200), (1, 100, 100), (2, 100, 100), (3, 100, 100)]);
    let bl = blacklist(&st, &epoch_info, &layout);
    assert_eq!(bl, HashMap::from([(shard_id, HashSet::from([0]))]));
}

// 2. produced*100 == expected*80 -> NOT blacklisted (strict `<`), missed >= 100.
#[test]
fn blacklist_exactly_at_threshold() {
    let (epoch_info, layout, shard_id) = single_shard_epoch(4);
    // id 0: 400/500 = exactly 80%, missed 100 (>= 100). Strict `<` must exclude it.
    let st = tracker(shard_id, &[(0, 400, 500), (1, 500, 500), (2, 500, 500), (3, 500, 500)]);
    let bl = blacklist(&st, &epoch_info, &layout);
    assert!(bl.is_empty());
}

// 3. missed < 100 -> not blacklisted regardless of ratio.
#[test]
fn blacklist_under_min_misses() {
    let (epoch_info, layout, shard_id) = single_shard_epoch(4);
    // id 0: 391/490 = 79.8% < 80% but missed only 99 (< 100).
    let st = tracker(shard_id, &[(0, 391, 490), (1, 100, 100), (2, 100, 100), (3, 100, 100)]);
    let bl = blacklist(&st, &epoch_info, &layout);
    assert!(bl.is_empty());
}

// 4. every producer would be blacklisted -> keep exactly one least-bad. Both here
//    have identical stats, so the tiebreak (lower validator_id) keeps id 0 and
//    blacklists id 1. The valve stat records the firing.
#[test]
fn blacklist_safety_valve_all_producers() {
    let (epoch_info, layout, shard_id) = single_shard_epoch(2);
    let st = tracker(shard_id, &[(0, 0, 100), (1, 0, 100)]);
    let res = compute_chunk_producer_blacklist(&st, &epoch_info, &layout);
    assert_eq!(res.blacklist, HashMap::from([(shard_id, HashSet::from([1]))]));
    let stats = &res.shard_stats[&shard_id];
    assert_eq!(stats.raw_candidate_count, 2);
    assert!(stats.safety_valve_fired(), "valve must fire when every producer is a candidate");
}

// 5. lone producer would be blacklisted -> keep-one leaves it eligible, so the shard
//    has no blacklist entry, and the stats record that the valve fired.
#[test]
fn blacklist_single_producer_shard() {
    let (epoch_info, layout, shard_id) = single_shard_epoch(1);
    let st = tracker(shard_id, &[(0, 0, 100)]);
    let res = compute_chunk_producer_blacklist(&st, &epoch_info, &layout);
    assert!(res.blacklist.is_empty(), "1-producer shard must never be blacklisted");
    let stats = &res.shard_stats[&shard_id];
    assert_eq!(stats.raw_candidate_count, 1);
    assert!(stats.safety_valve_fired(), "valve must fire when the only producer is a candidate");
}

// 6. missed exactly 100 at < 80% -> blacklisted. Sharp lower edge of the miss floor
//    (one miss above `blacklist_under_min_misses`).
#[test]
fn blacklist_at_min_misses_boundary() {
    let (epoch_info, layout, shard_id) = single_shard_epoch(4);
    // id 0: 390/490 = 79.6% < 80%, missed exactly 100.
    let st = tracker(shard_id, &[(0, 390, 490), (1, 100, 100), (2, 100, 100), (3, 100, 100)]);
    let bl = blacklist(&st, &epoch_info, &layout);
    assert_eq!(bl, HashMap::from([(shard_id, HashSet::from([0]))]));
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
    inner.insert(0, ChunkStats::new(0, 200, 1000, 1000));
    inner.insert(1, ChunkStats::new_with_production(100, 100));
    inner.insert(2, ChunkStats::new_with_production(100, 100));
    let st = HashMap::from([(shard_id, inner)]);
    let bl = blacklist(&st, &epoch_info, &layout);
    assert_eq!(bl, HashMap::from([(shard_id, HashSet::from([0]))]));
}

// 8. all producers above threshold -> empty map.
#[test]
fn blacklist_empty_when_healthy() {
    let (epoch_info, layout, shard_id) = single_shard_epoch(3);
    let st = tracker(shard_id, &[(0, 100, 100), (1, 96, 100), (2, 100, 100)]);
    let bl = blacklist(&st, &epoch_info, &layout);
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
    let epoch_info = epoch_info(
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
    let bl = blacklist(&st, &epoch_info, &shard_layout);
    assert_eq!(bl, HashMap::from([(shard_ids[0], HashSet::from([0]))]));
}

// --- keep-one safety-valve behavior (all producers are candidates) ---

// (a) recovering holder keeps its slot while its ratio is the highest among the
//     frozen candidates. All three producers are below threshold; id 1 has the
//     highest ratio and must be the survivor.
#[test]
fn keep_one_keeps_highest_ratio_holder() {
    let (epoch_info, layout, shard_id) = single_shard_epoch(3);
    // ratios: id 0 = 40%, id 1 = 79% (holder), id 2 = 50%. All candidates (missed >= 100).
    let st = tracker(shard_id, &[(0, 400, 1000), (1, 790, 1000), (2, 500, 1000)]);
    assert_eq!(kept_survivor(&st, &epoch_info, &layout, shard_id, &[0, 1, 2]), 1);
    assert_eq!(
        blacklist(&st, &epoch_info, &layout),
        HashMap::from([(shard_id, HashSet::from([0, 2]))])
    );
}

// (b) the slot rotates to a different frozen candidate once the current holder's
//     ratio drops below it. Same producer set, only ratios move.
#[test]
fn keep_one_rotates_when_holder_ratio_drops() {
    let (epoch_info, layout, shard_id) = single_shard_epoch(3);
    // id 0 is the holder with the highest ratio (79%).
    let holding = tracker(shard_id, &[(0, 790, 1000), (1, 500, 1000), (2, 400, 1000)]);
    assert_eq!(kept_survivor(&holding, &epoch_info, &layout, shard_id, &[0, 1, 2]), 0);
    // id 0 collapses to 10%; id 1 (50%) is now the least-bad and takes the slot.
    let dropped = tracker(shard_id, &[(0, 100, 1000), (1, 500, 1000), (2, 400, 1000)]);
    assert_eq!(kept_survivor(&dropped, &epoch_info, &layout, shard_id, &[0, 1, 2]), 1);
}

// (c) the worst producer is never the survivor. id 0 has the lowest ratio and must
//     always be blacklisted; the best producer (id 2) is kept.
#[test]
fn keep_one_never_keeps_worst() {
    let (epoch_info, layout, shard_id) = single_shard_epoch(3);
    // ratios: id 0 = 5% (worst), id 1 = 40%, id 2 = 79% (best).
    let st = tracker(shard_id, &[(0, 50, 1000), (1, 400, 1000), (2, 790, 1000)]);
    let bl = blacklist(&st, &epoch_info, &layout);
    assert!(bl[&shard_id].contains(&0), "worst producer must be blacklisted");
    assert_eq!(kept_survivor(&st, &epoch_info, &layout, shard_id, &[0, 1, 2]), 2);
}

// (d) after keep-one, exclusion always leaves >= 1 eligible producer, so
//     `sample_chunk_producer_excluding` never returns None on an all-bad shard.
#[test]
fn keep_one_leaves_sampler_nonempty() {
    let (epoch_info, layout, shard_id) = single_shard_epoch(3);
    // all three below threshold; id 2 is least-bad and kept.
    let st = tracker(shard_id, &[(0, 50, 1000), (1, 400, 1000), (2, 790, 1000)]);
    let exclude = blacklist(&st, &epoch_info, &layout)[&shard_id].clone();
    assert_eq!(exclude.len(), 2, "two of three producers must be excluded");
    for height in 0..50 {
        let sampled =
            epoch_info.sample_chunk_producer_excluding(&layout, shard_id, height, &exclude);
        assert_eq!(
            sampled,
            Some(2),
            "sampler must always yield the single surviving producer at height {height}"
        );
    }
}

// --- blacklist-math hardening (pure math) ---

// The safety valve's least-bad tiebreak resolves at the FEWER-EXPECTED level, not only
// on ratio and lower-id. Two all-bad producers with equal ratio (800/2000 == 400/1000):
// fewer-expected keeps id 1 (expected 1000 < 2000), while a lower-id-only tiebreak would
// keep id 0. Pins comparator level 2, which `blacklist_safety_valve_all_producers` (equal
// on every level) cannot.
#[test]
fn keep_one_fewer_expected_tiebreak() {
    let (epoch_info, layout, shard_id) = single_shard_epoch(2);
    let st = tracker(shard_id, &[(0, 800, 2000), (1, 400, 1000)]);
    assert_eq!(kept_survivor(&st, &epoch_info, &layout, shard_id, &[0, 1]), 1);
    assert_eq!(
        blacklist(&st, &epoch_info, &layout),
        HashMap::from([(shard_id, HashSet::from([0]))]),
    );
}

// Duplicate settlement entries do not inflate the safety-valve denominator: `producers` is
// a set, so a `[0, 0, 1]` settlement has two distinct producers, and two all-bad candidates
// trip the valve. Were the duplicate counted, the denominator would be 3, the valve would
// not fire, and both would be blacklisted. `EpochInfo::new` neither rejects nor dedups the
// duplicate.
#[test]
fn blacklist_dedups_duplicate_settlement_entries() {
    let layout = ShardLayout::single_shard();
    let shard_id = layout.shard_ids().next().unwrap();
    let epoch_info = epoch_info_for_layout(&layout, vec![vec![0, 0, 1]], 2);
    let st = tracker(shard_id, &[(0, 0, 100), (1, 0, 100)]);
    let res = compute_chunk_producer_blacklist(&st, &epoch_info, &layout);
    let stats = &res.shard_stats[&shard_id];
    assert_eq!(stats.raw_candidate_count, 2, "duplicate id must not inflate the candidate count");
    assert!(stats.safety_valve_fired(), "two distinct all-bad producers must trip the valve");
    assert_eq!(res.blacklist[&shard_id].len(), 1, "keep-one must leave exactly one survivor");
}

// Endorsement-only entries stay out of BOTH the safety-valve denominator and the kept set.
// Two all-bad producers trip the valve; an endorsement-only validator (id 2, not in the
// settlement) is neither counted nor kept. Existing test 7 covers endorsement-only exclusion
// from candidacy, but not this all-bad-shard interaction.
#[test]
fn blacklist_valve_ignores_endorsement_only_entry() {
    let (epoch_info, layout, shard_id) = single_shard_epoch(2);
    let mut inner = HashMap::new();
    inner.insert(0, ChunkStats::new_with_production(0, 100));
    inner.insert(1, ChunkStats::new_with_production(0, 100));
    // Endorsement-only, not in the settlement: high endorsement, zero production.
    inner.insert(2, ChunkStats::new(0, 0, 1000, 1000));
    let st = HashMap::from([(shard_id, inner)]);
    let res = compute_chunk_producer_blacklist(&st, &epoch_info, &layout);
    let stats = &res.shard_stats[&shard_id];
    assert_eq!(stats.raw_candidate_count, 2, "endorsement-only id must not be a candidate");
    assert!(stats.safety_valve_fired());
    assert_ne!(stats.kept, Some(2), "endorsement-only id must never be the kept producer");
    let bl = &res.blacklist[&shard_id];
    assert_eq!(bl.len(), 1, "keep-one must leave exactly one survivor");
    assert!(!bl.contains(&2), "endorsement-only id must never be blacklisted");
}

// u128 keeps both the ratio comparison and the safety-valve cross-multiply overflow-proof.
// A u64 cross-multiply of these operands would panic under `overflow-checks`, which is on in
// the `dev-release` profile CI builds. `produced == expected == u64::MAX` is avoided on
// purpose: missed would be 0, so it would not be a candidate.
#[test]
fn blacklist_u128_arithmetic_no_overflow() {
    let big = u64::MAX;
    // Ratio check: id 0 misses ~2^63 with produced*100 (~9.2e20) < expected*80 (~1.5e21), so
    // it is a candidate; ids 1, 2 are healthy so the valve does not fire.
    let (epoch_info, layout, shard_id) = single_shard_epoch(3);
    let st = tracker(shard_id, &[(0, big / 2, big), (1, 100, 100), (2, 100, 100)]);
    assert_eq!(
        blacklist(&st, &epoch_info, &layout),
        HashMap::from([(shard_id, HashSet::from([0]))]),
    );

    // Safety-valve comparator cross-multiply (pa*eb vs pb*ea) with operands near 2^127
    // (still under 2^128). id 0's ratio ~1/2 beats id 1's ~1/4, so the valve keeps id 0.
    let (epoch_info2, layout2, shard2) = single_shard_epoch(2);
    let st2 = tracker(shard2, &[(0, big / 2, big), (1, big / 4, big)]);
    assert_eq!(kept_survivor(&st2, &epoch_info2, &layout2, shard2, &[0, 1]), 0);
    assert_eq!(
        blacklist(&st2, &epoch_info2, &layout2),
        HashMap::from([(shard2, HashSet::from([1]))]),
    );
}

// --- Accessor tests (end-to-end through EpochManagerHandle) ---

/// Records a block at `cur` with an explicit per-shard `chunk_mask` (true =
/// produced, false = missed), so we can synthesize miss-heavy stats.
fn record_block_with_mask(
    em: &mut EpochManager,
    prev: CryptoHash,
    cur: CryptoHash,
    height: u64,
    chunk_mask: Vec<bool>,
) {
    // ~2-block finality: last-final = grandparent (height - 2). The seeder bases the
    // blacklist on this hash.
    let last_final = *em.get_block_info(&prev).unwrap().prev_hash();
    record_block_with_final_and_mask(
        em,
        prev,
        cur,
        height,
        last_final,
        height.saturating_sub(2),
        chunk_mask,
    );
}

/// Drives `count` blocks in epoch 0 where the single shard's chunk is missed
/// exactly on the heights where `target` is the scheduled producer. The result:
/// `target` accumulates 0 produced / many expected (blacklist candidate) while the
/// other producer stays at 100%. Returns the recorded block hashes (index = height).
///
/// Stable-only: the plain height sampler keeps missing heights on `target`. On nightly the
/// seeder excludes the target once the grace lifts, so those tests use `drive_down_node`.
#[cfg(not(feature = "nightly"))]
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

// Enabled-path end-to-end: v152+ protocol + miss-heavy stats past the grace window -> the
// down node is blacklisted on its shard (proves the accessor wires aggregator -> compute).
#[cfg(feature = "nightly")]
#[test]
fn get_chunk_producer_blacklist_blacklists_miss_heavy_producer() {
    let validators = vec![("test0".parse().unwrap(), STAKE), ("test1".parse().unwrap(), STAKE)];
    let handle = setup_default_epoch_manager(validators, 10_000, 1, 3, 90, 60).into_handle();
    // Drive past the 1000-block grace so the accumulated misses can take effect.
    let h = drive_down_node(&handle, 1200, 0);
    let prev = *h.last().unwrap();
    let epoch_id = handle.get_epoch_id_from_prev_block(&prev).unwrap();
    let shard_id = handle.get_shard_layout(&epoch_id).unwrap().shard_ids().next().unwrap();
    let bl = handle.get_chunk_producer_blacklist(&prev).unwrap();
    assert_eq!(bl, HashMap::from([(shard_id, HashSet::from([0]))]));
}

/// Drives `count` blocks in epoch 0 simulating `down` as a non-producing node, using
/// **blacklist-aware** assignment (mirrors the write path): at each height the chunk is
/// assigned to `sample_chunk_producer_excluding(current_blacklist)`. If that producer is
/// `down` the chunk is missed (mask=false); otherwise it is produced (mask=true). So once
/// `down` is blacklisted its slots reassign to a live producer that actually produces —
/// exactly what happens in production, with no phantom misses for the replacement.
/// Returns the recorded block hashes (index = height).
#[cfg(feature = "nightly")]
fn drive_down_node(handle: &EpochManagerHandle, count: u64, down: ValidatorId) -> Vec<CryptoHash> {
    let h: Vec<CryptoHash> = (0..=count).map(|i| hash(&i.to_le_bytes())).collect();
    record_block(&mut handle.write(), CryptoHash::default(), h[0], 0, vec![]);
    let epoch_id = handle.get_epoch_id(&h[0]).unwrap();
    let layout = handle.get_shard_layout(&epoch_id).unwrap();
    let shard_id = layout.shard_ids().next().unwrap();
    let epoch_info = handle.get_epoch_info(&epoch_id).unwrap();
    let empty = HashSet::new();
    let mut prev = h[0];
    for height in 1..=count {
        let blacklist = handle.get_chunk_producer_blacklist(&prev).unwrap();
        let assigned = epoch_info
            .sample_chunk_producer_excluding(
                &layout,
                shard_id,
                height,
                blacklist.get(&shard_id).unwrap_or(&empty),
            )
            .unwrap();
        let produced = assigned != down;
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

// Anti-flap attribution (headline guard): once validator 0 is blacklisted, its slots
// reassign to the replacement, which produces. The blacklist-aware seeder persists the
// replacement into `DBCol::ChunkProducers`, and the aggregator reads that row back via
// `anchored_chunk_producers_for_aggregator`, so the replacement (not validator 0) is
// credited on the reassigned heights. Validator 0 never recovers and never flaps back in.
#[cfg(feature = "nightly")]
#[test]
fn early_kickout_attribution_does_not_flap() {
    let validators = vec![("test0".parse().unwrap(), STAKE), ("test1".parse().unwrap(), STAKE)];
    let handle = setup_default_epoch_manager(validators, 10_000, 1, 3, 90, 60).into_handle();

    // Phase 1: drive past the 1000-block grace until validator 0 is blacklisted. With
    // blacklist-aware assignment the replacement (1) produces on the reassigned heights, so 1
    // stays healthy.
    let count = 1200;
    let h = drive_down_node(&handle, count, 0);
    let prev = *h.last().unwrap();
    let epoch_id = handle.get_epoch_id(&prev).unwrap();
    let shard_id = handle.get_shard_layout(&epoch_id).unwrap().shard_ids().next().unwrap();

    let bl = handle.get_chunk_producer_blacklist(&prev).unwrap();
    assert_eq!(
        bl,
        HashMap::from([(shard_id, HashSet::from([0]))]),
        "validator 0 must be blacklisted after sustained misses"
    );

    // Snapshot validator 0's and the replacement's stats at the blacklist point.
    let agg_before = handle.read().get_epoch_info_aggregator_upto_last(&prev).unwrap();
    let stats = |agg: &EpochInfoAggregator, id: ValidatorId| {
        agg.shard_tracker
            .get(&shard_id)
            .and_then(|m| m.get(&id))
            .map(|s| (s.produced(), s.expected()))
    };
    let before_0 = stats(&agg_before, 0).expect("validator 0 should have stats");
    let before_1 = stats(&agg_before, 1).expect("replacement should have stats");

    // Phase 2: keep driving with 0 still down. Its slots are reassigned to 1, which produces.
    let mut prev2 = prev;
    let extra = 80u64;
    for height in (count + 1)..=(count + extra) {
        let cur = hash(&height.to_le_bytes());
        record_block_with_mask(&mut handle.write(), prev2, cur, height, vec![true]);
        prev2 = cur;
    }

    let agg_after = handle.read().get_epoch_info_aggregator_upto_last(&prev2).unwrap();
    let after_0 = stats(&agg_after, 0).expect("validator 0 should still have stats");
    let after_1 = stats(&agg_after, 1).expect("replacement should still have stats");

    // Validator 0 is no longer assigned, so it accrues neither produced nor expected: it
    // cannot recover, hence cannot flap back in.
    assert_eq!(
        after_0, before_0,
        "blacklisted validator 0 must not accrue produced/expected (no recovery -> no flap)"
    );
    // The replacement absorbs the reassigned heights and produces them.
    assert!(
        after_1.0 > before_1.0 && after_1.1 > before_1.1,
        "replacement must accrue produced/expected on reassigned heights ({before_1:?} -> {after_1:?})"
    );
    // And the blacklist is stable: validator 0 stays blacklisted.
    let bl_after = handle.get_chunk_producer_blacklist(&prev2).unwrap();
    assert_eq!(
        bl_after,
        HashMap::from([(shard_id, HashSet::from([0]))]),
        "blacklist must remain stable (no flap)"
    );
}

// 11. v152+ epoch-boundary reset: the accessor samples the anchor's own epoch (mirroring the
//     seeder), so the boundary anchor (last block of epoch 0) still carries epoch 0's
//     miss-heavy blacklist. The reset lands on the first epoch-1 anchor: its own epoch flips
//     while its last-final block (the aggregator basis) still sits in epoch 0, so the
//     aggregator/target epoch mismatch empties the blacklist; the start-of-epoch grace keeps
//     it empty for the anchors after that.
//     Setup: epoch length 1200 exceeds the 1000-block grace (otherwise the whole epoch sits in
//     the grace and the reset check is vacuous), and the drive length 1300 crosses into epoch 1
//     so a boundary exists. `boundary_idx` is the last block whose next block starts a new epoch;
//     `h[i] == height` because `drive_down_node` stores hashes by height, so `boundary_idx`
//     is the boundary anchor and `boundary_idx + 1` the first epoch-1 anchor.
#[cfg(feature = "nightly")]
#[test]
fn get_chunk_producer_blacklist_resets_on_epoch_boundary() {
    let validators = vec![("test0".parse().unwrap(), STAKE), ("test1".parse().unwrap(), STAKE)];
    let handle = setup_default_epoch_manager(validators, 1200, 1, 3, 90, 60).into_handle();
    let h = drive_down_node(&handle, 1300, 0);
    let boundary_idx = (0..h.len())
        .rev()
        .find(|&i| handle.is_next_block_epoch_start(&h[i]).unwrap())
        .expect("expected an epoch boundary among recorded blocks");
    assert!(
        boundary_idx as u64 > EARLY_KICKOUT_EPOCH_GRACE_BLOCKS,
        "boundary at height {boundary_idx} must be past the grace for a non-vacuous reset check"
    );
    let bl_pre = handle.get_chunk_producer_blacklist(&h[boundary_idx - 1]).unwrap();
    assert!(
        !bl_pre.is_empty(),
        "pre-boundary anchor past the grace must be non-empty, got {bl_pre:?}"
    );
    // The boundary anchor samples its own epoch (epoch 0), matching the seeder: still
    // blacklisted.
    let bl_boundary = handle.get_chunk_producer_blacklist(&h[boundary_idx]).unwrap();
    assert!(!bl_boundary.is_empty(), "boundary anchor keeps its own epoch's blacklist, got empty");
    // First epoch-1 anchor: own epoch flips while the aggregator basis lags in epoch 0 — reset.
    let bl_next = handle.get_chunk_producer_blacklist(&h[boundary_idx + 1]).unwrap();
    assert!(bl_next.is_empty(), "first new-epoch anchor must reset blacklist, got {bl_next:?}");
}

// The first two anchors of a new epoch are the only ones whose aggregator basis (last-final
// block) sits in the previous epoch, and their rows ARE served on the consensus path (anchor
// epoch == chunk epoch). Guard that those rows carry the NEW epoch's canonical producer with
// an empty blacklist. This is what breaks if the aggregator/target epoch guard is dropped:
// sampling at the final block would bake an old-epoch producer into a new-epoch row, and
// keeping own-epoch sampling without the guard would match old-epoch stats (epoch-local
// `ValidatorId`s) against the new epoch's settlement.
#[cfg(feature = "nightly")]
#[test]
fn first_new_epoch_anchors_seed_new_epoch_canonical_producer() {
    // 3 validators (not the usual 2): enough schedule entropy that the old and new epoch
    // schedules diverge at a tested height, which the non-vacuity assertion below requires.
    let validators = vec![
        ("test0".parse().unwrap(), STAKE),
        ("test1".parse().unwrap(), STAKE),
        ("test2".parse().unwrap(), STAKE),
    ];
    let handle = setup_default_epoch_manager(validators, 1200, 1, 3, 90, 60).into_handle();
    // Cross TWO boundaries: every recorded block carries a zero VRF seed, so epoch 1 is
    // schedule-identical to epoch 0 and cross-epoch sampling would be invisible there. Epoch
    // 2's info is finalized from epoch 0's stats, where the down node fails the standard
    // epoch kickout, so its settlement genuinely differs from epoch 1's.
    let h = drive_down_node(&handle, 2500, 0);
    let boundary_idx = (0..h.len())
        .rev()
        .find(|&i| handle.is_next_block_epoch_start(&h[i]).unwrap())
        .expect("expected an epoch boundary among recorded blocks");
    assert!(
        boundary_idx as u64 > EARLY_KICKOUT_EPOCH_GRACE_BLOCKS,
        "boundary at height {boundary_idx} must be past the grace so the old epoch has a live \
         blacklist that could leak"
    );
    let old_epoch_id = handle.get_epoch_id(&h[boundary_idx]).unwrap();
    let old_epoch_info = handle.get_epoch_info(&old_epoch_id).unwrap();
    let old_layout = handle.get_shard_layout(&old_epoch_id).unwrap();
    let mut schedules_diverge = false;
    for i in [boundary_idx + 1, boundary_idx + 2] {
        let anchor = h[i];
        let epoch_id = handle.get_epoch_id(&anchor).unwrap();
        assert_ne!(epoch_id, old_epoch_id, "anchor at height {i} must be in the new epoch");
        let bl = handle.get_chunk_producer_blacklist(&anchor).unwrap();
        assert!(
            bl.is_empty(),
            "first new-epoch anchor at height {i} must have an empty blacklist, got {bl:?}"
        );
        let shard_id = handle.get_shard_layout(&epoch_id).unwrap().shard_ids().next().unwrap();
        let height_created = i as u64 + CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET;
        let stored = handle
            .get_chunk_producer_info_anchored(Some(&anchor), &epoch_id, height_created, shard_id)
            .unwrap();
        let canonical = handle
            .get_chunk_producer_info(&ChunkProductionKey { epoch_id, height_created, shard_id })
            .unwrap();
        assert_eq!(
            stored, canonical,
            "anchor at height {i}: seeded row must be the new epoch's canonical producer"
        );
        let old_pick = old_epoch_info.get_validator(
            old_epoch_info.sample_chunk_producer(&old_layout, shard_id, height_created).unwrap(),
        );
        schedules_diverge |= old_pick != canonical;
    }
    // Non-vacuous: the stored == canonical assertions can only catch old-epoch sampling if
    // the two epochs' schedules actually differ at a tested height. The fixture is fully
    // deterministic, so once this holds it holds forever.
    assert!(
        schedules_diverge,
        "old and new epoch schedules coincide at both tested heights; the canonical-producer \
         assertions would not catch old-epoch sampling"
    );
}

// Start-of-epoch grace: with the down node already miss-heavy, the accessor stays empty until
// the anchor is at least EARLY_KICKOUT_EPOCH_GRACE_BLOCKS into the epoch, then blacklists it.
#[cfg(feature = "nightly")]
#[test]
fn get_chunk_producer_blacklist_respects_epoch_grace() {
    let validators = vec![("test0".parse().unwrap(), STAKE), ("test1".parse().unwrap(), STAKE)];
    let handle = setup_default_epoch_manager(validators, 10_000, 1, 3, 90, 60).into_handle();
    let h = drive_down_node(&handle, 1200, 0);
    let epoch_id = handle.get_epoch_id(&h[1]).unwrap();
    let layout = handle.get_shard_layout(&epoch_id).unwrap();
    let shard_id = layout.shard_ids().next().unwrap();
    let epoch_info = handle.get_epoch_info(&epoch_id).unwrap();
    // Grace is measured against the last-final block (grandparent, anchor height - 2), so the
    // boundary in anchor height is the raw grace count + 2.
    // Same basis the production path uses (the `BlockInfo` walk), which in this fork-free
    // fixture has the same value as the `EpochStart` column.
    let epoch_start = handle.get_epoch_start_height(&h[1]).unwrap();

    // Down node is already miss-heavy, so an empty result here is the grace, not a lack of misses.
    let in_grace = (epoch_start + EARLY_KICKOUT_EPOCH_GRACE_BLOCKS + 1) as usize;
    let bl_grace = handle.get_chunk_producer_blacklist(&h[in_grace]).unwrap();
    assert!(bl_grace.is_empty(), "anchor inside the grace window must be empty, got {bl_grace:?}");
    // Inside the grace the seeded row is the plain pick (exclusion suppressed for both paths).
    let in_grace_ch = in_grace as u64 + CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET;
    let plain_in_grace = epoch_info
        .get_validator(epoch_info.sample_chunk_producer(&layout, shard_id, in_grace_ch).unwrap());
    let stored_in_grace = handle
        .get_chunk_producer_info_anchored(Some(&h[in_grace]), &epoch_id, in_grace_ch, shard_id)
        .unwrap();
    assert_eq!(stored_in_grace, plain_in_grace, "in-grace seeded row must be the plain pick");

    let past_grace = (epoch_start + EARLY_KICKOUT_EPOCH_GRACE_BLOCKS + 2) as usize;
    let bl_past = handle.get_chunk_producer_blacklist(&h[past_grace]).unwrap();
    assert_eq!(
        bl_past,
        HashMap::from([(shard_id, HashSet::from([0]))]),
        "anchor at the grace boundary must blacklist the down node"
    );
    // Consensus-sensitive: at the first active anchor the seeded `DBCol::ChunkProducers` row
    // must equal the accessor's blacklist-aware pick and never be the down node -- the seeder
    // and the accessor apply the same grace + blacklist at the exact threshold.
    let empty = HashSet::new();
    let past_ch = past_grace as u64 + CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET;
    let expected_past = epoch_info.get_validator(
        epoch_info
            .sample_chunk_producer_excluding(
                &layout,
                shard_id,
                past_ch,
                bl_past.get(&shard_id).unwrap_or(&empty),
            )
            .unwrap(),
    );
    let stored_past = handle
        .get_chunk_producer_info_anchored(Some(&h[past_grace]), &epoch_id, past_ch, shard_id)
        .unwrap();
    assert_eq!(
        stored_past, expected_past,
        "first-active-anchor seeded row must match the blacklist-aware sampler"
    );
    assert_ne!(
        epoch_info.get_validator_id(stored_past.account_id()).copied(),
        Some(0),
        "first-active-anchor seeded row must exclude the down node"
    );
}

// The seeded `DBCol::ChunkProducers` row equals the plain height sampler while the blacklist
// is empty, and the blacklist-aware sampler (never the down node) once it is non-empty. The
// strict consensus reader returns that same row.
#[cfg(feature = "nightly")]
#[test]
fn seeded_rows_match_blacklist_aware_sampler() {
    let validators = vec![("test0".parse().unwrap(), STAKE), ("test1".parse().unwrap(), STAKE)];
    let handle = setup_default_epoch_manager(validators, 10_000, 1, 3, 90, 60).into_handle();
    // Drive past the 1000-block grace so the late-window anchors have an active blacklist.
    let count = 1200;
    let h = drive_down_node(&handle, count, 0);
    let epoch_id = handle.get_epoch_id(&h[1]).unwrap();
    let layout = handle.get_shard_layout(&epoch_id).unwrap();
    let shard_id = layout.shard_ids().next().unwrap();
    let epoch_info = handle.get_epoch_info(&epoch_id).unwrap();

    // Early anchor: blacklist empty, so the row must equal the plain height sample.
    let early = 5u64;
    let early_bl = handle.get_chunk_producer_blacklist(&h[early as usize]).unwrap();
    assert!(early_bl.is_empty(), "early anchor should have an empty blacklist, got {early_bl:?}");
    let early_height = early + CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET;
    let plain = epoch_info
        .get_validator(epoch_info.sample_chunk_producer(&layout, shard_id, early_height).unwrap());
    let stored_early = handle
        .get_chunk_producer_info_anchored(
            Some(&h[early as usize]),
            &epoch_id,
            early_height,
            shard_id,
        )
        .unwrap();
    assert_eq!(stored_early, plain, "empty-blacklist row must equal the plain height sample");

    // Late window: blacklist is {0}. No seeded row may be the down node, even at heights
    // where the plain sampler would have picked it -> proves exclusion is applied.
    let late_bl = handle.get_chunk_producer_blacklist(&h[count as usize]).unwrap();
    assert_eq!(
        late_bl,
        HashMap::from([(shard_id, HashSet::from([0]))]),
        "late anchor should blacklist validator 0"
    );
    let mut plain_would_pick_down = false;
    for i in (count - 40)..=count {
        let anchor = h[i as usize];
        let ch = i + CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET;
        let stored = handle
            .get_chunk_producer_info_anchored(Some(&anchor), &epoch_id, ch, shard_id)
            .unwrap();
        assert_ne!(
            epoch_info.get_validator_id(stored.account_id()).copied(),
            Some(0),
            "no seeded row in the blacklist window may be the down node (anchor height {i})"
        );
        if epoch_info.sample_chunk_producer(&layout, shard_id, ch) == Some(0) {
            plain_would_pick_down = true;
        }
    }
    assert!(
        plain_would_pick_down,
        "expected a height where the plain sampler picks the down node, else exclusion is untested"
    );
}

// Missing-row invariant: wherever the blacklist as of an anchor is non-empty, that anchor's
// `DBCol::ChunkProducers` rows are present for every shard. So the
// aggregator's lenient reader never height-samples (which would re-credit the down node)
// while a blacklist is active -- the missing-row region and the non-empty-blacklist region
// are disjoint.
#[cfg(feature = "nightly")]
#[test]
fn nonempty_blacklist_anchor_always_has_row() {
    let validators = vec![("test0".parse().unwrap(), STAKE), ("test1".parse().unwrap(), STAKE)];
    let handle = setup_default_epoch_manager(validators, 10_000, 1, 3, 90, 60).into_handle();
    // Drive past the 1000-block grace so the late-window anchors have an active blacklist.
    let count = 1200;
    let h = drive_down_node(&handle, count, 0);
    let epoch_id = handle.get_epoch_id(&h[1]).unwrap();
    let layout = handle.get_shard_layout(&epoch_id).unwrap();

    let mut checked = 0;
    for i in 1..=count {
        let anchor = h[i as usize];
        let bl = handle.get_chunk_producer_blacklist(&anchor).unwrap();
        if bl.is_empty() {
            continue;
        }
        checked += 1;
        // The strict anchored reader errors `ChunkProducerNotInDB` on a miss, so `Ok`
        // proves the row is present (the lenient aggregator path would never fall back
        // to height sampling here).
        for shard_id in layout.shard_ids() {
            let res = handle.get_chunk_producer_info_anchored(
                Some(&anchor),
                &epoch_id,
                i + CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET,
                shard_id,
            );
            assert!(
                res.is_ok(),
                "anchor at height {i} has a non-empty blacklist but no ChunkProducers row for shard {shard_id}: {res:?}"
            );
        }
    }
    assert!(checked > 0, "no non-empty-blacklist anchor exercised; test is vacuous");
}

// Per-shard isolation: with 2 shards, driving shard 0's producer down blacklists it on
// shard 0 only and leaves shard 1 healthy. Exercises the seeder's per-shard blacklist + loop
// (every other early_kickout test is single-shard) and the reassignment metric.
#[cfg(feature = "nightly")]
#[test]
fn per_shard_blacklist_isolated() {
    use crate::metrics::EARLY_KICKOUT_CHUNK_PRODUCER_REASSIGNED;
    let validators = vec![
        ("test0".parse().unwrap(), STAKE),
        ("test1".parse().unwrap(), STAKE),
        ("test2".parse().unwrap(), STAKE),
        ("test3".parse().unwrap(), STAKE),
    ];
    let handle = setup_default_epoch_manager(validators, 10_000, 2, 4, 90, 60).into_handle();
    // Drive past the 1000-block grace so shard 0's blacklist activates.
    let count = 1200u64;
    let h: Vec<CryptoHash> = (0..=count).map(|i| hash(&i.to_le_bytes())).collect();
    record_block(&mut handle.write(), CryptoHash::default(), h[0], 0, vec![]);
    let epoch_id = handle.get_epoch_id(&h[0]).unwrap();
    let layout = handle.get_shard_layout(&epoch_id).unwrap();
    let epoch_info = handle.get_epoch_info(&epoch_id).unwrap();
    let shards: Vec<_> = layout.shard_ids().collect();
    assert_eq!(shards.len(), 2, "test needs a 2-shard layout");
    let (shard0, shard1) = (shards[0], shards[1]);
    // Down target = a producer on shard 0; the shard needs >= 2 producers so the safety
    // valve permits blacklisting one.
    let s0_index = layout.get_shard_index(shard0).unwrap();
    let s0_producers = epoch_info.chunk_producers_settlement()[s0_index].clone();
    assert!(
        s0_producers.len() >= 2,
        "shard 0 needs >= 2 producers to blacklist one, got {s0_producers:?}"
    );
    let down = s0_producers[0];

    let empty = HashSet::new();
    let shard0_label = shard0.to_string();
    let before = EARLY_KICKOUT_CHUNK_PRODUCER_REASSIGNED.with_label_values(&[&shard0_label]).get();
    let mut prev = h[0];
    for height in 1..=count {
        let bl = handle.get_chunk_producer_blacklist(&prev).unwrap();
        // Shard 0: miss whenever the (blacklist-aware) assignment is the down producer.
        let assigned0 = epoch_info
            .sample_chunk_producer_excluding(
                &layout,
                shard0,
                height,
                bl.get(&shard0).unwrap_or(&empty),
            )
            .unwrap();
        let produced0 = assigned0 != down;
        // Shard 1: always produced.
        record_block_with_mask(
            &mut handle.write(),
            prev,
            h[height as usize],
            height,
            vec![produced0, true],
        );
        prev = h[height as usize];
    }

    // Shard 0 blacklists the down producer; shard 1 blacklists nobody.
    let bl = handle.get_chunk_producer_blacklist(&prev).unwrap();
    assert_eq!(
        bl.get(&shard0),
        Some(&HashSet::from([down])),
        "shard 0 must blacklist exactly the down producer, got {bl:?}"
    );
    assert_eq!(bl.get(&shard1), None, "shard 1 must not blacklist anyone, got {bl:?}");

    // The reassignment metric fired for shard 0 (delta, since the counter is process-global).
    let after = EARLY_KICKOUT_CHUNK_PRODUCER_REASSIGNED.with_label_values(&[&shard0_label]).get();
    assert!(after > before, "reassignment metric must increment for shard 0 ({before} -> {after})");

    // Seeded rows: in the blacklist window, shard 0's row never the down producer (proven at a
    // height where the plain sampler would pick it), while shard 1's row equals the plain pick.
    let mut plain_would_pick_down = false;
    for i in (count - 40)..=count {
        let anchor = h[i as usize];
        let ch = i + CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET;
        let s0_stored =
            handle.get_chunk_producer_info_anchored(Some(&anchor), &epoch_id, ch, shard0).unwrap();
        assert_ne!(
            epoch_info.get_validator_id(s0_stored.account_id()).copied(),
            Some(down),
            "shard 0 row must exclude the down producer (anchor height {i})"
        );
        let s1_stored =
            handle.get_chunk_producer_info_anchored(Some(&anchor), &epoch_id, ch, shard1).unwrap();
        let s1_plain = epoch_info
            .get_validator(epoch_info.sample_chunk_producer(&layout, shard1, ch).unwrap());
        assert_eq!(s1_stored, s1_plain, "shard 1 row must equal the plain pick (no blacklist)");
        if epoch_info.sample_chunk_producer(&layout, shard0, ch) == Some(down) {
            plain_would_pick_down = true;
        }
    }
    assert!(
        plain_would_pick_down,
        "expected a height where the plain sampler picks the down producer on shard 0, else \
         exclusion is untested"
    );
}

/// Records a block whose finality is pinned to `(final_hash, final_height)` with all chunks
/// produced. Holding those fixed across many blocks freezes `largest_final_height`, so
/// `record_block_info`'s incremental aggregator update is skipped and the per-block seed walk
/// re-scans the growing not-yet-finalized suffix — the finality-stall regime.
#[cfg(feature = "nightly")]
fn record_block_frozen_final(
    em: &mut EpochManager,
    prev: CryptoHash,
    cur: CryptoHash,
    height: u64,
    final_hash: CryptoHash,
    final_height: u64,
) {
    let epoch_id = em.get_epoch_id(&prev).unwrap();
    let shard_layout = em.get_shard_layout(&epoch_id).unwrap();
    let chunk_mask = vec![true; shard_layout.shard_ids().count()];
    record_block_with_final_and_mask(em, prev, cur, height, final_hash, final_height, chunk_mask);
}

// Regression guard: with finality frozen the seeder walks only to the pinned last-final block,
// so per-block cost is O(1) and total is linear, not the old O(stall-depth) suffix re-walk. Two
// stall depths check the per-block walk does not grow with depth.
#[cfg(feature = "nightly")]
#[test]
fn seed_walk_bounded_under_finality_stall() {
    use std::sync::atomic::Ordering;

    // Total per-block seeding walk iterations over a `count`-block stall frozen at genesis.
    fn stall_walk(count: u64) -> usize {
        let validators = vec![("test0".parse().unwrap(), STAKE), ("test1".parse().unwrap(), STAKE)];
        let mut em = setup_default_epoch_manager(validators, 10_000, 1, 3, 90, 60);
        let h: Vec<CryptoHash> = (0..=count).map(|i| hash(&i.to_le_bytes())).collect();
        record_block(&mut em, CryptoHash::default(), h[0], 0, vec![]);
        let before = em.epoch_info_aggregator_loop_counter.load(Ordering::SeqCst);
        for height in 1..=count {
            record_block_frozen_final(
                &mut em,
                h[(height - 1) as usize],
                h[height as usize],
                height,
                h[0],
                0,
            );
        }
        em.epoch_info_aggregator_loop_counter.load(Ordering::SeqCst) - before
    }

    let short = 40u64;
    let long = 120u64;
    let walked_short = stall_walk(short);
    let walked_long = stall_walk(long);
    let per_block_cap = 4;
    assert!(
        walked_short >= short as usize,
        "seed walk should touch >= 1 block per recorded block, got {walked_short}"
    );
    assert!(
        walked_short <= per_block_cap * short as usize,
        "short stall walk {walked_short} exceeds {per_block_cap}/block — suffix re-walk regression?"
    );
    assert!(
        walked_long <= per_block_cap * long as usize,
        "long stall walk {walked_long} exceeds {per_block_cap}/block — suffix re-walk regression?"
    );
    // Linearity: 3x the depth must not more than 3x the walk (a quadratic re-walk would ~9x).
    assert!(
        walked_long * short as usize <= 2 * walked_short * long as usize,
        "per-block walk grew with stall depth ({walked_short} over {short} vs {walked_long} over \
         {long}) — finality-stall suffix re-walk regression?"
    );
}

/// Drives `count` blocks in epoch 0 where the single shard's chunk is ALWAYS missed,
/// so every scheduled producer accumulates 0 produced / many expected -> all
/// producers become blacklist candidates (all-bad shard). Returns block hashes.
#[cfg(feature = "nightly")]
fn drive_all_chunks_missed(handle: &EpochManagerHandle, count: u64) -> Vec<CryptoHash> {
    let h: Vec<CryptoHash> = (0..=count).map(|i| hash(&i.to_le_bytes())).collect();
    record_block(&mut handle.write(), CryptoHash::default(), h[0], 0, vec![]);
    let mut prev = h[0];
    for height in 1..=count {
        record_block_with_mask(&mut handle.write(), prev, h[height as usize], height, vec![false]);
        prev = h[height as usize];
    }
    h
}

// 12. v152+ protocol: an all-bad shard fires the safety valve on the SEEDER (the
//     production write path that runs once per recorded block), so the
//     `safety_valve_fired` counter increments as blocks are recorded. The accessor
//     then applies keep-one, keeping exactly one of the two producers eligible.
#[cfg(feature = "nightly")]
#[test]
fn seed_chunk_producers_fires_safety_valve_metric() {
    let validators = vec![("test0".parse().unwrap(), STAKE), ("test1".parse().unwrap(), STAKE)];
    let handle = setup_default_epoch_manager(validators, 10_000, 1, 3, 90, 60).into_handle();
    // Single shard; its label is stable across the whole drive (epoch length 10_000
    // keeps every block in epoch 0).
    let shard_id =
        handle.get_shard_layout(&EpochId::default()).unwrap().shard_ids().next().unwrap();
    let label = shard_id.to_string();
    use crate::metrics::EARLY_KICKOUT_SAFETY_VALVE_FIRED;
    // Snapshot before driving: the counter is a process-global monotonic counter, and
    // this is the only test whose seeder fires the valve for this shard (down-one-node
    // tests never blacklist every producer), so a strict increase is the robust check.
    let before = EARLY_KICKOUT_SAFETY_VALVE_FIRED.with_label_values(&[label.as_str()]).get();
    // drive past the 1000-block start-of-epoch grace (epoch length 10_000 keeps it all in
    // epoch 0) so the seeder applies the blacklist and fires the valve.
    let h = drive_all_chunks_missed(&handle, 1200);
    let after = EARLY_KICKOUT_SAFETY_VALVE_FIRED.with_label_values(&[label.as_str()]).get();
    assert!(
        after > before,
        "seeder must fire the safety-valve counter on an all-bad shard: {before} -> {after}"
    );
    let prev = *h.last().unwrap();
    let bl = handle.get_chunk_producer_blacklist(&prev).unwrap();
    assert_eq!(bl.len(), 1, "expected exactly one shard in the blacklist, got {bl:?}");
    assert_eq!(bl[&shard_id].len(), 1, "keep-one must blacklist exactly one of two producers");
}

/// The state epoch sync leaves behind: the prev epoch's first, second-last and last
/// `BlockInfo` are installed, the third-last one deliberately is NOT, even though it is the
/// aggregator position an anchor can be final on.
#[cfg(feature = "nightly")]
struct EpochSyncFixture {
    em: EpochManager,
    prev_epoch_id: EpochId,
    epoch_id: EpochId,
    epoch_info: EpochInfo,
    shard_layout: ShardLayout,
    /// The prev epoch's first block; every installed prev-epoch `BlockInfo` points here.
    first: CryptoHash,
    /// Parent of the prev epoch's last block.
    second_last: CryptoHash,
    /// The prev epoch's last block.
    last: CryptoHash,
    /// The uninstalled aggregator position.
    third_last: CryptoHash,
    third_last_height: u64,
}

#[cfg(feature = "nightly")]
impl EpochSyncFixture {
    /// An anchor a few blocks past the boundary, final on the uninstalled aggregator
    /// position — the shape all epoch-sync regression tests below exercise.
    fn seed_anchor(&self, hash: CryptoHash) -> SeedAnchor {
        SeedAnchor {
            hash,
            height: self.third_last_height + 4,
            final_hash: self.third_last,
            final_height: self.third_last_height,
        }
    }
}

#[cfg(feature = "nightly")]
fn epoch_sync_fixture() -> EpochSyncFixture {
    let validators = vec![("test0".parse().unwrap(), STAKE), ("test1".parse().unwrap(), STAKE)];
    let mut em = setup_default_epoch_manager(validators, 10, 1, 3, 90, 60);
    let epoch_info = em.get_epoch_info(&EpochId::default()).unwrap().as_ref().clone();
    let shard_layout = em.get_shard_layout(&EpochId::default()).unwrap();

    const PREV_EPOCH_FIRST_HEIGHT: u64 = 90;
    const PREV_EPOCH_LAST_HEIGHT: u64 = 99;
    let third_last_height = PREV_EPOCH_LAST_HEIGHT - 2;

    let prev_epoch_id = EpochId(hash(b"prev epoch"));
    let epoch_id = EpochId(hash(b"current epoch"));
    let next_epoch_id = EpochId(hash(b"next epoch"));
    let first = hash(b"prev epoch first block");
    let third_last = hash(b"prev epoch third-last block");
    let second_last = hash(b"prev epoch second-last block");
    let last = hash(b"prev epoch last block");

    // Mirrors what the epoch-sync proof installs, bypassing `record_block_info`.
    let prev_epoch_block = |cur: CryptoHash, height: u64, prev: CryptoHash| {
        let mut info = BlockInfo::new(
            cur,
            height,
            height.saturating_sub(2),
            CryptoHash::default(),
            prev,
            vec![],
            vec![true],
            DEFAULT_TOTAL_SUPPLY,
            PROTOCOL_VERSION,
            PROTOCOL_VERSION,
            height * NUM_NS_IN_SECOND,
            ChunkEndorsementsBitmap::new(1),
            None,
        );
        *info.epoch_id_mut() = prev_epoch_id;
        *info.epoch_first_block_mut() = first;
        info
    };

    let mut store_update = em.store.store_update();
    em.init_after_epoch_sync(
        &mut store_update,
        prev_epoch_block(first, PREV_EPOCH_FIRST_HEIGHT, hash(b"block before prev epoch")),
        prev_epoch_block(second_last, PREV_EPOCH_LAST_HEIGHT - 1, third_last),
        prev_epoch_block(last, PREV_EPOCH_LAST_HEIGHT, second_last),
        &prev_epoch_id,
        epoch_info.clone(),
        &epoch_id,
        epoch_info.clone(),
        &next_epoch_id,
        epoch_info.clone(),
    )
    .unwrap();
    store_update.commit();

    EpochSyncFixture {
        em,
        prev_epoch_id,
        epoch_id,
        epoch_info,
        shard_layout,
        first,
        second_last,
        last,
        third_last,
        third_last_height,
    }
}

// Post-epoch-sync regression: the aggregator sits on the prev epoch's third-last block,
// whose `BlockInfo` is deliberately never installed. An anchor final on that position is a
// legitimate state; the seeder's cross-epoch early-return must fire before the epoch-start
// walk, which would fail with `MissingBlock` there.
#[cfg(feature = "nightly")]
#[test]
fn seeder_tolerates_post_epoch_sync_aggregator_anchor() {
    let fx = epoch_sync_fixture();

    // Final block == the aggregator position: the aggregator walk short-circuits and
    // returns the prev-epoch aggregator, mismatching the (current) sample epoch.
    let anchor = fx.seed_anchor(hash(b"current epoch block"));
    let mut seed_update = fx.em.store.store_update();
    fx.em
        .seed_chunk_producers(
            &mut seed_update,
            &anchor,
            SampleEpoch {
                epoch_id: &fx.epoch_id,
                epoch_info: &fx.epoch_info,
                shard_layout: &fx.shard_layout,
            },
        )
        .expect("cross-epoch anchor after epoch sync must seed, not error");
    seed_update.commit();

    // Empty blacklist -> the seeded row is the canonical sample at the anchor offset.
    let shard_id = fx.shard_layout.shard_ids().next().unwrap();
    let key = get_block_shard_id(&anchor.hash, shard_id);
    let seeded = fx
        .em
        .store
        .store_ref()
        .get_ser::<ValidatorStake>(DBCol::ChunkProducers, &key)
        .expect("seeder must write the ChunkProducers row for the anchor");
    let canonical = fx
        .epoch_info
        .sample_chunk_producer(
            &fx.shard_layout,
            shard_id,
            anchor.height + CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET,
        )
        .unwrap();
    assert_eq!(
        seeded.account_id(),
        fx.epoch_info.get_validator(canonical).account_id(),
        "empty blacklist must seed the canonical sample",
    );
}

// Companion to the test above, same missing `BlockInfo` but with the sample epoch equal to
// the aggregator's epoch, so the cross-epoch early-return does NOT fire and the epoch-start
// walk runs. A missing block there is structural corruption: the seeder must propagate the
// error, not silently fall back to treating the epoch as just-started (grace).
#[cfg(feature = "nightly")]
#[test]
fn seeder_propagates_missing_block_info_on_same_epoch_basis() {
    let fx = epoch_sync_fixture();

    let anchor = fx.seed_anchor(hash(b"prev epoch extra block"));
    let mut seed_update = fx.em.store.store_update();
    let err = fx
        .em
        .seed_chunk_producers(
            &mut seed_update,
            &anchor,
            SampleEpoch {
                epoch_id: &fx.prev_epoch_id,
                epoch_info: &fx.epoch_info,
                shard_layout: &fx.shard_layout,
            },
        )
        .expect_err("a missing BlockInfo on a same-epoch basis must propagate");
    assert_eq!(
        err,
        EpochError::MissingBlock(fx.third_last),
        "expected the missing basis block to propagate verbatim, got {err:?}",
    );
}

/// Input for the `record_block_info` failure tests, final on the aggregator position the
/// fixture leaves uninstalled. Each call needs a fresh one: record consumes the value and
/// overwrites its epoch fields.
#[cfg(feature = "nightly")]
fn record_input_info(fx: &EpochSyncFixture, hash: CryptoHash, prev: CryptoHash) -> BlockInfo {
    const HEIGHT: u64 = 100;
    BlockInfo::new(
        hash,
        HEIGHT,
        fx.third_last_height,
        fx.third_last,
        prev,
        vec![],
        vec![true],
        DEFAULT_TOTAL_SUPPLY,
        PROTOCOL_VERSION,
        PROTOCOL_VERSION,
        HEIGHT * NUM_NS_IN_SECOND,
        ChunkEndorsementsBitmap::new(1),
        None,
    )
}

// A failed record must not leave the block in `blocks_info`: the caller drops the store
// update, so a surviving entry serves other readers a block that was never written.
// Reachable after epoch sync, where the seeder fails with `MissingBlock`.
#[cfg(feature = "nightly")]
#[test]
fn record_block_info_failure_does_not_poison_block_info_cache() {
    let mut fx = epoch_sync_fixture();

    // Sibling of the prev epoch's last block: same epoch as its parent, so the seeder walks
    // back to the third-last block, which epoch sync never installed.
    let sibling = hash(b"prev epoch last block sibling");
    let err = fx
        .em
        .record_block_info(record_input_info(&fx, sibling, fx.second_last), [0; 32])
        .map(|_| ())
        .expect_err("the seeder walk must fail on the uninstalled aggregator position");
    assert_eq!(err, EpochError::MissingBlock(fx.third_last), "unexpected error: {err:?}");

    let err = fx.em.get_block_info(&sibling).map(|_| ()).expect_err(
        "a discarded record must not leave the block in the cache, or the retry is skipped",
    );
    assert_eq!(err, EpochError::MissingBlock(sibling), "unexpected error: {err:?}");

    let err = fx
        .em
        .record_block_info(record_input_info(&fx, sibling, fx.second_last), [0; 32])
        .map(|_| ())
        .expect_err("the retry must fail the same way, not be skipped as already recorded");
    assert_eq!(err, EpochError::MissingBlock(fx.third_last), "unexpected error: {err:?}");
}

// Same for the epoch-start branch, where `save_epoch_start` now runs after the last fallible
// step. Fault injection: the fixture's current-epoch id is an arbitrary label, so the id the
// boundary derives has no `EpochInfo` and the record fails where the epoch-start write used
// to precede it. Not a state epoch sync can produce.
#[cfg(feature = "nightly")]
#[test]
fn record_block_info_failure_does_not_poison_epoch_start_cache() {
    let mut fx = epoch_sync_fixture();

    // What the boundary derives: the hash of the block before the prev epoch's first.
    let boundary_id = EpochId(*fx.em.get_block_info(&fx.first).unwrap().prev_hash());

    // Child of the prev epoch's last block, so the record takes the epoch-start branch.
    let child = hash(b"first block of the new epoch");
    let err = fx
        .em
        .record_block_info(record_input_info(&fx, child, fx.last), [0; 32])
        .map(|_| ())
        .expect_err("the derived boundary epoch has no EpochInfo installed");
    assert_eq!(err, EpochError::EpochOutOfBounds(boundary_id), "unexpected error: {err:?}");

    let err = fx
        .em
        .get_epoch_start_from_epoch_id(&boundary_id)
        .expect_err("a discarded record must not leave an epoch start behind");
    assert_eq!(err, EpochError::EpochOutOfBounds(boundary_id), "unexpected error: {err:?}");
    fx.em
        .get_block_info(&child)
        .map(|_| ())
        .expect_err("a discarded record must not leave the block in the cache");

    let err = fx
        .em
        .record_block_info(record_input_info(&fx, child, fx.last), [0; 32])
        .map(|_| ())
        .expect_err("the retry must fail the same way, not be skipped as already recorded");
    assert_eq!(err, EpochError::EpochOutOfBounds(boundary_id), "unexpected error: {err:?}");
}

// `has_block_info` reads the store, not the `blocks_info` cache, so a record whose update the
// caller drops is recorded again on retry. The chain drops it whenever a later step of the
// same block processing fails, not only when the record itself does.
//
// Covers the first real block: the default final hash makes the seeder return early, and
// nothing advances the aggregator or finalizes an epoch, so the retry is a plain re-record.
#[test]
fn record_block_info_dropped_update_is_recorded_on_retry() {
    let validators = vec![("test0".parse().unwrap(), STAKE), ("test1".parse().unwrap(), STAKE)];
    let mut em = setup_default_epoch_manager(validators, 10, 1, 3, 90, 60);
    let genesis = hash(b"genesis");
    record_block(&mut em, CryptoHash::default(), genesis, 0, vec![]);

    let b1 = hash(b"first real block");
    let block_info = || {
        BlockInfo::new(
            b1,
            1,
            0,
            CryptoHash::default(),
            genesis,
            vec![],
            vec![true],
            DEFAULT_TOTAL_SUPPLY,
            PROTOCOL_VERSION,
            PROTOCOL_VERSION,
            NUM_NS_IN_SECOND,
            ChunkEndorsementsBitmap::new(1),
            None,
        )
    };

    // Dropping the update stands in for a caller that fails after the record returned.
    em.record_block_info(block_info(), [0; 32]).map(|_| ()).expect("first record must succeed");

    // Pre-fix the cached entry made this a no-op that returned an empty update.
    let store_update = em.record_block_info(block_info(), [0; 32]).expect("retry must succeed");
    store_update.commit();
    em.store.get_block_info(&b1).expect("the retry must write the row it skipped before");
}

/// Builds the anchor `BlockInfo` the accessor tests install: same shape the fixture's
/// proof blocks have, final on the uninstalled aggregator position.
#[cfg(feature = "nightly")]
fn accessor_anchor_info(
    fx: &EpochSyncFixture,
    anchor_hash: CryptoHash,
    prev: CryptoHash,
) -> BlockInfo {
    let anchor_height = fx.third_last_height + 4;
    BlockInfo::new(
        anchor_hash,
        anchor_height,
        fx.third_last_height,
        fx.third_last,
        prev,
        vec![],
        vec![true],
        DEFAULT_TOTAL_SUPPLY,
        PROTOCOL_VERSION,
        PROTOCOL_VERSION,
        anchor_height * NUM_NS_IN_SECOND,
        ChunkEndorsementsBitmap::new(1),
        None,
    )
}

// Accessor-side companion to `seeder_tolerates_post_epoch_sync_aggregator_anchor`: the
// same epoch-sync miss state pinned through `get_chunk_producer_blacklist`, so the
// read-side contract stays enforced even if the accessor and seeder ever stop sharing
// `chunk_producer_blacklist_at_anchor`.
#[cfg(feature = "nightly")]
#[test]
fn accessor_tolerates_post_epoch_sync_aggregator_anchor() {
    let fx = epoch_sync_fixture();

    // A current-epoch anchor final on the uninstalled position, installed directly like
    // the fixture's proof blocks (the accessor reads the anchor's own `BlockInfo`).
    let anchor_hash = hash(b"current epoch block");
    let mut info = accessor_anchor_info(&fx, anchor_hash, fx.last);
    *info.epoch_id_mut() = fx.epoch_id;
    *info.epoch_first_block_mut() = anchor_hash;
    let mut store_update = fx.em.store.store_update();
    store_update.set_block_info(&info);
    store_update.commit();

    let handle = fx.em.into_handle();
    let blacklist = handle
        .get_chunk_producer_blacklist(&anchor_hash)
        .expect("cross-epoch anchor after epoch sync must read empty, not error");
    assert!(
        blacklist.is_empty(),
        "cross-epoch basis must read an empty blacklist, got {blacklist:?}",
    );
}

// Accessor-side companion to `seeder_propagates_missing_block_info_on_same_epoch_basis`.
#[cfg(feature = "nightly")]
#[test]
fn accessor_propagates_missing_block_info_on_same_epoch_basis() {
    let fx = epoch_sync_fixture();

    let anchor_hash = hash(b"prev epoch extra block");
    let mut info = accessor_anchor_info(&fx, anchor_hash, fx.second_last);
    *info.epoch_id_mut() = fx.prev_epoch_id;
    *info.epoch_first_block_mut() = fx.first;
    let mut store_update = fx.em.store.store_update();
    store_update.set_block_info(&info);
    store_update.commit();

    let handle = fx.em.into_handle();
    let err = handle
        .get_chunk_producer_blacklist(&anchor_hash)
        .expect_err("a missing BlockInfo on a same-epoch basis must propagate");
    assert_eq!(
        err,
        EpochError::MissingBlock(fx.third_last),
        "expected the missing basis block to propagate verbatim, got {err:?}",
    );
}

// 13. Resolving mixed parent/child shard ids against the layout the blacklist is computed
//     for. One `shard_tracker` carrying ids from both layouts is fed to each layout: every
//     valid shard keeps its own-settlement candidate, ids with no shard index there are
//     dropped. Distinct per-shard settlements make it discriminating — an identical `0..n`
//     settlement everywhere would hide a valid shard resolved to a wrong-but-in-range index.
//     Run over both V2 and V3, since production resharding derives V3 and `get_shard_index` +
//     settlement lookup behave the same in both. Unit coverage only: no epoch boundary is
//     driven and no split is finalized.
#[test]
fn blacklist_resharding_maps_to_current_layout_shard_ids() {
    // Explicit non-identity layout so `shard_id != shard_index` by construction. Parent
    // splits at "mmm" with ids [1, 0]; adding boundary "aaa" (sorts first) retires shard 1
    // into [2, 3] and keeps shard 0. V2 and V3 derive the same ids; pin them first.
    let split_boundary: AccountId = "aaa".parse().unwrap();
    let parent =
        ShardLayout::v2(vec!["mmm".parse().unwrap()], vec![ShardId::new(1), ShardId::new(0)], None);
    assert_eq!(
        parent.shard_ids().collect::<Vec<_>>(),
        vec![ShardId::new(1), ShardId::new(0)],
        "parent shard ids",
    );
    let v2_child = ShardLayout::derive_shard_layout(&parent, split_boundary.clone());
    let v3_child = parent.derive_v3(split_boundary, || vec![]).unwrap();
    for child in [&v2_child, &v3_child] {
        assert_eq!(
            child.shard_ids().collect::<Vec<_>>(),
            vec![ShardId::new(2), ShardId::new(3), ShardId::new(0)],
            "child shard ids",
        );
    }

    reshard_case(&parent, &v2_child);
    reshard_case(&parent, &v3_child);
}

/// One reshard mapping check for a `parent`/`child` layout pair (the caller pins the
/// concrete shard ids first). Derives the retired parent, survivor and children by set
/// difference, feeds one shared `shard_tracker` to both layouts, and asserts that each
/// keeps its valid shards' candidates and drops the ids foreign to it.
fn reshard_case(parent: &ShardLayout, child: &ShardLayout) {
    let parent_ids: Vec<ShardId> = parent.shard_ids().collect();
    let child_ids: Vec<ShardId> = child.shard_ids().collect();

    let retired: Vec<ShardId> =
        parent_ids.iter().copied().filter(|id| !child_ids.contains(id)).collect();
    let survivors: Vec<ShardId> =
        parent_ids.iter().copied().filter(|id| child_ids.contains(id)).collect();
    assert_eq!(retired.len(), 1, "exactly one parent shard is split/retired, got {retired:?}");
    assert_eq!(survivors.len(), 1, "exactly one parent shard survives, got {survivors:?}");
    let split_parent = retired[0];
    let surviving = survivors[0];
    let children = child.get_children_shards_ids(split_parent).expect("split parent has children");
    assert_eq!(children.len(), 2, "a split yields two children, got {children:?}");
    for c in &children {
        assert!(!parent_ids.contains(c), "child id {c} must not reuse a parent shard id");
        assert_eq!(
            child.get_parent_shard_id(*c).unwrap(),
            split_parent,
            "child {c} must map to the retired parent {split_parent}",
        );
    }

    // The drop is a layout-level `get_shard_index` miss: a retired parent lives only in
    // the split map, a child does not exist in the parent, so neither resolves.
    assert!(
        child.get_shard_index(split_parent).is_err(),
        "retired parent {split_parent} must have no shard index in the child layout",
    );
    assert!(
        parent.get_shard_index(children[0]).is_err(),
        "child {} must have no shard index in the parent layout",
        children[0],
    );

    // One bad producer per shard plus a shared healthy producer (id 4). The settlement Vec
    // is indexed by ShardIndex, so build it through `shard_infos()` (the API's index-order
    // contract) rather than `shard_ids()`, and never through `get_shard_index` (the mechanism
    // under test). Each valid shard's bad producer sits in that shard's own settlement.
    const HEALTHY: ValidatorId = 4;
    let num_producers = 5u64;
    let bad_producer = |shard: ShardId| -> ValidatorId {
        if shard == ShardId::new(1) {
            0
        } else if shard == ShardId::new(0) {
            1
        } else if shard == ShardId::new(2) {
            2
        } else if shard == ShardId::new(3) {
            3
        } else {
            panic!("unexpected shard id {shard}")
        }
    };
    let settlement_for =
        |shard: ShardId| -> Vec<ValidatorId> { vec![bad_producer(shard), HEALTHY] };
    let settlements = |layout: &ShardLayout| -> Vec<Vec<ValidatorId>> {
        layout.shard_infos().map(|info| settlement_for(info.shard_id())).collect()
    };
    let parent_ei = epoch_info_for_layout(parent, settlements(parent), num_producers);
    let child_ei = epoch_info_for_layout(child, settlements(child), num_producers);

    // One tracker carrying every shard id from both layouts. 0/100 clears both gates
    // (100 missed, 0% ratio); the healthy id is 100/100. One candidate per shard, so the
    // keep-one valve never fires (`kept == None`).
    let candidate_shard = |shard: ShardId| -> HashMap<ValidatorId, ChunkStats> {
        HashMap::from([
            (bad_producer(shard), ChunkStats::new_with_production(0, 100)),
            (HEALTHY, ChunkStats::new_with_production(100, 100)),
        ])
    };
    let all_shards: HashSet<ShardId> = parent_ids.iter().chain(child_ids.iter()).copied().collect();
    let st: HashMap<ShardId, HashMap<ValidatorId, ChunkStats>> =
        all_shards.iter().map(|&shard| (shard, candidate_shard(shard))).collect();

    // Parent layout: shards 1 and 0 resolve, the two child ids are foreign and dropped.
    let parent_result = compute_chunk_producer_blacklist(&st, &parent_ei, parent);
    assert_eq!(
        parent_result.blacklist,
        HashMap::from([
            (split_parent, HashSet::from([bad_producer(split_parent)])),
            (surviving, HashSet::from([bad_producer(surviving)])),
        ]),
        "parent layout must blacklist each valid shard's own-settlement candidate",
    );
    assert_eq!(
        shard_stats_projection(&parent_result),
        HashMap::from([(split_parent, (1, None)), (surviving, (1, None))]),
        "parent layout stats: one candidate per valid shard, valve not fired",
    );

    // Child layout: children and the survivor resolve, the retired parent id is dropped.
    let child_result = compute_chunk_producer_blacklist(&st, &child_ei, child);
    assert_eq!(
        child_result.blacklist,
        HashMap::from([
            (children[0], HashSet::from([bad_producer(children[0])])),
            (children[1], HashSet::from([bad_producer(children[1])])),
            (surviving, HashSet::from([bad_producer(surviving)])),
        ]),
        "child layout must blacklist each valid shard's own-settlement candidate",
    );
    assert_eq!(
        shard_stats_projection(&child_result),
        HashMap::from(
            [(children[0], (1, None)), (children[1], (1, None)), (surviving, (1, None)),]
        ),
        "child layout stats: one candidate per valid shard, valve not fired",
    );
}

/// A block hash on `branch` at `height`. The branch byte is a domain separator, so
/// sibling branches (and the shared prefix's branch 0) never collide, whatever heights
/// they reuse across the fork.
#[cfg(all(feature = "nightly", feature = "test_features"))]
fn fork_block_hash(branch: u8, height: u64) -> CryptoHash {
    hash(&[&[branch][..], &height.to_le_bytes()[..]].concat())
}

/// Records one block on `branch` at `height`, extending `prev`, using the same
/// blacklist-aware assignment as `drive_down_node`: the chunk is missed exactly when
/// `target` is the scheduled producer, so `target` accrues misses while its slots
/// reassign to producers that actually produce. Returns the new tip. Shared by the two
/// fork tests below (their only genuine overlap).
#[cfg(all(feature = "nightly", feature = "test_features"))]
fn fork_step(
    handle: &EpochManagerHandle,
    epoch_info: &EpochInfo,
    layout: &ShardLayout,
    shard_id: ShardId,
    prev: CryptoHash,
    branch: u8,
    height: u64,
    target: ValidatorId,
) -> CryptoHash {
    let cur = fork_block_hash(branch, height);
    let empty = HashSet::new();
    let blacklist = handle.get_chunk_producer_blacklist(&prev).unwrap();
    let assigned = epoch_info
        .sample_chunk_producer_excluding(
            layout,
            shard_id,
            height,
            blacklist.get(&shard_id).unwrap_or(&empty),
        )
        .unwrap();
    let produced = assigned != target;
    record_block_with_mask(&mut handle.write(), prev, cur, height, vec![produced]);
    cur
}

/// Drives one branch forward via `fork_step` until `done(tip)` holds, starting from
/// `(prev, height)`. Every phase of the fork tests is driven by an observable condition
/// rather than a fixed block count, because the sampler and the two-block finality lag
/// decide when a condition becomes true. Panics with the full fork state if `cap` steps
/// pass first. Returns the tip and its height.
#[cfg(all(feature = "nightly", feature = "test_features"))]
fn drive_until(
    handle: &EpochManagerHandle,
    epoch_info: &EpochInfo,
    layout: &ShardLayout,
    shard_id: ShardId,
    mut prev: CryptoHash,
    mut height: u64,
    branch: u8,
    target: ValidatorId,
    cap: u64,
    phase: &str,
    mut done: impl FnMut(&EpochManagerHandle, CryptoHash) -> bool,
) -> (CryptoHash, u64) {
    for _ in 0..cap {
        if done(handle, prev) {
            return (prev, height);
        }
        height += 1;
        prev = fork_step(handle, epoch_info, layout, shard_id, prev, branch, height, target);
    }
    if done(handle, prev) {
        return (prev, height);
    }
    let (basis, watermark, cache) = {
        let read = handle.read();
        let info = read.get_block_info(&prev).unwrap();
        (
            *info.last_final_block_hash(),
            read.largest_final_height,
            read.epoch_info_aggregator.last_block_hash,
        )
    };
    let target_stats = handle
        .read()
        .get_epoch_info_aggregator_upto_last(&basis)
        .ok()
        .and_then(|agg| agg.shard_tracker.get(&shard_id).and_then(|m| m.get(&target).cloned()))
        .map(|s| (s.produced(), s.expected()))
        .unwrap_or((0, 0));
    let blacklist = handle.get_chunk_producer_blacklist(&prev).unwrap();
    panic!(
        "phase {phase:?}: cap {cap} exceeded on branch {branch} at height {height}; anchor {prev} \
         basis {basis} cache {cache} watermark {watermark}; target {target} produced {} expected {} \
         missed {}; blacklist {blacklist:?}",
        target_stats.0,
        target_stats.1,
        target_stats.1.saturating_sub(target_stats.0),
    );
}

// v152+ protocol: two sibling forks off a shared prefix starve different producers, and the
// accessor (keyed on the anchor hash) resolves each fork to its own blacklist. Asserts the
// aggregator exit directly via `aggregate_epoch_info_upto`'s `full_info` flag rather than
// inferring it from blacklist equality.
//
// Lowered test-only thresholds (10 misses past a 20-block grace) keep it cheap. The prior
// fixture forked an equal-depth sibling at height 1; that ancient fork point pinned the cached
// aggregator to genesis, so every per-block seed walk ran to genesis and recording cost was
// quadratic in the sibling depth. Forking at the shared-prefix tip (at or above the cached
// last-final block) makes the sibling grow linearly, so real thresholds would also work here at
// ~1500 blocks — the lowered thresholds are for speed, not correctness. Real thresholds stay
// covered by the canonical tests.
#[cfg(all(feature = "nightly", feature = "test_features"))]
#[test]
fn get_chunk_producer_blacklist_isolates_abandoned_fork() {
    let _guard = set_early_kickout_thresholds_for_testing(Some(10), Some(20));
    // 3 validators so blacklisting two producers on the sibling does not trip the keep-one
    // valve; the contamination a leak would cause then shows up in the blacklist itself.
    let validators = vec![
        ("test0".parse().unwrap(), STAKE),
        ("test1".parse().unwrap(), STAKE),
        ("test2".parse().unwrap(), STAKE),
    ];
    // Epoch length large enough that everything stays inside epoch 0 (no boundary reset).
    let handle = setup_default_epoch_manager(validators, 10_000, 1, 3, 90, 60).into_handle();
    let epoch_id = EpochId::default();
    let layout = handle.get_shard_layout(&epoch_id).unwrap();
    let shard_id = layout.shard_ids().next().unwrap();
    let epoch_info = handle.get_epoch_info(&epoch_id).unwrap();
    // Premise for the {0,1} blacklist below: three distinct producers, so blacklisting two
    // leaves a survivor and the keep-one valve stays quiet.
    let shard_index = layout.get_shard_index(shard_id).unwrap();
    assert_eq!(
        epoch_info.chunk_producers_settlement()[shard_index].iter().collect::<HashSet<_>>().len(),
        3,
        "fork test needs a shard with exactly three distinct chunk producers",
    );

    let genesis = fork_block_hash(0, 0);
    record_block(&mut handle.write(), CryptoHash::default(), genesis, 0, vec![]);

    // Phase 1: shared prefix on branch 0, starving producer 0, until the accessor is
    // exactly {0}. The fork point is this prefix tip — at or above the cached last-final
    // block, so both forks descend from the cached position (linear seed walks).
    let only_0 = HashMap::from([(shard_id, HashSet::from([0]))]);
    let (fork_point, fork_point_height) = drive_until(
        &handle,
        epoch_info.as_ref(),
        &layout,
        shard_id,
        genesis,
        0,
        0,
        0,
        256,
        "shared prefix -> {0}",
        |h, tip| h.get_chunk_producer_blacklist(&tip).unwrap() == only_0,
    );

    // Phase 2: branch a canonical child (keeps starving 0) and a sibling child (starves 1)
    // off the shared fork point.
    let first_height = fork_point_height + 1;
    let canonical_first =
        fork_step(&handle, epoch_info.as_ref(), &layout, shard_id, fork_point, 1, first_height, 0);
    let sibling_first =
        fork_step(&handle, epoch_info.as_ref(), &layout, shard_id, fork_point, 2, first_height, 1);
    assert_ne!(canonical_first, sibling_first, "sibling branches must have distinct hashes");

    // Phase 3: aggregating to the first sibling block (not the accessor's basis for that
    // anchor, which is its last-final block) reaches the same-epoch cached-prefix sync point,
    // so `full_info` is false and the caller merges the cached prefix. Pin that the merge
    // actually ran: the merged producer-0 stats must equal cached-prefix + suffix exactly, and
    // the cached prefix must carry a real contribution (`expected > 0`). Asserting only that
    // the merged stats are nonzero would pass even without the merge, since the suffix can
    // itself contain producer-0 stats.
    let stats = |agg: &EpochInfoAggregator, id: ValidatorId| -> (u64, u64) {
        agg.shard_tracker
            .get(&shard_id)
            .and_then(|m| m.get(&id))
            .map_or((0, 0), |s| (s.produced(), s.expected()))
    };
    let prefix = handle.read().epoch_info_aggregator.clone();
    let (suffix, full_info) = handle
        .read()
        .aggregate_epoch_info_upto(&sibling_first)
        .unwrap()
        .expect("first sibling block differs from the cache position");
    assert!(!full_info, "first sibling block must hit the same-epoch cached-prefix exit");
    let merged = handle.read().get_epoch_info_aggregator_upto_last(&sibling_first).unwrap();
    let (prefix_0, suffix_0, merged_0) = (stats(&prefix, 0), stats(&suffix, 0), stats(&merged, 0));
    assert!(prefix_0.1 > 0, "cached prefix must carry producer 0's common-prefix stats");
    assert_eq!(
        merged_0,
        (prefix_0.0 + suffix_0.0, prefix_0.1 + suffix_0.1),
        "merged stats must be cached-prefix + suffix (proves merge_prefix ran)",
    );

    // Phase 4: extend the canonical branch until its anchor's last-final block is
    // canonical-only (past the shared prefix), then snapshot the canonical finality watermark.
    let (canonical_tip, _) = drive_until(
        &handle,
        epoch_info.as_ref(),
        &layout,
        shard_id,
        canonical_first,
        first_height,
        1,
        0,
        256,
        "canonical last-final becomes canonical-only",
        |h, tip| h.read().get_block_info(&tip).unwrap().last_finalized_height() > fork_point_height,
    );
    let canonical_watermark = handle.read().largest_final_height;

    // Phase 5: extend the sibling, starving producer 1, until it overtakes the canonical
    // watermark, the aggregator cache has moved onto the sibling (its `last_block_hash` is the
    // sibling's own last-final block), and its blacklist is exactly {0, 1}. The monotone
    // `largest_final_height` gate is what keeps the sibling from mutating the cache until it
    // genuinely overtakes; passing it replaces the cache with a fresh walk onto the sibling.
    let (sibling_tip, _) = drive_until(
        &handle,
        epoch_info.as_ref(),
        &layout,
        shard_id,
        sibling_first,
        first_height,
        2,
        1,
        256,
        "sibling overtakes and blacklist becomes {0,1}",
        |h, tip| {
            let (final_hash, final_height, cache) = {
                let read = h.read();
                let info = read.get_block_info(&tip).unwrap();
                (
                    *info.last_final_block_hash(),
                    info.last_finalized_height(),
                    read.epoch_info_aggregator.last_block_hash,
                )
            };
            if final_height <= canonical_watermark || cache != final_hash {
                return false;
            }
            h.get_chunk_producer_blacklist(&tip).unwrap().get(&shard_id)
                == Some(&HashSet::from([0, 1]))
        },
    );
    assert_ne!(canonical_tip, sibling_tip, "forks must have distinct anchor hashes");

    // Phase 6: the abandoned canonical anchor is unchanged. Its blacklist is still exactly
    // {0}, and aggregating to its own last-final basis now returns `full_info == true`: the
    // cache sits on the sibling, so the walk cannot reach the cached sync point and instead
    // walks the canonical chain from the epoch start.
    let canonical_bl = handle.get_chunk_producer_blacklist(&canonical_tip).unwrap();
    assert_eq!(
        canonical_bl,
        HashMap::from([(shard_id, HashSet::from([0]))]),
        "canonical blacklist must be unchanged after the sibling took over the cache",
    );
    let canonical_basis =
        *handle.read().get_block_info(&canonical_tip).unwrap().last_final_block_hash();
    let (_, canonical_full_info) = handle
        .read()
        .aggregate_epoch_info_upto(&canonical_basis)
        .unwrap()
        .expect("canonical basis differs from the sibling cache position");
    assert!(
        canonical_full_info,
        "after the sibling takeover the canonical basis must walk from the epoch start",
    );

    // Phase 7: the sibling resolves to its own {0, 1}; the valve did not fire (3 producers,
    // one survivor left).
    let sibling_bl = handle.get_chunk_producer_blacklist(&sibling_tip).unwrap();
    assert_eq!(
        sibling_bl,
        HashMap::from([(shard_id, HashSet::from([0, 1]))]),
        "sibling must resolve to its own blacklist, isolated from the canonical branch",
    );
}

/// Drives `branch` off the shared `fork_point`, starving `target`, until it blacklists
/// exactly `{target}`, then manufactures a non-vacuous anchor and asserts the seeded
/// `DBCol::ChunkProducers` row (the row consensus actually reads) reflects that branch's
/// blacklist: it excludes `target` at a height where the plain sampler would have picked it.
#[cfg(all(feature = "nightly", feature = "test_features"))]
fn assert_branch_seeds_own_blacklist(
    handle: &EpochManagerHandle,
    epoch_info: &EpochInfo,
    layout: &ShardLayout,
    shard_id: ShardId,
    epoch_id: &EpochId,
    fork_point: CryptoHash,
    fork_point_height: u64,
    branch: u8,
    target: ValidatorId,
) {
    let only_target = HashMap::from([(shard_id, HashSet::from([target]))]);
    let (mut tip, mut height) = drive_until(
        handle,
        epoch_info,
        layout,
        shard_id,
        fork_point,
        fork_point_height,
        branch,
        target,
        256,
        "reach single-producer blacklist",
        |h, t| h.get_chunk_producer_blacklist(&t).unwrap() == only_target,
    );

    // Extend (target stays down, so the blacklist stays {target}) until the PLAIN sampler
    // at the anchor offset would pick the blacklisted producer. Only then does the seeder's
    // reassignment change the stored row, so scanning only the anchors the drive happened to
    // leave is not enough — with one blacklisted producer of three a short window need not
    // contain a plain sample of it.
    for _ in 0..128 {
        let chunk_height = height + CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET;
        let plain = epoch_info.sample_chunk_producer(layout, shard_id, chunk_height).unwrap();
        let blacklist = handle.get_chunk_producer_blacklist(&tip).unwrap();
        let shard_blacklist = blacklist.get(&shard_id).cloned().unwrap_or_default();
        assert_eq!(
            shard_blacklist,
            HashSet::from([target]),
            "branch {branch} blacklist drifted from {{{target}}}",
        );
        if shard_blacklist.contains(&plain) {
            let expected = epoch_info
                .sample_chunk_producer_excluding(layout, shard_id, chunk_height, &shard_blacklist)
                .unwrap();
            let stored_stake = handle
                .get_chunk_producer_info_anchored(Some(&tip), epoch_id, chunk_height, shard_id)
                .unwrap();
            let stored = epoch_info.get_validator_id(stored_stake.account_id()).copied().unwrap();
            assert_eq!(
                stored, expected,
                "branch {branch}: seeded row must be the blacklist-aware sample",
            );
            assert_ne!(
                stored, plain,
                "branch {branch}: seeder must reassign away from the blacklisted plain pick",
            );
            assert!(
                !shard_blacklist.contains(&stored),
                "branch {branch}: seeded row must not be a blacklisted producer",
            );
            return;
        }
        height += 1;
        tip = fork_step(handle, epoch_info, layout, shard_id, tip, branch, height, target);
    }
    panic!(
        "branch {branch}: no anchor whose plain pick is the blacklisted producer within 128 blocks"
    );
}

// v152+ protocol, seeded-row companion to the fork isolation test: consensus reads the
// stored `DBCol::ChunkProducers` row, not the live recompute, so pin that the stored row on
// each fork reflects that fork's own blacklist. Two branches fork at genesis and starve
// different producers; each ends with its own single-producer blacklist, and its seeded row
// excludes that producer at a height where the plain sampler would have picked it.
#[cfg(all(feature = "nightly", feature = "test_features"))]
#[test]
fn fork_seeded_rows_reflect_each_branch_blacklist() {
    let _guard = set_early_kickout_thresholds_for_testing(Some(10), Some(20));
    let validators = vec![
        ("test0".parse().unwrap(), STAKE),
        ("test1".parse().unwrap(), STAKE),
        ("test2".parse().unwrap(), STAKE),
    ];
    let handle = setup_default_epoch_manager(validators, 10_000, 1, 3, 90, 60).into_handle();
    let epoch_id = EpochId::default();
    let layout = handle.get_shard_layout(&epoch_id).unwrap();
    let shard_id = layout.shard_ids().next().unwrap();
    let epoch_info = handle.get_epoch_info(&epoch_id).unwrap();

    let genesis = fork_block_hash(0, 0);
    record_block(&mut handle.write(), CryptoHash::default(), genesis, 0, vec![]);

    // Two forks off genesis, each starving its own producer. Each branch's blacklist is
    // self-contained: a non-ancestor branch aggregates afresh from the epoch boundary, so the
    // assertions hold regardless of which branch the aggregator cache currently sits on.
    assert_branch_seeds_own_blacklist(
        &handle,
        epoch_info.as_ref(),
        &layout,
        shard_id,
        &epoch_id,
        genesis,
        0,
        1,
        0,
    );
    assert_branch_seeds_own_blacklist(
        &handle,
        epoch_info.as_ref(),
        &layout,
        shard_id,
        &epoch_id,
        genesis,
        0,
        2,
        1,
    );
}
