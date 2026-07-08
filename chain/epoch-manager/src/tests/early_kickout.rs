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

/// Runs the math and returns just the blacklist map (drops observability stats).
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

// 1. produced/expected < 80%, missed >= 20, expected >= 50 -> blacklisted.
#[test]
fn blacklist_below_threshold() {
    let (epoch_info, layout, shard_id) = single_shard_epoch(4);
    // id 0: 50/100 = 50% < 80%, missed 50; others healthy.
    let st = tracker(shard_id, &[(0, 50, 100), (1, 100, 100), (2, 100, 100), (3, 100, 100)]);
    let bl = blacklist(&st, &epoch_info, &layout);
    assert_eq!(bl, HashMap::from([(shard_id, HashSet::from([0]))]));
}

// 2. produced*100 == expected*80 -> NOT blacklisted (strict `<`), missed >= 20.
#[test]
fn blacklist_exactly_at_threshold() {
    let (epoch_info, layout, shard_id) = single_shard_epoch(4);
    // id 0: 320/400 = exactly 80%, missed 80 (>= 20). Strict `<` must exclude it.
    let st = tracker(shard_id, &[(0, 320, 400), (1, 400, 400), (2, 400, 400), (3, 400, 400)]);
    let bl = blacklist(&st, &epoch_info, &layout);
    assert!(bl.is_empty());
}

// 3. missed < 20 -> not blacklisted regardless of ratio.
#[test]
fn blacklist_under_min_misses() {
    let (epoch_info, layout, shard_id) = single_shard_epoch(4);
    // id 0: 31/50 = 62% < 80% but missed only 19 (< 20); expected >= 50.
    let st = tracker(shard_id, &[(0, 31, 50), (1, 100, 100), (2, 100, 100), (3, 100, 100)]);
    let bl = blacklist(&st, &epoch_info, &layout);
    assert!(bl.is_empty());
}

// 4. every producer would be blacklisted -> keep exactly one least-bad. Both here
//    have identical stats, so the tiebreak (lower validator_id) keeps id 0 and
//    blacklists id 1.
#[test]
fn blacklist_safety_valve_all_producers() {
    let (epoch_info, layout, shard_id) = single_shard_epoch(2);
    let st = tracker(shard_id, &[(0, 0, 100), (1, 0, 100)]);
    let bl = blacklist(&st, &epoch_info, &layout);
    assert_eq!(bl, HashMap::from([(shard_id, HashSet::from([1]))]));
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
    assert!(stats.safety_valve_fired, "valve must fire when the only producer is a candidate");
}

// 6. expected < 50 at 0% production -> not blacklisted (sample-size guard).
#[test]
fn blacklist_minimum_observed_blocks() {
    let (epoch_info, layout, shard_id) = single_shard_epoch(2);
    // id 0: 0/49, below the observed-blocks floor.
    let st = tracker(shard_id, &[(0, 0, 49), (1, 100, 100)]);
    let bl = blacklist(&st, &epoch_info, &layout);
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
    // ratios: id 0 = 40%, id 1 = 79% (holder), id 2 = 50%. All candidates.
    let st = tracker(shard_id, &[(0, 40, 100), (1, 79, 100), (2, 50, 100)]);
    assert_eq!(kept_survivor(&st, &epoch_info, &layout, shard_id, &[0, 1, 2]), 1);
    // The two frozen candidates are blacklisted; the holder is not.
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
    let holding = tracker(shard_id, &[(0, 79, 100), (1, 50, 100), (2, 40, 100)]);
    assert_eq!(kept_survivor(&holding, &epoch_info, &layout, shard_id, &[0, 1, 2]), 0);
    // id 0 collapses to 10%; id 1 (50%) is now the least-bad and takes the slot.
    let dropped = tracker(shard_id, &[(0, 10, 100), (1, 50, 100), (2, 40, 100)]);
    assert_eq!(kept_survivor(&dropped, &epoch_info, &layout, shard_id, &[0, 1, 2]), 1);
}

// (c) the worst producer is never the survivor. id 0 has the lowest ratio and must
//     always be blacklisted; the best producer (id 2) is kept.
#[test]
fn keep_one_never_keeps_worst() {
    let (epoch_info, layout, shard_id) = single_shard_epoch(3);
    // ratios: id 0 = 5% (worst), id 1 = 40%, id 2 = 79% (best).
    let st = tracker(shard_id, &[(0, 5, 100), (1, 40, 100), (2, 79, 100)]);
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
    let st = tracker(shard_id, &[(0, 5, 100), (1, 40, 100), (2, 79, 100)]);
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

// (f) v152+ protocol: an all-bad shard fires the safety valve at the accessor. The
//     `safety_valve_fired` counter for that shard increments, and the accessor keeps
//     exactly one producer eligible (blacklisting the other of the two).
#[cfg(feature = "nightly")]
#[test]
fn get_chunk_producer_blacklist_safety_valve_metric_increments() {
    let validators = vec![("test0".parse().unwrap(), STAKE), ("test1".parse().unwrap(), STAKE)];
    let handle = setup_default_epoch_manager(validators, 10_000, 1, 3, 90, 60).into_handle();
    let h = drive_all_chunks_missed(&handle, 160);
    let prev = *h.last().unwrap();
    let epoch_id = handle.get_epoch_id_from_prev_block(&prev).unwrap();
    let shard_id = handle.get_shard_layout(&epoch_id).unwrap().shard_ids().next().unwrap();
    let label = shard_id.to_string();
    let before =
        crate::metrics::EARLY_KICKOUT_SAFETY_VALVE_FIRED.with_label_values(&[label.as_str()]).get();
    let bl = handle.get_chunk_producer_blacklist(&prev).unwrap();
    // Both producers are bad -> keep exactly one, blacklist the other.
    assert_eq!(bl.len(), 1, "expected exactly one shard in the blacklist, got {bl:?}");
    assert_eq!(bl[&shard_id].len(), 1, "keep-one must blacklist exactly one of two producers");
    let after =
        crate::metrics::EARLY_KICKOUT_SAFETY_VALVE_FIRED.with_label_values(&[label.as_str()]).get();
    assert_eq!(after, before + 1, "safety valve counter must increment once: {before} -> {after}");
}
