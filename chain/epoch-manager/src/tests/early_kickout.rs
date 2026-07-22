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

// 1. produced/expected < 80%, missed >= 100 -> blacklisted.
#[test]
fn blacklist_below_threshold() {
    let (epoch_info, layout, shard_id) = single_shard_epoch(4);
    // id 0: 0/200 = 0% < 80%, missed 200; others healthy.
    let st = tracker(shard_id, &[(0, 0, 200), (1, 100, 100), (2, 100, 100), (3, 100, 100)]);
    let bl = compute_chunk_producer_blacklist(&st, &epoch_info, &layout);
    assert_eq!(bl, HashMap::from([(shard_id, HashSet::from([0]))]));
}

// 2. produced*100 == expected*80 -> NOT blacklisted (strict `<`), missed >= 100.
#[test]
fn blacklist_exactly_at_threshold() {
    let (epoch_info, layout, shard_id) = single_shard_epoch(4);
    // id 0: 400/500 = exactly 80%, missed 100 (>= 100). Strict `<` must exclude it.
    let st = tracker(shard_id, &[(0, 400, 500), (1, 500, 500), (2, 500, 500), (3, 500, 500)]);
    let bl = compute_chunk_producer_blacklist(&st, &epoch_info, &layout);
    assert!(bl.is_empty());
}

// 3. missed < 100 -> not blacklisted regardless of ratio.
#[test]
fn blacklist_under_min_misses() {
    let (epoch_info, layout, shard_id) = single_shard_epoch(4);
    // id 0: 391/490 = 79.8% < 80% but missed only 99 (< 100).
    let st = tracker(shard_id, &[(0, 391, 490), (1, 100, 100), (2, 100, 100), (3, 100, 100)]);
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

// 6. missed exactly 100 at < 80% -> blacklisted. Sharp lower edge of the miss floor
//    (one miss above `blacklist_under_min_misses`).
#[test]
fn blacklist_at_min_misses_boundary() {
    let (epoch_info, layout, shard_id) = single_shard_epoch(4);
    // id 0: 390/490 = 79.6% < 80%, missed exactly 100.
    let st = tracker(shard_id, &[(0, 390, 490), (1, 100, 100), (2, 100, 100), (3, 100, 100)]);
    let bl = compute_chunk_producer_blacklist(&st, &epoch_info, &layout);
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
///
/// Uses the *plain* height sampler: with the early-kickout feature off there is no
/// blacklist-aware seeding, so a target's missed heights stay attributed to the target.
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

// 11. v152+ epoch-boundary reset: at an epoch boundary the aggregator still belongs to the
//     previous epoch, so the accessor returns empty even though epoch 0 stats are miss-heavy.
//     Setup: epoch length 1200 exceeds the 1000-block grace (otherwise the whole epoch sits in
//     the grace and the reset check is vacuous), and the drive length 1300 crosses into epoch 1
//     so a boundary exists. `boundary_idx` is the last block whose next block starts a new epoch;
//     `h[i] == height` because `drive_down_node` stores hashes by height, so `boundary_idx - 1`
//     is the mid-epoch anchor and `boundary_idx` is the boundary anchor.
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
    let bl_boundary = handle.get_chunk_producer_blacklist(&h[boundary_idx]).unwrap();
    assert!(bl_boundary.is_empty(), "epoch boundary must reset blacklist, got {bl_boundary:?}");
}

// Start-of-epoch grace: with the down node already miss-heavy, the accessor stays empty until
// the anchor is at least EARLY_KICKOUT_EPOCH_GRACE_BLOCKS into the epoch, then blacklists it.
// `blocks_into_epoch` is measured from the epoch start height (not 0), so the grace boundary is
// pinned against the actual start. Also checks the seeder and accessor agree at the exact
// threshold: inside the grace the seeded row is the plain pick, and at the first active anchor it
// is the blacklist-aware pick (never the down node).
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
    let epoch_start = handle.get_epoch_start_from_epoch_id(&epoch_id).unwrap();

    // Last anchor inside the grace (blocks_into_epoch == GRACE - 1).
    let in_grace = (epoch_start + EARLY_KICKOUT_EPOCH_GRACE_BLOCKS - 1) as usize;
    let bl_grace = handle.get_chunk_producer_blacklist(&h[in_grace]).unwrap();
    assert!(bl_grace.is_empty(), "anchor inside the grace window must be empty, got {bl_grace:?}");
    let in_grace_ch = in_grace as u64 + CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET;
    let plain_in_grace = epoch_info
        .get_validator(epoch_info.sample_chunk_producer(&layout, shard_id, in_grace_ch).unwrap());
    let stored_in_grace = handle
        .get_chunk_producer_info_anchored(Some(&h[in_grace]), &epoch_id, in_grace_ch, shard_id)
        .unwrap();
    assert_eq!(stored_in_grace, plain_in_grace, "in-grace seeded row must be the plain pick");

    // First anchor at the grace boundary (blocks_into_epoch == GRACE): blacklist active.
    let past_grace = (epoch_start + EARLY_KICKOUT_EPOCH_GRACE_BLOCKS) as usize;
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

// the seeded `DBCol::ChunkProducers` row equals the plain height
// sampler while the blacklist is empty, and equals the blacklist-aware sampler (never the
// down node) once it is non-empty. The strict consensus reader returns that same row.
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

// Missing-row invariant: wherever the blacklist as of an anchor is
// non-empty, that anchor's `DBCol::ChunkProducers` rows are present for every shard. So the
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
    let chunk_endorsements = ChunkEndorsementsBitmap::from_endorsements(
        shard_layout
            .shard_ids()
            .map(|shard_id| {
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
            final_height,
            final_hash,
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

// Regression guard for the per-block aggregator walk in `seed_chunk_producers`: with finality
// frozen, the incremental aggregator update is skipped while the seed re-scans the growing
// not-yet-finalized suffix every block. Pins the current per-block O(stall-depth) walk as a guard (a future cache
// tightens it) and catches a worse-than-quadratic regression. Distinct from the walk-count
// invariant in `test_finalize_epoch_large_epoch_length`, which is gated off nightly.
#[cfg(feature = "nightly")]
#[test]
fn seed_walk_bounded_under_finality_stall() {
    use std::sync::atomic::Ordering;
    let validators = vec![("test0".parse().unwrap(), STAKE), ("test1".parse().unwrap(), STAKE)];
    let mut em = setup_default_epoch_manager(validators, 10_000, 1, 3, 90, 60);
    let count = 40u64;
    let h: Vec<CryptoHash> = (0..=count).map(|i| hash(&i.to_le_bytes())).collect();
    record_block(&mut em, CryptoHash::default(), h[0], 0, vec![]);
    let before = em.epoch_info_aggregator_loop_counter.load(Ordering::SeqCst);
    // Every block reports finality frozen at genesis, so `largest_final_height` never advances.
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
    let walked = em.epoch_info_aggregator_loop_counter.load(Ordering::SeqCst) - before;
    // The seed re-scans the not-yet-finalized suffix each block: total ~ sum_{k=1..count} k. Pin a
    // generous O(depth^2) upper bound; a future cache drops it toward O(count).
    let upper = (count * (count + 1)) as usize;
    let count = count as usize;
    assert!(walked >= count, "seed walk should touch >= 1 block per recorded block, got {walked}");
    assert!(
        walked <= upper,
        "seed walk cost {walked} exceeds O(depth^2) bound {upper} — regression?"
    );
}
