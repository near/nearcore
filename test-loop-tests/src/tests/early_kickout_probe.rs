//! Shared `DBCol::ChunkProducers` probe for the EarlyKickout test-loop tests.
//!
//! One oracle serves every caller: a same-epoch anchor's row must equal
//! `sample_chunk_producer_excluding(layout, shard, anchor_height + 2, blacklist)`, with the
//! blacklist read from the same live accessor the consensus read uses. It collapses to
//! `sample_chunk_producer` wherever the blacklist is empty, so the sync tests (always inside
//! the grace window) and `tests::early_kickout_e2e` (grace shrunk, blacklist active) share it.

use crate::utils::node::TestLoopNode;
use near_chain::Error as ChainError;
use near_epoch_manager::CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET;
use near_primitives::epoch_info::EpochInfo;
use near_primitives::errors::EpochError;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{BlockHeight, EpochId, ShardId, ValidatorId};
use near_primitives::utils::get_block_shard_id;
use near_primitives::version::ProtocolFeature;
use near_store::DBCol;
use std::cmp::max;
use std::collections::HashSet;
use std::sync::Arc;

/// What one [`walk_anchor_rows`] pass checked. Call sites assert on these so a walk that
/// silently checked nothing cannot pass.
#[derive(Debug, Default)]
pub(crate) struct AnchorWalk {
    pub heights_walked: u64,
    pub lowest_height: BlockHeight,
    pub highest_height: BlockHeight,
    /// One per (height, shard). Equals the sum of the three row classes below.
    pub same_epoch_rows: u64,
    pub cross_epoch_heights: u64,
    pub pre_activation_heights: u64,
    /// Lowest walked height whose chunk epoch has EarlyKickout enabled.
    pub first_kickout_height: Option<BlockHeight>,
    /// Empty exclude set, so the row must equal the plain schedule.
    pub plain_rows: u64,
    /// Plain pick was itself blacklisted, so the row must differ from the plain schedule.
    pub reassigned_rows: u64,
    /// Blacklisted shard whose plain pick survived. The excluding sampler renormalizes over
    /// the survivors, so the row may still differ from the plain schedule — only the
    /// excluding sample pins it.
    pub excluding_only_rows: u64,
    /// Anchors with no `BlockInfo` for the accessor to read. Also counted in `plain_rows`:
    /// the exclude set was assumed empty, not read.
    pub blacklist_unavailable: u64,
}

impl AnchorWalk {
    /// Zero means no blacklist was ever in force.
    pub fn blacklisted_rows(&self) -> u64 {
        self.reassigned_rows + self.excluding_only_rows
    }
}

/// Which of the three headers a walked height resolves through failed to load.
#[derive(Debug)]
pub(crate) enum HeaderRole {
    Current,
    Parent,
    Anchor,
}

/// Why [`walk_anchor_rows`] could not reach its floor. Every variant is an error: a bounded
/// walk that stops early must never read as a pass.
// The fields reach the test failure output through the derived `Debug`, which dead-code
// analysis deliberately ignores.
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum WalkError {
    /// A header lookup failed while the floor was still uncovered. For
    /// [`HeaderRole::Current`] the missing header's own height is unknown, so `chunk_height`
    /// is the last processed height (zero when the very first lookup fails).
    Header { role: HeaderRole, hash: CryptoHash, chunk_height: BlockHeight, source: ChainError },
    /// The walk stepped below the floor without ever processing it: a skipped height
    /// straddled `low`, so the row at `low` went unchecked. Skipped-height support would
    /// redefine coverage over visited headers; until then this is an error.
    PassedFloor { requested: BlockHeight, next_height: BlockHeight },
    /// The chunk at `chunk_height` has a defaulted grandparent hash (genesis or genesis + 1),
    /// a real absence rather than a missing header. No current caller targets genesis; a
    /// future "walk to genesis" needs its own walk target, not a weakening of bounded walks.
    NoGrandparent { chunk_height: BlockHeight },
}

/// Walk `node`'s headers down from `start_hash` to exactly height `low`, asserting the
/// `DBCol::ChunkProducers` state each height's chunk resolution needs.
///
/// Headers rather than the height index: header-only heights below the tail have no index
/// entry, and that region is exactly where epoch-sync seeding has to be checked. The walk
/// succeeds only after fully processing height `low` — any lookup failure or floor straddle
/// before that is a [`WalkError`], so a truncated walk cannot read as a pass.
///
/// The branch per height mirrors `EpochManagerAdapter::get_chunk_producer_info_anchored`.
pub(crate) fn walk_anchor_rows(
    node: &TestLoopNode,
    start_hash: CryptoHash,
    low: BlockHeight,
) -> Result<AnchorWalk, WalkError> {
    let client = node.client();
    let chain = &client.chain;
    let epoch_manager = client.epoch_manager.as_ref();
    let store = node.store();

    let mut walk = AnchorWalk::default();
    // Monotone in epochs, so a single entry caches exactly.
    let mut cached: Option<(EpochId, ShardLayout, Arc<EpochInfo>)> = None;
    let no_exclusions = HashSet::new();
    let mut hash = start_hash;

    loop {
        let header = chain.get_block_header(&hash).map_err(|source| WalkError::Header {
            role: HeaderRole::Current,
            hash,
            chunk_height: walk.lowest_height,
            source,
        })?;
        let height = header.height();
        if height < low {
            return Err(WalkError::PassedFloor { requested: low, next_height: height });
        }
        // The chunk at this height: its parent carries the production key, its grandparent is
        // the anchor.
        let prev_hash = *header.prev_hash();
        let prev_header =
            chain.get_block_header(&prev_hash).map_err(|source| WalkError::Header {
                role: HeaderRole::Parent,
                hash: prev_hash,
                chunk_height: height,
                source,
            })?;
        let anchor_hash = *prev_header.prev_hash();
        if anchor_hash == CryptoHash::default() {
            return Err(WalkError::NoGrandparent { chunk_height: height });
        }
        let anchor_header =
            chain.get_block_header(&anchor_hash).map_err(|source| WalkError::Header {
                role: HeaderRole::Anchor,
                hash: anchor_hash,
                chunk_height: height,
                source,
            })?;

        let chunk_epoch_id = *header.epoch_id();
        let (shard_layout, epoch_info) = match &cached {
            Some((id, layout, info)) if *id == chunk_epoch_id => (layout.clone(), info.clone()),
            _ => {
                let layout = epoch_manager.get_shard_layout(&chunk_epoch_id).unwrap();
                let info = epoch_manager.get_epoch_info(&chunk_epoch_id).unwrap();
                cached = Some((chunk_epoch_id, layout.clone(), info.clone()));
                (layout, info)
            }
        };
        // The resolver derives the chunk's `height_created` from the parent `BlockInfo`, so
        // where it falls through to the canonical sampler the sample height is the parent's
        // height + 1 — equal to `height` only while skipped heights are rejected.
        let fallthrough_sample_height = prev_header.height() + 1;

        if !ProtocolFeature::EarlyKickout.enabled(epoch_info.protocol_version()) {
            walk.pre_activation_heights += 1;
            for shard_id in shard_layout.shard_ids() {
                let resolved = epoch_manager.get_chunk_producer_info_from_prev_block(&prev_hash, shard_id)
                    .unwrap_or_else(|err| {
                        panic!(
                            "pre-activation resolution failed at height {height} shard {shard_id}: {err:?}"
                        )
                    });
                assert_matches_canonical_sample(
                    &epoch_info,
                    &shard_layout,
                    shard_id,
                    fallthrough_sample_height,
                    &resolved,
                    "pre-activation",
                );
            }
        } else {
            // Descending walk, so the last write wins as the lowest active height.
            walk.first_kickout_height = Some(height);
            if anchor_header.epoch_id() == &chunk_epoch_id {
                let anchor_height = anchor_header.height();
                // The seeder samples at the anchor offset, not at the chunk's
                // `height_created`; those differ wherever a height was skipped.
                let sample_height = anchor_height + CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET;
                let blacklist = match epoch_manager.get_chunk_producer_blacklist(&anchor_hash) {
                    Ok(blacklist) => Some(blacklist),
                    // A legitimately absent `BlockInfo` is not a wrong row, so degrade to the
                    // plain schedule and count it rather than fail.
                    Err(EpochError::MissingBlock(_)) => None,
                    Err(err) => panic!(
                        "blacklist accessor failed for anchor {anchor_hash} (height \
                         {anchor_height}): {err:?}"
                    ),
                };
                if blacklist.is_none() {
                    walk.blacklist_unavailable += 1;
                }
                let account_of = |id: ValidatorId| epoch_info.get_validator(id).take_account_id();
                for shard_id in shard_layout.shard_ids() {
                    let key = get_block_shard_id(&anchor_hash, shard_id);
                    let row: ValidatorStake =
                        store.get_ser(DBCol::ChunkProducers, &key).unwrap_or_else(|| {
                            panic!(
                                "missing DBCol::ChunkProducers row for same-epoch anchor \
                                 {anchor_hash} (height {anchor_height}) shard {shard_id}, needed \
                                 by the chunk at height {height}"
                            )
                        });
                    let exclude = blacklist
                        .as_ref()
                        .and_then(|blacklist| blacklist.get(&shard_id))
                        .unwrap_or(&no_exclusions);
                    let expected_id = epoch_info
                        .sample_chunk_producer_excluding(
                            &shard_layout,
                            shard_id,
                            sample_height,
                            exclude,
                        )
                        .unwrap_or_else(|| {
                            panic!(
                                "canonical schedule has no producer for shard {shard_id} at \
                                 height {sample_height} excluding {exclude:?}"
                            )
                        });
                    let expected = epoch_info.get_validator(expected_id);
                    assert_eq!(
                        row, expected,
                        "row for anchor {anchor_hash} (height {anchor_height}) shard {shard_id} \
                         disagrees with the canonical schedule at height {sample_height} \
                         (exclude {exclude:?})"
                    );
                    if blacklist.is_some() {
                        // The production resolver must consume exactly this row.
                        let resolved = epoch_manager
                            .get_chunk_producer_info_from_prev_block(&prev_hash, shard_id)
                            .unwrap_or_else(|err| {
                                panic!(
                                    "same-epoch anchored resolver failed at height {height} \
                                     shard {shard_id}: {err:?}"
                                )
                            });
                        assert_eq!(
                            resolved, expected,
                            "resolver disagrees with the canonical schedule at height {height} \
                             shard {shard_id}"
                        );
                    } else {
                        // Classify the accessor's `MissingBlock` instead of loosely counting
                        // it. It hides two shapes: the anchor's own `BlockInfo` is missing
                        // (proof-omitted header — the resolver fails on the same read), or
                        // only the blacklist basis is missing (the aggregator walk from the
                        // anchor's last-final block — the resolver still consumes the row).
                        let resolver_result = epoch_manager
                            .get_chunk_producer_info_from_prev_block(&prev_hash, shard_id);
                        match epoch_manager.get_epoch_id(&anchor_hash) {
                            Ok(_) => {
                                let resolved = resolver_result.unwrap_or_else(|err| {
                                    panic!(
                                        "resolver failed at height {height} shard {shard_id} \
                                         though the anchor {anchor_hash} has a BlockInfo: \
                                         {err:?}"
                                    )
                                });
                                assert_eq!(
                                    resolved, expected,
                                    "resolver disagrees with the canonical schedule at height \
                                     {height} shard {shard_id}"
                                );
                            }
                            Err(EpochError::MissingBlock(_)) => {
                                let err = resolver_result.expect_err(
                                    "resolver cannot succeed where the anchor has no BlockInfo",
                                );
                                assert!(
                                    matches!(err, EpochError::MissingBlock(_)),
                                    "resolver failed with {err:?} at height {height} shard \
                                     {shard_id}, expected MissingBlock"
                                );
                            }
                            Err(err) => panic!(
                                "anchor epoch lookup failed for {anchor_hash} (height \
                                 {anchor_height}): {err:?}"
                            ),
                        }
                    }
                    let plain_id =
                        epoch_info.sample_chunk_producer(&shard_layout, shard_id, sample_height);
                    if exclude.is_empty() {
                        walk.plain_rows += 1;
                    } else if plain_id.is_some_and(|plain_id| exclude.contains(&plain_id)) {
                        // Mirrors the seeder's own reassignment test. Testing `plain != row`
                        // instead would be wrong: the excluding sampler renormalizes, so it can
                        // move a slot whose plain pick was fine.
                        assert_ne!(
                            Some(row.account_id()),
                            plain_id.map(account_of).as_ref(),
                            "row for anchor {anchor_hash} (height {anchor_height}) shard \
                             {shard_id} still names the blacklisted plain pick"
                        );
                        walk.reassigned_rows += 1;
                    } else {
                        walk.excluding_only_rows += 1;
                    }
                    walk.same_epoch_rows += 1;
                }
            } else {
                walk.cross_epoch_heights += 1;
                // No row required, but the sampler fallthrough still fails with
                // `EpochOutOfBounds` if the anchor's epoch info was never retained.
                for shard_id in shard_layout.shard_ids() {
                    let resolved = epoch_manager.get_chunk_producer_info_from_prev_block(&prev_hash, shard_id)
                        .unwrap_or_else(|err| {
                            panic!(
                                "cross-epoch anchored read failed at height {height} shard {shard_id}: {err:?}"
                            )
                        });
                    assert_matches_canonical_sample(
                        &epoch_info,
                        &shard_layout,
                        shard_id,
                        fallthrough_sample_height,
                        &resolved,
                        "cross-epoch",
                    );
                }
            }
        }

        if walk.heights_walked == 0 {
            walk.highest_height = height;
        }
        walk.heights_walked += 1;
        walk.lowest_height = height;
        if height == low {
            tracing::info!(target: "test", ?walk, "anchor row walk complete");
            return Ok(walk);
        }
        hash = prev_hash;
    }
}

/// Pins a resolution that fell through to the canonical sampler to the full
/// `ValidatorStake` the schedule names, not just its account id.
fn assert_matches_canonical_sample(
    epoch_info: &EpochInfo,
    shard_layout: &ShardLayout,
    shard_id: ShardId,
    sample_height: BlockHeight,
    resolved: &ValidatorStake,
    label: &str,
) {
    let expected_id = epoch_info
        .sample_chunk_producer(shard_layout, shard_id, sample_height)
        .unwrap_or_else(|| {
            panic!(
                "canonical schedule has no producer for shard {shard_id} at height \
                 {sample_height}"
            )
        });
    assert_eq!(
        *resolved,
        epoch_info.get_validator(expected_id),
        "{label}: resolved producer for shard {shard_id} disagrees with the canonical \
         schedule at height {sample_height}"
    );
}

pub(crate) fn assert_walk_window(walk: &AnchorWalk, min_width: u64, label: &str) {
    assert!(walk.heights_walked > 0, "{label}: walked nothing");
    let width = walk.highest_height - walk.lowest_height + 1;
    assert_eq!(walk.heights_walked, width, "{label}: header walk skipped heights ({walk:?})");
    assert!(width >= min_width, "{label}: window too narrow to be meaningful ({walk:?})");
}

/// Asserts no row fell back to an assumed-empty exclude set. Holds for any region built by
/// block processing, but not for the header-only region below an epoch-synced node's tail.
pub(crate) fn assert_blacklist_read_everywhere(walk: &AnchorWalk, label: &str) {
    assert_eq!(
        walk.blacklist_unavailable, 0,
        "{label}: the blacklist accessor found no `BlockInfo` for {} anchors, so the oracle \
         degraded to the plain schedule there ({walk:?})",
        walk.blacklist_unavailable
    );
}

/// Floor is `tail + 3`, not `tail + 1`: the anchor sits two heights below the walked height,
/// and GC removes rows for every cleared height.
pub(crate) fn probe_block_region(node: &TestLoopNode, epoch_length: u64) -> AnchorWalk {
    let head = node.head();
    let tail = node.tail();
    let low = max(tail + 3, head.height.saturating_sub(3 * epoch_length));
    let walk = walk_anchor_rows(node, head.last_block_hash, low)
        .unwrap_or_else(|err| panic!("block-region walk failed before reaching {low}: {err:?}"));
    // The walk's own postcondition, re-asserted here so a walker regression that returns
    // `Ok` early cannot silently narrow the probe.
    assert_eq!(
        walk.lowest_height, low,
        "block-region walk returned without reaching its floor ({walk:?})"
    );
    assert!(
        walk.lowest_height > tail,
        "block-region probe reached height {} at or below the tail {tail} ({walk:?})",
        walk.lowest_height
    );
    walk
}
