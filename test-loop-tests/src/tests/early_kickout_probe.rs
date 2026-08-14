//! Shared `DBCol::ChunkProducers` probe for the EarlyKickout test-loop tests.
//!
//! One oracle serves every caller: a same-epoch anchor's row must equal
//! `sample_chunk_producer_excluding(layout, shard, anchor_height + 2, blacklist)`, with the
//! blacklist read from `EpochManagerAdapter::get_chunk_producer_blacklist` — the same live
//! accessor the consensus read uses. Where the blacklist is empty this collapses to
//! `sample_chunk_producer`, so the sync tests (grace window never cleared, epoch of 10
//! against a 1000-block grace) and `tests::early_kickout_e2e` (grace shrunk, blacklist
//! active) can share it.
//!
//! Reading the blacklist rather than assuming it is empty buys two things: the walk is the
//! first non-test consumer to pin the seeder/accessor lockstep that
//! `EpochManager::seed_chunk_producers` and the accessor only claim in comments, and the
//! sync tests can *assert* their documented "no blacklist here" scope instead of relying
//! on it.
//!
//! Gated on `nightly` only (not `test_features`) so both the sync tests and the e2e tests
//! can use it.

use crate::utils::node::TestLoopNode;
use near_epoch_manager::CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET;
use near_primitives::epoch_info::EpochInfo;
use near_primitives::errors::EpochError;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{BlockHeight, EpochId, ValidatorId};
use near_primitives::utils::get_block_shard_id;
use near_primitives::version::ProtocolFeature;
use near_store::DBCol;
use std::cmp::max;
use std::collections::HashSet;
use std::sync::Arc;

/// What one [`walk_anchor_rows`] pass checked. Call sites assert on these so a walk
/// that silently checked nothing cannot pass.
#[derive(Debug, Default)]
pub(crate) struct AnchorWalk {
    pub heights_walked: u64,
    pub lowest_height: BlockHeight,
    pub highest_height: BlockHeight,
    /// One per (height, shard), not per height. Equals the sum of the three row
    /// classifications below.
    pub same_epoch_rows: u64,
    pub cross_epoch_heights: u64,
    pub pre_activation_heights: u64,
    /// Lowest walked height whose chunk epoch has EarlyKickout enabled.
    pub first_kickout_height: Option<BlockHeight>,
    /// Rows whose shard had an empty blacklist at the anchor: the row must equal the
    /// plain schedule.
    pub plain_rows: u64,
    /// Rows where the plain pick was itself blacklisted, so the slot was reassigned: the
    /// row must equal the excluding sample and differ from the plain schedule.
    pub reassigned_rows: u64,
    /// Rows on a shard with a non-empty blacklist whose plain pick survived it. The
    /// excluding sampler renormalizes over the survivors, so such a row may still differ
    /// from the plain schedule — only the excluding sample pins it.
    pub excluding_only_rows: u64,
    /// Rows whose anchor carried no `BlockInfo`, which the live blacklist accessor needs.
    /// Counted in `plain_rows` as well: the exclude set was assumed empty, not read.
    pub blacklist_unavailable: u64,
}

impl AnchorWalk {
    /// Rows written under a non-empty per-shard blacklist, whether or not the excluded
    /// producer was the plain pick. Zero means no blacklist was ever in force.
    pub fn blacklisted_rows(&self) -> u64 {
        self.reassigned_rows + self.excluding_only_rows
    }
}

/// Walk `node`'s headers down from `start_hash` while the height is at least `low`,
/// asserting the `DBCol::ChunkProducers` state each height's chunk resolution needs.
///
/// Headers rather than the height index: header-only heights below the tail have no
/// index entry, and that region is exactly where epoch-sync seeding has to be checked.
/// The walk also ends on the first missing header, so `low` may be genesis to mean "as
/// far down as this node goes".
///
/// The branch per height mirrors `EpochManagerAdapter::get_chunk_producer_info_anchored`.
pub(crate) fn walk_anchor_rows(
    node: &TestLoopNode,
    start_hash: CryptoHash,
    low: BlockHeight,
) -> AnchorWalk {
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
        let Ok(header) = chain.get_block_header(&hash) else { break };
        let height = header.height();
        if height < low {
            break;
        }
        // The chunk considered here is the one at this height: its parent carries the
        // production key, its grandparent is the anchor.
        let prev_hash = *header.prev_hash();
        let Ok(prev_header) = chain.get_block_header(&prev_hash) else { break };
        let anchor_hash = *prev_header.prev_hash();
        if anchor_hash == CryptoHash::default() {
            break; // genesis + 1, no grandparent
        }
        let Ok(anchor_header) = chain.get_block_header(&anchor_hash) else { break };

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

        if !ProtocolFeature::EarlyKickout.enabled(epoch_info.protocol_version()) {
            walk.pre_activation_heights += 1;
            for shard_id in shard_layout.shard_ids() {
                epoch_manager.get_chunk_producer_info_from_prev_block(&prev_hash, shard_id)
                    .unwrap_or_else(|err| {
                        panic!(
                            "pre-activation resolution failed at height {height} shard {shard_id}: {err:?}"
                        )
                    });
            }
        } else {
            // Descending walk, so the last write wins as the lowest active height.
            walk.first_kickout_height = Some(height);
            if anchor_header.epoch_id() == &chunk_epoch_id {
                let anchor_height = anchor_header.height();
                // The seeder samples at the anchor offset, not at the chunk's
                // `height_created`; those differ wherever a height was skipped.
                let sample_height = anchor_height + CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET;
                // Recomputed per anchor from the live accessor, never read off another
                // node's row, so each side of the comparison is independent.
                let blacklist = match epoch_manager.get_chunk_producer_blacklist(&anchor_hash) {
                    Ok(blacklist) => Some(blacklist),
                    // The accessor needs the anchor's `BlockInfo`. Degrade to the plain
                    // schedule rather than fail: a legitimately absent `BlockInfo` is not
                    // a wrong row, and the count keeps the degradation visible.
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
                    let expected = epoch_info
                        .sample_chunk_producer_excluding(
                            &shard_layout,
                            shard_id,
                            sample_height,
                            exclude,
                        )
                        .map(account_of)
                        .unwrap_or_else(|| {
                            panic!(
                                "canonical schedule has no producer for shard {shard_id} at \
                                 height {sample_height} excluding {exclude:?}"
                            )
                        });
                    assert_eq!(
                        row.account_id(),
                        &expected,
                        "row for anchor {anchor_hash} (height {anchor_height}) shard {shard_id} \
                         disagrees with the canonical schedule at height {sample_height} \
                         (exclude {exclude:?})"
                    );
                    let plain_id =
                        epoch_info.sample_chunk_producer(&shard_layout, shard_id, sample_height);
                    // Classify by what the oracle just proved, so a caller can tell an
                    // active blacklist from an empty one without a second read.
                    if exclude.is_empty() {
                        walk.plain_rows += 1;
                    } else if plain_id.is_some_and(|plain_id| exclude.contains(&plain_id)) {
                        // Mirrors the seeder's own reassignment test. Testing
                        // `plain != row` instead would be wrong: the excluding sampler
                        // renormalizes and can move a slot whose plain pick was fine.
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
                // No row required, but the sampler fallthrough can still fail with
                // `EpochOutOfBounds` if the anchor's epoch info was never retained.
                for shard_id in shard_layout.shard_ids() {
                    epoch_manager.get_chunk_producer_info_from_prev_block(&prev_hash, shard_id)
                        .unwrap_or_else(|err| {
                            panic!(
                                "cross-epoch anchored read failed at height {height} shard {shard_id}: {err:?}"
                            )
                        });
                }
            }
        }

        if walk.heights_walked == 0 {
            walk.highest_height = height;
        }
        walk.heights_walked += 1;
        walk.lowest_height = height;
        hash = prev_hash;
    }

    tracing::info!(target: "test", ?walk, "anchor row walk complete");
    walk
}

pub(crate) fn assert_walk_window(walk: &AnchorWalk, min_width: u64, label: &str) {
    assert!(walk.heights_walked > 0, "{label}: walked nothing");
    let width = walk.highest_height - walk.lowest_height + 1;
    assert_eq!(walk.heights_walked, width, "{label}: header walk skipped heights ({walk:?})");
    assert!(width >= min_width, "{label}: window too narrow to be meaningful ({walk:?})");
}

/// Asserts the blacklist half of the oracle was actually exercised: every same-epoch anchor
/// the walk checked had the `BlockInfo` the live accessor reads, so no row fell back to the
/// plain schedule with an assumed-empty exclude set.
///
/// Holds for any region built by block processing. It does *not* hold in the header-only
/// region an epoch-synced node keeps below its tail — see the caller in case A.
pub(crate) fn assert_blacklist_read_everywhere(walk: &AnchorWalk, label: &str) {
    assert_eq!(
        walk.blacklist_unavailable, 0,
        "{label}: the blacklist accessor found no `BlockInfo` for {} anchors, so the oracle \
         degraded to the plain schedule there ({walk:?})",
        walk.blacklist_unavailable
    );
}

/// Lowest height in `node`'s header chain that starts an epoch, walking down from
/// `start_hash`.
///
/// Floor of the header-only probe. An epoch-synced node keeps a few headers below the
/// synced epoch, carried by the epoch-sync proof: they arrive through
/// `apply_validated_proof`, never the seeder, so they hold no rows — and need none,
/// since the synced epoch's opening chunks anchor into that previous epoch and take
/// the cross-epoch arm.
pub(crate) fn lowest_epoch_start_in_headers(
    node: &TestLoopNode,
    start_hash: CryptoHash,
) -> BlockHeight {
    let chain = &node.client().chain;
    let mut lowest = None;
    let mut hash = start_hash;
    loop {
        let Ok(header) = chain.get_block_header(&hash) else { break };
        let prev_hash = *header.prev_hash();
        let Ok(prev_header) = chain.get_block_header(&prev_hash) else { break };
        if prev_header.epoch_id() != header.epoch_id() {
            lowest = Some(header.height());
        }
        hash = prev_hash;
    }
    lowest.unwrap_or_else(|| panic!("no epoch start in the node's header chain"))
}

/// Floor is `tail + 3`, not `tail + 1`: the anchor sits two heights below the walked
/// height, and GC removes rows for every cleared height.
pub(crate) fn probe_block_region(node: &TestLoopNode, epoch_length: u64) -> AnchorWalk {
    let head = node.head();
    let tail = node.tail();
    let low = max(tail + 3, head.height.saturating_sub(3 * epoch_length));
    let walk = walk_anchor_rows(node, head.last_block_hash, low);
    assert!(
        walk.lowest_height > tail,
        "block-region probe reached height {} at or below the tail {tail} ({walk:?})",
        walk.lowest_height
    );
    walk
}
