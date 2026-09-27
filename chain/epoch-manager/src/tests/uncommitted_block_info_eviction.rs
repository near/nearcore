//! Regression test for the 2026-09-27 testnet halt at height 270420307.
//!
//! `record_block_info` returns the new block's `BlockInfo` in an uncommitted store update
//! and keeps it readable only through the `blocks_info` LRU cache; the chain reads it back
//! (e.g. in `update_head`) before committing. Seeding the chunk-producer blacklist walks the
//! epoch-info aggregator back from the block's last-final block. When that block is behind
//! the aggregator's sync point the walk runs to the epoch start, and past
//! `BLOCK_CACHE_SIZE` blocks into the epoch it evicted the new block, so every read of it
//! failed with `MissingBlock`.
//!
//! Testnet shape: canonical `305 -> 307` (306 skipped, last-final 303 for both), then the
//! orphan `306` on `305` arrives late with last-final 304 and moves the aggregator to 304.
//! Every child of 307 inherits last-final 303, behind the aggregator, and could never be
//! processed.

use crate::test_utils::{
    record_block_with_final_and_mask, record_block_with_final_and_mask_uncommitted,
    setup_default_epoch_manager,
};
use crate::{BLOCK_CACHE_SIZE, EpochManager};
use near_primitives::hash::CryptoHash;
use near_primitives::types::{Balance, BlockHeight};
use near_primitives::version::PROTOCOL_VERSION;

const STAKE: Balance = Balance::from_yoctonear(1_000_000);

/// Long enough that the whole test stays inside the first epoch.
const EPOCH_LENGTH: u64 = 10_000;

/// Head of the committed canonical chain ("305"). Far enough into the epoch that a walk
/// back to the epoch start touches more than `BLOCK_CACHE_SIZE` blocks.
const HEAD: BlockHeight = BLOCK_CACHE_SIZE as BlockHeight + 200;

fn new_epoch_manager() -> EpochManager {
    let validators = vec![("test0".parse().unwrap(), STAKE), ("test1".parse().unwrap(), STAKE)];
    setup_default_epoch_manager(validators, EPOCH_LENGTH, 1, 2, 90, 60)
}

fn block_hash(name: &str, height: BlockHeight) -> CryptoHash {
    CryptoHash::hash_bytes(format!("uncommitted-block-info/{name}-{height}").as_bytes())
}

/// Commits genesis and a canonical chain up to `HEAD`, each block finalizing its
/// grandparent. Returns the canonical hashes indexed by height.
fn build_canonical_chain(em: &mut EpochManager) -> Vec<CryptoHash> {
    let mut hashes = vec![block_hash("canonical", 0)];
    record_block_with_final_and_mask(
        em,
        CryptoHash::default(),
        hashes[0],
        0,
        CryptoHash::default(),
        0,
        vec![true],
    );
    for height in 1..=HEAD {
        let cur = block_hash("canonical", height);
        let (final_hash, final_height) = if height >= 2 {
            (hashes[height as usize - 2], height - 2)
        } else {
            (CryptoHash::default(), 0)
        };
        record_block_with_final_and_mask(
            em,
            hashes[height as usize - 1],
            cur,
            height,
            final_hash,
            final_height,
            vec![true],
        );
        hashes.push(cur);
    }
    hashes
}

#[test]
fn block_info_survives_full_epoch_aggregator_walk_before_commit() {
    let mut em = new_epoch_manager();
    let canonical = build_canonical_chain(&mut em);
    let final_of_head = canonical[HEAD as usize - 2];

    // "307": skips HEAD + 1, so it keeps the head's last-final ("303").
    let skipper = block_hash("skipper", HEAD + 2);
    record_block_with_final_and_mask(
        &mut em,
        canonical[HEAD as usize],
        skipper,
        HEAD + 2,
        final_of_head,
        HEAD - 2,
        vec![true],
    );

    // "306": the late orphan sibling. Its last-final ("304") is ahead of every block on the
    // canonical fork and moves the aggregator's sync point there.
    record_block_with_final_and_mask(
        &mut em,
        canonical[HEAD as usize],
        block_hash("orphan", HEAD + 1),
        HEAD + 1,
        canonical[HEAD as usize - 1],
        HEAD - 1,
        vec![true],
    );

    // "308": child of the skipper, last-final still "303", behind the aggregator. Held
    // uncommitted, exactly like `ChainUpdate` holds it.
    let child = block_hash("child", HEAD + 3);
    let _store_update = record_block_with_final_and_mask_uncommitted(
        &mut em,
        skipper,
        child,
        HEAD + 3,
        final_of_head,
        HEAD - 2,
        vec![true],
        PROTOCOL_VERSION,
    );

    let block_info = em.get_block_info(&child).expect("uncommitted BlockInfo must stay readable");
    assert_eq!(block_info.height(), HEAD + 3);
}
