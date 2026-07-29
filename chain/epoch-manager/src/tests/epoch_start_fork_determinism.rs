//! Fork-order determinism of the early-kickout epoch-start basis.
//!
//! Two blocks on competing forks that share one parent at an epoch boundary are both
//! epoch-first and both claim the same `EpochId` (the epoch-first decision reads only the
//! parent), so `save_epoch_start` overwrites the shared `EpochStart` row: last write wins,
//! and nothing repairs it when one fork is abandoned. The doomslug fixture in
//! integration-tests already produces this shape (two children of genesis at heights 1 and
//! 3); this file reproduces it at the epoch-manager level and follows it downstream.
//!
//! Three layers, each asserting the *safe* behaviour so a failure is the reproduction:
//!
//! 1. `epoch_start_does_not_depend_on_fork_processing_order` — the consensus basis
//!    (`get_epoch_start_height`, the per-hash `BlockInfo` walk) must match across both
//!    processing orders. The `EpochStart` column itself stays order-dependent by design and
//!    is deliberately not on the consensus path.
//! 2. `grace_expiry_does_not_depend_on_fork_processing_order` — both nodes extend the same
//!    canonical chain past `EARLY_KICKOUT_EPOCH_GRACE_BLOCKS` and must agree on whether the
//!    blacklist is active at every anchor.
//! 3. `anchored_chunk_producer_does_not_depend_on_fork_processing_order` — both nodes must
//!    name the same producer for the anchor's grandchild chunk (the seeded
//!    `DBCol::ChunkProducers` row is read verbatim by chunk validation).
//!
//! Control: `same_processing_order_nodes_agree_everywhere` runs the identical fixture with
//! the same order on both nodes, ruling out fixture nondeterminism.

use crate::reward_calculator::NUM_NS_IN_SECOND;
use crate::test_utils::{DEFAULT_TOTAL_SUPPLY, record_block, setup_default_epoch_manager};
#[cfg(feature = "nightly")]
use crate::{CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET, EARLY_KICKOUT_EPOCH_GRACE_BLOCKS};
use crate::{EpochManager, EpochManagerAdapter, EpochManagerHandle};
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::stateless_validation::chunk_endorsements_bitmap::ChunkEndorsementsBitmap;
use near_primitives::types::{AccountId, Balance, BlockHeight, EpochId, ValidatorId};
#[cfg(feature = "nightly")]
use near_primitives::types::{ShardId, ValidatorInfoIdentifier};
use near_primitives::version::PROTOCOL_VERSION;
#[cfg(feature = "nightly")]
use std::collections::{BTreeMap, HashMap, HashSet};

const STAKE: Balance = Balance::from_yoctonear(1_000_000);

/// Long enough that every height driven here stays inside epoch 0. The only epoch boundary
/// in play is then the genesis boundary, where the two siblings collide.
const EPOCH_LENGTH: u64 = 10_000;

/// Single shard keeps the settlement to both validators, so the down node is always a
/// blacklist candidate and the safety valve (keep-one) never fires.
const NUM_SHARDS: u64 = 1;

/// The two same-parent boundary siblings, exactly the heights the doomslug fixture produces.
const SIBLING_LOW: BlockHeight = 1;
const SIBLING_HIGH: BlockHeight = 3;

fn validators() -> Vec<(AccountId, Balance)> {
    vec![("test0".parse().unwrap(), STAKE), ("test1".parse().unwrap(), STAKE)]
}

fn new_epoch_manager() -> EpochManager {
    setup_default_epoch_manager(validators(), EPOCH_LENGTH, NUM_SHARDS, 3, 90, 60)
}

fn genesis_hash() -> CryptoHash {
    CryptoHash::hash_bytes(b"epoch-start-fork/genesis")
}

/// Fork siblings and canonical blocks live in separate hash namespaces so the sibling at
/// `SIBLING_HIGH` never collides with the canonical block at the same height.
fn sibling_hash(height: BlockHeight) -> CryptoHash {
    CryptoHash::hash_bytes(format!("epoch-start-fork/sibling-{height}").as_bytes())
}

fn canonical_hash(height: BlockHeight) -> CryptoHash {
    CryptoHash::hash_bytes(format!("epoch-start-fork/canonical-{height}").as_bytes())
}

/// Records one block with explicit last-final fields. Not `record_block_with_mask`: deriving
/// last-final from the grandparent is wrong for a genesis-parented sibling at a skipped
/// height, which has no grandparent and nothing final yet.
fn record(
    em: &mut EpochManager,
    prev: CryptoHash,
    cur: CryptoHash,
    height: BlockHeight,
    last_final_hash: CryptoHash,
    last_final_height: BlockHeight,
    chunk_mask: Vec<bool>,
) {
    let epoch_id = em.get_epoch_id(&prev).unwrap();
    let shard_layout = em.get_shard_layout(&epoch_id).unwrap();
    // A missed chunk (mask == false) must carry an EMPTY endorsement bitmap for that shard.
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
            last_final_height,
            last_final_hash,
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

/// Builds one node: genesis, the two boundary siblings in the requested order, then the
/// canonical chain extending the LOW sibling. The orphaned high sibling never feeds the
/// epoch-info aggregator, so the only state it can leave behind is `EpochStart`. The chunk
/// mask is a pure function of height, so both nodes record byte-identical blocks and any
/// divergence observed later is fork order alone.
fn build_node(high_first: bool, down: ValidatorId, head_height: BlockHeight) -> EpochManagerHandle {
    let mut em = new_epoch_manager();
    let genesis = genesis_hash();
    record_block(&mut em, CryptoHash::default(), genesis, 0, vec![]);

    let epoch_info = em.get_epoch_info(&EpochId::default()).unwrap();
    let shard_layout = em.get_shard_layout(&EpochId::default()).unwrap();
    let shard_id = shard_layout.shard_ids().next().unwrap();
    let mask = |height: BlockHeight| -> Vec<bool> {
        vec![epoch_info.sample_chunk_producer(&shard_layout, shard_id, height).unwrap() != down]
    };

    let mut order = [SIBLING_LOW, SIBLING_HIGH];
    if high_first {
        order.reverse();
    }
    for height in order {
        record(
            &mut em,
            genesis,
            sibling_hash(height),
            height,
            // No grandparent, so nothing is final yet on either sibling.
            CryptoHash::default(),
            0,
            mask(height),
        );
    }

    let mut grandparent = genesis;
    let mut prev = sibling_hash(SIBLING_LOW);
    for height in (SIBLING_LOW + 1)..=head_height {
        let cur = canonical_hash(height);
        record(&mut em, prev, cur, height, grandparent, height - 2, mask(height));
        grandparent = prev;
        prev = cur;
    }
    em.into_handle()
}

fn epoch_start(handle: &EpochManagerHandle) -> BlockHeight {
    handle.get_epoch_start_from_epoch_id(&EpochId::default()).unwrap()
}

/// LAYER 1. The epoch-start basis the grace checks consume (`get_epoch_start_height`) must
/// not depend on fork processing order. The `EpochStart` column itself remains
/// order-dependent by design; it is deliberately not on the consensus path.
#[test]
fn epoch_start_does_not_depend_on_fork_processing_order() {
    let low_first = build_node(false, 0, SIBLING_LOW);
    let high_first = build_node(true, 0, SIBLING_LOW);

    println!(
        "EpochStart[EpochId::default()] after two same-parent boundary siblings at heights \
         {SIBLING_LOW} and {SIBLING_HIGH} (order-dependent by design, off the consensus \
         path):\n  \
         node A (committed {SIBLING_LOW} then {SIBLING_HIGH}): {}\n  \
         node B (committed {SIBLING_HIGH} then {SIBLING_LOW}): {}",
        epoch_start(&low_first),
        epoch_start(&high_first),
    );

    for anchor in [sibling_hash(SIBLING_LOW), sibling_hash(SIBLING_HIGH)] {
        let start_low_first = low_first.get_epoch_start_height(&anchor).unwrap();
        let start_high_first = high_first.get_epoch_start_height(&anchor).unwrap();
        assert_eq!(
            start_low_first, start_high_first,
            "two honest nodes that saw the same two blocks disagree about the BlockInfo-walk \
             epoch start for anchor {anchor:?}, purely because they committed the competing \
             forks in different orders: {start_low_first} vs {start_high_first}",
        );
    }
}

/// Anchor heights where the two nodes' `EpochStart` values straddle the grace threshold:
/// last-final is the anchor's grandparent, so a node holding `EpochStart = s` activates the
/// blacklist from `anchor_height = GRACE + s + 2` onward.
#[cfg(feature = "nightly")]
fn disagreement_anchors() -> std::ops::RangeInclusive<BlockHeight> {
    (EARLY_KICKOUT_EPOCH_GRACE_BLOCKS + SIBLING_LOW + 2)
        ..=(EARLY_KICKOUT_EPOCH_GRACE_BLOCKS + SIBLING_HIGH + 1)
}

/// Anchors inspected by layers 2 and 3: the disagreement window plus margin on both sides,
/// so the output shows agreement before it, disagreement inside it, agreement after it.
#[cfg(feature = "nightly")]
fn scan_anchors() -> std::ops::RangeInclusive<BlockHeight> {
    (EARLY_KICKOUT_EPOCH_GRACE_BLOCKS - 2)..=(EARLY_KICKOUT_EPOCH_GRACE_BLOCKS + SIBLING_HIGH + 4)
}

#[cfg(feature = "nightly")]
fn head_height() -> BlockHeight {
    *scan_anchors().end()
}

/// Chooses the down validator so the grandchild height of the first disagreeing anchor is
/// canonically theirs. Otherwise layer 3 would ride on sampler luck: if the canonical pick
/// there is the healthy validator, blacklist-aware and plain picks coincide and the test is
/// silent even when layer 2 diverged. This makes the divergence observable, not manufactured.
#[cfg(feature = "nightly")]
fn pick_down_validator() -> ValidatorId {
    let em = new_epoch_manager();
    let epoch_info = em.get_epoch_info(&EpochId::default()).unwrap();
    let shard_layout = em.get_shard_layout(&EpochId::default()).unwrap();
    let shard_id = shard_layout.shard_ids().next().unwrap();
    let height = disagreement_anchors().start() + CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET;
    epoch_info.sample_chunk_producer(&shard_layout, shard_id, height).unwrap()
}

#[cfg(feature = "nightly")]
fn shard_id_of(handle: &EpochManagerHandle) -> ShardId {
    handle.get_shard_layout(&EpochId::default()).unwrap().shard_ids().next().unwrap()
}

/// What one node believes at one anchor: whether the early-kickout blacklist is active, and
/// which producer the consensus reader returns for the anchor's grandchild chunk.
#[cfg(feature = "nightly")]
#[derive(Debug, PartialEq, Eq)]
struct Belief {
    blacklist: HashMap<ShardId, HashSet<ValidatorId>>,
    /// `get_chunk_producer_info_anchored` reads the seeded `DBCol::ChunkProducers` row
    /// verbatim; this is the value chunk validation compares the chunk's signer against.
    grandchild_producer: AccountId,
}

#[cfg(feature = "nightly")]
fn belief_at(handle: &EpochManagerHandle, anchor_height: BlockHeight) -> Belief {
    let anchor = canonical_hash(anchor_height);
    let shard_id = shard_id_of(handle);
    Belief {
        blacklist: handle.get_chunk_producer_blacklist(&anchor).unwrap(),
        grandchild_producer: handle
            .get_chunk_producer_info_anchored(
                Some(&anchor),
                &EpochId::default(),
                anchor_height + CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET,
                shard_id,
            )
            .unwrap()
            .take_account_id(),
    }
}

/// `(produced, expected)` per `(account, shard)` up to `block_hash`, via
/// `get_validator_info`, which reads the same aggregator `shard_tracker` the blacklist math
/// does — so this is the blacklist's only input besides the epoch start.
#[cfg(feature = "nightly")]
fn chunk_counters(
    handle: &EpochManagerHandle,
    block_hash: CryptoHash,
) -> BTreeMap<(AccountId, ShardId), (u64, u64)> {
    let info = handle.get_validator_info(ValidatorInfoIdentifier::BlockHash(block_hash)).unwrap();
    let mut counters = BTreeMap::new();
    for validator in info.current_validators {
        for (index, shard_id) in validator.shards_produced.iter().enumerate() {
            counters.insert(
                (validator.account_id.clone(), *shard_id),
                (
                    validator.num_produced_chunks_per_shard[index],
                    validator.num_expected_chunks_per_shard[index],
                ),
            );
        }
    }
    counters
}

/// Fails loudly if the fixture cannot express the property: the down node must actually be
/// blacklisted by the end of the scan on *both* nodes, otherwise every comparison below is
/// vacuously equal.
#[cfg(feature = "nightly")]
fn assert_fixture_is_live(handle: &EpochManagerHandle, down: ValidatorId, label: &str) {
    let last = *scan_anchors().end();
    let shard_id = shard_id_of(handle);
    let blacklist = handle.get_chunk_producer_blacklist(&canonical_hash(last)).unwrap();
    let info = handle
        .get_validator_info(ValidatorInfoIdentifier::BlockHash(canonical_hash(last)))
        .unwrap();
    let stats: Vec<String> = info
        .current_validators
        .iter()
        .flat_map(|v| {
            v.shards_produced.iter().enumerate().map(move |(i, shard)| {
                format!(
                    "{} shard {shard}: {}/{}",
                    v.account_id,
                    v.num_produced_chunks_per_shard[i],
                    v.num_expected_chunks_per_shard[i],
                )
            })
        })
        .collect();
    println!("{label} at anchor {last}: blacklist={blacklist:?}; produced/expected {stats:?}");
    assert_eq!(
        blacklist.get(&shard_id),
        Some(&HashSet::from([down])),
        "{label}: the down validator {down} must be blacklisted by anchor {last}, otherwise \
         the grace comparison is vacuous. Got {blacklist:?} with stats {stats:?}",
    );
}

/// Renders the per-anchor comparison so a failure shows exactly where the two nodes split.
#[cfg(feature = "nightly")]
fn render(a: &EpochManagerHandle, b: &EpochManagerHandle) -> (String, Vec<BlockHeight>) {
    let (start_a, start_b) = (epoch_start(a), epoch_start(b));
    let mut lines = Vec::new();
    let mut split = Vec::new();
    for anchor in scan_anchors() {
        let (belief_a, belief_b) = (belief_at(a, anchor), belief_at(b, anchor));
        let differs = belief_a != belief_b;
        if differs {
            split.push(anchor);
        }
        lines.push(format!(
            "  anchor {anchor} (final height {}): A[start={start_a} into_epoch={} \
             blacklist_active={} producer={}] B[start={start_b} into_epoch={} \
             blacklist_active={} producer={}]{}",
            anchor - 2,
            (anchor - 2).saturating_sub(start_a),
            !belief_a.blacklist.is_empty(),
            belief_a.grandchild_producer,
            (anchor - 2).saturating_sub(start_b),
            !belief_b.blacklist.is_empty(),
            belief_b.grandchild_producer,
            if differs { "   <-- DISAGREE" } else { "" },
        ));
    }
    (lines.join("\n"), split)
}

/// Builds the two nodes for layers 2 and 3.
#[cfg(feature = "nightly")]
fn build_pair(high_first_b: bool) -> (EpochManagerHandle, EpochManagerHandle, ValidatorId) {
    let down = pick_down_validator();
    let a = build_node(false, down, head_height());
    let b = build_node(high_first_b, down, head_height());
    assert_fixture_is_live(&a, down, "node A");
    assert_fixture_is_live(&b, down, "node B");
    (a, b, down)
}

/// LAYER 2. Does the divergence reach the grace window: two honest nodes on the same
/// canonical chain must agree on whether the early-kickout blacklist is active at a given
/// anchor.
#[cfg(feature = "nightly")]
#[test]
fn grace_expiry_does_not_depend_on_fork_processing_order() {
    let (a, b, down) = build_pair(true);
    let (table, _) = render(&a, &b);
    println!(
        "grace window (GRACE={EARLY_KICKOUT_EPOCH_GRACE_BLOCKS}, down validator {down}):\n{table}",
    );

    // Pin the aggregator at the first disagreeing anchor's basis so a split there can only
    // come from the epoch-start value, not aggregator drift.
    let first_split = *disagreement_anchors().start();
    let basis = canonical_hash(first_split - 2);
    let counters_a = chunk_counters(&a, basis);
    let counters_b = chunk_counters(&b, basis);
    println!(
        "aggregator at the first disagreeing anchor's basis (block {}): A={counters_a:?} \
         B={counters_b:?}",
        first_split - 2,
    );
    assert_eq!(
        counters_a, counters_b,
        "precondition: at the basis of anchor {first_split} the two nodes' aggregators must be \
         identical, otherwise a grace split there is aggregator drift, not EpochStart",
    );
    // Printed, not asserted: divergent seeded rows also make the end-of-epoch kickout
    // counters drift apart downstream.
    println!(
        "aggregator after the split (block {}): A={:?} B={:?}",
        head_height() - 2,
        chunk_counters(&a, canonical_hash(head_height() - 2)),
        chunk_counters(&b, canonical_hash(head_height() - 2)),
    );

    let mut disagreements = Vec::new();
    for anchor in scan_anchors() {
        let bl_a = a.get_chunk_producer_blacklist(&canonical_hash(anchor)).unwrap();
        let bl_b = b.get_chunk_producer_blacklist(&canonical_hash(anchor)).unwrap();
        if bl_a.is_empty() != bl_b.is_empty() || bl_a != bl_b {
            disagreements.push(format!("anchor {anchor}: A={bl_a:?} B={bl_b:?}"));
        }
    }
    assert!(
        disagreements.is_empty(),
        "two honest nodes with identical blocks, and identical epoch-info aggregators at the \
         basis of the split, disagree about whether the early-kickout grace window has \
         expired, because they committed the two boundary siblings in opposite orders and \
         hold different EpochStart values \
         (A={}, B={}). The grace test in `seed_chunk_producers` / \
         `get_chunk_producer_blacklist` is \
         `anchor.final_height - epoch_start >= {EARLY_KICKOUT_EPOCH_GRACE_BLOCKS}`; a split \
         here means the epoch-start basis is fork-order-dependent again (predicted split \
         anchors {:?}).\n  {}\n{table}",
        epoch_start(&a),
        epoch_start(&b),
        disagreement_anchors(),
        disagreements.join("\n  "),
    );
}

/// LAYER 3. Does the divergence reach consensus: the producer that chunk validation expects
/// for the anchor's grandchild chunk must be the same on both nodes.
#[cfg(feature = "nightly")]
#[test]
fn anchored_chunk_producer_does_not_depend_on_fork_processing_order() {
    let (a, b, down) = build_pair(true);
    let (table, _) = render(&a, &b);
    println!("anchored producers (down validator {down}):\n{table}");

    let shard_id = shard_id_of(&a);
    let mut disagreements = Vec::new();
    for anchor in scan_anchors() {
        let height = anchor + CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET;
        let producer_a = belief_at(&a, anchor).grandchild_producer;
        let producer_b = belief_at(&b, anchor).grandchild_producer;
        if producer_a != producer_b {
            disagreements.push(format!(
                "chunk (height {height}, shard {shard_id}) anchored at {anchor}: \
                 A expects {producer_a}, B expects {producer_b}"
            ));
        }
    }
    assert!(
        disagreements.is_empty(),
        "two honest nodes disagree about who is allowed to produce a chunk. Both hold the \
         same canonical chain and the same aggregator; they differ only in \
         EpochStart[EpochId::default()] (A={}, B={}), left behind by the order in which they \
         committed the two same-parent boundary siblings at heights {SIBLING_LOW} and \
         {SIBLING_HIGH}. get_chunk_producer_info_anchored returns the seeded \
         DBCol::ChunkProducers row verbatim, so each node will reject the other's chunk as \
         signed by the wrong producer.\n  {}\n{table}",
        epoch_start(&a),
        epoch_start(&b),
        disagreements.join("\n  "),
    );
}

/// Control. Same fixture, both nodes using the SAME processing order: everything must match.
/// If this ever fails, the three tests above prove nothing about fork order.
#[cfg(feature = "nightly")]
#[test]
fn same_processing_order_nodes_agree_everywhere() {
    let (a, b, _down) = build_pair(false);
    assert_eq!(
        epoch_start(&a),
        epoch_start(&b),
        "control: identical processing order must give identical EpochStart",
    );
    let (table, split) = render(&a, &b);
    assert!(
        split.is_empty(),
        "control: identical processing order must give identical beliefs, split at \
         {split:?}\n{table}",
    );
}
