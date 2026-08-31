//! Fork-order determinism of the early-kickout epoch-start basis.
//!
//! Two blocks on competing forks that share one parent at an epoch boundary are both
//! epoch-first and both claim the same `EpochId` (the epoch-first decision reads only the
//! parent), so `save_epoch_start` overwrites the shared `EpochStart` row: last write wins,
//! and nothing repairs it when one fork is abandoned. The doomslug fixture in
//! integration-tests already produces this shape (two children of genesis at heights 1 and
//! 3); this file reproduces it at the epoch-manager level and follows it downstream.
//!
//! Three layers, each asserting the *safe* behaviour. Layers 2 and 3 are the reproduction:
//! they fail before the fix and pass after it. Layer 1 pins the basis the fix switches to,
//! which was already fork-order independent, so it passed before the fix as well.
//!
//! 1. `epoch_start_does_not_depend_on_fork_processing_order` — the consensus basis
//!    (`get_epoch_start_height`, the per-hash `BlockInfo` walk) must match across both
//!    processing orders. Pre-existing property, pinned here so a future change away from the
//!    walk shows up. The `EpochStart` column itself stays order-dependent by design and is
//!    deliberately not on the consensus path.
//! 2. `grace_expiry_does_not_depend_on_fork_processing_order` — both nodes extend the same
//!    canonical chain past `EARLY_KICKOUT_EPOCH_GRACE_BLOCKS` and must agree on whether the
//!    blacklist is active at every anchor.
//! 3. `anchored_chunk_producer_does_not_depend_on_fork_processing_order` — both nodes must
//!    name the same producer for the anchor's grandchild chunk (the seeded
//!    `DBCol::ChunkProducers` row is read verbatim by chunk validation).
//!
//! Control: `same_processing_order_nodes_agree_everywhere` runs the identical fixture with
//! the same order on both nodes, ruling out fixture nondeterminism.

use crate::test_utils::{
    record_block, record_block_with_final_and_mask, setup_default_epoch_manager,
};
use crate::{CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET, EARLY_KICKOUT_EPOCH_GRACE_BLOCKS};
use crate::{EpochManager, EpochManagerAdapter, EpochManagerHandle};
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, Balance, BlockHeight, EpochId, ValidatorId};
use near_primitives::types::{ShardId, ValidatorInfoIdentifier};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::RangeInclusive;

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

/// The fixture's ~2-block finality: a block's stored last-final is its grandparent. Ties
/// `build_node`, the disagreement window, and the diagnostic table to one definition.
const FINALITY_LAG: BlockHeight = 2;

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
        // Explicit finals rather than the grandparent-derived ones: a genesis-parented
        // sibling has no grandparent and nothing final yet.
        record_block_with_final_and_mask(
            &mut em,
            genesis,
            sibling_hash(height),
            height,
            CryptoHash::default(),
            0,
            mask(height),
        );
    }

    let mut grandparent = genesis;
    let mut prev = sibling_hash(SIBLING_LOW);
    for height in (SIBLING_LOW + 1)..=head_height {
        let cur = canonical_hash(height);
        record_block_with_final_and_mask(
            &mut em,
            prev,
            cur,
            height,
            grandparent,
            height - FINALITY_LAG,
            mask(height),
        );
        grandparent = prev;
        prev = cur;
    }
    em.into_handle()
}

fn epoch_start(handle: &EpochManagerHandle) -> BlockHeight {
    handle.get_epoch_start_from_epoch_id(&EpochId::default()).unwrap()
}

/// LAYER 1. The epoch-start basis the grace checks consume (`get_epoch_start_height`) must
/// not depend on fork processing order. This held before the fix too — the walk is what the
/// fix moves the grace check onto, and this layer pins it. The `EpochStart` column itself
/// remains order-dependent by design; it is deliberately not on the consensus path.
#[test]
fn epoch_start_does_not_depend_on_fork_processing_order() {
    // Head past the siblings, so the canonical anchors below exercise a real multi-block
    // walk back to the epoch-first sibling, not just the siblings' self-resolution.
    let low_first = build_node(false, 0, SIBLING_HIGH + 2);
    let high_first = build_node(true, 0, SIBLING_HIGH + 2);

    println!(
        "EpochStart[EpochId::default()] after two same-parent boundary siblings at heights \
         {SIBLING_LOW} and {SIBLING_HIGH} (order-dependent by design, off the consensus \
         path):\n  \
         node A (committed {SIBLING_LOW} then {SIBLING_HIGH}): {}\n  \
         node B (committed {SIBLING_HIGH} then {SIBLING_LOW}): {}",
        epoch_start(&low_first),
        epoch_start(&high_first),
    );
    // The fixture must keep producing the column collision, or everything below is vacuous.
    assert_ne!(
        epoch_start(&low_first),
        epoch_start(&high_first),
        "fixture no longer produces the EpochStart collision",
    );

    // The boundary siblings are epoch-first blocks: the walk resolves to the anchor itself.
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

    // Canonical anchors walk across blocks back to the LOW sibling (the epoch-first block on
    // the canonical chain), so the expected value is pinned, not just cross-node equality.
    for height in (SIBLING_LOW + 1)..=(SIBLING_HIGH + 2) {
        let anchor = canonical_hash(height);
        for (label, node) in [("low-first", &low_first), ("high-first", &high_first)] {
            assert_eq!(
                node.get_epoch_start_height(&anchor).unwrap(),
                SIBLING_LOW,
                "{label}: the walk from canonical anchor at height {height} must reach the \
                 epoch-first sibling at height {SIBLING_LOW}",
            );
        }
    }
}

/// The genesis end of the walk: `get_epoch_start_height` resolves genesis to height 0 through
/// the dummy `BlockInfo` stored as its parent (`genesis.rs`) instead of erroring. This is the
/// case the removed `EpochOutOfBounds -> grace` mapping used to cover.
#[test]
fn genesis_final_basis_resolves_through_dummy_instead_of_erroring() {
    // Head just past the siblings: the anchor at height 2 is the first canonical block, and
    // `build_node` gives it genesis as its last-final block.
    let handle = build_node(false, 0, SIBLING_HIGH + 1);
    assert_eq!(
        handle.get_epoch_start_height(&genesis_hash()).unwrap(),
        0,
        "genesis must resolve to height 0 through the stored dummy BlockInfo",
    );
    {
        let blacklist = handle.get_chunk_producer_blacklist(&canonical_hash(2)).unwrap();
        assert!(
            blacklist.is_empty(),
            "this fixture's genesis is at height 0, so a genesis-final anchor is 0 blocks \
             into the epoch and the grace must hold, got {blacklist:?}",
        );
    }
}

/// Same walk mechanics on a chain whose genesis is NOT at height 0 (forknet forks mainnet
/// state at heights in the hundreds of millions): the walk resolves a genesis-final basis
/// through the dummy `BlockInfo` to height 0, so `blocks_into_epoch` lands numerically far
/// past the grace window. The blacklist must still be empty — a genesis-final basis has an
/// empty aggregator — and this pin keeps that safety a contract instead of an accident.
#[test]
fn high_genesis_final_basis_keeps_blacklist_empty() {
    const GENESIS_HEIGHT: BlockHeight = 150_000_000;
    let mut em = new_epoch_manager();
    let genesis = genesis_hash();
    record_block(&mut em, CryptoHash::default(), genesis, GENESIS_HEIGHT, vec![]);
    // First canonical block has nothing final yet; the second is final on genesis, the
    // same shape `build_node` gives the first block past the siblings.
    record_block_with_final_and_mask(
        &mut em,
        genesis,
        canonical_hash(GENESIS_HEIGHT + 1),
        GENESIS_HEIGHT + 1,
        CryptoHash::default(),
        0,
        vec![true],
    );
    record_block_with_final_and_mask(
        &mut em,
        canonical_hash(GENESIS_HEIGHT + 1),
        canonical_hash(GENESIS_HEIGHT + 2),
        GENESIS_HEIGHT + 2,
        genesis,
        GENESIS_HEIGHT,
        vec![true],
    );
    let handle = em.into_handle();
    assert_eq!(
        handle.get_epoch_start_height(&genesis).unwrap(),
        0,
        "the walk resolves a high genesis through the dummy BlockInfo to height 0",
    );
    {
        let blacklist =
            handle.get_chunk_producer_blacklist(&canonical_hash(GENESIS_HEIGHT + 2)).unwrap();
        assert!(
            blacklist.is_empty(),
            "a genesis-final basis must keep the blacklist empty even though \
             blocks_into_epoch ({GENESIS_HEIGHT}) is far past the grace window: the empty \
             aggregator at genesis — not the grace arithmetic — is what protects \
             high-genesis chains, got {blacklist:?}",
        );
    }
}

/// Anchor heights where the two nodes' `EpochStart` values straddle the grace threshold:
/// last-final is the anchor's grandparent, so a node holding `EpochStart = s` activates the
/// blacklist from `anchor_height = GRACE + s + 2` onward.
fn disagreement_anchors() -> RangeInclusive<BlockHeight> {
    (EARLY_KICKOUT_EPOCH_GRACE_BLOCKS + SIBLING_LOW + FINALITY_LAG)
        ..=(EARLY_KICKOUT_EPOCH_GRACE_BLOCKS + SIBLING_HIGH + FINALITY_LAG - 1)
}

/// Anchors inspected by layers 2 and 3: the disagreement window plus margin on both sides,
/// so the output shows agreement before it, disagreement inside it, agreement after it.
fn scan_anchors() -> RangeInclusive<BlockHeight> {
    (EARLY_KICKOUT_EPOCH_GRACE_BLOCKS - 2)..=(EARLY_KICKOUT_EPOCH_GRACE_BLOCKS + SIBLING_HIGH + 4)
}

fn head_height() -> BlockHeight {
    *scan_anchors().end()
}

/// Chooses the down validator so the grandchild height of the first disagreeing anchor is
/// canonically theirs. Otherwise layer 3 would ride on sampler luck: if the canonical pick
/// there is the healthy validator, blacklist-aware and plain picks coincide and the test is
/// silent even when layer 2 diverged. This makes the divergence observable, not manufactured.
fn pick_down_validator() -> ValidatorId {
    let em = new_epoch_manager();
    let epoch_info = em.get_epoch_info(&EpochId::default()).unwrap();
    let shard_layout = em.get_shard_layout(&EpochId::default()).unwrap();
    let shard_id = shard_layout.shard_ids().next().unwrap();
    let height = disagreement_anchors().start() + CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET;
    epoch_info.sample_chunk_producer(&shard_layout, shard_id, height).unwrap()
}

fn shard_id_of(handle: &EpochManagerHandle) -> ShardId {
    handle.get_shard_layout(&EpochId::default()).unwrap().shard_ids().next().unwrap()
}

/// What one node believes at one anchor: whether the early-kickout blacklist is active, and
/// which producer the consensus reader returns for the anchor's grandchild chunk.
#[derive(Debug, PartialEq, Eq)]
struct Belief {
    blacklist: HashMap<ShardId, HashSet<ValidatorId>>,
    /// `get_chunk_producer_info_anchored` reads the seeded `DBCol::ChunkProducers` row
    /// verbatim; this is the value chunk validation compares the chunk's signer against.
    grandchild_producer: AccountId,
    /// The walk-based epoch start of the anchor's stored last-final block — the value the
    /// grace checks consume. Part of `Belief` equality, so a DISAGREE covers the walk basis
    /// as well as the consensus outputs.
    walk_epoch_start: BlockHeight,
}

fn belief_at(handle: &EpochManagerHandle, anchor_height: BlockHeight) -> Belief {
    let anchor = canonical_hash(anchor_height);
    let shard_id = shard_id_of(handle);
    // The anchor's stored last-final basis, not fixture math, so this stays exactly what
    // the grace sites consume even if the fixture's finality shape changes.
    let final_hash = *handle.read().get_block_info(&anchor).unwrap().last_final_block_hash();
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
        walk_epoch_start: handle.get_epoch_start_height(&final_hash).unwrap(),
    }
}

/// `(produced, expected)` per `(account, shard)` up to `block_hash`, via
/// `get_validator_info`, which reads the same aggregator `shard_tracker` the blacklist math
/// does — so this is the blacklist's only input besides the epoch start.
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
fn assert_fixture_is_live(handle: &EpochManagerHandle, down: ValidatorId, label: &str) {
    let last = *scan_anchors().end();
    let shard_id = shard_id_of(handle);
    let blacklist = handle.get_chunk_producer_blacklist(&canonical_hash(last)).unwrap();
    let stats = chunk_counters(handle, canonical_hash(last));
    println!("{label} at anchor {last}: blacklist={blacklist:?}; produced/expected {stats:?}");
    assert_eq!(
        blacklist.get(&shard_id),
        Some(&HashSet::from([down])),
        "{label}: the down validator {down} must be blacklisted by anchor {last}, otherwise \
         the grace comparison is vacuous. Got {blacklist:?} with stats {stats:?}",
    );
}

/// Reads both nodes' beliefs at every scanned anchor, once. Layers 2 and 3 filter these rows
/// rather than sweeping the accessors again, so the table and the assertions can't disagree.
fn scan(a: &EpochManagerHandle, b: &EpochManagerHandle) -> Vec<(BlockHeight, Belief, Belief)> {
    scan_anchors().map(|anchor| (anchor, belief_at(a, anchor), belief_at(b, anchor))).collect()
}

/// Renders the per-anchor comparison so a failure shows exactly where the two nodes split.
/// Returns the table and the anchors where the two beliefs differ in any field.
fn render_table(
    rows: &[(BlockHeight, Belief, Belief)],
    start_a: BlockHeight,
    start_b: BlockHeight,
) -> (String, Vec<BlockHeight>) {
    let mut lines = Vec::new();
    let mut split = Vec::new();
    for (anchor, belief_a, belief_b) in rows {
        let differs = belief_a != belief_b;
        if differs {
            split.push(*anchor);
        }
        // `col_start` is the stale `EpochStart` column (the repro's subject); `walk_start`
        // is what the grace checks consume, so `into_epoch` derives from the walk.
        lines.push(format!(
            "  anchor {anchor} (final height {}): A[col_start={start_a} walk_start={} \
             into_epoch={} blacklist_active={} producer={}] B[col_start={start_b} \
             walk_start={} into_epoch={} blacklist_active={} producer={}]{}",
            anchor - FINALITY_LAG,
            belief_a.walk_epoch_start,
            (anchor - FINALITY_LAG).saturating_sub(belief_a.walk_epoch_start),
            !belief_a.blacklist.is_empty(),
            belief_a.grandchild_producer,
            belief_b.walk_epoch_start,
            (anchor - FINALITY_LAG).saturating_sub(belief_b.walk_epoch_start),
            !belief_b.blacklist.is_empty(),
            belief_b.grandchild_producer,
            if differs { "   <-- DISAGREE" } else { "" },
        ));
    }
    (lines.join("\n"), split)
}

/// Builds the two nodes for layers 2 and 3.
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
#[test]
fn grace_expiry_does_not_depend_on_fork_processing_order() {
    let (a, b, down) = build_pair(true);
    let rows = scan(&a, &b);
    let (table, _) = render_table(&rows, epoch_start(&a), epoch_start(&b));
    println!(
        "grace window (GRACE={EARLY_KICKOUT_EPOCH_GRACE_BLOCKS}, down validator {down}):\n{table}",
    );

    // Pin the aggregator at the first disagreeing anchor's basis so a split there can only
    // come from the epoch-start value, not aggregator drift.
    let first_split = *disagreement_anchors().start();
    let basis = canonical_hash(first_split - FINALITY_LAG);
    let counters_a = chunk_counters(&a, basis);
    let counters_b = chunk_counters(&b, basis);
    println!(
        "aggregator at the first disagreeing anchor's basis (block {}): A={counters_a:?} \
         B={counters_b:?}",
        first_split - FINALITY_LAG,
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
        head_height() - FINALITY_LAG,
        chunk_counters(&a, canonical_hash(head_height() - FINALITY_LAG)),
        chunk_counters(&b, canonical_hash(head_height() - FINALITY_LAG)),
    );

    let disagreements: Vec<String> = rows
        .iter()
        .filter(|(_, belief_a, belief_b)| belief_a.blacklist != belief_b.blacklist)
        .map(|(anchor, belief_a, belief_b)| {
            format!("anchor {anchor}: A={:?} B={:?}", belief_a.blacklist, belief_b.blacklist)
        })
        .collect();
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
#[test]
fn anchored_chunk_producer_does_not_depend_on_fork_processing_order() {
    let (a, b, down) = build_pair(true);
    let rows = scan(&a, &b);
    let (table, _) = render_table(&rows, epoch_start(&a), epoch_start(&b));
    println!("anchored producers (down validator {down}):\n{table}");

    let shard_id = shard_id_of(&a);
    let disagreements: Vec<String> = rows
        .iter()
        .filter(|(_, belief_a, belief_b)| {
            belief_a.grandchild_producer != belief_b.grandchild_producer
        })
        .map(|(anchor, belief_a, belief_b)| {
            let height = anchor + CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET;
            format!(
                "chunk (height {height}, shard {shard_id}) anchored at {anchor}: \
                 A expects {}, B expects {}",
                belief_a.grandchild_producer, belief_b.grandchild_producer,
            )
        })
        .collect();
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
#[test]
fn same_processing_order_nodes_agree_everywhere() {
    let (a, b, _down) = build_pair(false);
    assert_eq!(
        epoch_start(&a),
        epoch_start(&b),
        "control: identical processing order must give identical EpochStart",
    );
    let rows = scan(&a, &b);
    let (table, split) = render_table(&rows, epoch_start(&a), epoch_start(&b));
    assert!(
        split.is_empty(),
        "control: identical processing order must give identical beliefs, split at \
         {split:?}\n{table}",
    );
}
