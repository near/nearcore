use super::partial_witness_actor::{
    DeferOrigin, KickoutGate, PENDING_V2_WITNESS_CACHE_SIZE, PendingV2WitnessCache, kickout_gate,
};
use near_primitives::hash::CryptoHash;
use near_primitives::stateless_validation::partial_witness::VersionedPartialEncodedStateWitness;
use near_primitives::test_utils::{create_test_signer, test_chunk_header};
use near_primitives::types::EpochId;
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::version::{ProtocolFeature, ProtocolVersion};

fn post_kickout_version() -> ProtocolVersion {
    ProtocolFeature::EarlyKickout.protocol_version()
}

fn pre_kickout_version() -> ProtocolVersion {
    ProtocolFeature::EarlyKickout.protocol_version().checked_sub(1).unwrap()
}

fn make_witness(
    signer: &ValidatorSigner,
    prev_block_hash: CryptoHash,
    protocol_version: ProtocolVersion,
) -> VersionedPartialEncodedStateWitness {
    let chunk_header = test_chunk_header(prev_block_hash, signer, protocol_version);
    VersionedPartialEncodedStateWitness::new(
        EpochId(CryptoHash::default()),
        chunk_header,
        0,
        b"payload".to_vec(),
        7,
        signer,
        protocol_version,
    )
}

#[test]
fn inserts_group_by_prev_block_hash() {
    let signer = create_test_signer("test_account");
    let block_a = CryptoHash::hash_bytes(b"block_a");
    let block_b = CryptoHash::hash_bytes(b"block_b");
    let mut cache = PendingV2WitnessCache::new();

    cache.insert(
        block_a,
        make_witness(&signer, block_a, post_kickout_version()),
        DeferOrigin::InitEmit,
    );
    cache.insert(
        block_a,
        make_witness(&signer, block_a, post_kickout_version()),
        DeferOrigin::Forwarded,
    );
    cache.insert(
        block_b,
        make_witness(&signer, block_b, post_kickout_version()),
        DeferOrigin::InitEmit,
    );
    assert_eq!(cache.len(), 2);

    let drained_a = cache.drain(&block_a);
    assert_eq!(drained_a.len(), 2);
    assert!(
        drained_a.iter().all(|e| e.witness.prev_block_hash() == Some(&block_a)),
        "drained witnesses must all point to block_a",
    );
    // Both origins are preserved so replay can dispatch correctly.
    assert!(drained_a.iter().any(|e| e.origin == DeferOrigin::InitEmit), "init-emit entry present",);
    assert!(drained_a.iter().any(|e| e.origin == DeferOrigin::Forwarded), "forward entry present",);

    let drained_b = cache.drain(&block_b);
    assert_eq!(drained_b.len(), 1);
    assert_eq!(drained_b[0].witness.prev_block_hash(), Some(&block_b));
    assert_eq!(drained_b[0].origin, DeferOrigin::InitEmit);

    assert_eq!(cache.len(), 0);
    assert!(cache.drain(&block_a).is_empty(), "re-draining yields nothing");
}

#[test]
fn drain_unknown_block_is_empty() {
    let mut cache = PendingV2WitnessCache::new();
    assert!(cache.drain(&CryptoHash::hash_bytes(b"absent")).is_empty());
}

#[test]
fn capacity_cap_evicts_oldest_block() {
    let signer = create_test_signer("test_account");
    let mut cache = PendingV2WitnessCache::new();
    // Insert one entry per block hash until we overflow the cap.
    let total = PENDING_V2_WITNESS_CACHE_SIZE + 2;
    let hashes: Vec<CryptoHash> =
        (0..total).map(|i| CryptoHash::hash_bytes(format!("blk_{i}").as_bytes())).collect();
    for h in &hashes {
        cache.insert(*h, make_witness(&signer, *h, post_kickout_version()), DeferOrigin::InitEmit);
    }
    assert_eq!(cache.len(), PENDING_V2_WITNESS_CACHE_SIZE);

    // Oldest entries were evicted.
    for h in &hashes[..total - PENDING_V2_WITNESS_CACHE_SIZE] {
        assert!(cache.drain(h).is_empty(), "oldest block {h:?} should have been evicted",);
    }
    // Newer entries remain.
    for h in &hashes[total - PENDING_V2_WITNESS_CACHE_SIZE..] {
        assert_eq!(cache.drain(h).len(), 1, "newer block {h:?} must still be cached");
    }
}

/// V1 witnesses never carry a `prev_block_hash`, so they are never inserted
/// into the pending pool. This test guards the invariant that V1
/// discriminants can't accidentally slip through the cache if a caller
/// mistakenly routes them here.
#[test]
fn prev_block_hash_absent_for_v1() {
    let signer = create_test_signer("test_account");
    let block = CryptoHash::hash_bytes(b"block");
    let v1 = make_witness(&signer, block, pre_kickout_version());
    assert!(v1.prev_block_hash().is_none(), "V1 witness must not carry prev_block_hash");
    let v2 = make_witness(&signer, block, post_kickout_version());
    assert_eq!(v2.prev_block_hash(), Some(&block));
}

/// `drain_all` is the scan-on-notification replay primitive. It must
/// return every entry across every bucket (preserving the
/// `prev_block_hash` key so the caller can re-insert transient ones),
/// and leave the cache empty.
#[test]
fn drain_all_returns_every_entry_and_empties_cache() {
    let signer = create_test_signer("test_account");
    let block_a = CryptoHash::hash_bytes(b"drain_all_a");
    let block_b = CryptoHash::hash_bytes(b"drain_all_b");
    let mut cache = PendingV2WitnessCache::new();

    cache.insert(
        block_a,
        make_witness(&signer, block_a, post_kickout_version()),
        DeferOrigin::InitEmit,
    );
    cache.insert(
        block_a,
        make_witness(&signer, block_a, post_kickout_version()),
        DeferOrigin::Forwarded,
    );
    cache.insert(
        block_b,
        make_witness(&signer, block_b, post_kickout_version()),
        DeferOrigin::InitEmit,
    );
    assert_eq!(cache.len(), 2);

    let drained = cache.drain_all();
    assert_eq!(drained.len(), 3, "every entry across both buckets returned");
    assert_eq!(cache.len(), 0, "cache empty after drain_all");

    // Both origin variants survived the drain.
    assert!(drained.iter().any(|(_, e)| e.origin == DeferOrigin::InitEmit));
    assert!(drained.iter().any(|(_, e)| e.origin == DeferOrigin::Forwarded));

    // Each entry is paired with its source prev_block_hash — required by
    // the scan-on-notification caller to re-insert transient entries.
    for (hash, entry) in &drained {
        assert_eq!(
            entry.witness.prev_block_hash(),
            Some(hash),
            "drained entry's prev_block_hash must match its bucket key",
        );
    }

    // Draining an already-empty cache is safe and returns nothing.
    assert!(cache.drain_all().is_empty());
}

// Kickout gate symmetry tests. The gate is a pure function so the
// security boundary (which witness variants are dropped at which
// kickout state) is directly testable without standing up an actor.

fn v1_witness(signer: &ValidatorSigner) -> VersionedPartialEncodedStateWitness {
    make_witness(signer, CryptoHash::hash_bytes(b"v1_block"), pre_kickout_version())
}

fn v2_witness(signer: &ValidatorSigner) -> VersionedPartialEncodedStateWitness {
    make_witness(signer, CryptoHash::hash_bytes(b"v2_block"), post_kickout_version())
}

#[test]
fn kickout_gate_pre_kickout_drops_v2_proceeds_v1() {
    let signer = create_test_signer("test_account");
    assert_eq!(kickout_gate(Some(false), &v1_witness(&signer)), KickoutGate::Proceed);
    assert_eq!(kickout_gate(Some(false), &v2_witness(&signer)), KickoutGate::Drop);
}

#[test]
fn kickout_gate_post_kickout_drops_v1_proceeds_v2() {
    let signer = create_test_signer("test_account");
    assert_eq!(kickout_gate(Some(true), &v1_witness(&signer)), KickoutGate::Drop);
    assert_eq!(kickout_gate(Some(true), &v2_witness(&signer)), KickoutGate::Proceed);
}

/// Unknown epoch (header-sync lag at epoch boundary) must NOT drop
/// either variant. Dropping V2 here would discard legitimate
/// post-kickout traffic whose epoch info hasn't landed yet — there is
/// no part-retransmission loop, so the loss is permanent. The
/// downstream producer lookup will return `MissingBlock` and defer
/// V2 into the pending cache.
#[test]
fn kickout_gate_unknown_epoch_proceeds_both_variants() {
    let signer = create_test_signer("test_account");
    assert_eq!(kickout_gate(None, &v1_witness(&signer)), KickoutGate::Proceed);
    assert_eq!(kickout_gate(None, &v2_witness(&signer)), KickoutGate::Proceed);
}
