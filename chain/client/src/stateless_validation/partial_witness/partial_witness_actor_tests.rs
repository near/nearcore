use super::partial_witness_actor::{
    DeferOrigin, PENDING_V2_WITNESS_CACHE_SIZE, PartialWitnessActor, PendingV2WitnessCache,
    ReplayDisposition, pre_check_replay, witness_kicked_out,
};
use crate::metrics;
use crate::stateless_validation::chunk_validation_actor::ChunkValidationSenderForPartialWitness;
use near_async::futures::AsyncComputationSpawner;
use near_async::messaging::{IntoAsyncSender, IntoSender, noop};
use near_async::time::Clock;
use near_chain::test_utils::setup;
use near_chain::types::RuntimeAdapter;
use near_chain_configs::{MutableConfigValue, MutableValidatorSigner};
use near_epoch_manager::EpochManagerAdapter;
use near_network::types::PeerManagerAdapter;
use near_primitives::bandwidth_scheduler::BandwidthRequests;
use near_primitives::congestion_info::CongestionInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::{ShardChunkHeader, ShardChunkHeaderV3};
use near_primitives::stateless_validation::partial_witness::{
    PartialEncodedStateWitnessV2, VersionedPartialEncodedStateWitness,
};
use near_primitives::test_utils::{create_test_signer, test_chunk_header};
use near_primitives::types::{Balance, BlockHeight, EpochId, Gas, ShardId};
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature, ProtocolVersion};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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
    // Origins preserved so replay dispatches correctly.
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
    // One entry per block hash until cap overflows.
    let total = PENDING_V2_WITNESS_CACHE_SIZE + 2;
    let hashes: Vec<CryptoHash> =
        (0..total).map(|i| CryptoHash::hash_bytes(format!("blk_{i}").as_bytes())).collect();
    // Eviction counter is process-global; measure this test's own overflow via a
    // local before/after delta.
    let evictions_before = metrics::PARTIAL_WITNESS_PENDING_CACHE_EVICTIONS_TOTAL.get();
    for h in &hashes {
        cache.insert(*h, make_witness(&signer, *h, post_kickout_version()), DeferOrigin::InitEmit);
    }
    assert_eq!(cache.len(), PENDING_V2_WITNESS_CACHE_SIZE);
    // One eviction per insert past capacity.
    let evictions_delta =
        metrics::PARTIAL_WITNESS_PENDING_CACHE_EVICTIONS_TOTAL.get() - evictions_before;
    assert_eq!(evictions_delta, (total - PENDING_V2_WITNESS_CACHE_SIZE) as u64);

    // Oldest entries were evicted.
    for h in &hashes[..total - PENDING_V2_WITNESS_CACHE_SIZE] {
        assert!(cache.drain(h).is_empty(), "oldest block {h:?} should have been evicted",);
    }
    // Newer entries remain.
    for h in &hashes[total - PENDING_V2_WITNESS_CACHE_SIZE..] {
        assert_eq!(cache.drain(h).len(), 1, "newer block {h:?} must still be cached");
    }
}

/// V1 witnesses never carry `prev_block_hash`, so never inserted into pending
/// pool. Guards invariant that V1 discriminants cannot slip through cache
/// if caller routes wrong.
#[test]
fn prev_block_hash_absent_for_v1() {
    let signer = create_test_signer("test_account");
    let block = CryptoHash::hash_bytes(b"block");
    let v1 = make_witness(&signer, block, pre_kickout_version());
    assert!(v1.prev_block_hash().is_none(), "V1 witness must not carry prev_block_hash");
    let v2 = make_witness(&signer, block, post_kickout_version());
    assert_eq!(v2.prev_block_hash(), Some(&block));
}

/// `drain_all` is the scan-on-notification replay primitive. Returns every
/// entry across every bucket (preserving `prev_block_hash` key so caller
/// can re-insert transient), leaves cache empty.
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

    // Each entry paired with source prev_block_hash — required by
    // scan-on-notification caller to re-insert transient entries.
    for (hash, entry) in &drained {
        assert_eq!(
            entry.witness.prev_block_hash(),
            Some(hash),
            "drained entry's prev_block_hash must match its bucket key",
        );
    }

    // Draining empty cache is safe, returns nothing.
    assert!(cache.drain_all().is_empty());
}

// Kickout gate symmetry tests. Pure function: security boundary (which
// witness variants drop at which kickout state) directly testable without
// standing up an actor.

fn v1_witness(signer: &ValidatorSigner) -> VersionedPartialEncodedStateWitness {
    make_witness(signer, CryptoHash::hash_bytes(b"v1_block"), pre_kickout_version())
}

fn v2_witness(signer: &ValidatorSigner) -> VersionedPartialEncodedStateWitness {
    make_witness(signer, CryptoHash::hash_bytes(b"v2_block"), post_kickout_version())
}

#[test]
fn witness_kicked_out_pre_kickout_drops_v2_proceeds_v1() {
    let signer = create_test_signer("test_account");
    assert!(!witness_kicked_out(Some(pre_kickout_version()), &v1_witness(&signer)));
    assert!(witness_kicked_out(Some(pre_kickout_version()), &v2_witness(&signer)));
}

#[test]
fn witness_kicked_out_post_kickout_drops_v1_proceeds_v2() {
    let signer = create_test_signer("test_account");
    assert!(witness_kicked_out(Some(post_kickout_version()), &v1_witness(&signer)));
    assert!(!witness_kicked_out(Some(post_kickout_version()), &v2_witness(&signer)));
}

/// Unknown epoch (header-sync lag at epoch boundary) must NOT drop either
/// variant. Dropping V2 here discards legitimate post-kickout traffic whose
/// epoch info hasn't landed — no part-retransmission loop, loss permanent.
/// Downstream producer lookup returns `MissingBlock` and defers V2 into
/// pending cache.
#[test]
fn witness_kicked_out_unknown_epoch_proceeds_both_variants() {
    let signer = create_test_signer("test_account");
    assert!(!witness_kicked_out(None, &v1_witness(&signer)));
    assert!(!witness_kicked_out(None, &v2_witness(&signer)));
}

// `pre_check_replay` arm coverage via `setup()`. Post-setup: HEAD == FINAL_HEAD
// == genesis, shard 0, validator "test". Pure classifier (no kickout gate), so a
// V2 fixture in the pre-kickout genesis epoch reaches the arms on stable.

fn build_v2_witness(
    signer: &ValidatorSigner,
    epoch_id: EpochId,
    prev_block_hash: CryptoHash,
    height_created: BlockHeight,
    shard_id: ShardId,
) -> VersionedPartialEncodedStateWitness {
    let chunk_header = ShardChunkHeader::V3(ShardChunkHeaderV3::new(
        prev_block_hash,
        CryptoHash::default(),
        CryptoHash::default(),
        CryptoHash::default(),
        0,
        height_created,
        shard_id,
        Gas::ZERO,
        Gas::ZERO,
        Balance::ZERO,
        CryptoHash::default(),
        CryptoHash::default(),
        vec![],
        CongestionInfo::default(),
        BandwidthRequests::empty(),
        None,
        signer,
        PROTOCOL_VERSION,
    ));
    VersionedPartialEncodedStateWitness::V2(PartialEncodedStateWitnessV2::new(
        epoch_id,
        chunk_header,
        0,
        b"payload".to_vec(),
        7,
        signer,
    ))
}

fn mutable_signer(signer: Arc<ValidatorSigner>) -> MutableValidatorSigner {
    MutableConfigValue::new(Some(signer), "validator_signer")
}

/// Signer None → Requeue (validator reload in progress / not configured).
#[test]
fn pre_check_replay_requeue_signer_unavailable() {
    let (_chain, epoch_manager, runtime, signer) = setup(Clock::real());
    let no_signer =
        MutableConfigValue::<Option<Arc<ValidatorSigner>>>::new(None, "validator_signer");
    let witness = build_v2_witness(
        signer.as_ref(),
        EpochId(CryptoHash::default()),
        CryptoHash::default(),
        1,
        ShardId::new(0),
    );
    assert_eq!(
        pre_check_replay(epoch_manager.as_ref(), runtime.as_ref(), &no_signer, &witness),
        ReplayDisposition::Requeue,
    );
}

/// Height > HEAD + `MAX_HEIGHTS_AHEAD` (= 5) → Requeue.
#[test]
fn pre_check_replay_requeue_too_early() {
    let (chain, epoch_manager, runtime, signer) = setup(Clock::real());
    let genesis_hash = *chain.genesis().hash();
    let head_height = chain.genesis().height();
    let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();
    let too_early_height = head_height + 5 + 1;
    let witness = build_v2_witness(
        signer.as_ref(),
        epoch_id,
        genesis_hash,
        too_early_height,
        ShardId::new(0),
    );
    let my_signer = mutable_signer(signer);
    assert_eq!(
        pre_check_replay(epoch_manager.as_ref(), runtime.as_ref(), &my_signer, &witness),
        ReplayDisposition::Requeue,
    );
}

/// V2 with unknown prev_block_hash → Requeue via `MissingBlock`.
#[test]
fn pre_check_replay_requeue_missing_block_v2() {
    let (chain, epoch_manager, runtime, signer) = setup(Clock::real());
    let genesis_hash = *chain.genesis().hash();
    let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();
    let unknown_prev = CryptoHash::hash_bytes(b"unknown_prev_block");
    // height=1 keeps relevance window happy so V2 producer-DB arm runs.
    let witness = build_v2_witness(signer.as_ref(), epoch_id, unknown_prev, 1, ShardId::new(0));
    let my_signer = mutable_signer(signer);
    assert_eq!(
        pre_check_replay(epoch_manager.as_ref(), runtime.as_ref(), &my_signer, &witness),
        ReplayDisposition::Requeue,
    );
}

/// Height <= FINAL_HEAD → Retire.
#[test]
fn pre_check_replay_retire_too_late() {
    let (chain, epoch_manager, runtime, signer) = setup(Clock::real());
    let genesis_hash = *chain.genesis().hash();
    let final_head_height = chain.genesis().height();
    let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();
    let witness = build_v2_witness(
        signer.as_ref(),
        epoch_id,
        genesis_hash,
        final_head_height,
        ShardId::new(0),
    );
    let my_signer = mutable_signer(signer);
    assert_eq!(
        pre_check_replay(epoch_manager.as_ref(), runtime.as_ref(), &my_signer, &witness),
        ReplayDisposition::Retire,
    );
}

/// Signer account not in chunk validator set → Retire.
#[test]
fn pre_check_replay_retire_not_a_chunk_validator() {
    let (chain, epoch_manager, runtime, _signer) = setup(Clock::real());
    let genesis_hash = *chain.genesis().hash();
    let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();
    let stranger = Arc::new(create_test_signer("not_a_validator"));
    let witness = build_v2_witness(stranger.as_ref(), epoch_id, genesis_hash, 1, ShardId::new(0));
    let my_signer = mutable_signer(stranger);
    assert_eq!(
        pre_check_replay(epoch_manager.as_ref(), runtime.as_ref(), &my_signer, &witness),
        ReplayDisposition::Retire,
    );
}

/// Shard outside layout → Retire (catch-all `Err(_)` arm, `InvalidShardId`).
#[test]
fn pre_check_replay_retire_invalid_shard() {
    let (chain, epoch_manager, runtime, signer) = setup(Clock::real());
    let genesis_hash = *chain.genesis().hash();
    let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();
    let bogus_shard = ShardId::new(9999);
    let witness = build_v2_witness(signer.as_ref(), epoch_id, genesis_hash, 1, bogus_shard);
    let my_signer = mutable_signer(signer);
    assert_eq!(
        pre_check_replay(epoch_manager.as_ref(), runtime.as_ref(), &my_signer, &witness),
        ReplayDisposition::Retire,
    );
}

/// V2 on genesis → Ready. Genesis init populates `DBCol::ChunkProducers` for
/// every (genesis_hash, shard); strict DB read only runs under nightly
/// (adapter.rs:958).
#[cfg(feature = "nightly")]
#[test]
fn pre_check_replay_ready_when_v2_db_resolves() {
    let (chain, epoch_manager, runtime, signer) = setup(Clock::real());
    let genesis_hash = *chain.genesis().hash();
    let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();
    let witness = build_v2_witness(signer.as_ref(), epoch_id, genesis_hash, 1, ShardId::new(0));
    let my_signer = mutable_signer(signer);
    assert_eq!(
        pre_check_replay(epoch_manager.as_ref(), runtime.as_ref(), &my_signer, &witness),
        ReplayDisposition::Ready,
    );
}

/// Bogus `epoch_id` → `EpochOutOfBounds` at relevance. V2 prev unknown → behind
/// on headers → Requeue.
#[test]
fn pre_check_replay_requeue_unknown_epoch_unsynced_prev() {
    let (_chain, epoch_manager, runtime, signer) = setup(Clock::real());
    let bogus_epoch = EpochId(CryptoHash::hash_bytes(b"bogus_epoch"));
    let unknown_prev = CryptoHash::hash_bytes(b"unknown_prev_block");
    let witness = build_v2_witness(signer.as_ref(), bogus_epoch, unknown_prev, 1, ShardId::new(0));
    let my_signer = mutable_signer(signer);
    assert_eq!(
        pre_check_replay(epoch_manager.as_ref(), runtime.as_ref(), &my_signer, &witness),
        ReplayDisposition::Requeue,
    );
}

/// Bogus `epoch_id` → `EpochOutOfBounds`, but V2 prev (genesis) IS known →
/// forged epoch → Retire.
#[test]
fn pre_check_replay_retire_unknown_epoch_known_prev() {
    let (chain, epoch_manager, runtime, signer) = setup(Clock::real());
    let genesis_hash = *chain.genesis().hash();
    let bogus_epoch = EpochId(CryptoHash::hash_bytes(b"bogus_epoch"));
    let witness = build_v2_witness(signer.as_ref(), bogus_epoch, genesis_hash, 1, ShardId::new(0));
    let my_signer = mutable_signer(signer);
    assert_eq!(
        pre_check_replay(epoch_manager.as_ref(), runtime.as_ref(), &my_signer, &witness),
        ReplayDisposition::Retire,
    );
}

/// V2 producer-DB miss. Delete the (genesis_hash, shard 0) slot the lookup reads
/// → `get_chunk_producer_info_db` returns `ChunkProducerNotInDB` → Requeue.
/// Relevance (epoch-info based) unaffected. Strict DB read = nightly only.
#[cfg(feature = "nightly")]
#[test]
fn pre_check_replay_requeue_chunk_producer_not_in_db() {
    use near_primitives::utils::get_block_shard_id;
    use near_store::DBCol;

    let (chain, epoch_manager, runtime, signer) = setup(Clock::real());
    let genesis_hash = *chain.genesis().hash();
    let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();
    let shard_id = ShardId::new(0);

    let key = get_block_shard_id(&genesis_hash, shard_id);
    let mut update = runtime.store().store_update();
    update.delete(DBCol::ChunkProducers, &key);
    update.commit();

    let witness = build_v2_witness(signer.as_ref(), epoch_id, genesis_hash, 1, shard_id);
    let my_signer = mutable_signer(signer);
    assert_eq!(
        pre_check_replay(epoch_manager.as_ref(), runtime.as_ref(), &my_signer, &witness),
        ReplayDisposition::Requeue,
    );
}

// C6 regression: kickout gate on the replay act-site.
// `replay_forwarded_partial_witness` was the one act-site missing the gate; prove
// a kicked witness is dropped before spawn.

/// Counts spawns, doesn't run the closure.
struct CountingSpawner {
    count: Arc<AtomicUsize>,
}

impl AsyncComputationSpawner for CountingSpawner {
    fn spawn_boxed(&self, _name: &str, _f: Box<dyn FnOnce() + Send>) {
        self.count.fetch_add(1, Ordering::SeqCst);
    }
}

/// `PartialWitnessActor` with noop senders + `spawner` in all 3 spawn slots.
/// Tests hit only `partial_witness_spawner`; gate-drop/defer paths never touch
/// network/tracker.
fn build_test_actor(
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    runtime: Arc<dyn RuntimeAdapter>,
    signer: Arc<ValidatorSigner>,
    spawner: Arc<dyn AsyncComputationSpawner>,
) -> PartialWitnessActor {
    let network_adapter = PeerManagerAdapter {
        async_request_sender: noop().into_async_sender(),
        request_sender: noop().into_sender(),
        set_chain_info_sender: noop().into_sender(),
        state_sync_event_sender: noop().into_sender(),
    };
    let chunk_validation_sender =
        ChunkValidationSenderForPartialWitness { chunk_state_witness: noop().into_sender() };
    PartialWitnessActor::new(
        Clock::real(),
        network_adapter,
        chunk_validation_sender,
        mutable_signer(signer),
        epoch_manager,
        runtime,
        spawner.clone(),
        spawner.clone(),
        spawner,
    )
}

/// Forwarded witness on the wrong side of the EarlyKickout boundary → dropped by
/// the gate before spawn. Fixture kicked under both builds: V2/pre-kickout
/// (stable), V1/post-kickout (nightly). Signer present → spawn count 0 = gate,
/// not missing signer.
#[test]
fn replay_forwarded_drops_kicked_witness() {
    let (chain, epoch_manager, runtime, signer) = setup(Clock::real());
    let genesis_hash = *chain.genesis().hash();
    #[cfg(not(feature = "nightly"))]
    let witness = {
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();
        build_v2_witness(signer.as_ref(), epoch_id, genesis_hash, 1, ShardId::new(0))
    };
    #[cfg(feature = "nightly")]
    let witness = make_witness(signer.as_ref(), genesis_hash, pre_kickout_version());

    let spawn_count = Arc::new(AtomicUsize::new(0));
    let spawner = Arc::new(CountingSpawner { count: spawn_count.clone() });
    let actor = build_test_actor(epoch_manager, runtime, signer, spawner);
    actor.replay_forwarded_partial_witness(witness);

    assert_eq!(
        spawn_count.load(Ordering::SeqCst),
        0,
        "kicked V2 forward must be dropped by the gate before spawning validate+store",
    );
}

/// Runs the spawned closure inline so the test sees its side effects.
#[cfg(feature = "test_features")]
struct InlineSpawner;

#[cfg(feature = "test_features")]
impl AsyncComputationSpawner for InlineSpawner {
    fn spawn_boxed(&self, _name: &str, f: Box<dyn FnOnce() + Send>) {
        f();
    }
}

/// Forwarded V2 part for an unsynced epoch must defer, not drop:
/// `validate_partial_encoded_state_witness` resolves validator assignments from
/// the signed `epoch_id` first → unknown epoch = `EpochOutOfBounds`, not
/// `DBNotFoundErr`. Bogus epoch unresolvable → gate no-op (version `None`) → part
/// reaches spawner → new arm defers. Without fix: dropped, never retransmitted.
#[cfg(feature = "test_features")]
#[test]
fn forward_defers_v2_on_unknown_epoch_unsynced_prev() {
    let (_chain, epoch_manager, runtime, signer) = setup(Clock::real());
    let bogus_epoch = EpochId(CryptoHash::hash_bytes(b"bogus_epoch"));
    let unknown_prev = CryptoHash::hash_bytes(b"unknown_prev_block");
    let witness = build_v2_witness(signer.as_ref(), bogus_epoch, unknown_prev, 1, ShardId::new(0));

    let actor = build_test_actor(epoch_manager, runtime, signer, Arc::new(InlineSpawner));
    actor.handle_partial_encoded_state_witness_forward(witness).unwrap();

    assert_eq!(
        actor.pending_cache_bucket_count(),
        1,
        "forwarded V2 part on an unsynced epoch must be deferred, not dropped",
    );
}
