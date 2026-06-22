use super::partial_witness_actor::{PartialWitnessActor, witness_version_mismatch};
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
        CryptoHash::hash_bytes(b"prev_prev_block"),
        0,
        b"payload".to_vec(),
        7,
        signer,
        protocol_version,
    )
}

/// V1 witnesses never carry `prev_block_hash` or the grandparent anchor.
#[test]
fn anchor_hashes_absent_for_v1_present_for_v2() {
    let signer = create_test_signer("test_account");
    let block = CryptoHash::hash_bytes(b"block");
    let v1 = make_witness(&signer, block, pre_kickout_version());
    assert!(v1.prev_block_hash().is_none(), "V1 witness must not carry prev_block_hash");
    assert!(v1.prev_prev_block_hash().is_none(), "V1 witness must not carry prev_prev_block_hash");
    let v2 = make_witness(&signer, block, post_kickout_version());
    assert_eq!(v2.prev_block_hash(), Some(&block));
    assert_eq!(v2.prev_prev_block_hash(), Some(&CryptoHash::hash_bytes(b"prev_prev_block")));
}

// Kickout gate is a pure function: test the drop boundary without standing up an actor.

fn v1_witness(signer: &ValidatorSigner) -> VersionedPartialEncodedStateWitness {
    make_witness(signer, CryptoHash::hash_bytes(b"v1_block"), pre_kickout_version())
}

fn v2_witness(signer: &ValidatorSigner) -> VersionedPartialEncodedStateWitness {
    make_witness(signer, CryptoHash::hash_bytes(b"v2_block"), post_kickout_version())
}

#[test]
fn witness_version_mismatch_pre_kickout_drops_v2_proceeds_v1() {
    let signer = create_test_signer("test_account");
    assert!(!witness_version_mismatch(Some(pre_kickout_version()), &v1_witness(&signer)));
    assert!(witness_version_mismatch(Some(pre_kickout_version()), &v2_witness(&signer)));
}

#[test]
fn witness_version_mismatch_post_kickout_drops_v1_proceeds_v2() {
    let signer = create_test_signer("test_account");
    assert!(witness_version_mismatch(Some(post_kickout_version()), &v1_witness(&signer)));
    assert!(!witness_version_mismatch(Some(post_kickout_version()), &v2_witness(&signer)));
}

/// Unknown epoch (header-sync lag) must not drop either variant: V2 traffic never retransmits.
#[test]
fn witness_version_mismatch_unknown_epoch_proceeds_both_variants() {
    let signer = create_test_signer("test_account");
    assert!(!witness_version_mismatch(None, &v1_witness(&signer)));
    assert!(!witness_version_mismatch(None, &v2_witness(&signer)));
}

fn build_v2_witness(
    signer: &ValidatorSigner,
    epoch_id: EpochId,
    prev_block_hash: CryptoHash,
    prev_prev_block_hash: CryptoHash,
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
        prev_prev_block_hash,
        0,
        b"payload".to_vec(),
        7,
        signer,
    ))
}

fn mutable_signer(signer: Arc<ValidatorSigner>) -> MutableValidatorSigner {
    MutableConfigValue::new(Some(signer), "validator_signer")
}

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

/// Forwarded witness on the wrong side of the EarlyKickout boundary → dropped before spawn.
/// Fixture is kicked under both builds (V2/pre-kickout on stable, V1/post-kickout on nightly).
#[test]
fn forward_drops_kicked_witness_before_spawn() {
    let (chain, epoch_manager, runtime, signer) = setup(Clock::real());
    let genesis_hash = *chain.genesis().hash();
    #[cfg(not(feature = "nightly"))]
    let witness = {
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();
        build_v2_witness(
            signer.as_ref(),
            epoch_id,
            genesis_hash,
            CryptoHash::default(),
            1,
            ShardId::new(0),
        )
    };
    #[cfg(feature = "nightly")]
    let witness = make_witness(signer.as_ref(), genesis_hash, pre_kickout_version());

    let spawn_count = Arc::new(AtomicUsize::new(0));
    let spawner = Arc::new(CountingSpawner { count: spawn_count.clone() });
    let actor = build_test_actor(epoch_manager, runtime, signer, spawner);
    actor.handle_partial_encoded_state_witness_forward(witness).unwrap();

    assert_eq!(
        spawn_count.load(Ordering::SeqCst),
        0,
        "kicked forward must be dropped by the gate before spawning validate+store",
    );
}

/// Runs the spawned closure inline so the test sees its side effects.
struct InlineSpawner;

impl AsyncComputationSpawner for InlineSpawner {
    fn spawn_boxed(&self, _name: &str, f: Box<dyn FnOnce() + Send>) {
        f();
    }
}

/// P2-7 regression: a forwarded V2 part for an unsynced epoch (`EpochOutOfBounds`) is dropped quietly.
#[test]
fn forward_drops_v2_on_unknown_epoch_unsynced_prev() {
    let (_chain, epoch_manager, runtime, signer) = setup(Clock::real());
    let bogus_epoch = EpochId(CryptoHash::hash_bytes(b"bogus_epoch"));
    let unknown_prev = CryptoHash::hash_bytes(b"unknown_prev_block");
    let unknown_prev_prev = CryptoHash::hash_bytes(b"unknown_prev_prev_block");
    let witness = build_v2_witness(
        signer.as_ref(),
        bogus_epoch,
        unknown_prev,
        unknown_prev_prev,
        1,
        ShardId::new(0),
    );

    let actor = build_test_actor(epoch_manager, runtime, signer, Arc::new(InlineSpawner));
    actor.handle_partial_encoded_state_witness_forward(witness).unwrap();
}

/// A V2 part with an unprocessed grandparent anchor (node 2+ blocks behind) drops quietly, no spawn.
#[cfg(feature = "nightly")]
#[test]
fn init_emit_drops_v2_on_unprocessed_anchor_without_spawn() {
    let (chain, epoch_manager, runtime, signer) = setup(Clock::real());
    let genesis_hash = *chain.genesis().hash();
    let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();
    let unknown_prev = CryptoHash::hash_bytes(b"unknown_prev_block");
    let unknown_anchor = CryptoHash::hash_bytes(b"unknown_anchor_block");
    let witness = build_v2_witness(
        signer.as_ref(),
        epoch_id,
        unknown_prev,
        unknown_anchor,
        2,
        ShardId::new(0),
    );

    let spawn_count = Arc::new(AtomicUsize::new(0));
    let spawner = Arc::new(CountingSpawner { count: spawn_count.clone() });
    let actor = build_test_actor(epoch_manager, runtime, signer, spawner);
    actor.handle_partial_encoded_state_witness(witness).unwrap();

    assert_eq!(
        spawn_count.load(Ordering::SeqCst),
        0,
        "unresolvable anchor must drop before spawning validate+store",
    );
}
