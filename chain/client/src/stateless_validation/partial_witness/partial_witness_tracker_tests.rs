use super::encoding::WITNESS_RATIO_DATA_PARTS;
use super::partial_witness_tracker::{
    ACCESSED_CONTRACTS_REQUEST_TIMEOUT, PartialEncodedStateWitnessTracker,
};
use crate::stateless_validation::chunk_validation_actor::ChunkValidationSenderForPartialWitness;
use near_async::messaging::Sender;
use near_async::time::{Clock, Duration, FakeClock, Utc};
use near_chain::chain::ChunkStateWitnessMessage;
use near_chain::test_utils::{process_block_sync, setup};
use near_chain::{BlockProcessingArtifact, Chain, ChainStoreAccess, Provenance};
use near_chain_configs::Genesis;
use near_epoch_manager::{EpochManager, EpochManagerHandle};
use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_primitives::reed_solomon::ReedSolomonEncoderCache;
use near_primitives::state::PartialState;
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::stateless_validation::contract_distribution::{CodeBytes, CodeHash};
use near_primitives::stateless_validation::partial_witness::VersionedPartialEncodedStateWitness;
use near_primitives::stateless_validation::state_witness::{
    ChunkStateWitness, EncodedChunkStateWitness,
};
use near_primitives::test_utils::{TestBlockBuilder, create_test_signer, test_chunk_header};
use near_primitives::types::{AccountId, BlockHeight, EpochId, ShardId};
use near_primitives::utils::compression::CompressedData;
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature, ProtocolVersion};
use near_store::Store;
use near_store::genesis::initialize_genesis_state;
use near_store::test_utils::create_test_store;
use parking_lot::Mutex;
use std::collections::HashSet;
use std::sync::Arc;

const SHARD_ID: ShardId = ShardId::new(0);

/// Validators in the [`tracker_with_four_validators`] epoch, which is also the number of parts a
/// witness is split into there.
const NUM_VALIDATORS: usize = 4;

/// The tracker itself is version-agnostic; ask for the anchored message shape explicitly so these
/// tests run on stable builds too, where the feature is not enabled yet.
fn anchored_version() -> ProtocolVersion {
    ProtocolFeature::EarlyKickout.protocol_version()
}

/// Witnesses the tracker handed on to chunk validation.
type Delivered = Arc<Mutex<Vec<ChunkStateWitness>>>;

/// A tracker reading `store`, collecting whatever it hands on into [`Delivered`].
fn tracker_for(
    clock: Clock,
    store: Store,
    epoch_manager: Arc<EpochManagerHandle>,
) -> (PartialEncodedStateWitnessTracker, Delivered) {
    let delivered: Delivered = Arc::new(Mutex::new(Vec::new()));
    let sink = delivered.clone();
    let chunk_validation_sender = ChunkValidationSenderForPartialWitness {
        chunk_state_witness: Sender::from_fn(move |msg: ChunkStateWitnessMessage| {
            sink.lock().push(msg.witness);
        }),
    };
    let tracker = PartialEncodedStateWitnessTracker::new(
        clock,
        chunk_validation_sender,
        epoch_manager,
        store,
    );
    (tracker, delivered)
}

/// A tracker over a chain that produces no witnesses of its own, for the tests that only drive
/// contract state.
fn build_tracker() -> PartialEncodedStateWitnessTracker {
    let (chain, epoch_manager, _runtime, _signer) = setup(Clock::real());
    tracker_for(Clock::real(), chain.chain_store().store(), epoch_manager).0
}

fn chunk_key() -> ChunkProductionKey {
    ChunkProductionKey { shard_id: SHARD_ID, epoch_id: EpochId::default(), height_created: 42 }
}

fn code(bytes: &[u8]) -> (CodeHash, CodeBytes) {
    (CodeHash(CryptoHash::hash_bytes(bytes)), CodeBytes(bytes.to_vec().into()))
}

/// A `ContractCodeResponse` reaches every entry waiting for those code hashes, not just the
/// first one found.
///
/// The response carries no anchor, so entries are matched by the hashes they asked for, and two
/// entries under one chunk key can be waiting for the same hashes: under `EarlyKickout` the
/// producer is resolved from the grandparent anchor, so two authorized producers can name the
/// same contracts. Satisfying only one of them left the other waiting until it timed out and
/// then validating without code it needs.
#[test]
fn contract_codes_reach_every_waiting_entry() {
    let tracker = build_tracker();
    let key = chunk_key();
    let anchor_one = CryptoHash::hash_bytes(b"anchor_one");
    let anchor_two = CryptoHash::hash_bytes(b"anchor_two");
    let (hash, bytes) = code(b"contract");
    let hashes = HashSet::from([hash]);

    tracker.store_accessed_contract_hashes(key.clone(), Some(&anchor_one), hashes.clone()).unwrap();
    tracker.store_accessed_contract_hashes(key.clone(), Some(&anchor_two), hashes).unwrap();
    assert_eq!(tracker.contract_states(&key), vec!["requested", "requested"]);

    tracker.store_accessed_contract_codes(key.clone(), vec![bytes]).unwrap();

    assert_eq!(tracker.contract_states(&key), vec!["received", "received"]);
}

/// Codes that match no pending request leave every entry alone.
#[test]
fn contract_codes_nothing_asked_for_are_ignored() {
    let tracker = build_tracker();
    let key = chunk_key();
    let anchor = CryptoHash::hash_bytes(b"anchor");
    let (hash, _) = code(b"contract");
    let (_, other_bytes) = code(b"other_contract");

    tracker
        .store_accessed_contract_hashes(key.clone(), Some(&anchor), HashSet::from([hash]))
        .unwrap();
    tracker.store_accessed_contract_codes(key.clone(), vec![other_bytes]).unwrap();

    assert_eq!(tracker.contract_states(&key), vec!["requested"]);
}

/// A request whose response never arrives is given up on, so the witness can be validated
/// best-effort instead of being held until its entry is evicted.
///
/// Only an entry the request is the last thing holding up is given up on. An entry still missing
/// parts keeps waiting however long that takes: it has nothing to finalize either way, and the
/// response is routed by the hashes the entry is recorded as waiting for, so dropping the request
/// would make a response that is merely late unusable.
#[test]
fn unanswered_contract_request_is_given_up_on() {
    let signer = create_test_signer("test0");
    let (tracker, delivered, clock) = tracker_with_four_validators();

    let witness = dummy_witness(b"assembled", 1);
    let assembled_anchor = CryptoHash::hash_bytes(b"assembled_anchor");
    let parts = parts_for(&signer, assembled_anchor, &witness);
    let key = parts[0].chunk_production_key();
    let (hash, _) = code(b"contract");
    let hashes = HashSet::from([hash]);
    // Codes nothing asked for, to drive the sweep without touching either entry.
    let (_, unrelated) = code(b"unrelated_contract");

    // One entry with all its parts, held only by the outstanding request.
    tracker
        .store_accessed_contract_hashes(key.clone(), Some(&assembled_anchor), hashes.clone())
        .unwrap();
    for part in &parts {
        tracker.store_partial_encoded_state_witness(part.clone()).unwrap();
    }
    assert!(delivered.lock().is_empty(), "the witness waits while the request is outstanding");

    // And one still waiting for parts.
    let waiting_anchor = CryptoHash::hash_bytes(b"waiting_anchor");
    tracker.store_accessed_contract_hashes(key.clone(), Some(&waiting_anchor), hashes).unwrap();

    // While the requests still have time left, a message for the shard leaves both alone.
    tracker.store_accessed_contract_codes(key.clone(), vec![unrelated.clone()]).unwrap();
    assert_eq!(tracker.contract_states(&key), vec!["requested", "requested"]);
    assert!(delivered.lock().is_empty());

    // Past the timeout the next message for the shard gives up on the assembled entry and hands
    // its witness on. The one still missing parts keeps waiting.
    clock.advance(ACCESSED_CONTRACTS_REQUEST_TIMEOUT);
    tracker.store_accessed_contract_codes(key.clone(), vec![unrelated]).unwrap();

    assert_eq!(tracker.contract_states(&key), vec!["requested"]);
    assert_delivered_only(&delivered, &witness, "the assembled witness should have been handed on");
}

fn process(chain: &mut Chain, block: Arc<Block>) {
    process_block_sync(
        chain,
        block.into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();
}

fn build_on(
    clock: &FakeClock,
    chain: &Chain,
    prev: &CryptoHash,
    signer: &Arc<ValidatorSigner>,
    height: BlockHeight,
) -> Arc<Block> {
    let prev = chain.get_block(prev).unwrap();
    clock.advance(Duration::milliseconds(1));
    TestBlockBuilder::from_prev_block(clock.clock(), &prev, signer.clone()).height(height).build()
}

/// An anchor at a height our chain skipped counts as off our chain, so a witness entry anchored
/// there is the one eviction gives up first.
///
/// This is the shape the anchor collision takes: S is produced and processed, then orphaned when
/// the canonical chain skips its height.
#[test]
fn off_canonical_anchor_is_not_on_our_chain() {
    let ForkFixture { tracker, g_hash, s_hash, p_hash } = fork_fixture();

    assert!(tracker.anchor_on_our_chain(&g_hash), "G is the canonical block at its height");
    assert!(tracker.anchor_on_our_chain(&p_hash), "P is the canonical block at its height");
    assert!(!tracker.anchor_on_our_chain(&s_hash), "S sits at a height our chain skipped");
    assert!(
        tracker.anchor_on_our_chain(&CryptoHash::hash_bytes(b"never_processed")),
        "an anchor we cannot resolve is undeterminable, not off-chain"
    );
}

/// Eviction gives up an entry anchored off our chain before one anchored on it, so anchors we
/// will never build on cannot push out the entry we actually need. The cache holds only a handful
/// of entries per shard, so anchor-keying on its own would not be enough.
#[test]
fn eviction_prefers_entries_anchored_off_our_chain() {
    let ForkFixture { tracker, g_hash, s_hash, .. } = fork_fixture();
    let (hash, _) = code(b"contract");
    let key_at = |height| ChunkProductionKey {
        shard_id: SHARD_ID,
        epoch_id: EpochId::default(),
        height_created: height,
    };
    let store_entry = |height, anchor: &CryptoHash| {
        tracker
            .store_accessed_contract_hashes(
                key_at(height),
                Some(anchor),
                HashSet::from([hash.clone()]),
            )
            .unwrap()
    };

    // The entry we need is anchored on our chain and inserted first, so plain LRU would give it
    // up first. The rest of the cache is filled with entries anchored at S.
    store_entry(1, &g_hash);
    for height in 2..=5 {
        store_entry(height, &s_hash);
    }
    assert!(!tracker.contract_states(&key_at(1)).is_empty());

    // One more entry takes the cache over its cap.
    store_entry(6, &s_hash);

    assert!(
        !tracker.contract_states(&key_at(1)).is_empty(),
        "the entry anchored on our chain must survive"
    );
    assert!(
        tracker.contract_states(&key_at(2)).is_empty(),
        "the least recently used off-chain entry is the one given up"
    );
}

struct ForkFixture {
    tracker: PartialEncodedStateWitnessTracker,
    /// Canonical, the block S and P both build on.
    g_hash: CryptoHash,
    /// Processed and briefly the head, then orphaned at a height the canonical chain skips.
    s_hash: CryptoHash,
    /// Canonical head.
    p_hash: CryptoHash,
}

/// Builds the fork the anchor collision needs: G canonical, S processed then orphaned, P
/// canonical and skipping S's slot.
fn fork_fixture() -> ForkFixture {
    let clock = FakeClock::new(Utc::from_unix_timestamp(1601510400).unwrap());
    clock.advance(Duration::milliseconds(3444));
    let (mut chain, epoch_manager, _runtime, signer) = setup(clock.clock());

    // G, canonical, built on genesis.
    let genesis_hash = *chain.genesis().hash();
    let genesis_height = chain.genesis().height();
    let g = build_on(&clock, &chain, &genesis_hash, &signer, genesis_height + 1);
    let (g_hash, g_height) = (*g.hash(), g.header().height());
    process(&mut chain, g);

    // S at G.height + 1, processed and briefly the head.
    let s = build_on(&clock, &chain, &g_hash, &signer, g_height + 1);
    let s_hash = *s.hash();
    process(&mut chain, s);
    assert_eq!(chain.head().unwrap().last_block_hash, s_hash);

    // P at G.height + 2, also a child of G, skipping S's slot. P becomes the head, which
    // drops S's height from the canonical height index.
    let p = build_on(&clock, &chain, &g_hash, &signer, g_height + 2);
    let p_hash = *p.hash();
    process(&mut chain, p);
    assert_eq!(chain.head().unwrap().last_block_hash, p_hash);

    let (tracker, _delivered) =
        tracker_for(clock.clock(), chain.chain_store().store(), epoch_manager);
    ForkFixture { tracker, g_hash, s_hash, p_hash }
}

/// A tracker over an epoch with [`NUM_VALIDATORS`] validators, so a witness needs more than one
/// part and an entry holding a single part actually lingers in the cache.
fn tracker_with_four_validators() -> (PartialEncodedStateWitnessTracker, Delivered, FakeClock) {
    let clock = FakeClock::new(Utc::from_unix_timestamp(1601510400).unwrap());
    let accounts: Vec<AccountId> =
        ["test0", "test1", "test2", "test3"].iter().map(|a| a.parse().unwrap()).collect();
    let store = create_test_store();
    let mut genesis = Genesis::test_sharded(
        clock.clock(),
        accounts,
        NUM_VALIDATORS as u64,
        /* num_shards */ 1,
    );
    genesis.config.protocol_version = PROTOCOL_VERSION;
    let tempdir = tempfile::tempdir().unwrap();
    initialize_genesis_state(store.clone(), &genesis, Some(tempdir.path()));
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config, None);
    let (tracker, delivered) = tracker_for(clock.clock(), store, epoch_manager);
    (tracker, delivered, clock)
}

/// The parts a producer anchored at `anchor` would send for `witness`.
fn parts_for(
    signer: &ValidatorSigner,
    anchor: CryptoHash,
    witness: &ChunkStateWitness,
) -> Vec<VersionedPartialEncodedStateWitness> {
    let (encoded, _size) = EncodedChunkStateWitness::encode(witness).unwrap();
    let encoder = ReedSolomonEncoderCache::new(WITNESS_RATIO_DATA_PARTS).entry(NUM_VALIDATORS);
    let (parts, encoded_length) = encoder.encode(&encoded);
    let chunk_header = witness.chunk_header().clone();
    parts
        .into_iter()
        .enumerate()
        .map(|(part_ord, data)| {
            VersionedPartialEncodedStateWitness::new(
                *witness.epoch_id(),
                chunk_header.clone(),
                anchor,
                part_ord,
                data.unwrap().to_vec(),
                encoded_length,
                signer,
                anchored_version(),
            )
        })
        .collect()
}

/// A dummy witness carrying `extra_values` distinct trie values, so two witnesses for the same
/// chunk key can differ in encoded length the way two real ones would.
fn dummy_witness(marker: &[u8], extra_values: usize) -> ChunkStateWitness {
    let mut witness =
        ChunkStateWitness::new_dummy(1, SHARD_ID, CryptoHash::hash_bytes(marker), PROTOCOL_VERSION);
    let PartialState::TrieValues(values) = &mut witness.mut_main_state_transition().base_state;
    values.extend(
        (0..extra_values).map(|i| CryptoHash::hash_bytes(&[marker, &[i as u8]].concat()).0.into()),
    );
    witness
}

/// Feeds `parts` to the tracker, stopping once it has handed a witness on: past that point the
/// remaining parts belong to an entry that no longer exists.
fn feed_until_delivered(
    tracker: &PartialEncodedStateWitnessTracker,
    parts: &[VersionedPartialEncodedStateWitness],
    delivered: &Delivered,
) {
    for part in parts {
        tracker.store_partial_encoded_state_witness(part.clone()).unwrap();
        if !delivered.lock().is_empty() {
            break;
        }
    }
}

/// Asserts the tracker handed on `witness` and nothing else.
fn assert_delivered_only(delivered: &Delivered, witness: &ChunkStateWitness, msg: &str) {
    let delivered = delivered.lock();
    assert_eq!(delivered.len(), 1, "{msg}");
    assert_eq!(delivered[0].chunk_header().chunk_hash(), witness.chunk_header().chunk_hash());
}

/// A part from another anchor cannot pin our entry's `encoded_length`.
///
/// Both messages are validly signed under `EarlyKickout`, and the first part to arrive used to fix
/// the Reed-Solomon length for the whole chunk key, so every later part that disagreed was dropped
/// with no error and no eviction - one message was enough to stop the witness ever assembling.
#[test]
fn a_part_from_another_anchor_cannot_pin_our_length() {
    let signer = create_test_signer("test0");
    let (tracker, delivered, _clock) = tracker_with_four_validators();

    let ours = dummy_witness(b"ours", 1);
    let theirs = dummy_witness(b"theirs", 40);
    let their_anchor = CryptoHash::hash_bytes(b"their_anchor");
    let our_parts = parts_for(&signer, CryptoHash::hash_bytes(b"our_anchor"), &ours);
    let their_parts = parts_for(&signer, their_anchor, &theirs);
    let key = our_parts[0].chunk_production_key();
    assert_eq!(key, their_parts[0].chunk_production_key(), "same chunk key, different anchors");
    assert_ne!(
        our_parts[0].encoded_length(),
        their_parts[0].encoded_length(),
        "the fixture needs two payloads of different length to pin the entry"
    );

    // Their part lands first, in its own entry pinned to its own length.
    tracker.store_partial_encoded_state_witness(their_parts[0].clone()).unwrap();
    assert_eq!(
        tracker.witness_entries(&key),
        vec![(Some(their_anchor), Some(their_parts[0].encoded_length()))]
    );

    // Ours still assemble, against our own length.
    feed_until_delivered(&tracker, &our_parts, &delivered);

    assert_eq!(
        tracker.witness_entries(&key),
        vec![(Some(their_anchor), Some(their_parts[0].encoded_length()))],
        "ours is done and gone, theirs is untouched"
    );
    assert_delivered_only(&delivered, &ours, "our witness should have been handed on");
}

/// A failed decode does not latch the key, so later parts can still rebuild the witness.
#[test]
fn failed_decode_does_not_latch_the_key() {
    let signer = create_test_signer("test0");
    let (tracker, delivered, _clock) = tracker_with_four_validators();

    let witness = dummy_witness(b"real", 1);
    let anchor = CryptoHash::hash_bytes(b"anchor");
    let good = parts_for(&signer, anchor, &witness);

    // Enough junk parts under the same key and anchor to trigger a decode, which fails.
    let junk: Vec<_> = (0..2)
        .map(|part_ord| {
            VersionedPartialEncodedStateWitness::new(
                *witness.epoch_id(),
                test_chunk_header(
                    *witness.chunk_header().prev_block_hash(),
                    &signer,
                    anchored_version(),
                ),
                anchor,
                part_ord,
                vec![0xff; 8],
                16,
                &signer,
                anchored_version(),
            )
        })
        .collect();
    assert_eq!(junk[0].chunk_production_key(), good[0].chunk_production_key());

    for part in &junk {
        // The last one fails to decode, which surfaces as an error from the tracker.
        let _ = tracker.store_partial_encoded_state_witness(part.clone());
    }
    assert!(delivered.lock().is_empty(), "junk parts must not produce a witness");

    feed_until_delivered(&tracker, &good, &delivered);

    assert_delivered_only(
        &delivered,
        &witness,
        "the key must still accept parts after a failed decode",
    );
}
