use crate::metrics::ANCHORED_CHUNK_PRODUCER_LOOKUP_TOTAL;
use near_chain_primitives::Error;
use near_crypto::Signature;
use near_epoch_manager::{CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET, EpochManagerAdapter};
use near_primitives::{
    block::BlockHeader,
    errors::EpochError,
    hash::CryptoHash,
    sharding::{ChunkHash, ShardChunkHeader},
    stateless_validation::ChunkProductionKey,
    types::{BlockHeight, EpochId, ShardId, validator_stake::ValidatorStake},
};
use near_store::{Store, get_genesis_height};

pub fn verify_block_vrf(
    validator: ValidatorStake,
    prev_random_value: &CryptoHash,
    vrf_value: &near_crypto::vrf::Value,
    vrf_proof: &near_crypto::vrf::Proof,
) -> Result<(), Error> {
    let public_key =
        near_crypto::key_conversion::convert_public_key(validator.public_key().unwrap_as_ed25519())
            .unwrap();

    if !public_key.is_vrf_valid(&prev_random_value.as_ref(), vrf_value, vrf_proof) {
        return Err(Error::InvalidRandomnessBeaconOutput);
    }
    Ok(())
}

/// Verify chunk header signature using the anchored chunk producer lookup.
/// Under EarlyKickout the producer is read from the ChunkProducers DB column
/// keyed by the chunk's grandparent anchor; cross-epoch and low-height chunks,
/// and the feature-off path, fall back to the canonical height sampler.
pub fn verify_chunk_header_signature_by_hash(
    epoch_manager: &dyn EpochManagerAdapter,
    chunk_header: &ShardChunkHeader,
) -> Result<bool, Error> {
    verify_chunk_header_signature_by_hash_and_parts(
        epoch_manager,
        &chunk_header.chunk_hash(),
        chunk_header.signature(),
        chunk_header.prev_block_hash(),
        chunk_header.shard_id(),
    )
}

pub fn verify_chunk_header_signature_by_hash_and_parts(
    epoch_manager: &dyn EpochManagerAdapter,
    chunk_hash: &ChunkHash,
    signature: &Signature,
    prev_block_hash: &CryptoHash,
    shard_id: ShardId,
) -> Result<bool, Error> {
    let chunk_producer =
        epoch_manager.get_chunk_producer_info_from_prev_block(prev_block_hash, shard_id)?;
    Ok(signature.verify(chunk_hash.as_ref(), chunk_producer.public_key()))
}

/// Verify a chunk header signature by resolving the producer from `epoch_id`
/// (epoch-based).
pub fn verify_chunk_header_signature_with_epoch_manager(
    epoch_manager: &dyn EpochManagerAdapter,
    chunk_header: &ShardChunkHeader,
    epoch_id: EpochId,
) -> Result<bool, Error> {
    let key = ChunkProductionKey {
        epoch_id,
        height_created: chunk_header.height_created(),
        shard_id: chunk_header.shard_id(),
    };
    let chunk_producer = epoch_manager.get_chunk_producer_info(&key)?;
    Ok(chunk_header
        .signature()
        .verify(chunk_header.chunk_hash().as_ref(), chunk_producer.public_key()))
}

pub fn verify_block_header_signature_with_epoch_manager(
    epoch_manager: &dyn EpochManagerAdapter,
    header: &BlockHeader,
) -> Result<bool, Error> {
    let block_producer =
        epoch_manager.get_block_producer_info(header.epoch_id(), header.height())?;
    Ok(header.signature().verify(header.hash().as_ref(), block_producer.public_key()))
}

fn verify_anchored_chunk_key(
    epoch_manager: &dyn EpochManagerAdapter,
    epoch_id: &EpochId,
    height_created: BlockHeight,
    prev_block_hash: &CryptoHash,
    prev_prev_block_hash: &CryptoHash,
    store: &Store,
    msg_label: &str,
) -> Result<(), Error> {
    match epoch_manager.get_block_info(prev_block_hash) {
        Ok(parent_info) => {
            let expected_epoch_id = epoch_manager.get_epoch_id_from_prev_block(prev_block_hash)?;
            let expected_height =
                parent_info.height().checked_add(1).expect("block height overflow");
            if parent_info.prev_hash() != prev_prev_block_hash
                || expected_height != height_created
                || &expected_epoch_id != epoch_id
            {
                return Err(Error::InvalidPartialChunkStateWitness(format!(
                    "{msg_label} key mismatch: signed (epoch_id={:?}, height={}, \
                     prev_prev={:?}) does not match prev_block_hash-implied \
                     (epoch_id={:?}, height={}, prev_prev={:?})",
                    epoch_id,
                    height_created,
                    prev_prev_block_hash,
                    expected_epoch_id,
                    expected_height,
                    parent_info.prev_hash(),
                )));
            }
        }
        Err(EpochError::MissingBlock(_)) => {
            if prev_prev_block_hash != &CryptoHash::default() {
                // Parent not here yet, so only the anchor is known. A chunk on a skipped slot is dropped
                // here: the anchor is only a performance optimization for the common
                // no-skip case, and the chunk is re-requested once the parent is processed.
                let anchor = epoch_manager.get_block_info(prev_prev_block_hash)?;
                let expected_height = anchor
                    .height()
                    .checked_add(CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET)
                    .expect("block height overflow");
                if height_created < expected_height {
                    return Err(Error::InvalidPartialChunkStateWitness(format!(
                        "{msg_label} height {height_created} is below anchor-implied \
                         height {expected_height}"
                    )));
                }
                // Without the parent the grandchild's epoch is not derivable, and this branch
                // binds only the height. At an epoch tail the grandchild sits in the next
                // epoch, so a sender-claimed `epoch_id` would go unchecked. Defer tail anchors
                // until the parent is known; the parent-known branch above binds the epoch.
                // `epoch_length` comes from the anchor's own (trusted) epoch, never the claimed
                // one. The `epoch_length <= 3` arm closes the false negative left by
                // `is_next_block_in_next_epoch_impl`, which drops the `+ 3` slack for tiny
                // epochs; it runs before the penultimate predicate so those anchors skip that
                // predicate's extra ancestry read.
                let epoch_length = epoch_manager.get_epoch_config(anchor.epoch_id())?.epoch_length;
                if epoch_manager.is_next_block_epoch_start(prev_prev_block_hash)?
                    || epoch_length <= 3
                    || epoch_manager
                        .is_next_block_possibly_last_in_epoch(anchor.height(), anchor.prev_hash())?
                {
                    return Err(Error::DBNotFoundErr(format!(
                        "{msg_label} anchor {prev_prev_block_hash:?} is an epoch tail; \
                         deferring until parent {prev_block_hash:?} is known"
                    )));
                }
                if height_created > expected_height {
                    return Err(Error::DBNotFoundErr(format!(
                        "{msg_label} height {height_created} does not match anchor-implied \
                         height {expected_height}; deferring until parent {prev_block_hash:?} \
                         is known"
                    )));
                }
            } else {
                // Default (genesis) anchor with no parent: nothing pins the height. A real
                // default anchor only happens at genesis or genesis + 1, so reject higher
                // to avoid an any-height hole.
                let genesis_height = get_genesis_height(store)
                    .ok_or_else(|| Error::Other("genesis height not found".to_owned()))?;
                // Overflow is impossible: `genesis_height` is a small configured constant.
                let max_height = genesis_height.checked_add(1).expect("block height overflow");
                if height_created > max_height {
                    return Err(Error::InvalidPartialChunkStateWitness(format!(
                        "{msg_label} with default anchor at height {height_created} \
                         above genesis + 1 ({max_height})"
                    )));
                }
            }
        }
        Err(err) => return Err(err.into()),
    }
    Ok(())
}

fn resolve_anchored_producer(
    epoch_manager: &dyn EpochManagerAdapter,
    prev_prev_block_hash: &CryptoHash,
    epoch_id: &EpochId,
    height_created: BlockHeight,
    shard_id: ShardId,
    message_type: &str,
) -> Result<ValidatorStake, EpochError> {
    let result = epoch_manager.get_chunk_producer_info_anchored(
        Some(prev_prev_block_hash),
        epoch_id,
        height_created,
        shard_id,
    );
    let label = match &result {
        Ok(_) => "hit",
        // Anchor block not processed yet: this node is two or more blocks behind.
        Err(EpochError::MissingBlock(_)) => "miss_anchor_block",
        // Anchor is processed but has no `DBCol::ChunkProducers` row. Should be ~0
        // normally; if it persists, something that writes that row has a bug.
        Err(EpochError::ChunkProducerNotInDB(_, _)) => "miss_db_entry",
        Err(_) => "error",
    };
    ANCHORED_CHUNK_PRODUCER_LOOKUP_TOTAL
        .with_label_values(&[shard_id.to_string().as_str(), message_type, label])
        .inc();
    result
}

pub fn resolve_and_verify_anchored_producer(
    epoch_manager: &dyn EpochManagerAdapter,
    key: &ChunkProductionKey,
    prev_block_hash: &CryptoHash,
    prev_prev_block_hash: &CryptoHash,
    store: &Store,
    msg_label: &str,
) -> Result<ValidatorStake, Error> {
    let producer = resolve_anchored_producer(
        epoch_manager,
        prev_prev_block_hash,
        &key.epoch_id,
        key.height_created,
        key.shard_id,
        msg_label,
    )?;
    verify_anchored_chunk_key(
        epoch_manager,
        &key.epoch_id,
        key.height_created,
        prev_block_hash,
        prev_prev_block_hash,
        store,
        msg_label,
    )?;
    Ok(producer)
}

#[cfg(test)]
mod tests {
    use super::{resolve_and_verify_anchored_producer, verify_anchored_chunk_key};
    use crate::test_utils::setup_with_tx_validity_period;
    use crate::{Chain, ChainStoreAccess};
    use assert_matches::assert_matches;
    use near_async::time::{Duration, FakeClock, Utc};
    use near_chain_primitives::Error;
    use near_epoch_manager::{
        CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET, EpochManagerAdapter, EpochManagerHandle,
    };
    use near_primitives::hash::CryptoHash;
    use near_primitives::test_utils::TestBlockBuilder;
    use near_primitives::types::{BlockHeight, EpochId};
    use near_store::Store;
    use std::sync::Arc;

    /// A hash that is never a processed block, so `get_block_info` returns `MissingBlock` and
    /// the parent-absent branch of `verify_anchored_chunk_key` is selected.
    fn missing_hash(tag: u8) -> CryptoHash {
        CryptoHash::hash_bytes(&[tag])
    }

    /// A processed linear chain with a controllable epoch length. There are no skipped slots,
    /// so `hashes[i]` is the anchor of the chunk at `hashes[i] + 2`, whose parent is
    /// `hashes[i + 1]`. `hashes[0]` is genesis.
    struct ChainFixture {
        chain: Chain,
        epoch_manager: Arc<EpochManagerHandle>,
        hashes: Vec<CryptoHash>,
    }

    impl ChainFixture {
        fn new(epoch_length: u64, num_blocks: usize) -> Self {
            let clock = FakeClock::new(Utc::from_unix_timestamp(1601510400).unwrap());
            // The epoch manager asserts `tx_validity_period <= epoch_length * 2`.
            let (mut chain, epoch_manager, _runtime, signer) =
                setup_with_tx_validity_period(clock.clock(), epoch_length * 2, epoch_length);
            let mut hashes = vec![*chain.genesis().hash()];
            for _ in 0..num_blocks {
                let prev_hash = *chain.head_header().unwrap().hash();
                let prev = chain.get_block(&prev_hash).unwrap();
                // `TestBlockBuilder` copies the parent's epoch ids, so set them explicitly or
                // the chain never crosses an epoch boundary.
                let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&prev_hash).unwrap();
                let next_epoch_id =
                    epoch_manager.get_next_epoch_id_from_prev_block(&prev_hash).unwrap();
                let next_bp_hash =
                    Chain::compute_bp_hash(epoch_manager.as_ref(), next_epoch_id).unwrap();
                clock.advance(Duration::milliseconds(1));
                let block = TestBlockBuilder::from_prev_block(clock.clock(), &prev, signer.clone())
                    .epoch_id(epoch_id)
                    .next_epoch_id(next_epoch_id)
                    .next_bp_hash(next_bp_hash)
                    .build();
                hashes.push(*block.hash());
                chain.process_block_test(block).unwrap();
            }
            Self { chain, epoch_manager, hashes }
        }

        fn store(&self) -> Store {
            self.chain.chain_store().store()
        }

        fn height(&self, index: usize) -> BlockHeight {
            self.epoch_manager.get_block_info(&self.hashes[index]).unwrap().height()
        }

        fn epoch_of(&self, index: usize) -> EpochId {
            *self.epoch_manager.get_block_info(&self.hashes[index]).unwrap().epoch_id()
        }

        /// The epoch the chunk anchored at `hashes[index]` really belongs to, read off the real
        /// parent. This is the oracle the parent-missing branch cannot compute for itself.
        fn chunk_epoch(&self, index: usize) -> EpochId {
            self.epoch_manager.get_epoch_id_from_prev_block(&self.hashes[index + 1]).unwrap()
        }

        /// True when the chunk anchored at `hashes[index]` lands in a later epoch than the
        /// anchor, i.e. the anchor is a genuine epoch tail.
        fn is_epoch_tail(&self, index: usize) -> bool {
            self.chunk_epoch(index) != self.epoch_of(index)
        }

        fn verify_missing_parent(
            &self,
            index: usize,
            epoch_id: &EpochId,
            height_created: BlockHeight,
        ) -> Result<(), Error> {
            verify_anchored_chunk_key(
                self.epoch_manager.as_ref(),
                epoch_id,
                height_created,
                &missing_hash(42),
                &self.hashes[index],
                &self.store(),
                "chunk",
            )
        }

        /// The honest parent-missing case: the chunk sits exactly at `anchor + 2`.
        fn verify_missing_parent_at_offset(&self, index: usize) -> Result<(), Error> {
            let height = self.height(index) + CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET;
            self.verify_missing_parent(index, &self.chunk_epoch(index), height)
        }

        /// The same chunk with its real parent processed, which is the path all
        /// consensus-adjacent validation takes.
        fn verify_known_parent_at_offset(&self, index: usize) -> Result<(), Error> {
            let height = self.height(index) + CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET;
            verify_anchored_chunk_key(
                self.epoch_manager.as_ref(),
                &self.chunk_epoch(index),
                height,
                &self.hashes[index + 1],
                &self.hashes[index],
                &self.store(),
                "chunk",
            )
        }
    }

    /// Exercises the parent-absent branch of `verify_anchored_chunk_key`: when the chunk's
    /// parent is not processed yet only the grandparent anchor is known, so the height is
    /// pinned to exactly `anchor + CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET`.
    #[test]
    fn test_anchored_key_parent_absent_pins_height_to_anchor_offset() {
        // A long epoch keeps the anchor far from any epoch tail, so the tail gate below never
        // fires and only the height bind is under test.
        let fixture = ChainFixture::new(1000, 4);
        let anchor = 3;
        assert!(!fixture.is_epoch_tail(anchor), "anchor must be mid-epoch for this test");

        let epoch_id = fixture.chunk_epoch(anchor);
        let expected_height = fixture.height(anchor) + CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET;
        let check = |height| fixture.verify_missing_parent(anchor, &epoch_id, height);

        // Exactly the anchor-implied height is the only accepted value.
        assert_matches!(check(expected_height), Ok(()));
        // A height above it is a skipped slot or a forged height we cannot disprove yet, so it
        // is deferred via `DBNotFoundErr` rather than blamed on the sender. The chunk is
        // validated against the parent once that block is processed.
        assert_matches!(check(expected_height + 1), Err(Error::DBNotFoundErr(_)));
        assert_matches!(check(expected_height + 600), Err(Error::DBNotFoundErr(_)));
        // Below it is impossible for any chunk: the parent sits strictly above the anchor, so
        // no number of skipped slots can put the chunk under `anchor + offset`. That is
        // provable without the parent, so it is a hard rejection.
        assert_matches!(check(expected_height - 1), Err(Error::InvalidPartialChunkStateWitness(_)));
    }

    /// The parent-missing branch binds only the height, so it cannot notice that the chunk
    /// crossed into the next epoch. Every anchor whose grandchild really changes epoch must be
    /// deferred, and both tail arms (last-of-epoch and penultimate) must be exercised.
    #[test]
    // TestBlockBuilder does not maintain spice's prev_last_certified_block_epoch_id
    // across epoch boundaries, so header validation rejects the boundary block.
    #[cfg_attr(feature = "protocol_feature_spice", ignore)]
    fn test_anchored_key_parent_absent_defers_epoch_tail_anchors() {
        let fixture = ChainFixture::new(5, 14);

        let (mut last_of_epoch, mut penultimate, mut mid_epoch) = (0, 0, 0);
        for anchor in 0..fixture.hashes.len() - 1 {
            let result = fixture.verify_missing_parent_at_offset(anchor);
            if !fixture.is_epoch_tail(anchor) {
                continue;
            }
            // An honest chunk at exactly `anchor + 2` used to pass the height-only bind with a
            // sender-chosen `epoch_id`. It must now be deferred instead.
            assert_matches!(
                result,
                Err(Error::DBNotFoundErr(_)),
                "epoch-tail anchor at index {anchor} must be deferred"
            );
            // The anchor is the last block of its epoch, so `is_next_block_epoch_start` is
            // definitive; otherwise the parent is, and only the conservative
            // `is_next_block_possibly_last_in_epoch` can catch it.
            if fixture.epoch_of(anchor + 1) != fixture.epoch_of(anchor) {
                last_of_epoch += 1;
            } else {
                penultimate += 1;
            }
        }
        // Mid-epoch anchors keep the optimization: a chunk at exactly `anchor + 2` resolves
        // without waiting for the parent.
        for anchor in 0..fixture.hashes.len() - 1 {
            if fixture.is_epoch_tail(anchor) {
                continue;
            }
            if fixture.verify_missing_parent_at_offset(anchor).is_ok() {
                mid_epoch += 1;
            }
        }

        assert!(last_of_epoch > 0, "test must cover a last-of-epoch anchor");
        assert!(penultimate > 0, "test must cover a penultimate anchor");
        assert!(
            mid_epoch > 0,
            "test must keep the parent-missing optimization on mid-epoch anchors"
        );
    }

    /// Regression guard for the gating: with the parent processed, boundary chunks still
    /// validate. This is the path all consensus-adjacent callers take, and it must be
    /// untouched by the tail gate.
    #[test]
    #[cfg_attr(feature = "protocol_feature_spice", ignore)]
    fn test_anchored_key_parent_known_boundary_chunks_still_validate() {
        let fixture = ChainFixture::new(5, 14);

        let mut tails = 0;
        for anchor in 0..fixture.hashes.len() - 1 {
            assert_matches!(
                fixture.verify_known_parent_at_offset(anchor),
                Ok(()),
                "parent-known chunk anchored at index {anchor} must validate"
            );
            if fixture.is_epoch_tail(anchor) {
                tails += 1;
            }
        }
        assert!(tails > 0, "test must cover epoch-tail anchors with a known parent");
    }

    /// `is_next_block_in_next_epoch_impl` drops the `+ 3` finality slack when
    /// `epoch_length <= 3`, so neither tail predicate is trustworthy there. The catch-all
    /// answers by turning the parent-missing optimization off for every non-default anchor in a
    /// tiny epoch, mid-epoch anchors included.
    #[test]
    // TestBlockBuilder does not maintain spice's prev_last_certified_block_epoch_id
    // across epoch boundaries, so header validation rejects the boundary block.
    #[cfg_attr(feature = "protocol_feature_spice", ignore)]
    fn test_anchored_key_tiny_epoch_catch_all_defers_every_anchor() {
        let fixture = ChainFixture::new(3, 12);
        let epoch_manager = fixture.epoch_manager.as_ref();

        let mut catch_all_only = 0;
        for anchor in 0..fixture.hashes.len() - 1 {
            assert_matches!(
                fixture.verify_missing_parent_at_offset(anchor),
                Err(Error::DBNotFoundErr(_)),
                "tiny-epoch anchor at index {anchor} must be deferred"
            );
            let info = epoch_manager.get_block_info(&fixture.hashes[anchor]).unwrap();
            let definitive =
                epoch_manager.is_next_block_epoch_start(&fixture.hashes[anchor]).unwrap();
            let conservative = epoch_manager
                .is_next_block_possibly_last_in_epoch(info.height(), info.prev_hash())
                .unwrap();
            if !definitive && !conservative {
                catch_all_only += 1;
            }
        }
        assert!(catch_all_only > 0, "test must cover anchors only the catch-all defers");
    }

    /// A real genesis block hash as the anchor is an epoch tail by definition, so it is
    /// deferred. The `CryptoHash::default()` sentinel keeps its own genesis handling.
    #[test]
    fn test_anchored_key_genesis_anchors() {
        let fixture = ChainFixture::new(1000, 3);

        // `is_next_block_epoch_start(genesis)` is true, so a chunk at genesis + 2 with an
        // unprocessed parent waits for that parent.
        assert!(fixture.epoch_manager.is_next_block_epoch_start(&fixture.hashes[0]).unwrap());
        assert_matches!(
            fixture.verify_missing_parent_at_offset(0),
            Err(Error::DBNotFoundErr(_)),
            "genesis-anchored chunk with a missing parent must be deferred"
        );

        // The default-anchor sentinel means the chunk has no grandparent at all. Nothing pins
        // the height there, so the branch only bounds it by genesis + 1.
        let genesis_height = fixture.height(0);
        let epoch_id = fixture.epoch_of(0);
        let check = |height| {
            verify_anchored_chunk_key(
                fixture.epoch_manager.as_ref(),
                &epoch_id,
                height,
                &missing_hash(42),
                &CryptoHash::default(),
                &fixture.store(),
                "chunk",
            )
        };
        assert_matches!(check(genesis_height), Ok(()));
        assert_matches!(check(genesis_height + 1), Ok(()));
        assert_matches!(check(genesis_height + 2), Err(Error::InvalidPartialChunkStateWitness(_)));
    }

    #[cfg(feature = "nightly")]
    mod nightly {
        use super::{ChainFixture, missing_hash};
        use crate::signature_verification::verify_anchored_chunk_key;
        use assert_matches::assert_matches;
        use near_chain_primitives::Error;
        use near_epoch_manager::{CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET, EpochManagerAdapter};
        use near_primitives::epoch_block_info::BlockInfo;
        use near_primitives::hash::CryptoHash;
        use near_primitives::stateless_validation::ChunkProductionKey;
        use near_primitives::stateless_validation::chunk_endorsements_bitmap::ChunkEndorsementsBitmap;
        use near_primitives::types::ShardId;
        use near_primitives::utils::get_block_shard_id;
        use near_primitives::version::PROTOCOL_VERSION;
        use near_store::DBCol;

        /// Under EarlyKickout the producer is resolved from the *sender-claimed* epoch before
        /// the key is verified. At an epoch tail the anchor still carries a seeded row for its
        /// own (old) epoch, so a forged `epoch_id` would authenticate a message whose real
        /// epoch is the next one. The tail gate must drop it before that happens.
        #[test]
        // TestBlockBuilder does not maintain spice's prev_last_certified_block_epoch_id
        // across epoch boundaries, so header validation rejects the boundary block.
        #[cfg_attr(feature = "protocol_feature_spice", ignore)]
        fn test_forged_epoch_at_tail_anchor_is_dropped_before_authenticating() {
            let fixture = ChainFixture::new(5, 14);
            let epoch_manager = fixture.epoch_manager.as_ref();

            let (mut last_of_epoch, mut penultimate) = (0, 0);
            for anchor in 0..fixture.hashes.len() - 1 {
                if !fixture.is_epoch_tail(anchor) {
                    continue;
                }
                let anchor_hash = fixture.hashes[anchor];
                // The attacker claims the anchor's own epoch. The chunk really belongs to the
                // next one, which is what makes the claim a forgery.
                let forged_epoch = fixture.epoch_of(anchor);
                assert_ne!(forged_epoch, fixture.chunk_epoch(anchor));
                let height = fixture.height(anchor) + CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET;
                let shard_id = epoch_manager
                    .get_shard_layout(&forged_epoch)
                    .unwrap()
                    .shard_ids()
                    .next()
                    .unwrap();

                // The lookup the resolver runs first still succeeds: the seeded row is there,
                // so without the tail gate the caller would receive an authenticated producer.
                epoch_manager
                    .get_chunk_producer_info_anchored(
                        Some(&anchor_hash),
                        &forged_epoch,
                        height,
                        shard_id,
                    )
                    .expect("anchor must carry a seeded row for its own epoch");

                let key =
                    ChunkProductionKey { epoch_id: forged_epoch, height_created: height, shard_id };
                let result = resolve(&fixture, &key, &anchor_hash);
                assert_matches!(
                    result,
                    Err(Error::DBNotFoundErr(_)),
                    "forged-epoch message anchored at index {anchor} must be dropped"
                );

                if fixture.epoch_of(anchor + 1) != fixture.epoch_of(anchor) {
                    last_of_epoch += 1;
                } else {
                    penultimate += 1;
                }
            }
            assert!(last_of_epoch > 0, "test must cover a last-of-epoch anchor");
            assert!(penultimate > 0, "test must cover a penultimate anchor");
        }

        fn resolve(
            fixture: &ChainFixture,
            key: &ChunkProductionKey,
            anchor_hash: &CryptoHash,
        ) -> Result<near_primitives::types::validator_stake::ValidatorStake, Error> {
            super::resolve_and_verify_anchored_producer(
                fixture.epoch_manager.as_ref(),
                key,
                &missing_hash(42),
                anchor_hash,
                &fixture.store(),
                "witness",
            )
        }

        /// The penultimate arm reads the anchor's parent. After garbage collection or a state
        /// sync that parent can be gone; the read must surface as a quiet deferral, never a
        /// hard error that could get the sender banned.
        #[test]
        fn test_missing_anchor_ancestry_defers_quietly() {
            let fixture = ChainFixture::new(1000, 3);
            let epoch_manager = fixture.epoch_manager.as_ref();

            // A mid-epoch anchor whose own parent was never processed. Copying a real
            // `BlockInfo` keeps the epoch fields valid, so resolution gets all the way to the
            // ancestry read in the penultimate arm.
            let real = epoch_manager.get_block_info(&fixture.hashes[3]).unwrap();
            let orphan_hash = missing_hash(7);
            let mut orphan = BlockInfo::new(
                orphan_hash,
                real.height(),
                real.last_finalized_height(),
                *real.last_final_block_hash(),
                missing_hash(8),
                vec![],
                real.chunk_mask().to_vec(),
                *real.total_supply(),
                PROTOCOL_VERSION,
                *real.latest_protocol_version(),
                *real.timestamp_nanosec(),
                ChunkEndorsementsBitmap::new(0),
                None,
            );
            *orphan.epoch_id_mut() = *real.epoch_id();
            *orphan.epoch_first_block_mut() = *real.epoch_first_block();

            let shard_id: ShardId = epoch_manager
                .get_shard_layout(real.epoch_id())
                .unwrap()
                .shard_ids()
                .next()
                .unwrap();
            let producer = epoch_manager
                .get_chunk_producer_info_anchored(
                    Some(&fixture.hashes[3]),
                    real.epoch_id(),
                    real.height() + CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET,
                    shard_id,
                )
                .unwrap();

            let mut update = fixture.store().store_update();
            update.insert_ser(DBCol::BlockInfo, orphan_hash.as_ref(), &orphan);
            update.insert_ser(
                DBCol::ChunkProducers,
                &get_block_shard_id(&orphan_hash, shard_id),
                &producer,
            );
            update.commit();

            let key = ChunkProductionKey {
                epoch_id: *real.epoch_id(),
                height_created: real.height() + CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET,
                shard_id,
            };
            assert_matches!(
                resolve(&fixture, &key, &orphan_hash),
                Err(Error::DBNotFoundErr(_)),
                "a missing anchor ancestor must defer, not raise a hard error"
            );
            // The same input through the raw key check, to pin that the deferral comes from the
            // ancestry read and not from the resolver's own lookup.
            assert_matches!(
                verify_anchored_chunk_key(
                    epoch_manager,
                    real.epoch_id(),
                    real.height() + CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET,
                    &missing_hash(42),
                    &orphan_hash,
                    &fixture.store(),
                    "chunk",
                ),
                Err(Error::DBNotFoundErr(_))
            );
        }
    }
}
