use super::partial_witness::witness_part_length;
use crate::metrics;
use itertools::Itertools;
use near_chain::types::Tip;
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::errors::EpochError;
use near_primitives::hash::CryptoHash;
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::stateless_validation::chunk_endorsement::ChunkEndorsement;
use near_primitives::stateless_validation::contract_distribution::{
    ChunkContractAccesses, ContractCodeRequest, PartialEncodedContractDeploys,
};
use near_primitives::stateless_validation::partial_witness::{
    MAX_COMPRESSED_STATE_WITNESS_SIZE, VersionedPartialEncodedStateWitness,
};
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{AccountId, BlockHeightDelta};
use near_primitives::validator_signer::ValidatorSigner;
use near_store::{DBCol, FINAL_HEAD_KEY, HEAD_KEY, Store};
use strum::IntoStaticStr;

/// This is taken to be the same value as near_chunks::chunk_cache::MAX_HEIGHTS_AHEAD, and we
/// reject partial witnesses with height more than this value above the height of our current HEAD
const MAX_HEIGHTS_AHEAD: BlockHeightDelta = 5;

/// This enum represents whether a particular chunk is relevant in the context of validating
/// a chunk endorsement or a partial witness.
#[derive(IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum ChunkRelevance {
    Relevant,
    /// Chunk is irrelevant because it's height is less or equal to the current final head.
    TooLate,
    /// Chunk is irrelevant because it's height is more than `MAX_HEIGHTS_AHEAD`
    /// from the current final head.
    TooEarly,
    /// Chunk is irrelevant because it's impossible to establish the ID of the epoch
    /// to which it should belong.
    UnknownEpochId,
}

impl ChunkRelevance {
    /// Return `true` iff chunk is relevant.
    pub fn is_relevant(&self) -> bool {
        match self {
            ChunkRelevance::Relevant => true,
            _ => false,
        }
    }
}

macro_rules! require_relevant {
    ($expr:expr) => {
        match $expr {
            ChunkRelevance::Relevant => {}
            irrelevant => return Ok(irrelevant),
        }
    };
}

/// Narrow subset of `EpochManagerAdapter` used by
/// `resolve_chunk_producer_for_witness`. Extracted so the resolution logic can
/// be unit-tested without mocking the full adapter.
pub(crate) trait ChunkProducerLookup {
    fn cpk_producer(&self, cpk: &ChunkProductionKey) -> Result<ValidatorStake, EpochError>;
    fn db_producer(
        &self,
        prev_block_hash: &CryptoHash,
        shard_id: near_primitives::types::ShardId,
    ) -> Result<ValidatorStake, EpochError>;
    fn epoch_id_from_prev_block(
        &self,
        prev_block_hash: &CryptoHash,
    ) -> Result<near_primitives::types::EpochId, EpochError>;
    fn prev_block_height(
        &self,
        prev_block_hash: &CryptoHash,
    ) -> Result<near_primitives::types::BlockHeight, EpochError>;
}

impl<T: EpochManagerAdapter + ?Sized> ChunkProducerLookup for T {
    fn cpk_producer(&self, cpk: &ChunkProductionKey) -> Result<ValidatorStake, EpochError> {
        self.get_chunk_producer_info(cpk)
    }
    fn db_producer(
        &self,
        prev_block_hash: &CryptoHash,
        shard_id: near_primitives::types::ShardId,
    ) -> Result<ValidatorStake, EpochError> {
        self.get_chunk_producer_info_db(prev_block_hash, shard_id)
    }
    fn epoch_id_from_prev_block(
        &self,
        prev_block_hash: &CryptoHash,
    ) -> Result<near_primitives::types::EpochId, EpochError> {
        self.get_epoch_id_from_prev_block(prev_block_hash)
    }
    fn prev_block_height(
        &self,
        prev_block_hash: &CryptoHash,
    ) -> Result<near_primitives::types::BlockHeight, EpochError> {
        Ok(self.get_block_info(prev_block_hash)?.height())
    }
}

/// Resolves the chunk producer used to verify a witness signature.
///
/// For V2 witnesses (with `prev_block_hash`), we prefer the hash-based DB
/// lookup and cross-check that the witness's `cpk.epoch_id` and
/// `cpk.height_created` agree with what the parent block implies. The
/// signature only binds to `prev_block_hash` and the inner fields, not to the
/// epoch/height being consistent with each other — without the cross-check, a
/// legitimate producer at any `(prev_block_hash, shard)` could sign for an
/// arbitrary slot and burn downstream CPU before endorsement rejects.
///
/// For V1 witnesses (no `prev_block_hash`), or V2 witnesses whose parent
/// block hasn't been processed locally (`MissingBlock`), we fall back to the
/// CPK-based lookup. This is safe while blacklist writes to
/// `DBCol::ChunkProducers` are gated off (pre-PR 6) — the CPK lookup computes
/// the same producer the DB would have returned. `ChunkProducerNotInDB`
/// remains a hard error (data integrity).
pub(crate) fn resolve_chunk_producer_for_witness<L: ChunkProducerLookup + ?Sized>(
    lookup: &L,
    cpk: &ChunkProductionKey,
    prev_block_hash: Option<&CryptoHash>,
) -> Result<ValidatorStake, Error> {
    let Some(prev_block_hash) = prev_block_hash else {
        return Ok(lookup.cpk_producer(cpk)?);
    };
    match lookup.db_producer(prev_block_hash, cpk.shard_id) {
        Ok(info) => {
            check_cpk_matches_prev_block(lookup, cpk, prev_block_hash)?;
            Ok(info)
        }
        Err(EpochError::MissingBlock(_)) => {
            metrics::PARTIAL_WITNESS_FALLBACK_MISSING_BLOCK.inc();
            Ok(lookup.cpk_producer(cpk)?)
        }
        Err(err) => Err(err.into()),
    }
}

// TODO: this cross-check becomes unnecessary once epoch_id/height_created are
// removed from V2's inner (they'd be derived from prev_block_hash, not carried).
fn check_cpk_matches_prev_block<L: ChunkProducerLookup + ?Sized>(
    lookup: &L,
    cpk: &ChunkProductionKey,
    prev_block_hash: &CryptoHash,
) -> Result<(), Error> {
    let expected_epoch_id = lookup.epoch_id_from_prev_block(prev_block_hash)?;
    if cpk.epoch_id != expected_epoch_id {
        return Err(Error::InvalidPartialChunkStateWitness(format!(
            "cpk epoch_id {:?} does not match epoch implied by prev_block_hash {:?} (expected {:?})",
            cpk.epoch_id, prev_block_hash, expected_epoch_id
        )));
    }
    let expected_height = lookup.prev_block_height(prev_block_hash)? + 1;
    if cpk.height_created != expected_height {
        return Err(Error::InvalidPartialChunkStateWitness(format!(
            "cpk height_created {} does not match prev_block.height + 1 = {} for prev_block_hash {:?}",
            cpk.height_created, expected_height, prev_block_hash
        )));
    }
    Ok(())
}

/// Function to validate the partial encoded state witness. In addition of ChunkProductionKey, we check the following:
/// - part_ord is valid and within range of the number of expected parts for this chunk
/// - partial_witness signature is valid and from the expected chunk_producer
/// TODO(stateless_validation): Include checks from handle_orphan_witness in chunk_validation_actor.rs
/// These include checks based on epoch_id validity, witness size, height_created, distance from chain head, etc.
pub fn validate_partial_encoded_state_witness(
    epoch_manager: &dyn EpochManagerAdapter,
    partial_witness: &VersionedPartialEncodedStateWitness,
    validator_account_id: &AccountId,
    store: &Store,
) -> Result<ChunkRelevance, Error> {
    let cpk = partial_witness.chunk_production_key();
    let ChunkProductionKey { shard_id, epoch_id, height_created } = cpk;
    let _span = tracing::debug_span!(
        target: "client",
        "validate_partial_encoded_state_witness",
        height = %height_created,
        shard_id = %shard_id,
        part_ord = partial_witness.part_ord(),
        tag_witness_distribution = true,
    )
    .entered();
    let num_parts =
        epoch_manager.get_chunk_validator_assignments(&epoch_id, shard_id, height_created)?.len();
    if partial_witness.part_ord() >= num_parts {
        return Err(Error::InvalidPartialChunkStateWitness(format!(
            "Invalid part_ord in PartialEncodedStateWitness: {}",
            partial_witness.part_ord()
        )));
    }

    let max_part_len =
        witness_part_length(MAX_COMPRESSED_STATE_WITNESS_SIZE.as_u64() as usize, num_parts);
    if partial_witness.part_size() > max_part_len {
        return Err(Error::InvalidPartialChunkStateWitness(format!(
            "Part size {} exceed limit of {} (total parts: {})",
            partial_witness.part_size(),
            max_part_len,
            num_parts
        )));
    }

    require_relevant!(validate_chunk_relevant_as_validator(
        epoch_manager,
        &partial_witness.chunk_production_key(),
        validator_account_id,
        store,
    )?);

    let chunk_producer =
        resolve_chunk_producer_for_witness(epoch_manager, &cpk, partial_witness.prev_block_hash())?;
    if !partial_witness.verify(chunk_producer.public_key()) {
        return Err(Error::InvalidPartialChunkStateWitness("Invalid signature".to_string()));
    }

    Ok(ChunkRelevance::Relevant)
}

pub fn validate_partial_encoded_contract_deploys(
    epoch_manager: &dyn EpochManagerAdapter,
    partial_deploys: &PartialEncodedContractDeploys,
    store: &Store,
) -> Result<ChunkRelevance, Error> {
    let key = partial_deploys.chunk_production_key();
    require_relevant!(validate_chunk_relevant(epoch_manager, key, store)?);
    let chunk_producer = epoch_manager.get_chunk_producer_info(key)?;
    if !partial_deploys.verify_signature(chunk_producer.public_key()) {
        return Err(Error::Other("Invalid contract deploys signature".to_owned()));
    }
    Ok(ChunkRelevance::Relevant)
}

/// Function to validate the chunk endorsement. In addition of ChunkProductionKey, we check the following:
/// - signature of endorsement and metadata is valid
pub fn validate_chunk_endorsement(
    epoch_manager: &dyn EpochManagerAdapter,
    endorsement: &ChunkEndorsement,
    store: &Store,
) -> Result<ChunkRelevance, Error> {
    let _span = tracing::debug_span!(
        target: "stateless_validation",
        "validate_chunk_endorsement",
        height = endorsement.chunk_production_key().height_created,
        shard_id = %endorsement.chunk_production_key().shard_id,
        validator = %endorsement.account_id(),
        tag_block_production = true
    )
    .entered();

    require_relevant!(validate_chunk_relevant_as_validator(
        epoch_manager,
        &endorsement.chunk_production_key(),
        endorsement.account_id(),
        store,
    )?);
    validate_chunk_endorsement_signature(epoch_manager, endorsement)?;

    Ok(ChunkRelevance::Relevant)
}

pub fn validate_chunk_contract_accesses(
    epoch_manager: &dyn EpochManagerAdapter,
    accesses: &ChunkContractAccesses,
    signer: &ValidatorSigner,
    store: &Store,
) -> Result<ChunkRelevance, Error> {
    let key = accesses.chunk_production_key();
    require_relevant!(validate_chunk_relevant_as_validator(
        epoch_manager,
        key,
        signer.validator_id(),
        store
    )?);
    validate_witness_contract_accesses_signature(epoch_manager, accesses)?;

    Ok(ChunkRelevance::Relevant)
}

pub fn validate_contract_code_request(
    epoch_manager: &dyn EpochManagerAdapter,
    request: &ContractCodeRequest,
    store: &Store,
) -> Result<ChunkRelevance, Error> {
    let key = request.chunk_production_key();
    require_relevant!(validate_chunk_relevant_as_validator(
        epoch_manager,
        key,
        request.requester(),
        store
    )?);
    validate_witness_contract_code_request_signature(epoch_manager, request)?;

    Ok(ChunkRelevance::Relevant)
}

fn validate_chunk_relevant_as_validator(
    epoch_manager: &dyn EpochManagerAdapter,
    chunk: &ChunkProductionKey,
    validator_account_id: &AccountId,
    store: &Store,
) -> Result<ChunkRelevance, Error> {
    require_relevant!(validate_chunk_relevant(epoch_manager, chunk, store)?);
    ensure_chunk_validator(epoch_manager, chunk, validator_account_id)?;
    Ok(ChunkRelevance::Relevant)
}

fn ensure_chunk_validator(
    epoch_manager: &dyn EpochManagerAdapter,
    chunk: &ChunkProductionKey,
    account_id: &AccountId,
) -> Result<(), Error> {
    let chunk_validator_assignments = epoch_manager.get_chunk_validator_assignments(
        &chunk.epoch_id,
        chunk.shard_id,
        chunk.height_created,
    )?;
    if !chunk_validator_assignments.contains(account_id) {
        return Err(Error::NotAChunkValidator);
    }
    Ok(())
}

/// Function to validate ChunkProductionKey. We check the following:
/// - shard_id is valid
/// - height_created is in (last_final_height..chain_head_height + MAX_HEIGHTS_AHEAD] range
/// - epoch_id is within epoch_manager's possible_epochs_of_height_around_tip
/// Returns:
/// - Ok(true) if ChunkProductionKey is valid and we should process it.
/// - Ok(false) if ChunkProductionKey is potentially valid, but at this point we should not
///   process it. One example of that is if the witness is too old.
/// - Err if ChunkProductionKey is invalid which most probably indicates malicious behavior.
fn validate_chunk_relevant(
    epoch_manager: &dyn EpochManagerAdapter,
    chunk_production_key: &ChunkProductionKey,
    store: &Store,
) -> Result<ChunkRelevance, Error> {
    let shard_id = chunk_production_key.shard_id;
    let epoch_id = chunk_production_key.epoch_id;
    let height_created = chunk_production_key.height_created;

    if !epoch_manager.get_shard_layout(&epoch_id)?.shard_ids().contains(&shard_id) {
        tracing::error!(
            target: "stateless_validation",
            ?chunk_production_key,
            "shard id is not in the shard layout of the epoch"
        );
        return Err(Error::InvalidShardId(shard_id));
    }

    // TODO(https://github.com/near/nearcore/issues/11301): replace these direct DB accesses with messages
    // sent to the client actor. for a draft, see https://github.com/near/nearcore/commit/e186dc7c0b467294034c60758fe555c78a31ef2d
    let head = store.get_ser::<Tip>(DBCol::BlockMisc, HEAD_KEY);
    let final_head = store.get_ser::<Tip>(DBCol::BlockMisc, FINAL_HEAD_KEY);

    // Avoid processing state witness for old chunks.
    // In particular it is impossible for a chunk created at a height
    // that doesn't exceed the height of the current final block to be
    // included in the chain. This addresses both network-delayed messages
    // as well as malicious behavior of a chunk producer.
    if let Some(final_head) = final_head {
        if height_created <= final_head.height {
            tracing::debug!(
                target: "stateless_validation",
                ?chunk_production_key,
                final_head_height = final_head.height,
                "skipping because height created is not greater than final head height",
            );
            return Ok(ChunkRelevance::TooLate);
        }
    }
    if let Some(head) = head {
        if height_created > head.height + MAX_HEIGHTS_AHEAD {
            tracing::debug!(
                target: "stateless_validation",
                ?chunk_production_key,
                head_height = head.height,
                %MAX_HEIGHTS_AHEAD,
                "skipping because height created is more than max heights ahead blocks ahead of head height"
            );
            return Ok(ChunkRelevance::TooEarly);
        }

        // Try to find the EpochId to which this witness will belong based on its height.
        // It's not always possible to determine the exact epoch_id because the exact
        // starting height of the next epoch isn't known until it actually starts,
        // so things can get unclear around epoch boundaries.
        // Let's collect the epoch_ids in which the witness might possibly be.
        let possible_epochs =
            epoch_manager.possible_epochs_of_height_around_tip(&head, height_created)?;
        if !possible_epochs.contains(&epoch_id) {
            tracing::debug!(
                target: "stateless_validation",
                ?chunk_production_key,
                ?possible_epochs,
                "skipping because epoch id is not in the possible list of epochs"
            );
            return Ok(ChunkRelevance::UnknownEpochId);
        }
    }

    Ok(ChunkRelevance::Relevant)
}

fn validate_chunk_endorsement_signature(
    epoch_manager: &dyn EpochManagerAdapter,
    endorsement: &ChunkEndorsement,
) -> Result<(), Error> {
    let validator = epoch_manager.get_validator_by_account_id(
        &endorsement.chunk_production_key().epoch_id,
        &endorsement.account_id(),
    )?;
    if !endorsement.verify(validator.public_key()) {
        return Err(Error::InvalidChunkEndorsement);
    }
    Ok(())
}

fn validate_witness_contract_code_request_signature(
    epoch_manager: &dyn EpochManagerAdapter,
    request: &ContractCodeRequest,
) -> Result<(), Error> {
    let validator = epoch_manager.get_validator_by_account_id(
        &request.chunk_production_key().epoch_id,
        &request.requester(),
    )?;
    if !request.verify_signature(validator.public_key()) {
        return Err(Error::Other("Invalid witness contract code request signature".to_owned()));
    }
    Ok(())
}

fn validate_witness_contract_accesses_signature(
    epoch_manager: &dyn EpochManagerAdapter,
    accesses: &ChunkContractAccesses,
) -> Result<(), Error> {
    let chunk_producer = epoch_manager.get_chunk_producer_info(accesses.chunk_production_key())?;
    if !accesses.verify_signature(chunk_producer.public_key()) {
        return Err(Error::Other("Invalid witness contract accesses signature".to_owned()));
    }
    Ok(())
}

#[cfg(test)]
mod resolve_chunk_producer_tests {
    use super::*;
    use near_primitives::hash::CryptoHash;
    use near_primitives::types::{BlockHeight, EpochId, ShardId};
    use std::cell::Cell;

    /// Test stub that answers each method from a pre-seeded `Result`. Methods
    /// default to `panic!()` so an unexpected call fails loudly rather than
    /// silently returning wrong data.
    #[derive(Default)]
    struct StubLookup {
        cpk_answer: Option<Result<ValidatorStake, EpochError>>,
        db_answer: Option<Result<ValidatorStake, EpochError>>,
        epoch_id_answer: Option<Result<EpochId, EpochError>>,
        prev_height_answer: Option<Result<BlockHeight, EpochError>>,
        cpk_calls: Cell<u32>,
        db_calls: Cell<u32>,
    }

    impl ChunkProducerLookup for StubLookup {
        fn cpk_producer(&self, _cpk: &ChunkProductionKey) -> Result<ValidatorStake, EpochError> {
            self.cpk_calls.set(self.cpk_calls.get() + 1);
            self.cpk_answer.clone().expect("cpk_producer called but no answer seeded")
        }
        fn db_producer(
            &self,
            _prev_block_hash: &CryptoHash,
            _shard_id: ShardId,
        ) -> Result<ValidatorStake, EpochError> {
            self.db_calls.set(self.db_calls.get() + 1);
            self.db_answer.clone().expect("db_producer called but no answer seeded")
        }
        fn epoch_id_from_prev_block(
            &self,
            _prev_block_hash: &CryptoHash,
        ) -> Result<EpochId, EpochError> {
            self.epoch_id_answer
                .clone()
                .expect("epoch_id_from_prev_block called but no answer seeded")
        }
        fn prev_block_height(
            &self,
            _prev_block_hash: &CryptoHash,
        ) -> Result<BlockHeight, EpochError> {
            self.prev_height_answer.clone().expect("prev_block_height called but no answer seeded")
        }
    }

    fn cpk(epoch_id: EpochId, height_created: BlockHeight) -> ChunkProductionKey {
        ChunkProductionKey { shard_id: ShardId::new(0), epoch_id, height_created }
    }

    fn producer(name: &str) -> ValidatorStake {
        ValidatorStake::test(name.parse().unwrap())
    }

    /// V1 witness (no prev_block_hash) hits the CPK-based lookup only, never
    /// consults the DB or the cross-check.
    #[test]
    fn v1_uses_cpk_lookup_only() {
        let stub = StubLookup { cpk_answer: Some(Ok(producer("cpk.near"))), ..Default::default() };
        let key = cpk(EpochId(CryptoHash::hash_bytes(b"epoch")), 100);
        let got = resolve_chunk_producer_for_witness(&stub, &key, None).unwrap();
        assert_eq!(got.account_id().as_str(), "cpk.near");
        assert_eq!(stub.cpk_calls.get(), 1);
        assert_eq!(stub.db_calls.get(), 0);
    }

    /// V2 with a live parent block → DB lookup wins and the cross-check
    /// passes when cpk agrees with what the parent block implies.
    #[test]
    fn v2_db_hit_uses_db_lookup_and_cross_check_passes() {
        let epoch = EpochId(CryptoHash::hash_bytes(b"epoch"));
        let stub = StubLookup {
            cpk_answer: Some(Ok(producer("cpk.near"))),
            db_answer: Some(Ok(producer("db.near"))),
            epoch_id_answer: Some(Ok(epoch)),
            prev_height_answer: Some(Ok(99)),
            ..Default::default()
        };
        let key = cpk(epoch, 100);
        let prev = CryptoHash::hash_bytes(b"prev");
        let got = resolve_chunk_producer_for_witness(&stub, &key, Some(&prev)).unwrap();
        assert_eq!(got.account_id().as_str(), "db.near");
        assert_eq!(stub.cpk_calls.get(), 0);
        assert_eq!(stub.db_calls.get(), 1);
    }

    /// V2 with DB hit but cpk.epoch_id disagrees with the parent's epoch →
    /// hard error (Issue 1.1 — widened forgery surface closed).
    #[test]
    fn v2_db_hit_but_cpk_epoch_mismatch_rejects() {
        let stub = StubLookup {
            db_answer: Some(Ok(producer("db.near"))),
            epoch_id_answer: Some(Ok(EpochId(CryptoHash::hash_bytes(b"epoch-real")))),
            prev_height_answer: Some(Ok(99)),
            ..Default::default()
        };
        let key = cpk(EpochId(CryptoHash::hash_bytes(b"epoch-forged")), 100);
        let prev = CryptoHash::hash_bytes(b"prev");
        let err = resolve_chunk_producer_for_witness(&stub, &key, Some(&prev)).unwrap_err();
        assert!(
            matches!(err, Error::InvalidPartialChunkStateWitness(ref s) if s.contains("epoch_id")),
            "expected epoch_id mismatch error, got {err:?}"
        );
    }

    /// V2 with DB hit but cpk.height_created != prev.height + 1 → hard error.
    #[test]
    fn v2_db_hit_but_cpk_height_mismatch_rejects() {
        let epoch = EpochId(CryptoHash::hash_bytes(b"epoch"));
        let stub = StubLookup {
            db_answer: Some(Ok(producer("db.near"))),
            epoch_id_answer: Some(Ok(epoch)),
            prev_height_answer: Some(Ok(99)),
            ..Default::default()
        };
        // cpk.height_created should be 100 (99 + 1) but we send 200.
        let key = cpk(epoch, 200);
        let prev = CryptoHash::hash_bytes(b"prev");
        let err = resolve_chunk_producer_for_witness(&stub, &key, Some(&prev)).unwrap_err();
        assert!(
            matches!(err, Error::InvalidPartialChunkStateWitness(ref s) if s.contains("height_created")),
            "expected height_created mismatch error, got {err:?}"
        );
    }

    /// V2 whose parent block hasn't been processed locally → DB returns
    /// `MissingBlock` → fall back to CPK lookup, increment fallback metric.
    #[test]
    fn v2_missing_block_falls_back_to_cpk() {
        let before = metrics::PARTIAL_WITNESS_FALLBACK_MISSING_BLOCK.get();
        let stub = StubLookup {
            cpk_answer: Some(Ok(producer("cpk.near"))),
            db_answer: Some(Err(EpochError::MissingBlock(CryptoHash::hash_bytes(b"prev")))),
            ..Default::default()
        };
        let key = cpk(EpochId(CryptoHash::hash_bytes(b"epoch")), 100);
        let prev = CryptoHash::hash_bytes(b"prev");
        let got = resolve_chunk_producer_for_witness(&stub, &key, Some(&prev)).unwrap();
        assert_eq!(got.account_id().as_str(), "cpk.near");
        assert_eq!(stub.cpk_calls.get(), 1);
        assert_eq!(stub.db_calls.get(), 1);
        assert_eq!(metrics::PARTIAL_WITNESS_FALLBACK_MISSING_BLOCK.get(), before + 1);
    }

    /// V2 with a processed parent but the DB column has no entry →
    /// `ChunkProducerNotInDB` → data integrity error, hard fail.
    #[test]
    fn v2_chunk_producer_not_in_db_rejects() {
        let stub = StubLookup {
            db_answer: Some(Err(EpochError::ChunkProducerNotInDB(
                CryptoHash::hash_bytes(b"prev"),
                ShardId::new(0),
            ))),
            ..Default::default()
        };
        let key = cpk(EpochId(CryptoHash::hash_bytes(b"epoch")), 100);
        let prev = CryptoHash::hash_bytes(b"prev");
        let err = resolve_chunk_producer_for_witness(&stub, &key, Some(&prev)).unwrap_err();
        assert!(
            matches!(err, Error::DBNotFoundErr(ref s) if s.contains("chunk producer")),
            "expected DBNotFoundErr from ChunkProducerNotInDB conversion, got {err:?}"
        );
        assert_eq!(stub.cpk_calls.get(), 0);
    }
}
