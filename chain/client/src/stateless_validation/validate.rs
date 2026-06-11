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
use near_primitives::version::ProtocolFeature;
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
    /// V2 witness whose prev_prev anchor state is not available locally (node
    /// behind on blocks): the producer signature cannot be verified, so the
    /// message is dropped without being stored or forwarded.
    AnchorStateUnavailable,
    /// Message wire version is on the wrong side of the EarlyKickout activation
    /// boundary for its epoch. Honest peers can race the boundary, so this is a
    /// silent drop rather than an error.
    WrongWireVersion,
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
    let ChunkProductionKey { shard_id, epoch_id, height_created } =
        partial_witness.chunk_production_key();
    let _span = tracing::debug_span!(
        target: "client",
        "validate_partial_encoded_state_witness",
        height = %height_created,
        shard_id = %shard_id,
        part_ord = partial_witness.part_ord(),
        tag_witness_distribution = true,
    )
    .entered();

    let encoded_length = partial_witness.encoded_length();
    if encoded_length > MAX_COMPRESSED_STATE_WITNESS_SIZE.as_u64() as usize {
        return Err(Error::InvalidPartialChunkStateWitness(format!(
            "encoded_length {encoded_length} exceeds witness size cap {}",
            MAX_COMPRESSED_STATE_WITNESS_SIZE.as_u64()
        )));
    }

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

    // V1/V2 producer-resolution contract: see `VersionedPartialEncodedStateWitness` docstring.
    let chunk_producer = match partial_witness {
        VersionedPartialEncodedStateWitness::V1(_) => {
            epoch_manager.get_chunk_producer_info(&partial_witness.chunk_production_key())?
        }
        VersionedPartialEncodedStateWitness::V2(v2) => {
            let shard_id_label = shard_id.to_string();
            let anchor_hash = v2.prev_prev_hash();
            // Resolve the producer via the anchored lookup from the SIGNED
            // (anchor, epoch, height) — no dependency on the prev block, so the
            // signature is verifiable before anything is stored or forwarded.
            let result = epoch_manager.get_chunk_producer_info_anchored(
                anchor_hash,
                &epoch_id,
                height_created,
                shard_id,
            );
            let label = match &result {
                Ok(_) => "hit",
                // Anchor block not yet processed locally; witness will be dropped.
                Err(EpochError::MissingBlock(_)) => "miss_anchor_block",
                // Anchor known but no `DBCol::ChunkProducers` row; steady-state ~0, persistent = writer bug.
                Err(EpochError::ChunkProducerNotInDB(_, _)) => "miss_db_entry",
                Err(_) => "error",
            };
            metrics::PARTIAL_WITNESS_DB_LOOKUP_TOTAL
                .with_label_values(&[shard_id_label.as_str(), label])
                .inc();
            let info = match result {
                Ok(info) => info,
                // Behind node: the anchor state is not available, so the producer
                // signature cannot be verified. DROP — no deferral. See the
                // accepted-liveness-risk note on the drop counter metric.
                Err(EpochError::MissingBlock(_) | EpochError::ChunkProducerNotInDB(_, _)) => {
                    metrics::PARTIAL_WITNESS_ANCHOR_ABSENT_DROPS_TOTAL
                        .with_label_values(&[shard_id_label.as_str()])
                        .inc();
                    return Ok(ChunkRelevance::AnchorStateUnavailable);
                }
                Err(err) => return Err(err.into()),
            };
            // Anchor sanity checks (skipped for the default-hash anchor of
            // genesis-adjacent chunks, whose pseudo block info has no meaningful
            // height or epoch): the signed height must leave room for the prev
            // block between the anchor and the chunk, and the signed epoch must
            // be one of the two epochs reachable two blocks after the anchor.
            // Full canonicality (`prev_prev_hash` is really the prev of the
            // chunk's prev block) is checked once the prev block arrives.
            if *anchor_hash != CryptoHash::default() {
                let anchor_height = epoch_manager.get_block_info(anchor_hash)?.height();
                if height_created < anchor_height + 2 {
                    return Err(Error::InvalidPartialChunkStateWitness(format!(
                        "V2 witness signed height {} is too low for anchor height {}",
                        height_created, anchor_height,
                    )));
                }
                let state_epoch_id = epoch_manager.get_epoch_id_from_prev_block(anchor_hash)?;
                let next_epoch_id = epoch_manager.get_next_epoch_id_from_prev_block(anchor_hash)?;
                if epoch_id != state_epoch_id && epoch_id != next_epoch_id {
                    return Err(Error::InvalidPartialChunkStateWitness(format!(
                        "V2 witness signed epoch {:?} is not reachable from anchor {:?} \
                         (expected {:?} or {:?})",
                        epoch_id, anchor_hash, state_epoch_id, next_epoch_id,
                    )));
                }
            }
            info
        }
    };
    if !partial_witness.verify(chunk_producer.public_key()) {
        return Err(Error::InvalidPartialChunkStateWitness("Invalid signature".to_string()));
    }

    Ok(ChunkRelevance::Relevant)
}

/// Resolve the chunk producer for a producer-signed companion message
/// (`ChunkContractAccesses` / `PartialEncodedContractDeploys`).
///
/// The wire-version gate mirrors the witness path: epochs at/after
/// `EarlyKickout` activation require the anchored V2 shape (resolved via the
/// anchored lookup from the signed prev_prev anchor), pre-activation epochs
/// require V1 (resolved via the static sampler). A message on the wrong side
/// of the boundary, or one whose anchor state is unavailable, is irrelevant
/// (dropped), not an error.
fn resolve_companion_chunk_producer(
    epoch_manager: &dyn EpochManagerAdapter,
    key: &ChunkProductionKey,
    prev_prev_hash: Option<&CryptoHash>,
) -> Result<Result<ValidatorStake, ChunkRelevance>, Error> {
    let protocol_version = epoch_manager.get_epoch_protocol_version(&key.epoch_id)?;
    let expect_anchor = ProtocolFeature::EarlyKickout.enabled(protocol_version);
    match (expect_anchor, prev_prev_hash) {
        (true, Some(anchor_hash)) => {
            match epoch_manager.get_chunk_producer_info_anchored(
                anchor_hash,
                &key.epoch_id,
                key.height_created,
                key.shard_id,
            ) {
                Ok(info) => Ok(Ok(info)),
                Err(EpochError::MissingBlock(_) | EpochError::ChunkProducerNotInDB(_, _)) => {
                    Ok(Err(ChunkRelevance::AnchorStateUnavailable))
                }
                Err(err) => Err(err.into()),
            }
        }
        (false, None) => Ok(Ok(epoch_manager.get_chunk_producer_info(key)?)),
        (true, None) | (false, Some(_)) => Ok(Err(ChunkRelevance::WrongWireVersion)),
    }
}

pub fn validate_partial_encoded_contract_deploys(
    epoch_manager: &dyn EpochManagerAdapter,
    partial_deploys: &PartialEncodedContractDeploys,
    store: &Store,
) -> Result<ChunkRelevance, Error> {
    let key = partial_deploys.chunk_production_key();
    require_relevant!(validate_chunk_relevant(epoch_manager, key, store)?);
    let chunk_producer = match resolve_companion_chunk_producer(
        epoch_manager,
        key,
        partial_deploys.prev_prev_hash(),
    )? {
        Ok(chunk_producer) => chunk_producer,
        Err(irrelevant) => return Ok(irrelevant),
    };
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
    require_relevant!(validate_witness_contract_accesses_signature(epoch_manager, accesses)?);

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

pub fn validate_chunk_relevant_as_validator(
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
) -> Result<ChunkRelevance, Error> {
    let chunk_producer = match resolve_companion_chunk_producer(
        epoch_manager,
        accesses.chunk_production_key(),
        accesses.prev_prev_hash(),
    )? {
        Ok(chunk_producer) => chunk_producer,
        Err(irrelevant) => return Ok(irrelevant),
    };
    if !accesses.verify_signature(chunk_producer.public_key()) {
        return Err(Error::Other("Invalid witness contract accesses signature".to_owned()));
    }
    Ok(ChunkRelevance::Relevant)
}
