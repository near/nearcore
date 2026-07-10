use super::partial_witness::witness_part_length;
use crate::metrics;
use itertools::Itertools;
use near_chain::types::Tip;
use near_chain_primitives::Error;
use near_epoch_manager::{CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET, EpochManagerAdapter};
use near_primitives::errors::EpochError;
use near_primitives::hash::CryptoHash;
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::stateless_validation::chunk_endorsement::ChunkEndorsement;
use near_primitives::stateless_validation::contract_distribution::{
    ChunkContractAccesses, ContractCodeRequest, ContractCodeResponse, PartialEncodedContractDeploys,
};
use near_primitives::stateless_validation::partial_witness::{
    MAX_COMPRESSED_STATE_WITNESS_SIZE, VersionedPartialEncodedStateWitness,
};
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{AccountId, BlockHeight, BlockHeightDelta, EpochId, ShardId};
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::version::ProtocolFeature;
use near_store::{DBCol, FINAL_HEAD_KEY, HEAD_KEY, Store, get_genesis_height};
use strum::IntoStaticStr;

/// This is taken to be the same value as near_chunks::chunk_cache::MAX_HEIGHTS_AHEAD, and we
/// reject partial witnesses with height more than this value above the height of our current HEAD
const MAX_HEIGHTS_AHEAD: BlockHeightDelta = 5;

/// This enum represents whether a particular chunk is relevant in the context of validating
/// a chunk endorsement or a partial witness.
#[derive(Debug, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum ChunkRelevance {
    Relevant,
    /// Chunk is irrelevant because it's height is less or equal to the current final head.
    TooLate,
    /// Chunk is irrelevant because it's height is more than `MAX_HEIGHTS_AHEAD`
    /// from the current head.
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
        VersionedPartialEncodedStateWitness::V2(v2) => resolve_and_verify_anchored_producer(
            epoch_manager,
            &partial_witness.chunk_production_key(),
            v2.prev_block_hash(),
            v2.prev_prev_block_hash(),
            store,
            "witness",
        )?,
    };
    if !partial_witness.verify(chunk_producer.public_key()) {
        return Err(Error::InvalidPartialChunkStateWitness("Invalid signature".to_string()));
    }

    Ok(ChunkRelevance::Relevant)
}

/// Cross-check the signed (epoch, height) against the signed anchor before trusting the
/// producer resolved from it. The anchored lookup is keyed by anchor and shard only, not
/// height, so without this a producer could sign under any height and we would store or
/// forward under a forged key.
///
/// Parent known: must match the parent exactly. Parent absent, real anchor: height must be
/// anchor + 2. Parent absent, default anchor: only valid at genesis or genesis + 1.
/// `msg_label` names the caller in the error.
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
            if parent_info.prev_hash() != prev_prev_block_hash
                || parent_info.height() + 1 != height_created
                || &expected_epoch_id != epoch_id
            {
                return Err(Error::InvalidPartialChunkStateWitness(format!(
                    "V2 {msg_label} chunk key mismatch: signed (epoch_id={:?}, height={}, \
                     prev_prev={:?}) does not match prev_block_hash-implied \
                     (epoch_id={:?}, height={}, prev_prev={:?})",
                    epoch_id,
                    height_created,
                    prev_prev_block_hash,
                    expected_epoch_id,
                    parent_info.height() + 1,
                    parent_info.prev_hash(),
                )));
            }
        }
        Err(EpochError::MissingBlock(_)) => {
            if prev_prev_block_hash != &CryptoHash::default() {
                // Parent not here yet, so only the anchor is known. Requiring height ==
                // anchor + 2 ties one anchor to one chunk key per shard, so a producer
                // cannot reuse it across many cache slots. (Cross-epoch anchors fall back
                // to the resolver.) A skipped slot with a missing parent is dropped here;
                // that is fine, best-effort.
                let anchor_height = epoch_manager.get_block_info(prev_prev_block_hash)?.height();
                let expected_height = anchor_height + CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET;
                if height_created != expected_height {
                    return Err(Error::InvalidPartialChunkStateWitness(format!(
                        "V2 {msg_label} height {height_created} does not match \
                         anchor-implied height {expected_height}"
                    )));
                }
            } else {
                // Default (genesis) anchor with no parent: nothing pins the height. A real
                // default anchor only happens at genesis or genesis + 1, so reject higher
                // to avoid an any-height hole.
                let genesis_height = get_genesis_height(store)
                    .ok_or_else(|| Error::Other("genesis height not found".to_owned()))?;
                if height_created > genesis_height + 1 {
                    return Err(Error::InvalidPartialChunkStateWitness(format!(
                        "V2 {msg_label} with default anchor at height {height_created} \
                         above genesis + 1 ({})",
                        genesis_height + 1
                    )));
                }
            }
        }
        Err(err) => return Err(err.into()),
    }
    Ok(())
}

/// Look up the producer from the anchor and record the lookup-outcome metric (hit, anchor
/// block missing, or `DBCol::ChunkProducers` row missing). Error returned unchanged;
/// callers turn the node-behind cases (`MissingBlock`, `ChunkProducerNotInDB`) into a
/// quiet drop.
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
    metrics::ANCHORED_CHUNK_PRODUCER_LOOKUP_TOTAL
        .with_label_values(&[shard_id.to_string().as_str(), message_type, label])
        .inc();
    result
}

/// Resolve a V2 message's producer from the grandparent anchor, then cross-check the signed
/// chunk key against the anchor. Shared by the witness, contract-accesses and contract-deploys
/// V2 paths; `msg_label` names the caller in the metric and errors. Resolve runs before the
/// cross-check so the lookup metric is recorded even when the key check later rejects.
fn resolve_and_verify_anchored_producer(
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

pub fn validate_partial_encoded_contract_deploys(
    epoch_manager: &dyn EpochManagerAdapter,
    partial_deploys: &PartialEncodedContractDeploys,
    store: &Store,
) -> Result<ChunkRelevance, Error> {
    let key = partial_deploys.chunk_production_key();
    require_relevant!(validate_chunk_relevant(epoch_manager, key, store)?);
    // V1 uses the sampler; V2 looks the producer up from the anchor (like the witness
    // path) and cross-checks the key first. An unresolvable anchor returns `DBNotFoundErr`,
    // dropped quietly by the caller (this node is behind).
    let chunk_producer = match partial_deploys {
        PartialEncodedContractDeploys::V1(_) => epoch_manager.get_chunk_producer_info(key)?,
        PartialEncodedContractDeploys::V2(v2) => resolve_and_verify_anchored_producer(
            epoch_manager,
            key,
            v2.prev_block_hash(),
            v2.prev_prev_block_hash(),
            store,
            "contract deploys",
        )?,
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
    validate_witness_contract_accesses_signature(epoch_manager, accesses, store)?;

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

pub fn validate_contract_code_response(
    epoch_manager: &dyn EpochManagerAdapter,
    response: &ContractCodeResponse,
    store: &Store,
) -> Result<ChunkRelevance, Error> {
    let key = response.chunk_production_key();
    require_relevant!(validate_chunk_relevant(epoch_manager, key, store)?);
    let protocol_version = epoch_manager.get_epoch_protocol_version(&key.epoch_id)?;
    if ProtocolFeature::SignedContractCodeResponse.enabled(protocol_version) {
        validate_witness_contract_code_response_signature(epoch_manager, response)?;
    }

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
/// - `Ok(ChunkRelevance::Relevant)` if ChunkProductionKey is valid and we should process it.
/// - `Ok(ChunkRelevance::*)` (a non-relevant variant) if ChunkProductionKey is potentially valid,
///   but at this point we should not process it. One example of that is if the witness is too old.
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
    store: &Store,
) -> Result<(), Error> {
    let key = accesses.chunk_production_key();
    // V1 uses the sampler; V2 looks the producer up from the anchor (like the witness
    // path) and cross-checks the key first. An unresolvable anchor returns `DBNotFoundErr`,
    // dropped quietly by the caller (this node is behind).
    let chunk_producer = match accesses {
        ChunkContractAccesses::V1(_) => epoch_manager.get_chunk_producer_info(key)?,
        ChunkContractAccesses::V2(v2) => resolve_and_verify_anchored_producer(
            epoch_manager,
            key,
            v2.prev_block_hash(),
            v2.prev_prev_block_hash(),
            store,
            "contract accesses",
        )?,
    };
    if !accesses.verify_signature(chunk_producer.public_key()) {
        return Err(Error::Other("Invalid witness contract accesses signature".to_owned()));
    }
    Ok(())
}

fn validate_witness_contract_code_response_signature(
    epoch_manager: &dyn EpochManagerAdapter,
    response: &ContractCodeResponse,
) -> Result<(), Error> {
    let Some(responder) = response.responder() else {
        return Err(Error::Other(
            "Unsigned contract code response in epoch where signature is required".to_owned(),
        ));
    };
    let key = response.chunk_production_key();
    let chunk_producers =
        epoch_manager.get_epoch_chunk_producers_for_shard(&key.epoch_id, key.shard_id)?;
    if !chunk_producers.contains(responder) {
        return Err(Error::Other(format!(
            "Contract code response responder {responder} is not a chunk producer for shard {} in epoch {:?}",
            key.shard_id, key.epoch_id,
        )));
    }
    let validator = epoch_manager.get_validator_by_account_id(&key.epoch_id, responder)?;
    if !response.verify_signature(validator.public_key()) {
        return Err(Error::Other("Invalid witness contract code response signature".to_owned()));
    }
    Ok(())
}
