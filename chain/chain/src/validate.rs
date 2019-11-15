use borsh::{BorshDeserialize, BorshSerialize};

use near_primitives::block::{Block, BlockHeader};
use near_primitives::challenge::{
    BlockDoubleSign, Challenge, ChallengeBody, ChallengesResult, ChunkProofs, ChunkState,
};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::merkle::merklize;
use near_primitives::sharding::{ChunkHash, ShardChunk, ShardChunkHeader};
use near_primitives::types::{AccountId, ChunkExtra, EpochId};
use near_store::PartialStorage;

use crate::byzantine_assert;
use crate::types::{ApplyTransactionResult, ValidatorSignatureVerificationResult};
use crate::{ChainStore, Error, ErrorKind, RuntimeAdapter};

/// Gas limit cannot be adjusted for more than 0.1% at a time.
const GAS_LIMIT_ADJUSTMENT_FACTOR: u64 = 1000;

/// Verifies that chunk's proofs in the header match the body.
pub fn validate_chunk_proofs(
    chunk: &ShardChunk,
    runtime_adapter: &dyn RuntimeAdapter,
) -> Result<bool, Error> {
    // 1. Checking chunk.header.hash
    if chunk.header.hash != ChunkHash(hash(&chunk.header.inner.try_to_vec()?)) {
        byzantine_assert!(false);
        return Ok(false);
    }

    // 2. Checking that chunk body is valid
    // 2a. Checking chunk hash
    if chunk.chunk_hash != chunk.header.hash {
        byzantine_assert!(false);
        return Ok(false);
    }
    // 2b. Checking that chunk transactions are valid
    let (tx_root, _) = merklize(&chunk.transactions);
    if tx_root != chunk.header.inner.tx_root {
        byzantine_assert!(false);
        return Ok(false);
    }
    // 2c. Checking that chunk receipts are valid
    if chunk.header.inner.height_created == 0 {
        return Ok(chunk.receipts.len() == 0
            && chunk.header.inner.outgoing_receipts_root == CryptoHash::default());
    } else {
        let outgoing_receipts_hashes = runtime_adapter.build_receipts_hashes(&chunk.receipts)?;
        let (receipts_root, _) = merklize(&outgoing_receipts_hashes);
        if receipts_root != chunk.header.inner.outgoing_receipts_root {
            byzantine_assert!(false);
            return Ok(false);
        }
    }
    Ok(true)
}

/// Validate that all next chunk information matches previous chunk extra.
pub fn validate_chunk_with_chunk_extra(
    chain_store: &mut ChainStore,
    runtime_adapter: &dyn RuntimeAdapter,
    prev_block_hash: &CryptoHash,
    prev_chunk_extra: &ChunkExtra,
    prev_chunk_header: &ShardChunkHeader,
    chunk_header: &ShardChunkHeader,
) -> Result<(), Error> {
    if prev_chunk_extra.state_root != chunk_header.inner.prev_state_root {
        return Err(ErrorKind::InvalidStateRoot.into());
    }

    if prev_chunk_extra.outcome_root != chunk_header.inner.outcome_root {
        return Err(ErrorKind::InvalidOutcomesProof.into());
    }

    if prev_chunk_extra.validator_proposals != chunk_header.inner.validator_proposals {
        return Err(ErrorKind::InvalidValidatorProposals.into());
    }

    if prev_chunk_extra.gas_limit != chunk_header.inner.gas_limit {
        return Err(ErrorKind::InvalidGasLimit.into());
    }

    if prev_chunk_extra.gas_used != chunk_header.inner.gas_used {
        return Err(ErrorKind::InvalidGasUsed.into());
    }

    if prev_chunk_extra.rent_paid != chunk_header.inner.rent_paid {
        return Err(ErrorKind::InvalidRent.into());
    }

    if prev_chunk_extra.validator_reward != chunk_header.inner.validator_reward {
        return Err(ErrorKind::InvalidReward.into());
    }

    if prev_chunk_extra.balance_burnt != chunk_header.inner.balance_burnt {
        return Err(ErrorKind::InvalidBalanceBurnt.into());
    }

    let receipt_response = chain_store.get_outgoing_receipts_for_shard(
        *prev_block_hash,
        chunk_header.inner.shard_id,
        prev_chunk_header.height_included,
    )?;
    let outgoing_receipts_hashes = runtime_adapter.build_receipts_hashes(&receipt_response.1)?;
    let (outgoing_receipts_root, _) = merklize(&outgoing_receipts_hashes);

    if outgoing_receipts_root != chunk_header.inner.outgoing_receipts_root {
        return Err(ErrorKind::InvalidReceiptsProof.into());
    }

    let prev_gas_limit = prev_chunk_extra.gas_limit;
    if chunk_header.inner.gas_limit < prev_gas_limit - prev_gas_limit / GAS_LIMIT_ADJUSTMENT_FACTOR
        || chunk_header.inner.gas_limit
            > prev_gas_limit + prev_gas_limit / GAS_LIMIT_ADJUSTMENT_FACTOR
    {
        return Err(ErrorKind::InvalidGasLimit.into());
    }

    Ok(())
}

/// Validates a double sign challenge.
/// Only valid if ancestors of both blocks are present in the chain.
fn validate_double_sign(
    runtime_adapter: &dyn RuntimeAdapter,
    block_double_sign: &BlockDoubleSign,
) -> Result<(CryptoHash, Vec<AccountId>), Error> {
    let left_block_header = BlockHeader::try_from_slice(&block_double_sign.left_block_header)?;
    let right_block_header = BlockHeader::try_from_slice(&block_double_sign.right_block_header)?;
    let block_producer = runtime_adapter
        .get_block_producer(&left_block_header.inner.epoch_id, left_block_header.inner.height)?;
    if left_block_header.hash() != right_block_header.hash()
        && left_block_header.inner.height == right_block_header.inner.height
        && runtime_adapter
            .verify_validator_signature(
                &left_block_header.inner.epoch_id,
                &left_block_header.inner.prev_hash,
                &block_producer,
                left_block_header.hash().as_ref(),
                &left_block_header.signature,
            )
            .valid()
        && runtime_adapter
            .verify_validator_signature(
                &right_block_header.inner.epoch_id,
                &right_block_header.inner.prev_hash,
                &block_producer,
                right_block_header.hash().as_ref(),
                &right_block_header.signature,
            )
            .valid()
    {
        // Deterministically return header with higher hash.
        Ok(if left_block_header.hash() > right_block_header.hash() {
            (left_block_header.hash(), vec![block_producer])
        } else {
            (right_block_header.hash(), vec![block_producer])
        })
    } else {
        Err(ErrorKind::MaliciousChallenge.into())
    }
}

fn validate_header_authorship(
    runtime_adapter: &dyn RuntimeAdapter,
    block_header: &BlockHeader,
) -> Result<(), Error> {
    match runtime_adapter.verify_header_signature(block_header) {
        ValidatorSignatureVerificationResult::Valid => Ok(()),
        ValidatorSignatureVerificationResult::Invalid => Err(ErrorKind::InvalidChallenge.into()),
        ValidatorSignatureVerificationResult::UnknownEpoch => {
            Err(ErrorKind::EpochOutOfBounds.into())
        }
    }
}

fn validate_chunk_authorship(
    runtime_adapter: &dyn RuntimeAdapter,
    chunk_header: &ShardChunkHeader,
) -> Result<AccountId, Error> {
    match runtime_adapter.verify_chunk_header_signature(chunk_header) {
        Ok(true) => {
            let epoch_id = runtime_adapter
                .get_epoch_id_from_prev_block(&chunk_header.inner.prev_block_hash)?;
            let chunk_producer = runtime_adapter.get_chunk_producer(
                &epoch_id,
                chunk_header.inner.height_created,
                chunk_header.inner.shard_id,
            )?;
            Ok(chunk_producer)
        }
        Ok(false) => return Err(ErrorKind::InvalidChallenge.into()),
        Err(e) => Err(e),
    }
}

fn validate_chunk_proofs_challenge(
    runtime_adapter: &dyn RuntimeAdapter,
    chunk_proofs: &ChunkProofs,
) -> Result<(CryptoHash, Vec<AccountId>), Error> {
    let block_header = BlockHeader::try_from_slice(&chunk_proofs.block_header)?;
    validate_header_authorship(runtime_adapter, &block_header)?;
    let chunk_producer = validate_chunk_authorship(runtime_adapter, &chunk_proofs.chunk.header)?;
    if !Block::validate_chunk_header_proof(
        &chunk_proofs.chunk.header,
        &block_header.inner.chunk_headers_root,
        &chunk_proofs.merkle_proof,
    ) {
        return Err(ErrorKind::MaliciousChallenge.into());
    }
    match chunk_proofs
        .chunk
        .decode_chunk(
            runtime_adapter.num_data_parts(&chunk_proofs.chunk.header.inner.prev_block_hash),
        )
        .map_err(|err| err.into())
        .and_then(|chunk| validate_chunk_proofs(&chunk, &*runtime_adapter))
    {
        Ok(true) => Err(ErrorKind::MaliciousChallenge.into()),
        Ok(false) | Err(_) => Ok((block_header.hash(), vec![chunk_producer])),
    }
}

fn validate_chunk_state_challenge(
    runtime_adapter: &dyn RuntimeAdapter,
    chunk_state: &ChunkState,
) -> Result<(CryptoHash, Vec<AccountId>), Error> {
    let prev_block_header = BlockHeader::try_from_slice(&chunk_state.prev_block_header)?;
    let block_header = BlockHeader::try_from_slice(&chunk_state.block_header)?;

    // Validate previous chunk and block header.
    validate_header_authorship(runtime_adapter, &prev_block_header)?;
    let _ = validate_chunk_authorship(runtime_adapter, &chunk_state.prev_chunk.header)?;
    if !Block::validate_chunk_header_proof(
        &chunk_state.prev_chunk.header,
        &prev_block_header.inner.chunk_headers_root,
        &chunk_state.prev_merkle_proof,
    ) {
        return Err(ErrorKind::MaliciousChallenge.into());
    }

    // Validate current chunk and block header.
    validate_header_authorship(runtime_adapter, &block_header)?;
    let chunk_producer = validate_chunk_authorship(runtime_adapter, &chunk_state.chunk_header)?;
    if !Block::validate_chunk_header_proof(
        &chunk_state.chunk_header,
        &block_header.inner.chunk_headers_root,
        &chunk_state.merkle_proof,
    ) {
        return Err(ErrorKind::MaliciousChallenge.into());
    }

    // Apply state transition and check that the result state and other data doesn't match.
    let partial_storage = PartialStorage { nodes: chunk_state.partial_state.clone() };
    let result = runtime_adapter
        .check_state_transition(
            partial_storage,
            chunk_state.prev_chunk.header.inner.shard_id,
            &chunk_state.prev_chunk.header.inner.prev_state_root,
            block_header.inner.height,
            block_header.inner.timestamp,
            &block_header.inner.prev_hash,
            &block_header.hash(),
            &chunk_state.prev_chunk.receipts,
            &chunk_state.prev_chunk.transactions,
            &[],
            prev_block_header.inner.gas_price,
            chunk_state.prev_chunk.header.inner.gas_limit,
            &ChallengesResult::default(),
        )
        .map_err(|_| Error::from(ErrorKind::MaliciousChallenge))?;
    let outcome_root = ApplyTransactionResult::compute_outcomes_proof(&result.outcomes).0;
    if result.new_root != chunk_state.chunk_header.inner.prev_state_root
        || outcome_root != chunk_state.chunk_header.inner.outcome_root
        || result.validator_proposals != chunk_state.chunk_header.inner.validator_proposals
        || result.total_gas_burnt != chunk_state.chunk_header.inner.gas_used
        || result.total_rent_paid != chunk_state.chunk_header.inner.rent_paid
    {
        Ok((block_header.hash(), vec![chunk_producer]))
    } else {
        // If all the data matches, this is actually valid chunk and challenge is malicious.
        Err(ErrorKind::MaliciousChallenge.into())
    }
}

/// Returns Some(block hash, vec![account_id]) of invalid block and who to slash if challenge is correct and None if incorrect.
pub fn validate_challenge(
    runtime_adapter: &dyn RuntimeAdapter,
    epoch_id: &EpochId,
    last_block_hash: &CryptoHash,
    challenge: &Challenge,
) -> Result<(CryptoHash, Vec<AccountId>), Error> {
    // Check signature is correct on the challenge.
    if !runtime_adapter
        .verify_validator_signature(
            epoch_id,
            last_block_hash,
            &challenge.account_id,
            challenge.hash.as_ref(),
            &challenge.signature,
        )
        .valid()
    {
        return Err(ErrorKind::InvalidChallenge.into());
    }
    match &challenge.body {
        ChallengeBody::BlockDoubleSign(block_double_sign) => {
            validate_double_sign(runtime_adapter, block_double_sign)
        }
        ChallengeBody::ChunkProofs(chunk_proofs) => {
            validate_chunk_proofs_challenge(runtime_adapter, chunk_proofs)
        }
        ChallengeBody::ChunkState(chunk_state) => {
            validate_chunk_state_challenge(runtime_adapter, chunk_state)
        }
    }
}
