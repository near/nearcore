use std::collections::HashMap;

use borsh::BorshDeserialize;

use near_crypto::PublicKey;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block::{Block, BlockHeader};
use near_primitives::challenge::{
    BlockDoubleSign, Challenge, ChallengeBody, ChunkProofs, ChunkState, MaybeEncodedShardChunk,
};
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::merklize;
use near_primitives::sharding::{
    ShardChunk, ShardChunkHeader, ShardChunkHeaderV1, ShardChunkHeaderV2, ShardChunkHeaderV3,
};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{AccountId, BlockHeight, EpochId, Nonce};

use crate::types::RuntimeAdapter;
use crate::{byzantine_assert, Chain};
use crate::{ChainStore, Error};

/// Gas limit cannot be adjusted for more than 0.1% at a time.
const GAS_LIMIT_ADJUSTMENT_FACTOR: u64 = 1000;

/// Verifies that chunk's proofs in the header match the body.
pub fn validate_chunk_proofs(
    chunk: &ShardChunk,
    epoch_manager: &dyn EpochManagerAdapter,
) -> Result<bool, Error> {
    let correct_chunk_hash = match chunk {
        ShardChunk::V1(chunk) => ShardChunkHeaderV1::compute_hash(&chunk.header.inner),
        ShardChunk::V2(chunk) => match &chunk.header {
            ShardChunkHeader::V1(header) => ShardChunkHeaderV1::compute_hash(&header.inner),
            ShardChunkHeader::V2(header) => ShardChunkHeaderV2::compute_hash(&header.inner),
            ShardChunkHeader::V3(header) => ShardChunkHeaderV3::compute_hash(&header.inner),
        },
    };

    let header_hash = match chunk {
        ShardChunk::V1(chunk) => chunk.header.chunk_hash(),
        ShardChunk::V2(chunk) => chunk.header.chunk_hash(),
    };

    // 1. Checking chunk.header.hash
    if header_hash != correct_chunk_hash {
        byzantine_assert!(false);
        return Ok(false);
    }

    // 2. Checking that chunk body is valid
    // 2a. Checking chunk hash
    if chunk.chunk_hash() != correct_chunk_hash {
        byzantine_assert!(false);
        return Ok(false);
    }
    let height_created = chunk.height_created();
    let outgoing_receipts_root = chunk.outgoing_receipts_root();
    let (transactions, receipts) = (chunk.transactions(), chunk.receipts());

    // 2b. Checking that chunk transactions are valid
    let (tx_root, _) = merklize(transactions);
    if tx_root != chunk.tx_root() {
        byzantine_assert!(false);
        return Ok(false);
    }
    // 2c. Checking that chunk receipts are valid
    if height_created == 0 {
        return Ok(receipts.is_empty() && outgoing_receipts_root == CryptoHash::default());
    } else {
        let shard_layout = {
            let prev_block_hash = match chunk {
                ShardChunk::V1(chunk) => &chunk.header.inner.prev_block_hash,
                ShardChunk::V2(chunk) => chunk.header.prev_block_hash(),
            };
            epoch_manager.get_shard_layout_from_prev_block(prev_block_hash)?
        };
        let outgoing_receipts_hashes = Chain::build_receipts_hashes(receipts, &shard_layout);
        let (receipts_root, _) = merklize(&outgoing_receipts_hashes);
        if receipts_root != outgoing_receipts_root {
            byzantine_assert!(false);
            return Ok(false);
        }
    }
    Ok(true)
}

/// Validates that the given transactions are in proper valid order.
/// See <https://nomicon.io/ChainSpec/Transactions.html#transaction-ordering>
pub fn validate_transactions_order(transactions: &[SignedTransaction]) -> bool {
    let mut nonces: HashMap<(&AccountId, &PublicKey), Nonce> = HashMap::new();
    let mut batches: HashMap<(&AccountId, &PublicKey), usize> = HashMap::new();
    let mut current_batch = 1;

    for tx in transactions {
        let key = (&tx.transaction.signer_id, &tx.transaction.public_key);

        // Verifying nonce
        let nonce = tx.transaction.nonce;
        if let Some(last_nonce) = nonces.get(&key) {
            if nonce <= *last_nonce {
                // Nonces should increase.
                return false;
            }
        }
        nonces.insert(key, nonce);

        // Verifying batch
        let last_batch = *batches.get(&key).unwrap_or(&0);
        if last_batch == current_batch {
            current_batch += 1;
        } else if last_batch < current_batch - 1 {
            // The key was skipped in the previous batch
            return false;
        }
        batches.insert(key, current_batch);
    }
    true
}

/// Validate that all next chunk information matches previous chunk extra.
pub fn validate_chunk_with_chunk_extra(
    chain_store: &ChainStore,
    epoch_manager: &dyn EpochManagerAdapter,
    prev_block_hash: &CryptoHash,
    prev_chunk_extra: &ChunkExtra,
    prev_chunk_height_included: BlockHeight,
    chunk_header: &ShardChunkHeader,
) -> Result<(), Error> {
    if *prev_chunk_extra.state_root() != chunk_header.prev_state_root() {
        return Err(Error::InvalidStateRoot);
    }

    if *prev_chunk_extra.outcome_root() != chunk_header.outcome_root() {
        return Err(Error::InvalidOutcomesProof);
    }

    let chunk_extra_proposals = prev_chunk_extra.validator_proposals();
    let chunk_header_proposals = chunk_header.validator_proposals();
    if chunk_header_proposals.len() != chunk_extra_proposals.len()
        || !chunk_extra_proposals.eq(chunk_header_proposals)
    {
        return Err(Error::InvalidValidatorProposals);
    }

    if prev_chunk_extra.gas_limit() != chunk_header.gas_limit() {
        return Err(Error::InvalidGasLimit);
    }

    if prev_chunk_extra.gas_used() != chunk_header.gas_used() {
        return Err(Error::InvalidGasUsed);
    }

    if prev_chunk_extra.balance_burnt() != chunk_header.balance_burnt() {
        return Err(Error::InvalidBalanceBurnt);
    }

    let outgoing_receipts = chain_store.get_outgoing_receipts_for_shard(
        epoch_manager,
        *prev_block_hash,
        chunk_header.shard_id(),
        prev_chunk_height_included,
    )?;
    let outgoing_receipts_hashes = {
        let shard_layout = epoch_manager.get_shard_layout_from_prev_block(prev_block_hash)?;
        Chain::build_receipts_hashes(&outgoing_receipts, &shard_layout)
    };
    let (outgoing_receipts_root, _) = merklize(&outgoing_receipts_hashes);

    if outgoing_receipts_root != chunk_header.outgoing_receipts_root() {
        return Err(Error::InvalidReceiptsProof);
    }

    let prev_gas_limit = prev_chunk_extra.gas_limit();
    if chunk_header.gas_limit() < prev_gas_limit - prev_gas_limit / GAS_LIMIT_ADJUSTMENT_FACTOR
        || chunk_header.gas_limit() > prev_gas_limit + prev_gas_limit / GAS_LIMIT_ADJUSTMENT_FACTOR
    {
        return Err(Error::InvalidGasLimit);
    }

    Ok(())
}

/// Validates a double sign challenge.
/// Only valid if ancestors of both blocks are present in the chain.
fn validate_double_sign(
    epoch_manager: &dyn EpochManagerAdapter,
    block_double_sign: &BlockDoubleSign,
) -> Result<(CryptoHash, Vec<AccountId>), Error> {
    let left_block_header = BlockHeader::try_from_slice(&block_double_sign.left_block_header)?;
    let right_block_header = BlockHeader::try_from_slice(&block_double_sign.right_block_header)?;
    let block_producer = epoch_manager
        .get_block_producer(left_block_header.epoch_id(), left_block_header.height())?;
    if left_block_header.hash() != right_block_header.hash()
        && left_block_header.height() == right_block_header.height()
        && epoch_manager.verify_validator_signature(
            left_block_header.epoch_id(),
            left_block_header.prev_hash(),
            &block_producer,
            left_block_header.hash().as_ref(),
            left_block_header.signature(),
        )?
        && epoch_manager.verify_validator_signature(
            right_block_header.epoch_id(),
            right_block_header.prev_hash(),
            &block_producer,
            right_block_header.hash().as_ref(),
            right_block_header.signature(),
        )?
    {
        // Deterministically return header with higher hash.
        Ok(if left_block_header.hash() > right_block_header.hash() {
            (*left_block_header.hash(), vec![block_producer])
        } else {
            (*right_block_header.hash(), vec![block_producer])
        })
    } else {
        Err(Error::MaliciousChallenge)
    }
}

fn validate_header_authorship(
    epoch_manager: &dyn EpochManagerAdapter,
    block_header: &BlockHeader,
) -> Result<(), Error> {
    if epoch_manager.verify_header_signature(block_header)? {
        Ok(())
    } else {
        Err(Error::InvalidChallenge)
    }
}

fn validate_chunk_authorship(
    epoch_manager: &dyn EpochManagerAdapter,
    chunk_header: &ShardChunkHeader,
) -> Result<AccountId, Error> {
    let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&chunk_header.prev_block_hash())?;
    if epoch_manager.verify_chunk_header_signature(
        chunk_header,
        &epoch_id,
        &chunk_header.prev_block_hash(),
    )? {
        let chunk_producer = epoch_manager.get_chunk_producer(
            &epoch_id,
            chunk_header.height_created(),
            chunk_header.shard_id(),
        )?;
        Ok(chunk_producer)
    } else {
        Err(Error::InvalidChallenge)
    }
}

fn validate_chunk_proofs_challenge(
    epoch_manager: &dyn EpochManagerAdapter,
    chunk_proofs: &ChunkProofs,
) -> Result<(CryptoHash, Vec<AccountId>), Error> {
    let block_header = BlockHeader::try_from_slice(&chunk_proofs.block_header)?;
    validate_header_authorship(epoch_manager, &block_header)?;
    let chunk_header = match &chunk_proofs.chunk {
        MaybeEncodedShardChunk::Encoded(encoded_chunk) => encoded_chunk.cloned_header(),
        MaybeEncodedShardChunk::Decoded(chunk) => chunk.cloned_header(),
    };
    let chunk_producer = validate_chunk_authorship(epoch_manager, &chunk_header)?;
    let account_to_slash_for_valid_challenge = Ok((*block_header.hash(), vec![chunk_producer]));
    if !Block::validate_chunk_header_proof(
        &chunk_header,
        block_header.chunk_headers_root(),
        &chunk_proofs.merkle_proof,
    ) {
        // Merkle proof is invalid. It's a malicious challenge.
        return Err(Error::MaliciousChallenge);
    }
    // Temporary holds the decoded chunk, since we use a reference below to avoid cloning it.
    let tmp_chunk;
    let chunk_ref = match &chunk_proofs.chunk {
        MaybeEncodedShardChunk::Encoded(encoded_chunk) => {
            match encoded_chunk.decode_chunk(epoch_manager.num_data_parts()) {
                Ok(chunk) => {
                    tmp_chunk = Some(chunk);
                    tmp_chunk.as_ref().unwrap()
                }
                Err(_) => {
                    // Chunk can't be decoded. Good challenge.
                    return account_to_slash_for_valid_challenge;
                }
            }
        }
        MaybeEncodedShardChunk::Decoded(chunk) => chunk,
    };

    if !validate_chunk_proofs(chunk_ref, epoch_manager)? {
        // Chunk proofs are invalid. Good challenge.
        return account_to_slash_for_valid_challenge;
    }

    if !validate_transactions_order(chunk_ref.transactions()) {
        // Chunk transactions are invalid. Good challenge.
        return account_to_slash_for_valid_challenge;
    }

    // The chunk is fine. It's a malicious challenge.
    return Err(Error::MaliciousChallenge);
}

fn validate_chunk_state_challenge(
    _runtime: &dyn RuntimeAdapter,
    _chunk_state: &ChunkState,
) -> Result<(CryptoHash, Vec<AccountId>), Error> {
    // TODO (#2445): Enable challenges when they are working correctly.
    // let prev_block_header = BlockHeader::try_from_slice(&chunk_state.prev_block_header)?;
    // let block_header = BlockHeader::try_from_slice(&chunk_state.block_header)?;

    // // Validate previous chunk and block header.
    // validate_header_authorship(runtime_adapter, &prev_block_header)?;
    // let prev_chunk_header = chunk_state.prev_chunk.cloned_header();
    // let _ = validate_chunk_authorship(runtime_adapter, &prev_chunk_header)?;
    // if !Block::validate_chunk_header_proof(
    //     &prev_chunk_header,
    //     prev_block_header.chunk_headers_root(),
    //     &chunk_state.prev_merkle_proof,
    // ) {
    //     return Err(ErrorKind::MaliciousChallenge.into());
    // }
    //
    // // Validate current chunk and block header.
    // validate_header_authorship(runtime_adapter, &block_header)?;
    // let chunk_producer = validate_chunk_authorship(runtime_adapter, &chunk_state.chunk_header)?;
    // if !Block::validate_chunk_header_proof(
    //     &chunk_state.chunk_header,
    //     block_header.chunk_headers_root(),
    //     &chunk_state.merkle_proof,
    // ) {
    //     return Err(ErrorKind::MaliciousChallenge.into());
    // }

    // Apply state transition and check that the result state and other data doesn't match.
    // TODO (#6316): enable storage proof generation
    // let partial_storage = PartialStorage { nodes: chunk_state.partial_state.clone() };
    // let result = runtime
    //     .check_state_transition(
    //         partial_storage,
    //         prev_chunk_header.shard_id(),
    //         &prev_chunk_header.prev_state_root(),
    //         block_header.height(),
    //         block_header.raw_timestamp(),
    //         block_header.prev_hash(),
    //         block_header.hash(),
    //         chunk_state.prev_chunk.receipts(),
    //         chunk_state.prev_chunk.transactions(),
    //         ValidatorStakeIter::empty(),
    //         prev_block_header.gas_price(),
    //         prev_chunk_header.gas_limit(),
    //         &ChallengesResult::default(),
    //         *block_header.random_value(),
    //         // TODO: set it properly when challenges are enabled
    //         true,
    //         false,
    //     )
    //     .map_err(|_| Error::from(ErrorKind::MaliciousChallenge))?;
    // let outcome_root = ApplyTransactionResult::compute_outcomes_proof(&result.outcomes).0;
    // let proposals_match = result.validator_proposals.len()
    //     == chunk_state.chunk_header.validator_proposals().len()
    //     && result
    //         .validator_proposals
    //         .iter()
    //         .zip(chunk_state.chunk_header.validator_proposals())
    //         .all(|(x, y)| x == &y);
    // if result.new_root != chunk_state.chunk_header.prev_state_root()
    //     || outcome_root != chunk_state.chunk_header.outcome_root()
    //     || !proposals_match
    //     || result.total_gas_burnt != chunk_state.chunk_header.gas_used()
    // {
    //     Ok((*block_header.hash(), vec![chunk_producer]))
    // } else {
    //     // If all the data matches, this is actually valid chunk and challenge is malicious.
    //     Err(ErrorKind::MaliciousChallenge.into())
    // }
    // Ok((*block_header.hash(), vec![chunk_producer]))

    Err(Error::MaliciousChallenge)
}

/// Returns `Some(block_hash, vec![account_id])` of invalid block and who to
/// slash if challenge is correct and None if incorrect.
pub fn validate_challenge(
    epoch_manager: &dyn EpochManagerAdapter,
    runtime: &dyn RuntimeAdapter,
    epoch_id: &EpochId,
    last_block_hash: &CryptoHash,
    challenge: &Challenge,
) -> Result<(CryptoHash, Vec<AccountId>), Error> {
    // Check signature is correct on the challenge.
    if !epoch_manager.verify_validator_or_fisherman_signature(
        epoch_id,
        last_block_hash,
        &challenge.account_id,
        challenge.hash.as_ref(),
        &challenge.signature,
    )? {
        return Err(Error::InvalidChallenge);
    }
    match &challenge.body {
        ChallengeBody::BlockDoubleSign(block_double_sign) => {
            validate_double_sign(epoch_manager, block_double_sign)
        }
        ChallengeBody::ChunkProofs(chunk_proofs) => {
            validate_chunk_proofs_challenge(epoch_manager, chunk_proofs)
        }
        ChallengeBody::ChunkState(chunk_state) => {
            validate_chunk_state_challenge(runtime, chunk_state)
        }
    }
}

#[cfg(test)]
mod tests {
    use near_crypto::{InMemorySigner, KeyType};

    use super::*;

    fn make_tx(account_id: &str, seed: &str, nonce: Nonce) -> SignedTransaction {
        let account_id: AccountId = account_id.parse().unwrap();
        let signer = InMemorySigner::from_seed(account_id.clone(), KeyType::ED25519, seed);
        SignedTransaction::send_money(
            nonce,
            account_id,
            "bob".parse().unwrap(),
            &signer,
            10,
            CryptoHash::default(),
        )
    }

    #[test]
    pub fn test_transaction_order_empty() {
        let transactions = vec![];
        assert!(validate_transactions_order(&transactions));
    }

    #[test]
    pub fn test_transaction_order_one_tx() {
        let transactions = vec![make_tx("test_a", "test_A", 1)];
        assert!(validate_transactions_order(&transactions));
    }

    #[test]
    pub fn test_transaction_order_simple() {
        let transactions = vec![
            make_tx("test_a", "test_A", 1),
            make_tx("test_b", "test_A", 3),
            make_tx("test_a", "test_B", 4),
            make_tx("test_c", "test_A", 2),
            make_tx("test_b", "test_A", 6), // 2nd batch
            make_tx("test_c", "test_A", 5),
            make_tx("test_c", "test_A", 6), // 3rd batch
        ];
        assert!(validate_transactions_order(&transactions));
    }

    #[test]
    pub fn test_transaction_order_bad_nonce() {
        let transactions = vec![
            make_tx("test_a", "test_A", 2),
            make_tx("test_b", "test_A", 3),
            make_tx("test_c", "test_A", 2),
            make_tx("test_a", "test_A", 1), // 2nd batch, nonce 1 < 2
            make_tx("test_c", "test_A", 6),
        ];
        assert!(!validate_transactions_order(&transactions));
    }

    #[test]
    pub fn test_transaction_order_same_tx() {
        let transactions = vec![make_tx("test_a", "test_A", 1), make_tx("test_a", "test_A", 1)];
        assert!(!validate_transactions_order(&transactions));
    }

    #[test]
    pub fn test_transaction_order_skipped_in_first_batch() {
        let transactions = vec![
            make_tx("test_a", "test_A", 2),
            make_tx("test_c", "test_A", 2),
            make_tx("test_a", "test_A", 4), // 2nd batch starts
            make_tx("test_b", "test_A", 6), // Missing in the first batch
        ];
        assert!(!validate_transactions_order(&transactions));
    }

    #[test]
    pub fn test_transaction_order_skipped_in_2nd_batch() {
        let transactions = vec![
            make_tx("test_a", "test_A", 2),
            make_tx("test_c", "test_A", 2),
            make_tx("test_a", "test_A", 4), // 2nd batch starts
            make_tx("test_a", "test_A", 6), // 3rd batch starts
            make_tx("test_c", "test_A", 6), // Not in the 2nd batch
        ];
        assert!(!validate_transactions_order(&transactions));
    }
}
