use crate::stateless_validation::metrics::VALIDATE_CHUNK_WITH_ENCODED_MERKLE_ROOT_TIME;
use crate::{Chain, byzantine_assert};
use crate::{ChainStore, Error};
use borsh::BorshSerialize;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::bandwidth_scheduler::BandwidthRequests;
use near_primitives::congestion_info::CongestionInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::merklize;
use near_primitives::receipt::Receipt;
use near_primitives::sharding::{EncodedShardChunkBody, ShardChunk, ShardChunkHeader};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{BlockHeight, ShardId};
use near_primitives::version::ProtocolFeature;
use reed_solomon_erasure::galois_8::ReedSolomon;

/// Gas limit cannot be adjusted for more than 0.1% at a time.
const GAS_LIMIT_ADJUSTMENT_FACTOR: u64 = 1000;

/// Verifies that chunk's proofs in the header match the body.
pub fn validate_chunk_proofs(
    chunk: &ShardChunk,
    epoch_manager: &dyn EpochManagerAdapter,
) -> Result<bool, Error> {
    let correct_chunk_hash = chunk.compute_header_hash();

    // 1. Checking chunk.header.hash
    let header_hash = chunk.header_hash();
    if header_hash != &correct_chunk_hash {
        byzantine_assert!(false);
        return Ok(false);
    }

    // 2. Checking that chunk body is valid
    // 2a. Checking chunk hash
    if chunk.chunk_hash() != &correct_chunk_hash {
        byzantine_assert!(false);
        return Ok(false);
    }
    let height_created = chunk.height_created();
    let outgoing_receipts_root = chunk.prev_outgoing_receipts_root();
    let (transactions, receipts) = (chunk.to_transactions(), chunk.prev_outgoing_receipts());

    // 2b. Checking that chunk transactions are valid
    let (tx_root, _) = merklize(transactions);
    if &tx_root != chunk.tx_root() {
        byzantine_assert!(false);
        return Ok(false);
    }
    // 2c. Checking that chunk receipts are valid
    if height_created == 0 {
        return Ok(receipts.is_empty() && outgoing_receipts_root == &CryptoHash::default());
    } else {
        let shard_layout = {
            let prev_block_hash = chunk.prev_block_hash();
            epoch_manager.get_shard_layout_from_prev_block(&prev_block_hash)?
        };
        let outgoing_receipts_hashes = Chain::build_receipts_hashes(receipts, &shard_layout)?;
        let (receipts_root, _) = merklize(&outgoing_receipts_hashes);
        if &receipts_root != outgoing_receipts_root {
            byzantine_assert!(false);
            return Ok(false);
        }
    }
    Ok(true)
}

/// Validate that all next chunk information matches previous chunk extra.
pub fn validate_chunk_with_chunk_extra(
    chain_store: &ChainStore,
    epoch_manager: &dyn EpochManagerAdapter,
    prev_block_hash: &CryptoHash,
    prev_chunk_extra: &ChunkExtra,
    prev_chunk_height_included: BlockHeight,
    chunk_header: &ShardChunkHeader,
) -> Result<Vec<Receipt>, Error> {
    let outgoing_receipts = chain_store.get_outgoing_receipts_for_shard(
        epoch_manager,
        *prev_block_hash,
        chunk_header.shard_id(),
        prev_chunk_height_included,
    )?;
    let outgoing_receipts_hashes = {
        let shard_layout = epoch_manager.get_shard_layout_from_prev_block(prev_block_hash)?;
        Chain::build_receipts_hashes(&outgoing_receipts, &shard_layout)?
    };
    let (outgoing_receipts_root, _) = merklize(&outgoing_receipts_hashes);

    validate_chunk_with_chunk_extra_and_receipts_root(
        prev_chunk_extra,
        chunk_header,
        &outgoing_receipts_root,
    )?;

    Ok(outgoing_receipts)
}

pub fn validate_chunk_with_chunk_extra_and_roots(
    chain_store: &ChainStore,
    epoch_manager: &dyn EpochManagerAdapter,
    prev_block_hash: &CryptoHash,
    prev_chunk_extra: &ChunkExtra,
    prev_chunk_height_included: BlockHeight,
    chunk_header: &ShardChunkHeader,
    new_transactions: &[SignedTransaction],
    rs: &ReedSolomon,
) -> Result<(), Error> {
    let outgoing_receipts = validate_chunk_with_chunk_extra(
        chain_store,
        epoch_manager,
        prev_block_hash,
        prev_chunk_extra,
        prev_chunk_height_included,
        chunk_header,
    )?;

    let epoch_id = epoch_manager.get_epoch_id_from_prev_block(prev_block_hash)?;
    let protocol_version = epoch_manager.get_epoch_protocol_version(&epoch_id)?;
    if ProtocolFeature::ChunkPartChecks.enabled(protocol_version) {
        let (tx_root, _) = merklize(new_transactions);
        if &tx_root != chunk_header.tx_root() {
            return Err(Error::InvalidTxRoot);
        }

        validate_chunk_with_encoded_merkle_root(
            chunk_header,
            &outgoing_receipts,
            new_transactions,
            rs,
            chunk_header.shard_id(),
        )
    } else {
        Ok(())
    }
}

/// Validate that all next chunk information matches previous chunk extra.
pub fn validate_chunk_with_chunk_extra_and_receipts_root(
    prev_chunk_extra: &ChunkExtra,
    chunk_header: &ShardChunkHeader,
    outgoing_receipts_root: &CryptoHash,
) -> Result<(), Error> {
    if *prev_chunk_extra.state_root() != chunk_header.prev_state_root() {
        return Err(Error::InvalidStateRoot);
    }

    if prev_chunk_extra.outcome_root() != chunk_header.prev_outcome_root() {
        return Err(Error::InvalidOutcomesProof);
    }

    let chunk_extra_proposals = prev_chunk_extra.validator_proposals();
    let chunk_header_proposals = chunk_header.prev_validator_proposals();
    if chunk_header_proposals.len() != chunk_extra_proposals.len()
        || !chunk_extra_proposals.eq(chunk_header_proposals)
    {
        return Err(Error::InvalidValidatorProposals);
    }

    if prev_chunk_extra.gas_limit() != chunk_header.gas_limit() {
        return Err(Error::InvalidGasLimit);
    }

    if prev_chunk_extra.gas_used() != chunk_header.prev_gas_used() {
        return Err(Error::InvalidGasUsed);
    }

    if prev_chunk_extra.balance_burnt() != chunk_header.prev_balance_burnt() {
        return Err(Error::InvalidBalanceBurnt);
    }

    if outgoing_receipts_root != chunk_header.prev_outgoing_receipts_root() {
        return Err(Error::InvalidReceiptsProof);
    }

    let gas_limit = prev_chunk_extra.gas_limit();
    if chunk_header.gas_limit() < gas_limit - gas_limit / GAS_LIMIT_ADJUSTMENT_FACTOR
        || chunk_header.gas_limit() > gas_limit + gas_limit / GAS_LIMIT_ADJUSTMENT_FACTOR
    {
        return Err(Error::InvalidGasLimit);
    }

    validate_congestion_info(prev_chunk_extra.congestion_info(), chunk_header.congestion_info())?;
    validate_bandwidth_requests(
        prev_chunk_extra.bandwidth_requests(),
        chunk_header.bandwidth_requests(),
    )?;

    Ok(())
}

#[derive(BorshSerialize)]
struct TransactionReceiptRef<'a>(&'a [SignedTransaction], &'a [Receipt]);

pub fn validate_chunk_with_encoded_merkle_root(
    chunk_header: &ShardChunkHeader,
    outgoing_receipts: &[Receipt],
    new_transactions: &[SignedTransaction],
    rs: &ReedSolomon,
    shard_id: ShardId,
) -> Result<(), Error> {
    let shard_id_label = shard_id.to_string();
    let _timer = VALIDATE_CHUNK_WITH_ENCODED_MERKLE_ROOT_TIME
        .with_label_values(&[shard_id_label.as_str()])
        .start_timer();

    let receipt_ref = TransactionReceiptRef(new_transactions, outgoing_receipts);
    let (transaction_receipts_parts, encoded_length) =
        near_primitives::reed_solomon::reed_solomon_encode(rs, &receipt_ref);
    let content = EncodedShardChunkBody { parts: transaction_receipts_parts };
    let (encoded_merkle_root, _merkle_paths) = content.get_merkle_hash_and_paths();

    if encoded_merkle_root != *chunk_header.encoded_merkle_root() {
        return Err(Error::InvalidChunkEncodedMerkleRoot);
    }

    if encoded_length != chunk_header.encoded_length() as usize {
        return Err(Error::InvalidChunkEncodedLength);
    }

    Ok(())
}

/// Validate the congestion info propagation from the chunk extra of the previous
/// chunk to the chunk header of the current chunk. The extra congestion info is
/// trusted as it is the result of verified computation. The header congestion
/// info is being validated.
fn validate_congestion_info(
    extra_congestion_info: CongestionInfo,
    header_congestion_info: CongestionInfo,
) -> Result<(), Error> {
    CongestionInfo::validate_extra_and_header(&extra_congestion_info, &header_congestion_info)
        .then_some(())
        .ok_or_else(|| {
            Error::InvalidCongestionInfo(format!(
                "Congestion Information validate error. extra: {:?}, header: {:?}",
                extra_congestion_info, header_congestion_info
            ))
        })
}

fn validate_bandwidth_requests(
    extra_bandwidth_requests: Option<&BandwidthRequests>,
    header_bandwidth_requests: Option<&BandwidthRequests>,
) -> Result<(), Error> {
    if extra_bandwidth_requests != header_bandwidth_requests {
        fn requests_len(requests_opt: Option<&BandwidthRequests>) -> usize {
            match requests_opt {
                Some(BandwidthRequests::V1(requests_v1)) => requests_v1.requests.len(),
                None => 0,
            }
        }
        let error_info_str = format!(
            "chunk extra: (is_some: {}, len: {}) chunk header: (is_some: {}, len: {})",
            extra_bandwidth_requests.is_some(),
            requests_len(extra_bandwidth_requests),
            header_bandwidth_requests.is_some(),
            requests_len(header_bandwidth_requests)
        );
        return Err(Error::InvalidBandwidthRequests(error_info_str));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use borsh::to_vec;
    use near_crypto::{InMemorySigner, Signer};
    use near_primitives::receipt::{ActionReceipt, DataReceiver, Receipt, ReceiptEnum, ReceiptV1};
    use near_primitives::transaction::{Action, TransferAction};
    use near_primitives::types::{AccountId, Balance};

    #[test]
    /// Asserts that serializing `TransactionReceiptRef` produces the same output
    /// of serializing `TransactionReceipt`.
    fn transaction_receipt_ref_serialization() {
        let signer: Signer = InMemorySigner::test_signer(&"alice.near".parse().unwrap());
        let receiver: AccountId = "bob.near".parse().unwrap();

        // Create example tx and receipt
        let tx = SignedTransaction::from_actions(
            1,
            signer.get_account_id(),
            receiver.clone(),
            &signer,
            vec![Action::Transfer(TransferAction { deposit: 1 })],
            CryptoHash::default(),
            0,
        );
        let ar = ActionReceipt {
            signer_id: signer.get_account_id(),
            signer_public_key: signer.public_key(),
            gas_price: 0 as Balance,
            output_data_receivers: vec![DataReceiver {
                data_id: CryptoHash::default(),
                receiver_id: signer.get_account_id(),
            }],
            input_data_ids: vec![CryptoHash::default()],
            actions: vec![Action::Transfer(TransferAction { deposit: 1 })],
        };
        let receipt = Receipt::V1(ReceiptV1 {
            predecessor_id: signer.get_account_id(),
            receiver_id: receiver,
            receipt_id: CryptoHash::default(),
            receipt: ReceiptEnum::Action(ar),
            priority: 0,
        });

        // Cases: empty/empty, txs-only, receipts-only, both
        let cases: Vec<(Vec<SignedTransaction>, Vec<Receipt>)> = vec![
            (vec![], vec![]),
            (vec![tx.clone()], vec![]),
            (vec![], vec![receipt.clone()]),
            (vec![tx.clone(), tx], vec![receipt.clone(), receipt]),
        ];

        for (txs, rs) in cases {
            let owned = near_primitives::sharding::TransactionReceipt(txs.clone(), rs.clone());
            let owned_bytes = to_vec(&owned).unwrap();

            let borrowed = TransactionReceiptRef(&txs, &rs);
            let borrowed_bytes = to_vec(&borrowed).unwrap();

            assert_eq!(owned_bytes, borrowed_bytes);
        }
    }
}
