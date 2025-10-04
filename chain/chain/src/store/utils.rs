use super::{ReceiptFilter, filter_incoming_receipts_for_shard};
use crate::byzantine_assert;
use crate::metrics;
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block::BlockHeader;
use near_primitives::errors::InvalidTxError;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::sharding::{ShardChunk, ShardChunkHeader};
use near_primitives::state_sync::ReceiptProofResponse;
use near_primitives::types::{BlockHeight, BlockHeightDelta, ShardId};
use near_store::adapter::chain_store::ChainStoreAdapter;
use std::sync::Arc;

/// Get full chunk from header with `height_included` taken from `header`, with
/// possible error that contains the header for further retrieval.
/// TODO: consider less hacky way to set `height_included` for chunks.
pub fn get_chunk_clone_from_header(
    chain_store: &ChainStoreAdapter,
    header: &ShardChunkHeader,
) -> Result<ShardChunk, Error> {
    let shard_chunk_result = chain_store.get_chunk(&header.chunk_hash());
    match shard_chunk_result {
        Err(_) => {
            return Err(Error::ChunksMissing(vec![header.clone()]));
        }
        Ok(shard_chunk) => {
            byzantine_assert!(header.height_included() > 0 || header.height_created() == 0);
            if header.height_included() == 0 && header.height_created() > 0 {
                return Err(Error::Other(format!(
                    "Invalid header: {:?} for chunk {:?}",
                    header, shard_chunk
                )));
            }
            let mut shard_chunk_clone = ShardChunk::clone(&shard_chunk);
            shard_chunk_clone.set_height_included(header.height_included());
            Ok(shard_chunk_clone)
        }
    }
}

/// Returns block header from the current chain defined by `sync_hash` for given height if present.
pub fn get_block_header_on_chain_by_height(
    chain_store: &ChainStoreAdapter,
    sync_hash: &CryptoHash,
    height: BlockHeight,
) -> Result<Arc<BlockHeader>, Error> {
    let mut header = chain_store.get_block_header(sync_hash)?;
    let mut hash = *sync_hash;
    while header.height() > height {
        hash = *header.prev_hash();
        header = chain_store.get_block_header(&hash)?;
    }
    let header_height = header.height();
    if header_height < height {
        return Err(Error::InvalidBlockHeight(header_height));
    }
    chain_store.get_block_header(&hash)
}

/// For a given transaction, it expires if the block that the chunk points to is more than `validity_period`
/// ahead of the block that has `base_block_hash`.
pub fn check_transaction_validity_period(
    chain_store: &ChainStoreAdapter,
    prev_block_header: &BlockHeader,
    base_block_hash: &CryptoHash,
    transaction_validity_period: BlockHeightDelta,
) -> Result<(), InvalidTxError> {
    let base_header =
        chain_store.get_block_header(base_block_hash).map_err(|_| InvalidTxError::Expired)?;

    metrics::CHAIN_VALIDITY_PERIOD_CHECK_DELAY
        .observe(prev_block_header.height().saturating_sub(base_header.height()) as f64);

    // First check the distance between blocks
    if prev_block_header.height() > base_header.height() + transaction_validity_period {
        return Err(InvalidTxError::Expired);
    }

    // Then check if there is a path between the blocks (`base` is an ancestor of `prev`)
    validity_period_validate_is_ancestor(&base_header, prev_block_header, chain_store)
}

/// Transaction validity period check used in early prepare transactions. It's different from the
/// standard `check_transaction_validity_period` because early transaction preparation doesn't know
/// what the previous block hash is, it only knows the one before that. Normal validity check uses
/// `prev_block_header`, which is the previous block before the one which will contain the
/// transactions. This check uses `prev_prev_block_header`, which is the block before the previous
/// block and the same height as the normal check.
pub fn early_prepare_txs_check_validity_period(
    chain_store: &ChainStoreAdapter,
    prev_block_height: BlockHeight,
    prev_prev_block_header: &BlockHeader,
    base_block_hash: &CryptoHash,
    transaction_validity_period: BlockHeightDelta,
) -> Result<(), InvalidTxError> {
    let base_header =
        chain_store.get_block_header(base_block_hash).map_err(|_| InvalidTxError::Expired)?;

    metrics::CHAIN_VALIDITY_PERIOD_CHECK_DELAY
        .observe(prev_block_height.saturating_sub(base_header.height()) as f64);

    // First check the distance between blocks
    if prev_block_height > base_header.height() + transaction_validity_period {
        return Err(InvalidTxError::Expired);
    }

    // Then check if there is a path between the blocks (`base` is ancestor of `prev_prev`)
    validity_period_validate_is_ancestor(&base_header, prev_prev_block_header, chain_store)
}

/// Check if base_header is an ancestor of prev_block_header.
/// Used in check_transaction_validity_period.
fn validity_period_validate_is_ancestor(
    base_header: &BlockHeader,
    prev_block_header: &BlockHeader,
    chain_store: &ChainStoreAdapter,
) -> Result<(), InvalidTxError> {
    let base_height = base_header.height();
    let prev_height = prev_block_header.height();
    let base_block_hash = base_header.hash();

    // Base can't be an ancestor of prev if its height is bigger
    if base_height > prev_height {
        return Err(InvalidTxError::InvalidChain);
    }

    // if both are on the canonical chain, comparing height is sufficient
    // we special case this because it is expected that this scenario will happen in most cases.
    if let Ok(base_block_hash_by_height) = chain_store.get_block_hash_by_height(base_height) {
        if &base_block_hash_by_height == base_block_hash {
            if let Ok(prev_hash) = chain_store.get_block_hash_by_height(prev_height) {
                if &prev_hash == prev_block_header.hash() {
                    return Ok(());
                }
            }
        }
    }

    // if the base block height is smaller than `last_final_height` we only need to check
    // whether the base block is the same as the one with that height on the canonical fork.
    // Otherwise we walk back the chain to check whether base block is on the same chain.
    let last_final_height = chain_store
        .get_block_height(prev_block_header.last_final_block())
        .map_err(|_| InvalidTxError::InvalidChain)?;

    if last_final_height >= base_height {
        let base_block_hash_by_height = chain_store
            .get_block_hash_by_height(base_height)
            .map_err(|_| InvalidTxError::InvalidChain)?;
        if &base_block_hash_by_height == base_block_hash {
            Ok(())
        } else {
            Err(InvalidTxError::InvalidChain)
        }
    } else {
        let header =
            get_block_header_on_chain_by_height(chain_store, prev_block_header.hash(), base_height)
                .map_err(|_| InvalidTxError::InvalidChain)?;
        if header.hash() == base_block_hash { Ok(()) } else { Err(InvalidTxError::InvalidChain) }
    }
}

/// Collect incoming receipts for shard `shard_id` from
/// the block at height `last_chunk_height_included` (non-inclusive) to the
/// block `block_hash` (inclusive), leaving only receipts based on the
/// `receipts_filter`.
/// This is needed because for every empty chunk for the blocks in between,
/// receipts from other shards from these blocks still must be propagated.
pub fn get_incoming_receipts_for_shard(
    chain_store: &ChainStoreAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
    target_shard_id: ShardId,
    target_shard_layout: &ShardLayout,
    block_hash: CryptoHash,
    last_chunk_height_included: BlockHeight,
    receipts_filter: ReceiptFilter,
) -> Result<Vec<ReceiptProofResponse>, Error> {
    let _span =
            tracing::debug_span!(target: "chain", "get_incoming_receipts_for_shard", ?target_shard_id, ?block_hash, last_chunk_height_included).entered();

    let mut ret = vec![];

    let mut current_shard_id = target_shard_id;
    let mut current_block_hash = block_hash;
    let mut current_shard_layout = target_shard_layout.clone();

    loop {
        let header = chain_store.get_block_header(&current_block_hash)?;

        if header.height() < last_chunk_height_included {
            panic!("get_incoming_receipts_for_shard failed");
        }

        if header.height() == last_chunk_height_included {
            break;
        }

        let prev_hash = header.prev_hash();
        let prev_shard_layout = epoch_manager.get_shard_layout_from_prev_block(prev_hash)?;

        if prev_shard_layout != current_shard_layout {
            let parent_shard_id = current_shard_layout.get_parent_shard_id(current_shard_id)?;
            tracing::info!(
                target: "chain",
                version = current_shard_layout.version(),
                prev_version = prev_shard_layout.version(),
                ?current_shard_id,
                ?parent_shard_id,
                "crossing epoch boundary with shard layout change, updating shard id"
            );
            current_shard_id = parent_shard_id;
            current_shard_layout = prev_shard_layout;
        }

        let maybe_receipts_proofs =
            chain_store.get_incoming_receipts(&current_block_hash, current_shard_id);
        let receipts_proofs = match maybe_receipts_proofs {
            Ok(receipts_proofs) => {
                tracing::debug!(
                    target: "chain",
                    "found receipts from block with missing chunks",
                );
                receipts_proofs
            }
            Err(err) => {
                tracing::debug!(
                    target: "chain",
                    ?err,
                    "could not find receipts from block with missing chunks"
                );

                // This can happen when all chunks are missing in a block
                // and then we can safely assume that there aren't any
                // incoming receipts. It would be nicer to explicitly check
                // that condition rather than relying on errors when reading
                // from the db.
                Arc::new(vec![])
            }
        };

        let filtered_receipt_proofs = match receipts_filter {
            ReceiptFilter::All => receipts_proofs,
            ReceiptFilter::TargetShard => Arc::new(filter_incoming_receipts_for_shard(
                &target_shard_layout,
                target_shard_id,
                receipts_proofs,
            )?),
        };

        ret.push(ReceiptProofResponse(current_block_hash, filtered_receipt_proofs));
        current_block_hash = *prev_hash;
    }

    Ok(ret)
}

/// Finds first of the given hashes that is known on the main chain.
fn find_common_header(
    chain_store: &ChainStoreAdapter,
    hashes: &[CryptoHash],
) -> Option<Arc<BlockHeader>> {
    for hash in hashes {
        if let Ok(header) = chain_store.get_block_header(hash) {
            if let Ok(header_at_height) = chain_store.get_block_header_by_height(header.height()) {
                if header.hash() == header_at_height.hash() {
                    return Some(header);
                }
            }
        }
    }
    None
}

/// Retrieve the up to `max_headers_returned` headers on the main chain
/// `hashes`: a list of block "locators". `hashes` should be ordered from older blocks to
///           more recent blocks. This function will find the first block in `hashes`
///           that is on the main chain and returns the blocks after this block. If none of the
///           blocks in `hashes` are on the main chain, the function returns an empty vector.
pub fn retrieve_headers(
    chain_store: &ChainStoreAdapter,
    hashes: Vec<CryptoHash>,
    max_headers_returned: u64,
) -> Result<Vec<Arc<BlockHeader>>, Error> {
    let header = match find_common_header(chain_store, &hashes) {
        Some(header) => header,
        None => return Ok(vec![]),
    };

    // Use `get_block_merkle_tree` to get the block ordinal for this header.
    // We can't use the `header.block_ordinal()` method because older block headers don't have this field.
    // The same method is used in `get_locator` which creates the headers request and chain store when saving block ordinals.
    let block_ordinal = chain_store.get_block_merkle_tree(&header.hash())?.size();

    let mut headers = vec![];
    for i in 1..=max_headers_returned {
        match chain_store
            .get_block_hash_from_ordinal(block_ordinal.saturating_add(i))
            .and_then(|block_hash| chain_store.get_block_header(&block_hash))
        {
            Ok(h) => headers.push(h),
            Err(_) => break, // This is either the last block that we know of, or we don't have these block headers because of epoch sync.
        }
    }
    Ok(headers)
}
