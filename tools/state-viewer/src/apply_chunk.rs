use anyhow::{anyhow, Context};
use near_chain::chain::collect_receipts_from_response;
use near_chain::migrations::check_if_block_is_first_with_chunk_of_version;
use near_chain::types::ApplyTransactionResult;
use near_chain::{ChainStore, ChainStoreAccess, RuntimeAdapter};
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::combine_hash;
use near_primitives::receipt::Receipt;
use near_primitives::sharding::{ChunkHash, ReceiptProof};
use near_primitives::syncing::ReceiptProofResponse;
use near_primitives_core::hash::hash;
use near_primitives_core::types::Gas;
use near_store::Store;
use nearcore::{NearConfig, NightshadeRuntime};
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

// like ChainStoreUpdate::get_incoming_receipts_for_shard(), but for the case when we don't
// know of a block containing the target chunk
fn get_incoming_receipts(
    chain_store: &mut ChainStore,
    chunk_hash: &ChunkHash,
    shard_id: u64,
    target_height: u64,
    prev_hash: &CryptoHash,
    prev_height_included: u64,
) -> anyhow::Result<Vec<Receipt>> {
    let mut receipt_proofs = vec![];

    let chunk_hashes = chain_store.get_all_chunk_hashes_by_height(target_height)?;
    if !chunk_hashes.contains(chunk_hash) {
        return Err(anyhow!(
            "given chunk hash is not listed in ColChunkHashesByHeight[{}]",
            target_height
        ));
    }

    let chunks =
        chunk_hashes.iter().map(|h| chain_store.get_chunk(h).unwrap().clone()).collect::<Vec<_>>();

    for chunk in chunks {
        let partial_encoded_chunk = chain_store.get_partial_chunk(&chunk.chunk_hash()).unwrap();
        for receipt in partial_encoded_chunk.receipts().iter() {
            let ReceiptProof(_, shard_proof) = receipt;
            if shard_proof.to_shard_id == shard_id {
                receipt_proofs.push(receipt.clone());
            }
        }
    }
    let mut responses = vec![ReceiptProofResponse(CryptoHash::default(), receipt_proofs)];
    responses.extend_from_slice(&chain_store.store_update().get_incoming_receipts_for_shard(
        shard_id,
        *prev_hash,
        prev_height_included,
    )?);
    Ok(collect_receipts_from_response(&responses))
}

fn check_hashes_exist<T, F: Fn(&T) -> CryptoHash>(
    hashes: &Option<Vec<CryptoHash>>,
    items: &[T],
    item_hash: F,
) -> Result<(), CryptoHash> {
    match hashes {
        Some(hashes) => {
            let hashes_seen = items.iter().map(item_hash).collect::<HashSet<_>>();
            for hash in hashes.iter() {
                if !hashes_seen.contains(hash) {
                    return Err(*hash);
                }
            }
            Ok(())
        }
        None => Ok(()),
    }
}

// returns (apply_result, gas limit)
pub(crate) fn apply_chunk(
    home_dir: &Path,
    near_config: NearConfig,
    store: Store,
    chunk_hash: ChunkHash,
    target_height: Option<u64>,
    tx_hashes: &Option<Vec<CryptoHash>>,
    receipt_hashes: &Option<Vec<CryptoHash>>,
) -> anyhow::Result<(ApplyTransactionResult, Gas)> {
    let mut chain_store = ChainStore::new(store.clone(), near_config.genesis.config.genesis_height);
    let runtime = Arc::new(NightshadeRuntime::with_config(
        home_dir,
        store,
        &near_config,
        None,
        near_config.client_config.max_gas_burnt_view,
    ));
    let chunk = chain_store.get_chunk(&chunk_hash)?;
    let chunk_header = chunk.cloned_header();

    let prev_block_hash = chunk_header.prev_block_hash();
    let shard_id = chunk.shard_id();
    let prev_state_root = chunk.prev_state_root();

    let transactions = chunk.transactions().clone();
    check_hashes_exist(&tx_hashes, &transactions, |tx| tx.get_hash()).map_err(|hash| {
        anyhow!("transaction with hash {} not found in chunk {:?}", hash, &chunk_hash)
    })?;

    let prev_block =
        chain_store.get_block(&prev_block_hash).context("Failed getting chunk's prev block")?;
    let prev_height_included = prev_block.chunks()[shard_id as usize].height_included();
    let prev_height = prev_block.header().height();
    let target_height = match target_height {
        Some(h) => h,
        None => prev_height + 1,
    };
    let prev_timestamp = prev_block.header().raw_timestamp();
    let gas_price = prev_block.header().gas_price();
    let receipts = get_incoming_receipts(
        &mut chain_store,
        &chunk_hash,
        shard_id,
        target_height,
        &prev_block_hash,
        prev_height_included,
    )
    .context("Failed collecting incoming receipts")?;
    check_hashes_exist(&receipt_hashes, &receipts, |r| r.receipt_id).map_err(|hash| {
        anyhow!("receipt with ID {} not found in any incoming receipt for shard {}", hash, shard_id)
    })?;
    let is_first_block_with_chunk_of_version = check_if_block_is_first_with_chunk_of_version(
        &mut chain_store,
        runtime.as_ref(),
        &prev_block_hash,
        shard_id,
    )?;

    Ok((
        runtime.apply_transactions(
            shard_id,
            &prev_state_root,
            target_height,
            prev_timestamp + 1_000_000_000,
            &prev_block_hash,
            &combine_hash(
                &prev_block_hash,
                &hash("nonsense block hash for testing purposes".as_ref()),
            ),
            &receipts,
            &transactions,
            chunk_header.validator_proposals(),
            gas_price,
            chunk_header.gas_limit(),
            &vec![],
            hash("random seed".as_ref()),
            true,
            is_first_block_with_chunk_of_version,
            None,
        )?,
        chunk_header.gas_limit(),
    ))
}
