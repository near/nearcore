use anyhow::{Context, anyhow};
use borsh::BorshDeserialize;
use near_chain::chain::collect_receipts_from_response;
use near_chain::types::{
    ApplyChunkBlockContext, ApplyChunkResult, ApplyChunkShardContext, RuntimeAdapter,
};
use near_chain::{ChainStore, ChainStoreAccess, ReceiptFilter, get_incoming_receipts_for_shard};
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_epoch_manager::{EpochManagerAdapter, EpochManagerHandle};
use near_primitives::apply::ApplyChunkReason;
use near_primitives::bandwidth_scheduler::BlockBandwidthRequests;
use near_primitives::block::MaybeNew;
use near_primitives::congestion_info::BlockCongestionInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::combine_hash;
use near_primitives::receipt::Receipt;
use near_primitives::sharding::{ChunkHash, ReceiptProof};
use near_primitives::state_sync::ReceiptProofResponse;
use near_primitives::types::{BlockHeight, ShardId};
use near_primitives_core::hash::hash;
use near_primitives_core::types::Gas;
use near_store::DBCol;
use near_store::Store;
use node_runtime::SignedValidPeriodTransactions;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use crate::cli::StorageSource;
use crate::util::{check_apply_block_result, resulting_chunk_extra};

// `get_incoming_receipts_for_shard` implementation for the case when we don't
// know of a block containing the target chunk
fn get_incoming_receipts(
    chain_store: &ChainStore,
    epoch_manager: &EpochManagerHandle,
    chunk_hash: &ChunkHash,
    shard_id: ShardId,
    target_height: u64,
    prev_hash: &CryptoHash,
    prev_height_included: u64,
    rng: Option<StdRng>,
) -> anyhow::Result<Vec<Receipt>> {
    let mut receipt_proofs = vec![];

    let chunk_hashes = chain_store.get_all_chunk_hashes_by_height(target_height)?;
    if !chunk_hashes.contains(chunk_hash) {
        return Err(anyhow!(
            "given chunk hash is not listed in DBCol::ChunkHashesByHeight[{}]",
            target_height
        ));
    }

    let mut chunks =
        chunk_hashes.iter().map(|h| chain_store.get_chunk(h).unwrap()).collect::<Vec<_>>();
    chunks.sort_by_key(|chunk| chunk.shard_id());

    for chunk in chunks {
        if let Ok(partial_encoded_chunk) = chain_store.get_partial_chunk(&chunk.chunk_hash()) {
            for receipt in partial_encoded_chunk.prev_outgoing_receipts() {
                let ReceiptProof(_, shard_proof) = receipt;
                if shard_proof.to_shard_id == shard_id {
                    receipt_proofs.push(receipt.clone());
                }
            }
        } else {
            tracing::error!(target: "state-viewer", chunk_hash = ?chunk.chunk_hash(), "Failed to get a partial chunk");
        }
    }

    if let Some(mut rng) = rng {
        // for testing purposes, shuffle the receipts the same way it's done normally so we can compare the state roots
        receipt_proofs.shuffle(&mut rng);
    }
    let mut responses = vec![ReceiptProofResponse(CryptoHash::default(), Arc::new(receipt_proofs))];
    let shard_layout = epoch_manager.get_shard_layout_from_prev_block(prev_hash)?;
    responses.extend_from_slice(&get_incoming_receipts_for_shard(
        &chain_store,
        epoch_manager,
        shard_id,
        &shard_layout,
        *prev_hash,
        prev_height_included,
        ReceiptFilter::TargetShard,
    )?);
    Ok(collect_receipts_from_response(&responses))
}

// returns (apply_result, gas limit)
pub fn apply_chunk(
    epoch_manager: &EpochManagerHandle,
    runtime: &dyn RuntimeAdapter,
    chain_store: &mut ChainStore,
    chunk_hash: ChunkHash,
    target_height: Option<u64>,
    rng: Option<StdRng>,
    storage: StorageSource,
) -> anyhow::Result<(ApplyChunkResult, Gas)> {
    let chunk = chain_store.get_chunk(&chunk_hash)?;
    let chunk_header = chunk.cloned_header();

    // This doesn't work at resharding epoch boundary if there are missing chunks.
    let shard_id = chunk_header.shard_id();

    let prev_block_hash = chunk_header.prev_block_hash();
    let prev_state_root = chunk.prev_state_root();

    let transactions = chunk.to_transactions().to_vec();
    let prev_block =
        chain_store.get_block(prev_block_hash).context("Failed getting chunk's prev block")?;
    let prev_epoch_id = prev_block.header().epoch_id();
    let prev_shard_layout = epoch_manager.get_shard_layout(prev_epoch_id)?;
    let prev_shard_index = prev_shard_layout.get_shard_index(shard_id).unwrap();
    let prev_height_included = prev_block.chunks()[prev_shard_index].height_included();
    let prev_height = prev_block.header().height();
    let target_height = match target_height {
        Some(h) => h,
        None => prev_height + 1,
    };

    // Try to recover bandwidth requests and congestion info from the previous chunk extras.
    // Normally they would be taken from the block that contains the applied chunk,
    // but it's not available here.
    // The chunk can still be applied with the wrong bandwidth requests and congestion info,
    // but the resulting state root might end up being different.
    let mut shards_bandwidth_requests = BTreeMap::new();
    let mut shards_congestion_info = BTreeMap::new();
    for prev_chunk in prev_block.chunks().iter() {
        let shard_id = match prev_chunk {
            MaybeNew::New(new_chunk) => new_chunk.shard_id(),
            MaybeNew::Old(missing_chunk) => missing_chunk.shard_id(),
        };
        let shard_uid =
            shard_id_to_uid(epoch_manager, shard_id, prev_block.header().epoch_id()).unwrap();
        let Ok(chunk_extra) = chain_store.get_chunk_extra(&prev_block_hash, &shard_uid) else {
            continue;
        };
        if let Some(bandwidth_requests) = chunk_extra.bandwidth_requests() {
            shards_bandwidth_requests.insert(shard_id, bandwidth_requests.clone());
        }
        shards_congestion_info.insert(
            shard_id,
            near_primitives::congestion_info::ExtendedCongestionInfo {
                congestion_info: chunk_extra.congestion_info(),
                missed_chunks_count: 0, // Assume no missing chunks in this block
            },
        );
    }
    let block_bandwidth_requests = BlockBandwidthRequests { shards_bandwidth_requests };
    let block_congestion_info = BlockCongestionInfo::new(shards_congestion_info);

    let prev_timestamp = prev_block.header().raw_timestamp();
    let gas_price = prev_block.header().next_gas_price();
    let receipts = get_incoming_receipts(
        chain_store,
        epoch_manager,
        &chunk_hash,
        shard_id,
        target_height,
        prev_block_hash,
        prev_height_included,
        rng,
    )
    .context("Failed collecting incoming receipts")?;

    if matches!(storage, StorageSource::FlatStorage) {
        let shard_uid =
            shard_id_to_uid(epoch_manager, shard_id, prev_block.header().epoch_id()).unwrap();
        runtime.get_flat_storage_manager().create_flat_storage_for_shard(shard_uid).unwrap();
    }

    let valid_txs = chain_store.compute_transaction_validity(prev_block.header(), &chunk);

    Ok((
        runtime.apply_chunk(
            storage.create_runtime_storage(prev_state_root),
            ApplyChunkReason::UpdateTrackedShard,
            ApplyChunkShardContext {
                shard_id,
                last_validator_proposals: chunk_header.prev_validator_proposals(),
                gas_limit: chunk_header.gas_limit(),
                is_new_chunk: true,
            },
            ApplyChunkBlockContext {
                height: target_height,
                block_timestamp: prev_timestamp + 1_000_000_000,
                prev_block_hash: *prev_block_hash,
                block_hash: combine_hash(
                    prev_block_hash,
                    &hash("nonsense block hash for testing purposes".as_ref()),
                ),
                gas_price,
                random_seed: hash("random seed".as_ref()),
                congestion_info: block_congestion_info,
                bandwidth_requests: block_bandwidth_requests,
            },
            &receipts,
            SignedValidPeriodTransactions::new(transactions, valid_txs),
        )?,
        chunk_header.gas_limit(),
    ))
}

enum HashType {
    Tx,
    Receipt,
}

fn find_tx_or_receipt(
    hash: &CryptoHash,
    block_hash: &CryptoHash,
    epoch_manager: &EpochManagerHandle,
    chain_store: &ChainStore,
) -> anyhow::Result<Option<(HashType, ShardId)>> {
    let block = chain_store.get_block(block_hash)?;
    let chunk_hashes = block.chunks().iter_deprecated().map(|c| c.chunk_hash()).collect::<Vec<_>>();

    let epoch_id = block.header().epoch_id();
    let shard_layout = epoch_manager.get_shard_layout(epoch_id)?;

    for (shard_index, chunk_hash) in chunk_hashes.iter().enumerate() {
        let shard_id = shard_layout.get_shard_id(shard_index).unwrap();
        let chunk =
            chain_store.get_chunk(chunk_hash).context("Failed looking up candidate chunk")?;
        for tx in chunk.to_transactions() {
            if &tx.get_hash() == hash {
                return Ok(Some((HashType::Tx, shard_id)));
            }
        }
        for receipt in chunk.prev_outgoing_receipts() {
            if &receipt.get_hash() == hash {
                let shard_layout =
                    epoch_manager.get_shard_layout_from_prev_block(chunk.prev_block())?;
                let to_shard = shard_layout.account_id_to_shard_id(receipt.receiver_id());
                return Ok(Some((HashType::Receipt, to_shard)));
            }
        }
    }
    Ok(None)
}

fn apply_tx_in_block(
    epoch_manager: &EpochManagerHandle,
    runtime: &dyn RuntimeAdapter,
    chain_store: &ChainStore,
    tx_hash: &CryptoHash,
    block_hash: CryptoHash,
    storage: StorageSource,
) -> anyhow::Result<ApplyChunkResult> {
    match find_tx_or_receipt(tx_hash, &block_hash, epoch_manager, chain_store)? {
        Some((hash_type, shard_id)) => match hash_type {
            HashType::Tx => {
                println!(
                    "Found tx in block {} shard {}. equivalent command:\nview_state apply --height {} --shard-id {}\n",
                    &block_hash,
                    shard_id,
                    chain_store.get_block_header(&block_hash)?.height(),
                    shard_id
                );
                let (block, apply_result) = crate::commands::apply_block(
                    block_hash,
                    shard_id,
                    epoch_manager,
                    runtime,
                    chain_store,
                    storage,
                );
                check_apply_block_result(
                    &block,
                    &apply_result,
                    epoch_manager,
                    chain_store,
                    shard_id,
                )?;
                Ok(apply_result)
            }
            HashType::Receipt => Err(anyhow!(
                "{} appears to be a Receipt ID, not a tx hash. Try running:\nview_state apply_receipt --hash {}",
                tx_hash,
                tx_hash
            )),
        },
        None => Err(anyhow!(
            "Could not find tx with hash {} in block {}, even though `DBCol::TransactionResultForBlock` says it should be there",
            tx_hash,
            block_hash
        )),
    }
}

fn apply_tx_in_chunk(
    epoch_manager: &EpochManagerHandle,
    runtime: &dyn RuntimeAdapter,
    store: Store,
    chain_store: &mut ChainStore,
    tx_hash: &CryptoHash,
    storage: StorageSource,
) -> anyhow::Result<Vec<ApplyChunkResult>> {
    if chain_store.get_transaction(tx_hash)?.is_none() {
        return Err(anyhow!("tx with hash {} not known", tx_hash));
    }

    println!(
        "Transaction is known but doesn't seem to have been applied. Searching in chunks that haven't been applied..."
    );

    let head = chain_store.head()?.height;
    let mut chunk_hashes = vec![];

    for item in store.iter(DBCol::ChunkHashesByHeight) {
        let (k, v) = item.context("scanning ChunkHashesByHeight column")?;
        let height = BlockHeight::from_le_bytes(k[..].try_into().unwrap());
        if height > head {
            let hashes = HashSet::<ChunkHash>::try_from_slice(&v).unwrap();
            for chunk_hash in hashes {
                let chunk = match chain_store.get_chunk(&chunk_hash) {
                    Ok(c) => c,
                    Err(_) => {
                        tracing::warn!(target: "state-viewer", "chunk hash {:?} appears in DBCol::ChunkHashesByHeight but the chunk is not saved", &chunk_hash);
                        continue;
                    }
                };
                for hash in chunk.to_transactions().iter().map(|tx| tx.get_hash()) {
                    if hash == *tx_hash {
                        chunk_hashes.push(chunk_hash);
                        break;
                    }
                }
            }
        }
    }

    if chunk_hashes.is_empty() {
        return Err(anyhow!(
            "Could not find tx with hash {} in any chunk that hasn't been applied yet",
            tx_hash
        ));
    }

    let mut results = Vec::new();
    for chunk_hash in chunk_hashes {
        println!(
            "found tx in chunk {}. Equivalent command (which will run faster than apply_tx):\nview_state apply_chunk --chunk_hash {}\n",
            &chunk_hash.0, &chunk_hash.0
        );
        let (apply_result, gas_limit) =
            apply_chunk(epoch_manager, runtime, chain_store, chunk_hash, None, None, storage)?;
        println!("resulting chunk extra:\n{:?}", resulting_chunk_extra(&apply_result, gas_limit));
        results.push(apply_result);
    }
    Ok(results)
}

pub fn apply_tx(
    genesis_config: &near_chain_configs::GenesisConfig,
    epoch_manager: &EpochManagerHandle,
    runtime: &dyn RuntimeAdapter,
    store: Store,
    tx_hash: CryptoHash,
    storage: StorageSource,
) -> anyhow::Result<Vec<ApplyChunkResult>> {
    let mut chain_store =
        ChainStore::new(store.clone(), false, genesis_config.transaction_validity_period);
    let outcomes = chain_store.get_outcomes_by_id(&tx_hash)?;

    if let Some(outcome) = outcomes.first() {
        Ok(vec![apply_tx_in_block(
            epoch_manager,
            runtime,
            &mut chain_store,
            &tx_hash,
            outcome.block_hash,
            storage,
        )?])
    } else {
        apply_tx_in_chunk(epoch_manager, runtime, store, &mut chain_store, &tx_hash, storage)
    }
}

fn apply_receipt_in_block(
    epoch_manager: &EpochManagerHandle,
    runtime: &dyn RuntimeAdapter,
    chain_store: &ChainStore,
    id: &CryptoHash,
    block_hash: CryptoHash,
    storage: StorageSource,
) -> anyhow::Result<ApplyChunkResult> {
    match find_tx_or_receipt(id, &block_hash, epoch_manager, chain_store)? {
        Some((hash_type, shard_id)) => match hash_type {
            HashType::Tx => Err(anyhow!(
                "{} appears to be a tx hash, not a Receipt ID. Try running:\nview_state apply_tx --hash {}",
                id,
                id
            )),
            HashType::Receipt => {
                println!(
                    "Found receipt in block {}. Receiver is in shard {}. equivalent command:\nview_state apply --height {} --shard-id {}\n",
                    &block_hash,
                    shard_id,
                    chain_store.get_block_header(&block_hash)?.height(),
                    shard_id
                );
                let (block, apply_result) = crate::commands::apply_block(
                    block_hash,
                    shard_id,
                    epoch_manager,
                    runtime,
                    chain_store,
                    storage,
                );
                check_apply_block_result(
                    &block,
                    &apply_result,
                    epoch_manager,
                    chain_store,
                    shard_id,
                )?;
                Ok(apply_result)
            }
        },
        None => {
            // TODO: handle local/delayed receipts
            Err(anyhow!(
                "Could not find receipt with ID {} in block {}. Is it a local or delayed receipt?",
                id,
                block_hash
            ))
        }
    }
}

fn apply_receipt_in_chunk(
    epoch_manager: &EpochManagerHandle,
    runtime: &dyn RuntimeAdapter,
    store: Store,
    chain_store: &mut ChainStore,
    id: &CryptoHash,
    storage: StorageSource,
) -> anyhow::Result<Vec<ApplyChunkResult>> {
    println!("Receipt is not indexed; searching in chunks that haven't been applied...");

    let head = chain_store.head()?.height;
    let mut to_apply = HashSet::new();
    let mut non_applied_chunks = HashMap::new();

    for item in store.iter(DBCol::ChunkHashesByHeight) {
        let (k, v) = item.context("scanning ChunkHashesByHeight column")?;
        let height = BlockHeight::from_le_bytes(k[..].try_into().unwrap());
        if height > head {
            let hashes = HashSet::<ChunkHash>::try_from_slice(&v).unwrap();
            for chunk_hash in hashes {
                let chunk = match chain_store.get_chunk(&chunk_hash) {
                    Ok(c) => c,
                    Err(_) => {
                        tracing::warn!(target: "state-viewer", "chunk hash {:?} appears in DBCol::ChunkHashesByHeight but the chunk is not saved", &chunk_hash);
                        continue;
                    }
                };
                non_applied_chunks.insert((height, chunk.shard_id()), chunk_hash.clone());

                for receipt in chunk.prev_outgoing_receipts() {
                    if receipt.get_hash() == *id {
                        let shard_layout =
                            epoch_manager.get_shard_layout_from_prev_block(chunk.prev_block())?;
                        let to_shard = shard_layout.account_id_to_shard_id(receipt.receiver_id());
                        to_apply.insert((height, to_shard));
                        println!(
                            "found receipt in chunk {}. Receiver is in shard {}",
                            &chunk_hash.0, to_shard
                        );
                        break;
                    }
                }
            }
        }
    }

    if to_apply.is_empty() {
        return Err(anyhow!(
            "Could not find receipt with hash {} in any chunk that hasn't been applied yet",
            id
        ));
    }

    let mut results = Vec::new();
    for (height, shard_id) in to_apply {
        let chunk_hash = match non_applied_chunks.get(&(height, shard_id)) {
            Some(h) => h,
            None => {
                eprintln!(
                    "Wanted to apply chunk in shard {} at height {}, but no such chunk was found.",
                    shard_id, height,
                );
                continue;
            }
        };
        println!(
            "Applying chunk at height {} in shard {}. Equivalent command (which will run faster than apply_receipt):\nview_state apply_chunk --chunk_hash {}\n",
            height, shard_id, chunk_hash.0
        );
        let (apply_result, gas_limit) = apply_chunk(
            epoch_manager,
            runtime,
            chain_store,
            chunk_hash.clone(),
            None,
            None,
            storage,
        )?;
        let chunk_extra = resulting_chunk_extra(&apply_result, gas_limit);
        println!("resulting chunk extra:\n{:?}", chunk_extra);
        results.push(apply_result);
    }
    Ok(results)
}

pub fn apply_receipt(
    genesis_config: &near_chain_configs::GenesisConfig,
    epoch_manager: &EpochManagerHandle,
    runtime: &dyn RuntimeAdapter,
    store: Store,
    id: CryptoHash,
    storage: StorageSource,
) -> anyhow::Result<Vec<ApplyChunkResult>> {
    let mut chain_store =
        ChainStore::new(store.clone(), false, genesis_config.transaction_validity_period);
    let outcomes = chain_store.get_outcomes_by_id(&id)?;
    if let Some(outcome) = outcomes.first() {
        Ok(vec![apply_receipt_in_block(
            epoch_manager,
            runtime,
            &mut chain_store,
            &id,
            outcome.block_hash,
            storage,
        )?])
    } else {
        apply_receipt_in_chunk(epoch_manager, runtime, store, &mut chain_store, &id, storage)
    }
}
