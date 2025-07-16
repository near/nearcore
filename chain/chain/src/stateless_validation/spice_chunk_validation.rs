use std::collections::HashMap;
use std::sync::Arc;

use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block::Block;
use near_primitives::hash::hash;
use near_primitives::merkle::merklize;
use near_primitives::receipt::Receipt;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::sharding::{ChunkHash, ReceiptProof};
use near_primitives::stateless_validation::state_witness::ChunkStateWitness;
use near_primitives::types::{BlockExecutionResults, ShardId};
use near_store::PartialStorage;
use near_store::adapter::StoreAdapter as _;
use node_runtime::SignedValidPeriodTransactions;

use crate::chain::{NewChunkData, StorageContext};
use crate::sharding::{get_receipts_shuffle_salt, shuffle_receipt_proofs};
use crate::store::filter_incoming_receipts_for_shard;
use crate::types::StorageDataSource;
use crate::{Chain, ChainStore};

use super::chunk_validation::{MainTransition, PreValidationOutput, validate_receipt_proof};

pub fn spice_pre_validate_chunk_state_witness(
    state_witness: &ChunkStateWitness,
    block: &Block,
    prev_block: &Block,
    prev_execution_results: &BlockExecutionResults,
    epoch_manager: &dyn EpochManagerAdapter,
    store: &ChainStore,
) -> Result<PreValidationOutput, Error> {
    let epoch_id = epoch_manager.get_epoch_id(block.header().hash())?;

    if &epoch_id != state_witness.epoch_id() {
        return Err(Error::InvalidChunkStateWitness(format!(
            "Epoch id in block ({:?}) {:?} does not match epoch id in witness {:?}",
            block.header().hash(),
            epoch_id,
            state_witness.epoch_id(),
        )));
    }
    let chunk_header = state_witness.chunk_header().clone();
    if !block.chunks().contains(&chunk_header) {
        return Err(Error::InvalidChunkStateWitness(format!(
            "Block {:?} doesn't contain state witness' chunk header with hash {:?}",
            block.header().hash(),
            chunk_header.chunk_hash(),
        )));
    }

    // Ensure that the chunk header version is supported in this protocol version
    let protocol_version = epoch_manager.get_epoch_info(&epoch_id)?.protocol_version();
    chunk_header.validate_version(protocol_version)?;

    let prev_block_header = prev_block.header();

    let shard_id = chunk_header.shard_id();
    let shard_layout = epoch_manager.get_shard_layout(&epoch_id)?;

    // TODO(spice-resharding): Populate implicit_transition_params when resharding. See
    // get_resharding_transition in c/c/s/stateless_validation/chunk_validation.rs
    let implicit_transition_params = Vec::new();

    let receipts_to_apply = validate_source_receipts_proofs(
        &state_witness.source_receipt_proofs(),
        prev_execution_results,
        &shard_layout,
        shard_id,
        &prev_block,
        &block,
        epoch_manager,
    )?;
    let applied_receipts_hash = hash(&borsh::to_vec(receipts_to_apply.as_slice()).unwrap());
    if &applied_receipts_hash != state_witness.applied_receipts_hash() {
        return Err(Error::InvalidChunkStateWitness(format!(
            "Receipts hash {:?} does not match expected receipts hash {:?}",
            applied_receipts_hash,
            state_witness.applied_receipts_hash()
        )));
    }
    let (tx_root_from_state_witness, _) = merklize(&state_witness.transactions());
    if chunk_header.tx_root() != &tx_root_from_state_witness {
        return Err(Error::InvalidChunkStateWitness(format!(
            "Transaction root {:?} does not match expected transaction root {:?}",
            tx_root_from_state_witness,
            chunk_header.tx_root()
        )));
    }

    let transaction_validity_check_results = state_witness
        .transactions()
        .iter()
        .map(|tx| {
            store
                .check_transaction_validity_period(&prev_block_header, tx.transaction.block_hash())
                .is_ok()
        })
        .collect::<Vec<_>>();

    // Chunk executor actor doesn't execute genesis so there's no need to handle respective
    // witnesses. Execution results for genesis can be calculated on each node on their own.
    if block.header().is_genesis() {
        return Err(Error::InvalidChunkStateWitness(
            "State witness is for genesis block".to_string(),
        ));
    }

    let main_transition_params = {
        // For correct application we need to convert chunk_header into spice_chunk_header.
        let spice_chunk_header = if prev_block_header.is_genesis() {
            let prev_block_epoch_id = prev_block_header.epoch_id();
            let prev_block_shard_layout = epoch_manager.get_shard_layout(&prev_block_epoch_id)?;
            let chunk_extra = Chain::build_genesis_chunk_extra(
                &store.store(),
                &prev_block_shard_layout,
                shard_id,
                &prev_block,
            )?;
            chunk_header.into_spice_chunk_execution_header(&chunk_extra)
        } else {
            let prev_chunk_header = epoch_manager.get_prev_chunk_header(&prev_block, shard_id)?;
            let prev_execution_result = prev_execution_results
                .0
                .get(prev_chunk_header.chunk_hash())
                .expect("execution results for all prev_block chunks should be available");
            chunk_header.into_spice_chunk_execution_header(&prev_execution_result.chunk_extra)
        };

        let storage_context = StorageContext {
            storage_data_source: StorageDataSource::Recorded(PartialStorage {
                nodes: state_witness.main_state_transition().base_state.clone(),
            }),
            state_patch: Default::default(),
        };
        let is_new_chunk = true;
        MainTransition::NewChunk {
            new_chunk_data: NewChunkData {
                chunk_header: spice_chunk_header,
                transactions: SignedValidPeriodTransactions::new(
                    state_witness.transactions().clone(),
                    transaction_validity_check_results,
                ),
                receipts: receipts_to_apply,
                block: Chain::get_apply_chunk_block_context(
                    &block,
                    &prev_block_header,
                    is_new_chunk,
                )?,
                storage_context,
            },
            block_hash: *block.header().hash(),
        }
    };

    Ok(PreValidationOutput { main_transition_params, implicit_transition_params })
}

fn validate_source_receipts_proofs(
    source_receipt_proofs: &HashMap<ChunkHash, ReceiptProof>,
    prev_execution_results: &BlockExecutionResults,
    shard_layout: &ShardLayout,
    shard_id: ShardId,
    prev_block: &Block,
    block: &Block,
    epoch_manager: &dyn EpochManagerAdapter,
) -> Result<Vec<Receipt>, Error> {
    if prev_block.header().is_genesis() {
        if !source_receipt_proofs.is_empty() {
            return Err(Error::InvalidChunkStateWitness(format!(
                "genesis source_receipt_proofs should be empty, actual len is {}",
                source_receipt_proofs.len()
            )));
        }
        return Ok(vec![]);
    }

    if source_receipt_proofs.len() != prev_block.chunks().len() {
        return Err(Error::InvalidChunkStateWitness(format!(
            "source_receipt_proofs contains incorrect number of proofs. Expected {} proofs, found {}",
            prev_execution_results.0.len(),
            prev_block.chunks().len(),
        )));
    }

    let mut receipt_proofs = Vec::new();
    for chunk in prev_block.chunks().iter_raw() {
        let chunk_hash = chunk.chunk_hash();
        let prev_execution_result = prev_execution_results
            .0
            .get(chunk_hash)
            .expect("execution results for all prev_block chunks should be available");
        let Some(receipt_proof) = source_receipt_proofs.get(&chunk_hash) else {
            return Err(Error::InvalidChunkStateWitness(format!(
                "Missing source receipt proof for chunk {:?}",
                chunk_hash
            )));
        };

        validate_receipt_proof(
            receipt_proof,
            chunk,
            shard_id,
            prev_execution_result.outgoing_receipts_root,
        )?;

        receipt_proofs.push(receipt_proof.clone());
    }
    // TODO(spice): In chunk executor actor order of the receipts before shuffling is determined by
    // the way in which we retrieve them from the trie. At the moment they are retrieved sorted by
    // from_shard_id. To keep behaviour as similar as possible with what we had before spice, chunk
    // executor actor should process receipts in order in which respective chunks appear in block.
    receipt_proofs.sort_by_key(|proof| proof.1.from_shard_id);

    receipt_proofs =
        filter_incoming_receipts_for_shard(shard_layout, shard_id, Arc::new(receipt_proofs))?;

    let receipts_shuffle_salt = get_receipts_shuffle_salt(epoch_manager, &block)?;
    shuffle_receipt_proofs(&mut receipt_proofs, receipts_shuffle_salt);
    Ok(receipt_proofs.into_iter().map(|proof| proof.0).flatten().collect())
}
