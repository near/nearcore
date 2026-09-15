use crate::chain::{NewChunkData, NewChunkResult, ShardContext, StorageContext, apply_new_chunk};
use crate::sharding::{get_receipts_shuffle_salt, shuffle_receipt_proofs};
use crate::spice::boundary::{anchor_and_replay_blocks, boundary_source_blocks_for_target};
use crate::spice::chunk_application::build_spice_apply_chunk_block_context;
use crate::store::filter_incoming_receipts_for_shard;
use crate::types::MaybePinnedMemtrieRoot;
use crate::types::{ApplyChunkBlockContext, RuntimeAdapter, StorageDataSource};
use crate::update_shard::{OldChunkData, OldChunkResult, apply_old_chunk};
use crate::validate::validate_chunk_proofs;
use crate::{Chain, ChainStore};
use itertools::Itertools;
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_primitives::apply::ApplyChunkReason;
use near_primitives::block::Block;
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::merkle::merklize;
use near_primitives::receipt::Receipt;
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::sharding::{
    ChunkHash, EncodedShardChunk, EncodedShardChunkBody, EncodedShardChunkV2, ReceiptProof,
    ShardChunkHeader,
};
use near_primitives::spice::state_witness::SpiceChunkStateWitness;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{BlockExecutionResults, ChunkExecutionResult, ShardId};
use near_store::PartialStorage;
use node_runtime::SignedValidPeriodTransactions;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::Span;

pub struct SpicePreValidationOutput {
    new_chunk_data: NewChunkData,
    /// Contexts for the old-chunk replays of a boundary witness whose chunk is
    /// missing in its block, oldest first, derived from on-chain blocks. Empty
    /// otherwise. See `SpiceBoundaryWitnessData::implicit_transitions`.
    implicit_transition_params: Vec<(ApplyChunkBlockContext, ShardUId)>,
}

pub fn spice_pre_validate_chunk_state_witness(
    state_witness: &SpiceChunkStateWitness,
    block: &Block,
    prev_execution_results: &BlockExecutionResults,
    epoch_manager: &dyn EpochManagerAdapter,
    store: &ChainStore,
    prev_validator_proposals: Vec<ValidatorStake>,
) -> Result<SpicePreValidationOutput, Error> {
    assert_eq!(block.hash(), &state_witness.chunk_id().block_hash);
    let epoch_id = epoch_manager.get_epoch_id(block.header().hash())?;
    let shard_id = state_witness.chunk_id().shard_id;

    let shard_layout = epoch_manager.get_shard_layout(&epoch_id)?;
    if !shard_layout.shard_ids().contains(&shard_id) {
        return Err(Error::InvalidChunkStateWitness(format!(
            "Shard layout for block's ({:?}) epoch ({:?}) doesn't contain witness shard {:?}",
            block.hash(),
            epoch_id,
            shard_id
        )));
    }

    // Chunk executor actor doesn't execute genesis so there's no need to handle respective
    // witnesses. Execution results for genesis can be calculated on each node on their own.
    if block.header().is_genesis() {
        return Err(Error::InvalidChunkStateWitness(
            "State witness is for genesis block".to_string(),
        ));
    }

    let chunks = block.chunks();
    let shard_index = shard_layout.get_shard_index(shard_id).unwrap();
    let chunk_header = chunks.get(shard_index).unwrap();

    // Under spice a missing chunk is an empty new chunk, so only a pre-spice block
    // can anchor earlier than itself.
    let (anchor_block, replay_blocks) = if block.is_spice_block() {
        (store.get_block(block.hash())?, Vec::new())
    } else {
        anchor_and_replay_blocks(store, epoch_manager, block, shard_id)?
    };
    let anchor_prev_block = store.get_block(anchor_block.header().prev_hash())?;
    let anchor_epoch_id = epoch_manager.get_epoch_id(anchor_block.header().hash())?;
    let anchor_shard_layout = epoch_manager.get_shard_layout(&anchor_epoch_id)?;

    let mut implicit_transition_params = Vec::with_capacity(replay_blocks.len());
    for replay_block in &replay_blocks {
        let replay_prev_header = store.get_block_header(replay_block.header().prev_hash())?;
        let block_context =
            Chain::get_apply_chunk_block_context(replay_block, &replay_prev_header, false);
        let replay_epoch_id = epoch_manager.get_epoch_id(replay_block.header().hash())?;
        let shard_uid = shard_id_to_uid(epoch_manager, shard_id, &replay_epoch_id)?;
        implicit_transition_params.push((block_context, shard_uid));
    }

    // Ensure that the chunk header version is supported in this protocol version
    if chunk_header.is_new_chunk(anchor_block.header().height()) {
        let protocol_version = epoch_manager.get_epoch_info(&anchor_epoch_id)?.protocol_version();
        chunk_header.validate_version(protocol_version)?;
    }

    let prev_block_header = anchor_prev_block.header();

    // TODO(spice-resharding): Handle resharding, same as part of implicit_transition_params in
    // non-spice validation. See
    // get_resharding_transition in c/c/s/stateless_validation/chunk_validation.rs

    let receipts_to_apply = if !block.is_spice_block() {
        let Some(boundary) = state_witness.boundary() else {
            return Err(Error::InvalidChunkStateWitness(
                "witness of a pre-spice block carries no boundary data".to_string(),
            ));
        };
        if !state_witness.source_receipt_proofs().is_empty() {
            return Err(Error::InvalidChunkStateWitness(
                "boundary witness carries spice source receipt proofs".to_string(),
            ));
        }
        let source_blocks =
            boundary_source_blocks_for_target(store, epoch_manager, &anchor_block, shard_id)?;
        validate_boundary_source_receipts_proofs(
            &boundary.source_receipt_proofs,
            &source_blocks,
            &anchor_shard_layout,
            shard_id,
        )?
    } else {
        if state_witness.boundary().is_some() {
            return Err(Error::InvalidChunkStateWitness(
                "witness of a spice block carries boundary data".to_string(),
            ));
        }
        validate_source_receipts_proofs(
            &state_witness.source_receipt_proofs(),
            prev_execution_results,
            &anchor_shard_layout,
            shard_id,
            &anchor_prev_block,
            &anchor_block,
            epoch_manager,
        )?
    };
    let applied_receipts_hash = hash(&borsh::to_vec(receipts_to_apply.as_slice()).unwrap());
    if &applied_receipts_hash != state_witness.applied_receipts_hash() {
        return Err(Error::InvalidChunkStateWitness(format!(
            "Receipts hash {:?} does not match expected receipts hash {:?}",
            applied_receipts_hash,
            state_witness.applied_receipts_hash()
        )));
    }
    if let Some(proof) = state_witness.proof_of_invalid_chunk() {
        if !state_witness.transactions().is_empty() {
            return Err(Error::InvalidChunkStateWitness(
                "proof_of_invalid_chunk provided with non-empty transactions".to_string(),
            ));
        }
        if !chunk_header.is_new_chunk(anchor_block.header().height()) {
            return Err(Error::InvalidChunkStateWitness(
                "proof_of_invalid_chunk provided for non-new chunk".to_string(),
            ));
        }
        verify_proof_of_invalid_chunk(proof, chunk_header, epoch_manager)?;
    } else {
        let (tx_root_from_state_witness, _) = merklize(&state_witness.transactions());
        let chunk_tx_root = if chunk_header.is_new_chunk(anchor_block.header().height()) {
            *chunk_header.tx_root()
        } else {
            // Missing chunks are treated as empty chunks.
            let (empty_txs_root, _) = merklize::<SignedTransaction>(&[]);
            empty_txs_root
        };
        if chunk_tx_root != tx_root_from_state_witness {
            return Err(Error::InvalidChunkStateWitness(format!(
                "Transaction root {:?} does not match expected transaction root {:?}",
                tx_root_from_state_witness, chunk_tx_root
            )));
        }
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

    let new_chunk_data = {
        let prev_chunk_chunk_extra = {
            let (_, prev_shard_id, _prev_shard_index) = epoch_manager
                .get_prev_shard_id_from_prev_hash(anchor_prev_block.hash(), shard_id)?;
            let prev_execution_result = prev_execution_results
                .0
                .get(&prev_shard_id)
                .expect("execution results for all prev_block chunks should be available");
            &prev_execution_result.chunk_extra
        };

        let storage_context = StorageContext {
            storage_data_source: StorageDataSource::Recorded(PartialStorage {
                nodes: state_witness.pre_state().clone(),
            }),
            state_patch: Default::default(),
        };
        let block_context = if !block.is_spice_block() {
            Chain::get_apply_chunk_block_context(&anchor_block, anchor_prev_block.header(), true)
        } else {
            build_spice_apply_chunk_block_context(
                block.header(),
                prev_execution_results,
                epoch_manager,
            )?
        };
        NewChunkData {
            gas_limit: prev_chunk_chunk_extra.gas_limit(),
            prev_state_root: *prev_chunk_chunk_extra.state_root(),
            prev_validator_proposals,
            chunk_hash: if chunk_header.is_new_chunk(anchor_block.header().height()) {
                Some(chunk_header.chunk_hash().clone())
            } else {
                None
            },
            transactions: SignedValidPeriodTransactions::new(
                state_witness.transactions().to_vec(),
                transaction_validity_check_results,
            ),
            receipts: receipts_to_apply,
            block: block_context,
            storage_context,
        }
    };

    Ok(SpicePreValidationOutput { new_chunk_data, implicit_transition_params })
}

#[tracing::instrument(
    level = tracing::Level::DEBUG,
    skip_all,
    target = "spice_chunk_validator",
    fields(
        chunk_id = ?state_witness.chunk_id(),
    )
)]
pub fn spice_validate_chunk_state_witness(
    state_witness: SpiceChunkStateWitness,
    pre_validation_output: SpicePreValidationOutput,
    epoch_manager: &dyn EpochManagerAdapter,
    runtime_adapter: &dyn RuntimeAdapter,
) -> Result<ChunkExecutionResult, Error> {
    let chunk_id = state_witness.chunk_id();
    let _timer = crate::stateless_validation::metrics::CHUNK_STATE_WITNESS_VALIDATION_TIME
        .with_label_values(&[&chunk_id.shard_id.to_string()])
        .start_timer();

    let block_hash = &chunk_id.block_hash;
    let shard_id = chunk_id.shard_id;
    let epoch_id = epoch_manager.get_epoch_id(block_hash)?;
    let shard_uid = shard_id_to_uid(epoch_manager, shard_id, &epoch_id)?;

    // TODO(spice): Similar to non-spice validation consider using cache to avoid re-evaluating
    // the same witnesses.
    let (chunk_extra, outgoing_receipts) = {
        let gas_limit = pre_validation_output.new_chunk_data.gas_limit;
        let NewChunkResult { apply_result: mut main_apply_result, .. } = apply_new_chunk(
            ApplyChunkReason::ValidateChunkStateWitness,
            &Span::current(),
            pre_validation_output.new_chunk_data,
            ShardContext { shard_uid, should_apply_chunk: true },
            runtime_adapter,
            // Recorded-storage replay; no memtrie path.
            MaybePinnedMemtrieRoot::no_memtries(),
            None,
        )?;
        let outgoing_receipts = std::mem::take(&mut main_apply_result.outgoing_receipts);
        let chunk_extra = main_apply_result.to_chunk_extra(gas_limit);

        (chunk_extra, outgoing_receipts)
    };

    // Replay the boundary witness's implicit old-chunk transitions on top of the anchor transition
    let implicit_transition_params = pre_validation_output.implicit_transition_params;
    let implicit_transitions = state_witness
        .boundary()
        .map_or(&[][..], |boundary| boundary.implicit_transitions.as_slice());
    if implicit_transition_params.len() != implicit_transitions.len() {
        return Err(Error::InvalidChunkStateWitness(format!(
            "Implicit transitions count mismatch. Expected {}, found {}",
            implicit_transition_params.len(),
            implicit_transitions.len(),
        )));
    }
    let mut chunk_extra = chunk_extra;
    for ((block_context, transition_shard_uid), transition) in
        implicit_transition_params.into_iter().zip(implicit_transitions)
    {
        let transition_block_hash = transition.block_hash;
        let old_chunk_data = OldChunkData {
            prev_chunk_extra: chunk_extra.clone(),
            block: block_context,
            storage_context: StorageContext {
                storage_data_source: StorageDataSource::Recorded(PartialStorage {
                    nodes: transition.base_state.clone(),
                }),
                state_patch: Default::default(),
            },
        };
        let OldChunkResult { apply_result, .. } = apply_old_chunk(
            ApplyChunkReason::ValidateChunkStateWitness,
            &Span::current(),
            old_chunk_data,
            ShardContext { shard_uid: transition_shard_uid, should_apply_chunk: false },
            runtime_adapter,
            // Recorded-storage replay; no memtrie path.
            MaybePinnedMemtrieRoot::no_memtries(),
        )?;
        chunk_extra = chunk_extra.next_for_old_chunk(apply_result.new_root);
        if chunk_extra.state_root() != &transition.post_state_root {
            return Err(Error::InvalidChunkStateWitness(format!(
                "Post state root {:?} for implicit transition at block {:?} does not match expected state root {:?}",
                chunk_extra.state_root(),
                transition_block_hash,
                transition.post_state_root,
            )));
        }
    }

    // TODO(spice-resharding): Handle possible resharding transitions.

    let shard_layout = epoch_manager.get_shard_layout(&epoch_id)?;
    let outgoing_receipts_hashes = Chain::build_receipts_hashes(&outgoing_receipts, &shard_layout)?;
    let (outgoing_receipts_root, _) = merklize(&outgoing_receipts_hashes);

    let execution_result = ChunkExecutionResult { chunk_extra, outgoing_receipts_root };
    Ok(execution_result)
}

/// Verifies that the given `body` proves the chunk (identified by `chunk_header`)
/// is invalid. Accepts the proof if the body decodes to an invalid chunk;
/// rejects it if the chunk is actually valid (fraudulent proof).
fn verify_proof_of_invalid_chunk(
    body: &EncodedShardChunkBody,
    chunk_header: &ShardChunkHeader,
    epoch_manager: &dyn EpochManagerAdapter,
) -> Result<(), Error> {
    // Reject bodies with missing parts to avoid panicking in get_merkle_hash_and_paths.
    if body.parts.iter().any(|p| p.is_none()) {
        return Err(Error::InvalidChunkStateWitness(
            "proof_of_invalid_chunk body contains missing parts".to_string(),
        ));
    }
    // Verify the body is consistent with the chunk header's encoded_merkle_root.
    let (body_merkle_root, _) = body.get_merkle_hash_and_paths();
    if &body_merkle_root != chunk_header.encoded_merkle_root() {
        return Err(Error::InvalidChunkStateWitness(format!(
            "proof_of_invalid_chunk body encoded_merkle_root {:?} does not match \
             chunk header encoded_merkle_root {:?}",
            body_merkle_root,
            chunk_header.encoded_merkle_root()
        )));
    }

    // Attempt to decode the body.
    let encoded_chunk = EncodedShardChunk::V2(EncodedShardChunkV2 {
        header: chunk_header.clone(),
        content: body.clone(),
    });
    let shard_chunk = match encoded_chunk.decode_chunk() {
        Ok(chunk) => chunk,
        // Decode failure is sufficient proof of invalidity.
        Err(_) => return Ok(()),
    };

    // Attempt to validate the decoded chunk's proofs.
    let is_valid = validate_chunk_proofs(&shard_chunk, epoch_manager)?;
    if is_valid {
        return Err(Error::InvalidChunkStateWitness(
            "proof_of_invalid_chunk is fraudulent: chunk body is actually valid".to_string(),
        ));
    }

    Ok(())
}

/// Boundary-witness counterpart of [`validate_source_receipts_proofs`], shaped like
/// the pre-spice validation: the anchor's application consumed the incoming receipts
/// of every block from the target shard's previous inclusion (exclusive) through the
/// anchor (inclusive).
fn validate_boundary_source_receipts_proofs(
    source_receipt_proofs: &HashMap<ChunkHash, ReceiptProof>,
    source_blocks: &[Arc<Block>],
    shard_layout: &ShardLayout,
    target_shard_id: ShardId,
) -> Result<Vec<Receipt>, Error> {
    let mut receipts = Vec::new();
    let mut expected_proofs = 0;
    for source_block in source_blocks {
        let mut block_proofs = Vec::new();
        for chunk_header in source_block.chunks().iter_new() {
            let Some(receipt_proof) = source_receipt_proofs.get(&chunk_header.chunk_hash()) else {
                return Err(Error::InvalidChunkStateWitness(format!(
                    "Missing source receipt proof for chunk {:?}",
                    chunk_header.chunk_hash()
                )));
            };
            validate_receipt_proof(
                receipt_proof,
                chunk_header.shard_id(),
                target_shard_id,
                *chunk_header.prev_outgoing_receipts_root(),
            )?;
            expected_proofs += 1;
            block_proofs.push(receipt_proof.clone());
        }

        let mut block_proofs = filter_incoming_receipts_for_shard(
            shard_layout,
            target_shard_id,
            Arc::new(block_proofs),
        )?;
        shuffle_receipt_proofs(&mut block_proofs, get_receipts_shuffle_salt(source_block));
        receipts.extend(block_proofs.into_iter().map(|proof| proof.0).flatten());
    }

    if source_receipt_proofs.len() != expected_proofs {
        return Err(Error::InvalidChunkStateWitness(format!(
            "source_receipt_proofs contains too many proofs. Expected {} proofs, found {}",
            expected_proofs,
            source_receipt_proofs.len(),
        )));
    }
    Ok(receipts)
}

fn validate_source_receipts_proofs(
    source_receipt_proofs: &HashMap<ShardId, ReceiptProof>,
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

    let prev_block_shard_layout = epoch_manager.get_shard_layout(prev_block.header().epoch_id())?;

    if source_receipt_proofs.len() as u64 != prev_block_shard_layout.num_shards() {
        return Err(Error::InvalidChunkStateWitness(format!(
            "source_receipt_proofs contains incorrect number of proofs. Expected {} proofs, found {}",
            source_receipt_proofs.len(),
            prev_block_shard_layout.num_shards(),
        )));
    }

    let mut receipt_proofs = Vec::new();
    for prev_block_shard_id in prev_block_shard_layout.shard_ids() {
        let prev_execution_result = prev_execution_results
            .0
            .get(&prev_block_shard_id)
            .expect("execution results for all prev_block shards should be available");
        let Some(receipt_proof) = source_receipt_proofs.get(&prev_block_shard_id) else {
            return Err(Error::InvalidChunkStateWitness(format!(
                "Missing source receipt proof for shard {:?}",
                prev_block_shard_id
            )));
        };

        let from_shard_id = prev_block_shard_id;
        let target_shard_id = shard_id;
        validate_receipt_proof(
            receipt_proof,
            from_shard_id,
            target_shard_id,
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

    shuffle_receipt_proofs(&mut receipt_proofs, get_receipts_shuffle_salt(&block));
    Ok(receipt_proofs.into_iter().map(|proof| proof.0).flatten().collect())
}

fn validate_receipt_proof(
    receipt_proof: &ReceiptProof,
    from_shard_id: ShardId,
    target_chunk_shard_id: ShardId,
    outgoing_receipts_root: CryptoHash,
) -> Result<(), Error> {
    if receipt_proof.1.from_shard_id != from_shard_id {
        return Err(Error::InvalidChunkStateWitness(format!(
            "Receipt proof is from shard {}, expected shard {}",
            receipt_proof.1.from_shard_id, from_shard_id,
        )));
    }
    if receipt_proof.1.to_shard_id != target_chunk_shard_id {
        return Err(Error::InvalidChunkStateWitness(format!(
            "Receipt proof from shard {} is for shard {}, expected shard {}",
            from_shard_id, receipt_proof.1.to_shard_id, target_chunk_shard_id
        )));
    }
    if !receipt_proof.verify_against_receipt_root(outgoing_receipts_root) {
        return Err(Error::InvalidChunkStateWitness(format!(
            "Receipt proof from shard {} has invalid merkle path, doesn't match outgoing receipts root",
            from_shard_id
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::ChainStoreAccess;
    use crate::test_utils::{get_chain_with_genesis, process_block_sync};
    use crate::{BlockProcessingArtifact, Provenance};
    use near_async::time::Clock;
    use near_chain_configs::test_genesis::{TestGenesisBuilder, ValidatorsSpec};
    use near_o11y::testonly::init_test_logger;
    use near_primitives::hash::CryptoHash;
    use near_primitives::reed_solomon::reed_solomon_encode;
    use near_primitives::sharding::{
        EncodedShardChunkBody, ShardChunkHeader, ShardChunkHeaderV3, TransactionReceipt,
    };
    use near_primitives::state::PartialState;
    use near_primitives::stateless_validation::ChunkProductionKey;
    use near_primitives::stateless_validation::contract_distribution::CodeHash;
    use near_primitives::test_utils::{
        TestBlockBuilder, create_test_signer, create_user_test_signer,
    };
    use near_primitives::transaction::SignedTransaction;
    use near_primitives::types::Balance;
    use near_primitives::types::chunk_extra::ChunkExtra;
    use near_primitives::types::{AccountId, BlockHeight, ChunkExecutionResult, SpiceChunkId};
    use near_primitives::validator_signer::ValidatorSigner;
    use near_store::get_genesis_state_roots;
    use reed_solomon_erasure::galois_8::ReedSolomon;
    use std::collections::BTreeSet;
    use std::str::FromStr as _;
    use tracing::Span;

    const TEST_VALIDATORS: [&str; 2] = ["test-validator-1", "test-validator-2"];

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_pre_validation_output_is_correct() {
        let test_chain = setup();
        let witness = test_chain.valid_witness();

        let output = test_chain.run_pre_validation(&witness).unwrap();
        let new_chunk_data = output.new_chunk_data;

        let prev_execution_results = test_chain.prev_execution_results();
        let prev_chunk_header = test_chain.prev_chunk_header();
        let chunk_header = test_chain.chunk_header();
        let prev_chunk_chunk_extra =
            &prev_execution_results.0.get(&prev_chunk_header.shard_id()).unwrap().chunk_extra;
        assert_eq!(new_chunk_data.gas_limit, prev_chunk_chunk_extra.gas_limit());
        assert_eq!(&new_chunk_data.prev_state_root, prev_chunk_chunk_extra.state_root());
        assert_eq!(
            new_chunk_data.prev_validator_proposals,
            prev_chunk_chunk_extra.validator_proposals().collect_vec()
        );
        assert_eq!(new_chunk_data.chunk_hash, Some(chunk_header.chunk_hash().clone()));

        let receipts = test_chain.receipts_for_shard(prev_chunk_header.shard_id());
        assert_eq!(new_chunk_data.receipts, receipts);

        let transactions = test_chain.transactions();
        let new_chunk_data_transactions =
            new_chunk_data.transactions.into_nonexpired_transactions();
        assert!(!transactions.is_empty());
        assert_eq!(new_chunk_data_transactions, transactions);
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_pre_validation_fails_with_unrelated_shard_id() {
        let test_chain = setup();
        let valid_witness = test_chain.valid_witness();
        let block_hash = valid_witness.chunk_id().block_hash;

        let invalid_witness = TestWitnessBuilder::from_default(valid_witness)
            .chunk_id(SpiceChunkId { block_hash, shard_id: ShardId::new(42) })
            .build();

        let error_message = unwrap_error_message(test_chain.run_pre_validation(&invalid_witness));
        assert_contains(&error_message, "doesn't contain witness shard");
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_pre_validation_fails_with_incorrect_number_of_source_receipt_proofs() {
        let test_chain = setup();
        let valid_witness = test_chain.valid_witness();

        let invalid_receipt_proofs = HashMap::new();
        let invalid_witness = TestWitnessBuilder::from_default(valid_witness)
            .source_receipt_proofs(invalid_receipt_proofs)
            .build();

        let error_message = unwrap_error_message(test_chain.run_pre_validation(&invalid_witness));
        assert_contains(
            &error_message,
            "source_receipt_proofs contains incorrect number of proofs",
        );
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_pre_validation_fails_with_missing_source_receipt_proofs() {
        let test_chain = setup();
        let valid_witness = test_chain.valid_witness();

        let proof = valid_witness.source_receipt_proofs().values().next().unwrap();
        let invalid_receipt_proofs = (0..test_chain.prev_block().chunks().len())
            .map(|i| -> (ShardId, ReceiptProof) { (ShardId::new(42 + i as u64), proof.clone()) })
            .collect();
        let invalid_witness = TestWitnessBuilder::from_default(valid_witness)
            .source_receipt_proofs(invalid_receipt_proofs)
            .build();

        let error_message = unwrap_error_message(test_chain.run_pre_validation(&invalid_witness));
        assert_contains(&error_message, "Missing source receipt proof");
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_pre_validation_fails_with_source_receipt_proofs_from_incorrect_shard_id() {
        let test_chain = setup();
        let valid_witness = test_chain.valid_witness();

        let invalid_receipt_proofs = valid_witness
            .source_receipt_proofs()
            .clone()
            .into_iter()
            .map(|(chunk_hash, mut proof)| {
                proof.1.from_shard_id = ShardId::new(42);
                (chunk_hash, proof)
            })
            .collect();
        let invalid_witness = TestWitnessBuilder::from_default(valid_witness)
            .source_receipt_proofs(invalid_receipt_proofs)
            .build();

        let error_message = unwrap_error_message(test_chain.run_pre_validation(&invalid_witness));
        assert_contains(&error_message, "is from shard 42, expected shard");
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_pre_validation_fails_with_source_receipt_proofs_to_incorrect_shard_id() {
        let test_chain = setup();
        let valid_witness = test_chain.valid_witness();

        let invalid_receipt_proofs = valid_witness
            .source_receipt_proofs()
            .clone()
            .into_iter()
            .map(|(chunk_hash, mut proof)| {
                proof.1.to_shard_id = ShardId::new(42);
                (chunk_hash, proof)
            })
            .collect();
        let invalid_witness = TestWitnessBuilder::from_default(valid_witness)
            .source_receipt_proofs(invalid_receipt_proofs)
            .build();

        let error_message = unwrap_error_message(test_chain.run_pre_validation(&invalid_witness));
        assert_contains(&error_message, "is for shard 42, expected shard");
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_pre_validation_fails_with_source_receipt_proofs_not_matching_merkle_root() {
        let test_chain = setup();
        let valid_witness = test_chain.valid_witness();

        let shard_layout = &test_chain.shard_layout();
        let receipts = vec![];
        let invalid_receipt_proofs = valid_witness
            .source_receipt_proofs()
            .clone()
            .into_iter()
            .map(|(chunk_hash, valid_proof)| {
                let (_, proofs) = Chain::create_receipts_proofs_from_outgoing_receipts(
                    shard_layout,
                    valid_proof.1.from_shard_id,
                    receipts.clone(),
                )
                .unwrap();
                let proof = proofs
                    .into_iter()
                    .find(|p| p.1.to_shard_id == valid_proof.1.to_shard_id)
                    .unwrap();
                (chunk_hash, proof)
            })
            .collect();
        let invalid_witness = TestWitnessBuilder::from_default(valid_witness)
            .source_receipt_proofs(invalid_receipt_proofs)
            .build();

        let error_message = unwrap_error_message(test_chain.run_pre_validation(&invalid_witness));
        assert_contains(
            &error_message,
            "invalid merkle path, doesn't match outgoing receipts root",
        );
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_pre_validation_fails_with_invalid_applied_receipts_hash() {
        let test_chain = setup();
        let valid_witness = test_chain.valid_witness();

        let invalid_receipts_hash = CryptoHash::default();
        let invalid_witness = TestWitnessBuilder::from_default(valid_witness)
            .applied_receipts_hash(invalid_receipts_hash)
            .build();

        let error_message = unwrap_error_message(test_chain.run_pre_validation(&invalid_witness));
        assert_contains(&error_message, "does not match expected receipts hash");
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_pre_validation_fails_with_incorrect_transactions() {
        let test_chain = setup();
        let valid_witness = test_chain.valid_witness();

        let invalid_transactions =
            std::iter::from_fn(|| Some(valid_witness.transactions()[0].clone()))
                .take(valid_witness.transactions().len())
                .collect();
        assert_ne!(&invalid_transactions, valid_witness.transactions());
        let invalid_witness = TestWitnessBuilder::from_default(valid_witness)
            .transactions(invalid_transactions)
            .build();

        let error_message = unwrap_error_message(test_chain.run_pre_validation(&invalid_witness));
        assert_contains(&error_message, "does not match expected transaction root");
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_pre_validation_fails_with_genesis_block() {
        let test_chain = setup();
        let genesis = test_chain.chain.genesis_block();
        let shard_id = genesis.chunks()[0].shard_id();

        let receipts: Vec<Receipt> = Vec::new();
        let invalid_witness = SpiceChunkStateWitness::new(
            SpiceChunkId { block_hash: *genesis.hash(), shard_id },
            PartialState::TrieValues(vec![]),
            HashMap::new(),
            hash(&borsh::to_vec(receipts.as_slice()).unwrap()),
            vec![],
            BTreeSet::new(),
            None,
        );

        let result = spice_pre_validate_chunk_state_witness(
            &invalid_witness,
            &genesis,
            &BlockExecutionResults(HashMap::new()),
            test_chain.chain.epoch_manager.as_ref(),
            test_chain.chain.chain_store(),
            vec![],
        );

        let error_message = unwrap_error_message(result);
        assert_contains(&error_message, "witness is for genesis");
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_pre_validation_fails_with_non_empty_source_receipt_proof_after_genesis() {
        let mut test_chain = setup_without_blocks();
        let genesis = test_chain.chain.genesis_block();
        let block = test_chain.build_block(&genesis);
        process_block_sync(
            &mut test_chain.chain,
            block.clone().into(),
            Provenance::PRODUCED,
            &mut BlockProcessingArtifact::default(),
        )
        .unwrap();
        let shard_id = block.chunks()[0].shard_id();

        let receipts: Vec<Receipt> = Vec::new();
        let proof = test_chain.source_receipt_proofs().into_values().next().unwrap();
        let invalid_source_receipt_proofs = HashMap::from([(shard_id, proof)]);
        let invalid_witness = SpiceChunkStateWitness::new(
            SpiceChunkId { block_hash: *block.hash(), shard_id },
            PartialState::TrieValues(vec![]),
            invalid_source_receipt_proofs,
            hash(&borsh::to_vec(receipts.as_slice()).unwrap()),
            test_chain.transactions(),
            BTreeSet::new(),
            None,
        );

        let result = spice_pre_validate_chunk_state_witness(
            &invalid_witness,
            &block,
            &BlockExecutionResults(HashMap::new()),
            test_chain.chain.epoch_manager.as_ref(),
            test_chain.chain.chain_store(),
            vec![],
        );

        let error_message = unwrap_error_message(result);
        assert_contains(&error_message, "genesis source_receipt_proofs should be empty");
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_pre_validation_fails_with_missing_chunk_and_non_empty_txs() {
        let test_chain = setup();
        let valid_witness = test_chain.valid_witness_for_block_with_missing_chunks();

        let transactions = test_chain.transactions();
        assert!(!transactions.is_empty());
        let invalid_witness =
            TestWitnessBuilder::from_default(valid_witness).transactions(transactions).build();

        let error_message = unwrap_error_message(test_chain.run_pre_validation(&invalid_witness));
        assert_contains(&error_message, "does not match expected transaction root");
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_validation_succeeds_with_valid_witness() {
        let test_chain = setup();
        let witness = test_chain.valid_witness();
        let witness_execution_result = test_chain.run_validation(witness).unwrap();

        let (_, execution_result) = test_chain.simulate_chunk_application();

        assert_eq!(witness_execution_result, execution_result);
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_validation_succeeds_with_valid_witness_and_missing_chunk() {
        let test_chain = setup();
        let witness = test_chain.valid_witness_for_block_with_missing_chunks();
        let witness_execution_result = test_chain.run_validation(witness).unwrap();
        let (_, execution_result) =
            test_chain.simulate_chunk_application_for_block_with_missing_chunks();
        assert_eq!(witness_execution_result, execution_result);
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_pre_validation_succeeds_with_proof_of_invalid_chunk() {
        let mut test_chain = setup();
        let bad_tx_root = CryptoHash::hash_bytes(b"wrong tx root");
        let witness =
            test_chain.witness_with_proof_of_invalid_chunk(bad_tx_root, Default::default());

        let output = test_chain.run_pre_validation(&witness).unwrap();
        let transactions = output.new_chunk_data.transactions.into_nonexpired_transactions();
        assert!(transactions.is_empty());
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_pre_validation_rejects_proof_of_invalid_chunk_for_missing_chunk() {
        let test_chain = setup();
        let valid_witness = test_chain.valid_witness_for_block_with_missing_chunks();

        let any_body = EncodedShardChunkBody { parts: vec![] };
        let invalid_witness = TestWitnessBuilder::from_default(valid_witness)
            .proof_of_invalid_chunk(Some(Box::new(any_body)))
            .build();

        let error_message = unwrap_error_message(test_chain.run_pre_validation(&invalid_witness));
        assert_contains(&error_message, "non-new chunk");
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_pre_validation_rejects_proof_of_invalid_chunk_with_non_empty_transactions() {
        let test_chain = setup();
        let valid_witness = test_chain.valid_witness();

        let any_body = EncodedShardChunkBody { parts: vec![] };
        let invalid_witness = TestWitnessBuilder::from_default(valid_witness)
            .transactions(test_chain.transactions())
            .proof_of_invalid_chunk(Some(Box::new(any_body)))
            .build();

        let error_message = unwrap_error_message(test_chain.run_pre_validation(&invalid_witness));
        assert_contains(&error_message, "non-empty transactions");
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_pre_validation_rejects_proof_of_invalid_chunk_with_wrong_encoded_merkle_root() {
        let test_chain = setup();

        // Use a body whose merkle root does not match the chunk header's encoded_merkle_root.
        let wrong_body =
            EncodedShardChunkBody { parts: vec![Some(vec![0u8; 32].into_boxed_slice())] };
        let valid_witness = test_chain.valid_witness();
        let invalid_witness = TestWitnessBuilder::from_default(valid_witness)
            .transactions(vec![])
            .proof_of_invalid_chunk(Some(Box::new(wrong_body)))
            .build();

        let error_message = unwrap_error_message(test_chain.run_pre_validation(&invalid_witness));
        assert_contains(&error_message, "encoded_merkle_root");
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_pre_validation_rejects_fraudulent_proof_of_invalid_chunk() {
        let mut test_chain = setup();

        // Build a body where everything is correct — the chunk is actually valid,
        // so claiming it's invalid is fraudulent.
        let (correct_tx_root, _) = merklize::<SignedTransaction>(&[]);
        let shard_layout = test_chain.shard_layout();
        let empty_receipt_hashes = Chain::build_receipts_hashes(&[], &shard_layout).unwrap();
        let (correct_receipts_root, _) = merklize(&empty_receipt_hashes);

        let witness =
            test_chain.witness_with_proof_of_invalid_chunk(correct_tx_root, correct_receipts_root);

        let error_message = unwrap_error_message(test_chain.run_pre_validation(&witness));
        assert_contains(&error_message, "fraudulent");
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_pre_validation_rejects_proof_of_invalid_chunk_with_missing_parts() {
        let test_chain = setup();
        let valid_witness = test_chain.valid_witness();

        let body = EncodedShardChunkBody {
            parts: vec![None, Some(vec![0u8; 32].into_boxed_slice()), None],
        };
        let invalid_witness = TestWitnessBuilder::from_default(valid_witness)
            .transactions(vec![])
            .proof_of_invalid_chunk(Some(Box::new(body)))
            .build();

        let error_message = unwrap_error_message(test_chain.run_pre_validation(&invalid_witness));
        assert_contains(&error_message, "missing parts");
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_pre_validation_accepts_proof_of_invalid_chunk_on_decode_failure() {
        let mut test_chain = setup();

        // Build a body with garbage data that will fail to decode.
        let total_parts = test_chain.chain.epoch_manager.num_total_parts();
        let data_parts = test_chain.chain.epoch_manager.num_data_parts();
        let part_size = 64;
        let garbage_body = EncodedShardChunkBody {
            parts: (0..total_parts)
                .map(|_| Some(vec![u8::MAX; part_size].into_boxed_slice()))
                .collect(),
        };
        // Use a realistic encoded_length so decode failure is due to garbage
        // content, not a zero-length shortcut.
        let encoded_length = (part_size * data_parts) as u64;
        let witness = test_chain.witness_with_proof_of_invalid_chunk_body(
            garbage_body,
            encoded_length,
            Default::default(),
            Default::default(),
        );

        let output = test_chain.run_pre_validation(&witness).unwrap();
        let transactions = output.new_chunk_data.transactions.into_nonexpired_transactions();
        assert!(transactions.is_empty());
    }

    #[track_caller]
    fn assert_contains(message: &str, substring: &str) {
        assert!(
            message.contains(substring),
            "assertion failed: \"{}\".contains(\"{}\")",
            message,
            substring
        );
    }

    #[track_caller]
    fn unwrap_error_message<T>(result: Result<T, Error>) -> String {
        assert!(result.is_err());
        let err = result.err().unwrap();
        let Error::InvalidChunkStateWitness(message) = err else {
            panic!("wrong error kind: {:?}", err);
        };
        message
    }

    fn setup() -> TestChain {
        let mut test_chain = setup_without_blocks();
        let mut prev_block = test_chain.chain.genesis_block();
        for i in 0..3 {
            let block = if i == 1 {
                test_chain.build_block_with_missing_chunks(&prev_block)
            } else {
                test_chain.build_block(&prev_block)
            };
            process_block_sync(
                &mut test_chain.chain,
                block.clone().into(),
                Provenance::PRODUCED,
                &mut BlockProcessingArtifact::default(),
            )
            .unwrap();
            prev_block = block;
        }
        test_chain
    }

    fn setup_without_blocks() -> TestChain {
        init_test_logger();

        let boundary_accounts =
            TEST_VALIDATORS.iter().skip(1).map(|v| AccountId::from_str(v).unwrap()).collect();
        let genesis = TestGenesisBuilder::new()
            .validators_spec(ValidatorsSpec::desired_roles(&TEST_VALIDATORS, &[]))
            .shard_layout(ShardLayout::multi_shard_custom(boundary_accounts, 0))
            .build();
        TestChain { chain: get_chain_with_genesis(Clock::real(), genesis) }
    }

    fn test_receipts() -> Vec<Receipt> {
        vec![
            Receipt::new_balance_refund(
                &AccountId::from_str(TEST_VALIDATORS[0]).unwrap(),
                Balance::from_yoctonear(100),
            ),
            Receipt::new_balance_refund(
                &AccountId::from_str(TEST_VALIDATORS[1]).unwrap(),
                Balance::from_yoctonear(100),
            ),
        ]
    }

    fn test_chunk_header(
        height: BlockHeight,
        shard_id: ShardId,
        prev_block_hash: CryptoHash,
        signer: &ValidatorSigner,
        tx_root: CryptoHash,
    ) -> ShardChunkHeader {
        ShardChunkHeader::V3(ShardChunkHeaderV3::new_for_spice(
            prev_block_hash,
            Default::default(),
            Default::default(),
            height,
            shard_id,
            Default::default(),
            tx_root,
            signer,
        ))
    }

    fn test_transactions_from_prev_block_hash(
        prev_block_hash: CryptoHash,
    ) -> Vec<SignedTransaction> {
        let nonce = 1;
        let from = AccountId::from_str(TEST_VALIDATORS[0]).unwrap();
        let signer = create_user_test_signer(from.as_ref());
        let to = AccountId::from_str(TEST_VALIDATORS[1]).unwrap();

        let send_money = |amount| {
            SignedTransaction::send_money(
                nonce,
                from.clone(),
                to.clone(),
                &signer,
                amount,
                prev_block_hash,
            )
        };

        vec![
            send_money(Balance::from_yoctonear(100)),
            send_money(Balance::from_yoctonear(200)),
            send_money(Balance::from_yoctonear(300)),
        ]
    }

    struct TestWitnessBuilder {
        chunk_id: SpiceChunkId,
        pre_state: PartialState,
        source_receipt_proofs: HashMap<ShardId, ReceiptProof>,
        applied_receipts_hash: CryptoHash,
        transactions: Vec<SignedTransaction>,
        contract_accesses: BTreeSet<CodeHash>,
        proof_of_invalid_chunk: Option<Box<EncodedShardChunkBody>>,
    }

    macro_rules! builder_setter {
        ($field: ident, $type: ty) => {
            fn $field(mut self, value: $type) -> Self {
                self.$field = value;
                self
            }
        };
    }

    impl TestWitnessBuilder {
        builder_setter!(chunk_id, SpiceChunkId);
        builder_setter!(source_receipt_proofs, HashMap<ShardId, ReceiptProof>);
        builder_setter!(applied_receipts_hash, CryptoHash);
        builder_setter!(transactions, Vec<SignedTransaction>);
        builder_setter!(proof_of_invalid_chunk, Option<Box<EncodedShardChunkBody>>);

        fn from_default(default: SpiceChunkStateWitness) -> Self {
            Self {
                chunk_id: default.chunk_id().clone(),
                pre_state: default.pre_state().clone(),
                source_receipt_proofs: default.source_receipt_proofs().clone(),
                applied_receipts_hash: *default.applied_receipts_hash(),
                transactions: default.transactions().to_vec(),
                contract_accesses: default.contract_accesses().clone(),
                proof_of_invalid_chunk: default
                    .proof_of_invalid_chunk()
                    .map(|b| Box::new(b.clone())),
            }
        }

        fn build(self) -> SpiceChunkStateWitness {
            SpiceChunkStateWitness::new(
                self.chunk_id,
                self.pre_state,
                self.source_receipt_proofs,
                self.applied_receipts_hash,
                self.transactions,
                self.contract_accesses,
                self.proof_of_invalid_chunk,
            )
        }
    }

    /// Fabricated per-shard execution results and the source receipt proofs to
    /// `target_shard_id` consistent with them.
    fn fabricate_prev_results_and_proofs(
        shard_layout: &ShardLayout,
        target_shard_id: ShardId,
        state_root: &CryptoHash,
        receipts_by_shard: &HashMap<ShardId, Vec<Receipt>>,
    ) -> (BlockExecutionResults, HashMap<ShardId, ReceiptProof>) {
        let mut results = BlockExecutionResults(HashMap::new());
        let mut proofs = HashMap::new();
        for shard_id in shard_layout.shard_ids() {
            let receipts = receipts_by_shard.get(&shard_id).cloned().unwrap_or_default();
            let (root, shard_proofs) = Chain::create_receipts_proofs_from_outgoing_receipts(
                shard_layout,
                shard_id,
                receipts,
            )
            .unwrap();
            results.0.insert(
                shard_id,
                Arc::new(ChunkExecutionResult {
                    chunk_extra: ChunkExtra::new_with_only_state_root(state_root),
                    outgoing_receipts_root: root,
                }),
            );
            let proof = shard_proofs
                .into_iter()
                .find(|proof| proof.1.to_shard_id == target_shard_id)
                .unwrap();
            proofs.insert(shard_id, proof);
        }
        (results, proofs)
    }

    struct TestChain {
        chain: Chain,
    }

    impl TestChain {
        fn block(&self) -> Arc<Block> {
            let block = self.chain.get_head_block().unwrap();
            assert!(block.chunks()[0].is_new_chunk(block.header().height()));
            block
        }

        fn prev_block(&self) -> Arc<Block> {
            let block = self.block();
            self.chain.get_block(block.header().prev_hash()).unwrap()
        }

        fn block_with_missing_chunks(&self) -> Arc<Block> {
            let mut block = self.block();
            while block.chunks()[0].is_new_chunk(block.header().height()) {
                block = self.chain.get_block(block.header().prev_hash()).unwrap();
            }
            assert!(!block.header().is_genesis());
            block
        }

        fn shard_layout(&self) -> ShardLayout {
            self.chain.epoch_manager.get_shard_layout(self.block().header().epoch_id()).unwrap()
        }

        fn receipts_for_shard(&self, shard_id: ShardId) -> Vec<Receipt> {
            let shard_layout = self.shard_layout();
            let receipts: Vec<_> = test_receipts()
                .iter()
                .filter(|r| r.receiver_shard_id(&shard_layout).unwrap() == shard_id)
                .cloned()
                .collect();
            assert!(!receipts.is_empty());
            receipts
        }

        fn build_block_with_missing_chunks(&self, prev_block: &Block) -> Arc<Block> {
            let chunks = prev_block.chunks().iter_raw().cloned().collect_vec();
            self.build_block_with_chunks(prev_block, chunks)
        }

        fn build_block(&self, prev_block: &Block) -> Arc<Block> {
            let mut chunks = Vec::new();
            let txs = test_transactions_from_prev_block_hash(*prev_block.hash());
            let (tx_root, _) = merklize(&txs);
            for chunk in prev_block.chunks().iter_raw() {
                let shard_id = chunk.shard_id();
                let height = prev_block.header().height() + 1;
                let chunk_producer = self
                    .chain
                    .epoch_manager
                    .get_chunk_producer_info(&ChunkProductionKey {
                        shard_id,
                        epoch_id: *prev_block.header().epoch_id(),
                        height_created: height,
                    })
                    .unwrap();
                let signer = create_test_signer(chunk_producer.account_id().as_str());
                let mut chunk_header =
                    test_chunk_header(height, shard_id, *prev_block.hash(), &signer, tx_root);
                *chunk_header.height_included_mut() = height;
                chunks.push(chunk_header);
            }
            self.build_block_with_chunks(prev_block, chunks)
        }

        fn build_block_with_chunks(
            &self,
            prev_block: &Block,
            chunks: Vec<ShardChunkHeader>,
        ) -> Arc<Block> {
            let block_producer = self
                .chain
                .epoch_manager
                .get_block_producer_info(
                    prev_block.header().epoch_id(),
                    prev_block.header().height() + 1,
                )
                .unwrap();
            let signer = Arc::new(create_test_signer(block_producer.account_id().as_str()));
            TestBlockBuilder::from_prev_block(Clock::real(), prev_block, signer)
                .chunks(chunks)
                .spice_core_statements(vec![])
                .build()
        }

        /// RS-encodes an empty TransactionReceipt. Returns the fully reconstructed body
        /// (all parts filled) and the encoded_length. The decoded tx_root of the resulting
        /// chunk will be `merklize::<SignedTransaction>(&[]).0`.
        fn make_empty_encoded_body(&self) -> (EncodedShardChunkBody, u64) {
            let total_parts = self.chain.epoch_manager.num_total_parts();
            let data_parts = self.chain.epoch_manager.num_data_parts();
            let rs = ReedSolomon::new(data_parts, total_parts - data_parts).unwrap();
            let (parts, encoded_length) =
                reed_solomon_encode(&rs, &TransactionReceipt(vec![], vec![]));
            (EncodedShardChunkBody { parts }, encoded_length as u64)
        }

        /// Builds a block where each shard's chunk header has the given fields.
        fn build_block_with_custom_chunk_header(
            &self,
            prev_block: &Block,
            encoded_merkle_root: CryptoHash,
            encoded_length: u64,
            tx_root: CryptoHash,
            prev_outgoing_receipts_root: CryptoHash,
        ) -> Arc<Block> {
            let mut chunks = Vec::new();
            for chunk in prev_block.chunks().iter_raw() {
                let shard_id = chunk.shard_id();
                let height = prev_block.header().height() + 1;
                let chunk_producer = self
                    .chain
                    .epoch_manager
                    .get_chunk_producer_info(&ChunkProductionKey {
                        shard_id,
                        epoch_id: *prev_block.header().epoch_id(),
                        height_created: height,
                    })
                    .unwrap();
                let signer = create_test_signer(chunk_producer.account_id().as_str());
                let mut chunk_header = ShardChunkHeader::V3(ShardChunkHeaderV3::new_for_spice(
                    *prev_block.hash(),
                    encoded_merkle_root,
                    encoded_length,
                    height,
                    shard_id,
                    prev_outgoing_receipts_root,
                    tx_root,
                    &signer,
                ));
                *chunk_header.height_included_mut() = height;
                chunks.push(chunk_header);
            }
            self.build_block_with_chunks(prev_block, chunks)
        }

        /// Builds a witness with the given body as proof_of_invalid_chunk.
        /// Creates a block whose chunk header's encoded_merkle_root matches
        /// the body, with the given tx_root and prev_outgoing_receipts_root.
        fn witness_with_proof_of_invalid_chunk_body(
            &mut self,
            body: EncodedShardChunkBody,
            encoded_length: u64,
            tx_root: CryptoHash,
            prev_outgoing_receipts_root: CryptoHash,
        ) -> SpiceChunkStateWitness {
            let (encoded_merkle_root, _) = body.get_merkle_hash_and_paths();

            let prev_block = self.block();
            let block = self.build_block_with_custom_chunk_header(
                &prev_block,
                encoded_merkle_root,
                encoded_length,
                tx_root,
                prev_outgoing_receipts_root,
            );
            process_block_sync(
                &mut self.chain,
                block.clone().into(),
                Provenance::PRODUCED,
                &mut BlockProcessingArtifact::default(),
            )
            .unwrap();

            let shard_id = self.shard_id();
            SpiceChunkStateWitness::new(
                SpiceChunkId { block_hash: *block.hash(), shard_id },
                PartialState::TrieValues(vec![]),
                self.source_receipt_proofs(),
                self.applied_receipts_hash(),
                vec![],
                BTreeSet::new(),
                Some(Box::new(body)),
            )
        }

        /// Convenience wrapper: RS-encodes an empty body and builds a witness
        /// with it as proof_of_invalid_chunk.
        fn witness_with_proof_of_invalid_chunk(
            &mut self,
            tx_root: CryptoHash,
            prev_outgoing_receipts_root: CryptoHash,
        ) -> SpiceChunkStateWitness {
            let (body, encoded_length) = self.make_empty_encoded_body();
            self.witness_with_proof_of_invalid_chunk_body(
                body,
                encoded_length,
                tx_root,
                prev_outgoing_receipts_root,
            )
        }

        fn shard_id(&self) -> ShardId {
            self.shard_layout().shard_ids().next().unwrap()
        }

        fn chunk_header(&self) -> ShardChunkHeader {
            let block = self.block();
            block.chunks()[0].clone()
        }

        fn prev_chunk_header(&self) -> ShardChunkHeader {
            let prev_block = self.prev_block();
            let chunk_header = self.chunk_header();
            self.chain
                .epoch_manager
                .get_prev_chunk_header(&prev_block, chunk_header.shard_id())
                .unwrap()
        }

        fn receipts_by_shard(&self) -> HashMap<ShardId, Vec<Receipt>> {
            self.shard_layout()
                .shard_ids()
                .map(|shard_id| (shard_id, self.receipts_for_shard(shard_id)))
                .collect()
        }

        fn source_receipt_proofs(&self) -> HashMap<ShardId, ReceiptProof> {
            self.fabricated_results_and_proofs().1
        }

        fn prev_execution_results(&self) -> BlockExecutionResults {
            self.fabricated_results_and_proofs().0
        }

        fn fabricated_results_and_proofs(
            &self,
        ) -> (BlockExecutionResults, HashMap<ShardId, ReceiptProof>) {
            let genesis_state_root =
                get_genesis_state_roots(&self.chain.chain_store.store()).unwrap()[0];
            fabricate_prev_results_and_proofs(
                &self.shard_layout(),
                self.chunk_header().shard_id(),
                &genesis_state_root,
                &self.receipts_by_shard(),
            )
        }

        fn applied_receipts_hash(&self) -> CryptoHash {
            let chunk_header = self.chunk_header();
            let receipts = self.receipts_for_shard(chunk_header.shard_id());
            hash(&borsh::to_vec(receipts.as_slice()).unwrap())
        }

        fn transactions(&self) -> Vec<SignedTransaction> {
            let block = self.block();
            test_transactions_from_prev_block_hash(*block.header().prev_hash())
        }

        fn valid_witness(&self) -> SpiceChunkStateWitness {
            let block = self.block();
            let chunk_header = self.chunk_header();
            let shard_id = chunk_header.shard_id();
            let receipt_proofs = self.source_receipt_proofs();
            let receipts_hash = self.applied_receipts_hash();
            let transactions = self.transactions();

            let (transition, _execution_result) = self.simulate_chunk_application();
            SpiceChunkStateWitness::new(
                SpiceChunkId { block_hash: *block.hash(), shard_id },
                transition,
                receipt_proofs,
                receipts_hash,
                transactions,
                BTreeSet::new(),
                None,
            )
        }

        fn valid_witness_for_block_with_missing_chunks(&self) -> SpiceChunkStateWitness {
            let block = self.block_with_missing_chunks();
            let shard_layout = self.shard_layout();
            let shard_id = shard_layout.shard_ids().next().unwrap();
            let receipt_proofs = self.source_receipt_proofs();
            let receipts_hash = self.applied_receipts_hash();
            let transactions = vec![];

            let (transition, _execution_result) =
                self.simulate_chunk_application_for_block_with_missing_chunks();
            SpiceChunkStateWitness::new(
                SpiceChunkId { block_hash: *block.hash(), shard_id },
                transition,
                receipt_proofs,
                receipts_hash,
                transactions,
                BTreeSet::new(),
                None,
            )
        }

        fn run_validation(
            &self,
            state_witness: SpiceChunkStateWitness,
        ) -> Result<ChunkExecutionResult, Error> {
            let block = self.chain.get_block(&state_witness.chunk_id().block_hash).unwrap();
            let prev_block = self.chain.get_block(block.header().prev_hash()).unwrap();
            let prev_execution_results = self.prev_execution_results();
            let shard_id = state_witness.chunk_id().shard_id;
            let prev_validator_proposals = self
                .chain
                .spice_core_reader
                .prev_validator_proposals(prev_block.hash(), shard_id)
                .unwrap();
            let pre_validation_output = spice_pre_validate_chunk_state_witness(
                &state_witness,
                &block,
                &prev_execution_results,
                self.chain.epoch_manager.as_ref(),
                self.chain.chain_store(),
                prev_validator_proposals,
            )
            .unwrap();

            spice_validate_chunk_state_witness(
                state_witness,
                pre_validation_output,
                self.chain.epoch_manager.as_ref(),
                self.chain.runtime_adapter.as_ref(),
            )
        }

        fn run_pre_validation(
            &self,
            state_witness: &SpiceChunkStateWitness,
        ) -> Result<SpicePreValidationOutput, Error> {
            let block = self.chain.get_block(&state_witness.chunk_id().block_hash).unwrap();
            let prev_block = self.chain.get_block(block.header().prev_hash()).unwrap();
            let prev_execution_results = self.prev_execution_results();
            let shard_id = state_witness.chunk_id().shard_id;
            let prev_validator_proposals = self
                .chain
                .spice_core_reader
                .prev_validator_proposals(prev_block.hash(), shard_id)
                .unwrap();
            spice_pre_validate_chunk_state_witness(
                state_witness,
                &block,
                &prev_execution_results,
                self.chain.epoch_manager.as_ref(),
                self.chain.chain_store(),
                prev_validator_proposals,
            )
        }

        fn simulate_chunk_application(&self) -> (PartialState, ChunkExecutionResult) {
            let transactions = self.transactions();
            self.simulate_chunk_application_for_block(&self.block(), transactions)
        }

        fn simulate_chunk_application_for_block_with_missing_chunks(
            &self,
        ) -> (PartialState, ChunkExecutionResult) {
            self.simulate_chunk_application_for_block(&self.block_with_missing_chunks(), vec![])
        }

        fn simulate_chunk_application_for_block(
            &self,
            block: &Block,
            transactions: Vec<SignedTransaction>,
        ) -> (PartialState, ChunkExecutionResult) {
            let prev_execution_results = self.prev_execution_results();
            let receipts = self.receipts_for_shard(self.shard_id());
            let storage_context = StorageContext {
                storage_data_source: StorageDataSource::Db,
                state_patch: Default::default(),
            };

            let shard_uid = shard_id_to_uid(
                self.chain.epoch_manager.as_ref(),
                self.shard_id(),
                block.header().epoch_id(),
            )
            .unwrap();

            let prev_execution_result = prev_execution_results.0.get(&self.shard_id()).unwrap();
            let prev_chunk_chunk_extra = &prev_execution_result.chunk_extra;
            let prev_block_hash = block.header().prev_hash();
            let prev_validator_proposals = self
                .chain
                .spice_core_reader
                .prev_validator_proposals(prev_block_hash, self.shard_id())
                .unwrap();
            let txs_validity = std::iter::repeat_n(true, transactions.len()).collect_vec();
            let new_chunk_data = NewChunkData {
                gas_limit: prev_chunk_chunk_extra.gas_limit(),
                prev_state_root: *prev_chunk_chunk_extra.state_root(),
                prev_validator_proposals,
                chunk_hash: None,
                transactions: SignedValidPeriodTransactions::new(transactions, txs_validity),
                receipts,
                block: build_spice_apply_chunk_block_context(
                    block.header(),
                    &prev_execution_results,
                    self.chain.epoch_manager.as_ref(),
                )
                .unwrap(),
                storage_context,
            };
            let memtrie_pin = self
                .chain
                .runtime_adapter
                .get_tries()
                .maybe_pin_memtrie_root(shard_uid, *prev_chunk_chunk_extra.state_root())
                .expect("memtrie root pin should be acquirable in chunk-application simulator");
            let NewChunkResult { shard_uid: _, gas_limit, apply_result } = apply_new_chunk(
                ApplyChunkReason::UpdateTrackedShard,
                &Span::none(),
                new_chunk_data,
                ShardContext { shard_uid, should_apply_chunk: true },
                self.chain.runtime_adapter.as_ref(),
                memtrie_pin,
                None,
            )
            .unwrap();

            let chunk_extra = apply_result.to_chunk_extra(gas_limit);
            let shard_layout =
                self.chain.epoch_manager.get_shard_layout(block.header().epoch_id()).unwrap();
            let (outgoing_receipts_root, _) = Chain::create_receipts_proofs_from_outgoing_receipts(
                &shard_layout,
                self.shard_id(),
                apply_result.outgoing_receipts,
            )
            .unwrap();
            let execution_result = ChunkExecutionResult { chunk_extra, outgoing_receipts_root };

            (apply_result.proof.unwrap().nodes, execution_result)
        }
    }

    /// Era-semantics tests for a witness of the spice activation parent whose chunk
    /// is missing.
    mod pre_spice_boundary {
        use super::*;
        use crate::spice::boundary::execution_result_from_pre_spice_child;
        use crate::spice::tests::{
            pre_spice_chunk_endorsements, save_and_record_block, setup_pre_spice_chain,
        };
        use near_primitives::bandwidth_scheduler::BandwidthRequests;
        use near_primitives::congestion_info::CongestionInfo;
        use near_primitives::gas::Gas;
        use near_primitives::spice::state_witness::SpiceBoundaryWitnessData;
        use near_primitives::test_utils::{
            TestBlockBuilder, create_test_signer, pre_spice_protocol_version,
        };
        use near_primitives::types::{Balance, BlockHeight};

        struct BoundaryChain {
            chain: Chain,
            /// Block including the other shard's chunk, mid-range for the target
            /// shard's witness: the target's chunk is missing in it.
            mid_range_block: Arc<Block>,
            /// Block of the target shard's last included chunk.
            anchor_block: Arc<Block>,
            /// The pre-spice block the witness is keyed to; every chunk missing.
            boundary_block: Arc<Block>,
            target_shard_id: ShardId,
            other_shard_id: ShardId,
        }

        fn test_receiver() -> &'static str {
            "test1"
        }

        /// A staggered gap straddling the anchor:
        /// full block -> mid-range (other new, target missing) -> anchor (target
        /// new, other new iff `other_included_at_anchor`) -> boundary block
        /// (everything missing).
        fn setup_boundary_chain(other_included_at_anchor: bool) -> BoundaryChain {
            init_test_logger();
            let chain = setup_pre_spice_chain(2);
            let genesis_block = chain.get_block(&chain.genesis().hash().clone()).unwrap();
            let shard_layout =
                chain.epoch_manager.get_shard_layout(genesis_block.header().epoch_id()).unwrap();
            let target_shard_id = test_receiver()
                .parse::<AccountId>()
                .map(|account_id| shard_layout.account_id_to_shard_id(&account_id))
                .unwrap();
            let other_shard_id =
                shard_layout.shard_ids().find(|shard_id| shard_id != &target_shard_id).unwrap();

            let mut boundary_chain = BoundaryChain {
                chain,
                mid_range_block: genesis_block.clone(),
                anchor_block: genesis_block.clone(),
                boundary_block: genesis_block,
                target_shard_id,
                other_shard_id,
            };
            let full_block = boundary_chain
                .add_block(&boundary_chain.genesis(), &[target_shard_id, other_shard_id]);
            boundary_chain.mid_range_block =
                boundary_chain.add_block(&full_block, &[other_shard_id]);
            let anchor_shards: &[ShardId] = if other_included_at_anchor {
                &[target_shard_id, other_shard_id]
            } else {
                &[target_shard_id]
            };
            boundary_chain.anchor_block =
                boundary_chain.add_block(&boundary_chain.mid_range_block.clone(), anchor_shards);
            boundary_chain.boundary_block =
                boundary_chain.add_block(&boundary_chain.anchor_block.clone(), &[]);
            boundary_chain
        }

        impl BoundaryChain {
            fn genesis(&self) -> Arc<Block> {
                self.chain.get_block(&self.chain.genesis().hash().clone()).unwrap()
            }

            fn shard_layout(&self) -> ShardLayout {
                self.chain
                    .epoch_manager
                    .get_shard_layout(self.boundary_block.header().epoch_id())
                    .unwrap()
            }

            fn receipts_from(&self, from_shard_id: ShardId, height: BlockHeight) -> Vec<Receipt> {
                let shard_id: u64 = from_shard_id.into();
                let deposit = 100 * u128::from(height) + u128::from(shard_id);
                vec![Receipt::new_balance_refund(
                    &test_receiver().parse().unwrap(),
                    Balance::from_yoctonear(deposit),
                )]
            }

            /// The receipts root a new chunk of `shard_id` included at `height`
            /// commits to as its previous chunk's output.
            fn receipts_root_from(&self, shard_id: ShardId, height: BlockHeight) -> CryptoHash {
                let (root, _) = Chain::create_receipts_proofs_from_outgoing_receipts(
                    &self.shard_layout(),
                    shard_id,
                    self.receipts_from(shard_id, height),
                )
                .unwrap();
                root
            }

            /// Fabricates the next block: shards in `new_chunk_shards` get a new
            /// pre-spice chunk header whose `prev_outgoing_receipts_root` commits
            /// to [`Self::receipts_from`] that shard at this height — the root
            /// witness source proofs are verified against — and every other shard
            /// carries the previous block's header.
            fn add_block(
                &mut self,
                prev_block: &Block,
                new_chunk_shards: &[ShardId],
            ) -> Arc<Block> {
                let signer = Arc::new(create_test_signer("test1"));
                let height = prev_block.header().height() + 1;
                let chunks: Vec<_> = prev_block
                    .chunks()
                    .iter_raw()
                    .map(|carried| {
                        let shard_id = carried.shard_id();
                        if !new_chunk_shards.contains(&shard_id) {
                            return carried.clone();
                        }
                        let mut chunk_header = ShardChunkHeader::V3(ShardChunkHeaderV3::new(
                            *prev_block.hash(),
                            CryptoHash::default(),
                            CryptoHash::default(),
                            CryptoHash::default(),
                            0,
                            height,
                            shard_id,
                            Gas::ZERO,
                            Gas::ZERO,
                            Balance::ZERO,
                            self.receipts_root_from(shard_id, height),
                            CryptoHash::default(),
                            vec![],
                            CongestionInfo::default(),
                            BandwidthRequests::empty(),
                            None,
                            &signer,
                            pre_spice_protocol_version(),
                        ));
                        *chunk_header.height_included_mut() = height;
                        chunk_header
                    })
                    .collect();
                let chunk_endorsements =
                    pre_spice_chunk_endorsements(&self.chain, prev_block, height, &chunks);
                let block = TestBlockBuilder::from_prev_block(Clock::real(), prev_block, signer)
                    .chunks(chunks)
                    .chunk_endorsements(chunk_endorsements)
                    .protocol_version(pre_spice_protocol_version())
                    .build();
                save_and_record_block(&mut self.chain, &block, pre_spice_protocol_version());
                block
            }

            /// Hash of the chunk of `shard_id` carried by `block`.
            fn chunk_hash(&self, block: &Block, shard_id: ShardId) -> ChunkHash {
                let shard_index = self.shard_layout().get_shard_index(shard_id).unwrap();
                let chunks = block.chunks();
                chunks.get(shard_index).unwrap().chunk_hash().clone()
            }

            fn source_blocks(&self) -> Vec<Arc<Block>> {
                boundary_source_blocks_for_target(
                    &self.chain.chain_store,
                    self.chain.epoch_manager.as_ref(),
                    &self.anchor_block,
                    self.target_shard_id,
                )
                .unwrap()
            }

            fn source_receipt_proofs(&self) -> HashMap<ChunkHash, ReceiptProof> {
                let shard_layout = self.shard_layout();
                let mut source_receipt_proofs = HashMap::new();
                for source_block in self.source_blocks() {
                    let height = source_block.header().height();
                    for chunk_header in source_block.chunks().iter_new() {
                        let (_, proofs) = Chain::create_receipts_proofs_from_outgoing_receipts(
                            &shard_layout,
                            chunk_header.shard_id(),
                            self.receipts_from(chunk_header.shard_id(), height),
                        )
                        .unwrap();
                        let proof = proofs
                            .into_iter()
                            .find(|proof| proof.1.to_shard_id == self.target_shard_id)
                            .unwrap();
                        source_receipt_proofs.insert(chunk_header.chunk_hash().clone(), proof);
                    }
                }
                source_receipt_proofs
            }

            fn prev_execution_results(&self) -> BlockExecutionResults {
                let prev_result = execution_result_from_pre_spice_child(
                    self.chain.epoch_manager.as_ref(),
                    &self.anchor_block,
                    self.target_shard_id,
                )
                .unwrap()
                .unwrap();
                BlockExecutionResults(HashMap::from([(
                    self.target_shard_id,
                    Arc::new(prev_result),
                )]))
            }

            fn boundary_witness_with_proofs(
                &self,
                source_receipt_proofs: HashMap<ChunkHash, ReceiptProof>,
            ) -> SpiceChunkStateWitness {
                let receipts_to_apply = validate_boundary_source_receipts_proofs(
                    &self.source_receipt_proofs(),
                    &self.source_blocks(),
                    &self.shard_layout(),
                    self.target_shard_id,
                )
                .unwrap();
                let applied_receipts_hash =
                    hash(&borsh::to_vec(receipts_to_apply.as_slice()).unwrap());
                SpiceChunkStateWitness::new_boundary(
                    SpiceChunkId {
                        block_hash: *self.boundary_block.hash(),
                        shard_id: self.target_shard_id,
                    },
                    PartialState::TrieValues(vec![]),
                    SpiceBoundaryWitnessData {
                        source_receipt_proofs,
                        implicit_transitions: vec![],
                    },
                    applied_receipts_hash,
                    vec![],
                    BTreeSet::new(),
                )
            }

            fn boundary_witness(&self) -> SpiceChunkStateWitness {
                self.boundary_witness_with_proofs(self.source_receipt_proofs())
            }

            fn run_pre_validation(
                &self,
                state_witness: &SpiceChunkStateWitness,
            ) -> Result<SpicePreValidationOutput, Error> {
                spice_pre_validate_chunk_state_witness(
                    state_witness,
                    &self.boundary_block,
                    &self.prev_execution_results(),
                    self.chain.epoch_manager.as_ref(),
                    self.chain.chain_store(),
                    vec![],
                )
            }
        }

        fn assert_rejected(result: Result<SpicePreValidationOutput, Error>, what: &str) {
            let err = match result {
                Ok(_) => panic!("{what} must be rejected"),
                Err(err) => err,
            };
            assert!(matches!(err, Error::InvalidChunkStateWitness(_)), "wrong error kind: {err:?}",);
        }

        fn sorted(mut receipts: Vec<Receipt>) -> Vec<Receipt> {
            receipts.sort_by_cached_key(|receipt| borsh::to_vec(receipt).unwrap());
            receipts
        }

        /// The main transition's context must be the anchor's pre-spice context: the
        /// anchor's height and parent, the anchor parent's gas price, and the
        /// anchor's real missed-chunk counts (the other shard's chunk is missing in
        /// the anchor).
        #[test]
        #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
        fn test_boundary_witness_uses_pre_spice_anchor_context() {
            let boundary_chain = setup_boundary_chain(false);
            let witness = boundary_chain.boundary_witness();
            let output = boundary_chain.run_pre_validation(&witness).unwrap();

            let anchor_block = &boundary_chain.anchor_block;
            let block_context = &output.new_chunk_data.block;
            assert_eq!(block_context.height, anchor_block.header().height());
            assert_eq!(&block_context.prev_block_hash, anchor_block.header().prev_hash());
            let anchor_prev_header =
                boundary_chain.chain.get_block_header(anchor_block.header().prev_hash()).unwrap();
            assert_eq!(block_context.gas_price, anchor_prev_header.next_gas_price());
            let congestion_info = &block_context.congestion_info;
            assert_eq!(
                congestion_info.get(&boundary_chain.target_shard_id).unwrap().missed_chunks_count,
                0,
            );
            assert_eq!(
                congestion_info.get(&boundary_chain.other_shard_id).unwrap().missed_chunks_count,
                1,
            );

            // Main transition is the anchor's chunk.
            let shard_layout = boundary_chain.shard_layout();
            let target_shard_index =
                shard_layout.get_shard_index(boundary_chain.target_shard_id).unwrap();
            let anchor_chunks = anchor_block.chunks();
            let anchor_chunk_header = anchor_chunks.get(target_shard_index).unwrap();
            assert_eq!(
                output.new_chunk_data.chunk_hash,
                Some(anchor_chunk_header.chunk_hash().clone()),
            );

            // Receipts come from each source shard's own inclusion: the target's at
            // the anchor (newest first), the other shard's at the mid-range block.
            let expected_receipts = [
                boundary_chain
                    .receipts_from(boundary_chain.target_shard_id, anchor_block.header().height()),
                boundary_chain.receipts_from(
                    boundary_chain.other_shard_id,
                    boundary_chain.mid_range_block.header().height(),
                ),
            ]
            .concat();
            assert_eq!(output.new_chunk_data.receipts, expected_receipts);

            // One implicit replay: the boundary block itself, as an old chunk.
            assert_eq!(output.implicit_transition_params.len(), 1);
            let (replay_context, replay_shard_uid) = &output.implicit_transition_params[0];
            assert_eq!(replay_context.height, boundary_chain.boundary_block.header().height());
            assert_eq!(&replay_context.prev_block_hash, anchor_block.hash());
            assert_eq!(replay_shard_uid.shard_id(), boundary_chain.target_shard_id);
        }

        /// A proof carrying receipts other than the ones its chunk header commits to
        /// must be rejected.
        #[test]
        #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
        fn test_boundary_witness_source_proofs_verify_against_source_header_roots() {
            let boundary_chain = setup_boundary_chain(false);
            let mut source_receipt_proofs = boundary_chain.source_receipt_proofs();
            let target_id = boundary_chain
                .chunk_hash(&boundary_chain.anchor_block, boundary_chain.target_shard_id);
            let other_id = boundary_chain
                .chunk_hash(&boundary_chain.mid_range_block, boundary_chain.other_shard_id);
            let target = source_receipt_proofs[&target_id].clone();
            let mut other = source_receipt_proofs[&other_id].clone();
            // Swap the receipt payloads while keeping the shard routing.
            other.0 = target.0;
            source_receipt_proofs.insert(other_id, other);
            let witness = boundary_chain.boundary_witness_with_proofs(source_receipt_proofs);

            assert_rejected(boundary_chain.run_pre_validation(&witness), "tampered source proof");
        }

        /// The proof set must cover exactly the chunk headers included within the
        /// consumed range: dropping the mid-range contribution is rejected.
        #[test]
        #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
        fn test_boundary_witness_source_proofs_come_from_source_shard_inclusions() {
            let boundary_chain = setup_boundary_chain(false);
            let mut source_receipt_proofs = boundary_chain.source_receipt_proofs();
            source_receipt_proofs.remove(
                &boundary_chain
                    .chunk_hash(&boundary_chain.mid_range_block, boundary_chain.other_shard_id),
            );
            let witness = boundary_chain.boundary_witness_with_proofs(source_receipt_proofs);

            assert_rejected(boundary_chain.run_pre_validation(&witness), "dropped source proof");
        }

        /// A source shard included at both the mid-range block and the anchor
        /// contributes one proof per inclusion, each verified against its own
        /// header.
        #[test]
        #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
        fn test_boundary_witness_source_shard_included_twice_in_range() {
            let boundary_chain = setup_boundary_chain(true);
            let anchor_height = boundary_chain.anchor_block.header().height();
            let mid_range_height = boundary_chain.mid_range_block.header().height();
            let other_at_anchor = boundary_chain
                .chunk_hash(&boundary_chain.anchor_block, boundary_chain.other_shard_id);
            let other_at_mid_range = boundary_chain
                .chunk_hash(&boundary_chain.mid_range_block, boundary_chain.other_shard_id);

            let source_receipt_proofs = boundary_chain.source_receipt_proofs();
            assert_eq!(source_receipt_proofs.len(), 3);
            let witness =
                boundary_chain.boundary_witness_with_proofs(source_receipt_proofs.clone());
            let output = boundary_chain.run_pre_validation(&witness).unwrap();
            let expected_receipts = [
                boundary_chain.receipts_from(boundary_chain.target_shard_id, anchor_height),
                boundary_chain.receipts_from(boundary_chain.other_shard_id, anchor_height),
                boundary_chain.receipts_from(boundary_chain.other_shard_id, mid_range_height),
            ]
            .concat();
            assert_eq!(sorted(output.new_chunk_data.receipts), sorted(expected_receipts));

            let mut dropped = source_receipt_proofs.clone();
            dropped.remove(&other_at_mid_range);
            let witness = boundary_chain.boundary_witness_with_proofs(dropped);
            assert_rejected(
                boundary_chain.run_pre_validation(&witness),
                "dropped proof of one of two inclusions",
            );

            let mut swapped = source_receipt_proofs;
            let mut at_mid_range = swapped[&other_at_mid_range].clone();
            at_mid_range.0 = swapped[&other_at_anchor].0.clone();
            swapped.insert(other_at_mid_range, at_mid_range);
            let witness = boundary_chain.boundary_witness_with_proofs(swapped);
            assert_rejected(
                boundary_chain.run_pre_validation(&witness),
                "proof carrying the same shard's other inclusion",
            );
        }
    }
}
