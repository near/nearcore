use std::collections::HashMap;
use std::sync::Arc;

use itertools::Itertools;
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_primitives::apply::ApplyChunkReason;
use near_primitives::block::Block;
use near_primitives::hash::hash;
use near_primitives::merkle::merklize;
use near_primitives::receipt::Receipt;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::sharding::ReceiptProof;
use near_primitives::stateless_validation::spice_state_witness::SpiceChunkStateWitness;
use near_primitives::types::{BlockExecutionResults, ChunkExecutionResult, ShardId};
use near_store::PartialStorage;
use near_store::adapter::StoreAdapter as _;
use node_runtime::SignedValidPeriodTransactions;
use tracing::Span;

use crate::chain::{NewChunkData, NewChunkResult, ShardContext, StorageContext, apply_new_chunk};
use crate::sharding::{get_receipts_shuffle_salt, shuffle_receipt_proofs};
use crate::store::filter_incoming_receipts_for_shard;
use crate::types::{RuntimeAdapter, StorageDataSource};
use crate::{Chain, ChainStore};

use super::chunk_validation::{apply_result_to_chunk_extra, validate_receipt_proof};

pub struct SpicePreValidationOutput {
    new_chunk_data: NewChunkData,
}

pub fn spice_pre_validate_chunk_state_witness(
    state_witness: &SpiceChunkStateWitness,
    block: &Block,
    prev_block: &Block,
    prev_execution_results: &BlockExecutionResults,
    epoch_manager: &dyn EpochManagerAdapter,
    store: &ChainStore,
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

    let chunks = block.chunks();
    let chunk_header = chunks
        .iter_raw()
        .find(|chunk| chunk.shard_id() == shard_id)
        // TODO(spice): Handle a case of missing chunks by assuming it's empty.
        .unwrap();

    // Ensure that the chunk header version is supported in this protocol version
    let protocol_version = epoch_manager.get_epoch_info(&epoch_id)?.protocol_version();
    chunk_header.validate_version(protocol_version)?;

    let prev_block_header = prev_block.header();

    // TODO(spice-resharding): Handle resharding, same as part of implicit_transition_params in
    // non-spice validation. See
    // get_resharding_transition in c/c/s/stateless_validation/chunk_validation.rs

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

    let new_chunk_data = {
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
            chunk_header.clone().into_spice_chunk_execution_header(&chunk_extra)
        } else {
            let (_, prev_shard_id, _prev_shard_index) =
                epoch_manager.get_prev_shard_id_from_prev_hash(prev_block.hash(), shard_id)?;
            let prev_execution_result = prev_execution_results
                .0
                .get(&prev_shard_id)
                .expect("execution results for all prev_block chunks should be available");
            chunk_header
                .clone()
                .into_spice_chunk_execution_header(&prev_execution_result.chunk_extra)
        };

        let storage_context = StorageContext {
            storage_data_source: StorageDataSource::Recorded(PartialStorage {
                nodes: state_witness.main_state_transition().base_state.clone(),
            }),
            state_patch: Default::default(),
        };
        let is_new_chunk = true;
        NewChunkData {
            chunk_header: spice_chunk_header,
            transactions: SignedValidPeriodTransactions::new(
                state_witness.transactions().to_vec(),
                transaction_validity_check_results,
            ),
            receipts: receipts_to_apply,
            block: Chain::get_apply_chunk_block_context(&block, &prev_block_header, is_new_chunk)?,
            storage_context,
        }
    };

    Ok(SpicePreValidationOutput { new_chunk_data })
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

    assert_eq!(shard_id, pre_validation_output.new_chunk_data.chunk_header.shard_id());

    // TODO(spice): Similar to non-spice validation consider using cache to avoid re-evaluating
    // the same witnesses.
    let (chunk_extra, outgoing_receipts) = {
        let chunk_header = pre_validation_output.new_chunk_data.chunk_header.clone();
        let NewChunkResult { apply_result: mut main_apply_result, .. } = apply_new_chunk(
            ApplyChunkReason::ValidateChunkStateWitness,
            &Span::current(),
            pre_validation_output.new_chunk_data,
            ShardContext { shard_uid, should_apply_chunk: true },
            runtime_adapter,
        )?;
        let outgoing_receipts = std::mem::take(&mut main_apply_result.outgoing_receipts);
        let chunk_extra = apply_result_to_chunk_extra(main_apply_result, &chunk_header);

        (chunk_extra, outgoing_receipts)
    };

    if chunk_extra.state_root() != &state_witness.main_state_transition().post_state_root {
        return Err(Error::InvalidChunkStateWitness(format!(
            "Post state root {:?} for main transition does not match expected post state root {:?}",
            chunk_extra.state_root(),
            state_witness.main_state_transition().post_state_root,
        )));
    }

    // TODO(spice-resharding): Handle possible resharding transitions.

    let shard_layout = epoch_manager.get_shard_layout(&epoch_id)?;
    let outgoing_receipts_hashes = Chain::build_receipts_hashes(&outgoing_receipts, &shard_layout)?;
    let (outgoing_receipts_root, _) = merklize(&outgoing_receipts_hashes);

    let execution_result = ChunkExecutionResult { chunk_extra, outgoing_receipts_root };
    let execution_result_hash = execution_result.compute_hash();
    if &execution_result_hash != state_witness.execution_result_hash() {
        return Err(Error::InvalidChunkStateWitness(format!(
            "Execution result hash {:?} does not match expected execution result hash {:?}",
            execution_result_hash,
            state_witness.execution_result_hash(),
        )));
    }

    Ok(execution_result)
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
    for chunk in prev_block.chunks().iter_raw() {
        let prev_block_shard_id = chunk.shard_id();
        let prev_execution_result = prev_execution_results
            .0
            .get(&prev_block_shard_id)
            .expect("execution results for all prev_block chunks should be available");
        let Some(receipt_proof) = source_receipt_proofs.get(&prev_block_shard_id) else {
            return Err(Error::InvalidChunkStateWitness(format!(
                "Missing source receipt proof for shard {:?}",
                prev_block_shard_id
            )));
        };

        // TODO(spice): Allow validating receipt proofs without chunk available.
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

#[cfg(test)]
mod tests {
    use near_primitives::types::Balance;
    use std::str::FromStr as _;

    use near_async::time::Clock;
    use near_chain_configs::test_genesis::{TestGenesisBuilder, ValidatorsSpec};
    use near_o11y::testonly::init_test_logger;
    use near_primitives::bandwidth_scheduler::BandwidthRequests;
    use near_primitives::congestion_info::CongestionInfo;
    use near_primitives::hash::CryptoHash;
    use near_primitives::receipt::ReceiptPriority;
    use near_primitives::sharding::{ShardChunkHeader, ShardChunkHeaderV3};
    use near_primitives::state::PartialState;
    use near_primitives::stateless_validation::ChunkProductionKey;
    use near_primitives::stateless_validation::spice_state_witness::SpiceChunkStateTransition;
    use near_primitives::test_utils::{
        TestBlockBuilder, create_test_signer, create_user_test_signer,
    };
    use near_primitives::transaction::SignedTransaction;
    use near_primitives::types::chunk_extra::ChunkExtra;
    use near_primitives::types::{
        AccountId, BlockHeight, ChunkExecutionResult, ChunkExecutionResultHash, SpiceChunkId,
    };
    use near_primitives::validator_signer::ValidatorSigner;
    use near_store::get_genesis_state_roots;
    use tracing::Span;

    use crate::test_utils::{get_chain_with_genesis, process_block_sync};
    use crate::types::ApplyChunkResult;
    use crate::{BlockProcessingArtifact, Provenance};

    use super::*;

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
        let spice_chunk_header = test_chain.chunk_header().into_spice_chunk_execution_header(
            &prev_execution_results.0.get(&prev_chunk_header.shard_id()).unwrap().chunk_extra,
        );
        assert_eq!(new_chunk_data.chunk_header, spice_chunk_header);

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
            SpiceChunkStateTransition {
                base_state: PartialState::TrieValues(vec![]),
                post_state_root: CryptoHash::default(),
            },
            HashMap::new(),
            hash(&borsh::to_vec(receipts.as_slice()).unwrap()),
            vec![],
            ChunkExecutionResultHash(CryptoHash::default()),
        );

        let result = spice_pre_validate_chunk_state_witness(
            &invalid_witness,
            &genesis,
            &genesis,
            &BlockExecutionResults(HashMap::new()),
            test_chain.chain.epoch_manager.as_ref(),
            test_chain.chain.chain_store(),
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
            SpiceChunkStateTransition {
                base_state: PartialState::TrieValues(vec![]),
                post_state_root: CryptoHash::default(),
            },
            invalid_source_receipt_proofs,
            hash(&borsh::to_vec(receipts.as_slice()).unwrap()),
            test_chain.transactions(),
            ChunkExecutionResultHash(CryptoHash::default()),
        );

        let result = spice_pre_validate_chunk_state_witness(
            &invalid_witness,
            &block,
            &genesis,
            &BlockExecutionResults(HashMap::new()),
            test_chain.chain.epoch_manager.as_ref(),
            test_chain.chain.chain_store(),
        );

        let error_message = unwrap_error_message(result);
        assert_contains(&error_message, "genesis source_receipt_proofs should be empty");
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
    fn test_validation_fails_with_incorrect_post_main_transition_root() {
        let test_chain = setup();
        let valid_witness = test_chain.valid_witness();
        let main_state_transition = SpiceChunkStateTransition {
            base_state: valid_witness.main_state_transition().base_state.clone(),
            post_state_root: CryptoHash::default(),
        };
        let invalid_witness = TestWitnessBuilder::from_default(valid_witness)
            .main_state_transition(main_state_transition)
            .build();

        let error_message = unwrap_error_message(test_chain.run_validation(invalid_witness));
        assert_contains(&error_message, "main transition does not match expected post state root");
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_validation_fails_with_incorrect_execution_result_hash() {
        let test_chain = setup();
        let valid_witness = test_chain.valid_witness();
        let invalid_witness = TestWitnessBuilder::from_default(valid_witness)
            .execution_result_hash(ChunkExecutionResultHash(CryptoHash::default()))
            .build();

        let error_message = unwrap_error_message(test_chain.run_validation(invalid_witness));
        assert_contains(&error_message, "does not match expected execution result hash");
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
        for _ in 0..3 {
            let block = test_chain.build_block(&prev_block);
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
                ReceiptPriority::NoPriority,
            ),
            Receipt::new_balance_refund(
                &AccountId::from_str(TEST_VALIDATORS[1]).unwrap(),
                Balance::from_yoctonear(100),
                ReceiptPriority::NoPriority,
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
        ShardChunkHeader::V3(ShardChunkHeaderV3::new(
            prev_block_hash,
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            height,
            shard_id,
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            tx_root,
            Default::default(),
            CongestionInfo::default(),
            BandwidthRequests::empty(),
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
        main_state_transition: SpiceChunkStateTransition,
        source_receipt_proofs: HashMap<ShardId, ReceiptProof>,
        applied_receipts_hash: CryptoHash,
        transactions: Vec<SignedTransaction>,
        execution_result_hash: ChunkExecutionResultHash,
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
        builder_setter!(main_state_transition, SpiceChunkStateTransition);
        builder_setter!(source_receipt_proofs, HashMap<ShardId, ReceiptProof>);
        builder_setter!(applied_receipts_hash, CryptoHash);
        builder_setter!(transactions, Vec<SignedTransaction>);
        builder_setter!(execution_result_hash, ChunkExecutionResultHash);

        fn from_default(default: SpiceChunkStateWitness) -> Self {
            Self {
                chunk_id: default.chunk_id().clone(),
                main_state_transition: default.main_state_transition().clone(),
                source_receipt_proofs: default.source_receipt_proofs().clone(),
                applied_receipts_hash: *default.applied_receipts_hash(),
                transactions: default.transactions().to_vec(),
                execution_result_hash: default.execution_result_hash().clone(),
            }
        }

        fn build(self) -> SpiceChunkStateWitness {
            SpiceChunkStateWitness::new(
                self.chunk_id,
                self.main_state_transition,
                self.source_receipt_proofs,
                self.applied_receipts_hash,
                self.transactions,
                self.execution_result_hash,
            )
        }
    }

    struct TestChain {
        chain: Chain,
    }

    impl TestChain {
        fn block(&self) -> Arc<Block> {
            self.chain.get_head_block().unwrap()
        }

        fn prev_block(&self) -> Arc<Block> {
            let block = self.block();
            self.chain.get_block(block.header().prev_hash()).unwrap()
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
            let block_producer = self
                .chain
                .epoch_manager
                .get_block_producer_info(
                    prev_block.header().epoch_id(),
                    prev_block.header().height() + 1,
                )
                .unwrap();
            let signer = Arc::new(create_test_signer(block_producer.account_id().as_str()));
            TestBlockBuilder::new(Clock::real(), prev_block, signer)
                .chunks(chunks)
                .spice_core_statements(vec![])
                .build()
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

        fn source_receipt_proofs(&self) -> HashMap<ShardId, ReceiptProof> {
            let prev_block = self.prev_block();
            let shard_layout = self.shard_layout();
            let chunk_header = self.chunk_header();
            let mut receipt_proofs = HashMap::new();
            for prev_chunk_header in prev_block.chunks().iter_raw() {
                let receipts = self.receipts_for_shard(prev_chunk_header.shard_id());
                let (_root, proofs) = Chain::create_receipts_proofs_from_outgoing_receipts(
                    &shard_layout,
                    prev_chunk_header.shard_id(),
                    receipts,
                )
                .unwrap();
                let proof = proofs
                    .into_iter()
                    .find(|p| p.1.to_shard_id == chunk_header.shard_id())
                    .unwrap();
                receipt_proofs.insert(prev_chunk_header.shard_id(), proof);
            }
            receipt_proofs
        }

        fn prev_execution_results(&self) -> BlockExecutionResults {
            let shard_layout = self.shard_layout();
            let mut prev_execution_results = BlockExecutionResults(HashMap::new());
            let genesis_state_root =
                get_genesis_state_roots(&self.chain.chain_store.store()).unwrap().unwrap()[0];
            for shard_id in shard_layout.shard_ids() {
                let receipts = self.receipts_for_shard(shard_id);
                let (root, _proofs) = Chain::create_receipts_proofs_from_outgoing_receipts(
                    &shard_layout,
                    shard_id,
                    receipts,
                )
                .unwrap();
                prev_execution_results.0.insert(
                    shard_id,
                    Arc::new(ChunkExecutionResult {
                        chunk_extra: ChunkExtra::new_with_only_state_root(&genesis_state_root),
                        outgoing_receipts_root: root,
                    }),
                );
            }
            prev_execution_results
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

            let (transition, execution_result) = self.simulate_chunk_application();
            SpiceChunkStateWitness::new(
                SpiceChunkId { block_hash: *block.hash(), shard_id },
                transition,
                receipt_proofs,
                receipts_hash,
                transactions,
                execution_result.compute_hash(),
            )
        }

        fn run_validation(
            &self,
            state_witness: SpiceChunkStateWitness,
        ) -> Result<ChunkExecutionResult, Error> {
            let block = self.block();
            let prev_block = self.prev_block();
            let prev_execution_results = self.prev_execution_results();
            let pre_validation_output = spice_pre_validate_chunk_state_witness(
                &state_witness,
                &block,
                &prev_block,
                &prev_execution_results,
                self.chain.epoch_manager.as_ref(),
                self.chain.chain_store(),
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
            let block = self.block();
            let prev_block = self.prev_block();
            let prev_execution_results = self.prev_execution_results();
            spice_pre_validate_chunk_state_witness(
                state_witness,
                &block,
                &prev_block,
                &prev_execution_results,
                self.chain.epoch_manager.as_ref(),
                self.chain.chain_store(),
            )
        }

        fn simulate_chunk_application(&self) -> (SpiceChunkStateTransition, ChunkExecutionResult) {
            let prev_execution_results = self.prev_execution_results();
            let chunk_header = self.chunk_header();
            let block = self.block();
            let prev_block = self.prev_block();
            let prev_chunk_header = self.prev_chunk_header();
            let prev_execution_result =
                prev_execution_results.0.get(&prev_chunk_header.shard_id()).unwrap();
            let spice_chunk_header = chunk_header
                .clone()
                .into_spice_chunk_execution_header(&prev_execution_result.chunk_extra);
            let receipts = self.receipts_for_shard(chunk_header.shard_id());
            let is_new_chunk = true;
            let storage_context = StorageContext {
                storage_data_source: StorageDataSource::Db,
                state_patch: Default::default(),
            };

            let shard_uid = shard_id_to_uid(
                self.chain.epoch_manager.as_ref(),
                chunk_header.shard_id(),
                block.header().epoch_id(),
            )
            .unwrap();

            let transactions = self.transactions();
            let txs_validity = std::iter::repeat_n(true, transactions.len()).collect_vec();
            let new_chunk_data = NewChunkData {
                chunk_header: spice_chunk_header,
                transactions: SignedValidPeriodTransactions::new(transactions, txs_validity),
                receipts,
                block: Chain::get_apply_chunk_block_context(
                    &block,
                    &prev_block.header(),
                    is_new_chunk,
                )
                .unwrap(),
                storage_context,
            };
            let NewChunkResult { shard_uid: _, gas_limit, apply_result } = apply_new_chunk(
                ApplyChunkReason::UpdateTrackedShard,
                &Span::none(),
                new_chunk_data,
                ShardContext { shard_uid, should_apply_chunk: true },
                self.chain.runtime_adapter.as_ref(),
            )
            .unwrap();

            let (outcome_root, _) =
                ApplyChunkResult::compute_outcomes_proof(&apply_result.outcomes);
            let chunk_extra = ChunkExtra::new(
                &apply_result.new_root,
                outcome_root,
                apply_result.validator_proposals.clone(),
                apply_result.total_gas_burnt,
                gas_limit,
                apply_result.total_balance_burnt,
                apply_result.congestion_info,
                apply_result.bandwidth_requests.clone(),
            );
            let shard_layout =
                self.chain.epoch_manager.get_shard_layout(block.header().epoch_id()).unwrap();
            let (outgoing_receipts_root, _) = Chain::create_receipts_proofs_from_outgoing_receipts(
                &shard_layout,
                chunk_header.shard_id(),
                apply_result.outgoing_receipts,
            )
            .unwrap();
            let execution_result = ChunkExecutionResult { chunk_extra, outgoing_receipts_root };

            (
                SpiceChunkStateTransition {
                    base_state: apply_result.proof.unwrap().nodes,
                    post_state_root: apply_result.new_root,
                },
                execution_result,
            )
        }
    }
}
