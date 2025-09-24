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
            let (_, prev_shard_id, _prev_shard_index) =
                epoch_manager.get_prev_shard_id_from_prev_hash(prev_block.hash(), shard_id)?;
            let prev_execution_result = prev_execution_results
                .0
                .get(&prev_shard_id)
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

    let prev_block_shard_layout = epoch_manager.get_shard_layout(prev_block.header().epoch_id())?;

    if source_receipt_proofs.len() as u64 != prev_block_shard_layout.num_shards() {
        return Err(Error::InvalidChunkStateWitness(format!(
            "source_receipt_proofs contains incorrect number of proofs. Expected {} proofs, found {}",
            prev_execution_results.0.len(),
            prev_block.chunks().len(),
        )));
    }

    let mut receipt_proofs = Vec::new();
    for chunk in prev_block.chunks().iter_raw() {
        let prev_block_shard_id = chunk.shard_id();
        let chunk_hash = chunk.chunk_hash();
        let prev_execution_result = prev_execution_results
            .0
            .get(&prev_block_shard_id)
            .expect("execution results for all prev_block chunks should be available");

        // TODO(spice): Adjust witness to have source_receipt_proofs keyed by shard id which would
        // allow it work in case of missing chunks.
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

#[cfg(test)]
mod tests {
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
    use near_primitives::stateless_validation::state_witness::ChunkStateTransition;
    use near_primitives::test_utils::{
        TestBlockBuilder, create_test_signer, create_user_test_signer,
    };
    use near_primitives::transaction::SignedTransaction;
    use near_primitives::types::chunk_extra::ChunkExtra;
    use near_primitives::types::{AccountId, BlockHeight, ChunkExecutionResult, EpochId};
    use near_primitives::validator_signer::ValidatorSigner;
    use near_primitives::version::PROTOCOL_VERSION;

    use crate::test_utils::{get_chain_with_genesis, process_block_sync};
    use crate::{BlockProcessingArtifact, Provenance};

    use super::*;

    const TEST_VALIDATORS: [&str; 2] = ["test-validator-1", "test-validator-2"];

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_pre_validation_output_is_correct() {
        let test_chain = setup();
        let witness = test_chain.valid_witness();

        let output = test_chain.run_pre_validation(&witness).unwrap();
        assert!(output.implicit_transition_params.is_empty());
        let MainTransition::NewChunk { new_chunk_data, block_hash: _ } =
            output.main_transition_params
        else {
            unreachable!()
        };

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
        assert_eq!(new_chunk_data_transactions, transactions);
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_pre_validation_fails_with_incorrect_epoch_id() {
        let test_chain = setup();
        let valid_witness = test_chain.valid_witness();

        let invalid_epoch_id =
            EpochId::from_str("32222222222233333333334444444444445555555777").unwrap();
        let invalid_witness =
            TestWitnessBuilder::from_default(valid_witness).epoch_id(invalid_epoch_id).build();

        let error_message = unwrap_error_message(test_chain.run_pre_validation(&invalid_witness));
        assert_contains(&error_message, "does not match epoch id");
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_pre_validation_fails_with_unrelated_chunk_header() {
        let test_chain = setup();
        let valid_witness = test_chain.valid_witness();

        let invalid_chunk_header = test_chain.prev_chunk_header();
        let invalid_witness = TestWitnessBuilder::from_default(valid_witness)
            .chunk_header(invalid_chunk_header)
            .build();

        let error_message = unwrap_error_message(test_chain.run_pre_validation(&invalid_witness));
        assert_contains(&error_message, "doesn't contain state witness' chunk header");
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
        let invalid_receipt_proofs = test_chain
            // We use block's chunks instead of prev block's chunks (which would be valid keys)
            .block()
            .chunks()
            .iter_raw()
            .map(|chunk| -> (ChunkHash, ReceiptProof) {
                (chunk.chunk_hash().clone(), proof.clone())
            })
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
        let chunk_header = genesis.chunks()[0].clone();

        let receipts: Vec<Receipt> = Vec::new();
        let invalid_witness = ChunkStateWitness::new(
            AccountId::from_str("unused").unwrap(),
            *genesis.header().epoch_id(),
            chunk_header,
            ChunkStateTransition {
                block_hash: *genesis.hash(),
                base_state: PartialState::TrieValues(vec![]),
                post_state_root: CryptoHash::default(),
            },
            HashMap::new(),
            hash(&borsh::to_vec(receipts.as_slice()).unwrap()),
            vec![],
            vec![],
            vec![],
            PROTOCOL_VERSION,
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
        let chunk_header = block.chunks()[0].clone();

        let receipts: Vec<Receipt> = Vec::new();
        let proof = test_chain.source_receipt_proofs().into_values().next().unwrap();
        let invalid_source_receipt_proofs =
            HashMap::from([(chunk_header.chunk_hash().clone(), proof)]);
        let invalid_witness = ChunkStateWitness::new(
            AccountId::from_str("unused").unwrap(),
            *block.header().epoch_id(),
            chunk_header,
            ChunkStateTransition {
                block_hash: *block.hash(),
                base_state: PartialState::TrieValues(vec![]),
                post_state_root: CryptoHash::default(),
            },
            invalid_source_receipt_proofs,
            hash(&borsh::to_vec(receipts.as_slice()).unwrap()),
            test_chain.transactions(),
            vec![],
            vec![],
            PROTOCOL_VERSION,
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
    fn unwrap_error_message(result: Result<PreValidationOutput, Error>) -> String {
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
                100,
                ReceiptPriority::NoPriority,
            ),
            Receipt::new_balance_refund(
                &AccountId::from_str(TEST_VALIDATORS[1]).unwrap(),
                100,
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

        vec![send_money(100), send_money(200), send_money(300)]
    }

    struct TestWitnessBuilder {
        epoch_id: EpochId,
        chunk_header: ShardChunkHeader,
        main_state_transition: ChunkStateTransition,
        source_receipt_proofs: HashMap<ChunkHash, ReceiptProof>,
        applied_receipts_hash: CryptoHash,
        transactions: Vec<SignedTransaction>,
        implicit_transitions: Vec<ChunkStateTransition>,
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
        builder_setter!(epoch_id, EpochId);
        builder_setter!(chunk_header, ShardChunkHeader);
        builder_setter!(source_receipt_proofs, HashMap<ChunkHash, ReceiptProof>);
        builder_setter!(applied_receipts_hash, CryptoHash);
        builder_setter!(transactions, Vec<SignedTransaction>);

        fn from_default(default: ChunkStateWitness) -> Self {
            Self {
                epoch_id: *default.epoch_id(),
                chunk_header: default.chunk_header().clone(),
                main_state_transition: default.main_state_transition().clone(),
                source_receipt_proofs: default.source_receipt_proofs().clone(),
                applied_receipts_hash: *default.applied_receipts_hash(),
                transactions: default.transactions().clone(),
                implicit_transitions: default.implicit_transitions().clone(),
            }
        }

        fn build(self) -> ChunkStateWitness {
            let new_transactions = vec![];
            ChunkStateWitness::new(
                AccountId::from_str("unused").unwrap(),
                self.epoch_id,
                self.chunk_header,
                self.main_state_transition,
                self.source_receipt_proofs,
                self.applied_receipts_hash,
                self.transactions,
                self.implicit_transitions,
                new_transactions,
                PROTOCOL_VERSION,
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

        fn source_receipt_proofs(&self) -> HashMap<ChunkHash, ReceiptProof> {
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
                receipt_proofs.insert(prev_chunk_header.chunk_hash().clone(), proof);
            }
            receipt_proofs
        }

        fn prev_execution_results(&self) -> BlockExecutionResults {
            let shard_layout = self.shard_layout();
            let mut prev_execution_results = BlockExecutionResults(HashMap::new());
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
                        chunk_extra: ChunkExtra::new_with_only_state_root(&CryptoHash::default()),
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

        fn valid_witness(&self) -> ChunkStateWitness {
            let block = self.block();
            let chunk_header = self.chunk_header();
            let receipt_proofs = self.source_receipt_proofs();
            let receipts_hash = self.applied_receipts_hash();
            let transactions = self.transactions();

            let epoch_id = block.header().epoch_id();
            let implicit_transitions = vec![];
            let new_transactions = vec![];
            ChunkStateWitness::new(
                AccountId::from_str("unused").unwrap(),
                *epoch_id,
                chunk_header,
                ChunkStateTransition {
                    block_hash: *block.hash(),
                    // We only care about pre-validation validity, not overall validity.
                    base_state: PartialState::TrieValues(vec![]),
                    post_state_root: CryptoHash::default(),
                },
                receipt_proofs,
                receipts_hash,
                transactions,
                implicit_transitions,
                new_transactions,
                PROTOCOL_VERSION,
            )
        }

        fn run_pre_validation(
            &self,
            state_witness: &ChunkStateWitness,
        ) -> Result<PreValidationOutput, Error> {
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
    }
}
