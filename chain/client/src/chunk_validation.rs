use near_async::messaging::{CanSend, Sender};
use near_chain::migrations::check_if_block_is_first_with_chunk_of_version;
use near_chain::sharding::shuffle_receipt_proofs;
use near_chain::types::{
    ApplyChunkBlockContext, ApplyChunkResult, ApplyChunkShardContext, RuntimeAdapter,
    RuntimeStorageConfig, StorageDataSource,
};
use near_chain::validate::validate_chunk_with_chunk_extra_and_receipts_root;
use near_chain::{Block, BlockHeader, Chain, ChainStore, ChainStoreAccess};
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_network::types::{NetworkRequests, PeerManagerMessageRequest};
use near_primitives::challenge::PartialState;
use near_primitives::checked_feature;
use near_primitives::chunk_validation::{
    ChunkEndorsement, ChunkEndorsementInner, ChunkStateTransition, ChunkStateWitness,
};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::merkle::merklize;
use near_primitives::receipt::Receipt;
use near_primitives::sharding::{ShardChunk, ShardChunkHeader};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{EpochId, ShardId};
use near_primitives::validator_signer::ValidatorSigner;
use near_store::PartialStorage;
use std::collections::HashMap;
use std::sync::Arc;

use crate::Client;

/// A module that handles chunk validation logic. Chunk validation refers to a
/// critical process of stateless validation, where chunk validators (certain
/// validators selected to validate the chunk) verify that the chunk's state
/// witness is correct, and then send chunk endorsements to the block producer
/// so that the chunk can be included in the block.
pub struct ChunkValidator {
    /// The signer for our own node, if we are a validator. If not, this is None.
    my_signer: Option<Arc<dyn ValidatorSigner>>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    network_sender: Sender<PeerManagerMessageRequest>,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
}

impl ChunkValidator {
    pub fn new(
        my_signer: Option<Arc<dyn ValidatorSigner>>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        network_sender: Sender<PeerManagerMessageRequest>,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
    ) -> Self {
        Self { my_signer, epoch_manager, network_sender, runtime_adapter }
    }

    /// Performs the chunk validation logic. When done, it will send the chunk
    /// endorsement message to the block producer. The actual validation logic
    /// happens in a separate thread.
    pub fn start_validating_chunk(
        &self,
        state_witness: ChunkStateWitness,
        chain_store: &ChainStore,
    ) -> Result<(), Error> {
        let Some(my_signer) = self.my_signer.as_ref() else {
            return Err(Error::NotAValidator);
        };
        let chunk_header = state_witness.chunk_header.clone();
        let epoch_id =
            self.epoch_manager.get_epoch_id_from_prev_block(chunk_header.prev_block_hash())?;
        // We will only validate something if we are a chunk validator for this chunk.
        // Note this also covers the case before the protocol upgrade for chunk validators,
        // because the chunk validators will be empty.
        let chunk_validators = self.epoch_manager.get_chunk_validators(
            &epoch_id,
            chunk_header.shard_id(),
            chunk_header.height_created(),
        )?;
        if !chunk_validators.contains_key(my_signer.validator_id()) {
            return Err(Error::NotAChunkValidator);
        }

        let pre_validation_result = pre_validate_chunk_state_witness(
            &state_witness,
            chain_store,
            self.epoch_manager.as_ref(),
        )?;

        let block_producer =
            self.epoch_manager.get_block_producer(&epoch_id, chunk_header.height_created())?;

        let network_sender = self.network_sender.clone();
        let signer = self.my_signer.clone().unwrap();
        let epoch_manager = self.epoch_manager.clone();
        let runtime_adapter = self.runtime_adapter.clone();
        rayon::spawn(move || {
            match validate_chunk_state_witness(
                state_witness,
                pre_validation_result,
                epoch_manager.as_ref(),
                runtime_adapter.as_ref(),
            ) {
                Ok(()) => {
                    tracing::debug!(
                        target: "chunk_validation",
                        chunk_hash=?chunk_header.chunk_hash(),
                        block_producer=%block_producer,
                        "Chunk validated successfully, sending endorsement",
                    );
                    let endorsement_to_sign = ChunkEndorsementInner::new(chunk_header.chunk_hash());
                    let endorsement = ChunkEndorsement {
                        account_id: signer.validator_id().clone(),
                        signature: signer.sign_chunk_endorsement(&endorsement_to_sign),
                        inner: endorsement_to_sign,
                    };
                    network_sender.send(PeerManagerMessageRequest::NetworkRequests(
                        NetworkRequests::ChunkEndorsement(block_producer, endorsement),
                    ));
                }
                Err(err) => {
                    tracing::error!("Failed to validate chunk: {:?}", err);
                }
            }
        });
        Ok(())
    }
}

/// Pre-validates the chunk's receipts and transactions against the chain.
/// We do this before handing off the computationally intensive part to a
/// validation thread.
fn pre_validate_chunk_state_witness(
    state_witness: &ChunkStateWitness,
    store: &ChainStore,
    epoch_manager: &dyn EpochManagerAdapter,
) -> Result<PreValidationOutput, Error> {
    let shard_id = state_witness.chunk_header.shard_id();

    // First, go back through the blockchain history to locate the last new chunk
    // and last last new chunk for the shard.

    // Blocks from the last new chunk (exclusive) to the parent block (inclusive).
    let mut blocks_after_last_chunk = Vec::new();
    // Blocks from the last last new chunk (exclusive) to the last new chunk (inclusive).
    let mut blocks_after_last_last_chunk = Vec::new();

    {
        let mut block_hash = *state_witness.chunk_header.prev_block_hash();
        let mut prev_chunks_seen = 0;
        loop {
            let block = store.get_block(&block_hash)?;
            let chunks = block.chunks();
            let Some(chunk) = chunks.get(shard_id as usize) else {
                return Err(Error::InvalidChunkStateWitness(format!(
                    "Shard {} does not exist in block {:?}",
                    shard_id, block_hash
                )));
            };
            block_hash = *block.header().prev_hash();
            if chunk.is_new_chunk() {
                prev_chunks_seen += 1;
            }
            if prev_chunks_seen == 0 {
                blocks_after_last_chunk.push(block);
            } else if prev_chunks_seen == 1 {
                blocks_after_last_last_chunk.push(block);
            }
            if prev_chunks_seen == 2 {
                break;
            }
        }
    }

    // Compute the chunks from which receipts should be collected.
    let mut chunks_to_collect_receipts_from = Vec::new();
    for block in blocks_after_last_last_chunk.iter().rev() {
        // To stay consistent with the order in which receipts are applied,
        // blocks are iterated in reverse order (from new to old), and
        // chunks are shuffled for each block.
        let mut chunks_in_block = block
            .chunks()
            .iter()
            .map(|chunk| (chunk.chunk_hash(), chunk.prev_outgoing_receipts_root()))
            .collect::<Vec<_>>();
        shuffle_receipt_proofs(&mut chunks_in_block, block.hash());
        chunks_to_collect_receipts_from.extend(chunks_in_block);
    }

    // Verify that for each chunk, the receipts that have been provided match
    // the receipts that we are expecting.
    let mut receipts_to_apply = Vec::new();
    for (chunk_hash, receipt_root) in chunks_to_collect_receipts_from {
        let Some(receipt_proof) = state_witness.source_receipt_proofs.get(&chunk_hash) else {
            return Err(Error::InvalidChunkStateWitness(format!(
                "Missing source receipt proof for chunk {:?}",
                chunk_hash
            )));
        };
        if !receipt_proof.verify_against_receipt_root(receipt_root) {
            return Err(Error::InvalidChunkStateWitness(format!(
                "Provided receipt proof failed verification against receipt root for chunk {:?}",
                chunk_hash
            )));
        }
        // TODO(#10265): This does not currently handle shard layout change.
        if receipt_proof.1.to_shard_id != shard_id {
            return Err(Error::InvalidChunkStateWitness(format!(
                "Receipt proof for chunk {:?} is for shard {}, expected shard {}",
                chunk_hash, receipt_proof.1.to_shard_id, shard_id
            )));
        }
        receipts_to_apply.extend(receipt_proof.0.iter().cloned());
    }
    let exact_receipts_hash = hash(&borsh::to_vec(&receipts_to_apply).unwrap());
    if exact_receipts_hash != state_witness.exact_receipts_hash {
        return Err(Error::InvalidChunkStateWitness(format!(
            "Receipts hash {:?} does not match expected receipts hash {:?}",
            exact_receipts_hash, state_witness.exact_receipts_hash
        )));
    }
    let tx_root_from_state_witness = hash(&borsh::to_vec(&state_witness.transactions).unwrap());
    let block_of_last_new_chunk = blocks_after_last_chunk.last().unwrap();
    let last_new_chunk_tx_root =
        block_of_last_new_chunk.chunks().get(shard_id as usize).unwrap().tx_root();
    if last_new_chunk_tx_root != tx_root_from_state_witness {
        return Err(Error::InvalidChunkStateWitness(format!(
            "Transaction root {:?} does not match expected transaction root {:?}",
            tx_root_from_state_witness, last_new_chunk_tx_root
        )));
    }

    Ok(PreValidationOutput {
        receipts_to_apply,
        main_transition_params: get_state_transition_validation_params(
            store,
            epoch_manager,
            block_of_last_new_chunk,
            shard_id,
        )?,
        implicit_transition_params: blocks_after_last_chunk
            .into_iter()
            .map(|block| {
                get_state_transition_validation_params(store, epoch_manager, &block, shard_id)
            })
            .collect::<Result<_, _>>()?,
    })
}

fn get_state_transition_validation_params(
    store: &ChainStore,
    epoch_manager: &dyn EpochManagerAdapter,
    block: &Block,
    shard_id: ShardId,
) -> Result<ChunkStateTransitionValidationParams, Error> {
    let chunks = block.chunks();
    let chunk = chunks.get(shard_id as usize).ok_or_else(|| {
        Error::InvalidChunkStateWitness(format!(
            "Shard {} does not exist in block {:?}",
            shard_id,
            block.hash()
        ))
    })?;
    let is_first_block_with_chunk_of_version = check_if_block_is_first_with_chunk_of_version(
        store,
        epoch_manager,
        block.header().prev_hash(),
        shard_id,
    )?;
    let prev_block_header = store.get_block_header(&block.header().prev_hash())?;
    Ok(ChunkStateTransitionValidationParams {
        chunk: chunk.clone(),
        block: block.header().clone(),
        gas_price: prev_block_header.next_gas_price(),
        is_first_block_with_chunk_of_version,
    })
}

struct ChunkStateTransitionValidationParams {
    chunk: ShardChunkHeader,
    block: BlockHeader,
    gas_price: u128,
    is_first_block_with_chunk_of_version: bool,
}

struct PreValidationOutput {
    receipts_to_apply: Vec<Receipt>,
    main_transition_params: ChunkStateTransitionValidationParams,
    implicit_transition_params: Vec<ChunkStateTransitionValidationParams>,
}

fn validate_chunk_state_witness(
    state_witness: ChunkStateWitness,
    pre_validation_output: PreValidationOutput,
    epoch_manager: &dyn EpochManagerAdapter,
    runtime_adapter: &dyn RuntimeAdapter,
) -> Result<(), Error> {
    let main_transition = pre_validation_output.main_transition_params;
    let runtime_storage_config = RuntimeStorageConfig {
        record_storage: false,
        source: StorageDataSource::Recorded(PartialStorage {
            nodes: state_witness.main_state_transition.base_state,
        }),
        state_patch: Default::default(),
        state_root: main_transition.chunk.prev_state_root(),
        use_flat_storage: true,
    };
    let mut main_apply_result = runtime_adapter.apply_chunk(
        runtime_storage_config,
        ApplyChunkShardContext {
            gas_limit: main_transition.chunk.gas_limit(),
            is_first_block_with_chunk_of_version: main_transition
                .is_first_block_with_chunk_of_version,
            is_new_chunk: true,
            last_validator_proposals: main_transition.chunk.prev_validator_proposals(),
            shard_id: main_transition.chunk.shard_id(),
        },
        ApplyChunkBlockContext::from_header(&main_transition.block, main_transition.gas_price),
        &pre_validation_output.receipts_to_apply,
        &state_witness.transactions,
    )?;
    let outgoing_receipts = std::mem::take(&mut main_apply_result.outgoing_receipts);
    let mut chunk_extra = apply_result_to_chunk_extra(main_apply_result, &main_transition.chunk);
    if chunk_extra.state_root() != &state_witness.main_state_transition.post_state_root {
        // This is an early check, it's not for correctness, only for better
        // error reporting in case of an invalid state witness due to a bug.
        // Only the final state root check against the chunk header is required.
        return Err(Error::InvalidChunkStateWitness(format!(
            "Post state root {:?} for main transition does not match expected post state root {:?}",
            chunk_extra.state_root(),
            state_witness.main_state_transition.post_state_root,
        )));
    }

    for (transition_params, transition) in pre_validation_output
        .implicit_transition_params
        .into_iter()
        .zip(state_witness.implicit_transitions.into_iter())
    {
        let runtime_storage_config = RuntimeStorageConfig {
            record_storage: false,
            source: StorageDataSource::Recorded(PartialStorage { nodes: transition.base_state }),
            state_patch: Default::default(),
            state_root: *chunk_extra.state_root(),
            use_flat_storage: true,
        };
        let apply_result = runtime_adapter.apply_chunk(
            runtime_storage_config,
            ApplyChunkShardContext {
                gas_limit: transition_params.chunk.gas_limit(),
                is_first_block_with_chunk_of_version: transition_params
                    .is_first_block_with_chunk_of_version,
                is_new_chunk: false,
                last_validator_proposals: transition_params.chunk.prev_validator_proposals(),
                shard_id: transition_params.chunk.shard_id(),
            },
            ApplyChunkBlockContext::from_header(
                &transition_params.block,
                transition_params.gas_price,
            ),
            &[],
            &[],
        )?;
        *chunk_extra.state_root_mut() = apply_result.new_root;
        if chunk_extra.state_root() != &transition.post_state_root {
            // This is an early check, it's not for correctness, only for better
            // error reporting in case of an invalid state witness due to a bug.
            // Only the final state root check against the chunk header is required.
            return Err(Error::InvalidChunkStateWitness(format!(
                "Post state root {:?} for implicit transition at chunk {:?}, does not match expected state root {:?}",
                chunk_extra.state_root(), transition_params.chunk.chunk_hash(), transition.post_state_root
            )));
        }
    }

    // Finally, verify that the newly proposed chunk matches everything we have computed.
    let outgoing_receipts_hashes = {
        let shard_layout = epoch_manager
            .get_shard_layout_from_prev_block(state_witness.chunk_header.prev_block_hash())?;
        Chain::build_receipts_hashes(&outgoing_receipts, &shard_layout)
    };
    let (outgoing_receipts_root, _) = merklize(&outgoing_receipts_hashes);
    validate_chunk_with_chunk_extra_and_receipts_root(
        &chunk_extra,
        &state_witness.chunk_header,
        &outgoing_receipts_root,
    )?;

    // Before we're done we have one last thing to do: verify that the proposed transactions
    // are valid.
    // TODO(#9292): Not sure how to do this.

    Ok(())
}

fn apply_result_to_chunk_extra(
    apply_result: ApplyChunkResult,
    chunk: &ShardChunkHeader,
) -> ChunkExtra {
    let (outcome_root, _) = ApplyChunkResult::compute_outcomes_proof(&apply_result.outcomes);
    ChunkExtra::new(
        &apply_result.new_root,
        outcome_root,
        apply_result.validator_proposals,
        apply_result.total_gas_burnt,
        chunk.gas_limit(),
        apply_result.total_balance_burnt,
    )
}

impl Client {
    /// Responds to a network request to verify a `ChunkStateWitness`, which is
    /// sent by chunk producers after they produce a chunk.
    pub fn process_chunk_state_witness(&mut self, witness: ChunkStateWitness) -> Result<(), Error> {
        // TODO(#10265): If the previous block does not exist, we should
        // queue this (similar to orphans) to retry later.
        self.chunk_validator.start_validating_chunk(witness, self.chain.chain_store())
    }

    /// Distributes the chunk state witness to chunk validators that are
    /// selected to validate this chunk.
    pub fn send_chunk_state_witness_to_chunk_validators(
        &mut self,
        epoch_id: &EpochId,
        chunk_header: &ShardChunkHeader,
        chunk: &ShardChunk,
    ) -> Result<(), Error> {
        let protocol_version = self.epoch_manager.get_epoch_protocol_version(epoch_id)?;
        if !checked_feature!("stable", ChunkValidation, protocol_version) {
            return Ok(());
        }
        let chunk_validators = self.epoch_manager.get_chunk_validators(
            epoch_id,
            chunk_header.shard_id(),
            chunk_header.height_created(),
        )?;
        let witness = ChunkStateWitness {
            chunk_header: chunk_header.clone(),
            main_state_transition: ChunkStateTransition {
                // TODO(#9292): Fetch from StoredChunkStateTransitionData.
                base_state: PartialState::default(),
                // TODO(#9292): Block hash of the last new chunk.
                block_hash: CryptoHash::default(),
                // TODO(#9292): Fetch from ChunkExtra of the last new chunk.
                post_state_root: CryptoHash::default(),
            },
            // TODO(#9292): Iterate through the chain to derive this.
            source_receipt_proofs: HashMap::new(),
            // TODO(#9292): Include transactions from the last new chunk.
            transactions: Vec::new(),
            // TODO(#9292): Fetch from StoredChunkStateTransitionData.
            // (Could also be derived from iterating through the receipts, but
            // that defeats the purpose of this check being a debugging
            // mechanism.)
            exact_receipts_hash: CryptoHash::default(),
            // TODO(#9292): Fetch from StoredChunkStateTransitionData for each missing
            // chunk.
            implicit_transitions: Vec::new(),
            new_transactions: chunk.transactions().to_vec(),
            // TODO(#9292): Derive this during chunk production, during
            // prepare_transactions or the like.
            new_transactions_validation_state: PartialState::default(),
        };
        tracing::debug!(
            target: "chunk_validation",
            "Sending chunk state witness for chunk {:?} to chunk validators {:?}",
            chunk_header.chunk_hash(),
            chunk_validators.keys(),
        );
        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::ChunkStateWitness(chunk_validators.into_keys().collect(), witness),
        ));
        Ok(())
    }

    /// Function to process an incoming chunk endorsement from chunk validators.
    pub fn process_chunk_endorsement(
        &mut self,
        _endorsement: ChunkEndorsement,
    ) -> Result<(), Error> {
        // TODO(10265): Here if we are the current block producer, we would store the chunk endorsement
        // for each chunk which would later be used during block production to check whether to include the
        // chunk or not.
        Ok(())
    }
}
