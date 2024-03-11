use std::time::Instant;

use near_chain::types::{RuntimeStorageConfig, StorageDataSource};
use near_chain::{Block, BlockHeader};
use near_chain_primitives::Error;
use near_primitives::challenge::PartialState;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::{ShardChunk, ShardChunkHeader};
use near_primitives::stateless_validation::{ChunkStateTransition, ChunkStateWitnessInner};
use near_primitives::types::ShardId;

use crate::metrics::STATE_VALUES_CACHE_SIZE;
use crate::stateless_validation::chunk_validator::{
    pre_validate_chunk_state_witness, validate_chunk_state_witness, validate_prepared_transactions,
};
use crate::{metrics, Client};

impl Client {
    // Temporary feature to make node produce state witness for every chunk in every processed block
    // and then self-validate it.
    pub(crate) fn shadow_validate_block_chunks(&mut self, block: &Block) -> Result<(), Error> {
        if !cfg!(feature = "shadow_chunk_validation") {
            return Ok(());
        }
        let block_hash = block.hash();
        tracing::debug!(target: "stateless_validation", ?block_hash, "shadow validation for block chunks");
        let prev_block = self.chain.get_block(block.header().prev_hash())?;
        let prev_block_chunks = prev_block.chunks();
        for chunk in
            block.chunks().iter().filter(|chunk| chunk.is_new_chunk(block.header().height()))
        {
            let chunk = self.chain.get_chunk_clone_from_header(chunk)?;
            let prev_chunk_header = prev_block_chunks.get(chunk.shard_id() as usize).unwrap();
            if let Err(err) =
                self.shadow_validate_chunk(prev_block.header(), prev_chunk_header, &chunk)
            {
                metrics::SHADOW_CHUNK_VALIDATION_FAILED_TOTAL.inc();
                tracing::error!(
                    target: "stateless_validation",
                    ?err,
                    shard_id = chunk.shard_id(),
                    ?block_hash,
                    "shadow chunk validation failed"
                );
            }
        }
        Ok(())
    }

    fn shadow_validate_chunk(
        &mut self,
        prev_block_header: &BlockHeader,
        prev_chunk_header: &ShardChunkHeader,
        chunk: &ShardChunk,
    ) -> Result<(), Error> {
        let shard_id = chunk.shard_id();
        let chunk_hash = chunk.chunk_hash();
        let chunk_header = chunk.cloned_header();

        let transactions_validation_storage_config = RuntimeStorageConfig {
            state_root: chunk_header.prev_state_root(),
            use_flat_storage: true,
            source: StorageDataSource::Db,
            state_patch: Default::default(),
            record_storage: true,
        };

        // We call `validate_prepared_transactions()` here because we need storage proof for transactions validation.
        // Normally it is provided by chunk producer, but for shadow validation we need to generate it ourselves.
        let Ok(validated_transactions) = validate_prepared_transactions(
            &self.chain,
            self.runtime_adapter.as_ref(),
            &chunk_header,
            transactions_validation_storage_config,
            chunk.transactions(),
        ) else {
            return Err(Error::Other(
                "Could not produce storage proof for new transactions".to_owned(),
            ));
        };

        let witness = self.create_state_witness_inner(
            prev_block_header,
            prev_chunk_header,
            chunk,
            validated_transactions.storage_proof,
        )?;
        let witness_size = borsh::to_vec(&witness)?.len();
        metrics::CHUNK_STATE_WITNESS_TOTAL_SIZE
            .with_label_values(&[&shard_id.to_string()])
            .observe(witness_size as f64);
        self.apply_witness_state_cache(witness.clone());
        let pre_validation_start = Instant::now();
        let pre_validation_result = pre_validate_chunk_state_witness(
            &witness,
            &self.chain,
            self.epoch_manager.as_ref(),
            self.runtime_adapter.as_ref(),
        )?;
        tracing::debug!(
            target: "stateless_validation",
            shard_id,
            ?chunk_hash,
            witness_size,
            pre_validation_elapsed = ?pre_validation_start.elapsed(),
            "completed shadow chunk pre-validation"
        );
        let epoch_manager = self.epoch_manager.clone();
        let runtime_adapter = self.runtime_adapter.clone();
        rayon::spawn(move || {
            let validation_start = Instant::now();
            match validate_chunk_state_witness(
                witness,
                pre_validation_result,
                epoch_manager.as_ref(),
                runtime_adapter.as_ref(),
            ) {
                Ok(()) => {
                    tracing::debug!(
                        target: "stateless_validation",
                        shard_id,
                        ?chunk_hash,
                        validation_elapsed = ?validation_start.elapsed(),
                        "completed shadow chunk validation"
                    );
                }
                Err(err) => {
                    metrics::SHADOW_CHUNK_VALIDATION_FAILED_TOTAL.inc();
                    tracing::error!(
                        target: "stateless_validation",
                        ?err,
                        shard_id,
                        ?chunk_hash,
                        "shadow chunk validation failed"
                    );
                }
            }
        });
        Ok(())
    }

    fn apply_witness_state_cache(
        &mut self,
        mut witness: ChunkStateWitnessInner
    ) {
        let shard_id = witness.chunk_header.shard_id();
        self.apply_transition_state_cache(shard_id, &mut witness.main_state_transition);
        for transition in witness.implicit_transitions.iter_mut() {
            self.apply_transition_state_cache(shard_id, transition);
        }
        let witness_size = borsh::to_vec(&witness).unwrap().len();
        metrics::CHUNK_STATE_WITNESS_CACHED_STATE_PROOF_SIZE
            .with_label_values(&[&shard_id.to_string()])
            .observe(witness_size as f64);
    }

    fn apply_transition_state_cache(
        &mut self,
        shard_id: ShardId,
        transition: &mut ChunkStateTransition
    ) {
        const CUT_OFF_VALUE_SIZE: usize = 8000;
        const MAX_CACHE_SIZE: usize = 1000;
        let cache = self.state_cache.entry(shard_id).or_default();
        let PartialState::TrieValues(values) = &mut transition.base_state;
        values.retain(|v| v.len() < CUT_OFF_VALUE_SIZE || !cache.contains(&CryptoHash::hash_bytes(v.as_ref())));
        values.sort_by_key(|v| v.len());
        for v in values.iter().rev() {
            if cache.len() < MAX_CACHE_SIZE && v.len() >= CUT_OFF_VALUE_SIZE {
                cache.insert(CryptoHash::hash_bytes(v.as_ref()));
            }
        }
        STATE_VALUES_CACHE_SIZE
            .with_label_values(&[&shard_id.to_string()])
            .set(cache.len() as i64);
    }
}
