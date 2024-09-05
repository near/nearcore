use std::collections::HashSet;

use near_chain::stateless_validation::chunk_validation::validate_prepared_transactions;
use near_chain::stateless_validation::metrics::{CHUNK_STATE_WITNESS_WITH_CACHE_SIZE, MAIN_STORAGE_PROOF_SIZE};
use near_chain::types::{RuntimeStorageConfig, StorageDataSource};
use near_chain::{Block, BlockHeader};
use near_chain_primitives::Error;
use near_primitives::challenge::{PartialState, TrieValue};
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::{ShardChunk, ShardChunkHeader};
use near_primitives::stateless_validation::state_witness::{ChunkStateWitness, EncodedChunkStateWitness};

use crate::metrics::STATE_CACHE_SIZE;
use crate::Client;

impl Client {
    // Temporary feature to make node produce state witness for every chunk in every processed block
    // and then self-validate it.
    pub(crate) fn shadow_validate_block_chunks(&mut self, block: &Block) -> Result<(), Error> {
        if !cfg!(feature = "shadow_chunk_validation") {
            return Ok(());
        }
        let block_hash = block.hash();
        tracing::debug!(target: "client", ?block_hash, "shadow validation for block chunks");
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
                near_chain::stateless_validation::metrics::SHADOW_CHUNK_VALIDATION_FAILED_TOTAL
                    .inc();
                tracing::error!(
                    target: "client",
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
        let chunk_header = chunk.cloned_header();
        let last_chunk = self.chain.get_chunk(&prev_chunk_header.chunk_hash())?;

        let transactions_validation_storage_config = RuntimeStorageConfig {
            state_root: chunk_header.prev_state_root(),
            use_flat_storage: true,
            source: StorageDataSource::Db,
            state_patch: Default::default(),
        };

        // We call `validate_prepared_transactions()` here because we need storage proof for transactions validation.
        // Normally it is provided by chunk producer, but for shadow validation we need to generate it ourselves.
        let Ok(validated_transactions) = validate_prepared_transactions(
            &self.chain,
            self.runtime_adapter.as_ref(),
            &chunk_header,
            transactions_validation_storage_config,
            chunk.transactions(),
            last_chunk.transactions(),
        ) else {
            return Err(Error::Other(
                "Could not produce storage proof for new transactions".to_owned(),
            ));
        };

        let witness = self.create_state_witness(
            // Setting arbitrary chunk producer is OK for shadow validation
            "alice.near".parse().unwrap(),
            prev_block_header,
            prev_chunk_header,
            chunk,
            validated_transactions.storage_proof,
        )?;
        self.apply_state_caching(witness.clone());
        if self.config.save_latest_witnesses {
            self.chain.chain_store.save_latest_chunk_state_witness(&witness)?;
        }
        self.chain.shadow_validate_state_witness(
            witness,
            self.epoch_manager.as_ref(),
            self.runtime_adapter.as_ref(),
            None,
        )?;
        Ok(())
    }

    fn apply_state_caching(&mut self, mut witness: ChunkStateWitness) {
        let shard_id = witness.chunk_header.shard_id();
        let PartialState::TrieValues(values) = &mut witness.main_state_transition.base_state;
        let cache = self.state_cache.entry(shard_id).or_default();
        MAIN_STORAGE_PROOF_SIZE
            .with_label_values(&[&shard_id.to_string(), "no_cache"])
            .observe(borsh::to_vec(values).unwrap().len() as f64);
        *values = Self::filter_state_values(cache, std::mem::take(values));
        MAIN_STORAGE_PROOF_SIZE
            .with_label_values(&[&shard_id.to_string(), "with_cache"])
            .observe(borsh::to_vec(values).unwrap().len() as f64);
        STATE_CACHE_SIZE.with_label_values(&[&shard_id.to_string()]).set(cache.len() as i64);
        let (encoded_witness, _raw_witness_size) =
            EncodedChunkStateWitness::encode(&witness).unwrap();
        CHUNK_STATE_WITNESS_WITH_CACHE_SIZE
            .with_label_values(&[&shard_id.to_string()])
            .observe(encoded_witness.size_bytes() as f64);
    }

    fn filter_state_values(
        cache: &mut HashSet<CryptoHash>,
        values: Vec<TrieValue>,
    ) -> Vec<TrieValue> {
        const SIZE_THRESHOLD: usize = 64000;
        let mut ret = Vec::new();
        for val in values {
            if val.len() >= SIZE_THRESHOLD {
                let key = CryptoHash::hash_bytes(val.as_ref());
                if !cache.insert(key) {
                    continue;
                }
            }
            ret.push(val);
        }
        ret
    }
}
