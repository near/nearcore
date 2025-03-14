use crate::Client;
use crate::stateless_validation::state_witness_producer::CreateWitnessResult;
use near_chain::get_chunk_clone_from_header;
use near_chain::stateless_validation::chunk_validation::validate_prepared_transactions;
use near_chain::types::{RuntimeStorageConfig, StorageDataSource};
use near_chain::{Block, BlockHeader};
use near_chain_primitives::Error;
use near_primitives::sharding::{ShardChunk, ShardChunkHeader};
use near_primitives::transaction::ValidatedTransaction;

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
        for (shard_index, chunk) in block
            .chunks()
            .iter_deprecated()
            .enumerate()
            .filter(|(_, chunk)| chunk.is_new_chunk(block.header().height()))
        {
            let chunk = get_chunk_clone_from_header(&self.chain.chain_store, chunk)?;
            // TODO(resharding) This doesn't work if shard layout changes.
            let prev_chunk_header = prev_block_chunks.get(shard_index).unwrap();
            if let Err(err) =
                self.shadow_validate_chunk(prev_block.header(), prev_chunk_header, &chunk)
            {
                near_chain::stateless_validation::metrics::SHADOW_CHUNK_VALIDATION_FAILED_TOTAL
                    .inc();
                tracing::error!(
                    target: "client",
                    ?err,
                    shard_id = ?chunk.shard_id(),
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
        let epoch_id = self.epoch_manager.get_epoch_id_from_prev_block(prev_block_header.hash())?;
        let protocol_version = self.epoch_manager.get_epoch_protocol_version(&epoch_id)?;
        let config = self.runtime_adapter.get_runtime_config(protocol_version);

        let transactions_validation_storage_config = RuntimeStorageConfig {
            state_root: chunk_header.prev_state_root(),
            use_flat_storage: true,
            source: StorageDataSource::Db,
            state_patch: Default::default(),
        };

        let validated_txs =
            ValidatedTransaction::new_list(&config, chunk.transactions().into_iter().cloned())
                .map_err(|(err, signed_tx)| {
                    Error::Other(format!(
                        "Validating signed tx ({:?}) failed with {:?}",
                        signed_tx, err,
                    ))
                })?;

        // We call `validate_prepared_transactions()` here because we need storage proof for transactions validation.
        // Normally it is provided by chunk producer, but for shadow validation we need to generate it ourselves.
        let Ok(prepared_txs) = validate_prepared_transactions(
            &self.chain,
            self.runtime_adapter.as_ref(),
            &chunk_header,
            transactions_validation_storage_config,
            validated_txs,
            last_chunk.transactions(),
        ) else {
            return Err(Error::Other(
                "Could not produce storage proof for new transactions".to_owned(),
            ));
        };

        let CreateWitnessResult { state_witness, .. } = self.create_state_witness(
            // Setting arbitrary chunk producer is OK for shadow validation
            "alice.near".parse().unwrap(),
            prev_block_header,
            prev_chunk_header,
            chunk,
            prepared_txs.storage_proof,
        )?;
        if self.config.save_latest_witnesses {
            self.chain.chain_store.save_latest_chunk_state_witness(&state_witness)?;
        }
        self.chain.shadow_validate_state_witness(
            state_witness,
            self.epoch_manager.as_ref(),
            self.runtime_adapter.as_ref(),
            None,
        )?;
        Ok(())
    }
}
