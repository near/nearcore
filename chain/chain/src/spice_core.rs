use near_chain_primitives::Error;
use near_crypto::Signature;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block::{Block, BlockHeader};
use near_primitives::block_body::SpiceCoreStatement;
use near_primitives::errors::InvalidSpiceCoreStatementsError;
use near_primitives::gas::Gas;
use near_primitives::hash::CryptoHash;
use near_primitives::stateless_validation::spice_chunk_endorsement::{
    SpiceEndorsementCoreStatement, SpiceStoredVerifiedEndorsement,
};
use near_primitives::types::{
    AccountId, BlockExecutionResults, BlockHeight, ChunkExecutionResult, ChunkExecutionResultHash,
    ShardId, SpiceChunkId, SpiceUncertifiedChunkInfo,
};
use near_primitives::utils::{get_endorsements_key, get_execution_results_key};
use near_store::adapter::StoreAdapter as _;
use near_store::adapter::chain_store::ChainStoreAdapter;
use near_store::{DBCol, Store};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::{Chain, ChainStoreAccess, ChainStoreUpdate};

#[derive(Clone)]
pub struct SpiceCoreReader {
    chain_store: ChainStoreAdapter,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    genesis_gas_limit: Gas,
}

impl SpiceCoreReader {
    pub fn new(
        chain_store: ChainStoreAdapter,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        genesis_gas_limit: Gas,
    ) -> Self {
        Self { chain_store, epoch_manager, genesis_gas_limit }
    }

    pub fn all_execution_results_exist(&self, block_header: &BlockHeader) -> Result<bool, Error> {
        let shard_layout = self.epoch_manager.get_shard_layout(block_header.epoch_id())?;
        for shard_id in shard_layout.shard_ids() {
            if self.get_execution_result(block_header, shard_id)?.is_none() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub fn endorsement_exists(
        &self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
        account_id: &AccountId,
    ) -> bool {
        self.chain_store
            .store()
            .exists(DBCol::endorsements(), &get_endorsements_key(block_hash, shard_id, account_id))
    }

    fn get_endorsement(
        &self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
        account_id: &AccountId,
    ) -> Result<Option<SpiceStoredVerifiedEndorsement>, std::io::Error> {
        self.chain_store.store().get_ser(
            DBCol::endorsements(),
            &get_endorsements_key(block_hash, shard_id, &account_id),
        )
    }

    fn get_execution_result(
        &self,
        block_header: &BlockHeader,
        shard_id: ShardId,
    ) -> Result<Option<Arc<ChunkExecutionResult>>, Error> {
        if block_header.is_genesis() {
            let shard_layout = self.epoch_manager.get_shard_layout(block_header.epoch_id())?;
            let chunk_extra = Chain::build_genesis_chunk_extra(
                self.chain_store.store_ref(),
                &shard_layout,
                shard_id,
                self.genesis_gas_limit,
            )?;
            Ok(Some(Arc::new(ChunkExecutionResult {
                chunk_extra,
                outgoing_receipts_root: CryptoHash::default(),
            })))
        } else {
            Ok(self.get_execution_result_from_store(block_header.hash(), shard_id)?)
        }
    }

    fn get_execution_result_from_store(
        &self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<Option<Arc<ChunkExecutionResult>>, std::io::Error> {
        let key = get_execution_results_key(block_hash, shard_id);
        self.chain_store.store().caching_get_ser(DBCol::execution_results(), &key)
    }

    fn get_uncertified_chunks(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<Vec<SpiceUncertifiedChunkInfo>, Error> {
        get_uncertified_chunks(&self.chain_store, block_hash)
    }

    /// Returns the height of the last fully certified block relative to the given block.
    /// All chunks in blocks at or below this height have been certified.
    pub fn get_last_certified_block_height(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<BlockHeight, Error> {
        get_last_certified_block_height(&self.chain_store, block_hash)
    }

    pub fn get_execution_results_by_shard_id(
        &self,
        block_header: &BlockHeader,
    ) -> Result<HashMap<ShardId, Arc<ChunkExecutionResult>>, Error> {
        assert!(cfg!(feature = "protocol_feature_spice"));

        let mut results = HashMap::new();

        let shard_layout = self.epoch_manager.get_shard_layout(block_header.epoch_id())?;
        for shard_id in shard_layout.shard_ids() {
            let Some(result) = self.get_execution_result(block_header, shard_id)? else {
                continue;
            };
            results.insert(shard_id, result.clone());
        }
        Ok(results)
    }

    /// Returns ChunkExecutionResult for all chunks or None if at least one execution result is
    /// missing;
    pub fn get_block_execution_results(
        &self,
        block_header: &BlockHeader,
    ) -> Result<Option<BlockExecutionResults>, Error> {
        assert!(cfg!(feature = "protocol_feature_spice"));

        let mut results = HashMap::new();

        let shard_layout = self.epoch_manager.get_shard_layout(block_header.epoch_id())?;
        for shard_id in shard_layout.shard_ids() {
            let Some(result) = self.get_execution_result(block_header, shard_id)? else {
                return Ok(None);
            };
            results.insert(shard_id, result.clone());
        }
        Ok(Some(BlockExecutionResults(results)))
    }

    pub fn core_statement_for_next_block(
        &self,
        block_header: &BlockHeader,
    ) -> Result<Vec<SpiceCoreStatement>, Error> {
        if block_header.is_genesis() {
            return Ok(vec![]);
        }
        let block_hash = block_header.hash();

        let uncertified_chunks = self.get_uncertified_chunks(block_hash)?;

        let mut core_statements = Vec::new();
        for chunk_info in uncertified_chunks {
            for account_id in chunk_info.missing_endorsements {
                if let Some(endorsement) = self.get_endorsement(
                    &chunk_info.chunk_id.block_hash,
                    chunk_info.chunk_id.shard_id,
                    &account_id,
                )? {
                    core_statements.push(
                        endorsement.into_core_statement(chunk_info.chunk_id.clone(), account_id),
                    );
                }
            }

            let Some(execution_result) = self.get_execution_result_from_store(
                &chunk_info.chunk_id.block_hash,
                chunk_info.chunk_id.shard_id,
            )?
            else {
                continue;
            };
            // Execution results are stored only for endorsed chunks.
            core_statements.push(SpiceCoreStatement::ChunkExecutionResult {
                chunk_id: chunk_info.chunk_id,
                execution_result: Arc::unwrap_or_clone(execution_result),
            });
        }
        Ok(core_statements)
    }

    pub fn validate_core_statements_in_block(
        &self,
        block: &Block,
    ) -> Result<(), InvalidSpiceCoreStatementsError> {
        use InvalidSpiceCoreStatementsError::*;

        fn get_block(
            store: &Store,
            block_hash: &CryptoHash,
        ) -> Result<Arc<Block>, InvalidSpiceCoreStatementsError> {
            store
                .caching_get_ser(DBCol::Block, block_hash.as_ref())
                .map_err(|error| IoError { error })?
                .ok_or(UnknownBlock { block_hash: *block_hash })
        }

        let prev_uncertified_chunks = self
            .get_uncertified_chunks(block.header().prev_hash())
            .map_err(|err| {
                tracing::debug!(target: "spice_core", prev_hash=?block.header().prev_hash(), ?err, "failed getting uncertified_chunks");
                NoPrevUncertifiedChunks
            })?;
        let waiting_on_endorsements: HashSet<_> = prev_uncertified_chunks
            .iter()
            .flat_map(|info| {
                info.missing_endorsements.iter().map(|account_id| (&info.chunk_id, account_id))
            })
            .collect();
        let known_endorsements: HashMap<
            (&SpiceChunkId, &AccountId),
            &SpiceStoredVerifiedEndorsement,
        > = prev_uncertified_chunks
            .iter()
            .flat_map(|info| {
                info.present_endorsements
                    .iter()
                    .map(|(account_id, endorsement)| ((&info.chunk_id, account_id), endorsement))
            })
            .collect();

        let mut in_block_endorsements: HashMap<
            &SpiceChunkId,
            HashMap<ChunkExecutionResultHash, HashMap<&AccountId, Signature>>,
        > = HashMap::new();
        let mut block_execution_results = HashMap::new();
        let mut max_endorsed_height_created = HashMap::new();

        for (index, core_statement) in block.spice_core_statements().iter().enumerate() {
            match core_statement {
                SpiceCoreStatement::Endorsement(endorsement) => {
                    let chunk_id = endorsement.chunk_id();
                    let account_id = endorsement.account_id();
                    // Checking contents of waiting_on_endorsements makes sure that
                    // chunk_id and account_id are valid.
                    if !waiting_on_endorsements.contains(&(chunk_id, account_id)) {
                        return Err(InvalidCoreStatement {
                            index,
                            // It can either be already included in the ancestry or be for a block
                            // outside of ancestry.
                            reason: "endorsement is irrelevant",
                        });
                    }

                    let block = get_block(self.chain_store.store_ref(), &chunk_id.block_hash)?;

                    let validator_info = self
                        .epoch_manager
                        .get_validator_by_account_id(block.header().epoch_id(), account_id)
                        .expect("we are waiting on endorsement for this account so relevant validator has to exist");

                    let Some((signed_data, signature)) =
                        endorsement.verified_signed_data(validator_info.public_key())
                    else {
                        return Err(InvalidCoreStatement { index, reason: "invalid signature" });
                    };

                    if in_block_endorsements
                        .entry(chunk_id)
                        .or_default()
                        .entry(signed_data.execution_result_hash.clone())
                        .or_default()
                        .insert(account_id, signature.clone())
                        .is_some()
                    {
                        return Err(InvalidCoreStatement {
                            index,
                            reason: "duplicate endorsement",
                        });
                    }
                }
                SpiceCoreStatement::ChunkExecutionResult { chunk_id, execution_result } => {
                    if block_execution_results.insert(chunk_id, (execution_result, index)).is_some()
                    {
                        return Err(InvalidCoreStatement {
                            index,
                            reason: "duplicate execution result",
                        });
                    }

                    let block = get_block(self.chain_store.store_ref(), &chunk_id.block_hash)?;
                    let height = block.header().height();

                    let max_endorsed_height =
                        max_endorsed_height_created.entry(chunk_id.shard_id).or_insert(height);
                    *max_endorsed_height = height.max(*max_endorsed_height);
                }
            };
        }

        // TODO(spice): Add validation that endorsements for blocks are included only when previous
        // block is fully endorsed (as part of block we are validating or it's ancestry).
        for (chunk_id, _) in &waiting_on_endorsements {
            if block_execution_results.contains_key(chunk_id) {
                continue;
            }
            let Some(max_endorsed_height_created) =
                max_endorsed_height_created.get(&chunk_id.shard_id)
            else {
                continue;
            };
            let block = get_block(self.chain_store.store_ref(), &chunk_id.block_hash)?;
            let height = block.header().height();
            if height < *max_endorsed_height_created {
                // We cannot be waiting on an endorsement for chunk created at height that is less
                // than maximum endorsed height for the chunk as that would mean that child is
                // endorsed before parent.
                return Err(SkippedExecutionResult { chunk_id: (*chunk_id).clone() });
            }
        }

        for (chunk_id, mut on_chain_endorsements) in in_block_endorsements {
            let block = get_block(self.chain_store.store_ref(), &chunk_id.block_hash)?;
            let chunk_validator_assignments = self
                .epoch_manager
                .get_chunk_validator_assignments(
                    &block.header().epoch_id(),
                    chunk_id.shard_id,
                    block.header().height(),
                )
                .expect(
                    "since we are waiting for endorsement we should know it's validator assignments",
                );
            for (account_id, _) in chunk_validator_assignments.assignments() {
                // We cannot look for endorsements in store since there they are written by a
                // separate actor which isn't synchronized with block processing.
                if !waiting_on_endorsements.contains(&(chunk_id, account_id)) {
                    let endorsement = known_endorsements.get(&(chunk_id, account_id))
                        .expect(
                        "if we aren't waiting for endorsement in this block it should be in ancestry and known"
                    );
                    on_chain_endorsements
                        .entry(endorsement.execution_result_hash.clone())
                        .or_default()
                        .insert(account_id, endorsement.signature.clone());
                }
            }
            for (execution_result_hash, validator_signatures) in on_chain_endorsements {
                let endorsement_state =
                    chunk_validator_assignments.compute_endorsement_state(validator_signatures);
                if !endorsement_state.is_endorsed {
                    continue;
                }

                let Some((execution_result, index)) = block_execution_results.remove(chunk_id)
                else {
                    return Err(NoExecutionResultForEndorsedChunk { chunk_id: chunk_id.clone() });
                };
                if execution_result.compute_hash() != execution_result_hash {
                    return Err(InvalidCoreStatement {
                        index,
                        reason: "endorsed execution result is different from execution result in block",
                    });
                }
            }
        }

        if !block_execution_results.is_empty() {
            let (_, index) = block_execution_results.into_values().next().unwrap();
            return Err(InvalidCoreStatement {
                index,
                reason: "execution results included without enough corresponding endorsement",
            });
        }
        Ok(())
    }

    pub fn get_last_certified_execution_results_for_next_block(
        &self,
        block_header: &BlockHeader,
        core_statements_for_next_block: &[SpiceCoreStatement],
    ) -> Result<BlockExecutionResults, Error> {
        let new_execution_results: HashMap<_, _> = core_statements_for_next_block
            .iter()
            .filter_map(|core_statement| match core_statement {
                SpiceCoreStatement::ChunkExecutionResult { execution_result, chunk_id } => {
                    Some((chunk_id, execution_result))
                }
                _ => None,
            })
            .collect();

        let mut uncertified_chunks =
            get_uncertified_chunks(&self.chain_store, block_header.hash())?;
        uncertified_chunks
            .retain(|chunk_info| !new_execution_results.contains_key(&chunk_info.chunk_id));
        let oldest_uncertified_block_header =
            find_oldest_uncertified_block_header(&self.chain_store, uncertified_chunks)?;
        let last_certified_block_header =
            if let Some(oldest_uncertified_block_header) = oldest_uncertified_block_header {
                &self.chain_store.get_block_header(oldest_uncertified_block_header.prev_hash())?
            } else {
                // If there are no uncertified blocks it means block with block_header is last certified.
                block_header
            };

        let mut execution_results =
            self.get_execution_results_by_shard_id(last_certified_block_header)?;

        for shard_id in self.epoch_manager.shard_ids(block_header.epoch_id())? {
            if execution_results.contains_key(&shard_id) {
                continue;
            }
            let execution_result = new_execution_results
            .get(&SpiceChunkId { block_hash: *last_certified_block_header.hash(), shard_id })
            .expect(
                "for certified block we should have execution either in store or core statements",
            );
            execution_results.insert(shard_id, Arc::new((*execution_result).clone()));
        }

        Ok(BlockExecutionResults(execution_results))
    }
}

fn get_uncertified_chunks(
    chain_store: &ChainStoreAdapter,
    block_hash: &CryptoHash,
) -> Result<Vec<SpiceUncertifiedChunkInfo>, Error> {
    let block = chain_store.get_block(block_hash)?;

    if block.header().is_genesis() || !block.is_spice_block() {
        Ok(vec![])
    } else {
        let Some(uncertified_chunks) =
            chain_store.store_ref().get_ser(DBCol::uncertified_chunks(), block_hash.as_ref())?
        else {
            debug_assert!(
                false,
                "spice blocks in store should always have uncertified_chunks present"
            );
            return Err(Error::Other(format!("missing uncertified chunks for {}", block_hash)));
        };

        Ok(uncertified_chunks)
    }
}

fn get_block_execution_results(block: &Block) -> HashMap<&SpiceChunkId, &ChunkExecutionResult> {
    block
        .spice_core_statements()
        .iter()
        .filter_map(|core_statement| match core_statement {
            SpiceCoreStatement::ChunkExecutionResult { execution_result, chunk_id } => {
                Some((chunk_id, execution_result))
            }
            _ => None,
        })
        .collect()
}

fn get_block_endorsements(
    block: &Block,
) -> HashMap<(&SpiceChunkId, &AccountId), &SpiceEndorsementCoreStatement> {
    block
        .spice_core_statements()
        .iter()
        .filter_map(|core_statement| match core_statement {
            SpiceCoreStatement::Endorsement(endorsement) => {
                Some(((endorsement.chunk_id(), endorsement.account_id()), endorsement))
            }
            _ => None,
        })
        .collect()
}

/// Uncertified chunks for block should always be saved together with the block itself for spice.
pub fn record_uncertified_chunks_for_block(
    chain_store_update: &mut ChainStoreUpdate,
    epoch_manager: &dyn EpochManagerAdapter,
    block: &Block,
) -> Result<(), Error> {
    let block_endorsements = get_block_endorsements(block);
    let block_execution_results = get_block_execution_results(block);

    let prev_hash = block.header().prev_hash();
    let mut uncertified_chunks =
        get_uncertified_chunks(chain_store_update.chain_store(), prev_hash)?;
    uncertified_chunks
        .retain(|chunk_info| !block_execution_results.contains_key(&chunk_info.chunk_id));
    for chunk_info in &mut uncertified_chunks {
        for account_id in &chunk_info.missing_endorsements {
            let Some(endorsement_core_statement) =
                block_endorsements.get(&(&chunk_info.chunk_id, account_id))
            else {
                continue;
            };
            // By the time of recording block is already validated.
            let endorsement = endorsement_core_statement.unchecked_to_stored();
            chunk_info.present_endorsements.push((account_id.clone(), endorsement));
        }

        chunk_info.missing_endorsements.retain(|account_id| {
            !block_endorsements.contains_key(&(&chunk_info.chunk_id, account_id))
        });
        assert!(
            !chunk_info.missing_endorsements.is_empty(),
            "when there are no missing endorsements execution result should be present"
        );
    }

    let shard_layout = epoch_manager.get_shard_layout(block.header().epoch_id())?;
    uncertified_chunks.reserve_exact(shard_layout.num_shards() as usize);
    for shard_id in shard_layout.shard_ids() {
        let chunk_validator_assignments = epoch_manager.get_chunk_validator_assignments(
            block.header().epoch_id(),
            shard_id,
            block.header().height(),
        )?;

        let missing_endorsements = chunk_validator_assignments
            .assignments()
            .iter()
            .map(|(account_id, _)| account_id)
            .cloned()
            .collect();
        uncertified_chunks.push(SpiceUncertifiedChunkInfo {
            chunk_id: SpiceChunkId { block_hash: *block.hash(), shard_id },
            missing_endorsements,
            present_endorsements: Vec::new(),
        });
    }

    let mut store_update = chain_store_update.chain_store().store_ref().store_update();
    store_update.insert_ser(
        DBCol::uncertified_chunks(),
        block.header().hash().as_ref(),
        &uncertified_chunks,
    )?;
    chain_store_update.merge(store_update);
    Ok(())
}

fn find_oldest_uncertified_block_header(
    chain_store: &ChainStoreAdapter,
    uncertified_chunks: Vec<SpiceUncertifiedChunkInfo>,
) -> Result<Option<Arc<BlockHeader>>, Error> {
    let uncertified_block_hashes: HashSet<_> =
        uncertified_chunks.into_iter().map(|chunk_info| chunk_info.chunk_id.block_hash).collect();
    let uncertified_block_headers: Vec<_> = uncertified_block_hashes
        .iter()
        // If this needs to be optimized SpiceUncertifiedChunkInfo can contain block height.
        .map(|block_hash| chain_store.get_block_header(block_hash))
        .collect::<Result<Vec<_>, Error>>()?;
    Ok(uncertified_block_headers.into_iter().min_by_key(|header| header.height()))
}

/// Returns the height of the last fully certified block relative to the given block.
/// All chunks in blocks at or below this height have been certified.
fn get_last_certified_block_height(
    chain_store: &ChainStoreAdapter,
    block_hash: &CryptoHash,
) -> Result<BlockHeight, Error> {
    let uncertified_chunks = get_uncertified_chunks(chain_store, block_hash)?;
    let oldest_uncertified = find_oldest_uncertified_block_header(chain_store, uncertified_chunks)?;
    if let Some(header) = oldest_uncertified {
        let prev_header = chain_store.get_block_header(header.prev_hash())?;
        Ok(prev_header.height())
    } else {
        Ok(chain_store.get_block_header(block_hash)?.height())
    }
}
