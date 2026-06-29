use crate::{Chain, ChainStoreAccess, ChainStoreUpdate};
use near_chain_primitives::Error;
use near_crypto::Signature;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block::{Block, BlockHeader};
use near_primitives::block_body::{SpiceCoreStatement, SpiceCoreStatements};
use near_primitives::epoch_info::EpochInfo;
use near_primitives::errors::InvalidSpiceCoreStatementsError;
use near_primitives::gas::Gas;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::merklize;
use near_primitives::shard_layout::ShardUId;
use near_primitives::spice::chunk_endorsement::{
    SpiceEndorsementCoreStatement, SpiceStoredVerifiedEndorsement,
};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{
    AccountId, BlockExecutionResults, BlockHeight, ChunkExecutionResult, ChunkExecutionResultHash,
    EpochId, ShardId, SpiceChunkEndorsementStats, SpiceChunkId, SpiceUncertifiedChunkInfo,
    ValidatorId,
};
use near_primitives::utils::{get_endorsements_key, get_execution_results_key};
use near_store::adapter::StoreAdapter as _;
use near_store::adapter::chain_store::ChainStoreAdapter;
use near_store::{DBCol, Store};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

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
    ) -> Option<SpiceStoredVerifiedEndorsement> {
        self.chain_store
            .store()
            .get_ser(DBCol::endorsements(), &get_endorsements_key(block_hash, shard_id, account_id))
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
            Ok(get_execution_result_from_store(&self.chain_store, block_header.hash(), shard_id))
        }
    }

    /// Returns the list of uncertified chunks as of the given block.
    /// Returns an empty vec for genesis or non-Spice blocks.
    /// Errors if a Spice block is missing uncertified_chunks in storage.
    pub fn get_uncertified_chunks(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<Vec<SpiceUncertifiedChunkInfo>, Error> {
        get_uncertified_chunks(&self.chain_store, block_hash)
    }

    /// Returns ChunkExtra for a given chunk, trying the ChunkExtra column first
    /// (available on tracking nodes), then falling back to execution results
    /// written by the core writer to `DBCol::execution_results`.
    fn get_trusted_chunk_extra(&self, chunk_id: &SpiceChunkId) -> Result<Arc<ChunkExtra>, Error> {
        let epoch_id = self.epoch_manager.get_epoch_id(&chunk_id.block_hash)?;
        let shard_layout = self.epoch_manager.get_shard_layout(&epoch_id)?;
        let shard_uid = ShardUId::from_shard_id_and_layout(chunk_id.shard_id, &shard_layout);
        if let Ok(chunk_extra) =
            self.chain_store.chunk_store().get_chunk_extra(&chunk_id.block_hash, &shard_uid)
        {
            return Ok(chunk_extra);
        }
        let header = self.chain_store.get_block_header(&chunk_id.block_hash)?;
        if let Some(result) = self.get_execution_result(&header, chunk_id.shard_id)? {
            return Ok(Arc::new(result.chunk_extra.clone()));
        }
        Err(Error::Other(format!("no trusted chunk extra for {:?}", chunk_id)))
    }

    /// Returns the most recent validator proposals from uncertified chunks for
    /// a given shard. These are proposals that are not yet certified on-chain
    /// at the given block hash, and need to be accounted for in
    /// `last_proposals` at epoch boundaries.
    ///
    /// Proposals are sorted ascending by block height. If multiple uncertified
    /// chunks contain proposals for the same account, the most recent one (last
    /// in iteration order) should be kept by the caller's fold/insert logic.
    pub fn get_uncertified_validator_proposals(
        &self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<Vec<ValidatorStake>, Error> {
        let uncertified_chunks = self.get_uncertified_chunks(block_hash)?;
        let shard_uncertified_chunks: Vec<_> = uncertified_chunks
            .into_iter()
            .filter(|info| info.chunk_id.shard_id == shard_id)
            .collect();
        let mut height_proposals: Vec<(BlockHeight, Vec<ValidatorStake>)> = Vec::new();
        for info in &shard_uncertified_chunks {
            let chunk_extra = self.get_trusted_chunk_extra(&info.chunk_id)?;
            let proposals: Vec<ValidatorStake> = chunk_extra.validator_proposals().collect();
            if !proposals.is_empty() {
                let height = self.chain_store.get_block_height(&info.chunk_id.block_hash)?;
                height_proposals.push((height, proposals));
            }
        }
        height_proposals.sort_by_key(|(h, _)| *h);
        debug_assert!(
            height_proposals.windows(2).all(|w| w[0].0 != w[1].0),
            "multiple uncertified chunks at the same height for shard {shard_id}"
        );
        Ok(height_proposals.into_iter().flat_map(|(_, proposals)| proposals).collect())
    }

    /// Returns validator proposals to use as `prev_validator_proposals` when
    /// constructing `NewChunkData`. At epoch boundaries, returns proposals from
    /// uncertified chunks; otherwise returns an empty vec.
    pub fn prev_validator_proposals(
        &self,
        prev_block_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<Vec<ValidatorStake>, Error> {
        if self.epoch_manager.is_next_block_epoch_start(prev_block_hash)? {
            self.get_uncertified_validator_proposals(prev_block_hash, shard_id)
        } else {
            Ok(vec![])
        }
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

    /// State root certified as of `block_hash`: the merkle root over per-shard
    /// state roots of the last fully certified block. Mirrors the non-spice
    /// `Chunks::compute_state_root`. Returns `None` when the certified block's
    /// execution results are not all available yet.
    pub fn last_certified_state_root(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<Option<CryptoHash>, Error> {
        let last_certified = get_last_certified_block_header(&self.chain_store, block_hash)?;
        let shard_layout = self.epoch_manager.get_shard_layout(last_certified.epoch_id())?;

        // Fast path: `DBCol::execution_results`, written asynchronously by
        // `SpiceCoreWriterActor`. By the time a block is fully certified the writer has
        // almost always recorded its results, so this usually returns everything.
        let mut results = self.get_execution_results_by_shard_id(&last_certified)?;

        // Slow path: when the writer has not caught up yet some shards are missing. Recover
        // them from the ancestry's block bodies, which is also the only source for shards
        // this node does not track. Genesis carries no certifying statements, so its results
        // only ever come from the fast path above.
        let all_present = shard_layout.shard_ids().all(|shard_id| results.contains_key(&shard_id));
        if !all_present && !last_certified.is_genesis() {
            let relevant_blocks = HashSet::from([*last_certified.hash()]);
            let mut results_by_block = HashMap::new();
            self.collect_certified_execution_results_from_ancestry(
                block_hash,
                &last_certified,
                &relevant_blocks,
                &mut results_by_block,
            )?;
            for (shard_id, result) in
                results_by_block.remove(last_certified.hash()).unwrap_or_default()
            {
                results.entry(shard_id).or_insert(result);
            }
        }

        let mut state_roots = Vec::with_capacity(shard_layout.num_shards() as usize);
        for shard_id in shard_layout.shard_ids() {
            let Some(result) = results.get(&shard_id) else {
                return Ok(None);
            };
            state_roots.push(*result.chunk_extra.state_root());
        }
        Ok(Some(merklize(&state_roots).0))
    }

    /// Walks the canonical ancestry backwards from `from_hash` down to (but excluding)
    /// `stop_header`, collecting `ChunkExecutionResult` core statements whose certified
    /// chunk belongs to a block in `relevant_blocks`, grouped by block hash and shard.
    /// Reads block bodies because `DBCol::execution_results` is written asynchronously by
    /// `SpiceCoreWriterActor`. Pre-existing entries in `results_by_block` take precedence.
    fn collect_certified_execution_results_from_ancestry(
        &self,
        from_hash: &CryptoHash,
        stop_header: &BlockHeader,
        relevant_blocks: &HashSet<CryptoHash>,
        results_by_block: &mut HashMap<CryptoHash, HashMap<ShardId, Arc<ChunkExecutionResult>>>,
    ) -> Result<(), Error> {
        let mut current_hash = *from_hash;
        while current_hash != *stop_header.hash() {
            let block = self.chain_store.get_block(&current_hash)?;
            if block.header().height() <= stop_header.height() {
                break;
            }
            for (chunk_id, result) in block.spice_core_statements().iter_execution_results() {
                if !relevant_blocks.contains(&chunk_id.block_hash) {
                    continue;
                }
                results_by_block
                    .entry(chunk_id.block_hash)
                    .or_default()
                    .entry(chunk_id.shard_id)
                    .or_insert_with(|| Arc::new(result.clone()));
            }
            current_hash = *block.header().prev_hash();
        }
        Ok(())
    }

    pub fn core_statements_for_next_block(
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
                ) {
                    core_statements.push(
                        endorsement.into_core_statement(chunk_info.chunk_id.clone(), account_id),
                    );
                }
            }

            let Some(execution_result) = get_execution_result_from_store(
                &self.chain_store,
                &chunk_info.chunk_id.block_hash,
                chunk_info.chunk_id.shard_id,
            ) else {
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

    /// Epoch id of the last fully certified block as of `prev_hash`.
    /// Used by the block producer to populate `prev_last_certified_block_epoch_id`
    /// and by the validator to check it.
    pub fn prev_last_certified_block_epoch_id(
        &self,
        prev_hash: &CryptoHash,
    ) -> Result<EpochId, Error> {
        Ok(*get_last_certified_block_header(&self.chain_store, prev_hash)?.epoch_id())
    }

    pub fn validate_prev_last_certified_block_epoch_id(
        &self,
        header: &BlockHeader,
    ) -> Result<(), Error> {
        let actual = header.prev_last_certified_block_epoch_id().ok_or_else(|| {
            Error::InvalidPrevLastCertifiedBlockEpochId(
                "missing field on spice block header".to_string(),
            )
        })?;
        let expected = self.prev_last_certified_block_epoch_id(header.prev_hash())?;
        if &expected != actual {
            return Err(Error::InvalidPrevLastCertifiedBlockEpochId(format!(
                "expected {expected:?}, got {actual:?}"
            )));
        }
        Ok(())
    }

    /// Value for the block header's `spice_chunk_endorsement_stats` field, for a
    /// block at `height` built on `prev_header`. Non-empty only on the epoch's
    /// last block, where it carries the running per-epoch accumulator
    /// (canonicalized to empty when all-zero). Resolves the epoch and
    /// last-block-of-epoch gating from `prev_header`/`height`, so the producer
    /// and the validator derive the same value without reproducing the
    /// `last_final_block` computation.
    pub fn spice_chunk_endorsement_stats_for_next_block(
        &self,
        prev_header: &BlockHeader,
        height: BlockHeight,
    ) -> Result<Vec<SpiceChunkEndorsementStats>, Error> {
        let last_final_block = prev_header.last_final_block_for_height(height);
        let is_last_block_in_epoch = self.epoch_manager.is_produced_block_last_in_epoch(
            height,
            prev_header.hash(),
            &last_final_block,
        )?;
        if !is_last_block_in_epoch {
            return Ok(Vec::new());
        }
        let epoch_id = self.epoch_manager.get_epoch_id_from_prev_block(prev_header.hash())?;
        let mut stats = compute_spice_endorsement_stats(
            &self.chain_store,
            self.epoch_manager.as_ref(),
            &epoch_id,
            prev_header.hash(),
        )?;
        if stats.iter().all(|s| *s == SpiceChunkEndorsementStats::default()) {
            stats.clear();
        }
        Ok(stats)
    }

    pub fn validate_spice_chunk_endorsement_stats(
        &self,
        header: &BlockHeader,
    ) -> Result<(), Error> {
        let actual = header.spice_chunk_endorsement_stats().ok_or_else(|| {
            Error::InvalidSpiceChunkEndorsementStats(
                "missing field on spice block header".to_string(),
            )
        })?;
        let prev_header = self.chain_store.get_block_header(header.prev_hash())?;
        let expected =
            self.spice_chunk_endorsement_stats_for_next_block(&prev_header, header.height())?;
        if actual != expected.as_slice() {
            return Err(Error::InvalidSpiceChunkEndorsementStats(format!(
                "expected {expected:?}, got {actual:?}"
            )));
        }
        Ok(())
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
        let mut endorsed_chunk_accounts: HashSet<(&SpiceChunkId, &AccountId)> = HashSet::new();
        let mut block_execution_results = HashMap::new();
        let mut max_endorsed_height_created = HashMap::new();

        for (index, core_statement) in block.spice_core_statements().iter().enumerate() {
            match core_statement {
                SpiceCoreStatement::Endorsement(endorsement) => {
                    let chunk_id = endorsement.chunk_id();
                    let account_id = endorsement.account_id();
                    // Membership in waiting_on_endorsements also validates that chunk_id and
                    // account_id are valid.
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

                    // Reject more than one endorsement per (chunk, account) regardless of result
                    // hash, so an equivocating validator cannot count toward two results.
                    if !endorsed_chunk_accounts.insert((chunk_id, account_id)) {
                        return Err(InvalidCoreStatement {
                            index,
                            reason: "duplicate endorsement",
                        });
                    }

                    in_block_endorsements
                        .entry(chunk_id)
                        .or_default()
                        .entry(signed_data.execution_result_hash.clone())
                        .or_default()
                        .insert(account_id, signature.clone());
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

    /// Returns execution results for all blocks that become newly certified when
    /// `core_statements_for_next_block` is applied to the current chain state.
    /// The results are ordered oldest-first. When no new certification frontier
    /// advancement occurs but the last certified block is genesis, returns genesis
    /// execution results to bootstrap gas price computation.
    pub fn get_newly_certified_block_execution_results_for_next_block(
        &self,
        block_header: &BlockHeader,
        core_statements_for_next_block: &SpiceCoreStatements,
    ) -> Result<Vec<BlockExecutionResults>, Error> {
        // Find old last certified (before applying new core statements).
        let old_last_certified =
            get_last_certified_block_header(&self.chain_store, block_header.hash())?;

        // Find new last certified (after applying new core statements).
        let newly_certified_chunks: HashSet<&SpiceChunkId> =
            core_statements_for_next_block.iter_execution_results().map(|(id, _)| id).collect();
        let mut uncertified_chunks =
            get_uncertified_chunks(&self.chain_store, block_header.hash())?;
        uncertified_chunks
            .retain(|chunk_info| !newly_certified_chunks.contains(&chunk_info.chunk_id));
        let oldest_uncertified_block_header =
            find_oldest_uncertified_block_header(&self.chain_store, uncertified_chunks)?;
        let new_last_certified =
            if let Some(oldest_uncertified_block_header) = oldest_uncertified_block_header {
                self.chain_store.get_block_header(oldest_uncertified_block_header.prev_hash())?
            } else {
                // After applying new core statements, all blocks up to block_header are certified.
                Arc::new(block_header.clone())
            };

        if new_last_certified.hash() == old_last_certified.hash() {
            if old_last_certified.is_genesis() {
                // Genesis is certified by definition. Return its execution results so gas price
                // is computed from genesis gas_limit until the first real certification.
                return Ok(vec![BlockExecutionResults(
                    self.get_execution_results_by_shard_id(&old_last_certified)?,
                )]);
            }
            return Ok(vec![]);
        }
        assert!(
            old_last_certified.height() < new_last_certified.height(),
            "old last certified should be a strict ancestor of new"
        );

        // Enumerate newly certified blocks by walking backwards from new_last_certified
        // to old_last_certified (exclusive), then reverse for oldest-first order.
        let mut newly_certified_hashes = Vec::new();
        let mut current = new_last_certified;
        while current.hash() != old_last_certified.hash()
            && current.height() > old_last_certified.height()
        {
            newly_certified_hashes.push(*current.hash());
            current = self.chain_store.get_block_header(current.prev_hash())?;
        }
        newly_certified_hashes.reverse();

        // Collect execution results from core_statements_for_next_block and block ancestry,
        // grouped by block_hash.
        let newly_certified_set: HashSet<CryptoHash> =
            newly_certified_hashes.iter().copied().collect();
        let mut results_by_block: HashMap<CryptoHash, HashMap<ShardId, Arc<ChunkExecutionResult>>> =
            HashMap::new();
        for (chunk_id, result) in core_statements_for_next_block.iter_execution_results() {
            if !newly_certified_set.contains(&chunk_id.block_hash) {
                continue;
            }
            results_by_block
                .entry(chunk_id.block_hash)
                .or_default()
                .entry(chunk_id.shard_id)
                .or_insert_with(|| Arc::new(result.clone()));
        }

        // Collect execution results from the ancestry's block bodies, keeping any
        // already seeded from `core_statements_for_next_block` above.
        self.collect_certified_execution_results_from_ancestry(
            block_header.hash(),
            &old_last_certified,
            &newly_certified_set,
            &mut results_by_block,
        )?;

        // Build result for each newly certified block, oldest-first.
        let mut result = Vec::with_capacity(newly_certified_hashes.len());
        for block_hash in &newly_certified_hashes {
            let block_header = self.chain_store.get_block_header(block_hash)?;
            let num_shards = self.epoch_manager.shard_ids(block_header.epoch_id())?.len();
            let execution_results = results_by_block.remove(block_hash).unwrap_or_default();
            assert_eq!(
                execution_results.len(),
                num_shards,
                "should have found all shard's execution results for newly certified block {}",
                block_hash,
            );
            result.push(BlockExecutionResults(execution_results));
        }
        Ok(result)
    }
}

/// Reads the certified chunk execution result for `(block_hash, shard_id)` from
/// `DBCol::execution_results`, as written by the spice core writer. Returns
/// `None` when absent (e.g. the chunk is not yet certified). Does not
/// special-case genesis; callers that need genesis results should go through
/// [`SpiceCoreReader`].
pub(crate) fn get_execution_result_from_store(
    chain_store: &ChainStoreAdapter,
    block_hash: &CryptoHash,
    shard_id: ShardId,
) -> Option<Arc<ChunkExecutionResult>> {
    let key = get_execution_results_key(block_hash, shard_id);
    chain_store.store().caching_get_ser(DBCol::execution_results(), &key)
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
            chain_store.store_ref().get_ser(DBCol::uncertified_chunks(), block_hash.as_ref())
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

/// Uncertified chunks for block should always be saved together with the block itself for spice.
pub fn record_uncertified_chunks_for_block(
    chain_store_update: &mut ChainStoreUpdate,
    epoch_manager: &dyn EpochManagerAdapter,
    block: &Block,
) -> Result<(), Error> {
    let block_endorsements: HashMap<(&SpiceChunkId, &AccountId), &SpiceEndorsementCoreStatement> =
        block
            .spice_core_statements()
            .iter_endorsements()
            .map(|e| ((e.chunk_id(), e.account_id()), e))
            .collect();
    let block_execution_results: HashMap<&SpiceChunkId, &ChunkExecutionResult> =
        block.spice_core_statements().iter_execution_results().collect();

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
    );
    chain_store_update.merge(store_update);
    Ok(())
}

/// Adds `src` into `dst` element-wise, erroring on overflow. `src` may be empty
/// (treated as all-zero) or the same length as `dst`.
fn add_endorsement_stats(
    dst: &mut [SpiceChunkEndorsementStats],
    src: &[SpiceChunkEndorsementStats],
) -> Result<(), Error> {
    // Lengths are node-local (epoch validator count), so a mismatch is an internal
    // bug, not adversarial input: fail fast instead of silently truncating via zip.
    if !src.is_empty() && src.len() != dst.len() {
        debug_assert!(
            false,
            "endorsement stats length mismatch: dst {}, src {}",
            dst.len(),
            src.len()
        );
        return Err(Error::Other(format!(
            "endorsement stats length mismatch: dst {}, src {}",
            dst.len(),
            src.len()
        )));
    }
    let overflow = || Error::Other("overflow accumulating spice endorsement stats".to_string());
    for (entry, delta) in dst.iter_mut().zip(src) {
        entry.produced = entry.produced.checked_add(delta.produced).ok_or_else(overflow)?;
        entry.expected = entry.expected.checked_add(delta.expected).ok_or_else(overflow)?;
    }
    Ok(())
}

/// One block's contribution to per-validator endorsement stats: for every
/// ChunkExecutionResult in `credited_block`'s body, credits each assigned
/// validator with one expected endorsement (and one produced if it endorsed),
/// indexed by `epoch_info`'s validator id. Validators no longer in that epoch
/// are ignored. Returns an empty vec for genesis/non-spice blocks.
fn endorsement_contribution_of_block(
    chain_store: &ChainStoreAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
    epoch_info: &EpochInfo,
    credited_block_hash: &CryptoHash,
) -> Result<Vec<SpiceChunkEndorsementStats>, Error> {
    let credited_block = chain_store.get_block(credited_block_hash)?;
    if credited_block.header().is_genesis() || !credited_block.is_spice_block() {
        return Ok(vec![]);
    }
    let prev_uncertified =
        get_uncertified_chunks(chain_store, credited_block.header().prev_hash())?;
    let statements = credited_block.spice_core_statements();
    // (chunk, validator) -> the result they endorsed, from prior blocks and this one.
    // `unchecked_to_stored` is safe here: signatures are verified in
    // `validate_core_statements_in_block` before stats are computed.
    let endorsed_hash: HashMap<(&SpiceChunkId, &AccountId), ChunkExecutionResultHash> =
        prev_uncertified
            .iter()
            .flat_map(|info| {
                info.present_endorsements.iter().map(|(account, endorsement)| {
                    ((&info.chunk_id, account), endorsement.execution_result_hash.clone())
                })
            })
            .chain(statements.iter_endorsements().map(|endorsement| {
                (
                    (endorsement.chunk_id(), endorsement.account_id()),
                    endorsement.unchecked_to_stored().execution_result_hash,
                )
            }))
            .collect();

    let mut stats = vec![SpiceChunkEndorsementStats::default(); epoch_info.validators_iter().len()];
    for (chunk_id, execution_result) in statements.iter_execution_results() {
        let certified_hash = execution_result.compute_hash();
        let chunk_block = epoch_manager.get_block_info(&chunk_id.block_hash)?;
        let assignments = epoch_manager.get_chunk_validator_assignments(
            chunk_block.epoch_id(),
            chunk_id.shard_id,
            chunk_block.height(),
        )?;
        credit_chunk_endorsement_stats(
            &mut stats,
            assignments.assignments().iter().map(|(account_id, _stake)| account_id),
            |account_id| epoch_info.get_validator_id(account_id).copied(),
            |account_id| endorsed_hash.get(&(chunk_id, account_id)) == Some(&certified_hash),
        );
    }
    Ok(stats)
}

/// Credits one certified chunk's validator assignment into `stats`: each assigned
/// validator still in the current epoch (`validator_id` returns `Some`) gets one
/// expected endorsement, plus one produced if `endorsed_certified` holds for it.
/// Validators no longer in the epoch (`validator_id` returns `None`) are ignored.
pub(crate) fn credit_chunk_endorsement_stats<'a>(
    stats: &mut [SpiceChunkEndorsementStats],
    assignment: impl IntoIterator<Item = &'a AccountId>,
    validator_id: impl Fn(&AccountId) -> Option<ValidatorId>,
    endorsed_certified: impl Fn(&AccountId) -> bool,
) {
    for account_id in assignment {
        let Some(validator_id) = validator_id(account_id) else {
            continue;
        };
        let entry = &mut stats[validator_id as usize];
        entry.expected += 1;
        if endorsed_certified(account_id) {
            entry.produced += 1;
        }
    }
}

/// Reads the stored running endorsement-stats accumulator for `block_hash`.
/// Returns an empty vec for genesis/non-spice blocks.
fn get_spice_endorsement_stats(
    chain_store: &ChainStoreAdapter,
    block_hash: &CryptoHash,
) -> Result<Vec<SpiceChunkEndorsementStats>, Error> {
    let block = chain_store.get_block(block_hash)?;
    if block.header().is_genesis() || !block.is_spice_block() {
        return Ok(vec![]);
    }
    let Some(stats) =
        chain_store.store_ref().get_ser(DBCol::spice_endorsement_stats(), block_hash.as_ref())
    else {
        debug_assert!(false, "spice blocks in store should always have endorsement stats present");
        return Err(Error::Other(format!("missing spice endorsement stats for {block_hash}")));
    };
    Ok(stats)
}

/// Computes the running per-epoch accumulator for a block in `epoch_id` built
/// on `prev_hash`: the previous block's value (carried only within the same
/// epoch, reset at the boundary) plus this epoch's contribution from the
/// previous block's body. Indexed by the current epoch's validator id.
pub fn compute_spice_endorsement_stats(
    chain_store: &ChainStoreAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
    epoch_id: &EpochId,
    prev_hash: &CryptoHash,
) -> Result<Vec<SpiceChunkEndorsementStats>, Error> {
    let epoch_info = epoch_manager.get_epoch_info(epoch_id)?;
    let num_validators = epoch_info.validators_iter().len();
    let contribution =
        endorsement_contribution_of_block(chain_store, epoch_manager, &epoch_info, prev_hash)?;

    // Carry the previous block's accumulator only within the same epoch; at the
    // epoch boundary (and genesis) the per-epoch accumulator resets.
    let prev_header = chain_store.get_block_header(prev_hash)?;
    let prev_stats = if !prev_header.is_genesis() && prev_header.epoch_id() == epoch_id {
        Some(get_spice_endorsement_stats(chain_store, prev_hash)?)
    } else {
        None
    };
    fold_endorsement_stats(num_validators, prev_stats.as_deref(), &contribution)
}

/// Folds the previous block's running accumulator (`prev_stats`) and this block's
/// `contribution` into the running per-epoch value. `prev_stats` is `None` at an
/// epoch boundary or genesis, where the accumulator resets to this block's
/// contribution alone.
pub(crate) fn fold_endorsement_stats(
    num_validators: usize,
    prev_stats: Option<&[SpiceChunkEndorsementStats]>,
    contribution: &[SpiceChunkEndorsementStats],
) -> Result<Vec<SpiceChunkEndorsementStats>, Error> {
    let mut stats = vec![SpiceChunkEndorsementStats::default(); num_validators];
    if let Some(prev_stats) = prev_stats {
        add_endorsement_stats(&mut stats, prev_stats)?;
    }
    add_endorsement_stats(&mut stats, contribution)?;
    Ok(stats)
}

/// Stores the running endorsement-stats accumulator for `block`, alongside its
/// uncertified chunks. Must be called for every spice block during processing.
pub fn record_spice_endorsement_stats_for_block(
    chain_store_update: &mut ChainStoreUpdate,
    epoch_manager: &dyn EpochManagerAdapter,
    block: &Block,
) -> Result<(), Error> {
    let stats = compute_spice_endorsement_stats(
        chain_store_update.chain_store(),
        epoch_manager,
        block.header().epoch_id(),
        block.header().prev_hash(),
    )?;
    let mut store_update = chain_store_update.chain_store().store_ref().store_update();
    store_update.insert_ser(
        DBCol::spice_endorsement_stats(),
        block.header().hash().as_ref(),
        &stats,
    );
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

/// Returns block hashes that became fully certified due to the execution results
/// in `core_statements`. A block is newly certified when all of its previously
/// uncertified chunks appear in the execution results.
pub fn find_newly_certified_block_hashes(
    prev_uncertified_chunks: &[SpiceUncertifiedChunkInfo],
    core_statements: &SpiceCoreStatements,
) -> Vec<CryptoHash> {
    let newly_certified_chunk_ids: HashSet<&SpiceChunkId> =
        core_statements.iter_execution_results().map(|(id, _)| id).collect();
    // Group uncertified chunks by block hash, tracking whether all are now certified.
    let mut blocks: HashMap<CryptoHash, bool> = HashMap::new();
    for chunk_info in prev_uncertified_chunks {
        let block_entry = blocks.entry(chunk_info.chunk_id.block_hash).or_insert(true);
        if !newly_certified_chunk_ids.contains(&chunk_info.chunk_id) {
            *block_entry = false;
        }
    }
    blocks.into_iter().filter(|(_, all_certified)| *all_certified).map(|(hash, _)| hash).collect()
}

/// Returns the header of the last fully certified block relative to the given block.
/// All chunks in blocks at or below this height have been certified.
pub fn get_last_certified_block_header(
    chain_store: &ChainStoreAdapter,
    block_hash: &CryptoHash,
) -> Result<Arc<BlockHeader>, Error> {
    let uncertified_chunks = get_uncertified_chunks(chain_store, block_hash)?;
    let oldest_uncertified = find_oldest_uncertified_block_header(chain_store, uncertified_chunks)?;
    if let Some(header) = oldest_uncertified {
        Ok(chain_store.get_block_header(header.prev_hash())?)
    } else {
        // No uncertified-chunks tracking means the block has nothing to
        // certify: genesis, or a pre-spice block at the activation
        // boundary. Both are fully certified by definition.
        let header = chain_store.get_block_header(block_hash)?;
        debug_assert!(
            header.is_genesis() || !header.is_spice(),
            "post-genesis spice blocks should always have uncertified chunks"
        );
        Ok(header)
    }
}
