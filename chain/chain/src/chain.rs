use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration as TimeDuration, Instant};

use borsh::BorshSerialize;
use chrono::prelude::{DateTime, Utc};
use chrono::Duration;
use log::{debug, error, info};

use near_primitives::block::genesis_chunks;
use near_primitives::challenge::{
    BlockDoubleSign, Challenge, ChallengeBody, ChallengesResult, ChunkProofs, ChunkState,
};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::merkle::{merklize, verify_path};
use near_primitives::receipt::Receipt;
use near_primitives::sharding::{
    ChunkHash, ChunkHashHeight, ReceiptProof, ShardChunk, ShardChunkHeader, ShardProof,
};
use near_primitives::transaction::{ExecutionOutcome, ExecutionStatus};
use near_primitives::types::{
    AccountId, Balance, BlockExtra, BlockIndex, ChunkExtra, EpochId, Gas, ShardId,
};
use near_primitives::unwrap_or_return;
use near_primitives::views::{
    ExecutionOutcomeView, ExecutionOutcomeWithIdView, ExecutionStatusView,
    FinalExecutionOutcomeView, FinalExecutionStatus,
};
use near_store::{Store, COL_STATE_HEADERS};

use crate::byzantine_assert;
use crate::error::{Error, ErrorKind};
use crate::metrics;
use crate::store::{ChainStore, ChainStoreAccess, ChainStoreUpdate, ShardInfo, StateSyncInfo};
use crate::types::{
    AcceptedBlock, Block, BlockHeader, BlockStatus, Provenance, ReceiptList, ReceiptProofResponse,
    ReceiptResponse, RootProof, RuntimeAdapter, ShardStateSyncResponseHeader,
    ShardStateSyncResponsePart, StateHeaderKey, Tip, ValidatorSignatureVerificationResult,
};
use crate::validate::{validate_challenge, validate_chunk_proofs, validate_chunk_with_chunk_extra};

/// Maximum number of orphans chain can store.
pub const MAX_ORPHAN_SIZE: usize = 1024;

/// Maximum age of orhpan to store in the chain.
const MAX_ORPHAN_AGE_SECS: u64 = 300;

/// Refuse blocks more than this many block intervals in the future (as in bitcoin).
const ACCEPTABLE_TIME_DIFFERENCE: i64 = 12 * 10;

enum ApplyChunksMode {
    ThisEpoch,
    NextEpoch,
}

pub struct Orphan {
    block: Block,
    provenance: Provenance,
    added: Instant,
}

pub struct OrphanBlockPool {
    orphans: HashMap<CryptoHash, Orphan>,
    height_idx: HashMap<BlockIndex, Vec<CryptoHash>>,
    prev_hash_idx: HashMap<CryptoHash, Vec<CryptoHash>>,
    evicted: usize,
}

impl OrphanBlockPool {
    fn new() -> OrphanBlockPool {
        OrphanBlockPool {
            orphans: HashMap::default(),
            height_idx: HashMap::default(),
            prev_hash_idx: HashMap::default(),
            evicted: 0,
        }
    }

    fn len(&self) -> usize {
        self.orphans.len()
    }

    fn len_evicted(&self) -> usize {
        self.evicted
    }

    fn add(&mut self, orphan: Orphan) {
        let height_hashes =
            self.height_idx.entry(orphan.block.header.inner.height).or_insert_with(|| vec![]);
        height_hashes.push(orphan.block.hash());
        let prev_hash_entries =
            self.prev_hash_idx.entry(orphan.block.header.inner.prev_hash).or_insert_with(|| vec![]);
        prev_hash_entries.push(orphan.block.hash());
        self.orphans.insert(orphan.block.hash(), orphan);

        if self.orphans.len() > MAX_ORPHAN_SIZE {
            let old_len = self.orphans.len();

            self.orphans.retain(|_, ref mut x| {
                x.added.elapsed() < TimeDuration::from_secs(MAX_ORPHAN_AGE_SECS)
            });
            let mut heights = self.height_idx.keys().cloned().collect::<Vec<u64>>();
            heights.sort_unstable();
            let mut removed_hashes: HashSet<CryptoHash> = HashSet::default();
            for h in heights.iter().rev() {
                if let Some(hash) = self.height_idx.remove(h) {
                    for h in hash {
                        let _ = self.orphans.remove(&h);
                        removed_hashes.insert(h);
                    }
                }
                if self.orphans.len() < MAX_ORPHAN_SIZE {
                    break;
                }
            }
            self.height_idx.retain(|_, ref mut xs| xs.iter().any(|x| !removed_hashes.contains(&x)));
            self.prev_hash_idx
                .retain(|_, ref mut xs| xs.iter().any(|x| !removed_hashes.contains(&x)));

            self.evicted += old_len - self.orphans.len();
        }
    }

    pub fn contains(&self, hash: &CryptoHash) -> bool {
        self.orphans.contains_key(hash)
    }

    pub fn remove_by_prev_hash(&mut self, prev_hash: CryptoHash) -> Option<Vec<Orphan>> {
        let mut removed_hashes: HashSet<CryptoHash> = HashSet::default();
        let ret = self.prev_hash_idx.remove(&prev_hash).map(|hs| {
            hs.iter()
                .filter_map(|h| {
                    removed_hashes.insert(h.clone());
                    self.orphans.remove(h)
                })
                .collect()
        });

        self.height_idx.retain(|_, ref mut xs| xs.iter().any(|x| !removed_hashes.contains(&x)));

        ret
    }
}

/// Chain genesis configuration.
#[derive(Clone)]
pub struct ChainGenesis {
    pub chain_id: String,
    pub time: DateTime<Utc>,
    pub gas_limit: Gas,
    pub gas_price: Balance,
    pub total_supply: Balance,
    pub max_inflation_rate: u8,
    pub gas_price_adjustment_rate: u8,
    pub transaction_validity_period: BlockIndex,
}

impl ChainGenesis {
    pub fn new(
        chain_id: String,
        time: DateTime<Utc>,
        gas_limit: Gas,
        gas_price: Balance,
        total_supply: Balance,
        max_inflation_rate: u8,
        gas_price_adjustment_rate: u8,
        transaction_validity_period: BlockIndex,
    ) -> Self {
        Self {
            chain_id,
            time,
            gas_limit,
            gas_price,
            total_supply,
            max_inflation_rate,
            gas_price_adjustment_rate,
            transaction_validity_period,
        }
    }
}

/// Facade to the blockchain block processing and storage.
/// Provides current view on the state according to the chain state.
pub struct Chain {
    store: ChainStore,
    pub runtime_adapter: Arc<dyn RuntimeAdapter>,
    orphans: OrphanBlockPool,
    blocks_with_missing_chunks: OrphanBlockPool,
    chain_id: String,
    genesis: BlockHeader,
    pub transaction_validity_period: BlockIndex,
}

impl Chain {
    pub fn new(
        store: Arc<Store>,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        chain_genesis: &ChainGenesis,
    ) -> Result<Chain, Error> {
        let mut store = ChainStore::new(store);

        // Get runtime initial state and create genesis block out of it.
        let (state_store_update, state_roots) = runtime_adapter.genesis_state();
        let genesis_chunks = genesis_chunks(
            state_roots.clone(),
            runtime_adapter.num_shards(),
            chain_genesis.gas_limit,
        );
        let genesis = Block::genesis(
            genesis_chunks.iter().map(|chunk| chunk.header.clone()).collect(),
            chain_genesis.time,
            chain_genesis.gas_limit,
            chain_genesis.gas_price,
            chain_genesis.total_supply,
        );

        // Check if we have a head in the store, otherwise pick genesis block.
        let mut store_update = store.store_update();
        let head_res = store_update.head();
        let head: Tip;
        match head_res {
            Ok(h) => {
                head = h;

                // Check that genesis in the store is the same as genesis given in the config.
                let genesis_hash = store_update.get_block_hash_by_height(0)?;
                if genesis_hash != genesis.hash() {
                    return Err(ErrorKind::Other(format!(
                        "Genesis mismatch between storage and config: {:?} vs {:?}",
                        genesis_hash,
                        genesis.hash()
                    ))
                    .into());
                }

                // Check we have the header corresponding to the header_head.
                let header_head = store_update.header_head()?;
                if store_update.get_block_header(&header_head.last_block_hash).is_err() {
                    // Reset header head and "sync" head to be consistent with current block head.
                    store_update.save_header_head_if_not_challenged(&head)?;
                    store_update.save_sync_head(&head);
                } else {
                    // Reset sync head to be consistent with current header head.
                    store_update.save_sync_head(&header_head);
                }
                // TODO: perform validation that latest state in runtime matches the stored chain.
            }
            Err(err) => match err.kind() {
                ErrorKind::DBNotFoundErr(_) => {
                    for chunk in genesis_chunks {
                        store_update.save_chunk(&chunk.chunk_hash, chunk.clone());
                    }
                    runtime_adapter.add_validator_proposals(
                        CryptoHash::default(),
                        genesis.hash(),
                        genesis.header.inner.height,
                        vec![],
                        vec![],
                        vec![],
                        0,
                        0,
                        chain_genesis.total_supply,
                    )?;
                    store_update.save_block_header(genesis.header.clone());
                    store_update.save_block(genesis.clone());
                    store_update.save_block_extra(
                        &genesis.hash(),
                        BlockExtra { challenges_result: vec![] },
                    );

                    for (chunk_header, state_root) in genesis.chunks.iter().zip(state_roots.iter())
                    {
                        store_update.save_chunk_extra(
                            &genesis.hash(),
                            chunk_header.inner.shard_id,
                            ChunkExtra::new(
                                state_root,
                                vec![],
                                0,
                                chain_genesis.gas_limit,
                                0,
                                0,
                                0,
                            ),
                        );
                    }

                    head = Tip::from_header(&genesis.header);
                    store_update.save_head(&head)?;
                    store_update.save_sync_head(&head);

                    store_update.merge(state_store_update);

                    info!(target: "chain", "Init: saved genesis: {:?} / {:?}", genesis.hash(), state_roots);
                }
                e => return Err(e.into()),
            },
        }
        store_update.commit()?;

        info!(target: "chain", "Init: head: {} @ {} [{}]", head.total_weight.to_num(), head.height, head.last_block_hash);

        Ok(Chain {
            store,
            runtime_adapter,
            orphans: OrphanBlockPool::new(),
            blocks_with_missing_chunks: OrphanBlockPool::new(),
            chain_id: chain_genesis.chain_id.clone(),
            genesis: genesis.header,
            transaction_validity_period: chain_genesis.transaction_validity_period,
        })
    }

    /// Reset "sync" head to current header head.
    /// Do this when first transition to header syncing.
    pub fn reset_sync_head(&mut self) -> Result<Tip, Error> {
        let mut chain_store_update = self.store.store_update();
        let header_head = chain_store_update.header_head()?;
        chain_store_update.save_sync_head(&header_head);
        chain_store_update.commit()?;
        Ok(header_head)
    }

    pub fn save_block(&mut self, block: &Block) -> Result<(), Error> {
        let mut chain_store_update = ChainStoreUpdate::new(&mut self.store);

        if !block.check_validity() {
            byzantine_assert!(false);
            return Err(ErrorKind::Other("Invalid block".into()).into());
        }

        chain_store_update.save_block(block.clone());

        chain_store_update.commit()?;
        Ok(())
    }

    /// Process a block header received during "header first" propagation.
    pub fn process_block_header<F>(
        &mut self,
        header: &BlockHeader,
        on_challenge: F,
    ) -> Result<(), Error>
    where
        F: FnMut(ChallengeBody) -> (),
    {
        // We create new chain update, but it's not going to be committed so it's read only.
        let mut chain_update = ChainUpdate::new(
            &mut self.store,
            self.runtime_adapter.clone(),
            &self.orphans,
            &self.blocks_with_missing_chunks,
            self.transaction_validity_period,
        );
        chain_update.process_block_header(header, on_challenge)?;
        Ok(())
    }

    pub fn mark_block_as_challenged(
        &mut self,
        block_hash: &CryptoHash,
        challenger_hash: &CryptoHash,
    ) -> Result<(), Error> {
        let mut chain_update = ChainUpdate::new(
            &mut self.store,
            self.runtime_adapter.clone(),
            &self.orphans,
            &self.blocks_with_missing_chunks,
            self.transaction_validity_period,
        );
        chain_update.mark_block_as_challenged(block_hash, Some(challenger_hash))?;
        chain_update.commit()?;
        Ok(())
    }

    /// Process a received or produced block, and unroll any orphans that may depend on it.
    /// Changes current state, and calls `block_accepted` callback in case block was successfully applied.
    pub fn process_block<F, F2, F3>(
        &mut self,
        me: &Option<AccountId>,
        block: Block,
        provenance: Provenance,
        block_accepted: F,
        block_misses_chunks: F2,
        on_challenge: F3,
    ) -> Result<Option<Tip>, Error>
    where
        F: Copy + FnMut(AcceptedBlock) -> (),
        F2: Copy + FnMut(Vec<ShardChunkHeader>) -> (),
        F3: Copy + FnMut(ChallengeBody) -> (),
    {
        let block_hash = block.hash();
        let timer = near_metrics::start_timer(&metrics::BLOCK_PROCESSING_TIME);
        let res = self.process_block_single(
            me,
            block,
            provenance,
            block_accepted,
            block_misses_chunks,
            on_challenge,
        );
        near_metrics::stop_timer(timer);
        if res.is_ok() {
            near_metrics::inc_counter(&metrics::BLOCK_PROCESSED_SUCCESSFULLY_TOTAL);

            if let Some(new_res) = self.check_orphans(
                me,
                block_hash,
                block_accepted,
                block_misses_chunks,
                on_challenge,
            ) {
                return Ok(Some(new_res));
            }
        }
        res
    }

    /// Process challenge to invalidate chain. This is done between blocks to unroll the chain as
    /// soon as possible and allow next block producer to skip invalid blocks.  
    pub fn process_challenge(&mut self, challenge: &Challenge) {
        let head = unwrap_or_return!(self.head());
        let mut chain_update = ChainUpdate::new(
            &mut self.store,
            self.runtime_adapter.clone(),
            &self.orphans,
            &self.blocks_with_missing_chunks,
            self.transaction_validity_period,
        );
        match chain_update.verify_challenges(
            &vec![challenge.clone()],
            &head.epoch_id,
            &head.last_block_hash,
            None,
        ) {
            Ok(_) => {}
            Err(err) => {
                debug!(target: "chain", "Invalid challenge: {}", err);
            }
        }
        unwrap_or_return!(chain_update.commit());
    }

    /// Processes headers and adds them to store for syncing.
    pub fn sync_block_headers<F>(
        &mut self,
        mut headers: Vec<BlockHeader>,
        on_challenge: F,
    ) -> Result<(), Error>
    where
        F: Copy + FnMut(ChallengeBody) -> (),
    {
        // Sort headers by heights if they are out of order.
        headers.sort_by(|left, right| left.inner.height.cmp(&right.inner.height));

        let _first_header = if let Some(header) = headers.first() {
            debug!(target: "chain", "Sync block headers: {} headers from {} at {}", headers.len(), header.hash(), header.inner.height);
            header
        } else {
            return Ok(());
        };

        let all_known = if let Some(last_header) = headers.last() {
            self.store.get_block_header(&last_header.hash()).is_ok()
        } else {
            false
        };

        if !all_known {
            // Validate header and then add to the chain.
            for header in headers.iter() {
                let mut chain_update = ChainUpdate::new(
                    &mut self.store,
                    self.runtime_adapter.clone(),
                    &self.orphans,
                    &self.blocks_with_missing_chunks,
                    self.transaction_validity_period,
                );

                chain_update.validate_header(header, &Provenance::SYNC, on_challenge)?;
                chain_update.chain_store_update.save_block_header(header.clone());
                chain_update.commit()?;

                // Add validator proposals for given header.
                self.runtime_adapter.add_validator_proposals(
                    header.inner.prev_hash,
                    header.hash(),
                    header.inner.height,
                    header.inner.validator_proposals.clone(),
                    vec![],
                    header.inner.chunk_mask.clone(),
                    header.inner.rent_paid,
                    header.inner.validator_reward,
                    header.inner.total_supply,
                )?;
            }
        }

        let mut chain_update = ChainUpdate::new(
            &mut self.store,
            self.runtime_adapter.clone(),
            &self.orphans,
            &self.blocks_with_missing_chunks,
            self.transaction_validity_period,
        );

        if let Some(header) = headers.last() {
            // Update sync_head regardless of the total weight.
            chain_update.update_sync_head(header)?;
            // Update header_head if total weight changed.
            chain_update.update_header_head_if_not_challenged(header)?;
        }

        chain_update.commit()
    }

    /// Check if state download is required, otherwise return hashes of blocks to fetch.
    pub fn check_state_needed(
        &mut self,
        block_fetch_horizon: BlockIndex,
    ) -> Result<(bool, Vec<CryptoHash>), Error> {
        let block_head = self.head()?;
        let header_head = self.header_head()?;
        let mut hashes = vec![];

        if block_head.total_weight >= header_head.total_weight {
            return Ok((false, hashes));
        }

        // Find common block between header chain and block chain.
        let mut oldest_height = 0;
        let mut current = self.get_block_header(&header_head.last_block_hash).map(|h| h.clone());
        while let Ok(header) = current {
            if header.inner.height <= block_head.height {
                if self.is_on_current_chain(&header).is_ok() {
                    break;
                }
            }

            oldest_height = header.inner.height;
            hashes.push(header.hash());
            current = self.get_previous_header(&header).map(|h| h.clone());
        }

        let sync_head = self.sync_head()?;
        if oldest_height < sync_head.height.saturating_sub(block_fetch_horizon) {
            return Ok((true, vec![]));
        }
        Ok((false, hashes))
    }

    /// Returns if given block header on the current chain.
    fn is_on_current_chain(&mut self, header: &BlockHeader) -> Result<(), Error> {
        let chain_header = self.get_header_by_height(header.inner.height)?;
        if chain_header.hash() == header.hash() {
            Ok(())
        } else {
            Err(ErrorKind::Other(format!("{} not on current chain", header.hash())).into())
        }
    }

    /// Finds first of the given hashes that is known on the main chain.
    pub fn find_common_header(&mut self, hashes: &Vec<CryptoHash>) -> Option<BlockHeader> {
        for hash in hashes {
            if let Ok(header) = self.get_block_header(&hash).map(|h| h.clone()) {
                if let Ok(header_at_height) = self.get_header_by_height(header.inner.height) {
                    if header.hash() == header_at_height.hash() {
                        return Some(header);
                    }
                }
            }
        }
        None
    }

    fn determine_status(&self, head: Option<Tip>, prev_head: Tip) -> BlockStatus {
        let has_head = head.is_some();
        let mut is_next_block = false;

        let old_hash = if let Some(head) = head {
            if head.prev_block_hash == prev_head.last_block_hash {
                is_next_block = true;
                None
            } else {
                Some(prev_head.last_block_hash)
            }
        } else {
            None
        };

        match (has_head, is_next_block) {
            (true, true) => BlockStatus::Next,
            (true, false) => BlockStatus::Reorg(old_hash.unwrap()),
            (false, _) => BlockStatus::Fork,
        }
    }

    pub fn reset_heads_post_state_sync<F, F2, F3>(
        &mut self,
        me: &Option<AccountId>,
        sync_hash: CryptoHash,
        block_accepted: F,
        block_misses_chunks: F2,
        on_challenge: F3,
    ) -> Result<(), Error>
    where
        F: Copy + FnMut(AcceptedBlock) -> (),
        F2: Copy + FnMut(Vec<ShardChunkHeader>) -> (),
        F3: Copy + FnMut(ChallengeBody) -> (),
    {
        // Get header we were syncing into.
        let header = self.get_block_header(&sync_hash)?;
        let hash = header.inner.prev_hash;
        let prev_header = self.get_block_header(&hash)?;
        let tip = Tip::from_header(prev_header);
        // Update related heads now.
        let mut chain_store_update = self.mut_store().store_update();
        chain_store_update.save_body_head(&tip)?;
        chain_store_update.save_body_tail(&tip);
        chain_store_update.commit()?;

        // Check if there are any orphans unlocked by this state sync.
        // We can't fail beyond this point because the caller will not process accepted blocks
        //    and the blocks with missing chunks if this method fails
        self.check_orphans(me, hash, block_accepted, block_misses_chunks, on_challenge);
        Ok(())
    }

    fn start_downloading_state(
        &mut self,
        me: &Option<AccountId>,
        block: &Block,
    ) -> Result<(), Error> {
        let prev_hash = block.header.inner.prev_hash;
        let shards_to_dl = self.get_shards_to_dl_state(me, &prev_hash);
        let prev_block = self.get_block(&prev_hash)?;

        debug!(target: "chain", "Downloading state for {:?}, I'm {:?}", shards_to_dl, me);

        let state_dl_info = StateSyncInfo {
            epoch_tail_hash: block.header.hash(),
            shards: shards_to_dl
                .iter()
                .map(|shard_id| {
                    let chunk = &prev_block.chunks[*shard_id as usize];
                    ShardInfo(*shard_id, chunk.chunk_hash())
                })
                .collect(),
        };

        let mut chain_store_update = ChainStoreUpdate::new(&mut self.store);

        chain_store_update.add_state_dl_info(state_dl_info);

        chain_store_update.commit()?;

        Ok(())
    }

    fn process_block_single<F, F2, F3>(
        &mut self,
        me: &Option<AccountId>,
        block: Block,
        provenance: Provenance,
        mut block_accepted: F,
        mut block_misses_chunks: F2,
        on_challenge: F3,
    ) -> Result<Option<Tip>, Error>
    where
        F: FnMut(AcceptedBlock) -> (),
        F2: Copy + FnMut(Vec<ShardChunkHeader>) -> (),
        F3: FnMut(ChallengeBody) -> (),
    {
        near_metrics::inc_counter(&metrics::BLOCK_PROCESSED_TOTAL);

        if block.chunks.len() != self.runtime_adapter.num_shards() as usize {
            return Err(ErrorKind::IncorrectNumberOfChunkHeaders.into());
        }

        let prev_head = self.store.head()?;
        let mut chain_update = ChainUpdate::new(
            &mut self.store,
            self.runtime_adapter.clone(),
            &self.orphans,
            &self.blocks_with_missing_chunks,
            self.transaction_validity_period,
        );
        let maybe_new_head = chain_update.process_block(me, &block, &provenance, on_challenge);

        match maybe_new_head {
            Ok((head, needs_to_start_fetching_state)) => {
                chain_update.commit()?;

                if needs_to_start_fetching_state {
                    debug!("Downloading state for block {}", block.hash());
                    self.start_downloading_state(me, &block)?;
                }

                match &head {
                    Some(tip) => {
                        near_metrics::set_gauge(
                            &metrics::VALIDATOR_ACTIVE_TOTAL,
                            match self
                                .runtime_adapter
                                .get_epoch_block_producers(&tip.epoch_id, &tip.last_block_hash)
                            {
                                Ok(value) => value
                                    .iter()
                                    .map(|(_, is_slashed)| if *is_slashed { 0 } else { 1 })
                                    .sum(),
                                Err(_) => 0,
                            },
                        );
                    }
                    None => {}
                }
                // Sum validator balances in full NEARs (divided by 10**18)
                let sum = block
                    .header
                    .inner
                    .validator_proposals
                    .iter()
                    .map(|validator_stake| {
                        (validator_stake.amount / 1_000_000_000_000_000_000) as i64
                    })
                    .sum::<i64>();
                near_metrics::set_gauge(&metrics::VALIDATOR_AMOUNT_STAKED, sum);

                let status = self.determine_status(head.clone(), prev_head);

                // Notify other parts of the system of the update.
                block_accepted(AcceptedBlock {
                    hash: block.hash(),
                    status,
                    provenance,
                    gas_used: block.header.inner.gas_used,
                    gas_limit: block.header.inner.gas_limit,
                });

                Ok(head)
            }
            Err(e) => match e.kind() {
                ErrorKind::Orphan => {
                    let block_hash = block.hash();
                    let orphan = Orphan { block, provenance, added: Instant::now() };

                    self.orphans.add(orphan);

                    debug!(
                        target: "chain",
                        "Process block: orphan: {:?}, # orphans {}{}",
                        block_hash,
                        self.orphans.len(),
                        if self.orphans.len_evicted() > 0 {
                            format!(", # evicted {}", self.orphans.len_evicted())
                        } else {
                            String::new()
                        },
                    );
                    Err(ErrorKind::Orphan.into())
                }
                ErrorKind::ChunksMissing(missing_chunks) => {
                    let block_hash = block.hash();
                    block_misses_chunks(missing_chunks.clone());
                    let orphan = Orphan { block, provenance, added: Instant::now() };

                    self.blocks_with_missing_chunks.add(orphan);

                    debug!(
                        target: "chain",
                        "Process block: missing chunks. Block hash: {:?}. Missing chunks: {:?}",
                        block_hash, missing_chunks,
                    );
                    Err(ErrorKind::ChunksMissing(missing_chunks).into())
                }
                ErrorKind::EpochOutOfBounds => {
                    // Possibly block arrived before we finished processing all of the blocks for epoch before last.
                    debug!(target: "chain", "Received block {}/{} ignored, as epoch is unknown", block.header.inner.height, block.hash());
                    Ok(Some(prev_head))
                }
                ErrorKind::Unfit(ref msg) => {
                    debug!(
                        target: "chain",
                        "Block {} at {} is unfit at this time: {}",
                        block.hash(),
                        block.header.inner.height,
                        msg
                    );
                    Err(ErrorKind::Unfit(msg.clone()).into())
                }
                _ => Err(e),
            },
        }
    }

    pub fn prev_block_is_caught_up(
        &self,
        prev_prev_hash: &CryptoHash,
        prev_hash: &CryptoHash,
    ) -> Result<bool, Error> {
        // This method is identical to `ChainUpdate::prev_block_is_caught_up`, see important
        //    disclaimers in there on some dangers of using it
        Ok(!self.store.get_blocks_to_catchup(prev_prev_hash)?.contains(&prev_hash))
    }

    fn get_shards_to_dl_state(
        &self,
        me: &Option<AccountId>,
        parent_hash: &CryptoHash,
    ) -> Vec<ShardId> {
        (0..self.runtime_adapter.num_shards())
            .filter(|shard_id| {
                self.runtime_adapter.will_care_about_shard(
                    me.as_ref(),
                    parent_hash,
                    *shard_id,
                    true,
                ) && !self.runtime_adapter.cares_about_shard(
                    me.as_ref(),
                    parent_hash,
                    *shard_id,
                    true,
                )
            })
            .collect()
    }

    /// Check if any block with missing chunk is ready to be processed
    pub fn check_blocks_with_missing_chunks<F, F2, F3>(
        &mut self,
        me: &Option<AccountId>,
        prev_hash: CryptoHash,
        block_accepted: F,
        block_misses_chunks: F2,
        on_challenge: F3,
    ) where
        F: Copy + FnMut(AcceptedBlock) -> (),
        F2: Copy + FnMut(Vec<ShardChunkHeader>) -> (),
        F3: Copy + FnMut(ChallengeBody) -> (),
    {
        let mut new_blocks_accepted = vec![];
        if let Some(orphans) = self.blocks_with_missing_chunks.remove_by_prev_hash(prev_hash) {
            for orphan in orphans.into_iter() {
                let block_hash = orphan.block.header.hash();
                let res = self.process_block_single(
                    me,
                    orphan.block,
                    orphan.provenance,
                    block_accepted,
                    block_misses_chunks,
                    on_challenge,
                );
                match res {
                    Ok(_) => {
                        debug!(target: "chain", "Block with missing chunks is accepted; me: {:?}", me);
                        new_blocks_accepted.push(block_hash);
                    }
                    Err(_) => {
                        debug!(target: "chain", "Block with missing chunks is declined; me: {:?}", me);
                    }
                }
            }
        };

        for accepted_block in new_blocks_accepted {
            self.check_orphans(
                me,
                accepted_block,
                block_accepted,
                block_misses_chunks,
                on_challenge,
            );
        }
    }

    /// Check for orphans, once a block is successfully added.
    pub fn check_orphans<F, F2, F3>(
        &mut self,
        me: &Option<AccountId>,
        prev_hash: CryptoHash,
        block_accepted: F,
        block_misses_chunks: F2,
        on_challenge: F3,
    ) -> Option<Tip>
    where
        F: Copy + FnMut(AcceptedBlock) -> (),
        F2: Copy + FnMut(Vec<ShardChunkHeader>) -> (),
        F3: Copy + FnMut(ChallengeBody) -> (),
    {
        let mut queue = vec![prev_hash];
        let mut queue_idx = 0;

        let mut maybe_new_head = None;

        // Check if there are orphans we can process.
        debug!(target: "chain", "Check orphans: from {}, # orphans {}", prev_hash, self.orphans.len());
        while queue_idx < queue.len() {
            if let Some(orphans) = self.orphans.remove_by_prev_hash(queue[queue_idx]) {
                debug!(target: "chain", "Check orphans: found {} orphans", orphans.len());
                for orphan in orphans.into_iter() {
                    let block_hash = orphan.block.hash();
                    let timer = near_metrics::start_timer(&metrics::BLOCK_PROCESSING_TIME);
                    let res = self.process_block_single(
                        me,
                        orphan.block,
                        orphan.provenance,
                        block_accepted,
                        block_misses_chunks,
                        on_challenge,
                    );
                    near_metrics::stop_timer(timer);
                    match res {
                        Ok(maybe_tip) => {
                            near_metrics::inc_counter(&metrics::BLOCK_PROCESSED_SUCCESSFULLY_TOTAL);
                            maybe_new_head = maybe_tip;
                            queue.push(block_hash);
                        }
                        Err(_) => {
                            debug!(target: "chain", "Orphan declined");
                        }
                    }
                }
            }
            queue_idx += 1;
        }

        if queue.len() > 1 {
            debug!(
                target: "chain",
                "Check orphans: {} blocks accepted, remaining # orphans {}",
                queue.len() - 1,
                self.orphans.len(),
            );
        }

        maybe_new_head
    }

    pub fn get_outgoing_receipts_for_shard(
        &mut self,
        prev_block_hash: CryptoHash,
        shard_id: ShardId,
        last_height_included: BlockIndex,
    ) -> Result<ReceiptResponse, Error> {
        self.store.get_outgoing_receipts_for_shard(prev_block_hash, shard_id, last_height_included)
    }

    pub fn get_state_response_header(
        &mut self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
    ) -> Result<ShardStateSyncResponseHeader, Error> {
        // Consistency rules:
        // 1. Everything prefixed with `sync_` indicates new epoch, for which we are syncing.
        // 1a. `sync_prev` means the last of the prev epoch.
        // 2. Empty prefix means the height where chunk was applied last time in the prev epoch.
        //    Let's call it `current`.
        // 2a. `prev_` means we're working with height before current.
        // 3. In inner loops we use all prefixes with no relation to the context described above.
        let sync_block = self.get_block(&sync_hash)?;
        let sync_block_header = sync_block.header.clone();
        if shard_id as usize >= sync_block.chunks.len() {
            return Err(ErrorKind::Other("Invalid request: ShardId out of bounds".into()).into());
        }

        // The chunk was applied at height `chunk_header.height_included`.
        // Getting the `current` state.
        let sync_prev_block = self.get_block(&sync_block_header.inner.prev_hash)?;
        if shard_id as usize >= sync_prev_block.chunks.len() {
            return Err(ErrorKind::Other("Invalid request: ShardId out of bounds".into()).into());
        }
        // Chunk header here is the same chunk header as at the `current` height.
        let chunk_header = sync_prev_block.chunks[shard_id as usize].clone();
        let (chunk_headers_root, chunk_proofs) = merklize(
            &sync_prev_block
                .chunks
                .iter()
                .map(|shard_chunk| {
                    ChunkHashHeight(shard_chunk.hash.clone(), shard_chunk.height_included)
                })
                .collect::<Vec<ChunkHashHeight>>(),
        );
        assert_eq!(chunk_headers_root, sync_prev_block.header.inner.chunk_headers_root);

        let chunk = self.get_chunk_clone_from_header(&chunk_header)?;
        let chunk_proof = chunk_proofs[shard_id as usize].clone();
        let block_header =
            self.get_header_on_chain_by_height(&sync_hash, chunk_header.height_included)?.clone();

        // Collecting the `prev` state.
        let prev_block = self.get_block(&block_header.inner.prev_hash)?;
        if shard_id as usize >= prev_block.chunks.len() {
            return Err(ErrorKind::Other("Invalid request: ShardId out of bounds".into()).into());
        }
        let prev_chunk_header = prev_block.chunks[shard_id as usize].clone();
        let (prev_chunk_headers_root, prev_chunk_proofs) = merklize(
            &prev_block
                .chunks
                .iter()
                .map(|shard_chunk| {
                    ChunkHashHeight(shard_chunk.hash.clone(), shard_chunk.height_included)
                })
                .collect::<Vec<ChunkHashHeight>>(),
        );
        assert_eq!(prev_chunk_headers_root, prev_block.header.inner.chunk_headers_root);

        let prev_chunk_proof = prev_chunk_proofs[shard_id as usize].clone();
        let prev_chunk_height_included = prev_chunk_header.height_included;

        // Getting all existing incoming_receipts from prev_chunk height to the new epoch.
        let incoming_receipts_proofs = ChainStoreUpdate::new(&mut self.store)
            .get_incoming_receipts_for_shard(shard_id, sync_hash, prev_chunk_height_included)?
            .clone();

        // Collecting proofs for incoming receipts.
        let mut root_proofs = vec![];
        for receipt_response in incoming_receipts_proofs.iter() {
            let ReceiptProofResponse(block_hash, receipt_proofs) = receipt_response;
            let block_header = self.get_block_header(&block_hash)?.clone();
            let block = self.get_block(&block_hash)?;
            let (block_receipts_root, block_receipts_proofs) = merklize(
                &block
                    .chunks
                    .iter()
                    .map(|chunk| chunk.inner.outgoing_receipts_root)
                    .collect::<Vec<CryptoHash>>(),
            );

            let mut root_proofs_cur = vec![];
            assert_eq!(receipt_proofs.len(), block_header.inner.chunks_included as usize);
            for receipt_proof in receipt_proofs {
                let ReceiptProof(receipts, shard_proof) = receipt_proof;
                let ShardProof { from_shard_id, to_shard_id: _, proof } = shard_proof;
                let receipts_hash = hash(&ReceiptList(shard_id, receipts.to_vec()).try_to_vec()?);
                let from_shard_id = *from_shard_id as usize;

                let root_proof = block.chunks[from_shard_id].inner.outgoing_receipts_root;
                root_proofs_cur
                    .push(RootProof(root_proof, block_receipts_proofs[from_shard_id].clone()));

                // Make sure we send something reasonable.
                assert_eq!(block_header.inner.chunk_receipts_root, block_receipts_root);
                assert!(verify_path(root_proof, &proof, &receipts_hash));
                assert!(verify_path(
                    block_receipts_root,
                    &block_receipts_proofs[from_shard_id],
                    &root_proof,
                ));
            }
            root_proofs.push(root_proofs_cur);
        }

        Ok(ShardStateSyncResponseHeader {
            chunk,
            chunk_proof,
            prev_chunk_header,
            prev_chunk_proof,
            incoming_receipts_proofs,
            root_proofs,
        })
    }

    pub fn get_state_response_part(
        &mut self,
        shard_id: ShardId,
        part_id: u64,
        sync_hash: CryptoHash,
    ) -> Result<ShardStateSyncResponsePart, Error> {
        let sync_block = self.get_block(&sync_hash)?;
        let sync_block_header = sync_block.header.clone();
        if shard_id as usize >= sync_block.chunks.len() {
            return Err(ErrorKind::Other(
                "get_syncing_state_root fail: shard_id out of bounds".into(),
            )
            .into());
        }
        let sync_prev_block = self.get_block(&sync_block_header.inner.prev_hash)?;
        if shard_id as usize >= sync_prev_block.chunks.len() {
            return Err(ErrorKind::Other(
                "get_syncing_state_root fail: shard_id out of bounds".into(),
            )
            .into());
        }
        let state_root = sync_prev_block.chunks[shard_id as usize].inner.prev_state_root.clone();

        if part_id >= state_root.num_parts {
            return Err(ErrorKind::Other(
                "get_state_response_part fail: part_id out of bound".to_string(),
            )
            .into());
        }
        let (state_part, proof) = self
            .runtime_adapter
            .obtain_state_part(shard_id, part_id, &state_root)
            .map_err(|err| ErrorKind::Other(err.to_string()))?;

        Ok(ShardStateSyncResponsePart { state_part, proof })
    }

    pub fn set_state_header(
        &mut self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
        shard_state_header: ShardStateSyncResponseHeader,
    ) -> Result<(), Error> {
        let sync_block_header = self.get_block_header(&sync_hash)?.clone();

        let ShardStateSyncResponseHeader {
            chunk,
            chunk_proof,
            prev_chunk_header,
            prev_chunk_proof,
            incoming_receipts_proofs,
            root_proofs,
        } = &shard_state_header;

        // 1-2. Checking chunk validity
        if !validate_chunk_proofs(&chunk, &*self.runtime_adapter)? {
            byzantine_assert!(false);
            return Err(ErrorKind::Other(
                "set_shard_state failed: chunk header proofs are invalid".into(),
            )
            .into());
        }

        // Consider chunk itself is valid.

        // 3. Checking that chunks `chunk` and `prev_chunk` are included in appropriate blocks
        // 3a. Checking that chunk `chunk` is included into block at last height before sync_hash
        // 3aa. Also checking chunk.height_included
        let sync_prev_block_header =
            self.get_block_header(&sync_block_header.inner.prev_hash)?.clone();
        if !verify_path(
            sync_prev_block_header.inner.chunk_headers_root,
            &chunk_proof,
            &ChunkHashHeight(chunk.chunk_hash.clone(), chunk.header.height_included),
        ) {
            byzantine_assert!(false);
            return Err(ErrorKind::Other(
                "set_shard_state failed: chunk isn't included into block".into(),
            )
            .into());
        }

        let block_header =
            self.get_header_on_chain_by_height(&sync_hash, chunk.header.height_included)?.clone();
        // 3b. Checking that chunk `prev_chunk` is included into block at height before chunk.height_included
        // 3ba. Also checking prev_chunk.height_included - it's important for getting correct incoming receipts
        let prev_block_header = self.get_block_header(&block_header.inner.prev_hash)?.clone();
        let prev_chunk_height_included = prev_chunk_header.height_included;
        if !verify_path(
            prev_block_header.inner.chunk_headers_root,
            &prev_chunk_proof,
            &ChunkHashHeight(prev_chunk_header.hash.clone(), prev_chunk_height_included),
        ) {
            byzantine_assert!(false);
            return Err(ErrorKind::Other(
                "set_shard_state failed: prev_chunk isn't included into block".into(),
            )
            .into());
        }

        // 4. Proving incoming receipts validity
        // 4a. Checking len of proofs
        if root_proofs.len() != incoming_receipts_proofs.len() {
            byzantine_assert!(false);
            return Err(ErrorKind::Other("set_shard_state failed: invalid proofs".into()).into());
        }
        let mut hash_to_compare = sync_hash;
        for (i, receipt_response) in incoming_receipts_proofs.iter().enumerate() {
            let ReceiptProofResponse(block_hash, receipt_proofs) = receipt_response;

            // 4b. Checking that there is a valid sequence of continuous blocks
            if *block_hash != hash_to_compare {
                byzantine_assert!(false);
                return Err(ErrorKind::Other(
                    "set_shard_state failed: invalid incoming receipts".into(),
                )
                .into());
            }
            let header = self.get_block_header(&hash_to_compare)?;
            hash_to_compare = header.inner.prev_hash;

            let block_header = self.get_block_header(&block_hash)?;
            // 4c. Checking len of receipt_proofs for current block
            if receipt_proofs.len() != root_proofs[i].len()
                || receipt_proofs.len() != block_header.inner.chunks_included as usize
            {
                byzantine_assert!(false);
                return Err(
                    ErrorKind::Other("set_shard_state failed: invalid proofs".into()).into()
                );
            }
            // We know there were exactly `block_header.inner.chunks_included` chunks included
            // on the height of block `block_hash`.
            // There were no other proofs except for included chunks.
            // According to Pigeonhole principle, it's enough to ensure all receipt_proofs are distinct
            // to prove that all receipts were received and no receipts were hidden.
            let mut visited_shard_ids = HashSet::<ShardId>::new();
            for (j, receipt_proof) in receipt_proofs.iter().enumerate() {
                let ReceiptProof(receipts, shard_proof) = receipt_proof;
                let ShardProof { from_shard_id, to_shard_id: _, proof } = shard_proof;
                // 4d. Checking uniqueness for set of `from_shard_id`
                match visited_shard_ids.get(from_shard_id) {
                    Some(_) => {
                        byzantine_assert!(false);
                        return Err(ErrorKind::Other(
                            "set_shard_state failed: invalid proofs".into(),
                        )
                        .into());
                    }
                    _ => visited_shard_ids.insert(*from_shard_id),
                };
                let RootProof(root, block_proof) = &root_proofs[i][j];
                let receipts_hash = hash(&ReceiptList(shard_id, receipts.to_vec()).try_to_vec()?);
                // 4e. Proving the set of receipts is the subset of outgoing_receipts of shard `shard_id`
                if !verify_path(*root, &proof, &receipts_hash) {
                    byzantine_assert!(false);
                    return Err(
                        ErrorKind::Other("set_shard_state failed: invalid proofs".into()).into()
                    );
                }
                // 4f. Proving the outgoing_receipts_root matches that in the block
                if !verify_path(block_header.inner.chunk_receipts_root, block_proof, root) {
                    byzantine_assert!(false);
                    return Err(
                        ErrorKind::Other("set_shard_state failed: invalid proofs".into()).into()
                    );
                }
            }
        }
        // 4g. Checking that there are no more heights to get incoming_receipts
        let header = self.get_block_header(&hash_to_compare)?;
        if header.inner.height != prev_chunk_height_included {
            byzantine_assert!(false);
            return Err(ErrorKind::Other(
                "set_shard_state failed: invalid incoming receipts".into(),
            )
            .into());
        }

        // Saving the header data.
        let mut store_update = self.store.store().store_update();
        let key = StateHeaderKey(shard_id, sync_hash).try_to_vec()?;
        store_update.set_ser(COL_STATE_HEADERS, &key, &shard_state_header)?;
        store_update.commit()?;

        Ok(())
    }

    pub fn get_received_state_header(
        &mut self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
    ) -> Result<ShardStateSyncResponseHeader, Error> {
        let key = StateHeaderKey(shard_id, sync_hash).try_to_vec()?;
        /*self.store.store().get_ser(COL_STATE_HEADERS, sync_hash.as_ref())?.unwrap_or(
            return Err(
            ErrorKind::Other("set_state_finalize failed: cannot get shard_state_header".into())
                .into(),
        ));*/
        // TODO achtung, line above compiles weirdly, remove unwrap
        Ok(self.store.store().get_ser(COL_STATE_HEADERS, &key)?.unwrap())
    }

    pub fn set_state_part(
        &mut self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
        part: ShardStateSyncResponsePart,
    ) -> Result<(), Error> {
        let shard_state_header = self.get_received_state_header(shard_id, sync_hash)?;
        let ShardStateSyncResponseHeader { chunk, .. } = shard_state_header;
        let state_root = &chunk.header.inner.prev_state_root;
        self.runtime_adapter
            .accept_state_part(state_root, &part.state_part, &part.proof)
            .map_err(|_| ErrorKind::InvalidStatePayload)?;
        Ok(())
    }

    pub fn set_state_finalize(
        &mut self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
    ) -> Result<(), Error> {
        let shard_state_header = self.get_received_state_header(shard_id, sync_hash)?;
        let mut chain_update = ChainUpdate::new(
            &mut self.store,
            self.runtime_adapter.clone(),
            &self.orphans,
            &self.blocks_with_missing_chunks,
            self.transaction_validity_period,
        );
        chain_update.set_state_finalize(shard_id, sync_hash, shard_state_header)?;
        chain_update.commit()
    }

    /// Apply transactions in chunks for the next epoch in blocks that were blocked on the state sync
    pub fn catchup_blocks<F, F2, F3>(
        &mut self,
        me: &Option<AccountId>,
        epoch_first_block: &CryptoHash,
        block_accepted: F,
        block_misses_chunks: F2,
        on_challenge: F3,
    ) -> Result<(), Error>
    where
        F: Copy + FnMut(AcceptedBlock) -> (),
        F2: Copy + FnMut(Vec<ShardChunkHeader>) -> (),
        F3: Copy + FnMut(ChallengeBody) -> (),
    {
        debug!("Catching up blocks after syncing pre {:?}, me: {:?}", epoch_first_block, me);

        let mut affected_blocks: HashSet<CryptoHash> = HashSet::new();

        // Apply the epoch start block separately, since it doesn't follow the pattern
        let block = self.store.get_block(&epoch_first_block)?.clone();
        let prev_block = self.store.get_block(&block.header.inner.prev_hash)?.clone();

        let mut chain_update = ChainUpdate::new(
            &mut self.store,
            self.runtime_adapter.clone(),
            &self.orphans,
            &self.blocks_with_missing_chunks,
            self.transaction_validity_period,
        );
        chain_update.apply_chunks(me, &block, &prev_block, ApplyChunksMode::NextEpoch)?;
        chain_update.commit()?;

        affected_blocks.insert(block.header.hash());

        let first_epoch = block.header.inner.epoch_id.clone();

        let mut queue = vec![*epoch_first_block];
        let mut cur = 0;

        while cur < queue.len() {
            let block_hash = queue[cur];

            // TODO: cloning these blocks is extremely wasteful, figure out how to not to clone them
            //    without summoning mutable references tomfoolery
            let prev_block = self.store.get_block(&block_hash).unwrap().clone();

            let mut saw_one = false;
            for next_block_hash in self.store.get_blocks_to_catchup(&block_hash)?.clone() {
                saw_one = true;
                let block = self.store.get_block(&next_block_hash).unwrap().clone();

                let mut chain_update = ChainUpdate::new(
                    &mut self.store,
                    self.runtime_adapter.clone(),
                    &self.orphans,
                    &self.blocks_with_missing_chunks,
                    self.transaction_validity_period,
                );

                chain_update.apply_chunks(me, &block, &prev_block, ApplyChunksMode::NextEpoch)?;

                chain_update.commit()?;

                affected_blocks.insert(block.header.hash());
                queue.push(next_block_hash);
            }
            if saw_one {
                assert_eq!(
                    self.runtime_adapter.get_epoch_id_from_prev_block(&block_hash)?,
                    first_epoch
                );
            }

            cur += 1;
        }

        let mut chain_store_update = ChainStoreUpdate::new(&mut self.store);

        // `blocks_to_catchup` consists of pairs (`prev_hash`, `hash`). For the block that precedes
        // `epoch_first_block` we should only remove the pair with hash = epoch_first_block, while
        // for all the blocks in the queue we can remove all the pairs that have them as `prev_hash`
        // since we processed all the blocks built on top of them above during the BFS
        chain_store_update
            .remove_block_to_catchup(block.header.inner.prev_hash, *epoch_first_block);

        for block_hash in queue {
            debug!(target: "chain", "Catching up: removing prev={:?} from the queue. I'm {:?}", block_hash, me);
            chain_store_update.remove_prev_block_to_catchup(block_hash);
        }
        chain_store_update.remove_state_dl_info(*epoch_first_block);

        chain_store_update.commit()?;

        for hash in affected_blocks.iter() {
            self.check_orphans(me, hash.clone(), block_accepted, block_misses_chunks, on_challenge);
        }

        Ok(())
    }

    pub fn get_transaction_execution_result(
        &mut self,
        hash: &CryptoHash,
    ) -> Result<ExecutionOutcomeView, String> {
        match self.get_transaction_result(hash) {
            Ok(result) => Ok(result.clone().into()),
            Err(err) => match err.kind() {
                ErrorKind::DBNotFoundErr(_) => {
                    Ok(ExecutionOutcome { status: ExecutionStatus::Unknown, ..Default::default() }
                        .into())
                }
                _ => Err(err.to_string()),
            },
        }
    }

    fn get_recursive_transaction_results(
        &mut self,
        hash: &CryptoHash,
    ) -> Result<Vec<ExecutionOutcomeWithIdView>, String> {
        let outcome = self.get_transaction_execution_result(hash)?;
        let receipt_ids = outcome.receipt_ids.clone();
        let mut transactions = vec![ExecutionOutcomeWithIdView { id: (*hash).into(), outcome }];
        for hash in &receipt_ids {
            transactions
                .extend(self.get_recursive_transaction_results(&hash.clone().into())?.into_iter());
        }
        Ok(transactions)
    }

    pub fn get_final_transaction_result(
        &mut self,
        hash: &CryptoHash,
    ) -> Result<FinalExecutionOutcomeView, String> {
        let mut outcomes = self.get_recursive_transaction_results(hash)?;
        let mut looking_for_id = (*hash).into();
        let num_outcomes = outcomes.len();
        let status = outcomes
            .iter()
            .find_map(|outcome_with_id| {
                if outcome_with_id.id == looking_for_id {
                    match &outcome_with_id.outcome.status {
                        ExecutionStatusView::Unknown if num_outcomes == 1 => {
                            Some(FinalExecutionStatus::NotStarted)
                        }
                        ExecutionStatusView::Unknown => Some(FinalExecutionStatus::Started),
                        ExecutionStatusView::Failure(e) => {
                            Some(FinalExecutionStatus::Failure(e.clone()))
                        }
                        ExecutionStatusView::SuccessValue(v) => {
                            Some(FinalExecutionStatus::SuccessValue(v.clone()))
                        }
                        ExecutionStatusView::SuccessReceiptId(id) => {
                            looking_for_id = id.clone();
                            None
                        }
                    }
                } else {
                    None
                }
            })
            .expect("results should resolve to a final outcome");
        let receipts = outcomes.split_off(1);
        Ok(FinalExecutionOutcomeView { status, transaction: outcomes.pop().unwrap(), receipts })
    }
}

/// Various chain getters.
impl Chain {
    /// Gets chain head.
    #[inline]
    pub fn head(&self) -> Result<Tip, Error> {
        self.store.head()
    }

    /// Gets chain header head.
    #[inline]
    pub fn header_head(&self) -> Result<Tip, Error> {
        self.store.header_head()
    }

    /// Gets "sync" head. This may be significantly different to current header chain.
    #[inline]
    pub fn sync_head(&self) -> Result<Tip, Error> {
        self.store.sync_head()
    }

    /// Header of the block at the head of the block chain (not the same thing as header_head).
    #[inline]
    pub fn head_header(&mut self) -> Result<&BlockHeader, Error> {
        self.store.head_header()
    }

    /// Gets a block by hash.
    #[inline]
    pub fn get_block(&mut self, hash: &CryptoHash) -> Result<&Block, Error> {
        self.store.get_block(hash)
    }

    /// Gets a chunk from hash.
    #[inline]
    pub fn get_chunk(&mut self, chunk_hash: &ChunkHash) -> Result<&ShardChunk, Error> {
        self.store.get_chunk(chunk_hash)
    }

    /// Gets a chunk from header.
    #[inline]
    pub fn get_chunk_clone_from_header(
        &mut self,
        header: &ShardChunkHeader,
    ) -> Result<ShardChunk, Error> {
        self.store.get_chunk_clone_from_header(header)
    }

    /// Gets a block from the current chain by height.
    #[inline]
    pub fn get_block_by_height(&mut self, height: BlockIndex) -> Result<&Block, Error> {
        let hash = self.store.get_block_hash_by_height(height)?;
        self.store.get_block(&hash)
    }

    /// Gets a block header by hash.
    #[inline]
    pub fn get_block_header(&mut self, hash: &CryptoHash) -> Result<&BlockHeader, Error> {
        self.store.get_block_header(hash)
    }

    /// Returns block header from the canonical chain for given height if present.
    #[inline]
    pub fn get_header_by_height(&mut self, height: BlockIndex) -> Result<&BlockHeader, Error> {
        self.store.get_header_by_height(height)
    }

    /// Returns block header from the current chain defined by `sync_hash` for given height if present.
    #[inline]
    pub fn get_header_on_chain_by_height(
        &mut self,
        sync_hash: &CryptoHash,
        height: BlockIndex,
    ) -> Result<&BlockHeader, Error> {
        self.store.get_header_on_chain_by_height(sync_hash, height)
    }

    /// Get previous block header.
    #[inline]
    pub fn get_previous_header(&mut self, header: &BlockHeader) -> Result<&BlockHeader, Error> {
        self.store.get_previous_header(header)
    }

    /// Check if block exists.
    #[inline]
    pub fn block_exists(&self, hash: &CryptoHash) -> Result<bool, Error> {
        self.store.block_exists(hash)
    }

    /// Get block extra that was computer after applying previous block.
    #[inline]
    pub fn get_block_extra(&mut self, block_hash: &CryptoHash) -> Result<&BlockExtra, Error> {
        self.store.get_block_extra(block_hash)
    }

    /// Get chunk extra that was computed after applying chunk with given hash.
    #[inline]
    pub fn get_chunk_extra(
        &mut self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<&ChunkExtra, Error> {
        self.store.get_chunk_extra(block_hash, shard_id)
    }

    /// Helper to return latest chunk extra for given shard.
    #[inline]
    pub fn get_latest_chunk_extra(&mut self, shard_id: ShardId) -> Result<&ChunkExtra, Error> {
        self.store.get_chunk_extra(&self.head()?.last_block_hash, shard_id)
    }

    /// Get transaction result for given hash of transaction.
    #[inline]
    pub fn get_transaction_result(
        &mut self,
        hash: &CryptoHash,
    ) -> Result<&ExecutionOutcome, Error> {
        self.store.get_transaction_result(hash)
    }

    /// Returns underlying ChainStore.
    #[inline]
    pub fn store(&self) -> &ChainStore {
        &self.store
    }

    /// Returns mutable ChainStore.
    #[inline]
    pub fn mut_store(&mut self) -> &mut ChainStore {
        &mut self.store
    }

    /// Returns underlying RuntimeAdapter.
    #[inline]
    pub fn runtime_adapter(&self) -> Arc<dyn RuntimeAdapter> {
        self.runtime_adapter.clone()
    }

    #[inline]
    pub fn chain_id(&self) -> &String {
        &self.chain_id
    }

    /// Returns genesis block header.
    #[inline]
    pub fn genesis(&self) -> &BlockHeader {
        &self.genesis
    }

    /// Returns number of orphans currently in the orphan pool.
    #[inline]
    pub fn orphans_len(&self) -> usize {
        self.orphans.len()
    }

    /// Returns number of orphans currently in the orphan pool.
    #[inline]
    pub fn blocks_with_missing_chunks_len(&self) -> usize {
        self.blocks_with_missing_chunks.len()
    }

    /// Returns number of evicted orphans.
    #[inline]
    pub fn orphans_evicted_len(&self) -> usize {
        self.orphans.len_evicted()
    }

    /// Check if hash is for a known orphan.
    #[inline]
    pub fn is_orphan(&self, hash: &CryptoHash) -> bool {
        self.orphans.contains(hash)
    }
}

/// Chain update helper, contains information that is needed to process block
/// and decide to accept it or reject it.
/// If rejected nothing will be updated in underlying storage.
/// Safe to stop process mid way (Ctrl+C or crash).
struct ChainUpdate<'a> {
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    chain_store_update: ChainStoreUpdate<'a, ChainStore>,
    orphans: &'a OrphanBlockPool,
    blocks_with_missing_chunks: &'a OrphanBlockPool,
    transaction_validity_period: BlockIndex,
}

impl<'a> ChainUpdate<'a> {
    pub fn new(
        store: &'a mut ChainStore,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        orphans: &'a OrphanBlockPool,
        blocks_with_missing_chunks: &'a OrphanBlockPool,
        transaction_validity_period: BlockIndex,
    ) -> Self {
        let chain_store_update = store.store_update();
        ChainUpdate {
            runtime_adapter,
            chain_store_update,
            orphans,
            blocks_with_missing_chunks,
            transaction_validity_period,
        }
    }

    /// Commit changes to the chain into the database.
    pub fn commit(self) -> Result<(), Error> {
        self.chain_store_update.commit()
    }

    /// Process block header as part of "header first" block propagation.
    /// We validate the header but we do not store it or update header head
    /// based on this. We will update these once we get the block back after
    /// requesting it.
    pub fn process_block_header<F>(
        &mut self,
        header: &BlockHeader,
        on_challenge: F,
    ) -> Result<(), Error>
    where
        F: FnMut(ChallengeBody) -> (),
    {
        debug!(target: "chain", "Process block header: {} at {}", header.hash(), header.inner.height);

        self.check_header_known(header)?;
        self.validate_header(header, &Provenance::NONE, on_challenge)?;
        Ok(())
    }

    /// Find previous header or return Orphan error if not found.
    pub fn get_previous_header(&mut self, header: &BlockHeader) -> Result<&BlockHeader, Error> {
        self.chain_store_update.get_previous_header(header).map_err(|e| match e.kind() {
            ErrorKind::DBNotFoundErr(_) => ErrorKind::Orphan.into(),
            other => other.into(),
        })
    }

    pub fn ping_missing_chunks(
        &mut self,
        me: &Option<AccountId>,
        parent_hash: CryptoHash,
        block: &Block,
    ) -> Result<(), Error> {
        let mut missing = vec![];
        let height = block.header.inner.height;
        for (shard_id, chunk_header) in block.chunks.iter().enumerate() {
            // Check if any chunks are invalid in this block.
            if let Some(encoded_chunk) =
                self.chain_store_update.is_invalid_chunk(&chunk_header.hash)?
            {
                let merkle_paths = Block::compute_chunk_headers_root(&block.chunks).1;
                let chunk_proof = ChunkProofs {
                    block_header: block.header.try_to_vec().expect("Failed to serialize"),
                    merkle_proof: merkle_paths[shard_id].clone(),
                    chunk: encoded_chunk.clone(),
                };
                return Err(ErrorKind::InvalidChunkProofs(chunk_proof).into());
            }
            let shard_id = shard_id as ShardId;
            if chunk_header.height_included == height {
                let chunk_hash = chunk_header.chunk_hash();

                if let Err(_) = self.chain_store_update.get_chunk_one_part(chunk_header) {
                    missing.push(chunk_header.clone());
                } else if self.runtime_adapter.cares_about_shard(
                    me.as_ref(),
                    &parent_hash,
                    shard_id,
                    true,
                ) || self.runtime_adapter.will_care_about_shard(
                    me.as_ref(),
                    &parent_hash,
                    shard_id,
                    true,
                ) {
                    if let Err(_) = self.chain_store_update.get_chunk(&chunk_hash) {
                        missing.push(chunk_header.clone());
                    }
                }
            }
        }
        if !missing.is_empty() {
            return Err(ErrorKind::ChunksMissing(missing).into());
        }
        Ok(())
    }

    pub fn save_incoming_receipts_from_block(&mut self, block: &Block) -> Result<(), Error> {
        let height = block.header.inner.height;
        let mut receipt_proofs_by_shard_id = HashMap::new();

        for chunk_header in block.chunks.iter() {
            if chunk_header.height_included == height {
                let one_part = self.chain_store_update.get_chunk_one_part(chunk_header).unwrap();
                for receipt_proof in one_part.receipt_proofs.iter() {
                    let ReceiptProof(_, shard_proof) = receipt_proof;
                    let ShardProof { from_shard_id: _, to_shard_id, proof: _ } = shard_proof;
                    receipt_proofs_by_shard_id
                        .entry(*to_shard_id)
                        .or_insert_with(Vec::new)
                        .push(receipt_proof.clone());
                }
            }
        }

        for (shard_id, receipt_proofs) in receipt_proofs_by_shard_id {
            self.chain_store_update.save_incoming_receipt(&block.hash(), shard_id, receipt_proofs);
        }

        Ok(())
    }

    fn create_chunk_state_challenge(
        &mut self,
        prev_block: &Block,
        block: &Block,
        prev_chunk_header: &ShardChunkHeader,
        chunk_header: &ShardChunkHeader,
    ) -> Result<ChunkState, Error> {
        let prev_merkle_proofs = Block::compute_chunk_headers_root(&prev_block.chunks).1;
        let merkle_proofs = Block::compute_chunk_headers_root(&block.chunks).1;
        let prev_chunk = self
            .chain_store_update
            .get_chain_store()
            .get_chunk_clone_from_header(&prev_block.chunks[chunk_header.inner.shard_id as usize])
            .unwrap();
        let receipt_proof_response: Vec<ReceiptProofResponse> =
            self.chain_store_update.get_incoming_receipts_for_shard(
                chunk_header.inner.shard_id,
                prev_block.hash(),
                prev_chunk_header.height_included,
            )?;
        let receipts = collect_receipts_from_response(&receipt_proof_response);

        let challenges_result = self.verify_challenges(
            &block.challenges,
            &block.header.inner.epoch_id,
            &block.header.inner.prev_hash,
            Some(&block.hash()),
        )?;
        let apply_result = self
            .runtime_adapter
            .apply_transactions_with_optional_storage_proof(
                chunk_header.inner.shard_id,
                &prev_chunk.header.inner.prev_state_root,
                prev_chunk.header.height_included,
                prev_block.header.inner.timestamp,
                &prev_chunk.header.inner.prev_block_hash,
                &prev_block.hash(),
                &receipts,
                &prev_chunk.transactions,
                &prev_chunk.header.inner.validator_proposals,
                prev_block.header.inner.gas_price,
                &challenges_result,
                true,
            )
            .unwrap();
        let partial_state = apply_result.proof.unwrap().nodes;
        Ok(ChunkState {
            prev_block_header: prev_block.header.try_to_vec()?,
            block_header: block.header.try_to_vec()?,
            prev_merkle_proof: prev_merkle_proofs[chunk_header.inner.shard_id as usize].clone(),
            merkle_proof: merkle_proofs[chunk_header.inner.shard_id as usize].clone(),
            prev_chunk,
            chunk_header: chunk_header.clone(),
            partial_state,
        })
    }

    fn apply_chunks(
        &mut self,
        me: &Option<AccountId>,
        block: &Block,
        prev_block: &Block,
        mode: ApplyChunksMode,
    ) -> Result<(), Error> {
        let challenges_result = self.verify_challenges(
            &block.challenges,
            &block.header.inner.epoch_id,
            &block.header.inner.prev_hash,
            Some(&block.hash()),
        )?;
        self.chain_store_update.save_block_extra(&block.hash(), BlockExtra { challenges_result });

        for (shard_id, (chunk_header, prev_chunk_header)) in
            (block.chunks.iter().zip(prev_block.chunks.iter())).enumerate()
        {
            let shard_id = shard_id as ShardId;
            let care_about_shard = match mode {
                ApplyChunksMode::ThisEpoch => self.runtime_adapter.cares_about_shard(
                    me.as_ref(),
                    &block.header.inner.prev_hash,
                    shard_id,
                    true,
                ),
                ApplyChunksMode::NextEpoch => {
                    self.runtime_adapter.will_care_about_shard(
                        me.as_ref(),
                        &block.header.inner.prev_hash,
                        shard_id,
                        true,
                    ) && !self.runtime_adapter.cares_about_shard(
                        me.as_ref(),
                        &block.header.inner.prev_hash,
                        shard_id,
                        true,
                    )
                }
            };
            if care_about_shard {
                if chunk_header.height_included == block.header.inner.height {
                    // Validate state root.
                    let prev_chunk_extra = self
                        .chain_store_update
                        .get_chunk_extra(&block.header.inner.prev_hash, shard_id)?
                        .clone();

                    // Validate that all next chunk information matches previous chunk extra.
                    validate_chunk_with_chunk_extra(
                        // It's safe here to use ChainStore instead of ChainStoreUpdate
                        // because we're asking prev_chunk_header for already committed block
                        self.chain_store_update.get_chain_store(),
                        &*self.runtime_adapter,
                        &block.header.inner.prev_hash,
                        &prev_chunk_extra,
                        prev_chunk_header,
                        chunk_header,
                    )
                    .map_err(|_| {
                        byzantine_assert!(false);
                        match self.create_chunk_state_challenge(
                            &prev_block,
                            &block,
                            prev_chunk_header,
                            chunk_header,
                        ) {
                            Ok(chunk_state) => {
                                Error::from(ErrorKind::InvalidChunkState(chunk_state))
                            }
                            Err(err) => err,
                        }
                    })?;

                    let receipt_proof_response: Vec<ReceiptProofResponse> =
                        self.chain_store_update.get_incoming_receipts_for_shard(
                            shard_id,
                            block.hash(),
                            prev_chunk_header.height_included,
                        )?;
                    let receipts = collect_receipts_from_response(&receipt_proof_response);

                    let chunk =
                        self.chain_store_update.get_chunk_clone_from_header(&chunk_header)?;

                    let any_transaction_is_invalid = chunk.transactions.iter().any(|t| {
                        self.chain_store_update
                            .get_chain_store()
                            .check_blocks_on_same_chain(
                                &block.header,
                                &t.transaction.block_hash,
                                self.transaction_validity_period,
                            )
                            .is_err()
                    });
                    if any_transaction_is_invalid {
                        debug!(target: "chain", "Invalid transactions in the chunk: {:?}", chunk.transactions);
                        return Err(ErrorKind::InvalidTransactions.into());
                    }
                    let gas_limit = chunk.header.inner.gas_limit;

                    // Apply transactions and receipts.
                    let mut apply_result = self
                        .runtime_adapter
                        .apply_transactions(
                            shard_id,
                            &chunk.header.inner.prev_state_root,
                            chunk_header.height_included,
                            block.header.inner.timestamp,
                            &chunk_header.inner.prev_block_hash,
                            &block.hash(),
                            &receipts,
                            &chunk.transactions,
                            &chunk.header.inner.validator_proposals,
                            block.header.inner.gas_price,
                            &block.header.inner.challenges_result,
                        )
                        .map_err(|e| ErrorKind::Other(e.to_string()))?;

                    self.chain_store_update.save_trie_changes(apply_result.trie_changes);
                    // Save state root after applying transactions.
                    self.chain_store_update.save_chunk_extra(
                        &block.hash(),
                        shard_id,
                        ChunkExtra::new(
                            &apply_result.new_root,
                            apply_result.validator_proposals,
                            apply_result.total_gas_burnt,
                            gas_limit,
                            apply_result.total_rent_paid,
                            apply_result.total_validator_reward,
                            apply_result.total_balance_burnt,
                        ),
                    );
                    // Save resulting receipts.
                    let mut outgoing_receipts = vec![];
                    for (_receipt_shard_id, receipts) in apply_result.receipt_result.drain() {
                        // The receipts in store are indexed by the SOURCE shard_id, not destination,
                        //    since they are later retrieved by the chunk producer of the source
                        //    shard to be distributed to the recipients.
                        outgoing_receipts.extend(receipts);
                    }
                    self.chain_store_update.save_outgoing_receipt(
                        &block.hash(),
                        shard_id,
                        outgoing_receipts,
                    );
                    // Save receipt and transaction results.
                    for tx_result in apply_result.transaction_results {
                        self.chain_store_update
                            .save_transaction_result(&tx_result.id, tx_result.outcome);
                    }
                } else {
                    let mut new_extra = self
                        .chain_store_update
                        .get_chunk_extra(&prev_block.hash(), shard_id)?
                        .clone();

                    let apply_result = self
                        .runtime_adapter
                        .apply_transactions(
                            shard_id,
                            &new_extra.state_root,
                            block.header.inner.height,
                            block.header.inner.timestamp,
                            &prev_block.hash(),
                            &block.hash(),
                            &vec![],
                            &vec![],
                            &new_extra.validator_proposals,
                            block.header.inner.gas_price,
                            &block.header.inner.challenges_result,
                        )
                        .map_err(|e| ErrorKind::Other(e.to_string()))?;

                    self.chain_store_update.save_trie_changes(apply_result.trie_changes);
                    new_extra.state_root = apply_result.new_root;

                    self.chain_store_update.save_chunk_extra(&block.hash(), shard_id, new_extra);
                }
            }
        }

        Ok(())
    }

    /// Runs the block processing, including validation and finding a place for the new block in the chain.
    /// Returns new head if chain head updated, as well as a boolean indicating if we need to start
    ///    fetching state for the next epoch.
    fn process_block<F>(
        &mut self,
        me: &Option<AccountId>,
        block: &Block,
        provenance: &Provenance,
        on_challenge: F,
    ) -> Result<(Option<Tip>, bool), Error>
    where
        F: FnMut(ChallengeBody) -> (),
    {
        debug!(target: "chain", "Process block {} at {}, approvals: {}, me: {:?}", block.hash(), block.header.inner.height, block.header.num_approvals(), me);

        // Check if we have already processed this block previously.
        self.check_known(&block)?;

        // Delay hitting the db for current chain head until we know this block is not already known.
        let head = self.chain_store_update.head()?;
        let is_next = block.header.inner.prev_hash == head.last_block_hash;

        // First real I/O expense.
        let prev = self.get_previous_header(&block.header)?;
        let prev_hash = prev.hash();
        let prev_prev_hash = prev.inner.prev_hash;

        // Block is an orphan if we do not know about the previous full block.
        if !is_next && !self.chain_store_update.block_exists(&prev_hash)? {
            return Err(ErrorKind::Orphan.into());
        }

        let (is_caught_up, needs_to_start_fetching_state) =
            if self.runtime_adapter.is_next_block_epoch_start(&prev_hash)? {
                if !self.prev_block_is_caught_up(&prev_prev_hash, &prev_hash)? {
                    // The previous block is not caught up for the next epoch relative to the previous
                    // block, which is the current epoch for this block, so this block cannot be applied
                    // at all yet, needs to be orphaned
                    return Err(ErrorKind::Orphan.into());
                }

                // For the first block of the epoch we never apply state for the next epoch, so it's
                // always caught up.
                (false, true)
            } else {
                (self.prev_block_is_caught_up(&prev_prev_hash, &prev_hash)?, false)
            };

        debug!(target: "chain", "{:?} Process block {}, is_caught_up: {}, need_to_start_fetching_state: {}", me, block.hash(), is_caught_up, needs_to_start_fetching_state);

        // Check the header is valid before we proceed with the full block.
        self.process_header_for_block(&block.header, provenance, on_challenge)?;

        if !block.check_validity() {
            byzantine_assert!(false);
            return Err(ErrorKind::Other("Invalid block".into()).into());
        }

        let prev_block = self.chain_store_update.get_block(&prev_hash)?.clone();

        self.ping_missing_chunks(me, prev_hash, &block)?;
        self.save_incoming_receipts_from_block(&block)?;

        // Do basic validation of chunks before applying the transactions
        for (chunk_header, prev_chunk_header) in block.chunks.iter().zip(prev_block.chunks.iter()) {
            if chunk_header.height_included == block.header.inner.height {
                if chunk_header.inner.prev_block_hash != block.header.inner.prev_hash {
                    return Err(ErrorKind::InvalidChunk.into());
                }
            } else {
                if prev_chunk_header != chunk_header {
                    return Err(ErrorKind::InvalidChunk.into());
                }
            }
        }

        // Always apply state transition for shards in the current epoch
        self.apply_chunks(me, block, &prev_block, ApplyChunksMode::ThisEpoch)?;

        // If we have the state for the next epoch already downloaded, apply the state transition for the next epoch as well,
        //    otherwise put the block into the permanent storage to have the state transition applied later
        if is_caught_up {
            self.apply_chunks(me, block, &prev_block, ApplyChunksMode::NextEpoch)?;
        } else {
            self.chain_store_update.add_block_to_catchup(prev_hash, block.hash());
        }

        // Verify that proposals from chunks match block header proposals.
        let mut all_chunk_proposals = vec![];
        for chunk in block.chunks.iter() {
            if block.header.inner.height == chunk.height_included {
                all_chunk_proposals.extend(chunk.inner.validator_proposals.clone());
            }
        }
        if all_chunk_proposals != block.header.inner.validator_proposals {
            return Err(ErrorKind::InvalidValidatorProposals.into());
        }

        // If block checks out, record validator proposals for given block.
        self.runtime_adapter.add_validator_proposals(
            block.header.inner.prev_hash,
            block.hash(),
            block.header.inner.height,
            block.header.inner.validator_proposals.clone(),
            block.header.inner.challenges_result.clone(),
            block.header.inner.chunk_mask.clone(),
            block.header.inner.rent_paid,
            block.header.inner.validator_reward,
            block.header.inner.total_supply,
        )?;

        // Add validated block to the db, even if it's not the selected fork.
        self.chain_store_update.save_block(block.clone());

        // Update the chain head if total weight has increased.
        let res = self.update_head(block)?;
        Ok((res, needs_to_start_fetching_state))
    }

    fn prev_block_is_caught_up(
        &self,
        prev_prev_hash: &CryptoHash,
        prev_hash: &CryptoHash,
    ) -> Result<bool, Error> {
        // Needs to be used with care: for the first block of each epoch the semantic is slightly
        // different, since the prev_block is in a different epoch. So for all the blocks but the
        // first one in each epoch this method returns true if the block is ready to have state
        // applied for the next epoch, while for the first block in a particular epoch this method
        // returns true if the block is ready to have state applied for the current epoch (and
        // otherwise should be orphaned)
        Ok(!self.chain_store_update.get_blocks_to_catchup(prev_prev_hash)?.contains(&prev_hash))
    }

    /// Process a block header as part of processing a full block.
    /// We want to be sure the header is valid before processing the full block.
    fn process_header_for_block<F>(
        &mut self,
        header: &BlockHeader,
        provenance: &Provenance,
        on_challenge: F,
    ) -> Result<(), Error>
    where
        F: FnMut(ChallengeBody) -> (),
    {
        self.validate_header(header, provenance, on_challenge)?;
        self.chain_store_update.save_block_header(header.clone());
        self.update_header_head_if_not_challenged(header)?;
        Ok(())
    }

    fn validate_header<F>(
        &mut self,
        header: &BlockHeader,
        provenance: &Provenance,
        mut on_challenge: F,
    ) -> Result<(), Error>
    where
        F: FnMut(ChallengeBody) -> (),
    {
        // Refuse blocks from the too distant future.
        if header.timestamp() > Utc::now() + Duration::seconds(ACCEPTABLE_TIME_DIFFERENCE) {
            return Err(ErrorKind::InvalidBlockFutureTime(header.timestamp()).into());
        }

        // First I/O cost, delay as much as possible.
        match self.runtime_adapter.verify_header_signature(header) {
            ValidatorSignatureVerificationResult::Valid => {}
            ValidatorSignatureVerificationResult::Invalid => {
                return Err(ErrorKind::InvalidSignature.into());
            }
            ValidatorSignatureVerificationResult::UnknownEpoch => {
                return Err(ErrorKind::EpochOutOfBounds.into());
            }
        }

        // Check we don't know a block with given height already.
        // If we do - send out double sign challenge and keep going as double signed blocks are valid blocks.
        if let Ok(other_hash) = self
            .chain_store_update
            .get_any_block_hash_by_height(header.inner.height)
            .map(Clone::clone)
        {
            let other_header = self.chain_store_update.get_block_header(&other_hash)?;

            on_challenge(ChallengeBody::BlockDoubleSign(BlockDoubleSign {
                left_block_header: header.try_to_vec().expect("Failed to serialize"),
                right_block_header: other_header.try_to_vec().expect("Failed to serialize"),
            }));
        }

        let prev_header = self.get_previous_header(header)?.clone();

        // Check that epoch_id in the header does match epoch given previous header (only if previous header is present).
        if self.runtime_adapter.get_epoch_id_from_prev_block(&header.inner.prev_hash).unwrap()
            != header.inner.epoch_id
        {
            return Err(ErrorKind::InvalidEpochHash.into());
        }

        if header.inner.chunk_mask.len() as u64 != self.runtime_adapter.num_shards() {
            return Err(ErrorKind::InvalidChunkMask.into());
        }

        // Prevent time warp attacks and some timestamp manipulations by forcing strict
        // time progression.
        if header.inner.timestamp <= prev_header.inner.timestamp {
            return Err(ErrorKind::InvalidBlockPastTime(
                prev_header.timestamp(),
                header.timestamp(),
            )
            .into());
        }
        // If this is not the block we produced (hence trust in it) - validates block
        // producer, confirmation signatures to check that total weight is correct.
        if *provenance != Provenance::PRODUCED {
            // first verify aggregated signature
            let prev_header = self.get_previous_header(header)?.clone();
            if !self.runtime_adapter.verify_approval_signature(
                &prev_header.inner.epoch_id,
                &prev_header.hash,
                &header.inner.approval_mask,
                &header.inner.approval_sigs,
                prev_header.hash.as_ref(),
            )? {
                return Err(ErrorKind::InvalidApprovals.into());
            };
            let weight = self.runtime_adapter.compute_block_weight(&prev_header, header)?;
            if weight != header.inner.total_weight {
                return Err(ErrorKind::InvalidBlockWeight.into());
            }
        }

        Ok(())
    }

    /// Update the header head if this header has most work.
    fn update_header_head_if_not_challenged(
        &mut self,
        header: &BlockHeader,
    ) -> Result<Option<Tip>, Error> {
        let header_head = self.chain_store_update.header_head()?;
        if header.inner.total_weight > header_head.total_weight {
            let tip = Tip::from_header(header);
            self.chain_store_update.save_header_head_if_not_challenged(&tip)?;
            debug!(target: "chain", "Header head updated to {} at {}", tip.last_block_hash, tip.height);

            Ok(Some(tip))
        } else {
            Ok(None)
        }
    }

    /// Directly updates the head if we've just appended a new block to it or handle
    /// the situation where we've just added enough weight to have a fork with more
    /// work than the head.
    fn update_head(&mut self, block: &Block) -> Result<Option<Tip>, Error> {
        // if we made a fork with more weight than the head (which should also be true
        // when extending the head), update it
        let head = self.chain_store_update.head()?;
        if block.header.inner.total_weight > head.total_weight {
            let tip = Tip::from_header(&block.header);

            self.chain_store_update.save_body_head(&tip)?;
            near_metrics::set_gauge(&metrics::BLOCK_HEIGHT_HEAD, tip.height as i64);
            debug!(target: "chain", "Head updated to {} at {}", tip.last_block_hash, tip.height);
            Ok(Some(tip))
        } else {
            Ok(None)
        }
    }

    /// Updates "sync" head with given block header.
    fn update_sync_head(&mut self, header: &BlockHeader) -> Result<(), Error> {
        let tip = Tip::from_header(header);
        self.chain_store_update.save_sync_head(&tip);
        debug!(target: "chain", "Sync head {} @ {}", tip.last_block_hash, tip.height);
        Ok(())
    }

    /// Marks a block as invalid,
    fn mark_block_as_challenged(
        &mut self,
        block_hash: &CryptoHash,
        challenger_hash: Option<&CryptoHash>,
    ) -> Result<(), Error> {
        info!(target: "chain", "Marking {} as challenged block (challenged in {:?}) and updating the chain.", block_hash, challenger_hash);
        let block_header = match self.chain_store_update.get_block_header(block_hash) {
            Ok(block_header) => block_header.clone(),
            Err(e) => match e.kind() {
                ErrorKind::DBNotFoundErr(_) => {
                    // The block wasn't seen yet, still challenge is good.
                    self.chain_store_update.save_challenged_block(*block_hash);
                    return Ok(());
                }
                _ => return Err(e),
            },
        };

        let cur_block_at_same_height =
            match self.chain_store_update.get_block_hash_by_height(block_header.inner.height) {
                Ok(bh) => Some(bh),
                Err(e) => match e.kind() {
                    ErrorKind::DBNotFoundErr(_) => None,
                    _ => return Err(e),
                },
            };

        self.chain_store_update.save_challenged_block(*block_hash);

        // If the block being invalidated is on the canonical chain, update head
        if cur_block_at_same_height == Some(*block_hash) {
            // We only consider two candidates for the new head: the challenger and the block
            //   immediately preceding the block being challenged
            // It could be that there is a better chain known. However, it is extremely unlikely,
            //   and even if there's such chain available, the very next block built on it will
            //   bring this node's head to that chain.
            let prev_header =
                self.chain_store_update.get_block_header(&block_header.inner.prev_hash)?.clone();
            let prev_weight = prev_header.inner.total_weight;
            let new_head_header = if let Some(hash) = challenger_hash {
                let challenger_header = self.chain_store_update.get_block_header(hash)?;
                if challenger_header.inner.total_weight > prev_weight {
                    challenger_header
                } else {
                    &prev_header
                }
            } else {
                &prev_header
            };

            let tip = Tip::from_header(new_head_header);
            self.chain_store_update.save_head(&tip)?;
        }

        Ok(())
    }

    /// Quick in-memory check to fast-reject any block header we've already handled
    /// recently. Keeps duplicates from the network in check.
    /// ctx here is specific to the header_head (tip of the header chain)
    fn check_header_known(&mut self, header: &BlockHeader) -> Result<(), Error> {
        let header_head = self.chain_store_update.header_head()?;
        if header.hash() == header_head.last_block_hash
            || header.hash() == header_head.prev_block_hash
        {
            return Err(ErrorKind::Unfit("header already known".to_string()).into());
        }
        Ok(())
    }

    /// Quick in-memory check for fast-reject any block handled recently.
    fn check_known_head(&self, header: &BlockHeader) -> Result<(), Error> {
        let head = self.chain_store_update.head()?;
        let bh = header.hash();
        if bh == head.last_block_hash || bh == head.prev_block_hash {
            return Err(ErrorKind::Unfit("already known in head".to_string()).into());
        }
        Ok(())
    }

    /// Check if this block is in the set of known orphans.
    fn check_known_orphans(&self, header: &BlockHeader) -> Result<(), Error> {
        if self.orphans.contains(&header.hash()) {
            return Err(ErrorKind::Unfit("already known in orphans".to_string()).into());
        }
        if self.blocks_with_missing_chunks.contains(&header.hash()) {
            return Err(ErrorKind::Unfit(
                "already known in blocks with missing chunks".to_string(),
            )
            .into());
        }
        Ok(())
    }

    /// Check if this block is in the store already.
    fn check_known_store(&self, header: &BlockHeader) -> Result<(), Error> {
        match self.chain_store_update.block_exists(&header.hash()) {
            Ok(true) => {
                let head = self.chain_store_update.head()?;
                if head.height > 50 && header.inner.height < head.height - 50 {
                    // We flag this as an "abusive peer" but only in the case
                    // where we have the full block in our store.
                    // So this is not a particularly exhaustive check.
                    Err(ErrorKind::OldBlock.into())
                } else {
                    Err(ErrorKind::Unfit("already known in store".to_string()).into())
                }
            }
            Ok(false) => {
                // Not yet processed this block, we can proceed.
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Check if header is known: head, orphan or in store.
    #[allow(dead_code)]
    fn is_header_known(&self, header: &BlockHeader) -> Result<bool, Error> {
        let check = || {
            self.check_known_head(header)?;
            self.check_known_orphans(header)?;
            self.check_known_store(header)
        };
        match check() {
            Ok(()) => Ok(false),
            Err(err) => match err.kind() {
                ErrorKind::Unfit(_) => Ok(true),
                kind => Err(kind.into()),
            },
        }
    }

    /// Check if block is known: head, orphan or in store.
    fn check_known(&self, block: &Block) -> Result<(), Error> {
        self.check_known_head(&block.header)?;
        self.check_known_orphans(&block.header)?;
        self.check_known_store(&block.header)?;
        Ok(())
    }

    pub fn set_state_finalize(
        &mut self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
        shard_state_header: ShardStateSyncResponseHeader,
    ) -> Result<(), Error> {
        let ShardStateSyncResponseHeader {
            chunk,
            chunk_proof: _,
            prev_chunk_header: _,
            prev_chunk_proof: _,
            incoming_receipts_proofs,
            root_proofs: _,
        } = shard_state_header;
        let state_root = &chunk.header.inner.prev_state_root;

        let block_header = self
            .chain_store_update
            .get_header_on_chain_by_height(&sync_hash, chunk.header.height_included)?
            .clone();

        // Applying chunk starts here.

        // Confirm that state matches the parts we received.
        self.runtime_adapter.confirm_state(&state_root)?;

        // Getting actual incoming receipts.
        let mut receipt_proof_response: Vec<ReceiptProofResponse> = vec![];
        for incoming_receipt_proof in incoming_receipts_proofs.iter() {
            let ReceiptProofResponse(hash, _) = incoming_receipt_proof;
            let block_header = self.chain_store_update.get_block_header(&hash)?;
            if block_header.inner.height <= chunk.header.height_included {
                receipt_proof_response.push(incoming_receipt_proof.clone());
            }
        }
        let receipts = collect_receipts_from_response(&receipt_proof_response);

        let gas_limit = chunk.header.inner.gas_limit;
        let mut apply_result = self.runtime_adapter.apply_transactions(
            shard_id,
            &chunk.header.inner.prev_state_root,
            chunk.header.height_included,
            block_header.inner.timestamp,
            &chunk.header.inner.prev_block_hash,
            &block_header.hash,
            &receipts,
            &chunk.transactions,
            &chunk.header.inner.validator_proposals,
            block_header.inner.gas_price,
            &block_header.inner.challenges_result,
        )?;

        self.chain_store_update.save_chunk(&chunk.chunk_hash, chunk.clone());

        self.chain_store_update.save_trie_changes(apply_result.trie_changes);
        let chunk_extra = ChunkExtra::new(
            &apply_result.new_root,
            apply_result.validator_proposals,
            apply_result.total_gas_burnt,
            gas_limit,
            apply_result.total_rent_paid,
            apply_result.total_validator_reward,
            apply_result.total_balance_burnt,
        );
        self.chain_store_update.save_chunk_extra(&block_header.hash, shard_id, chunk_extra);

        // Saving outgoing receipts.
        let mut outgoing_receipts = vec![];
        for (_receipt_shard_id, receipts) in apply_result.receipt_result.drain() {
            outgoing_receipts.extend(receipts);
        }
        self.chain_store_update.save_outgoing_receipt(
            &block_header.hash(),
            shard_id,
            outgoing_receipts,
        );
        // Saving transaction results.
        for tx_result in apply_result.transaction_results {
            self.chain_store_update.save_transaction_result(&tx_result.id, tx_result.outcome);
        }
        // Saving all incoming receipts.
        for receipt_proof_response in incoming_receipts_proofs {
            self.chain_store_update.save_incoming_receipt(
                &receipt_proof_response.0,
                shard_id,
                receipt_proof_response.1,
            );
        }

        // We restored the state on height `chunk.header.height_included`.
        // Now we should build a chain up to height of `sync_hash` block.
        let mut current_height = chunk.header.height_included;
        loop {
            current_height += 1;
            let block_header_result =
                self.chain_store_update.get_header_on_chain_by_height(&sync_hash, current_height);
            if let Err(_) = block_header_result {
                // No such height, go ahead.
                continue;
            }
            let block_header = block_header_result?.clone();
            if block_header.hash == sync_hash {
                break;
            }
            let prev_block_header =
                self.chain_store_update.get_block_header(&block_header.inner.prev_hash)?.clone();

            let mut chunk_extra = self
                .chain_store_update
                .get_chunk_extra(&prev_block_header.hash(), shard_id)?
                .clone();

            let apply_result = self.runtime_adapter.apply_transactions(
                shard_id,
                &chunk_extra.state_root,
                block_header.inner.height,
                block_header.inner.timestamp,
                &prev_block_header.hash(),
                &block_header.hash(),
                &vec![],
                &vec![],
                &chunk_extra.validator_proposals,
                block_header.inner.gas_price,
                &block_header.inner.challenges_result,
            )?;

            self.chain_store_update.save_trie_changes(apply_result.trie_changes);
            chunk_extra.state_root = apply_result.new_root;

            self.chain_store_update.save_chunk_extra(&block_header.hash(), shard_id, chunk_extra);
        }

        Ok(())
    }

    /// Returns correct / malicious challenges or Error if any challenge is invalid.
    pub fn verify_challenges(
        &mut self,
        challenges: &Vec<Challenge>,
        epoch_id: &EpochId,
        prev_block_hash: &CryptoHash,
        block_hash: Option<&CryptoHash>,
    ) -> Result<ChallengesResult, Error> {
        debug!(target: "chain", "Verifying challenges {:?}", challenges);
        let mut result = vec![];
        for challenge in challenges.iter() {
            match validate_challenge(&*self.runtime_adapter, &epoch_id, &prev_block_hash, challenge)
            {
                Ok((hash, account_ids)) => {
                    match challenge.body {
                        // If it's double signed block, we don't invalidate blocks just slash.
                        ChallengeBody::BlockDoubleSign(_) => {}
                        _ => {
                            self.mark_block_as_challenged(&hash, block_hash)?;
                        }
                    }
                    result.extend(account_ids);
                }
                Err(ref err) if err.kind() == ErrorKind::MaliciousChallenge => {
                    result.push(challenge.account_id.clone());
                }
                Err(err) => return Err(err),
            }
        }
        Ok(result)
    }
}

pub fn collect_receipts(receipt_proofs: &Vec<ReceiptProof>) -> Vec<Receipt> {
    receipt_proofs.iter().map(|x| x.0.clone()).flatten().collect()
}

pub fn collect_receipts_from_response(
    receipt_proof_response: &Vec<ReceiptProofResponse>,
) -> Vec<Receipt> {
    let receipt_proofs = &receipt_proof_response.iter().map(|x| x.1.clone()).flatten().collect();
    collect_receipts(receipt_proofs)
}
