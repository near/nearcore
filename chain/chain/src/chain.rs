use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration as TimeDuration, Instant};

use chrono::prelude::{DateTime, Utc};
use chrono::Duration;
use log::{debug, info};

use near_primitives::hash::CryptoHash;
use near_primitives::merkle::merklize;
use near_primitives::receipt::Receipt;
use near_primitives::sharding::{ChunkHash, ShardChunk, ShardChunkHeader};
use near_primitives::transaction::{check_tx_history, TransactionResult};
use near_primitives::types::{AccountId, Balance, BlockIndex, ChunkExtra, Gas, ShardId};
use near_store::Store;

use crate::byzantine_assert;
use crate::error::{Error, ErrorKind};
use crate::store::{ChainStore, ChainStoreAccess, ChainStoreUpdate, ShardInfo, StateSyncInfo};
use crate::types::{
    Block, BlockHeader, BlockStatus, Provenance, ReceiptResponse, RuntimeAdapter,
    ShardFullChunkOrOnePart, Tip, ValidatorSignatureVerificationResult,
};

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
    pub time: DateTime<Utc>,
    pub gas_limit: Gas,
    pub gas_price: Balance,
    pub total_supply: Balance,
    pub max_inflation_rate: u8,
    pub gas_price_adjustment_rate: u8,
    transaction_validity_period: BlockIndex,
}

impl ChainGenesis {
    pub fn new(
        time: DateTime<Utc>,
        gas_limit: Gas,
        gas_price: Balance,
        total_supply: Balance,
        max_inflation_rate: u8,
        gas_price_adjustment_rate: u8,
        transaction_validity_period: BlockIndex,
    ) -> Self {
        Self {
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
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    orphans: OrphanBlockPool,
    blocks_with_missing_chunks: OrphanBlockPool,
    genesis: BlockHeader,
    transaction_validity_period: BlockIndex,
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
        let genesis = Block::genesis(
            state_roots.clone(),
            chain_genesis.time,
            runtime_adapter.num_shards(),
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
                    store_update.save_header_head(&head)?;
                    store_update.save_sync_head(&head);
                } else {
                    // Reset sync head to be consistent with current header head.
                    store_update.save_sync_head(&header_head);
                }
                // TODO: perform validation that latest state in runtime matches the stored chain.
            }
            Err(err) => match err.kind() {
                ErrorKind::DBNotFoundErr(_) => {
                    runtime_adapter.add_validator_proposals(
                        CryptoHash::default(),
                        genesis.hash(),
                        genesis.header.inner.height,
                        vec![],
                        vec![],
                        vec![],
                        0,
                        chain_genesis.gas_price,
                        chain_genesis.total_supply,
                    )?;
                    store_update.save_block_header(genesis.header.clone());
                    store_update.save_block(genesis.clone());

                    for (chunk_header, state_root) in genesis.chunks.iter().zip(state_roots.iter())
                    {
                        store_update.save_chunk_extra(
                            &genesis.hash(),
                            chunk_header.inner.shard_id,
                            ChunkExtra::new(state_root, vec![], 0, chain_genesis.gas_limit),
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

        // Before saving the very first block in the epoch,
        // we ensure that the block contains the correct data.
        // It is sufficient to check the correctness of tx_root, state_root
        // of the block matches the stated hash
        //
        // NOTE: we don't need to re-compute hash of entire block
        // because we always call BlockHeader::init() when we received a block
        //
        // 1. Checking state_root validity
        let state_root = Block::compute_state_root(&block.chunks);
        if block.header.inner.prev_state_root != state_root {
            return Err(ErrorKind::InvalidStateRoot.into());
        }
        // 2. Checking tx_root validity
        let tx_root = Block::compute_tx_root(&block.transactions);
        if block.header.inner.tx_root != tx_root {
            return Err(ErrorKind::InvalidTxRoot.into());
        }

        chain_store_update.save_block(block.clone());

        chain_store_update.commit()?;
        Ok(())
    }

    /// Process a block header received during "header first" propagation.
    pub fn process_block_header(&mut self, header: &BlockHeader) -> Result<(), Error> {
        // We create new chain update, but it's not going to be committed so it's read only.
        let mut chain_update = ChainUpdate::new(
            &mut self.store,
            self.runtime_adapter.clone(),
            &self.orphans,
            &self.blocks_with_missing_chunks,
            self.transaction_validity_period,
        );
        chain_update.process_block_header(header)?;
        Ok(())
    }

    /// Process a received or produced block, and unroll any orphans that may depend on it.
    /// Changes current state, and calls `block_accepted` callback in case block was successfully applied.
    pub fn process_block<F, F2>(
        &mut self,
        me: &Option<AccountId>,
        block: Block,
        provenance: Provenance,
        block_accepted: F,
        block_misses_chunks: F2,
    ) -> Result<Option<Tip>, Error>
    where
        F: Copy + FnMut(&Block, BlockStatus, Provenance) -> (),
        F2: Copy + FnMut(Vec<ShardChunkHeader>) -> (),
    {
        let block_hash = block.hash();
        let res =
            self.process_block_single(me, block, provenance, block_accepted, block_misses_chunks);
        if res.is_ok() {
            if let Some(new_res) =
                self.check_orphans(me, block_hash, block_accepted, block_misses_chunks)
            {
                return Ok(Some(new_res));
            }
        }
        res
    }

    /// Processes headers and adds them to store for syncing.
    pub fn sync_block_headers(&mut self, mut headers: Vec<BlockHeader>) -> Result<(), Error> {
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

                chain_update.validate_header(header, &Provenance::SYNC)?;
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
                    header.inner.gas_used,
                    header.inner.gas_price,
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
            chain_update.update_header_head(header)?;
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

    pub fn reset_heads_post_state_sync<F, F2>(
        &mut self,
        me: &Option<AccountId>,
        sync_hash: CryptoHash,
        block_accepted: F,
        block_misses_chunks: F2,
    ) -> Result<(), Error>
    where
        F: Copy + FnMut(&Block, BlockStatus, Provenance) -> (),
        F2: Copy + FnMut(Vec<ShardChunkHeader>) -> (),
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
        self.check_orphans(me, hash, block_accepted, block_misses_chunks);
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

    fn process_block_single<F, F2>(
        &mut self,
        me: &Option<AccountId>,
        block: Block,
        provenance: Provenance,
        mut block_accepted: F,
        mut block_misses_chunks: F2,
    ) -> Result<Option<Tip>, Error>
    where
        F: FnMut(&Block, BlockStatus, Provenance) -> (),
        F2: Copy + FnMut(Vec<ShardChunkHeader>) -> (),
    {
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
        let maybe_new_head = chain_update.process_block(me, &block, &provenance);

        match maybe_new_head {
            Ok((head, needs_to_start_fetching_state)) => {
                chain_update.commit()?;
                if needs_to_start_fetching_state {
                    debug!("Downloading state for block {}", block.hash());
                    self.start_downloading_state(me, &block)?;
                }

                let status = self.determine_status(head.clone(), prev_head);

                // Notify other parts of the system of the update.
                block_accepted(&block, status, provenance);

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
                _ => Err(ErrorKind::Other(format!("{:?}", e)).into()),
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
    pub fn check_blocks_with_missing_chunks<F, F2>(
        &mut self,
        me: &Option<AccountId>,
        prev_hash: CryptoHash,
        block_accepted: F,
        block_misses_chunks: F2,
    ) where
        F: Copy + FnMut(&Block, BlockStatus, Provenance) -> (),
        F2: Copy + FnMut(Vec<ShardChunkHeader>) -> (),
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
            self.check_orphans(me, accepted_block, block_accepted, block_misses_chunks);
        }
    }

    /// Check for orphans, once a block is successfully added.
    pub fn check_orphans<F, F2>(
        &mut self,
        me: &Option<AccountId>,
        prev_hash: CryptoHash,
        block_accepted: F,
        block_misses_chunks: F2,
    ) -> Option<Tip>
    where
        F: Copy + FnMut(&Block, BlockStatus, Provenance) -> (),
        F2: Copy + FnMut(Vec<ShardChunkHeader>) -> (),
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
                    let res = self.process_block_single(
                        me,
                        orphan.block,
                        orphan.provenance,
                        block_accepted,
                        block_misses_chunks,
                    );
                    match res {
                        Ok(maybe_tip) => {
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

    pub fn state_request(
        &mut self,
        shard_id: ShardId,
        hash: CryptoHash,
    ) -> Result<(ChunkExtra, Vec<u8>, ReceiptResponse, Vec<ReceiptResponse>), Error> {
        let prev_hash = self.get_block_header(&hash)?.inner.prev_hash;

        let prev_block = self.get_block(&prev_hash)?;

        if shard_id as usize >= prev_block.chunks.len() {
            return Err(ErrorKind::Other("Invalid request: ShardId out of bounds".into()).into());
        }

        let prev_chunk_header = prev_block.chunks[shard_id as usize].clone();
        let prev_chunk_extra = self.store.get_chunk_extra(&prev_hash, shard_id)?.clone();

        let payload = self
            .runtime_adapter
            .dump_state(shard_id, prev_chunk_extra.state_root)
            .map_err(|err| ErrorKind::Other(err.to_string()))?;

        let outgoing_receipts = self.get_outgoing_receipts_for_shard(
            hash,
            shard_id,
            prev_chunk_header.height_included,
        )?;
        let incoming_receipts = ChainStoreUpdate::new(&mut self.store)
            .get_incoming_receipts_for_shard(shard_id, hash, &prev_chunk_header)?;

        Ok((prev_chunk_extra, payload, outgoing_receipts, incoming_receipts.clone()))
    }

    pub fn set_shard_state(
        &mut self,
        _me: &Option<AccountId>,
        shard_id: ShardId,
        sync_hash: CryptoHash,
        prev_extra: ChunkExtra,
        payload: Vec<u8>,
        outgoing_receipts: ReceiptResponse,
        incoming_receipts: Vec<ReceiptResponse>,
    ) -> Result<(), Error> {
        // TODO (#1126): verify that prev_state_root, payload and receipts match
        //    the corresponding merkle roots

        // Save state in the runtime, will also check it's validity.
        self.runtime_adapter
            .set_state(shard_id, prev_extra.state_root, payload)
            .map_err(|_| ErrorKind::InvalidStatePayload)?;

        // Update pointers to state root and receipts.
        let prev_block_hash = self.get_block_header(&sync_hash)?.inner.prev_hash;
        let mut chain_store_update = self.store.store_update();
        chain_store_update.save_chunk_extra(&prev_block_hash, shard_id, prev_extra);
        chain_store_update.save_outgoing_receipt(
            &outgoing_receipts.0,
            shard_id,
            outgoing_receipts.1,
        );
        for incoming_receipt in incoming_receipts {
            chain_store_update.save_incoming_receipt(
                &incoming_receipt.0,
                shard_id,
                incoming_receipt.1,
            );
        }
        chain_store_update.commit()?;

        Ok(())
    }

    /// Apply transactions in chunks for the next epoch in blocks that were blocked on the state sync
    pub fn catchup_blocks<F, F2>(
        &mut self,
        me: &Option<AccountId>,
        epoch_first_block: &CryptoHash,
        block_accepted: F,
        block_misses_chunks: F2,
    ) -> Result<(), Error>
    where
        F: Copy + FnMut(&Block, BlockStatus, Provenance) -> (),
        F2: Copy + FnMut(Vec<ShardChunkHeader>) -> (),
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

        // Skip processing the prev of epoch_start (thus cur=1), but keep it in the queue so that
        //    we later properly remove epoch_start itself from the permanent storage, since it is
        //    indexed by the prev block.
        let mut queue = vec![block.header.inner.prev_hash, *epoch_first_block];
        let mut cur = 1;

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

        for block_hash in queue {
            debug!(target: "chain", "Catching up: removing prev={:?} from the queue. I'm {:?}", block_hash, me);
            chain_store_update.remove_block_to_catchup(block_hash);
        }
        chain_store_update.remove_state_dl_info(*epoch_first_block);

        chain_store_update.commit()?;

        for hash in affected_blocks.iter() {
            self.check_orphans(me, hash.clone(), block_accepted, block_misses_chunks);
        }

        Ok(())
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

    /// Get a chunk from hash.
    #[inline]
    pub fn get_chunk(&mut self, chunk_hash: &ChunkHash) -> Result<&ShardChunk, Error> {
        self.store.get_chunk(chunk_hash)
    }

    /// Gets a chunk from header.
    #[inline]
    pub fn get_chunk_from_header(
        &mut self,
        header: &ShardChunkHeader,
    ) -> Result<&ShardChunk, Error> {
        self.store.get_chunk_from_header(header)
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

    /// Returns block header from the current chain for given height if present.
    #[inline]
    pub fn get_header_by_height(&mut self, height: BlockIndex) -> Result<&BlockHeader, Error> {
        let hash = self.store.get_block_hash_by_height(height)?;
        self.store.get_block_header(&hash)
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
    ) -> Result<&TransactionResult, Error> {
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
    pub fn runtime_adapter(&self) -> Arc<RuntimeAdapter> {
        self.runtime_adapter.clone()
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
    pub fn process_block_header(&mut self, header: &BlockHeader) -> Result<(), Error> {
        debug!(target: "chain", "Process block header: {} at {}", header.hash(), header.inner.height);

        self.check_header_known(header)?;
        self.validate_header(header, &Provenance::NONE)?;
        Ok(())
    }

    /// Find previous header or return Orphan error if not found.
    pub fn get_previous_header(&mut self, header: &BlockHeader) -> Result<&BlockHeader, Error> {
        self.chain_store_update.get_previous_header(header).map_err(|e| match e.kind() {
            ErrorKind::DBNotFoundErr(_) => ErrorKind::Orphan.into(),
            other => other.into(),
        })
    }

    pub fn save_incoming_receipts_from_block(
        &mut self,
        me: &Option<AccountId>,
        parent_hash: CryptoHash,
        block: &Block,
    ) -> Result<(), Error> {
        let all_chunks_or_oneparts: Vec<ShardFullChunkOrOnePart> =
            self.chain_store_update.get_chunks_or_one_parts(
                me,
                parent_hash,
                block.header.inner.height,
                self.runtime_adapter.clone(),
                block.chunks.as_ref(),
            )?;

        let mut receipts_by_shard_id = HashMap::new();

        let all_receipts = all_chunks_or_oneparts
            .iter()
            .map(|chunk_or_onepart| match chunk_or_onepart {
                ShardFullChunkOrOnePart::FullChunk(chunk) => chunk.receipts.clone(),
                ShardFullChunkOrOnePart::OnePart(onepart) => onepart.receipts.clone(),
                ShardFullChunkOrOnePart::NoChunk => vec![],
            })
            .flatten();

        for receipt in all_receipts {
            let shard_id = self.runtime_adapter.account_id_to_shard_id(&receipt.receiver_id);
            receipts_by_shard_id.entry(shard_id).or_insert_with(Vec::new).push(receipt);
        }

        for (shard_id, receipts) in receipts_by_shard_id {
            self.chain_store_update.save_incoming_receipt(&block.hash(), shard_id, receipts);
        }

        Ok(())
    }

    fn apply_chunks(
        &mut self,
        me: &Option<AccountId>,
        block: &Block,
        prev_block: &Block,
        mode: ApplyChunksMode,
    ) -> Result<(), Error> {
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
                    if prev_chunk_extra.state_root != chunk_header.inner.prev_state_root {
                        byzantine_assert!(false);
                        return Err(ErrorKind::InvalidStateRoot.into());
                    }

                    // It's safe here to use ChainStore instead of ChainStoreUpdate
                    // because we're asking prev_chunk_header for already committed block
                    let receipt_response =
                        self.chain_store_update.get_chain_store().get_outgoing_receipts_for_shard(
                            block.header.inner.prev_hash,
                            shard_id,
                            prev_chunk_header.height_included,
                        )?;
                    let outgoing_receipts_hashes =
                        self.runtime_adapter.build_receipts_hashes(&receipt_response.1)?;
                    let (outgoing_receipts_root, _) = merklize(&outgoing_receipts_hashes);

                    if outgoing_receipts_root != chunk_header.inner.receipts_root {
                        byzantine_assert!(false);
                        return Err(ErrorKind::InvalidReceiptsProof.into());
                    }

                    let receipts: Vec<Receipt> = self
                        .chain_store_update
                        .get_incoming_receipts_for_shard(shard_id, block.hash(), prev_chunk_header)?
                        .iter()
                        .map(|x| x.1.clone())
                        .flatten()
                        .collect();

                    let mut transactions = block
                        .transactions
                        .iter()
                        .filter(|tx| {
                            self.runtime_adapter.account_id_to_shard_id(&tx.transaction.signer_id)
                                == shard_id
                        })
                        .cloned()
                        .collect::<Vec<_>>();

                    let chunk =
                        self.chain_store_update.get_chunk_from_header(&chunk_header)?.clone();
                    transactions.extend(chunk.transactions.iter().cloned());

                    let any_transaction_is_invalid = transactions.iter().any(|t| {
                        !check_tx_history(
                            self.chain_store_update
                                .get_block_header(&t.transaction.block_hash)
                                .ok(),
                            chunk_header.inner.height_created,
                            self.transaction_validity_period,
                        )
                    });
                    if any_transaction_is_invalid {
                        debug!(target: "chain", "Invalid transactions in the block: {:?}", transactions);
                        return Err(ErrorKind::InvalidTransactions.into());
                    }
                    let gas_limit = chunk.header.inner.gas_limit;

                    // Apply block to runtime.
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
                            block.header.inner.gas_price,
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
                    for tx_result in apply_result.transaction_results.into_iter() {
                        self.chain_store_update
                            .save_transaction_result(&tx_result.hash, tx_result.result);
                    }
                } else {
                    let mut new_extra = self
                        .chain_store_update
                        .get_chunk_extra(&prev_block.hash(), shard_id)?
                        .clone();

                    // TODO(1306): Transactions directly included in the block are not getting applied if chunk is skipped.
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
                            block.header.inner.gas_price,
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
    fn process_block(
        &mut self,
        me: &Option<AccountId>,
        block: &Block,
        provenance: &Provenance,
    ) -> Result<(Option<Tip>, bool), Error> {
        debug!(target: "chain", "Process block {} at {}, approvals: {}, tx: {}, me: {:?}", block.hash(), block.header.inner.height, block.header.inner.approval_sigs.len(), block.transactions.len(), me);

        // Check if we have already processed this block previously.
        self.check_known(&block)?;

        // Delay hitting the db for current chain head until we know this block is not already known.
        let head = self.chain_store_update.head()?;
        let is_next = block.header.inner.prev_hash == head.last_block_hash;

        self.check_header_signature(&block.header)?;

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
                // always caught up. State download also doesn't happen until the next block.
                (false, true)
            } else {
                (self.prev_block_is_caught_up(&prev_prev_hash, &prev_hash)?, false)
            };

        debug!(target: "chain", "{:?} Process block {}, is_caught_up: {}, need_to_start_fetching_state: {}", me, block.hash(), is_caught_up, needs_to_start_fetching_state);

        // This is a fork in the context of both header and block processing
        // if this block does not immediately follow the chain head.
        // let is_fork = !is_next;

        // Check the header is valid before we proceed with the full block.
        self.process_header_for_block(&block.header, provenance)?;

        // Check that state root stored in the header matches the state root of the chunks
        let state_root = Block::compute_state_root(&block.chunks);
        if block.header.inner.prev_state_root != state_root {
            return Err(ErrorKind::InvalidStateRoot.into());
        }

        let prev_block = self.chain_store_update.get_block(&prev_hash)?.clone();

        self.save_incoming_receipts_from_block(me, prev_hash, &block)?;

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
            if chunk.inner.height_created == chunk.height_included {
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
            vec![],
            block.header.inner.chunk_mask.clone(),
            block.header.inner.gas_used,
            block.header.inner.gas_price,
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
    fn process_header_for_block(
        &mut self,
        header: &BlockHeader,
        provenance: &Provenance,
    ) -> Result<(), Error> {
        self.validate_header(header, provenance)?;
        self.chain_store_update.save_block_header(header.clone());
        self.update_header_head(header)?;
        Ok(())
    }

    fn check_header_signature(&self, header: &BlockHeader) -> Result<(), Error> {
        let validator =
            self.runtime_adapter.get_block_producer(&header.inner.epoch_id, header.inner.height)?;
        if self.runtime_adapter.verify_validator_signature(
            &header.inner.epoch_id,
            &validator,
            header.hash().as_ref(),
            &header.signature,
        ) == ValidatorSignatureVerificationResult::Valid
        {
            Ok(())
        } else {
            Err(ErrorKind::InvalidSignature.into())
        }
    }

    fn validate_header(
        &mut self,
        header: &BlockHeader,
        provenance: &Provenance,
    ) -> Result<(), Error> {
        // Refuse blocks from the too distant future.
        if header.timestamp() > Utc::now() + Duration::seconds(ACCEPTABLE_TIME_DIFFERENCE) {
            return Err(ErrorKind::InvalidBlockFutureTime(header.timestamp()).into());
        }

        // First I/O cost, delay as much as possible.
        self.check_header_signature(header)?;

        let prev_header = self.get_previous_header(header)?.clone();

        // Check that epoch_id in the header does match epoch given previous header (only if previous header is present).
        if self.runtime_adapter.get_epoch_id_from_prev_block(&header.inner.prev_hash).unwrap()
            != header.inner.epoch_id
        {
            return Err(ErrorKind::InvalidEpochHash.into());
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
            let prev_header = self.get_previous_header(header)?.clone();
            let weight = self.runtime_adapter.compute_block_weight(&prev_header, header)?;
            if weight != header.inner.total_weight {
                return Err(ErrorKind::InvalidBlockWeight.into());
            }
        }

        Ok(())
    }

    /// Update the header head if this header has most work.
    fn update_header_head(&mut self, header: &BlockHeader) -> Result<Option<Tip>, Error> {
        let header_head = self.chain_store_update.header_head()?;
        if header.inner.total_weight > header_head.total_weight {
            let tip = Tip::from_header(header);
            self.chain_store_update.save_header_head(&tip)?;
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
        // if we made a fork with more work than the head (which should also be true
        // when extending the head), update it
        let head = self.chain_store_update.head()?;
        if block.header.inner.total_weight > head.total_weight {
            let tip = Tip::from_header(&block.header);

            self.chain_store_update.save_body_head(&tip)?;
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
                if header.inner.height > 50 && header.inner.height < head.height - 50 {
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
}
