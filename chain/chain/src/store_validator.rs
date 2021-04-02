use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::sync::Arc;
use std::time::{Duration, Instant};

use borsh::BorshDeserialize;
use strum::IntoEnumIterator;
use tracing::warn;

use near_chain_configs::GenesisConfig;
use near_primitives::block::{Block, BlockHeader};
use near_primitives::borsh;
use near_primitives::epoch_manager::block_info::BlockInfo;
use near_primitives::epoch_manager::epoch_info::EpochInfo;
use near_primitives::epoch_manager::AGGREGATOR_KEY;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::{ChunkHash, ShardChunk, StateSyncInfo};
use near_primitives::syncing::{ShardStateSyncResponseHeader, StateHeaderKey, StatePartKey};
use near_primitives::transaction::ExecutionOutcomeWithIdAndProof;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{AccountId, BlockHeight, EpochId, GCCount, ShardId};
use near_primitives::utils::get_block_shard_id_rev;
use near_store::{
    decode_value_with_rc, DBCol, Store, TrieChanges, NUM_COLS, SHOULD_COL_GC, SKIP_COL_GC,
};
use validate::StoreValidatorError;

use crate::RuntimeAdapter;

mod validate;

fn to_string<T: std::fmt::Debug>(v: &T) -> String {
    format!("{:?}", v)
}

#[derive(Debug)]
pub struct StoreValidatorCache {
    head: BlockHeight,
    header_head: BlockHeight,
    tail: BlockHeight,
    chunk_tail: BlockHeight,
    block_heights_less_tail: Vec<CryptoHash>,
    gc_col: Vec<u64>,
    tx_refcount: HashMap<CryptoHash, u64>,
    receipt_refcount: HashMap<CryptoHash, u64>,
    block_refcount: HashMap<CryptoHash, u64>,
    genesis_blocks: Vec<CryptoHash>,
}

impl StoreValidatorCache {
    fn new() -> Self {
        Self {
            head: 0,
            header_head: 0,
            tail: 0,
            chunk_tail: 0,
            block_heights_less_tail: vec![],
            gc_col: vec![0; NUM_COLS],
            tx_refcount: HashMap::new(),
            receipt_refcount: HashMap::new(),
            block_refcount: HashMap::new(),
            genesis_blocks: vec![],
        }
    }
}

#[derive(Debug)]
pub struct ErrorMessage {
    pub col: String,
    pub key: String,
    pub err: StoreValidatorError,
}

pub struct StoreValidator {
    me: Option<AccountId>,
    config: GenesisConfig,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    store: Arc<Store>,
    inner: StoreValidatorCache,
    timeout: Option<u64>,
    start_time: Instant,

    pub errors: Vec<ErrorMessage>,
    tests: u64,
}

impl StoreValidator {
    pub fn new(
        me: Option<AccountId>,
        config: GenesisConfig,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        store: Arc<Store>,
    ) -> Self {
        StoreValidator {
            me,
            config,
            runtime_adapter,
            store: store.clone(),
            inner: StoreValidatorCache::new(),
            timeout: None,
            start_time: Instant::now(),
            errors: vec![],
            tests: 0,
        }
    }
    pub fn set_timeout(&mut self, timeout: u64) {
        self.timeout = Some(timeout)
    }
    pub fn is_failed(&self) -> bool {
        self.tests == 0 || self.errors.len() > 0
    }
    pub fn get_gc_counters(&self) -> Vec<(String, u64)> {
        let mut res = vec![];
        for col in DBCol::iter() {
            if SHOULD_COL_GC[col as usize] && self.inner.gc_col[col as usize] == 0 {
                if SKIP_COL_GC[col as usize] {
                    res.push((
                        to_string(&col) + " (skipping is acceptable)",
                        self.inner.gc_col[col as usize],
                    ))
                } else {
                    res.push((to_string(&col), self.inner.gc_col[col as usize]))
                }
            }
        }
        res.sort();
        res
    }
    pub fn num_failed(&self) -> u64 {
        self.errors.len() as u64
    }
    pub fn tests_done(&self) -> u64 {
        self.tests
    }
    fn process_error<K: std::fmt::Debug>(&mut self, err: StoreValidatorError, key: K, col: DBCol) {
        self.errors.push(ErrorMessage { key: to_string(&key), col: to_string(&col), err })
    }
    fn validate_col(&mut self, col: DBCol) -> Result<(), StoreValidatorError> {
        for (key, value) in self.store.clone().iter_without_rc_logic(col) {
            let key_ref = key.as_ref();
            let value_ref = value.as_ref();
            match col {
                DBCol::ColBlockHeader => {
                    let block_hash = CryptoHash::try_from(key_ref)?;
                    let header = BlockHeader::try_from_slice(value_ref)?;
                    // Block Header Hash is valid
                    self.check(&validate::block_header_hash_validity, &block_hash, &header, col);
                    // Block Header Height is valid
                    self.check(&validate::block_header_height_validity, &block_hash, &header, col);
                    // Block Header can be indexed by Height
                    self.check(&validate::header_hash_indexed_by_height, &block_hash, &header, col);
                }
                DBCol::ColBlock => {
                    let block_hash = CryptoHash::try_from(key_ref)?;
                    let block = Block::try_from_slice(value_ref)?;
                    // Block Hash is valid
                    self.check(&validate::block_hash_validity, &block_hash, &block, col);
                    // Block Height is valid
                    self.check(&validate::block_height_validity, &block_hash, &block, col);
                    // Block can be indexed by its Height
                    self.check(&validate::block_indexed_by_height, &block_hash, &block, col);
                    // Block Header for current Block exists
                    self.check(&validate::block_header_exists, &block_hash, &block, col);
                    // Chunks for current Block exist
                    self.check(&validate::block_chunks_exist, &block_hash, &block, col);
                    // Chunks for current Block have Height Created not higher than Block Height
                    self.check(&validate::block_chunks_height_validity, &block_hash, &block, col);
                    // BlockInfo for current Block exists
                    self.check(&validate::block_info_exists, &block_hash, &block, col);
                    // EpochInfo for current Epoch id of Block exists
                    self.check(&validate::block_epoch_exists, &block_hash, &block, col);
                    // Increase Block Refcount
                    self.check(&validate::block_increase_refcount, &block_hash, &block, col);
                }
                DBCol::ColBlockHeight => {
                    let height = BlockHeight::try_from_slice(key_ref)?;
                    let hash = CryptoHash::try_from(value_ref)?;
                    // Block on the Canonical Chain is stored properly
                    self.check(&validate::canonical_header_validity, &height, &hash, col);
                    // If prev Block exists, it's also on the Canonical Chain and
                    // there are no Blocks in range (prev_height, height) on the Canonical Chain
                    self.check(&validate::canonical_prev_block_validity, &height, &hash, col);
                }
                DBCol::ColChunks => {
                    let chunk_hash = ChunkHash::try_from_slice(key_ref)?;
                    let shard_chunk = ShardChunk::try_from_slice(value_ref)?;
                    // Chunk Hash is valid
                    self.check(&validate::chunk_hash_validity, &chunk_hash, &shard_chunk, col);
                    // Chunk Height Created is not lower than Chunk Tail
                    self.check(&validate::chunk_tail_validity, &chunk_hash, &shard_chunk, col);
                    // ShardChunk can be indexed by Height
                    self.check(
                        &validate::chunk_indexed_by_height_created,
                        &chunk_hash,
                        &shard_chunk,
                        col,
                    );
                    // Check that all Txs in Chunk exist
                    self.check(&validate::chunk_tx_exists, &chunk_hash, &shard_chunk, col);
                }
                DBCol::ColChunkExtra => {
                    let (block_hash, _) = get_block_shard_id_rev(key_ref)?;
                    let chunk_extra = ChunkExtra::try_from_slice(value_ref)?;
                    self.check(&validate::chunk_extra_block_exists, &block_hash, &chunk_extra, col);
                }
                DBCol::ColTrieChanges => {
                    let (block_hash, shard_id) = get_block_shard_id_rev(key_ref)?;
                    let trie_changes = TrieChanges::try_from_slice(value_ref)?;
                    // ShardChunk should exist for current TrieChanges
                    self.check(
                        &validate::trie_changes_chunk_extra_exists,
                        &(block_hash, shard_id),
                        &trie_changes,
                        col,
                    );
                }
                DBCol::ColChunkHashesByHeight => {
                    let height = BlockHeight::try_from_slice(key_ref)?;
                    let chunk_hashes = HashSet::<ChunkHash>::try_from_slice(value_ref)?;
                    // ShardChunk which can be indexed by Height exists
                    self.check(&validate::chunk_of_height_exists, &height, &chunk_hashes, col);
                }
                DBCol::ColHeaderHashesByHeight => {
                    let height = BlockHeight::try_from_slice(key_ref)?;
                    let header_hashes = HashSet::<CryptoHash>::try_from_slice(value_ref)?;
                    // Headers which can be indexed by Height exists
                    self.check(
                        &validate::header_hash_of_height_exists,
                        &height,
                        &header_hashes,
                        col,
                    );
                }
                DBCol::ColOutcomeIds => {
                    let (block_hash, _) = get_block_shard_id_rev(key_ref)?;
                    let outcome_ids = Vec::<CryptoHash>::try_from_slice(value_ref)?;
                    // TransactionResult which can be indexed by Outcome id exists
                    self.check(
                        &validate::outcome_by_outcome_id_exists,
                        &block_hash,
                        &outcome_ids,
                        col,
                    );
                    // Block which can be indexed by Outcome block_hash exists
                    self.check(&validate::outcome_id_block_exists, &block_hash, &outcome_ids, col);
                }
                DBCol::ColTransactionResult => {
                    let outcome_id = CryptoHash::try_from_slice(key_ref)?;
                    let outcomes =
                        <Vec<ExecutionOutcomeWithIdAndProof>>::try_from_slice(value_ref)?;
                    // Outcome is reachable in ColOutcomesByBlockHash
                    self.check(
                        &validate::outcome_indexed_by_block_hash,
                        &outcome_id,
                        &outcomes,
                        col,
                    );
                }
                DBCol::ColStateDlInfos => {
                    let block_hash = CryptoHash::try_from(key_ref)?;
                    let state_sync_info = StateSyncInfo::try_from_slice(value_ref)?;
                    // StateSyncInfo is valid
                    self.check(
                        &validate::state_sync_info_valid,
                        &block_hash,
                        &state_sync_info,
                        col,
                    );
                    // Block which can be indexed by StateSyncInfo exists
                    self.check(
                        &validate::state_sync_info_block_exists,
                        &block_hash,
                        &state_sync_info,
                        col,
                    );
                }
                DBCol::ColBlockInfo => {
                    let block_hash = CryptoHash::try_from(key_ref)?;
                    let block_info = BlockInfo::try_from_slice(value_ref)?;
                    // Block which can be indexed by BlockInfo exists
                    self.check(
                        &validate::block_info_block_header_exists,
                        &block_hash,
                        &block_info,
                        col,
                    );
                }
                DBCol::ColEpochInfo => {
                    if key_ref != AGGREGATOR_KEY {
                        let epoch_id = EpochId::try_from_slice(key_ref)?;
                        let epoch_info = EpochInfo::try_from_slice(value_ref)?;
                        // Epoch should exist
                        self.check(&validate::epoch_validity, &epoch_id, &epoch_info, col);
                    }
                }
                DBCol::ColLastBlockWithNewChunk => {
                    let shard_id = ShardId::try_from_slice(key_ref)?;
                    let block_hash = CryptoHash::try_from(value_ref)?;
                    // Block which is stored in ColLastBlockWithNewChunk exists and its ShardChunk is included
                    self.check(&validate::last_block_chunk_included, &shard_id, &block_hash, col);
                }
                DBCol::ColGCCount => {
                    let col = DBCol::try_from_slice(key_ref)?;
                    let count = GCCount::try_from_slice(value_ref)?;
                    self.check(&validate::gc_col_count, &col, &count, col);
                }
                DBCol::ColTransactions => {
                    let (_value, rc) = decode_value_with_rc(value_ref);
                    let tx_hash = CryptoHash::try_from(key_ref)?;
                    self.check(&validate::tx_refcount, &tx_hash, &(rc as u64), col);
                }
                DBCol::ColReceipts => {
                    let (_value, rc) = decode_value_with_rc(value_ref);
                    let receipt_id = CryptoHash::try_from(key_ref)?;
                    self.check(&validate::receipt_refcount, &receipt_id, &(rc as u64), col);
                }
                DBCol::ColBlockRefCount => {
                    let block_hash = CryptoHash::try_from(key_ref)?;
                    let refcount = u64::try_from_slice(value_ref)?;
                    self.check(&validate::block_refcount, &block_hash, &refcount, col);
                }
                DBCol::ColStateHeaders => {
                    let key = StateHeaderKey::try_from_slice(key_ref)?;
                    let header = ShardStateSyncResponseHeader::try_from_slice(value_ref)?;
                    self.check(&validate::state_header_block_exists, &key, &header, col);
                }
                DBCol::ColStateParts => {
                    let key = StatePartKey::try_from_slice(key_ref)?;
                    let part = value_ref.to_vec();
                    self.check(&validate::state_part_header_exists, &key, &part, col);
                }
                _ => {}
            }
            if let Some(timeout) = self.timeout {
                if self.start_time.elapsed() > Duration::from_millis(timeout) {
                    return Ok(());
                }
            }
        }
        Ok(())
    }
    pub fn validate(&mut self) {
        self.start_time = Instant::now();

        // Init checks
        // Check Head-Tail validity and fill cache with their values
        if let Err(e) = validate::head_tail_validity(self) {
            self.process_error(e, "HEAD / HEADER_HEAD / TAIL / CHUNK_TAIL", DBCol::ColBlockMisc)
        }

        // Main loop
        for col in DBCol::iter() {
            if let Err(e) = self.validate_col(col) {
                self.process_error(e, col.to_string(), col)
            }
            if let Some(timeout) = self.timeout {
                if self.start_time.elapsed() > Duration::from_millis(timeout) {
                    warn!(target: "adversary", "Store validator hit timeout at {:?} ({:?}/{:?})", col, col as usize, NUM_COLS);
                    return;
                }
            }
        }
        if let Some(timeout) = self.timeout {
            // We didn't complete all Column checks and cannot do final checks, returning here
            if self.start_time.elapsed() > Duration::from_millis(timeout) {
                warn!(target: "adversary", "Store validator hit timeout before final checks");
                return;
            }
        }

        // Final checks
        // There is no more than one Block which Height is lower than Tail and not equal to Genesis
        if let Err(e) = validate::block_height_cmp_tail_final(self) {
            self.process_error(e, "TAIL", DBCol::ColBlockMisc)
        }
        // Check GC counters
        if let Err(_) = validate::gc_col_count_final(self) {
            // TODO #2861
        }
        // Check that all refs are counted
        if let Err(e) = validate::tx_refcount_final(self) {
            self.process_error(e, "TX_REFCOUNT", DBCol::ColTransactions)
        }
        if let Err(e) = validate::receipt_refcount_final(self) {
            self.process_error(e, "RECEIPT_REFCOUNT", DBCol::ColReceipts)
        }
        // Check that all Block Refcounts are counted
        if let Err(e) = validate::block_refcount_final(self) {
            self.process_error(e, "BLOCK_REFCOUNT", DBCol::ColBlockRefCount)
        }
    }

    fn check<K: std::fmt::Debug, V>(
        &mut self,
        f: &dyn Fn(&mut StoreValidator, &K, &V) -> Result<(), StoreValidatorError>,
        key: &K,
        value: &V,
        col: DBCol,
    ) {
        self.tests += 1;
        if let Err(e) = f(self, key, value) {
            self.process_error(e, key, col);
        }
    }
}

#[cfg(test)]
mod tests {
    use near_store::test_utils::create_test_store;

    use crate::test_utils::KeyValueRuntime;
    use crate::{Chain, ChainGenesis, DoomslugThresholdMode};

    use super::*;

    fn init() -> (Chain, StoreValidator) {
        let store = create_test_store();
        let chain_genesis = ChainGenesis::test();
        let runtime_adapter = Arc::new(KeyValueRuntime::new(store.clone()));
        let mut genesis = GenesisConfig::default();
        genesis.genesis_height = 0;
        let chain =
            Chain::new(runtime_adapter.clone(), &chain_genesis, DoomslugThresholdMode::NoApprovals)
                .unwrap();
        (chain, StoreValidator::new(None, genesis.clone(), runtime_adapter, store))
    }

    #[test]
    fn test_io_error() {
        let (mut chain, mut sv) = init();
        let mut store_update = chain.store().owned_store().store_update();
        assert!(sv.validate_col(DBCol::ColBlock).is_ok());
        store_update
            .set_ser::<Vec<u8>>(
                DBCol::ColBlock,
                chain.get_block_by_height(0).unwrap().hash().as_ref(),
                &vec![123],
            )
            .unwrap();
        store_update.commit().unwrap();
        match sv.validate_col(DBCol::ColBlock) {
            Err(StoreValidatorError::IOError(_)) => {}
            _ => assert!(false),
        }
    }

    #[test]
    fn test_db_corruption() {
        let (chain, mut sv) = init();
        let mut store_update = chain.store().owned_store().store_update();
        assert!(sv.validate_col(DBCol::ColTrieChanges).is_ok());
        store_update.set_ser::<Vec<u8>>(DBCol::ColTrieChanges, "567".as_ref(), &vec![123]).unwrap();
        store_update.commit().unwrap();
        match sv.validate_col(DBCol::ColTrieChanges) {
            Err(StoreValidatorError::DBCorruption(_)) => {}
            _ => assert!(false),
        }
    }

    #[test]
    fn test_db_not_found() {
        let (mut chain, mut sv) = init();
        let block = chain.get_block_by_height(0).unwrap();
        assert!(validate::block_header_exists(&mut sv, &block.hash(), block).is_ok());
        match validate::block_header_exists(&mut sv, &CryptoHash::default(), block) {
            Err(StoreValidatorError::DBNotFound { .. }) => {}
            _ => assert!(false),
        }
    }

    #[test]
    fn test_discrepancy() {
        let (mut chain, mut sv) = init();
        let block_header = chain.get_header_by_height(0).unwrap();
        assert!(validate::block_header_hash_validity(&mut sv, block_header.hash(), block_header)
            .is_ok());
        match validate::block_header_hash_validity(&mut sv, &CryptoHash::default(), block_header) {
            Err(StoreValidatorError::Discrepancy { .. }) => {}
            _ => assert!(false),
        }
    }

    #[test]
    fn test_validation_failed() {
        let (_chain, mut sv) = init();
        assert!(validate::block_height_cmp_tail_final(&mut sv).is_ok());
        sv.inner.block_heights_less_tail.push(CryptoHash::default());
        assert!(validate::block_height_cmp_tail_final(&mut sv).is_ok());
        sv.inner.block_heights_less_tail.push(CryptoHash::default());
        match validate::block_height_cmp_tail_final(&mut sv) {
            Err(StoreValidatorError::ValidationFailed { .. }) => {}
            _ => assert!(false),
        }
    }
}
