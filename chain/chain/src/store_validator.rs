use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::sync::Arc;
use std::time::{Duration, Instant};

use borsh::BorshDeserialize;
use strum::IntoEnumIterator;

use near_chain_configs::GenesisConfig;
use near_primitives::block::{Block, BlockHeader};
use near_primitives::borsh;
use near_primitives::epoch_manager::{BlockInfo, EpochInfo, AGGREGATOR_KEY};
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::{ChunkHash, ShardChunk, StateSyncInfo};
use near_primitives::transaction::ExecutionOutcomeWithIdAndProof;
use near_primitives::types::{AccountId, BlockHeight, EpochId, GCCount, ShardId};
use near_primitives::utils::get_block_shard_id_rev;
use near_store::{DBCol, Store, TrieChanges, NUM_COLS, SHOULD_COL_GC, SKIP_COL_GC};
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

    is_misc_set: bool,
    is_block_height_cmp_tail_prepared: bool,
    is_tx_refcount_calculated: bool,
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
            is_misc_set: false,
            is_block_height_cmp_tail_prepared: false,
            is_tx_refcount_calculated: false,
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
        for (key, value) in self.store.clone().iter(col) {
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
                    // Increase Tx Refcount for checking it later
                    self.check(
                        &validate::chunk_increase_tx_refcount,
                        &chunk_hash,
                        &shard_chunk,
                        col,
                    );
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
                DBCol::ColOutcomesByBlockHash => {
                    let block_hash = CryptoHash::try_from(key_ref)?;
                    let outcome_ids = HashSet::<CryptoHash>::try_from_slice(value_ref)?;
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
                    let outcome = ExecutionOutcomeWithIdAndProof::try_from_slice(value_ref)?;
                    // Outcome is reachable in ColOutcomesByBlockHash
                    self.check(
                        &validate::outcome_indexed_by_block_hash,
                        &outcome_id,
                        &outcome,
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
                    self.check(&validate::block_info_block_exists, &block_hash, &block_info, col);
                }
                DBCol::ColEpochInfo => {
                    if key_ref != AGGREGATOR_KEY {
                        let epoch_id = EpochId::try_from_slice(key_ref)?;
                        let epoch_info = EpochInfo::try_from_slice(value_ref)?;
                        // Epoch should exist
                        self.check(&validate::epoch_validity, &epoch_id, &epoch_info, col);
                        // TODO #2893: Bowen which consistency we expect from ColEpochInfo?
                    }
                }
                DBCol::ColEpochStart => {
                    let _epoch_id = EpochId::try_from_slice(key_ref)?;
                    let _epoch_start = BlockHeight::try_from_slice(value_ref)?;
                    // TODO #2893: Bowen which consistency we expect from ColEpochStart?
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
                DBCol::ColTransactionRefCount => {
                    let tx_hash = CryptoHash::try_from(key_ref)?;
                    let refcount = u64::try_from_slice(value_ref)?;
                    self.check(&validate::tx_refcount, &tx_hash, &refcount, col);
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

        // Check Head-Tail validity and fill cache with their values
        if let Err(e) = validate::head_tail_validity(self) {
            self.process_error(e, "HEAD / HEADER_HEAD / TAIL / CHUNK_TAIL", DBCol::ColBlockMisc)
        }
        for col in DBCol::iter() {
            if let Err(e) = self.validate_col(col) {
                self.process_error(e, col.to_string(), col)
            }
        }
        // There is no more than one Block which Height is lower than Tail and not equal to Genesis
        if let Err(e) = validate::block_height_cmp_tail(self) {
            self.process_error(e, "TAIL", DBCol::ColBlockMisc)
        }
        // Check that all refs are counted
        if let Err(e) = validate::tx_refcount_remaining(self) {
            self.process_error(e, "TX_REFCOUNT", DBCol::ColTransactionRefCount)
        }
        // Check GC counters
        if let Err(_) = validate::gc_col_count_total(self) {
            // TODO #2861
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
        match f(self, key, value) {
            Ok(_) => {}
            Err(e) => {
                self.process_error(e, key, col);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use near_store::test_utils::create_test_store;

    use super::*;
    use crate::test_utils::KeyValueRuntime;
    use crate::{Chain, ChainGenesis, DoomslugThresholdMode};

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
    fn test_cache_not_found() {
        let (mut chain, mut sv) = init();
        let genesis_hash = chain.genesis().hash().clone();
        match validate::block_height_validity(
            &mut sv,
            &CryptoHash::default(),
            chain.get_block(&genesis_hash).unwrap(),
        ) {
            Err(StoreValidatorError::CacheNotFound { .. }) => {}
            _ => assert!(false),
        }
        match validate::block_height_cmp_tail(&mut sv) {
            Err(StoreValidatorError::CacheNotFound { .. }) => {}
            _ => assert!(false),
        }
        assert!(validate::head_tail_validity(&mut sv).is_ok());
        match validate::block_height_cmp_tail(&mut sv) {
            Err(StoreValidatorError::CacheNotFound { .. }) => {}
            _ => assert!(false),
        }
        assert!(validate::block_height_validity(
            &mut sv,
            &CryptoHash::default(),
            chain.get_block(&genesis_hash).unwrap(),
        )
        .is_ok());
        assert!(validate::block_height_cmp_tail(&mut sv).is_ok());
    }

    #[test]
    fn test_validation_failed() {
        let (_chain, mut sv) = init();
        sv.inner.is_block_height_cmp_tail_prepared = true;
        assert!(validate::block_height_cmp_tail(&mut sv).is_ok());
        sv.inner.block_heights_less_tail.push(CryptoHash::default());
        assert!(validate::block_height_cmp_tail(&mut sv).is_ok());
        sv.inner.block_heights_less_tail.push(CryptoHash::default());
        match validate::block_height_cmp_tail(&mut sv) {
            Err(StoreValidatorError::ValidationFailed { .. }) => {}
            _ => assert!(false),
        }
    }
}
