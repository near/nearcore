use std::convert::TryFrom;
use std::sync::Arc;
use std::time::{Duration, Instant};

use borsh::BorshDeserialize;

use near_chain_configs::GenesisConfig;
use near_primitives::borsh;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ChunkHash;
use near_primitives::types::{AccountId, BlockHeight};
use near_store::{DBCol, Store};

use crate::RuntimeAdapter;

mod validate;

#[derive(Debug)]
pub struct StoreValidatorCache {
    block_heights_less_tail: Vec<CryptoHash>,
    is_block_height_cmp_tail_prepared: bool,
}

impl StoreValidatorCache {
    fn new() -> Self {
        Self { block_heights_less_tail: vec![], is_block_height_cmp_tail_prepared: false }
    }
}

#[derive(Debug)]
pub struct ErrorMessage {
    pub col: Option<DBCol>,
    pub key: Option<String>,
    pub func: String,
    pub reason: String,
}

impl ErrorMessage {
    fn new(func: String, reason: String) -> Self {
        Self { col: None, key: None, func, reason }
    }
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
    pub fn num_failed(&self) -> u64 {
        self.errors.len() as u64
    }
    pub fn tests_done(&self) -> u64 {
        self.tests
    }
    fn col_to_key(col: DBCol, key: &[u8]) -> String {
        match col {
            DBCol::ColBlockHeight => format!("{:?}", BlockHeight::try_from_slice(key.as_ref())),
            DBCol::ColBlockHeader | DBCol::ColBlock => {
                format!("{:?}", CryptoHash::try_from(key.as_ref()))
            }
            DBCol::ColChunks => format!("{:?}", ChunkHash::try_from_slice(key.as_ref())),
            _ => format!("{:?}", std::str::from_utf8(key)),
        }
    }
    fn validate_col(&mut self, col: DBCol) {
        for (key, value) in self.store.clone().iter(col) {
            match col {
                DBCol::ColBlockHeader => {
                    // Block Header Hash is valid
                    self.check(&validate::block_header_basic_validity, &key, &value, col);
                }
                DBCol::ColBlock => {
                    // Block Hash is valid
                    self.check(&validate::block_basic_validity, &key, &value, col);
                    // Block Header for current Block exists
                    self.check(&validate::block_header_exists, &key, &value, col);
                    // Chunks for current Block exist
                    self.check(&validate::block_chunks_exist, &key, &value, col);
                }
                DBCol::ColBlockHeight => {
                    // Block on the Canonical Chain is stored properly
                    self.check(&validate::block_indexed_by_height, &key, &value, col);
                }
                DBCol::ColChunks => {
                    // Chunk Hash is valid
                    self.check(&validate::chunk_basic_validity, &key, &value, col);
                    // There is a State Root in the Trie
                    self.check(&validate::chunk_state_roots_in_trie, &key, &value, col);
                    // ShardChunk can be indexed by Height
                    self.check(&validate::chunk_indexed_by_height_created, &key, &value, col);
                }
                DBCol::ColChunkHashesByHeight => {
                    // ShardChunk which can be indexed by Height exists
                    self.check(&validate::chunk_of_height_exists, &key, &value, col);
                }
                _ => unimplemented!(),
            }
            if let Some(timeout) = self.timeout {
                if self.start_time.elapsed() > Duration::from_millis(timeout) {
                    return;
                }
            }
        }
    }
    pub fn validate(&mut self) {
        self.start_time = Instant::now();

        self.check_simple(
            &validate::head_tail_validity,
            "HEAD, TAIL, CHUNK_TAIL",
            DBCol::ColBlockMisc,
        );
        self.validate_col(DBCol::ColBlockHeader);
        self.validate_col(DBCol::ColBlockHeight);
        self.validate_col(DBCol::ColBlock);
        // There is no more than one Block which Height is lower than Tail and not equal to Genesis
        self.check_simple(&validate::block_height_cmp_tail, "TAIL", DBCol::ColBlockMisc);
        self.validate_col(DBCol::ColChunks);
        self.validate_col(DBCol::ColChunkHashesByHeight);
    }

    fn check(
        &mut self,
        f: &dyn Fn(&mut StoreValidator, &[u8], &[u8]) -> Result<(), ErrorMessage>,
        key: &[u8],
        value: &[u8],
        col: DBCol,
    ) {
        let result = f(self, key, value);
        self.tests += 1;
        match result {
            Ok(_) => {}
            Err(e) => {
                let mut e = e;
                e.col = Some(col);
                e.key = Some(Self::col_to_key(col, key));
                self.errors.push(e)
            }
        }
    }

    fn check_simple(
        &mut self,
        f: &dyn Fn(&mut StoreValidator, &[u8], &[u8]) -> Result<(), ErrorMessage>,
        key: &str,
        col: DBCol,
    ) {
        self.check(f, key.as_ref(), &[0], col)
    }
}
