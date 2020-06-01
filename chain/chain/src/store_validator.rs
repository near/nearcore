use std::convert::TryFrom;
use std::sync::Arc;

use borsh::BorshDeserialize;

use near_chain_configs::GenesisConfig;
use near_primitives::borsh;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ChunkHash;
use near_primitives::types::AccountId;
use near_store::{DBCol, Store};

use crate::RuntimeAdapter;

mod validate;

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
            errors: vec![],
            tests: 0,
        }
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
            DBCol::ColBlockHeader | DBCol::ColBlock => {
                format!("{:?}", CryptoHash::try_from(key.as_ref()))
            }
            DBCol::ColChunks => format!("{:?}", ChunkHash::try_from_slice(key.as_ref())),
            _ => format!("{:?}", key),
        }
    }
    fn validate_col(&mut self, col: DBCol) {
        for (key, value) in self.store.clone().iter(col) {
            match col {
                DBCol::ColBlockHeader => {
                    // Block Header Hash is valid
                    self.check(&validate::block_header_validity, &key, &value, col);
                }
                DBCol::ColBlock => {
                    // Block Hash is valid
                    self.check(&validate::block_hash_validity, &key, &value, col);
                    // Block Header for current Block exists
                    self.check(&validate::block_header_exists, &key, &value, col);
                    // Block Height is greater or equal to tail, or to Genesis Height
                    self.check(&validate::block_height_cmp_tail, &key, &value, col);
                    // Chunks for current Block exist
                    self.check(&validate::block_chunks_exist, &key, &value, col);
                }
                DBCol::ColChunks => {
                    // Chunk Hash is valid
                    self.check(&validate::chunk_basic_validity, &key, &value, col);
                    // There is a State Root in the Trie
                    self.check(&validate::chunks_state_roots_in_trie, &key, &value, col);
                    // ShardChunk can be indexed by Height
                    self.check(&validate::chunks_indexed_by_height_created, &key, &value, col);
                }
                DBCol::ColChunkHashesByHeight => {
                    // ShardChunk which can be indexed by Height exists
                    self.check(&validate::chunk_of_height_exists, &key, &value, col);
                }
                _ => unimplemented!(),
            }
        }
    }
    pub fn validate(&mut self) {
        self.check(&validate::head_tail_validity, &[0], &[0], DBCol::ColBlockMisc);
        self.validate_col(DBCol::ColBlockHeader);
        self.validate_col(DBCol::ColBlock);
        self.validate_col(DBCol::ColChunks);
        self.validate_col(DBCol::ColChunkHashesByHeight);
    }

    fn check(
        &mut self,
        f: &dyn Fn(&StoreValidator, &[u8], &[u8]) -> Result<(), ErrorMessage>,
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
}
