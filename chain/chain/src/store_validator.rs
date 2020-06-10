use std::collections::HashSet;
use std::convert::TryFrom;
use std::sync::Arc;
use std::time::{Duration, Instant};

use borsh::BorshDeserialize;

use near_chain_configs::GenesisConfig;
use near_primitives::block::{Block, BlockHeader};
use near_primitives::borsh;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::{ChunkHash, ShardChunk};
use near_primitives::types::{AccountId, BlockHeight};
use near_primitives::utils::get_block_shard_id_rev;
use near_store::{DBCol, Store, TrieChanges};

use crate::RuntimeAdapter;

mod validate;

const STORAGE_COMMON_FAILURE: &str = "STORAGE_COMMON_FAILURE";

#[derive(Debug)]
pub struct StoreValidatorCache {
    head: BlockHeight,
    header_head: BlockHeight,
    tail: BlockHeight,
    chunk_tail: BlockHeight,
    block_heights_less_tail: Vec<CryptoHash>,

    is_misc_set: bool,
    is_block_height_cmp_tail_prepared: bool,
}

impl StoreValidatorCache {
    fn new() -> Self {
        Self {
            head: 0,
            header_head: 0,
            tail: 0,
            chunk_tail: 0,
            block_heights_less_tail: vec![],
            is_misc_set: false,
            is_block_height_cmp_tail_prepared: false,
        }
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
    fn new(func: &str, reason: String) -> Self {
        Self { col: None, key: None, func: func.to_string(), reason }
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
    fn process_error<K: std::fmt::Debug>(&mut self, mut e: ErrorMessage, key: K, col: DBCol) {
        e.col = Some(col);
        e.key = Some(format!("{:?}", key));
        self.errors.push(e)
    }
    fn unwrap_kv<K, V, E1: std::fmt::Debug, E2: std::fmt::Debug>(
        &mut self,
        key: Result<K, E1>,
        value: Result<V, E2>,
        key_ref: &[u8],
        col: DBCol,
    ) -> Option<(K, V)> {
        match key {
            Ok(key_data) => match value {
                Ok(value_data) => Some((key_data, value_data)),
                Err(e) => {
                    let err = ErrorMessage::new(
                        STORAGE_COMMON_FAILURE,
                        format!("Can't deserialize Value, {:?}", e),
                    );
                    self.process_error(err, key_ref, col);
                    None
                }
            },
            Err(e) => {
                let err = ErrorMessage::new(
                    STORAGE_COMMON_FAILURE,
                    format!("Can't deserialize Key, {:?}", e),
                );
                self.process_error(err, key_ref, col);
                None
            }
        }
    }
    fn validate_col(&mut self, col: DBCol) {
        for (key, value) in self.store.clone().iter(col) {
            let key_ref = key.as_ref();
            let value_ref = value.as_ref();
            match col {
                DBCol::ColBlockHeader => {
                    if let Some((block_hash, header)) = self.unwrap_kv(
                        CryptoHash::try_from(key_ref),
                        BlockHeader::try_from_slice(value_ref),
                        key_ref,
                        col,
                    ) {
                        // Block Header Hash is valid
                        self.check(
                            &validate::block_header_hash_validity,
                            &block_hash,
                            &header,
                            col,
                        );
                        // Block Header Height is valid
                        self.check(
                            &validate::block_header_height_validity,
                            &block_hash,
                            &header,
                            col,
                        );
                    }
                }
                DBCol::ColBlock => {
                    if let Some((block_hash, block)) = self.unwrap_kv(
                        CryptoHash::try_from(key_ref),
                        Block::try_from_slice(value_ref),
                        key_ref,
                        col,
                    ) {
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
                        self.check(
                            &validate::block_chunks_height_validity,
                            &block_hash,
                            &block,
                            col,
                        );
                    }
                }
                DBCol::ColBlockHeight => {
                    if let Some((height, hash)) = self.unwrap_kv(
                        BlockHeight::try_from_slice(key_ref),
                        CryptoHash::try_from(value_ref),
                        key_ref,
                        col,
                    ) {
                        // Block on the Canonical Chain is stored properly
                        self.check(&validate::canonical_header_validity, &height, &hash, col);
                        // If prev Block exists, it's also on the Canonical Chain and
                        // there are no Blocks in range (prev_height, height) on the Canonical Chain
                        self.check(&validate::canonical_prev_block_validity, &height, &hash, col);
                    }
                }
                DBCol::ColChunks => {
                    if let Some((chunk_hash, shard_chunk)) = self.unwrap_kv(
                        ChunkHash::try_from_slice(key_ref),
                        ShardChunk::try_from_slice(value_ref),
                        key_ref,
                        col,
                    ) {
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
                    }
                }
                DBCol::ColTrieChanges => {
                    if let Some(((block_hash, shard_id), trie_changes)) = self.unwrap_kv(
                        get_block_shard_id_rev(key_ref),
                        TrieChanges::try_from_slice(value_ref),
                        key_ref,
                        col,
                    ) {
                        // ShardChunk should exist for current TrieChanges
                        self.check(
                            &validate::trie_changes_chunk_extra_exists,
                            &(block_hash, shard_id),
                            &trie_changes,
                            col,
                        );
                    }
                }
                DBCol::ColChunkHashesByHeight => {
                    if let Some((height, chunk_hashes)) = self.unwrap_kv(
                        BlockHeight::try_from_slice(key_ref),
                        HashSet::<ChunkHash>::try_from_slice(value_ref),
                        key_ref,
                        col,
                    ) {
                        // ShardChunk which can be indexed by Height exists
                        self.check(&validate::chunk_of_height_exists, &height, &chunk_hashes, col);
                    }
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

        self.check(
            &validate::head_tail_validity,
            &"HEAD, TAIL, CHUNK_TAIL",
            &0,
            DBCol::ColBlockMisc,
        );
        self.validate_col(DBCol::ColBlockHeader);
        self.validate_col(DBCol::ColBlockHeight);
        self.validate_col(DBCol::ColBlock);
        // There is no more than one Block which Height is lower than Tail and not equal to Genesis
        self.check(&validate::block_height_cmp_tail, &"TAIL", &0, DBCol::ColBlockMisc);
        self.validate_col(DBCol::ColChunks);
        self.validate_col(DBCol::ColTrieChanges);
        self.validate_col(DBCol::ColChunkHashesByHeight);
    }

    fn check<K: std::fmt::Debug, V>(
        &mut self,
        f: &dyn Fn(&mut StoreValidator, &K, &V) -> Result<(), ErrorMessage>,
        key: &K,
        value: &V,
        col: DBCol,
    ) {
        let result = f(self, key, value);
        self.tests += 1;
        match result {
            Ok(_) => {}
            Err(e) => {
                self.process_error(e, key, col);
            }
        }
    }
}
