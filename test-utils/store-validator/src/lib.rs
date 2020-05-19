use std::sync::Arc;

use near_chain_configs::GenesisConfig;
use near_store::{DBCol, ShardTries, Store};

mod validate;

#[derive(Debug)]
pub struct ErrorMessage {
    pub col: DBCol,
    pub msg: String,
}

impl ErrorMessage {
    fn new(col: DBCol, msg: String) -> Self {
        Self { col, msg }
    }
}

pub struct StoreValidator {
    config: GenesisConfig,
    shard_tries: ShardTries,
    store: Arc<Store>,

    pub errors: Vec<ErrorMessage>,
    tests: u64,
}

impl StoreValidator {
    pub fn new(config: GenesisConfig, shard_tries: ShardTries, store: Arc<Store>) -> Self {
        StoreValidator {
            config,
            shard_tries: shard_tries.clone(),
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
    pub fn validate(&mut self) {
        self.check(&validate::nothing, &[0], &[0], DBCol::ColBlockMisc);
        for (key, value) in self.store.clone().iter(DBCol::ColBlockHeader) {
            // Block Header Hash is valid
            self.check(&validate::block_header_validity, &key, &value, DBCol::ColBlockHeader);
        }
        for (key, value) in self.store.clone().iter(DBCol::ColBlock) {
            // Block Hash is valid
            self.check(&validate::block_hash_validity, &key, &value, DBCol::ColBlock);
            // Block Header for current Block exists
            self.check(&validate::block_header_exists, &key, &value, DBCol::ColBlock);
            // Block Height is greater or equal to tail, or to Genesis Height
            self.check(&validate::block_height_cmp_tail, &key, &value, DBCol::ColBlock);
        }
        for (key, value) in self.store.clone().iter(DBCol::ColChunks) {
            // Chunk Hash is valid
            self.check(&validate::chunk_hash_validity, &key, &value, DBCol::ColChunks);
            // Block for current Chunk exists
            self.check(&validate::block_of_chunk_exists, &key, &value, DBCol::ColChunks);
            // There is a State Root in the Trie
            self.check(&validate::chunks_state_roots_in_trie, &key, &value, DBCol::ColChunks);
        }
        /*for shard_id in 0..self.shard_tries.tries.len() {
            println!("{}", shard_id);
            // TODO ??
        }*/
    }

    fn check(
        &mut self,
        f: &dyn Fn(&StoreValidator, &[u8], &[u8]) -> Result<(), String>,
        key: &[u8],
        value: &[u8],
        col: DBCol,
    ) {
        let result = f(self, key, value);
        self.tests += 1;
        match result {
            Ok(_) => {}
            Err(msg) => self.errors.push(ErrorMessage::new(col, msg)),
        }
    }
}
