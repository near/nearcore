use borsh::BorshDeserialize;

use near_chain_configs::GenesisConfig;
use near_primitives::borsh;
use near_store::{DBCol, Store};

mod validate;

//
// All validations end here
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

#[derive(Default, BorshDeserialize)]
pub struct StoreValidator {
    #[borsh_skip]
    pub errors: Vec<ErrorMessage>,
    tests: u64,
}

impl StoreValidator {
    pub fn is_failed(&self) -> bool {
        self.tests == 0 || self.errors.len() > 0
    }
    pub fn num_failed(&self) -> u64 {
        self.errors.len() as u64
    }
    pub fn tests_done(&self) -> u64 {
        self.tests
    }
    pub fn validate(&mut self, store: &Store, config: &GenesisConfig) {
        self.check(&validate::nothing, store, config, &[0], &[0], DBCol::ColBlockMisc);
        for (key, value) in store.iter(DBCol::ColBlockHeader) {
            // Block Header Hash is valid
            self.check(
                &validate::block_header_validity,
                store,
                config,
                &key,
                &value,
                DBCol::ColBlockHeader,
            );
        }
        for (key, value) in store.iter(DBCol::ColBlock) {
            // Block Hash is valid
            self.check(
                &validate::block_hash_validity,
                store,
                config,
                &key,
                &value,
                DBCol::ColBlock,
            );
            // Block Header for current Block exists
            self.check(
                &validate::block_header_exists,
                store,
                config,
                &key,
                &value,
                DBCol::ColBlock,
            );
            // Block Height is greater or equal to tail, or to Genesis Height
            self.check(
                &validate::block_height_cmp_tail,
                store,
                config,
                &key,
                &value,
                DBCol::ColBlock,
            );
        }
        for (key, value) in store.iter(DBCol::ColChunks) {
            // Chunk Hash is valid
            self.check(
                &validate::chunk_hash_validity,
                store,
                config,
                &key,
                &value,
                DBCol::ColChunks,
            );
            // Block for current Chunk exists
            self.check(
                &validate::block_of_chunk_exists,
                store,
                config,
                &key,
                &value,
                DBCol::ColChunks,
            );
        }
    }

    fn check(
        &mut self,
        f: &dyn Fn(&Store, &GenesisConfig, &[u8], &[u8]) -> Result<(), String>,
        store: &Store,
        config: &GenesisConfig,
        key: &[u8],
        value: &[u8],
        col: DBCol,
    ) {
        let result = f(store, config, key, value);
        self.tests += 1;
        match result {
            Ok(_) => {}
            Err(msg) => self.errors.push(ErrorMessage::new(col, msg)),
        }
    }
}
