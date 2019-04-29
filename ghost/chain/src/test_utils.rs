use std::sync::Arc;

use near_store::{Store, StoreUpdate};
use primitives::types::{AccountId, BlockIndex, MerkleHash};

use crate::error::{Error, ErrorKind};
use crate::types::{Block, BlockHeader, Bytes, Provenance, RuntimeAdapter, Weight};
use primitives::transaction::SignedTransaction;

/// Simple key value runtime for tests.
pub struct KeyValueRuntime {
    store: Arc<Store>,
    root: MerkleHash,
    authorities: Vec<AccountId>,
}

impl KeyValueRuntime {
    pub fn new(store: Arc<Store>) -> Self {
        Self::new_with_authorities(store, vec!["test".to_string()])
    }

    pub fn new_with_authorities(store: Arc<Store>, authorities: Vec<AccountId>) -> Self {
        KeyValueRuntime { store, root: MerkleHash::default(), authorities }
    }

    pub fn get_root(&self) -> MerkleHash {
        self.root
    }
}

impl RuntimeAdapter for KeyValueRuntime {
    fn genesis_state(&self) -> (StoreUpdate, MerkleHash) {
        (self.store.store_update(), MerkleHash::default())
    }

    fn compute_block_weight(
        &self,
        prev_header: &BlockHeader,
        header: &BlockHeader,
    ) -> Result<Weight, Error> {
        Ok(header.total_weight)
    }

    fn get_block_proposer(&self, height: BlockIndex) -> Option<AccountId> {
        Some(self.authorities[(height as usize) % self.authorities.len()].clone())
    }

    fn apply_transactions(
        &self,
        transactions: &Vec<SignedTransaction>,
    ) -> (StoreUpdate, MerkleHash) {
        (self.store.store_update(), MerkleHash::default())
    }
}
