use std::sync::Arc;

use near_store::{Store, StoreUpdate};
use primitives::crypto::signature::{PublicKey, Signature};
use primitives::types::{AccountId, BlockIndex, MerkleHash};

use crate::error::{Error, ErrorKind};
use crate::types::{BlockHeader, RuntimeAdapter, Weight};
use primitives::test_utils::get_public_key_from_seed;
use primitives::transaction::SignedTransaction;

/// Simple key value runtime for tests.
pub struct KeyValueRuntime {
    store: Arc<Store>,
    root: MerkleHash,
    authorities: Vec<(AccountId, PublicKey)>,
}

impl KeyValueRuntime {
    pub fn new(store: Arc<Store>) -> Self {
        Self::new_with_authorities(store, vec!["test".to_string()])
    }

    pub fn new_with_authorities(store: Arc<Store>, authorities: Vec<AccountId>) -> Self {
        KeyValueRuntime {
            store,
            root: MerkleHash::default(),
            authorities: authorities
                .iter()
                .map(|account_id| (account_id.clone(), get_public_key_from_seed(account_id)))
                .collect(),
        }
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
        let (account_id, public_key) =
            &self.authorities[(header.height as usize) % self.authorities.len()];
        if !header.verify_block_producer(public_key) {
            return Err(ErrorKind::InvalidBlockProposer.into());
        }
        Ok(prev_header.total_weight.next(header.approval_sigs.len() as u64))
    }

    fn get_epoch_block_proposers(&self, _height: BlockIndex) -> Vec<AccountId> {
        self.authorities.iter().map(|x| x.0.clone()).collect()
    }

    fn get_block_proposer(&self, height: BlockIndex) -> Result<AccountId, String> {
        Ok(self.authorities[(height as usize) % self.authorities.len()].0.clone())
    }

    fn validate_authority_signature(
        &self,
        _account_id: &AccountId,
        _signature: &Signature,
    ) -> bool {
        true
    }

    fn apply_transactions(
        &self,
        state_root: &MerkleHash,
        transactions: &Vec<SignedTransaction>,
    ) -> Result<(StoreUpdate, MerkleHash), String> {
        Ok((self.store.store_update(), MerkleHash::default()))
    }
}
