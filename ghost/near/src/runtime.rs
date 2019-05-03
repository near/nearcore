use std::convert::TryFrom;
use std::sync::Arc;

use near_chain::{BlockHeader, Error, ErrorKind, RuntimeAdapter, Weight};
use near_store::{Store, StoreUpdate};
use node_runtime::chain_spec::ChainSpec;
use primitives::crypto::signature::{PublicKey, Signature};
use primitives::transaction::SignedTransaction;
use primitives::types::{AccountId, BlockIndex, MerkleHash, ShardId};

/// Defines Nightshade state transition, authority rotation and block weight for fork choice rule.
pub struct NightshadeRuntime {
    store: Arc<Store>,
    chain_spec: ChainSpec,
}

impl NightshadeRuntime {
    pub fn new(store: Arc<Store>, chain_spec: ChainSpec) -> Self {
        NightshadeRuntime { store, chain_spec }
    }
}

impl RuntimeAdapter for NightshadeRuntime {
    fn genesis_state(&self, shard_id: ShardId) -> (StoreUpdate, MerkleHash) {
        (self.store.store_update(), MerkleHash::default())
    }

    fn compute_block_weight(
        &self,
        prev_header: &BlockHeader,
        header: &BlockHeader,
    ) -> Result<Weight, Error> {
        let (account_id, public_key, _, _) = &self.chain_spec.initial_authorities
            [(header.height as usize) % self.chain_spec.initial_authorities.len()];
        if !header.verify_block_producer(&PublicKey::try_from(public_key.0.as_str()).unwrap()) {
            return Err(ErrorKind::InvalidBlockProposer.into());
        }
        Ok(prev_header.total_weight.next(header.approval_sigs.len() as u64))
    }

    fn get_epoch_block_proposers(&self, _height: BlockIndex) -> Vec<AccountId> {
        self.chain_spec.initial_authorities.iter().map(|x| x.0.clone()).collect()
    }

    fn get_block_proposer(&self, height: BlockIndex) -> Result<AccountId, String> {
        Ok(self.chain_spec.initial_authorities
            [(height as usize) % self.chain_spec.initial_authorities.len()]
        .0
        .clone())
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
        shard_id: ShardId,
        state_root: &MerkleHash,
        transactions: &Vec<SignedTransaction>,
    ) -> Result<(StoreUpdate, MerkleHash), String> {
        Ok((self.store.store_update(), MerkleHash::default()))
    }
}
