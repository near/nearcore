use std::convert::TryFrom;
use std::sync::Arc;

use chrono::{DateTime, Utc};

use near_chain::{BlockHeader, Error, ErrorKind, RuntimeAdapter, Weight};
use near_store::{Store, StoreUpdate};
use node_runtime::chain_spec::ChainSpec;
use primitives::crypto::signature::{PublicKey, Signature};
use primitives::crypto::signer::InMemorySigner;
use primitives::hash::hash;
use primitives::transaction::SignedTransaction;
use primitives::types::{AccountId, Balance, BlockIndex, MerkleHash, ReadablePublicKey, ShardId};

use crate::config::GenesisConfig;

/// Defines Nightshade state transition, authority rotation and block weight for fork choice rule.
pub struct NightshadeRuntime {
    genesis_config: GenesisConfig,
    store: Arc<Store>,
}

impl NightshadeRuntime {
    pub fn new(store: Arc<Store>, genesis_config: GenesisConfig) -> Self {
        NightshadeRuntime { store, genesis_config }
    }
}

impl NightshadeRuntime {
    fn num_shards(&self) -> ShardId {
        // TODO: should be dynamic.
        self.genesis_config.num_shards
    }

    /// Maps account into shard, given current number of shards.
    fn account_to_shard(&self, account_id: AccountId) -> ShardId {
        // tODO: a better way to do this.
        ((hash(&account_id.into_bytes()).0).0[0] % (self.num_shards() as u8)) as u32
    }
}

impl RuntimeAdapter for NightshadeRuntime {
    fn genesis_state(&self, shard_id: ShardId) -> (StoreUpdate, MerkleHash) {
        let mut store_update = self.store.store_update();
        for (account_id, readable_pk, balance) in self.genesis_config.accounts.iter() {
            if self.account_to_shard(account_id.clone()) == shard_id {}
        }
        (self.store.store_update(), MerkleHash::default())
    }

    fn compute_block_weight(
        &self,
        prev_header: &BlockHeader,
        header: &BlockHeader,
    ) -> Result<Weight, Error> {
        let (account_id, public_key, _) = &self.genesis_config.authorities
            [(header.height as usize) % self.genesis_config.authorities.len()];
        if !header.verify_block_producer(&PublicKey::try_from(public_key.0.as_str()).unwrap()) {
            return Err(ErrorKind::InvalidBlockProposer.into());
        }
        Ok(prev_header.total_weight.next(header.approval_sigs.len() as u64))
    }

    fn get_epoch_block_proposers(&self, _height: BlockIndex) -> Vec<AccountId> {
        self.genesis_config.authorities.iter().map(|x| x.0.clone()).collect()
    }

    fn get_block_proposer(&self, height: BlockIndex) -> Result<AccountId, String> {
        Ok(self.genesis_config.authorities
            [(height as usize) % self.genesis_config.authorities.len()]
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
