use std::convert::TryFrom;
use std::path::Path;
use std::sync::{Arc, Mutex};

use near_chain::{BlockHeader, Error, ErrorKind, RuntimeAdapter, Weight};
use near_primitives::crypto::signature::{PublicKey, Signature};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::rpc::ABCIQueryResponse;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockIndex, MerkleHash, ShardId};
use near_store::{Store, StoreUpdate};
use near_store::{Trie, TrieUpdate};
use node_runtime::adapter::query_client;
use node_runtime::ethereum::EthashProvider;
use node_runtime::state_viewer::{AccountViewCallResult, TrieViewer};
use node_runtime::{ApplyState, Runtime, ETHASH_CACHE_PATH};

use crate::config::GenesisConfig;

/// Defines Nightshade state transition, authority rotation and block weight for fork choice rule.
pub struct NightshadeRuntime {
    genesis_config: GenesisConfig,

    trie: Arc<Trie>,
    trie_viewer: TrieViewer,
    runtime: Runtime,
}

impl NightshadeRuntime {
    pub fn new(home_dir: &Path, store: Arc<Store>, genesis_config: GenesisConfig) -> Self {
        let trie = Arc::new(Trie::new(store.clone()));
        let mut ethash_dir = home_dir.to_owned();
        ethash_dir.push(ETHASH_CACHE_PATH);
        let ethash_provider = Arc::new(Mutex::new(EthashProvider::new(ethash_dir.as_path())));
        let runtime = Runtime::new(ethash_provider.clone());
        let trie_viewer = TrieViewer::new(ethash_provider);
        NightshadeRuntime { genesis_config, trie, runtime, trie_viewer }
    }

    fn num_shards(&self) -> ShardId {
        // TODO: should be dynamic.
        self.genesis_config.num_shards
    }

    /// Maps account into shard, given current number of shards.
    fn account_to_shard(&self, account_id: AccountId) -> ShardId {
        // TODO: a better way to do this.
        ((hash(&account_id.into_bytes()).0).0[0] % (self.num_shards() as u8)) as u32
    }
}

impl RuntimeAdapter for NightshadeRuntime {
    fn genesis_state(&self, shard_id: ShardId) -> (StoreUpdate, MerkleHash) {
        let accounts = self
            .genesis_config
            .accounts
            .iter()
            .filter(|(account_id, _, _)| self.account_to_shard(account_id.clone()) == shard_id)
            .cloned()
            .collect::<Vec<_>>();
        let authorities = self
            .genesis_config
            .authorities
            .iter()
            .filter(|(account_id, _, _)| self.account_to_shard(account_id.clone()) == shard_id)
            .cloned()
            .collect::<Vec<_>>();
        let state_update = TrieUpdate::new(self.trie.clone(), MerkleHash::default());
        let (store_update, state_root) =
            self.runtime.apply_genesis_state(state_update, &accounts, &authorities);
        (store_update, state_root)
    }

    fn compute_block_weight(
        &self,
        prev_header: &BlockHeader,
        header: &BlockHeader,
    ) -> Result<Weight, Error> {
        let (_account_id, public_key, _) = &self.genesis_config.authorities
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
        block_index: BlockIndex,
        prev_block_hash: &CryptoHash,
        transactions: &Vec<SignedTransaction>,
    ) -> Result<(StoreUpdate, MerkleHash), String> {
        let apply_state = ApplyState {
            root: state_root.clone(),
            shard_id,
            block_index,
            parent_block_hash: *prev_block_hash,
        };
        // XXX: terrible place for clearing the cache.
        self.trie.clear_cache();
        let state_update = TrieUpdate::new(self.trie.clone(), apply_state.root);
        let apply_result = self.runtime.apply(
            state_update,
            &apply_state,
            &vec![], // TODO: prev receipts
            &transactions,
        );
        if apply_result.tx_result.len() > 0 {
            println!("{:?}", apply_result.tx_result);
        }
        Ok((apply_result.state_update, apply_result.root))
    }

    fn query(
        &self,
        state_root: MerkleHash,
        height: BlockIndex,
        path: &str,
        data: &[u8],
    ) -> Result<ABCIQueryResponse, String> {
        query_client(self, state_root, height, path, data)
    }
}

impl node_runtime::adapter::RuntimeAdapter for NightshadeRuntime {
    fn view_account(
        &self,
        state_root: MerkleHash,
        account_id: &AccountId,
    ) -> Result<AccountViewCallResult, String> {
        let state_update = TrieUpdate::new(self.trie.clone(), state_root);
        self.trie_viewer.view_account(&state_update, account_id)
    }

    fn call_function(
        &self,
        state_root: MerkleHash,
        height: BlockIndex,
        contract_id: &AccountId,
        method_name: &str,
        args: &[u8],
        logs: &mut Vec<String>,
    ) -> Result<Vec<u8>, String> {
        let state_update = TrieUpdate::new(self.trie.clone(), state_root);
        self.trie_viewer.call_function(state_update, height, contract_id, method_name, args, logs)
    }
}
