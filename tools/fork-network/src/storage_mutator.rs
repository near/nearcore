use std::sync::Arc;

use near_chain::types::RuntimeAdapter;
use near_crypto::PublicKey;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::{
    account::{AccessKey, Account},
    borsh::BorshSerialize,
    hash::CryptoHash,
    trie_key::TrieKey,
    types::{AccountId, EpochId, StateRoot},
};
use near_store::{flat::FlatStateChanges, ShardTries, TrieUpdate};
use nearcore::NightshadeRuntime;

pub struct StorageMutator {
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    epoch_id: EpochId,
    tries: Vec<TrieUpdate>,
    shard_tries: ShardTries,
}

impl StorageMutator {
    pub fn new(
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        runtime: &NightshadeRuntime,
        epoch_id: &EpochId,
        prev_block_hash: CryptoHash,
        state_roots: &[StateRoot],
    ) -> anyhow::Result<Self> {
        let shard_layout = epoch_manager.get_shard_layout(epoch_id)?;
        let num_shards = shard_layout.num_shards();
        let mut trie_updates = Vec::with_capacity(num_shards as usize);
        for shard_id in 0..num_shards {
            let trie = runtime.get_trie_for_shard(
                shard_id,
                &prev_block_hash,
                state_roots[shard_id as usize],
                false,
            )?;
            let trie_update = TrieUpdate::new(trie);
            trie_updates.push(trie_update);
        }
        Ok(Self {
            epoch_manager,
            epoch_id: epoch_id.clone(),
            tries: trie_updates,
            shard_tries: runtime.get_tries(),
        })
    }

    pub fn set_account(&mut self, account_id: AccountId, value: Account) -> anyhow::Result<()> {
        let shard_id = self.epoch_manager.account_id_to_shard_id(&account_id, &self.epoch_id)?;
        self.tries[shard_id as usize].set(TrieKey::Account { account_id }, value.try_to_vec()?);
        Ok(())
    }

    pub fn set_access_key(
        &mut self,
        account_id: AccountId,
        public_key: PublicKey,
        access_key: AccessKey,
    ) -> anyhow::Result<()> {
        let shard_id = self.epoch_manager.account_id_to_shard_id(&account_id, &self.epoch_id)?;
        self.tries[shard_id as usize]
            .set(TrieKey::AccessKey { account_id, public_key }, access_key.try_to_vec()?);
        Ok(())
    }

    pub fn commit(self) -> anyhow::Result<Vec<StateRoot>> {
        let shard_layout = self.epoch_manager.get_shard_layout(&self.epoch_id)?;
        let all_shard_uids = shard_layout.get_shard_uids();
        let mut state_roots = Vec::new();

        let mut update = self.shard_tries.store_update();
        for (mut trie_update, shard_uid) in self.tries.into_iter().zip(all_shard_uids.into_iter()) {
            trie_update.commit(near_primitives::types::StateChangeCause::Migration);
            let (_, trie_updates, raw_changes) = trie_update.finalize()?;
            let state_root = self.shard_tries.apply_all(&trie_updates, shard_uid, &mut update);
            state_roots.push(state_root);
            let flat_state_changes = FlatStateChanges::from_state_changes(&raw_changes);
            flat_state_changes.apply_to_flat_state(&mut update, shard_uid);
        }
        update.commit()?;
        Ok(state_roots)
    }
}
