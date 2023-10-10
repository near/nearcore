use std::sync::Arc;

use near_chain::types::RuntimeAdapter;
use near_crypto::PublicKey;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::receipt::Receipt;
use near_primitives::types::{ShardId, StoreKey, StoreValue};
use near_primitives::{
    account::{AccessKey, Account},
    borsh::{self},
    hash::CryptoHash,
    trie_key::TrieKey,
    types::{AccountId, EpochId, StateRoot},
};
use near_store::{flat::FlatStateChanges, ShardTries, TrieUpdate};
use nearcore::NightshadeRuntime;

/// Object that updates the existing state.
/// Combines all changes, commits them and returns new state roots of all shards.
pub(crate) struct StorageMutator {
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    epoch_id: EpochId,
    tries: Vec<TrieUpdate>,
    shard_tries: ShardTries,
}

impl StorageMutator {
    pub(crate) fn new(
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        runtime: &NightshadeRuntime,
        epoch_id: EpochId,
        prev_block_hash: CryptoHash,
        state_roots: Vec<StateRoot>,
    ) -> anyhow::Result<Self> {
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id)?;
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
        Ok(Self { epoch_manager, epoch_id, tries: trie_updates, shard_tries: runtime.get_tries() })
    }

    pub(crate) fn set_account(
        &mut self,
        account_id: AccountId,
        value: Account,
    ) -> anyhow::Result<()> {
        let shard_id = self.epoch_manager.account_id_to_shard_id(&account_id, &self.epoch_id)?;
        self.tries[shard_id as usize].set(TrieKey::Account { account_id }, borsh::to_vec(&value)?);
        Ok(())
    }

    pub(crate) fn delete_account(&mut self, account_id: AccountId) -> anyhow::Result<()> {
        let shard_id = self.epoch_manager.account_id_to_shard_id(&account_id, &self.epoch_id)?;
        self.tries[shard_id as usize].remove(TrieKey::Account { account_id });
        Ok(())
    }

    pub(crate) fn set_access_key(
        &mut self,
        account_id: AccountId,
        public_key: PublicKey,
        access_key: AccessKey,
    ) -> anyhow::Result<()> {
        let shard_id = self.epoch_manager.account_id_to_shard_id(&account_id, &self.epoch_id)?;
        self.tries[shard_id as usize]
            .set(TrieKey::AccessKey { account_id, public_key }, borsh::to_vec(&access_key)?);
        Ok(())
    }

    pub(crate) fn delete_access_key(
        &mut self,
        account_id: AccountId,
        public_key: PublicKey,
    ) -> anyhow::Result<()> {
        let shard_id = self.epoch_manager.account_id_to_shard_id(&account_id, &self.epoch_id)?;
        self.tries[shard_id as usize].remove(TrieKey::AccessKey { account_id, public_key });
        Ok(())
    }

    pub(crate) fn set_data(
        &mut self,
        account_id: AccountId,
        data_key: &StoreKey,
        value: StoreValue,
    ) -> anyhow::Result<()> {
        let shard_id = self.epoch_manager.account_id_to_shard_id(&account_id, &self.epoch_id)?;
        self.tries[shard_id as usize]
            .set(TrieKey::ContractData { account_id, key: data_key.to_vec() }, value.try_to_vec()?);
        Ok(())
    }

    pub(crate) fn delete_data(
        &mut self,
        account_id: AccountId,
        data_key: &StoreKey,
    ) -> anyhow::Result<()> {
        let shard_id = self.epoch_manager.account_id_to_shard_id(&account_id, &self.epoch_id)?;
        self.tries[shard_id as usize]
            .remove(TrieKey::ContractData { account_id, key: data_key.to_vec() });
        Ok(())
    }

    pub(crate) fn set_code(&mut self, account_id: AccountId, value: Vec<u8>) -> anyhow::Result<()> {
        let shard_id = self.epoch_manager.account_id_to_shard_id(&account_id, &self.epoch_id)?;
        self.tries[shard_id as usize].set(TrieKey::ContractCode { account_id }, value);
        Ok(())
    }

    pub(crate) fn delete_code(&mut self, account_id: AccountId) -> anyhow::Result<()> {
        let shard_id = self.epoch_manager.account_id_to_shard_id(&account_id, &self.epoch_id)?;
        self.tries[shard_id as usize].remove(TrieKey::ContractCode { account_id });
        Ok(())
    }

    pub(crate) fn set_postponed_receipt(&mut self, receipt: &Receipt) -> anyhow::Result<()> {
        let shard_id =
            self.epoch_manager.account_id_to_shard_id(&receipt.receiver_id, &self.epoch_id)?;
        self.tries[shard_id as usize].set(
            TrieKey::PostponedReceipt {
                receiver_id: receipt.receiver_id.clone(),
                receipt_id: receipt.receipt_id,
            },
            receipt.try_to_vec().unwrap(),
        );
        Ok(())
    }

    pub(crate) fn delete_postponed_receipt(&mut self, receipt: Box<Receipt>) -> anyhow::Result<()> {
        let shard_id =
            self.epoch_manager.account_id_to_shard_id(&receipt.receiver_id, &self.epoch_id)?;
        self.tries[shard_id as usize].remove(TrieKey::PostponedReceipt {
            receiver_id: receipt.receiver_id,
            receipt_id: receipt.receipt_id,
        });
        Ok(())
    }

    pub(crate) fn set_received_data(
        &mut self,
        account_id: AccountId,
        data_id: CryptoHash,
        data: &Option<Vec<u8>>,
    ) -> anyhow::Result<()> {
        let shard_id = self.epoch_manager.account_id_to_shard_id(&account_id, &self.epoch_id)?;
        self.tries[shard_id as usize]
            .set(TrieKey::ReceivedData { receiver_id: account_id, data_id }, data.try_to_vec()?);
        Ok(())
    }

    pub(crate) fn delete_received_data(
        &mut self,
        account_id: AccountId,
        data_id: CryptoHash,
    ) -> anyhow::Result<()> {
        let shard_id = self.epoch_manager.account_id_to_shard_id(&account_id, &self.epoch_id)?;
        self.tries[shard_id as usize]
            .remove(TrieKey::ReceivedData { receiver_id: account_id, data_id });
        Ok(())
    }

    pub(crate) fn set_delayed_receipt(
        &mut self,
        shard_id: ShardId,
        index: u64,
        receipt: &Receipt,
    ) -> anyhow::Result<()> {
        self.tries[shard_id as usize].set(TrieKey::DelayedReceipt { index }, receipt.try_to_vec()?);
        Ok(())
    }

    pub(crate) fn delete_delayed_receipt(
        &mut self,
        shard_id: ShardId,
        index: u64,
    ) -> anyhow::Result<()> {
        self.tries[shard_id as usize].remove(TrieKey::DelayedReceipt { index });
        Ok(())
    }

    pub(crate) fn commit(self) -> anyhow::Result<Vec<StateRoot>> {
        tracing::info!("StorageMutator::commit");
        let shard_layout = self.epoch_manager.get_shard_layout(&self.epoch_id)?;
        let all_shard_uids = shard_layout.get_shard_uids();
        let mut state_roots = Vec::new();

        let mut update = self.shard_tries.store_update();
        for (mut trie_update, shard_uid) in self.tries.into_iter().zip(all_shard_uids.into_iter()) {
            tracing::info!(?shard_uid, "finalizing");
            trie_update.commit(near_primitives::types::StateChangeCause::Migration);
            let (_, trie_updates, raw_changes) = trie_update.finalize()?;
            let state_root = self.shard_tries.apply_all(&trie_updates, shard_uid, &mut update);
            state_roots.push(state_root);
            let flat_state_changes = FlatStateChanges::from_state_changes(&raw_changes);
            flat_state_changes.apply_to_flat_state(&mut update, shard_uid);
        }
        tracing::info!("committing");
        update.commit()?;
        tracing::info!("commit is done");
        Ok(state_roots)
    }
}
