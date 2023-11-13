use near_chain::types::RuntimeAdapter;
use near_crypto::PublicKey;
use near_primitives::account::{AccessKey, Account};
use near_primitives::borsh;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::shard_layout::ShardUId;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{AccountId, StateRoot};
use near_primitives::types::{ShardId, StoreKey, StoreValue};
use near_store::{flat::FlatStateChanges, DBCol, ShardTries, TrieUpdate};
use nearcore::NightshadeRuntime;

/// Object that updates the existing state. Combines all changes, commits them
/// and returns new state roots.
pub(crate) struct SingleShardStorageMutator {
    trie_update: TrieUpdate,
    shard_tries: ShardTries,
    num_changes: u64,
}

impl SingleShardStorageMutator {
    pub(crate) fn new(
        shard_id: ShardId,
        runtime: &NightshadeRuntime,
        prev_block_hash: CryptoHash,
        state_root: StateRoot,
    ) -> anyhow::Result<Self> {
        let trie = runtime.get_trie_for_shard(shard_id, &prev_block_hash, state_root, false)?;
        let trie_update = TrieUpdate::new(trie);
        Ok(Self { trie_update, shard_tries: runtime.get_tries(), num_changes: 0 })
    }

    fn trie_update(&mut self) -> &mut TrieUpdate {
        self.num_changes += 1;
        &mut self.trie_update
    }

    pub(crate) fn set_account(
        &mut self,
        account_id: AccountId,
        value: Account,
    ) -> anyhow::Result<()> {
        self.trie_update().set(TrieKey::Account { account_id }, borsh::to_vec(&value)?);
        Ok(())
    }

    pub(crate) fn delete_account(&mut self, account_id: AccountId) -> anyhow::Result<()> {
        self.trie_update().remove(TrieKey::Account { account_id });
        Ok(())
    }

    pub(crate) fn set_access_key(
        &mut self,
        account_id: AccountId,
        public_key: PublicKey,
        access_key: AccessKey,
    ) -> anyhow::Result<()> {
        self.trie_update()
            .set(TrieKey::AccessKey { account_id, public_key }, borsh::to_vec(&access_key)?);
        Ok(())
    }

    pub(crate) fn delete_access_key(
        &mut self,
        account_id: AccountId,
        public_key: PublicKey,
    ) -> anyhow::Result<()> {
        self.trie_update().remove(TrieKey::AccessKey { account_id, public_key });
        Ok(())
    }

    pub(crate) fn set_data(
        &mut self,
        account_id: AccountId,
        data_key: &StoreKey,
        value: StoreValue,
    ) -> anyhow::Result<()> {
        self.trie_update().set(
            TrieKey::ContractData { account_id, key: data_key.to_vec() },
            borsh::to_vec(&value)?,
        );
        Ok(())
    }

    pub(crate) fn delete_data(
        &mut self,
        account_id: AccountId,
        data_key: &StoreKey,
    ) -> anyhow::Result<()> {
        self.trie_update().remove(TrieKey::ContractData { account_id, key: data_key.to_vec() });
        Ok(())
    }

    pub(crate) fn set_code(&mut self, account_id: AccountId, value: Vec<u8>) -> anyhow::Result<()> {
        self.trie_update().set(TrieKey::ContractCode { account_id }, value);
        Ok(())
    }

    pub(crate) fn delete_code(&mut self, account_id: AccountId) -> anyhow::Result<()> {
        self.trie_update().remove(TrieKey::ContractCode { account_id });
        Ok(())
    }

    pub(crate) fn set_postponed_receipt(&mut self, receipt: &Receipt) -> anyhow::Result<()> {
        self.trie_update().set(
            TrieKey::PostponedReceipt {
                receiver_id: receipt.receiver_id.clone(),
                receipt_id: receipt.receipt_id,
            },
            borsh::to_vec(&receipt)?,
        );
        Ok(())
    }

    pub(crate) fn delete_postponed_receipt(&mut self, receipt: Box<Receipt>) -> anyhow::Result<()> {
        self.trie_update().remove(TrieKey::PostponedReceipt {
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
        self.trie_update()
            .set(TrieKey::ReceivedData { receiver_id: account_id, data_id }, borsh::to_vec(data)?);
        Ok(())
    }

    pub(crate) fn delete_received_data(
        &mut self,
        account_id: AccountId,
        data_id: CryptoHash,
    ) -> anyhow::Result<()> {
        self.trie_update().remove(TrieKey::ReceivedData { receiver_id: account_id, data_id });
        Ok(())
    }

    pub(crate) fn set_delayed_receipt(
        &mut self,
        index: u64,
        receipt: &Receipt,
    ) -> anyhow::Result<()> {
        self.trie_update().set(TrieKey::DelayedReceipt { index }, borsh::to_vec(receipt)?);
        Ok(())
    }

    pub(crate) fn delete_delayed_receipt(&mut self, index: u64) -> anyhow::Result<()> {
        self.trie_update().remove(TrieKey::DelayedReceipt { index });
        Ok(())
    }

    pub(crate) fn should_commit(&self, batch_size: u64) -> bool {
        self.num_changes >= batch_size
    }

    pub(crate) fn commit(mut self, shard_uid: &ShardUId) -> anyhow::Result<StateRoot> {
        tracing::info!(?shard_uid, num_changes = ?self.num_changes, "commit");
        let mut update = self.shard_tries.store_update();
        self.trie_update.commit(near_primitives::types::StateChangeCause::Migration);
        let (_, trie_updates, raw_changes) = self.trie_update.finalize()?;
        let state_root = self.shard_tries.apply_all(&trie_updates, *shard_uid, &mut update);
        let flat_state_changes = FlatStateChanges::from_state_changes(&raw_changes);
        flat_state_changes.apply_to_flat_state(&mut update, *shard_uid);
        tracing::info!(?shard_uid, num_changes = ?self.num_changes, "committing");
        update.set_ser(
            DBCol::Misc,
            format!("FORK_TOOL_SHARD_ID:{}", shard_uid.shard_id).as_bytes(),
            &state_root,
        )?;

        update.commit()?;
        tracing::info!(?shard_uid, ?state_root, "Commit is done");
        Ok(state_root)
    }
}
