use near_chain::types::RuntimeAdapter;
use near_crypto::PublicKey;
use near_primitives::account::{AccessKey, Account};
use near_primitives::borsh;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::shard_layout::ShardUId;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{AccountId, StateRoot};
use near_primitives::types::{StoreKey, StoreValue};
use near_store::adapter::StoreUpdateAdapter;
use near_store::{flat::FlatStateChanges, DBCol, ShardTries};
use nearcore::NightshadeRuntime;

/// Object that updates the existing state. Combines all changes, commits them
/// and returns new state roots.
pub(crate) struct SingleShardStorageMutator {
    updates: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    state_root: StateRoot,
    shard_tries: ShardTries,
}

impl SingleShardStorageMutator {
    pub(crate) fn new(runtime: &NightshadeRuntime, state_root: StateRoot) -> anyhow::Result<Self> {
        Ok(Self { updates: Vec::new(), state_root, shard_tries: runtime.get_tries() })
    }

    fn set(&mut self, key: TrieKey, value: Vec<u8>) -> anyhow::Result<()> {
        self.updates.push((key.to_vec(), Some(value)));
        Ok(())
    }

    fn remove(&mut self, key: TrieKey) -> anyhow::Result<()> {
        self.updates.push((key.to_vec(), None));
        Ok(())
    }

    pub(crate) fn set_account(
        &mut self,
        account_id: AccountId,
        value: Account,
    ) -> anyhow::Result<()> {
        self.set(TrieKey::Account { account_id }, borsh::to_vec(&value)?)
    }

    pub(crate) fn delete_account(&mut self, account_id: AccountId) -> anyhow::Result<()> {
        self.remove(TrieKey::Account { account_id })
    }

    pub(crate) fn set_access_key(
        &mut self,
        account_id: AccountId,
        public_key: PublicKey,
        access_key: AccessKey,
    ) -> anyhow::Result<()> {
        self.set(TrieKey::AccessKey { account_id, public_key }, borsh::to_vec(&access_key)?)
    }

    pub(crate) fn delete_access_key(
        &mut self,
        account_id: AccountId,
        public_key: PublicKey,
    ) -> anyhow::Result<()> {
        self.remove(TrieKey::AccessKey { account_id, public_key })
    }

    pub(crate) fn set_data(
        &mut self,
        account_id: AccountId,
        data_key: &StoreKey,
        value: StoreValue,
    ) -> anyhow::Result<()> {
        self.set(
            TrieKey::ContractData { account_id, key: data_key.to_vec() },
            borsh::to_vec(&value)?,
        )
    }

    pub(crate) fn delete_data(
        &mut self,
        account_id: AccountId,
        data_key: &StoreKey,
    ) -> anyhow::Result<()> {
        self.remove(TrieKey::ContractData { account_id, key: data_key.to_vec() })
    }

    pub(crate) fn set_code(&mut self, account_id: AccountId, value: Vec<u8>) -> anyhow::Result<()> {
        self.set(TrieKey::ContractCode { account_id }, value)
    }

    pub(crate) fn delete_code(&mut self, account_id: AccountId) -> anyhow::Result<()> {
        self.remove(TrieKey::ContractCode { account_id })
    }

    pub(crate) fn set_postponed_receipt(&mut self, receipt: &Receipt) -> anyhow::Result<()> {
        self.set(
            TrieKey::PostponedReceipt {
                receiver_id: receipt.receiver_id().clone(),
                receipt_id: *receipt.receipt_id(),
            },
            borsh::to_vec(&receipt)?,
        )
    }

    pub(crate) fn delete_postponed_receipt(&mut self, receipt: &Receipt) -> anyhow::Result<()> {
        self.remove(TrieKey::PostponedReceipt {
            receiver_id: receipt.receiver_id().clone(),
            receipt_id: *receipt.receipt_id(),
        })
    }

    pub(crate) fn set_received_data(
        &mut self,
        account_id: AccountId,
        data_id: CryptoHash,
        data: &Option<Vec<u8>>,
    ) -> anyhow::Result<()> {
        self.set(TrieKey::ReceivedData { receiver_id: account_id, data_id }, borsh::to_vec(data)?)
    }

    pub(crate) fn delete_received_data(
        &mut self,
        account_id: AccountId,
        data_id: CryptoHash,
    ) -> anyhow::Result<()> {
        self.remove(TrieKey::ReceivedData { receiver_id: account_id, data_id })
    }

    pub(crate) fn set_delayed_receipt(
        &mut self,
        index: u64,
        receipt: &Receipt,
    ) -> anyhow::Result<()> {
        self.set(TrieKey::DelayedReceipt { index }, borsh::to_vec(receipt)?)
    }

    pub(crate) fn delete_delayed_receipt(&mut self, index: u64) -> anyhow::Result<()> {
        self.remove(TrieKey::DelayedReceipt { index })
    }

    pub(crate) fn should_commit(&self, batch_size: u64) -> bool {
        self.updates.len() >= batch_size as usize
    }

    /// The fake block height is used to allow memtries to garbage collect.
    /// Otherwise it would take significantly more memory holding old nodes.
    pub(crate) fn commit(
        self,
        shard_uid: &ShardUId,
        fake_block_height: u64,
    ) -> anyhow::Result<StateRoot> {
        let num_updates = self.updates.len();
        tracing::info!(?shard_uid, num_updates, "commit");
        let flat_state_changes = FlatStateChanges::from_raw_key_value(&self.updates);
        let mut update = self.shard_tries.store_update();
        flat_state_changes.apply_to_flat_state(&mut update.flat_store_update(), *shard_uid);

        let trie_changes = self
            .shard_tries
            .get_trie_for_shard(*shard_uid, self.state_root)
            .update(self.updates)?;
        tracing::info!(
            ?shard_uid,
            num_trie_node_insertions = trie_changes.insertions().len(),
            num_trie_node_deletions = trie_changes.deletions().len()
        );
        let state_root = self.shard_tries.apply_all(&trie_changes, *shard_uid, &mut update);
        self.shard_tries.apply_memtrie_changes(&trie_changes, *shard_uid, fake_block_height);
        // We may not have loaded memtries (some commands don't need to), so check.
        if let Some(mem_tries) = self.shard_tries.get_mem_tries(*shard_uid) {
            mem_tries.write().unwrap().delete_until_height(fake_block_height - 1);
        }
        tracing::info!(?shard_uid, num_updates, "committing");
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
