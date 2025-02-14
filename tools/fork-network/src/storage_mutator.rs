use near_chain::types::RuntimeAdapter;
use near_crypto::PublicKey;
use near_mirror::key_mapping::map_account;
use near_primitives::account::{AccessKey, Account};
use near_primitives::borsh;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{
    AccountId, BlockHeight, ShardId, ShardIndex, StateRoot, StoreKey, StoreValue,
};
use near_store::adapter::flat_store::FlatStoreAdapter;
use near_store::adapter::StoreUpdateAdapter;
use near_store::flat::{FlatStateChanges, FlatStorageStatus};
use near_store::{DBCol, ShardTries};
use nearcore::NightshadeRuntime;

use anyhow::Context;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Stores the state root and next height we want to pass to apply_memtrie_changes() and delete_until_height()
/// When multiple StorageMutators in different threads want to commit changes to the same shard, they'll first
/// lock this and then update the fields with the result of the trie change.
struct InProgressRoot {
    state_root: StateRoot,
    // When we apply changes with apply_memtrie_changes(), we then call delete_until_height()
    // in order to garbage collect unneeded memtrie nodes.
    update_height: BlockHeight,
}

#[derive(Clone)]
pub(crate) struct ShardUpdateState {
    root: Arc<Mutex<InProgressRoot>>,
}

impl ShardUpdateState {
    // here we set the given state root as the one we start with, and we set Self::update_height to be
    // one bigger than the highest block height we have flat state for. The reason for this is that the
    // memtries will initially be loaded with nodes referenced by each block height we have deltas for.
    pub(crate) fn new(
        flat_store: &FlatStoreAdapter,
        shard_uid: ShardUId,
        state_root: CryptoHash,
    ) -> anyhow::Result<Self> {
        let deltas = flat_store
            .get_all_deltas_metadata(shard_uid)
            .with_context(|| format!("failed getting flat storage deltas for {}", shard_uid))?;

        let max_delta_height = deltas.iter().map(|d| d.block.height).max();
        let max_delta_height = match max_delta_height {
            Some(h) => h,
            None => {
                match flat_store.get_flat_storage_status(shard_uid).with_context(|| {
                    format!("failed getting flat storage status for {}", shard_uid)
                })? {
                    FlatStorageStatus::Ready(status) => status.flat_head.height,
                    status => anyhow::bail!(
                        "expected Ready flat storage for {}, got {:?}",
                        shard_uid,
                        status
                    ),
                }
            }
        };
        Ok(Self {
            root: Arc::new(Mutex::new(InProgressRoot {
                state_root,
                update_height: max_delta_height + 1,
            })),
        })
    }

    pub(crate) fn new_sharded(
        flat_store: &FlatStoreAdapter,
        shard_layout: &ShardLayout,
        state_roots: HashMap<ShardId, CryptoHash>,
    ) -> anyhow::Result<Vec<Self>> {
        assert_eq!(state_roots.len(), shard_layout.shard_ids().count());
        let mut update_state = vec![None; state_roots.len()];
        for (shard_id, state_root) in state_roots {
            let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, shard_layout);
            let state = Self::new(&flat_store, shard_uid, state_root)?;

            let shard_idx = shard_layout.get_shard_index(shard_id).unwrap();
            assert!(update_state[shard_idx].is_none());
            update_state[shard_idx] = Some(state);
        }
        Ok(update_state.into_iter().map(|s| s.unwrap()).collect())
    }

    pub(crate) fn state_root(&self) -> CryptoHash {
        self.root.lock().unwrap().state_root
    }
}

struct ShardUpdates {
    update_state: ShardUpdateState,
    updates: Vec<(TrieKey, Option<Vec<u8>>)>,
}

impl ShardUpdates {
    fn set(&mut self, key: TrieKey, value: Vec<u8>) {
        self.updates.push((key, Some(value)));
    }

    fn remove(&mut self, key: TrieKey) {
        self.updates.push((key, None));
    }
}

/// Object that updates the existing state. Combines all changes, commits them
/// and returns new state roots.
// TODO: add stats on how many keys are updated/removed/left in place and log it in commit()
pub(crate) struct StorageMutator {
    updates: Vec<ShardUpdates>,
    shard_tries: ShardTries,
    shard_layout: ShardLayout,
}

impl StorageMutator {
    pub(crate) fn new(
        runtime: &NightshadeRuntime,
        update_state: Vec<ShardUpdateState>,
        shard_layout: ShardLayout,
    ) -> anyhow::Result<Self> {
        let updates = update_state
            .into_iter()
            .map(|update_state| ShardUpdates { update_state, updates: Vec::new() })
            .collect();
        Ok(Self { updates, shard_tries: runtime.get_tries(), shard_layout })
    }

    fn set(&mut self, shard_idx: ShardIndex, key: TrieKey, value: Vec<u8>) -> anyhow::Result<()> {
        self.updates[shard_idx].set(key, value);
        Ok(())
    }

    pub(crate) fn remove(&mut self, shard_idx: ShardIndex, key: TrieKey) -> anyhow::Result<()> {
        self.updates[shard_idx].remove(key);
        Ok(())
    }

    pub(crate) fn set_account(
        &mut self,
        shard_idx: ShardIndex,
        account_id: AccountId,
        value: Account,
    ) -> anyhow::Result<()> {
        self.set(shard_idx, TrieKey::Account { account_id }, borsh::to_vec(&value)?)
    }

    pub(crate) fn map_account(
        &mut self,
        source_shard_idx: ShardIndex,
        account_id: AccountId,
        account: Account,
    ) -> anyhow::Result<()> {
        let new_account_id = map_account(&account_id, None);
        let new_shard_id = self.shard_layout.account_id_to_shard_id(&new_account_id);
        let new_shard_idx = self.shard_layout.get_shard_index(new_shard_id).unwrap();

        if new_account_id != account_id || source_shard_idx != new_shard_idx {
            self.remove(source_shard_idx, TrieKey::Account { account_id })?;
            self.set(
                new_shard_idx,
                TrieKey::Account { account_id: new_account_id },
                borsh::to_vec(&account).unwrap(),
            )?;
        }
        Ok(())
    }

    pub(crate) fn remove_access_key(
        &mut self,
        shard_idx: ShardIndex,
        account_id: AccountId,
        public_key: PublicKey,
    ) -> anyhow::Result<()> {
        self.remove(shard_idx, TrieKey::AccessKey { account_id, public_key })
    }

    pub(crate) fn set_access_key(
        &mut self,
        shard_idx: ShardIndex,
        account_id: AccountId,
        public_key: PublicKey,
        access_key: AccessKey,
    ) -> anyhow::Result<()> {
        self.set(
            shard_idx,
            TrieKey::AccessKey { account_id, public_key },
            borsh::to_vec(&access_key)?,
        )
    }

    pub(crate) fn map_data(
        &mut self,
        source_shard_idx: ShardIndex,
        account_id: AccountId,
        data_key: &StoreKey,
        value: StoreValue,
    ) -> anyhow::Result<()> {
        let new_account_id = map_account(&account_id, None);
        let new_shard_id = self.shard_layout.account_id_to_shard_id(&new_account_id);
        let new_shard_idx = self.shard_layout.get_shard_index(new_shard_id).unwrap();

        if new_account_id != account_id || source_shard_idx != new_shard_idx {
            self.remove(
                source_shard_idx,
                TrieKey::ContractData { account_id, key: data_key.to_vec() },
            )?;
            self.set(
                new_shard_idx,
                TrieKey::ContractData { account_id: new_account_id, key: data_key.to_vec() },
                borsh::to_vec(&value)?,
            )?;
        }
        Ok(())
    }

    pub(crate) fn map_code(
        &mut self,
        source_shard_idx: ShardIndex,
        account_id: AccountId,
        value: Vec<u8>,
    ) -> anyhow::Result<()> {
        let new_account_id = map_account(&account_id, None);
        let new_shard_id = self.shard_layout.account_id_to_shard_id(&new_account_id);
        let new_shard_idx = self.shard_layout.get_shard_index(new_shard_id).unwrap();

        if new_account_id != account_id || source_shard_idx != new_shard_idx {
            self.remove(source_shard_idx, TrieKey::ContractCode { account_id })?;
            self.set(new_shard_idx, TrieKey::ContractCode { account_id: new_account_id }, value)?;
        }
        Ok(())
    }

    pub(crate) fn remove_postponed_receipt(
        &mut self,
        shard_idx: ShardIndex,
        receiver_id: AccountId,
        receipt_id: CryptoHash,
    ) -> anyhow::Result<()> {
        self.remove(shard_idx, TrieKey::PostponedReceipt { receiver_id, receipt_id })
    }

    pub(crate) fn set_postponed_receipt(
        &mut self,
        shard_idx: ShardIndex,
        receipt: &Receipt,
    ) -> anyhow::Result<()> {
        self.set(
            shard_idx,
            TrieKey::PostponedReceipt {
                receiver_id: receipt.receiver_id().clone(),
                receipt_id: *receipt.receipt_id(),
            },
            borsh::to_vec(&receipt)?,
        )
    }

    pub(crate) fn map_received_data(
        &mut self,
        source_shard_idx: ShardIndex,
        account_id: AccountId,
        data_id: CryptoHash,
        data: &Option<Vec<u8>>,
    ) -> anyhow::Result<()> {
        let new_account_id = map_account(&account_id, None);
        let new_shard_id = self.shard_layout.account_id_to_shard_id(&new_account_id);
        let new_shard_idx = self.shard_layout.get_shard_index(new_shard_id).unwrap();

        if new_account_id != account_id || source_shard_idx != new_shard_idx {
            self.remove(
                source_shard_idx,
                TrieKey::ReceivedData { receiver_id: account_id, data_id },
            )?;
            self.set(
                new_shard_idx,
                TrieKey::ReceivedData { receiver_id: new_account_id, data_id },
                borsh::to_vec(data)?,
            )?;
        }
        Ok(())
    }

    pub(crate) fn should_commit(&self, batch_size: u64) -> bool {
        self.updates.len() >= batch_size as usize
    }

    /// Commits any pending trie changes for all shards
    pub(crate) fn commit(self) -> anyhow::Result<()> {
        let Self { updates, shard_tries, shard_layout } = self;

        for (shard_index, update) in updates.into_iter().enumerate() {
            let shard_id = shard_layout.get_shard_id(shard_index).unwrap();
            let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);
            commit_shard(shard_uid, &shard_tries, &update.update_state, update.updates)?;
        }
        Ok(())
    }
}

pub(crate) fn commit_shard(
    shard_uid: ShardUId,
    shard_tries: &ShardTries,
    update_state: &ShardUpdateState,
    updates: Vec<(TrieKey, Option<Vec<u8>>)>,
) -> anyhow::Result<()> {
    if updates.is_empty() {
        return Ok(());
    }
    let updates =
        updates.into_iter().map(|(trie_key, value)| (trie_key.to_vec(), value)).collect::<Vec<_>>();

    let mut root = update_state.root.lock().unwrap();
    let num_updates = updates.len();
    tracing::info!(?shard_uid, num_updates, "commit");
    let flat_state_changes = FlatStateChanges::from_raw_key_value(&updates);
    let mut update = shard_tries.store_update();
    flat_state_changes.apply_to_flat_state(&mut update.flat_store_update(), shard_uid);

    let trie_changes =
        shard_tries.get_trie_for_shard(shard_uid, root.state_root).update(updates)?;
    tracing::info!(
        ?shard_uid,
        num_trie_node_insertions = trie_changes.insertions().len(),
        num_trie_node_deletions = trie_changes.deletions().len()
    );
    let state_root = shard_tries.apply_all(&trie_changes, shard_uid, &mut update);
    shard_tries.apply_memtrie_changes(&trie_changes, shard_uid, root.update_height);
    // We may not have loaded memtries (some commands don't need to), so check.
    if let Some(memtries) = shard_tries.get_memtries(shard_uid) {
        memtries.write().unwrap().delete_until_height(root.update_height);
    }
    root.update_height += 1;
    root.state_root = state_root;

    tracing::info!(?shard_uid, num_updates, "committing");
    update.store_update().set_ser(
        DBCol::Misc,
        format!("FORK_TOOL_SHARD_ID:{}", shard_uid.shard_id).as_bytes(),
        &state_root,
    )?;

    update.commit()?;
    tracing::info!(?shard_uid, ?state_root, "Commit is done");
    Ok(())
}
