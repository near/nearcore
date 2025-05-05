use near_crypto::PublicKey;
use near_mirror::key_mapping::map_account;
use near_primitives::account::{AccessKey, Account};
use near_primitives::bandwidth_scheduler::{
    BandwidthSchedulerState, BandwidthSchedulerStateV1, LinkAllowance,
};
use near_primitives::borsh;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{
    AccountId, BlockHeight, ShardIndex, StateChangeCause, StateRoot, StoreKey, StoreValue,
};
use near_store::adapter::StoreUpdateAdapter;
use near_store::adapter::flat_store::FlatStoreAdapter;
use near_store::flat::{BlockInfo, FlatStateChanges, FlatStorageReadyStatus, FlatStorageStatus};
use near_store::trie::AccessOptions;
use near_store::trie::update::TrieUpdateResult;
use near_store::{DBCol, ShardTries};

use anyhow::Context;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

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
    root: Arc<Mutex<Option<InProgressRoot>>>,
}

impl ShardUpdateState {
    // here we set the given state root as the one we start with, and we set Self::update_height to be
    // one bigger than the highest block height we have flat state for. The reason for this is that the
    // memtries will initially be loaded with nodes referenced by each block height we have deltas for.
    fn new(
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
            root: Arc::new(Mutex::new(Some(InProgressRoot {
                state_root,
                update_height: max_delta_height + 1,
            }))),
        })
    }

    fn new_empty() -> Self {
        Self { root: Arc::new(Mutex::new(None)) }
    }

    /// Returns a vec of length equal to the number of shards in `target_shard_layout`,
    /// indexed by ShardIndex.
    /// `state_roots` should have ShardUIds belonging to source_shard_layout
    pub(crate) fn new_update_state(
        flat_store: &FlatStoreAdapter,
        source_shard_layout: &ShardLayout,
        target_shard_layout: &ShardLayout,
        state_roots: &HashMap<ShardUId, CryptoHash>,
    ) -> anyhow::Result<Vec<Self>> {
        let source_shards = source_shard_layout.shard_uids().collect::<HashSet<_>>();
        assert_eq!(&source_shards, &state_roots.iter().map(|(k, _v)| *k).collect::<HashSet<_>>());
        let target_shards = target_shard_layout.shard_uids().collect::<HashSet<_>>();
        let mut update_state = vec![None; target_shards.len()];
        for (shard_uid, state_root) in state_roots {
            if !target_shards.contains(shard_uid) {
                continue;
            }

            let state = Self::new(&flat_store, *shard_uid, *state_root)?;

            let shard_idx = target_shard_layout.get_shard_index(shard_uid.shard_id()).unwrap();
            assert!(update_state[shard_idx].is_none());
            update_state[shard_idx] = Some(state);
        }
        for shard_uid in target_shards {
            if source_shards.contains(&shard_uid) {
                continue;
            }
            let state = Self::new_empty();

            let shard_idx = target_shard_layout.get_shard_index(shard_uid.shard_id()).unwrap();
            assert!(update_state[shard_idx].is_none());
            update_state[shard_idx] = Some(state);
        }

        Ok(update_state.into_iter().map(|s| s.unwrap()).collect())
    }

    pub(crate) fn state_root(&self) -> CryptoHash {
        self.root.lock().as_ref().map_or_else(CryptoHash::default, |s| s.state_root)
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
    target_shard_layout: ShardLayout,
    // For efficiency/convenience
    target_shards: HashSet<ShardUId>,
}

struct MappedAccountId {
    new_account_id: AccountId,
    source: Option<ShardIndex>,
    target: ShardIndex,
    need_rewrite: bool,
}

impl StorageMutator {
    pub(crate) fn new(
        shard_tries: ShardTries,
        update_state: Vec<ShardUpdateState>,
        target_shard_layout: ShardLayout,
    ) -> anyhow::Result<Self> {
        let updates = update_state
            .into_iter()
            .map(|update_state| ShardUpdates { update_state, updates: Vec::new() })
            .collect();
        let target_shards = target_shard_layout.shard_uids().collect();
        Ok(Self { updates, shard_tries, target_shard_layout, target_shards })
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

    fn mapped_account_id(
        &self,
        source_shard_uid: ShardUId,
        account_id: &AccountId,
    ) -> MappedAccountId {
        let new_account_id = map_account(&account_id, None);
        let target_shard_id = self.target_shard_layout.account_id_to_shard_id(&new_account_id);
        let target = self.target_shard_layout.get_shard_index(target_shard_id).unwrap();
        let source = if self.target_shards.contains(&source_shard_uid) {
            Some(self.target_shard_layout.get_shard_index(source_shard_uid.shard_id()).unwrap())
        } else {
            None
        };
        let need_rewrite = account_id != &new_account_id || source != Some(target);
        MappedAccountId { new_account_id, source, target, need_rewrite }
    }

    pub(crate) fn map_account(
        &mut self,
        source_shard_uid: ShardUId,
        account_id: AccountId,
        account: Account,
    ) -> anyhow::Result<()> {
        let mapped = self.mapped_account_id(source_shard_uid, &account_id);

        if mapped.need_rewrite {
            if let Some(source_shard_idx) = mapped.source {
                self.remove(source_shard_idx, TrieKey::Account { account_id })?;
            }
            self.set(
                mapped.target,
                TrieKey::Account { account_id: mapped.new_account_id },
                borsh::to_vec(&account).unwrap(),
            )?;
        }
        Ok(())
    }

    pub(crate) fn remove_access_key(
        &mut self,
        source_shard_uid: ShardUId,
        account_id: AccountId,
        public_key: PublicKey,
    ) -> anyhow::Result<()> {
        if self.target_shards.contains(&source_shard_uid) {
            let shard_idx =
                self.target_shard_layout.get_shard_index(source_shard_uid.shard_id()).unwrap();
            self.remove(shard_idx, TrieKey::AccessKey { account_id, public_key })?;
        }
        Ok(())
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
        source_shard_uid: ShardUId,
        account_id: AccountId,
        data_key: &StoreKey,
        value: StoreValue,
    ) -> anyhow::Result<()> {
        let mapped = self.mapped_account_id(source_shard_uid, &account_id);

        if mapped.need_rewrite {
            if let Some(source_shard_idx) = mapped.source {
                self.remove(
                    source_shard_idx,
                    TrieKey::ContractData { account_id, key: data_key.to_vec() },
                )?;
            }
            self.set(
                mapped.target,
                TrieKey::ContractData { account_id: mapped.new_account_id, key: data_key.to_vec() },
                borsh::to_vec(&value)?,
            )?;
        }
        Ok(())
    }

    pub(crate) fn map_code(
        &mut self,
        source_shard_uid: ShardUId,
        account_id: AccountId,
        value: Vec<u8>,
    ) -> anyhow::Result<()> {
        let mapped = self.mapped_account_id(source_shard_uid, &account_id);

        if mapped.need_rewrite {
            if let Some(source_shard_idx) = mapped.source {
                self.remove(source_shard_idx, TrieKey::ContractCode { account_id })?;
            }
            self.set(
                mapped.target,
                TrieKey::ContractCode { account_id: mapped.new_account_id },
                value,
            )?;
        }
        Ok(())
    }

    pub(crate) fn remove_postponed_receipt(
        &mut self,
        source_shard_uid: ShardUId,
        receiver_id: AccountId,
        receipt_id: CryptoHash,
    ) -> anyhow::Result<()> {
        if self.target_shards.contains(&source_shard_uid) {
            let shard_idx =
                self.target_shard_layout.get_shard_index(source_shard_uid.shard_id()).unwrap();
            self.remove(shard_idx, TrieKey::PostponedReceipt { receiver_id, receipt_id })?;
        }
        Ok(())
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
        source_shard_uid: ShardUId,
        account_id: AccountId,
        data_id: CryptoHash,
        data: &Option<Vec<u8>>,
    ) -> anyhow::Result<()> {
        let mapped = self.mapped_account_id(source_shard_uid, &account_id);

        if mapped.need_rewrite {
            if let Some(source_shard_idx) = mapped.source {
                self.remove(
                    source_shard_idx,
                    TrieKey::ReceivedData { receiver_id: account_id, data_id },
                )?;
            }
            self.set(
                mapped.target,
                TrieKey::ReceivedData { receiver_id: mapped.new_account_id, data_id },
                borsh::to_vec(data)?,
            )?;
        }
        Ok(())
    }

    fn set_bandwidth_scheduler_state(
        &mut self,
        shard_idx: ShardIndex,
        state: BandwidthSchedulerState,
    ) -> anyhow::Result<()> {
        self.set(shard_idx, TrieKey::BandwidthSchedulerState, borsh::to_vec(&state)?)
    }

    pub(crate) fn should_commit(&self, batch_size: u64) -> bool {
        self.updates.len() >= batch_size as usize
    }

    /// Commits any pending trie changes for all shards
    pub(crate) fn commit(self) -> anyhow::Result<Vec<StateRoot>> {
        let Self { updates, shard_tries, target_shard_layout, .. } = self;
        let mut state_roots = Vec::new();

        for (shard_index, update) in updates.into_iter().enumerate() {
            let shard_id = target_shard_layout.get_shard_id(shard_index).unwrap();
            let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, &target_shard_layout);
            let new_state_root =
                commit_shard(shard_uid, &shard_tries, &update.update_state, update.updates)?;
            state_roots.push(new_state_root);
        }
        Ok(state_roots)
    }
}

fn commit_to_existing_state(
    shard_tries: &ShardTries,
    shard_uid: ShardUId,
    root: &mut InProgressRoot,
    updates: Vec<(TrieKey, Option<Vec<u8>>)>,
) -> anyhow::Result<()> {
    let updates =
        updates.into_iter().map(|(trie_key, value)| (trie_key.to_vec(), value)).collect::<Vec<_>>();

    let num_updates = updates.len();
    tracing::info!(?shard_uid, num_updates, "commit");
    let flat_state_changes = FlatStateChanges::from_raw_key_value(&updates);
    let mut update = shard_tries.store_update();
    flat_state_changes.apply_to_flat_state(&mut update.flat_store_update(), shard_uid);

    let trie_changes = shard_tries
        .get_trie_for_shard(shard_uid, root.state_root)
        .update(updates, AccessOptions::DEFAULT)
        .with_context(|| format!("failed updating trie for shard {}", shard_uid))?;
    tracing::info!(
        ?shard_uid,
        num_trie_node_insertions = trie_changes.insertions().len(),
        num_trie_node_deletions = trie_changes.deletions().len()
    );
    let state_root = shard_tries.apply_all(&trie_changes, shard_uid, &mut update);
    shard_tries.apply_memtrie_changes(&trie_changes, shard_uid, root.update_height);
    // We may not have loaded memtries (some commands don't need to), so check.
    if let Some(memtries) = shard_tries.get_memtries(shard_uid) {
        memtries.write().delete_until_height(root.update_height);
    }
    root.update_height += 1;
    root.state_root = state_root;

    tracing::info!(?shard_uid, num_updates, "committing");
    let key = crate::cli::make_state_roots_key(shard_uid);
    update.store_update().set_ser(DBCol::Misc, &key, &state_root)?;

    update.commit()?;
    tracing::info!(?shard_uid, ?state_root, "Commit is done");
    Ok(())
}

fn commit_to_new_state(
    shard_tries: &ShardTries,
    shard_uid: ShardUId,
    updates: Vec<(TrieKey, Option<Vec<u8>>)>,
) -> anyhow::Result<StateRoot> {
    let num_updates = updates.len();
    tracing::info!(?shard_uid, num_updates, "commit new");

    let mut trie_update = shard_tries.new_trie_update(shard_uid, StateRoot::default());
    for (key, value) in updates {
        match value {
            Some(value) => trie_update.set(key, value),
            None => trie_update.remove(key),
        }
    }
    trie_update.commit(StateChangeCause::InitialState);
    let TrieUpdateResult { trie_changes, state_changes, .. } =
        trie_update.finalize().with_context(|| {
            format!("Initial trie update finalization failed for shard {}", shard_uid)
        })?;
    let mut store_update = shard_tries.store_update();
    let state_root = shard_tries.apply_all(&trie_changes, shard_uid, &mut store_update);
    FlatStateChanges::from_state_changes(&state_changes)
        .apply_to_flat_state(&mut store_update.flat_store_update(), shard_uid);
    let key = crate::cli::make_state_roots_key(shard_uid);
    store_update.store_update().set_ser(DBCol::Misc, &key, &state_root)?;
    tracing::info!(?shard_uid, "committing initial state to new shard");
    store_update
        .commit()
        .with_context(|| format!("Initial flat storage commit failed for shard {}", shard_uid))?;

    Ok(state_root)
}

pub(crate) fn commit_shard(
    shard_uid: ShardUId,
    // TODO: Don't create the Trie object every time
    shard_tries: &ShardTries,
    update_state: &ShardUpdateState,
    updates: Vec<(TrieKey, Option<Vec<u8>>)>,
) -> anyhow::Result<StateRoot> {
    let mut root = update_state.root.lock();

    let new_root = match root.as_mut() {
        Some(root) => {
            commit_to_existing_state(shard_tries, shard_uid, root, updates)?;
            root.state_root
        }
        None => {
            let state_root = commit_to_new_state(shard_tries, shard_uid, updates)?;
            // TODO: load memtrie
            *root = Some(InProgressRoot { state_root, update_height: 1 });
            state_root
        }
    };

    Ok(new_root)
}

pub(crate) fn write_bandwidth_scheduler_state(
    shard_tries: &ShardTries,
    source_shard_layout: &ShardLayout,
    target_shard_layout: &ShardLayout,
    state_roots: &HashMap<ShardUId, StateRoot>,
    update_state: &[ShardUpdateState],
) -> anyhow::Result<()> {
    let source_shards = source_shard_layout.shard_uids().collect::<HashSet<_>>();
    let target_shards = target_shard_layout.shard_uids().collect::<HashSet<_>>();

    if source_shards == target_shards {
        return Ok(());
    }
    let (shard_uid, state_root) = state_roots.iter().next().unwrap();
    let trie = shard_tries.get_trie_for_shard(*shard_uid, *state_root);
    let Some(BandwidthSchedulerState::V1(state)) = near_store::get_bandwidth_scheduler_state(&trie)
        .with_context(|| format!("failed getting bandwidth scheduler state for {}", shard_uid))?
    else {
        return Ok(());
    };

    let mut link_allowances = Vec::new();
    // TODO: maybe do something other than this
    let allowance = state.link_allowances[0].allowance;
    for sender in target_shard_layout.shard_ids() {
        for receiver in target_shard_layout.shard_ids() {
            link_allowances.push(LinkAllowance { sender, receiver, allowance })
        }
    }
    let new_state = BandwidthSchedulerState::V1(BandwidthSchedulerStateV1 {
        link_allowances,
        sanity_check_hash: state.sanity_check_hash,
    });
    let mut mutator = StorageMutator::new(
        shard_tries.clone(),
        update_state.to_vec(),
        target_shard_layout.clone(),
    )?;
    for shard_idx in target_shard_layout.shard_indexes() {
        mutator.set_bandwidth_scheduler_state(shard_idx, new_state.clone())?;
    }
    mutator.commit()?;
    Ok(())
}

// After we rewrite everything in the trie to the target shards, write flat storage statuses for new shards
// TODO: remove all state that belongs to source shards not in the target shard layout
pub(crate) fn finalize_state(
    shard_tries: &ShardTries,
    source_shard_layout: &ShardLayout,
    target_shard_layout: &ShardLayout,
    flat_head: BlockInfo,
) -> anyhow::Result<()> {
    let source_shards = source_shard_layout.shard_uids().collect::<HashSet<_>>();

    for shard_uid in target_shard_layout.shard_uids() {
        if source_shards.contains(&shard_uid) {
            continue;
        }

        let mut trie_update = shard_tries.store_update();
        let store_update = trie_update.store_update();
        store_update
            .set_ser(
                DBCol::FlatStorageStatus,
                &shard_uid.to_bytes(),
                &FlatStorageStatus::Ready(FlatStorageReadyStatus { flat_head }),
            )
            .unwrap();
        trie_update
            .commit()
            .with_context(|| format!("failed writing flat storage status for {}", shard_uid))?;
        tracing::info!(?shard_uid, "wrote flat storage status for new shard");
    }
    Ok(())
}
