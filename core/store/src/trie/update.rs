pub use self::iterator::TrieUpdateIterator;
use super::{AccessOptions, OptimizedValueRef, Trie, TrieWithReadLock};
use crate::StorageError;
use crate::contract::ContractStorage;
use crate::trie::TrieAccess;
use crate::trie::{KeyLookupMode, TrieChanges};
use near_primitives::account::AccountContract;
use near_primitives::action::GlobalContractIdentifier;
use near_primitives::apply::ApplyChunkReason;
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::stateless_validation::contract_distribution::ContractUpdates;
use near_primitives::trie_key::{GlobalContractCodeIdentifier, TrieKey};
use near_primitives::types::{
    AccountId, RawStateChange, RawStateChanges, RawStateChangesWithTrieKey, StateChangeCause,
    StateRoot,
};
use near_vm_runner::ContractCode;
use std::collections::BTreeMap;

mod iterator;

/// Key-value update. Contains a TrieKey and a value.
pub struct TrieKeyValueUpdate {
    pub trie_key: TrieKey,
    pub value: Option<Vec<u8>>,
}

/// key that was updated -> the update.
pub type TrieUpdates = BTreeMap<Vec<u8>, TrieKeyValueUpdate>;

/// Provides a way to access Storage and record changes with future commit.
/// TODO (#7327): rename to StateUpdate
pub struct TrieUpdate {
    pub trie: Trie,
    contract_storage: ContractStorage,
    committed: RawStateChanges,
    prospective: TrieUpdates,
}

static_assertions::assert_impl_all!(TrieUpdate: Send, Sync);

pub enum TrieUpdateValuePtr<'a> {
    Ref(&'a Trie, OptimizedValueRef),
    MemoryRef(&'a [u8]),
}

impl<'a> TrieUpdateValuePtr<'a> {
    /// Returns the length (in num bytes) of the value pointed by this pointer.
    pub fn len(&self) -> u32 {
        match self {
            TrieUpdateValuePtr::MemoryRef(value) => value.len() as u32,
            TrieUpdateValuePtr::Ref(_, value_ref) => value_ref.len() as u32,
        }
    }

    /// Returns the hash of the value pointed by this pointer.
    pub fn value_hash(&self) -> CryptoHash {
        match self {
            TrieUpdateValuePtr::MemoryRef(value) => hash(*value),
            TrieUpdateValuePtr::Ref(_, value_ref) => value_ref.value_hash(),
        }
    }

    pub fn deref_value(&self, opts: AccessOptions) -> Result<Vec<u8>, StorageError> {
        match self {
            TrieUpdateValuePtr::MemoryRef(value) => Ok(value.to_vec()),
            TrieUpdateValuePtr::Ref(trie, value_ref) => Ok(trie.deref_optimized(opts, value_ref)?),
        }
    }
}

/// Contains the result of trie updates generated during the finalization of [`TrieUpdate`].
pub struct TrieUpdateResult {
    pub trie: Trie,
    pub trie_changes: TrieChanges,
    pub state_changes: Vec<RawStateChangesWithTrieKey>,
    /// Contracts accessed and deployed while applying the chunk.
    pub contract_updates: ContractUpdates,
}

impl TrieUpdate {
    pub fn new(trie: Trie) -> Self {
        let trie_storage = trie.storage.clone();
        Self {
            trie,
            contract_storage: ContractStorage::new(trie_storage),
            committed: Default::default(),
            prospective: Default::default(),
        }
    }

    pub fn trie(&self) -> &Trie {
        &self.trie
    }

    /// Gets a clone of the `ContractStorage`` (which internally points to the same storage).
    pub fn contract_storage(&self) -> ContractStorage {
        self.contract_storage.clone()
    }

    pub fn get_ref(
        &self,
        key: &TrieKey,
        mode: KeyLookupMode,
        opts: AccessOptions,
    ) -> Result<Option<TrieUpdateValuePtr<'_>>, StorageError> {
        let key = key.to_vec();
        if let Some(value_ref) = self.get_ref_from_updates(&key) {
            return Ok(value_ref);
        }

        let result = self
            .trie
            .get_optimized_ref(&key, mode, opts)?
            .map(|optimized_value_ref| TrieUpdateValuePtr::Ref(&self.trie, optimized_value_ref));
        Ok(result)
    }

    fn get_ref_from_updates(&self, key: &[u8]) -> Option<Option<TrieUpdateValuePtr<'_>>> {
        if let Some(key_value) = self.prospective.get(key) {
            return Some(key_value.value.as_deref().map(TrieUpdateValuePtr::MemoryRef));
        } else if let Some(changes_with_trie_key) = self.committed.get(key) {
            if let Some(RawStateChange { data, .. }) = changes_with_trie_key.changes.last() {
                return Some(data.as_deref().map(TrieUpdateValuePtr::MemoryRef));
            }
        }
        None
    }

    pub fn contains_key(&self, key: &TrieKey, opts: AccessOptions) -> Result<bool, StorageError> {
        let key = key.to_vec();
        if self.prospective.contains_key(&key) {
            return Ok(true);
        } else if let Some(changes_with_trie_key) = self.committed.get(&key) {
            if let Some(RawStateChange { data, .. }) = changes_with_trie_key.changes.last() {
                return Ok(data.is_some());
            }
        }
        self.trie.contains_key(&key, opts)
    }

    pub fn set(&mut self, trie_key: TrieKey, value: Vec<u8>) {
        // NOTE: Converting `TrieKey` to a `Vec<u8>` is useful here for 2 reasons:
        // - Using `Vec<u8>` for sorting `BTreeMap` in the same order as a `Trie` and
        //   avoid recomputing `Vec<u8>` every time. It helps for merging iterators.
        // - Using `TrieKey` later for `RawStateChangesWithTrieKey` for State changes RPCs.
        self.prospective
            .insert(trie_key.to_vec(), TrieKeyValueUpdate { trie_key, value: Some(value) });
    }

    pub fn remove(&mut self, trie_key: TrieKey) {
        // We count removals performed by the contracts and charge extra for them.
        // A malicious contract could generate a lot of storage proof by a removal,
        // charging extra provides a safe upper bound. (https://github.com/near/nearcore/issues/10890)
        // This only applies to removals performed by the contracts. Removals performed
        // by the runtime are assumed to be non-malicious and we don't charge extra for them.
        if let Some(recorder) = &self.trie.recorder {
            if matches!(trie_key, TrieKey::ContractData { .. }) {
                recorder.write().record_key_removal();
            }
        }

        self.prospective.insert(trie_key.to_vec(), TrieKeyValueUpdate { trie_key, value: None });
    }

    // Deprecated, will be removed when ExcludeExistingCodeFromWitnessForCodeLen is stabilized.
    // `get_account_contract_code` should be used instead.
    pub fn get_code(
        &self,
        account_id: AccountId,
        code_hash: CryptoHash,
    ) -> Result<Option<ContractCode>, StorageError> {
        let key = TrieKey::ContractCode { account_id };
        self.get(&key, AccessOptions::DEFAULT)
            .map(|opt| opt.map(|code| ContractCode::new(code, Some(code_hash))))
    }

    pub fn get_account_contract_code(
        &self,
        account_id: &AccountId,
        account_contract: &AccountContract,
    ) -> Result<Option<ContractCode>, StorageError> {
        let Some(key) = Self::account_contract_code_trie_key(account_id, account_contract) else {
            return Ok(None);
        };
        let code_hash = match account_contract {
            AccountContract::None | AccountContract::GlobalByAccount(_) => None,
            AccountContract::Local(hash) | AccountContract::Global(hash) => Some(*hash),
        };
        self.get(&key, AccessOptions::DEFAULT)
            .map(|opt| opt.map(|code| ContractCode::new(code, code_hash)))
    }

    /// Returns the size (in num bytes) of the contract code for the given account.
    ///
    /// This is different from `get_code` in that it does not read the code from storage.
    /// However, the trie nodes traversed to get the code size are recorded.
    pub fn get_code_len(
        &self,
        account_id: AccountId,
        code_hash: CryptoHash,
    ) -> Result<Option<usize>, StorageError> {
        let key = TrieKey::ContractCode { account_id };
        let value_ptr =
            self.get_ref(&key, KeyLookupMode::MemOrFlatOrTrie, AccessOptions::DEFAULT)?;
        if let Some(value_ptr) = value_ptr {
            debug_assert_eq!(
                code_hash,
                value_ptr.value_hash(),
                "Code-hash in trie does not match code-hash in account"
            );
            Ok(Some(value_ptr.len() as usize))
        } else {
            Ok(None)
        }
    }

    pub fn set_code(&mut self, account_id: AccountId, code: &ContractCode) {
        let key = TrieKey::ContractCode { account_id };
        self.set(key, code.code().to_vec());
    }

    pub fn commit(&mut self, event: StateChangeCause) {
        let prospective = std::mem::take(&mut self.prospective);
        for (raw_key, TrieKeyValueUpdate { trie_key, value }) in prospective {
            self.committed
                .entry(raw_key)
                .or_insert_with(|| RawStateChangesWithTrieKey { trie_key, changes: Vec::new() })
                .changes
                .push(RawStateChange { cause: event.clone(), data: value });
        }
        self.contract_storage.commit_deploys();
    }

    pub fn rollback(&mut self) {
        self.prospective.clear();
        self.contract_storage.rollback_deploys();
    }

    /// Prepare the accumulated state changes to be applied to the underlying storage.
    ///
    /// This Function returns the [`Trie`] with which the [`TrieUpdate`] has been initially
    /// constructed. It can be reused to construct another `TrieUpdate` or to operate with `Trie`
    /// in any other way as desired.
    #[tracing::instrument(
        level = "debug",
        target = "store::trie",
        "TrieUpdate::finalize",
        skip_all,
        fields(committed.len = self.committed.len())
    )]
    pub fn finalize(self) -> Result<TrieUpdateResult, StorageError> {
        assert!(self.prospective.is_empty(), "Finalize cannot be called with uncommitted changes.");
        let TrieUpdate { trie, committed, contract_storage, .. } = self;
        let mut state_changes = Vec::with_capacity(committed.len());
        let trie_changes = trie.update(
            committed.into_iter().map(|(k, changes_with_trie_key)| {
                let data = changes_with_trie_key
                    .changes
                    .last()
                    .expect("Committed entry should have at least one change")
                    .data
                    .clone();
                state_changes.push(changes_with_trie_key);
                (k, data)
            }),
            AccessOptions::DEFAULT,
        )?;
        let contract_updates = contract_storage.finalize();
        Ok(TrieUpdateResult { trie, trie_changes, state_changes, contract_updates })
    }

    /// Returns Error if the underlying storage fails
    pub fn iter(&self, key_prefix: &[u8]) -> Result<TrieUpdateIterator<'_>, StorageError> {
        TrieUpdateIterator::new(self, key_prefix, None)
    }

    pub fn locked_iter<'a>(
        &'a self,
        key_prefix: &[u8],
        lock: &'a TrieWithReadLock<'_>,
    ) -> Result<TrieUpdateIterator<'a>, StorageError> {
        TrieUpdateIterator::new(self, key_prefix, Some(lock))
    }

    pub fn get_root(&self) -> &StateRoot {
        self.trie.get_root()
    }

    fn get_from_updates(
        &self,
        key: &TrieKey,
        fallback: impl FnOnce(&[u8]) -> Result<Option<Vec<u8>>, StorageError>,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let key = key.to_vec();
        if let Some(key_value) = self.prospective.get(&key) {
            return Ok(key_value.value.as_ref().map(<Vec<u8>>::clone));
        } else if let Some(changes_with_trie_key) = self.committed.get(&key) {
            if let Some(RawStateChange { data, .. }) = changes_with_trie_key.changes.last() {
                return Ok(data.as_ref().map(<Vec<u8>>::clone));
            }
        }
        fallback(&key)
    }

    /// Records deployment of a contract due to a deploy-contract action.
    pub fn record_contract_deploy(&self, code: ContractCode) {
        self.contract_storage.record_deploy(code);
    }

    /// Records an access to the contract code due to a function call.
    ///
    /// The contract code is either included in the state witness or distributed
    /// separately from the witness (see `ExcludeContractCodeFromStateWitness` feature).
    /// In the former case, we record a Trie read from the `TrieKey::ContractCode` for each contract.
    /// In the latter case, the Trie read does not happen and the code-size does not contribute to
    /// the storage-proof limit. Instead we just record that the code with the given hash was called,
    /// so that we can identify which contract-code to distribute to the validators.
    pub fn record_contract_call(
        &self,
        account_id: AccountId,
        code_hash: CryptoHash,
        account_contract: &AccountContract,
        apply_reason: ApplyChunkReason,
    ) -> Result<(), StorageError> {
        // The recording of contracts when they are excluded from the witness are only for distributing them to the validators,
        // and not needed for validating the chunks, thus we skip the recording if we are not applying the chunk for updating the shard.
        if apply_reason != ApplyChunkReason::UpdateTrackedShard {
            return Ok(());
        }

        // Only record the call if trie contains the contract (with the given hash) being called deployed to the given account.
        // This avoids recording contracts that do not exist or are newly-deployed to the account.
        // Note that the check below to see if the contract exists has no side effects (not charging gas or recording trie nodes)
        let Some(trie_key) = Self::account_contract_code_trie_key(&account_id, account_contract)
        else {
            return Ok(());
        };
        let contract_ref = self
            .trie
            .get_optimized_ref(
                &trie_key.to_vec(),
                KeyLookupMode::MemOrFlatOrTrie,
                AccessOptions::NO_SIDE_EFFECTS,
            )
            .or_else(|err| {
                // If the value for the trie key is not found, we treat it as if the contract does not exist.
                // In this case, we ignore the error and skip recording the contract call below.
                if matches!(err, StorageError::MissingTrieValue(_, _)) {
                    Ok(None)
                } else {
                    Err(err)
                }
            })?;
        let contract_exists =
            contract_ref.is_some_and(|value_ref| value_ref.value_hash() == code_hash);
        if contract_exists {
            self.contract_storage.record_call(code_hash);
        }
        Ok(())
    }

    pub fn get_account_contract_hash(
        &self,
        contract: &AccountContract,
    ) -> Result<CryptoHash, StorageError> {
        let hash = match contract {
            AccountContract::None => CryptoHash::default(),
            AccountContract::Local(code_hash) | AccountContract::Global(code_hash) => *code_hash,
            AccountContract::GlobalByAccount(account_id) => {
                let identifier = GlobalContractIdentifier::AccountId(account_id.clone());
                let key = TrieKey::GlobalContractCode { identifier: identifier.into() };
                let value_ref = self
                    .get_ref(&key, KeyLookupMode::MemOrFlatOrTrie, AccessOptions::DEFAULT)?
                    .ok_or_else(|| {
                        let TrieKey::GlobalContractCode { identifier } = key else {
                            unreachable!()
                        };
                        StorageError::StorageInconsistentState(format!(
                            "Global contract identifier not found {:?}",
                            identifier
                        ))
                    })?;
                value_ref.value_hash()
            }
        };
        Ok(hash)
    }

    fn account_contract_code_trie_key(
        account_id: &AccountId,
        account_contract: &AccountContract,
    ) -> Option<TrieKey> {
        let trie_key = match account_contract {
            AccountContract::None => return None,
            AccountContract::Local(_) => TrieKey::ContractCode { account_id: account_id.clone() },
            AccountContract::Global(code_hash) => TrieKey::GlobalContractCode {
                identifier: GlobalContractCodeIdentifier::CodeHash(*code_hash),
            },
            AccountContract::GlobalByAccount(account_id) => TrieKey::GlobalContractCode {
                identifier: GlobalContractCodeIdentifier::AccountId(account_id.clone()),
            },
        };
        Some(trie_key)
    }
}

impl TrieAccess for TrieUpdate {
    fn get(&self, key: &TrieKey, opts: AccessOptions) -> Result<Option<Vec<u8>>, StorageError> {
        self.get_from_updates(key, |_| TrieAccess::get(&self.trie, key, opts))
    }

    fn contains_key(&self, key: &TrieKey, opts: AccessOptions) -> Result<bool, StorageError> {
        TrieUpdate::contains_key(&self, key, opts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ShardUId;
    use crate::test_utils::TestTriesBuilder;
    use near_primitives::hash::CryptoHash;
    use near_primitives::shard_layout::ShardLayout;
    const SHARD_VERSION: u32 = 1;

    fn test_key(key: Vec<u8>) -> TrieKey {
        TrieKey::ContractData { account_id: "alice".parse().unwrap(), key }
    }

    #[test]
    fn trie() {
        let shard_layout = ShardLayout::multi_shard(2, SHARD_VERSION);
        let shard_uid = shard_layout.shard_uids().next().unwrap();

        let tries = TestTriesBuilder::new().with_shard_layout(shard_layout).build();
        let root = Trie::EMPTY_ROOT;
        let mut trie_update = tries.new_trie_update(shard_uid, root);
        trie_update.set(test_key(b"dog".to_vec()), b"puppy".to_vec());
        trie_update.set(test_key(b"dog2".to_vec()), b"puppy".to_vec());
        trie_update.set(test_key(b"xxx".to_vec()), b"puppy".to_vec());
        trie_update
            .commit(StateChangeCause::TransactionProcessing { tx_hash: CryptoHash::default() });
        let trie_changes = trie_update.finalize().unwrap().trie_changes;
        let mut store_update = tries.store_update();
        let new_root = tries.apply_all(&trie_changes, shard_uid, &mut store_update);
        store_update.commit().unwrap();
        let trie_update2 = tries.new_trie_update(shard_uid, new_root);
        assert_eq!(
            trie_update2.get(&test_key(b"dog".to_vec()), AccessOptions::DEFAULT),
            Ok(Some(b"puppy".to_vec()))
        );
        let values = trie_update2
            .iter(&test_key(b"dog".to_vec()).to_vec())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            values,
            vec![test_key(b"dog".to_vec()).to_vec(), test_key(b"dog2".to_vec()).to_vec()]
        );
    }

    #[test]
    fn trie_remove() {
        let shard_layout = ShardLayout::multi_shard(2, SHARD_VERSION);
        let shard_uid = shard_layout.shard_uids().next().unwrap();

        let tries = TestTriesBuilder::new().with_shard_layout(shard_layout).build();

        // Delete non-existing element.
        let mut trie_update = tries.new_trie_update(shard_uid, Trie::EMPTY_ROOT);
        trie_update.remove(test_key(b"dog".to_vec()));
        trie_update.commit(StateChangeCause::TransactionProcessing { tx_hash: Trie::EMPTY_ROOT });
        let trie_changes = trie_update.finalize().unwrap().trie_changes;
        let mut store_update = tries.store_update();
        let new_root = tries.apply_all(&trie_changes, shard_uid, &mut store_update);
        store_update.commit().unwrap();
        assert_eq!(new_root, Trie::EMPTY_ROOT);

        // Add and right away delete element.
        let mut trie_update = tries.new_trie_update(shard_uid, Trie::EMPTY_ROOT);
        trie_update.set(test_key(b"dog".to_vec()), b"puppy".to_vec());
        trie_update.remove(test_key(b"dog".to_vec()));
        trie_update
            .commit(StateChangeCause::TransactionProcessing { tx_hash: CryptoHash::default() });
        let trie_changes = trie_update.finalize().unwrap().trie_changes;
        let mut store_update = tries.store_update();
        let new_root = tries.apply_all(&trie_changes, shard_uid, &mut store_update);
        store_update.commit().unwrap();
        assert_eq!(new_root, Trie::EMPTY_ROOT);

        // Add, apply changes and then delete element.
        let mut trie_update = tries.new_trie_update(shard_uid, Trie::EMPTY_ROOT);
        trie_update.set(test_key(b"dog".to_vec()), b"puppy".to_vec());
        trie_update
            .commit(StateChangeCause::TransactionProcessing { tx_hash: CryptoHash::default() });
        let trie_changes = trie_update.finalize().unwrap().trie_changes;
        let mut store_update = tries.store_update();
        let new_root = tries.apply_all(&trie_changes, shard_uid, &mut store_update);
        store_update.commit().unwrap();
        assert_ne!(new_root, Trie::EMPTY_ROOT);
        let mut trie_update = tries.new_trie_update(shard_uid, new_root);
        trie_update.remove(test_key(b"dog".to_vec()));
        trie_update
            .commit(StateChangeCause::TransactionProcessing { tx_hash: CryptoHash::default() });
        let trie_changes = trie_update.finalize().unwrap().trie_changes;
        let mut store_update = tries.store_update();
        let new_root = tries.apply_all(&trie_changes, shard_uid, &mut store_update);
        store_update.commit().unwrap();
        assert_eq!(new_root, Trie::EMPTY_ROOT);
    }

    #[test]
    fn trie_iter() {
        let tries = TestTriesBuilder::new().build();
        let mut trie_update = tries.new_trie_update(ShardUId::single_shard(), Trie::EMPTY_ROOT);
        trie_update.set(test_key(b"dog".to_vec()), b"puppy".to_vec());
        trie_update.set(test_key(b"aaa".to_vec()), b"puppy".to_vec());
        trie_update
            .commit(StateChangeCause::TransactionProcessing { tx_hash: CryptoHash::default() });
        let trie_changes = trie_update.finalize().unwrap().trie_changes;
        let mut store_update = tries.store_update();
        let new_root = tries.apply_all(&trie_changes, ShardUId::single_shard(), &mut store_update);
        store_update.commit().unwrap();

        let mut trie_update = tries.new_trie_update(ShardUId::single_shard(), new_root);
        trie_update.set(test_key(b"dog2".to_vec()), b"puppy".to_vec());
        trie_update.set(test_key(b"xxx".to_vec()), b"puppy".to_vec());

        let values: Result<Vec<Vec<u8>>, _> =
            trie_update.iter(&test_key(b"dog".to_vec()).to_vec()).unwrap().collect();
        assert_eq!(
            values.unwrap(),
            vec![test_key(b"dog".to_vec()).to_vec(), test_key(b"dog2".to_vec()).to_vec()]
        );

        trie_update.rollback();

        let values: Result<Vec<Vec<u8>>, _> =
            trie_update.iter(&test_key(b"dog".to_vec()).to_vec()).unwrap().collect();
        assert_eq!(values.unwrap(), vec![test_key(b"dog".to_vec()).to_vec()]);

        let mut trie_update = tries.new_trie_update(ShardUId::single_shard(), new_root);
        trie_update.remove(test_key(b"dog".to_vec()));

        let values: Result<Vec<Vec<u8>>, _> =
            trie_update.iter(&test_key(b"dog".to_vec()).to_vec()).unwrap().collect();
        assert_eq!(values.unwrap().len(), 0);

        let mut trie_update = tries.new_trie_update(ShardUId::single_shard(), new_root);
        trie_update.set(test_key(b"dog2".to_vec()), b"puppy".to_vec());
        trie_update
            .commit(StateChangeCause::TransactionProcessing { tx_hash: CryptoHash::default() });
        trie_update.remove(test_key(b"dog2".to_vec()));

        let values: Result<Vec<Vec<u8>>, _> =
            trie_update.iter(&test_key(b"dog".to_vec()).to_vec()).unwrap().collect();
        assert_eq!(values.unwrap(), vec![test_key(b"dog".to_vec()).to_vec()]);

        let mut trie_update = tries.new_trie_update(ShardUId::single_shard(), new_root);
        trie_update.set(test_key(b"dog2".to_vec()), b"puppy".to_vec());
        trie_update
            .commit(StateChangeCause::TransactionProcessing { tx_hash: CryptoHash::default() });
        trie_update.set(test_key(b"dog3".to_vec()), b"puppy".to_vec());

        let values: Result<Vec<Vec<u8>>, _> =
            trie_update.iter(&test_key(b"dog".to_vec()).to_vec()).unwrap().collect();
        assert_eq!(
            values.unwrap(),
            vec![
                test_key(b"dog".to_vec()).to_vec(),
                test_key(b"dog2".to_vec()).to_vec(),
                test_key(b"dog3".to_vec()).to_vec()
            ]
        );
    }
}
