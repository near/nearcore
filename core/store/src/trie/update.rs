pub use self::iterator::TrieUpdateIterator;
use super::{OptimizedValueRef, Trie, TrieWithReadLock};
use crate::trie::{KeyLookupMode, TrieChanges};
use crate::{StorageError, TrieStorage};
use near_primitives::hash::CryptoHash;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{
    AccountId, RawStateChange, RawStateChanges, RawStateChangesWithTrieKey, StateChangeCause,
    StateRoot, TrieCacheMode,
};
use near_vm_runner::ContractCode;
use std::collections::BTreeMap;
use std::rc::Rc;

mod iterator;

/// Reads contract code from the trie by its hash.
/// Currently, uses `TrieStorage`. Consider implementing separate logic for
/// requesting and compiling contracts, as any contract code read and
/// compilation is a major bottleneck during chunk execution.
struct ContractStorage {
    storage: Rc<dyn TrieStorage>,
}

impl ContractStorage {
    fn new(storage: Rc<dyn TrieStorage>) -> Self {
        Self { storage }
    }

    pub fn get(&self, code_hash: CryptoHash) -> Option<ContractCode> {
        match self.storage.retrieve_raw_bytes(&code_hash) {
            Ok(raw_code) => Some(ContractCode::new(raw_code.to_vec(), Some(code_hash))),
            Err(_) => None,
        }
    }
}

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

pub enum TrieUpdateValuePtr<'a> {
    Ref(&'a Trie, OptimizedValueRef),
    MemoryRef(&'a [u8]),
}

impl<'a> TrieUpdateValuePtr<'a> {
    pub fn len(&self) -> u32 {
        match self {
            TrieUpdateValuePtr::MemoryRef(value) => value.len() as u32,
            TrieUpdateValuePtr::Ref(_, value_ref) => value_ref.len() as u32,
        }
    }

    pub fn deref_value(&self) -> Result<Vec<u8>, StorageError> {
        match self {
            TrieUpdateValuePtr::MemoryRef(value) => Ok(value.to_vec()),
            TrieUpdateValuePtr::Ref(trie, value_ref) => Ok(trie.deref_optimized(value_ref)?),
        }
    }
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

    pub fn get_ref(
        &self,
        key: &TrieKey,
        mode: KeyLookupMode,
    ) -> Result<Option<TrieUpdateValuePtr<'_>>, StorageError> {
        let key = key.to_vec();
        if let Some(key_value) = self.prospective.get(&key) {
            return Ok(key_value.value.as_deref().map(TrieUpdateValuePtr::MemoryRef));
        } else if let Some(changes_with_trie_key) = self.committed.get(&key) {
            if let Some(RawStateChange { data, .. }) = changes_with_trie_key.changes.last() {
                return Ok(data.as_deref().map(TrieUpdateValuePtr::MemoryRef));
            }
        }

        let result = self
            .trie
            .get_optimized_ref(&key, mode)?
            .map(|optimized_value_ref| TrieUpdateValuePtr::Ref(&self.trie, optimized_value_ref));

        Ok(result)
    }

    pub fn contains_key(&self, key: &TrieKey) -> Result<bool, StorageError> {
        let key = key.to_vec();
        if self.prospective.contains_key(&key) {
            return Ok(true);
        } else if let Some(changes_with_trie_key) = self.committed.get(&key) {
            if let Some(RawStateChange { data, .. }) = changes_with_trie_key.changes.last() {
                return Ok(data.is_some());
            }
        }
        self.trie.contains_key(&key)
    }

    pub fn get(&self, key: &TrieKey) -> Result<Option<Vec<u8>>, StorageError> {
        let key = key.to_vec();
        if let Some(key_value) = self.prospective.get(&key) {
            return Ok(key_value.value.as_ref().map(<Vec<u8>>::clone));
        } else if let Some(changes_with_trie_key) = self.committed.get(&key) {
            if let Some(RawStateChange { data, .. }) = changes_with_trie_key.changes.last() {
                return Ok(data.as_ref().map(<Vec<u8>>::clone));
            }
        }
        self.trie.get(&key)
    }

    /// Gets code from trie updates or directly from contract storage,
    /// bypassing the trie.
    pub fn get_code(
        &self,
        account_id: AccountId,
        code_hash: CryptoHash,
    ) -> Option<near_vm_runner::ContractCode> {
        let key = TrieKey::ContractCode { account_id }.to_vec();
        let raw_code_update = if let Some(key_value) = self.prospective.get(&key) {
            Some(key_value.value.as_ref().map(<Vec<u8>>::clone))
        } else if let Some(changes_with_trie_key) = self.committed.get(&key) {
            if let Some(RawStateChange { data, .. }) = changes_with_trie_key.changes.last() {
                Some(data.as_ref().map(<Vec<u8>>::clone))
            } else {
                None
            }
        } else {
            None
        };
        match raw_code_update {
            Some(raw_code) => {
                raw_code.map(|code| near_vm_runner::ContractCode::new(code, Some(code_hash)))
            }
            None => self.contract_storage.get(code_hash),
        }
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
                recorder.borrow_mut().record_removal();
            }
        }

        self.prospective.insert(trie_key.to_vec(), TrieKeyValueUpdate { trie_key, value: None });
    }

    pub fn commit(&mut self, event: StateChangeCause) {
        let prospective = std::mem::take(&mut self.prospective);
        for (raw_key, TrieKeyValueUpdate { trie_key, value }) in prospective.into_iter() {
            self.committed
                .entry(raw_key)
                .or_insert_with(|| RawStateChangesWithTrieKey { trie_key, changes: Vec::new() })
                .changes
                .push(RawStateChange { cause: event.clone(), data: value });
        }
    }

    pub fn rollback(&mut self) {
        self.prospective.clear();
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
        fields(
            committed.len = self.committed.len(),
            mem_reads = tracing::field::Empty,
            db_reads = tracing::field::Empty
        )
    )]
    pub fn finalize(
        self,
    ) -> Result<(Trie, TrieChanges, Vec<RawStateChangesWithTrieKey>), StorageError> {
        assert!(self.prospective.is_empty(), "Finalize cannot be called with uncommitted changes.");
        let span = tracing::Span::current();
        let TrieUpdate { trie, committed, .. } = self;
        let start_counts = trie.accounting_cache.borrow().get_trie_nodes_count();
        let mut state_changes = Vec::with_capacity(committed.len());
        let trie_changes =
            trie.update(committed.into_iter().map(|(k, changes_with_trie_key)| {
                let data = changes_with_trie_key
                    .changes
                    .last()
                    .expect("Committed entry should have at least one change")
                    .data
                    .clone();
                state_changes.push(changes_with_trie_key);
                (k, data)
            }))?;
        let end_counts = trie.accounting_cache.borrow().get_trie_nodes_count();
        if let Some(iops_delta) = end_counts.checked_sub(&start_counts) {
            span.record("mem_reads", iops_delta.mem_reads);
            span.record("db_reads", iops_delta.db_reads);
        }
        Ok((trie, trie_changes, state_changes))
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

    pub fn set_trie_cache_mode(&self, state: TrieCacheMode) {
        self.trie.accounting_cache.borrow_mut().set_enabled(state == TrieCacheMode::CachingChunk);
    }
}

impl crate::TrieAccess for TrieUpdate {
    fn get(&self, key: &TrieKey) -> Result<Option<Vec<u8>>, StorageError> {
        TrieUpdate::get(self, key)
    }

    fn contains_key(&self, key: &TrieKey) -> Result<bool, StorageError> {
        TrieUpdate::contains_key(&self, key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::TestTriesBuilder;
    use crate::ShardUId;
    use near_primitives::hash::CryptoHash;
    const SHARD_VERSION: u32 = 1;
    const COMPLEX_SHARD_UID: ShardUId = ShardUId { version: SHARD_VERSION, shard_id: 0 };

    fn test_key(key: Vec<u8>) -> TrieKey {
        TrieKey::ContractData { account_id: "alice".parse().unwrap(), key }
    }

    #[test]
    fn trie() {
        let tries = TestTriesBuilder::new().with_shard_layout(SHARD_VERSION, 2).build();
        let root = Trie::EMPTY_ROOT;
        let mut trie_update = tries.new_trie_update(COMPLEX_SHARD_UID, root);
        trie_update.set(test_key(b"dog".to_vec()), b"puppy".to_vec());
        trie_update.set(test_key(b"dog2".to_vec()), b"puppy".to_vec());
        trie_update.set(test_key(b"xxx".to_vec()), b"puppy".to_vec());
        trie_update
            .commit(StateChangeCause::TransactionProcessing { tx_hash: CryptoHash::default() });
        let trie_changes = trie_update.finalize().unwrap().1;
        let mut store_update = tries.store_update();
        let new_root = tries.apply_all(&trie_changes, COMPLEX_SHARD_UID, &mut store_update);
        store_update.commit().unwrap();
        let trie_update2 = tries.new_trie_update(COMPLEX_SHARD_UID, new_root);
        assert_eq!(trie_update2.get(&test_key(b"dog".to_vec())), Ok(Some(b"puppy".to_vec())));
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
        let tries = TestTriesBuilder::new().with_shard_layout(SHARD_VERSION, 2).build();

        // Delete non-existing element.
        let mut trie_update = tries.new_trie_update(COMPLEX_SHARD_UID, Trie::EMPTY_ROOT);
        trie_update.remove(test_key(b"dog".to_vec()));
        trie_update.commit(StateChangeCause::TransactionProcessing { tx_hash: Trie::EMPTY_ROOT });
        let trie_changes = trie_update.finalize().unwrap().1;
        let mut store_update = tries.store_update();
        let new_root = tries.apply_all(&trie_changes, COMPLEX_SHARD_UID, &mut store_update);
        store_update.commit().unwrap();
        assert_eq!(new_root, Trie::EMPTY_ROOT);

        // Add and right away delete element.
        let mut trie_update = tries.new_trie_update(COMPLEX_SHARD_UID, Trie::EMPTY_ROOT);
        trie_update.set(test_key(b"dog".to_vec()), b"puppy".to_vec());
        trie_update.remove(test_key(b"dog".to_vec()));
        trie_update
            .commit(StateChangeCause::TransactionProcessing { tx_hash: CryptoHash::default() });
        let trie_changes = trie_update.finalize().unwrap().1;
        let mut store_update = tries.store_update();
        let new_root = tries.apply_all(&trie_changes, COMPLEX_SHARD_UID, &mut store_update);
        store_update.commit().unwrap();
        assert_eq!(new_root, Trie::EMPTY_ROOT);

        // Add, apply changes and then delete element.
        let mut trie_update = tries.new_trie_update(COMPLEX_SHARD_UID, Trie::EMPTY_ROOT);
        trie_update.set(test_key(b"dog".to_vec()), b"puppy".to_vec());
        trie_update
            .commit(StateChangeCause::TransactionProcessing { tx_hash: CryptoHash::default() });
        let trie_changes = trie_update.finalize().unwrap().1;
        let mut store_update = tries.store_update();
        let new_root = tries.apply_all(&trie_changes, COMPLEX_SHARD_UID, &mut store_update);
        store_update.commit().unwrap();
        assert_ne!(new_root, Trie::EMPTY_ROOT);
        let mut trie_update = tries.new_trie_update(COMPLEX_SHARD_UID, new_root);
        trie_update.remove(test_key(b"dog".to_vec()));
        trie_update
            .commit(StateChangeCause::TransactionProcessing { tx_hash: CryptoHash::default() });
        let trie_changes = trie_update.finalize().unwrap().1;
        let mut store_update = tries.store_update();
        let new_root = tries.apply_all(&trie_changes, COMPLEX_SHARD_UID, &mut store_update);
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
        let trie_changes = trie_update.finalize().unwrap().1;
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
