use std::collections::BTreeMap;
use std::iter::Peekable;
use std::ops::Bound;

use near_primitives::hash::CryptoHash;
use near_primitives::types::{
    RawStateChange, RawStateChanges, RawStateChangesWithTrieKey, StateChangeCause, StateRoot,
    TrieCacheMode,
};

use crate::trie::TrieChanges;
use crate::StorageError;

use super::{Trie, TrieIterator};
use near_primitives::state::ValueRef;
use near_primitives::trie_key::TrieKey;
use std::rc::Rc;

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
    pub trie: Rc<Trie>,
    committed: RawStateChanges,
    prospective: TrieUpdates,
}

pub enum TrieUpdateValuePtr<'a> {
    HashAndSize(&'a Trie, u32, CryptoHash),
    MemoryRef(&'a [u8]),
}

impl<'a> TrieUpdateValuePtr<'a> {
    pub fn len(&self) -> u32 {
        match self {
            TrieUpdateValuePtr::MemoryRef(value) => value.len() as u32,
            TrieUpdateValuePtr::HashAndSize(_, length, _) => *length,
        }
    }

    pub fn deref_value(&self) -> Result<Vec<u8>, StorageError> {
        match self {
            TrieUpdateValuePtr::MemoryRef(value) => Ok(value.to_vec()),
            TrieUpdateValuePtr::HashAndSize(trie, _, hash) => {
                trie.storage.retrieve_raw_bytes(hash).map(|bytes| bytes.to_vec())
            }
        }
    }
}

impl TrieUpdate {
    pub fn new(trie: Rc<Trie>) -> Self {
        TrieUpdate { trie, committed: Default::default(), prospective: Default::default() }
    }

    pub fn trie(&self) -> &Rc<Trie> {
        &self.trie
    }

    pub fn get_ref(&self, key: &TrieKey) -> Result<Option<TrieUpdateValuePtr<'_>>, StorageError> {
        let key = key.to_vec();
        if let Some(key_value) = self.prospective.get(&key) {
            return Ok(key_value.value.as_deref().map(TrieUpdateValuePtr::MemoryRef));
        } else if let Some(changes_with_trie_key) = self.committed.get(&key) {
            if let Some(RawStateChange { data, .. }) = changes_with_trie_key.changes.last() {
                return Ok(data.as_deref().map(TrieUpdateValuePtr::MemoryRef));
            }
        }

        self.trie.get_ref(&key).map(|option| {
            option.map(|ValueRef { length, hash }| {
                TrieUpdateValuePtr::HashAndSize(&self.trie, length, hash)
            })
        })
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

    pub fn set(&mut self, trie_key: TrieKey, value: Vec<u8>) {
        // NOTE: Converting `TrieKey` to a `Vec<u8>` is useful here for 2 reasons:
        // - Using `Vec<u8>` for sorting `BTreeMap` in the same order as a `Trie` and
        //   avoid recomputing `Vec<u8>` every time. It helps for merging iterators.
        // - Using `TrieKey` later for `RawStateChangesWithTrieKey` for State changes RPCs.
        self.prospective
            .insert(trie_key.to_vec(), TrieKeyValueUpdate { trie_key, value: Some(value) });
    }
    pub fn remove(&mut self, trie_key: TrieKey) {
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

    pub fn finalize(self) -> Result<(TrieChanges, Vec<RawStateChangesWithTrieKey>), StorageError> {
        assert!(self.prospective.is_empty(), "Finalize cannot be called with uncommitted changes.");
        let TrieUpdate { trie, committed, .. } = self;
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
        Ok((trie_changes, state_changes))
    }

    /// Returns Error if the underlying storage fails
    pub fn iter(&self, key_prefix: &[u8]) -> Result<TrieUpdateIterator<'_>, StorageError> {
        TrieUpdateIterator::new(self, key_prefix)
    }

    pub fn get_root(&self) -> &StateRoot {
        self.trie.get_root()
    }

    pub fn set_trie_cache_mode(&self, state: TrieCacheMode) {
        if let Some(storage) = self.trie.storage.as_caching_storage() {
            storage.set_mode(state);
        }
    }
}

impl crate::TrieAccess for TrieUpdate {
    fn get(&self, key: &TrieKey) -> Result<Option<Vec<u8>>, StorageError> {
        TrieUpdate::get(self, key)
    }
}

struct MergeIter<'a> {
    left: Peekable<Box<dyn Iterator<Item = (&'a [u8], Option<&'a [u8]>)> + 'a>>,
    right: Peekable<Box<dyn Iterator<Item = (&'a [u8], Option<&'a [u8]>)> + 'a>>,
}

impl<'a> Iterator for MergeIter<'a> {
    type Item = (&'a [u8], Option<&'a [u8]>);

    fn next(&mut self) -> Option<Self::Item> {
        let res = match (self.left.peek(), self.right.peek()) {
            (Some(&(ref left_key, _)), Some(&(ref right_key, _))) => left_key.cmp(right_key),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => return None,
        };

        // Check which elements comes first and only advance the corresponding iterator.
        // If two keys are equal, take the value from `right`.
        match res {
            std::cmp::Ordering::Less => self.left.next(),
            std::cmp::Ordering::Greater => self.right.next(),
            std::cmp::Ordering::Equal => {
                self.left.next();
                self.right.next()
            }
        }
    }
}

pub struct TrieUpdateIterator<'a>(Option<(Peekable<TrieIterator<'a>>, Peekable<MergeIter<'a>>)>);

/// Returns an end bound for a range which corresponds to all values with
/// a given prefix.
///
/// In other words, the smallest value larger than the `prefix` which does not
/// start with the `prefix`.  If no such value exists, returns `None`.
fn make_prefix_range_end_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let ffs = prefix.iter().rev().take_while(|&&byte| byte == u8::MAX).count();
    let next = &prefix[..(prefix.len() - ffs)];
    if next.is_empty() {
        // Prefix consisted of \xff bytes.  There is no key that follows it.
        None
    } else {
        let mut next = next.to_vec();
        *next.last_mut().unwrap() += 1;
        Some(next)
    }
}

impl<'a> TrieUpdateIterator<'a> {
    #![allow(clippy::new_ret_no_self)]
    pub fn new(state_update: &'a TrieUpdate, prefix: &[u8]) -> Result<Self, StorageError> {
        let mut trie_iter = state_update.trie.iter()?;
        trie_iter.seek_prefix(prefix)?;

        let end_bound = make_prefix_range_end_bound(prefix);
        let end_bound = if let Some(end_bound) = &end_bound {
            Bound::Excluded(end_bound.as_slice())
        } else {
            Bound::Unbounded
        };
        let range = (Bound::Included(prefix), end_bound);

        let committed_iter = state_update.committed.range::<[u8], _>(range).map(
            |(raw_key, changes_with_trie_key)| {
                let key = raw_key.as_slice();
                let value = changes_with_trie_key
                    .changes
                    .last()
                    .as_ref()
                    .expect("Committed entry should have at least one change.")
                    .data
                    .as_deref();
                (key, value)
            },
        );
        let prospective_iter = state_update
            .prospective
            .range::<[u8], _>(range)
            .map(|(raw_key, key_value)| (raw_key.as_slice(), key_value.value.as_deref()));
        let overlay_iter = MergeIter {
            left: (Box::new(committed_iter) as Box<dyn Iterator<Item = _>>).peekable(),
            right: (Box::new(prospective_iter) as Box<dyn Iterator<Item = _>>).peekable(),
        }
        .peekable();
        Ok(TrieUpdateIterator(Some((trie_iter.peekable(), overlay_iter))))
    }
}

impl<'a> Iterator for TrieUpdateIterator<'a> {
    type Item = Result<Vec<u8>, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        #[derive(Eq, PartialEq)]
        enum Ordering {
            Trie,
            Overlay,
            Both,
        }
        // Usually one iteration, unless need to skip None values in prospective / committed.
        let iterators = self.0.as_mut()?;
        loop {
            let res = match (iterators.0.peek(), iterators.1.peek()) {
                (Some(Err(_)), _) => {
                    let err = iterators.0.next().unwrap().unwrap_err();
                    self.0 = None;
                    return Some(Err(err));
                }

                (Some(Ok((left_key, _))), Some((right_key, _))) => {
                    match left_key.as_slice().cmp(right_key) {
                        std::cmp::Ordering::Less => Ordering::Trie,
                        std::cmp::Ordering::Equal => Ordering::Both,
                        std::cmp::Ordering::Greater => Ordering::Overlay,
                    }
                }
                (Some(_), None) => Ordering::Trie,
                (None, Some(_)) => Ordering::Overlay,
                (None, None) => {
                    self.0 = None;
                    return None;
                }
            };

            // Check which element comes first and advance the corresponding
            // iterator only.  If both keys are equal, check if overlay doesn’t
            // delete the value.
            let trie_item = if res != Ordering::Overlay { iterators.0.next() } else { None };
            if res == Ordering::Trie {
                if let Some(Ok((key, _))) = trie_item {
                    return Some(Ok(key));
                }
            } else if let Some((overlay_key, Some(_))) = iterators.1.next() {
                return Some(Ok(if let Some(Ok((trie_key, _))) = trie_item {
                    debug_assert_eq!(trie_key.as_slice(), overlay_key);
                    trie_key
                } else {
                    overlay_key.to_vec()
                }));
            }
        }
    }
}

impl<'a> std::iter::FusedIterator for TrieUpdateIterator<'a> {}

#[cfg(test)]
mod tests {
    use crate::test_utils::{create_tries, create_tries_complex};

    use super::*;
    use crate::ShardUId;
    const SHARD_VERSION: u32 = 1;
    const COMPLEX_SHARD_UID: ShardUId = ShardUId { version: SHARD_VERSION, shard_id: 0 };

    fn test_key(key: Vec<u8>) -> TrieKey {
        TrieKey::ContractData { account_id: "alice".parse().unwrap(), key }
    }

    #[test]
    fn trie() {
        let tries = create_tries_complex(SHARD_VERSION, 2);
        let root = Trie::EMPTY_ROOT;
        let mut trie_update = tries.new_trie_update(COMPLEX_SHARD_UID, root);
        trie_update.set(test_key(b"dog".to_vec()), b"puppy".to_vec());
        trie_update.set(test_key(b"dog2".to_vec()), b"puppy".to_vec());
        trie_update.set(test_key(b"xxx".to_vec()), b"puppy".to_vec());
        trie_update
            .commit(StateChangeCause::TransactionProcessing { tx_hash: CryptoHash::default() });
        let trie_changes = trie_update.finalize().unwrap().0;
        let (store_update, new_root) = tries.apply_all(&trie_changes, COMPLEX_SHARD_UID);
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
        let tries = create_tries_complex(SHARD_VERSION, 2);

        // Delete non-existing element.
        let mut trie_update = tries.new_trie_update(COMPLEX_SHARD_UID, Trie::EMPTY_ROOT);
        trie_update.remove(test_key(b"dog".to_vec()));
        trie_update.commit(StateChangeCause::TransactionProcessing { tx_hash: Trie::EMPTY_ROOT });
        let trie_changes = trie_update.finalize().unwrap().0;
        let (store_update, new_root) = tries.apply_all(&trie_changes, COMPLEX_SHARD_UID);
        store_update.commit().unwrap();
        assert_eq!(new_root, Trie::EMPTY_ROOT);

        // Add and right away delete element.
        let mut trie_update = tries.new_trie_update(COMPLEX_SHARD_UID, Trie::EMPTY_ROOT);
        trie_update.set(test_key(b"dog".to_vec()), b"puppy".to_vec());
        trie_update.remove(test_key(b"dog".to_vec()));
        trie_update
            .commit(StateChangeCause::TransactionProcessing { tx_hash: CryptoHash::default() });
        let trie_changes = trie_update.finalize().unwrap().0;
        let (store_update, new_root) = tries.apply_all(&trie_changes, COMPLEX_SHARD_UID);
        store_update.commit().unwrap();
        assert_eq!(new_root, Trie::EMPTY_ROOT);

        // Add, apply changes and then delete element.
        let mut trie_update = tries.new_trie_update(COMPLEX_SHARD_UID, Trie::EMPTY_ROOT);
        trie_update.set(test_key(b"dog".to_vec()), b"puppy".to_vec());
        trie_update
            .commit(StateChangeCause::TransactionProcessing { tx_hash: CryptoHash::default() });
        let trie_changes = trie_update.finalize().unwrap().0;
        let (store_update, new_root) = tries.apply_all(&trie_changes, COMPLEX_SHARD_UID);
        store_update.commit().unwrap();
        assert_ne!(new_root, Trie::EMPTY_ROOT);
        let mut trie_update = tries.new_trie_update(COMPLEX_SHARD_UID, new_root);
        trie_update.remove(test_key(b"dog".to_vec()));
        trie_update
            .commit(StateChangeCause::TransactionProcessing { tx_hash: CryptoHash::default() });
        let trie_changes = trie_update.finalize().unwrap().0;
        let (store_update, new_root) = tries.apply_all(&trie_changes, COMPLEX_SHARD_UID);
        store_update.commit().unwrap();
        assert_eq!(new_root, Trie::EMPTY_ROOT);
    }

    #[test]
    fn trie_iter() {
        let tries = create_tries();
        let mut trie_update = tries.new_trie_update(ShardUId::single_shard(), Trie::EMPTY_ROOT);
        trie_update.set(test_key(b"dog".to_vec()), b"puppy".to_vec());
        trie_update.set(test_key(b"aaa".to_vec()), b"puppy".to_vec());
        trie_update
            .commit(StateChangeCause::TransactionProcessing { tx_hash: CryptoHash::default() });
        let trie_changes = trie_update.finalize().unwrap().0;
        let (store_update, new_root) = tries.apply_all(&trie_changes, ShardUId::single_shard());
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

    #[test]
    fn test_make_prefix_range_end_bound() {
        fn test(want: Option<&[u8]>, prefix: &[u8]) {
            assert_eq!(want, make_prefix_range_end_bound(prefix).as_deref());
        }

        test(None, b"");
        test(None, b"\xff");
        test(None, b"\xff\xff\xff\xff");
        test(Some(b"b"), b"a");
        test(Some(b"b"), b"a\xff\xff\xff");
    }
}
