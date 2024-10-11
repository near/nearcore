use std::io;
use std::num::NonZero;
use std::sync::Arc;

use near_primitives::errors::{MissingTrieValueContext, StorageError};
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{get_block_shard_uid, ShardUId};
use near_primitives::types::RawStateChangesWithTrieKey;

use crate::{DBCol, KeyForStateChanges, Store, StoreUpdate, TrieChanges, STATE_SNAPSHOT_KEY};

use super::{StoreAdapter, StoreUpdateAdapter, StoreUpdateHolder};

#[derive(Clone)]
pub struct TrieStoreAdapter {
    store: Store,
}

impl StoreAdapter for TrieStoreAdapter {
    fn store(&self) -> Store {
        self.store.clone()
    }
}

impl TrieStoreAdapter {
    pub fn new(store: Store) -> Self {
        Self { store }
    }

    pub fn store_update(&self) -> TrieStoreUpdateAdapter<'static> {
        TrieStoreUpdateAdapter { store_update: StoreUpdateHolder::Owned(self.store.store_update()) }
    }

    /// Reads shard_uid mapping for given shard.
    /// If the mapping does not exist, it means that `shard_uid` maps to itself.
    pub(crate) fn read_shard_uid_mapping_from_db(
        &self,
        shard_uid: ShardUId,
    ) -> io::Result<ShardUId> {
        let mapped_shard_uid =
            self.get_ser::<ShardUId>(DBCol::ShardUIdMapping, &shard_uid.to_bytes())?;
        Ok(mapped_shard_uid.unwrap_or(shard_uid))
    }

    /// Replaces shard_uid prefix with a mapped value according to mapping strategy in Resharding V3.
    /// For this, it does extra read from `DBCol::ShardUIdMapping`.
    pub fn get(&self, shard_uid: ShardUId, hash: &CryptoHash) -> Result<Arc<[u8]>, StorageError> {
        let mapped_shard_uid = self.read_shard_uid_mapping_from_db(shard_uid)?;
        let key = get_key_from_shard_uid_and_hash(mapped_shard_uid, hash);
        let val = self
            .store
            .get(DBCol::State, key.as_ref())
            .map_err(|_| StorageError::StorageInternalError)?
            .ok_or(StorageError::MissingTrieValue(MissingTrieValueContext::TrieStorage, *hash))?;
        Ok(val.into())
    }

    pub fn get_state_snapshot_hash(&self) -> Result<CryptoHash, StorageError> {
        let val = self
            .store
            .get_ser(DBCol::BlockMisc, STATE_SNAPSHOT_KEY)
            .map_err(|_| StorageError::StorageInternalError)?
            .ok_or(StorageError::StorageInternalError)?;
        Ok(val)
    }

    #[cfg(test)]
    pub fn iter_raw_bytes(&self) -> crate::db::DBIterator {
        self.store.iter_raw_bytes(DBCol::State)
    }
}

pub struct TrieStoreUpdateAdapter<'a> {
    store_update: StoreUpdateHolder<'a>,
}

impl Into<StoreUpdate> for TrieStoreUpdateAdapter<'static> {
    fn into(self) -> StoreUpdate {
        self.store_update.into()
    }
}

impl TrieStoreUpdateAdapter<'static> {
    pub fn commit(self) -> io::Result<()> {
        let store_update: StoreUpdate = self.into();
        store_update.commit()
    }
}

impl<'a> StoreUpdateAdapter for TrieStoreUpdateAdapter<'a> {
    fn store_update(&mut self) -> &mut StoreUpdate {
        &mut self.store_update
    }
}

impl<'a> TrieStoreUpdateAdapter<'a> {
    pub fn new(store_update: &'a mut StoreUpdate) -> Self {
        Self { store_update: StoreUpdateHolder::Reference(store_update) }
    }

    pub fn decrement_refcount_by(
        &mut self,
        shard_uid: ShardUId,
        hash: &CryptoHash,
        decrement: NonZero<u32>,
    ) {
        let key = get_key_from_shard_uid_and_hash(shard_uid, hash);
        self.store_update.decrement_refcount_by(DBCol::State, key.as_ref(), decrement);
    }

    pub fn decrement_refcount(&mut self, shard_uid: ShardUId, hash: &CryptoHash) {
        let key = get_key_from_shard_uid_and_hash(shard_uid, hash);
        self.store_update.decrement_refcount(DBCol::State, key.as_ref());
    }

    pub fn increment_refcount_by(
        &mut self,
        shard_uid: ShardUId,
        hash: &CryptoHash,
        data: &[u8],
        decrement: NonZero<u32>,
    ) {
        let key = get_key_from_shard_uid_and_hash(shard_uid, hash);
        self.store_update.increment_refcount_by(DBCol::State, key.as_ref(), data, decrement);
    }

    pub fn set_state_snapshot_hash(&mut self, hash: Option<CryptoHash>) {
        let key = STATE_SNAPSHOT_KEY;
        match hash {
            Some(hash) => self.store_update.set_ser(DBCol::BlockMisc, key, &hash).unwrap(),
            None => self.store_update.delete(DBCol::BlockMisc, key),
        }
    }

    pub fn set_trie_changes(
        &mut self,
        shard_uid: ShardUId,
        block_hash: &CryptoHash,
        trie_changes: &TrieChanges,
    ) {
        let key = get_block_shard_uid(block_hash, &shard_uid);
        self.store_update.set_ser(DBCol::TrieChanges, &key, trie_changes).unwrap();
    }

    pub fn set_state_changes(
        &mut self,
        key: KeyForStateChanges,
        value: &RawStateChangesWithTrieKey,
    ) {
        self.store_update.set(
            DBCol::StateChanges,
            key.as_ref(),
            &borsh::to_vec(&value).expect("Borsh serialize cannot fail"),
        )
    }

    pub fn delete_all_state(&mut self) {
        self.store_update.delete_all(DBCol::State)
    }
}

pub fn get_key_from_shard_uid_and_hash(shard_uid: ShardUId, hash: &CryptoHash) -> [u8; 40] {
    let mut key = [0; 40];
    key[0..8].copy_from_slice(&shard_uid.to_bytes());
    key[8..].copy_from_slice(hash.as_ref());
    key
}
