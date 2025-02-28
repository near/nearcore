use std::io;
use std::num::NonZero;
use std::sync::Arc;

use borsh::BorshDeserialize;
use near_primitives::errors::{MissingTrieValueContext, StorageError};
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{ShardUId, get_block_shard_uid};
use near_primitives::types::RawStateChangesWithTrieKey;

use crate::{DBCol, KeyForStateChanges, STATE_SNAPSHOT_KEY, Store, StoreUpdate, TrieChanges};

use super::{StoreAdapter, StoreUpdateAdapter, StoreUpdateHolder};

#[derive(Clone)]
pub struct TrieStoreAdapter {
    store: Store,
}

impl StoreAdapter for TrieStoreAdapter {
    fn store_ref(&self) -> &Store {
        &self.store
    }
}

impl TrieStoreAdapter {
    pub fn new(store: Store) -> Self {
        Self { store }
    }

    pub fn store_update(&self) -> TrieStoreUpdateAdapter<'static> {
        TrieStoreUpdateAdapter { store_update: StoreUpdateHolder::Owned(self.store.store_update()) }
    }

    /// Replaces shard_uid prefix with a mapped value according to mapping strategy in Resharding V3.
    /// For this, it does extra read from `DBCol::StateShardUIdMapping`.
    ///
    /// For more details, see `get_key_from_shard_uid_and_hash()` docs.
    pub fn get(&self, shard_uid: ShardUId, hash: &CryptoHash) -> Result<Arc<[u8]>, StorageError> {
        let key = get_key_from_shard_uid_and_hash(&self.store, shard_uid, hash);
        let val = self
            .store
            .get(DBCol::State, key.as_ref())
            .map_err(|_| StorageError::StorageInternalError)?
            .ok_or(StorageError::MissingTrieValue(MissingTrieValueContext::TrieStorage, *hash))?;
        Ok(val.into())
    }

    pub fn get_ser<T: BorshDeserialize>(
        &self,
        shard_uid: ShardUId,
        hash: &CryptoHash,
    ) -> Result<T, StorageError> {
        let bytes = self.get(shard_uid, hash)?;
        T::try_from_slice(&bytes).map_err(|e| StorageError::StorageInconsistentState(e.to_string()))
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

    fn get_key_from_shard_uid_and_hash(&self, shard_uid: ShardUId, hash: &CryptoHash) -> [u8; 40] {
        get_key_from_shard_uid_and_hash(&self.store_update.store, shard_uid, hash)
    }

    pub fn decrement_refcount_by(
        &mut self,
        shard_uid: ShardUId,
        hash: &CryptoHash,
        decrement: NonZero<u32>,
    ) {
        let key = self.get_key_from_shard_uid_and_hash(shard_uid, hash);
        self.store_update.decrement_refcount_by(DBCol::State, key.as_ref(), decrement);
    }

    pub fn decrement_refcount(&mut self, shard_uid: ShardUId, hash: &CryptoHash) {
        let key = self.get_key_from_shard_uid_and_hash(shard_uid, hash);
        self.store_update.decrement_refcount(DBCol::State, key.as_ref());
    }

    pub fn increment_refcount_by(
        &mut self,
        shard_uid: ShardUId,
        hash: &CryptoHash,
        data: &[u8],
        increment: NonZero<u32>,
    ) {
        let key = self.get_key_from_shard_uid_and_hash(shard_uid, hash);
        self.store_update.increment_refcount_by(DBCol::State, key.as_ref(), data, increment);
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

    /// Set the mapping from `child_shard_uid` to `parent_shard_uid`.
    /// Used by Resharding V3 for State mapping.
    pub fn set_shard_uid_mapping(&mut self, child_shard_uid: ShardUId, parent_shard_uid: ShardUId) {
        self.store_update.set(
            DBCol::StateShardUIdMapping,
            child_shard_uid.to_bytes().as_ref(),
            &borsh::to_vec(&parent_shard_uid).expect("Borsh serialize cannot fail"),
        )
    }

    /// Remove State of any shard that uses `shard_uid_db_key_prefix` as database key prefix.
    /// That is potentially State of any descendant of the shard with the given `ShardUId`.
    /// Use with caution, as it might potentially remove the State of a descendant shard that is still in use!
    pub fn delete_shard_uid_prefixed_state(&mut self, shard_uid_db_key_prefix: ShardUId) {
        let key_from = shard_uid_db_key_prefix.to_bytes();
        let key_to = ShardUId::get_upper_bound_db_key(&key_from);
        self.store_update.delete_range(DBCol::State, &key_from, &key_to);
    }

    pub fn delete_all_state(&mut self) {
        self.store_update.delete_all(DBCol::State)
    }
}

/// Get the `ShardUId` mapping for child_shard_uid. If the mapping does not exist, map the shard to itself.
/// Used by Resharding V3 for State mapping.
///
/// It is kept out of `TrieStoreAdapter`, so that `TrieStoreUpdateAdapter` can use it without
/// cloning `store` each time, see https://github.com/near/nearcore/pull/12232#discussion_r1804810508.
pub fn get_shard_uid_mapping(store: &Store, child_shard_uid: ShardUId) -> ShardUId {
    store
        .get_ser::<ShardUId>(DBCol::StateShardUIdMapping, &child_shard_uid.to_bytes())
        .unwrap_or_else(|_| {
            panic!("get_shard_uid_mapping() failed for child_shard_uid = {}", child_shard_uid)
        })
        .unwrap_or(child_shard_uid)
}

/// Constructs db key to be used to access the State column.
/// First, it consults the `StateShardUIdMapping` column to map the `shard_uid` prefix
/// to its ancestor in the resharding tree (according to Resharding V3)
/// or map to itself if the mapping does not exist.
///
/// Please note that the mapped shard uid is read from db each time which may seem slow.
/// In practice the `StateShardUIdMapping` is very small and should always be stored in the RocksDB cache.
/// The deserialization of ShardUId is also very cheap.
fn get_key_from_shard_uid_and_hash(
    store: &Store,
    shard_uid: ShardUId,
    hash: &CryptoHash,
) -> [u8; 40] {
    let mapped_shard_uid = get_shard_uid_mapping(store, shard_uid);
    let mut key = [0; 40];
    key[0..8].copy_from_slice(&mapped_shard_uid.to_bytes());
    key[8..].copy_from_slice(hash.as_ref());
    key
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use near_primitives::errors::StorageError;
    use near_primitives::hash::CryptoHash;
    use near_primitives::shard_layout::ShardUId;

    use crate::NodeStorage;
    use crate::adapter::trie_store::TrieStoreAdapter;

    const ONE: std::num::NonZeroU32 = match std::num::NonZeroU32::new(1) {
        Some(num) => num,
        None => panic!(),
    };

    #[test]
    fn test_trie_store_adapter() {
        let (_tmp_dir, opener) = NodeStorage::test_opener();
        let store = TrieStoreAdapter::new(opener.open().unwrap().get_hot_store());
        let shard_uids: Vec<ShardUId> =
            (0..3).map(|i| ShardUId { version: 0, shard_id: i }).collect();
        let dummy_hash = CryptoHash::default();

        assert_matches!(
            store.get(shard_uids[0], &dummy_hash),
            Err(StorageError::MissingTrieValue(_, _))
        );
        {
            let mut store_update = store.store_update();
            store_update.increment_refcount_by(shard_uids[0], &dummy_hash, &[0], ONE);
            store_update.increment_refcount_by(shard_uids[1], &dummy_hash, &[1], ONE);
            store_update.increment_refcount_by(shard_uids[2], &dummy_hash, &[2], ONE);
            store_update.commit().unwrap();
        }
        assert_eq!(*store.get(shard_uids[0], &dummy_hash).unwrap(), [0]);
        {
            let mut store_update = store.store_update();
            store_update.delete_all_state();
            store_update.commit().unwrap();
        }
        assert_matches!(
            store.get(shard_uids[0], &dummy_hash),
            Err(StorageError::MissingTrieValue(_, _))
        );
    }

    #[test]
    fn test_shard_uid_mapping() {
        let (_tmp_dir, opener) = NodeStorage::test_opener();
        let store = TrieStoreAdapter::new(opener.open().unwrap().get_hot_store());
        let parent_shard = ShardUId { version: 0, shard_id: 0 };
        let child_shard = ShardUId { version: 0, shard_id: 1 };
        let dummy_hash = CryptoHash::default();
        // Write some data to `parent_shard`.
        {
            let mut store_update = store.store_update();
            store_update.increment_refcount_by(parent_shard, &dummy_hash, &[0], ONE);
            store_update.commit().unwrap();
        }
        // The data is not yet visible to child shard, because the mapping has not been set yet.
        assert_matches!(
            store.get(child_shard, &dummy_hash),
            Err(StorageError::MissingTrieValue(_, _))
        );
        // Set the shard_uid mapping from `child_shard` to `parent_shard`.
        {
            let mut store_update = store.store_update();
            store_update.set_shard_uid_mapping(child_shard, parent_shard);
            store_update.commit().unwrap();
        }
        // The data is now visible to both `parent_shard` and `child_shard`.
        assert_eq!(*store.get(child_shard, &dummy_hash).unwrap(), [0]);
        assert_eq!(*store.get(parent_shard, &dummy_hash).unwrap(), [0]);
        // Remove the data using `parent_shard` UId.
        {
            let mut store_update = store.store_update();
            store_update.decrement_refcount(parent_shard, &dummy_hash);
            store_update.commit().unwrap();
        }
        // The data is now not visible to any shard.
        assert_matches!(
            store.get(child_shard, &dummy_hash),
            Err(StorageError::MissingTrieValue(_, _))
        );
        assert_matches!(
            store.get(parent_shard, &dummy_hash),
            Err(StorageError::MissingTrieValue(_, _))
        );
        // Restore the data now using the `child_shard` UId.
        {
            let mut store_update = store.store_update();
            store_update.increment_refcount_by(child_shard, &dummy_hash, &[0], ONE);
            store_update.commit().unwrap();
        }
        // The data is now visible to both shards again.
        assert_eq!(*store.get(child_shard, &dummy_hash).unwrap(), [0]);
        assert_eq!(*store.get(parent_shard, &dummy_hash).unwrap(), [0]);
        // Remove the data using `child_shard` UId.
        {
            let mut store_update = store.store_update();
            store_update.decrement_refcount_by(child_shard, &dummy_hash, ONE);
            store_update.commit().unwrap();
        }
        // The data is not visible to any shard again.
        assert_matches!(
            store.get(child_shard, &dummy_hash),
            Err(StorageError::MissingTrieValue(_, _))
        );
    }
}
