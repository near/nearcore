use crate::columns::DBKeyType;
use crate::refcount::add_positive_refcount;
use crate::trie::TrieRefcountChange;
use crate::{DBCol, DBTransaction, Database, Store, TrieChanges};

use borsh::BorshDeserialize;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::BlockHeight;
use std::collections::HashMap;
use std::io;
use strum::IntoEnumIterator;

type StoreKey = Vec<u8>;
type StoreValue = Option<Vec<u8>>;
type StoreCache = HashMap<(DBCol, StoreKey), StoreValue>;

struct StoreWithCache<'a> {
    store: &'a Store,
    cache: StoreCache,
}

/// Updates provided cold database from provided hot store with information about block at `height`.
/// Wraps hot store in `StoreWithCache` for optimizing reads.
///
/// First, we read from hot store information necessary
/// to determine all the keys that need to be updated in cold db.
/// Then we write updates to cold db column by column.
///
/// This approach is used, because a key for db often combines several parts,
/// and many of those parts are reused across several cold columns (block hash, shard id, chunk hash, tx hash, ...).
/// Rather than manually combining those parts in the right order for every cold column,
/// we define `DBCol::key_type` to determine how a key for the column is formed,
/// `get_keys_from_store` to determine all possible keys only for needed key parts,
/// and `combine_keys` to generated all possible whole keys for the column based on order of those parts.
///
/// To add a new column to cold storage, we need to
/// 1. add it to `DBCol::is_cold` list
/// 2. define `DBCol::key_type` for it (if it isn't already defined)
/// 3. add new clause in `get_keys_from_store` for new key types used for this column (if there are any)
pub fn update_cold_db(
    cold_db: &dyn Database,
    hot_store: &Store,
    shard_layout: &ShardLayout,
    height: &BlockHeight,
) -> io::Result<()> {
    let _span = tracing::debug_span!(target: "store", "update cold db", height = height);

    let mut store_with_cache = StoreWithCache { store: hot_store, cache: StoreCache::new() };

    let key_type_to_keys = get_keys_from_store(&mut store_with_cache, shard_layout, height)?;
    for col in DBCol::iter() {
        if col.is_cold() {
            copy_from_store(
                cold_db,
                &mut store_with_cache,
                col,
                combine_keys(&key_type_to_keys, &col.key_type()),
            )?;
        }
    }

    Ok(())
}

/// Gets values for given keys in a column from provided hot_store.
/// Creates a transaction based on that values with set DBOp s.
/// Writes that transaction to cold_db.
fn copy_from_store(
    cold_db: &dyn Database,
    hot_store: &mut StoreWithCache,
    col: DBCol,
    keys: Vec<StoreKey>,
) -> io::Result<()> {
    let _span = tracing::debug_span!(target: "store", "create and write transaction to cold db", col = %col);

    let mut transaction = DBTransaction::new();
    for key in keys {
        // TODO: Look into using RocksDB’s multi_key function.  It
        // might speed things up.  Currently our Database abstraction
        // doesn’t offer interface for it so that would need to be
        // added.
        let data = hot_store.get(col, &key)?;
        if let Some(value) = data {
            // Database checks col.is_rc() on read and write
            // And in every way expects rc columns to be written with rc
            //
            // TODO: As an optimisation, we might consider breaking the
            // abstraction layer.  Since we’re always writing to cold database,
            // rather than using `cold_db: &dyn Database` argument we cloud have
            // `cold_db: &ColdDB` and then some custom function which lets us
            // write raw bytes.
            if col.is_rc() {
                transaction.update_refcount(
                    col,
                    key,
                    add_positive_refcount(&value, std::num::NonZeroU32::new(1).unwrap()),
                );
            } else {
                transaction.set(col, key, value);
            }
        }
    }
    cold_db.write(transaction)?;
    return Ok(());
}

pub fn test_cold_genesis_update(cold_db: &dyn Database, hot_store: &Store) -> io::Result<()> {
    let mut store_with_cache = StoreWithCache { store: hot_store, cache: StoreCache::new() };
    for col in DBCol::iter() {
        if col.is_cold() {
            copy_from_store(
                cold_db,
                &mut store_with_cache,
                col,
                hot_store.iter(col).map(|x| x.unwrap().0.to_vec()).collect(),
            )?;
        }
    }
    Ok(())
}

pub fn test_get_store_reads(column: DBCol) -> u64 {
    crate::metrics::COLD_MIGRATION_READS.with_label_values(&[<&str>::from(column)]).get()
}

/// Returns HashMap from DBKeyType to possible keys of that type for provided height.
/// Only constructs keys for key types that are used in cold columns.
/// The goal is to capture all changes to db made during production of the block at provided height.
/// So, for every KeyType we need to capture all the keys that are related to that block.
/// For BlockHash it is just one key -- block hash of that height.
/// But for TransactionHash, for example, it is all of the tx hashes in that block.
fn get_keys_from_store(
    store: &mut StoreWithCache,
    shard_layout: &ShardLayout,
    height: &BlockHeight,
) -> io::Result<HashMap<DBKeyType, Vec<StoreKey>>> {
    let mut key_type_to_keys = HashMap::new();

    let height_key = height.to_le_bytes();
    let block_hash_key = store.get_or_err(DBCol::BlockHeight, &height_key)?.as_slice().to_vec();

    for key_type in DBKeyType::iter() {
        key_type_to_keys.insert(
            key_type,
            match key_type {
                DBKeyType::BlockHash => vec![block_hash_key.clone()],
                DBKeyType::ShardUId => shard_layout
                    .get_shard_uids()
                    .iter()
                    .map(|uid| uid.to_bytes().to_vec())
                    .collect(),
                // TODO: don't write values of State column to cache. Write them directly to colddb.
                DBKeyType::TrieNodeOrValueHash => {
                    let mut keys = vec![];
                    for shard_uid in shard_layout.get_shard_uids() {
                        let shard_uid_key = shard_uid.to_bytes();

                        debug_assert_eq!(
                            DBCol::TrieChanges.key_type(),
                            &[DBKeyType::BlockHash, DBKeyType::ShardUId]
                        );
                        let trie_changes_option: Option<TrieChanges> =
                            store
                                .get_ser(
                                    DBCol::TrieChanges,
                                    &join_two_keys(&block_hash_key, &shard_uid_key),
                                )?;

                        if let Some(trie_changes) = trie_changes_option {
                            for op in trie_changes.insertions() {
                                store.insert_state_to_cache_from_op(op, &shard_uid_key);
                                keys.push(op.hash().as_bytes().to_vec());
                            }
                        }
                    }
                    keys
                }
                _ => {
                    vec![]
                }
            },
        );
    }

    Ok(key_type_to_keys)
}

pub fn join_two_keys(prefix_key: &[u8], suffix_key: &[u8]) -> StoreKey {
    [prefix_key, suffix_key].concat()
}

/// Returns all possible keys for a column with key represented by a specific sequence of key types.
/// `key_type_to_value` -- result of `get_keys_from_store`, mapping from KeyType to all possible keys of that type.
/// `key_types` -- description of a final key, what sequence of key types forms a key, result of `DBCol::key_type`.
/// Basically, returns all possible combinations of keys from `key_type_to_value` for given order of key types.
pub fn combine_keys(
    key_type_to_value: &HashMap<DBKeyType, Vec<StoreKey>>,
    key_types: &[DBKeyType],
) -> Vec<StoreKey> {
    combine_keys_with_stop(key_type_to_value, key_types, key_types.len())
}

/// Recursive method to create every combination of keys values for given order of key types.
/// stop: usize -- what length of key_types to consider.
/// first generates all the key combination for first stop - 1 key types
/// then adds every key value for the last key type to every key value generated by previous call.
fn combine_keys_with_stop(
    key_type_to_keys: &HashMap<DBKeyType, Vec<StoreKey>>,
    keys_order: &[DBKeyType],
    stop: usize,
) -> Vec<StoreKey> {
    // if no key types are provided, return one empty key value
    if stop == 0 {
        return vec![StoreKey::new()];
    }
    let last_kt = &keys_order[stop - 1];
    // if one of the key types has no keys, no need to calculate anything, the result is empty
    if key_type_to_keys[last_kt].is_empty() {
        return vec![];
    }
    let all_smaller_keys = combine_keys_with_stop(key_type_to_keys, keys_order, stop - 1);
    let mut result_keys = vec![];
    for prefix_key in &all_smaller_keys {
        for suffix_key in &key_type_to_keys[last_kt] {
            result_keys.push(join_two_keys(prefix_key, suffix_key));
        }
    }
    result_keys
}

fn option_to_not_found<T, F>(res: io::Result<Option<T>>, field_name: F) -> io::Result<T>
where
    F: std::string::ToString,
{
    match res {
        Ok(Some(o)) => Ok(o),
        Ok(None) => Err(io::Error::new(io::ErrorKind::NotFound, field_name.to_string())),
        Err(e) => Err(e),
    }
}

#[allow(dead_code)]
impl StoreWithCache<'_> {
    pub fn get(&mut self, column: DBCol, key: &[u8]) -> io::Result<StoreValue> {
        if !self.cache.contains_key(&(column, key.to_vec())) {
            crate::metrics::COLD_MIGRATION_READS.with_label_values(&[<&str>::from(column)]).inc();
            self.cache.insert(
                (column.clone(), key.to_vec()),
                self.store.get(column, key)?.map(|x| x.as_slice().to_vec()),
            );
        }
        Ok(self.cache[&(column, key.to_vec())].clone())
    }

    pub fn get_ser<T: BorshDeserialize>(
        &mut self,
        column: DBCol,
        key: &[u8],
    ) -> io::Result<Option<T>> {
        match self.get(column, key)? {
            Some(bytes) => Ok(Some(T::try_from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    pub fn get_or_err(&mut self, column: DBCol, key: &[u8]) -> io::Result<Vec<u8>> {
        option_to_not_found(self.get(column, key), format_args!("{:?}: {:?}", column, key))
    }

    pub fn get_ser_or_err<T: BorshDeserialize>(
        &mut self,
        column: DBCol,
        key: &[u8],
    ) -> io::Result<T> {
        option_to_not_found(self.get_ser(column, key), format_args!("{:?}: {:?}", column, key))
    }

    pub fn insert_state_to_cache_from_op(
        &mut self,
        op: &TrieRefcountChange,
        shard_uid_key: &[u8],
    ) {
        debug_assert_eq!(
            DBCol::State.key_type(),
            &[DBKeyType::ShardUId, DBKeyType::TrieNodeOrValueHash]
        );
        self.cache.insert(
            (DBCol::State, join_two_keys(shard_uid_key, op.hash().as_bytes())),
            Some(op.payload().to_vec()),
        );
    }
}

#[cfg(test)]
mod test {
    use super::{combine_keys, StoreKey};
    use crate::columns::DBKeyType;
    use std::collections::{HashMap, HashSet};

    #[test]
    fn test_combine_keys() {
        // What DBKeyType s are used here does not matter
        let key_type_to_keys = HashMap::from([
            (DBKeyType::BlockHash, vec![vec![1, 2, 3], vec![2, 3]]),
            (DBKeyType::BlockHeight, vec![vec![0, 1], vec![3, 4, 5]]),
            (DBKeyType::ShardId, vec![]),
        ]);

        assert_eq!(
            HashSet::<StoreKey>::from_iter(combine_keys(
                &key_type_to_keys,
                &vec![DBKeyType::BlockHash, DBKeyType::BlockHeight]
            )),
            HashSet::<StoreKey>::from_iter(vec![
                vec![1, 2, 3, 0, 1],
                vec![1, 2, 3, 3, 4, 5],
                vec![2, 3, 0, 1],
                vec![2, 3, 3, 4, 5]
            ])
        );

        assert_eq!(
            HashSet::<StoreKey>::from_iter(combine_keys(
                &key_type_to_keys,
                &vec![DBKeyType::BlockHeight, DBKeyType::BlockHash, DBKeyType::BlockHeight]
            )),
            HashSet::<StoreKey>::from_iter(vec![
                vec![0, 1, 1, 2, 3, 0, 1],
                vec![0, 1, 1, 2, 3, 3, 4, 5],
                vec![0, 1, 2, 3, 0, 1],
                vec![0, 1, 2, 3, 3, 4, 5],
                vec![3, 4, 5, 1, 2, 3, 0, 1],
                vec![3, 4, 5, 1, 2, 3, 3, 4, 5],
                vec![3, 4, 5, 2, 3, 0, 1],
                vec![3, 4, 5, 2, 3, 3, 4, 5]
            ])
        );

        assert_eq!(
            HashSet::<StoreKey>::from_iter(combine_keys(
                &key_type_to_keys,
                &vec![DBKeyType::ShardId, DBKeyType::BlockHeight]
            )),
            HashSet::<StoreKey>::from_iter(vec![])
        );

        assert_eq!(
            HashSet::<StoreKey>::from_iter(combine_keys(
                &key_type_to_keys,
                &vec![DBKeyType::BlockHash, DBKeyType::ShardId]
            )),
            HashSet::<StoreKey>::from_iter(vec![])
        );

        assert_eq!(
            HashSet::<StoreKey>::from_iter(combine_keys(&key_type_to_keys, &vec![])),
            HashSet::<StoreKey>::from_iter(vec![vec![]])
        );
    }
}
