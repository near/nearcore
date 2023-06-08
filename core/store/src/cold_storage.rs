use crate::columns::DBKeyType;
use crate::db::{ColdDB, COLD_HEAD_KEY, HEAD_KEY};
use crate::trie::TrieRefcountChange;
use crate::{metrics, DBCol, DBTransaction, Database, Store, TrieChanges};

use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::block::{Block, BlockHeader, Tip};
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::sharding::ShardChunk;
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

/// The BatchTransaction can be used to write multiple set operations to the cold db in batches.
/// [`write`] is called every time `transaction_size` overgrows `threshold_transaction_size`.
/// [`write`] should also be called manually before dropping BatchTransaction to write any leftovers.
struct BatchTransaction {
    cold_db: std::sync::Arc<ColdDB>,
    transaction: DBTransaction,
    /// Size of all values keys and values in `transaction` in bytes.
    transaction_size: usize,
    /// Minimum size, after which we write transaction
    threshold_transaction_size: usize,
}

/// Updates provided cold database from provided hot store with information about block at `height`.
/// Returns if the block was copied (false only if height is not present in `hot_store`).
/// Block as `height` has to be final.
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
    cold_db: &ColdDB,
    hot_store: &Store,
    shard_layout: &ShardLayout,
    height: &BlockHeight,
) -> io::Result<bool> {
    let _span = tracing::debug_span!(target: "store", "update cold db", height = height);
    let _timer = metrics::COLD_COPY_DURATION.start_timer();

    let mut store_with_cache = StoreWithCache { store: hot_store, cache: StoreCache::new() };

    if store_with_cache.get(DBCol::BlockHeight, &height.to_le_bytes())?.is_none() {
        return Ok(false);
    }

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

    Ok(true)
}

// Correctly set the key and value on DBTransaction, taking reference counting
// into account. For non-rc columns it just sets the value. For rc columns it
// appends rc = 1 to the value and sets it.
fn rc_aware_set(
    transaction: &mut DBTransaction,
    col: DBCol,
    key: Vec<u8>,
    mut value: Vec<u8>,
) -> usize {
    const ONE: &[u8] = &1i64.to_le_bytes();
    match col.is_rc() {
        false => {
            let size = key.len() + value.len();
            transaction.set(col, key, value);
            return size;
        }
        true => {
            value.extend_from_slice(&ONE);
            let size = key.len() + value.len();
            transaction.update_refcount(col, key, value);
            return size;
        }
    };
}

/// Gets values for given keys in a column from provided hot_store.
/// Creates a transaction based on that values with set DBOp s.
/// Writes that transaction to cold_db.
fn copy_from_store(
    cold_db: &ColdDB,
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
            // TODO: As an optimisation, we might consider breaking the
            // abstraction layer.  Since we’re always writing to cold database,
            // rather than using `cold_db: &dyn Database` argument we could have
            // `cold_db: &ColdDB` and then some custom function which lets us
            // write raw bytes. This would also allow us to bypass stripping and
            // re-adding the reference count.

            rc_aware_set(&mut transaction, col, key, value);
        }
    }
    cold_db.write(transaction)?;
    return Ok(());
}

/// This function sets the cold head to the Tip that reflect provided height in two places:
/// - In cold storage in HEAD key in BlockMisc column.
/// - In hot storage in COLD_HEAD key in BlockMisc column.
/// This function should be used after all of the blocks from genesis to `height` inclusive had been copied.
///
/// This method relies on the fact that BlockHeight and BlockHeader are not garbage collectable.
/// (to construct the Tip we query hot_store for block hash and block header)
/// If this is to change, caller should be careful about `height` not being garbage collected in hot storage yet.
pub fn update_cold_head(
    cold_db: &ColdDB,
    hot_store: &Store,
    height: &BlockHeight,
) -> io::Result<()> {
    tracing::debug!(target: "store", "update HEAD of cold db to {}", height);

    let mut store = StoreWithCache { store: hot_store, cache: StoreCache::new() };

    let height_key = height.to_le_bytes();
    let block_hash_key = store.get_or_err(DBCol::BlockHeight, &height_key)?.as_slice().to_vec();
    let tip_header = &store.get_ser_or_err::<BlockHeader>(DBCol::BlockHeader, &block_hash_key)?;
    let tip = Tip::from_header(tip_header);

    // Write HEAD to the cold db.
    {
        let mut transaction = DBTransaction::new();
        transaction.set(DBCol::BlockMisc, HEAD_KEY.to_vec(), tip.try_to_vec()?);
        cold_db.write(transaction)?;
    }

    // Write COLD_HEAD_KEY to the cold db.
    {
        let mut transaction = DBTransaction::new();
        transaction.set(DBCol::BlockMisc, COLD_HEAD_KEY.to_vec(), tip.try_to_vec()?);
        cold_db.write(transaction)?;
    }

    // Write COLD_HEAD to the hot db.
    {
        let mut transaction = DBTransaction::new();
        transaction.set(DBCol::BlockMisc, COLD_HEAD_KEY.to_vec(), tip.try_to_vec()?);
        hot_store.storage.write(transaction)?;

        crate::metrics::COLD_HEAD_HEIGHT.set(*height as i64);
    }

    return Ok(());
}

pub enum CopyAllDataToColdStatus {
    EverythingCopied,
    Interrupted,
}

/// Copies all contents of all cold columns from `hot_store` to `cold_db`.
/// Does it column by column, and because columns can be huge, writes in batches of ~`batch_size`.
pub fn copy_all_data_to_cold(
    cold_db: std::sync::Arc<ColdDB>,
    hot_store: &Store,
    batch_size: usize,
    keep_going: &std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> io::Result<CopyAllDataToColdStatus> {
    for col in DBCol::iter() {
        if col.is_cold() {
            tracing::info!(target: "cold_store", ?col, "Started column migration");
            let mut transaction = BatchTransaction::new(cold_db.clone(), batch_size);
            for result in hot_store.iter(col) {
                if !keep_going.load(std::sync::atomic::Ordering::Relaxed) {
                    tracing::debug!(target: "cold_store", "stopping copy_all_data_to_cold");
                    return Ok(CopyAllDataToColdStatus::Interrupted);
                }
                let (key, value) = result?;
                transaction.set_and_write_if_full(col, key.to_vec(), value.to_vec())?;
            }
            transaction.write()?;
            tracing::info!(target: "cold_store", ?col, "Finished column migration");
        }
    }
    Ok(CopyAllDataToColdStatus::EverythingCopied)
}

pub fn test_cold_genesis_update(cold_db: &ColdDB, hot_store: &Store) -> io::Result<()> {
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

pub fn test_get_store_initial_writes(column: DBCol) -> u64 {
    crate::metrics::COLD_STORE_MIGRATION_BATCH_WRITE_COUNT
        .with_label_values(&[<&str>::from(column)])
        .get()
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

    let block: Block = store.get_ser_or_err(DBCol::Block, &block_hash_key)?;
    let chunks = block
        .chunks()
        .iter()
        .map(|chunk_header| {
            store.get_ser_or_err(DBCol::Chunks, chunk_header.chunk_hash().as_bytes())
        })
        .collect::<io::Result<Vec<ShardChunk>>>()?;

    for key_type in DBKeyType::iter() {
        key_type_to_keys.insert(
            key_type,
            match key_type {
                DBKeyType::BlockHeight => vec![height_key.to_vec()],
                DBKeyType::BlockHash => vec![block_hash_key.clone()],
                DBKeyType::PreviousBlockHash => {
                    vec![block.header().prev_hash().as_bytes().to_vec()]
                }
                DBKeyType::ShardId => {
                    (0..shard_layout.num_shards()).map(|si| si.to_le_bytes().to_vec()).collect()
                }
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
                        let trie_changes_option: Option<TrieChanges> = store.get_ser(
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
                // TODO: write StateChanges values to colddb directly, not to cache.
                DBKeyType::TrieKey => {
                    let mut keys = vec![];
                    store.iter_prefix_with_callback(
                        DBCol::StateChanges,
                        &block_hash_key,
                        |full_key| {
                            let mut full_key = Vec::from(full_key);
                            full_key.drain(..block_hash_key.len());
                            keys.push(full_key);
                        },
                    )?;
                    keys
                }
                DBKeyType::TransactionHash => chunks
                    .iter()
                    .flat_map(|c| c.transactions().iter().map(|t| t.get_hash().as_bytes().to_vec()))
                    .collect(),
                DBKeyType::ReceiptHash => chunks
                    .iter()
                    .flat_map(|c| c.receipts().iter().map(|r| r.get_hash().as_bytes().to_vec()))
                    .collect(),
                DBKeyType::ChunkHash => {
                    chunks.iter().map(|c| c.chunk_hash().as_bytes().to_vec()).collect()
                }
                DBKeyType::OutcomeId => {
                    debug_assert_eq!(
                        DBCol::OutcomeIds.key_type(),
                        &[DBKeyType::BlockHash, DBKeyType::ShardId]
                    );
                    (0..shard_layout.num_shards())
                        .map(|shard_id| {
                            store.get_ser(
                                DBCol::OutcomeIds,
                                &join_two_keys(&block_hash_key, &shard_id.to_le_bytes()),
                            )
                        })
                        .collect::<io::Result<Vec<Option<Vec<CryptoHash>>>>>()?
                        .into_iter()
                        .flat_map(|hashes| {
                            hashes
                                .unwrap_or_default()
                                .into_iter()
                                .map(|hash| hash.as_bytes().to_vec())
                        })
                        .collect()
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
    pub fn iter_prefix_with_callback(
        &mut self,
        col: DBCol,
        key_prefix: &[u8],
        mut callback: impl FnMut(Box<[u8]>),
    ) -> io::Result<()> {
        for iter_result in self.store.iter_prefix(col, key_prefix) {
            let (key, value) = iter_result?;
            self.cache.insert((col, key.to_vec()), Some(value.into()));
            callback(key);
        }
        Ok(())
    }

    pub fn get(&mut self, column: DBCol, key: &[u8]) -> io::Result<StoreValue> {
        if !self.cache.contains_key(&(column, key.to_vec())) {
            crate::metrics::COLD_MIGRATION_READS.with_label_values(&[<&str>::from(column)]).inc();
            self.cache.insert(
                (column, key.to_vec()),
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

    pub fn insert_state_to_cache_from_op(&mut self, op: &TrieRefcountChange, shard_uid_key: &[u8]) {
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

impl BatchTransaction {
    pub fn new(cold_db: std::sync::Arc<ColdDB>, batch_size: usize) -> Self {
        Self {
            cold_db,
            transaction: DBTransaction::new(),
            transaction_size: 0,
            threshold_transaction_size: batch_size,
        }
    }

    /// Adds a set DBOp to `self.transaction`. Updates `self.transaction_size`.
    /// If `self.transaction_size` becomes too big, calls for write.
    pub fn set_and_write_if_full(
        &mut self,
        col: DBCol,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> io::Result<()> {
        let size = rc_aware_set(&mut self.transaction, col, key, value);
        self.transaction_size += size;

        if self.transaction_size > self.threshold_transaction_size {
            self.write()?;
        }
        Ok(())
    }

    /// Writes `self.transaction` and replaces it with new empty DBTransaction.
    /// Sets `self.transaction_size` to 0.
    fn write(&mut self) -> io::Result<()> {
        if self.transaction.ops.is_empty() {
            return Ok(());
        }

        let column_label = [<&str>::from(self.transaction.ops[0].col())];

        crate::metrics::COLD_STORE_MIGRATION_BATCH_WRITE_COUNT
            .with_label_values(&column_label)
            .inc();
        let _timer = crate::metrics::COLD_STORE_MIGRATION_BATCH_WRITE_TIME
            .with_label_values(&column_label)
            .start_timer();

        tracing::info!(
                target: "cold_store",
                ?column_label,
                tx_size_in_megabytes = self.transaction_size as f64 / 1e6,
                "Writing a Cold Store transaction");

        let transaction = std::mem::take(&mut self.transaction);
        self.cold_db.write(transaction)?;
        self.transaction_size = 0;

        Ok(())
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
