#![cfg_attr(enable_const_type_id, feature(const_type_id))]

extern crate core;

pub use crate::columns::DBCol;
pub use crate::config::{Mode, StoreConfig};
pub use crate::db::{
    CHUNK_TAIL_KEY, COLD_HEAD_KEY, FINAL_HEAD_KEY, FORK_TAIL_KEY, GENESIS_STATE_ROOTS_KEY,
    HEAD_KEY, HEADER_HEAD_KEY, LARGEST_TARGET_HEIGHT_KEY, LATEST_KNOWN_KEY, STATE_SNAPSHOT_KEY,
    STATE_SYNC_DUMP_KEY, TAIL_KEY,
};
use crate::db::{DBTransaction, Database, StoreStatistics, metadata};
pub use crate::node_storage::opener::{
    StoreMigrator, StoreOpener, StoreOpenerError, checkpoint_hot_storage_and_cleanup_columns,
    clear_columns,
};
pub use crate::node_storage::{NodeStorage, Temperature};
pub use crate::store::{Store, StoreUpdate};
pub use crate::trie::update::{TrieUpdate, TrieUpdateIterator, TrieUpdateValuePtr};
pub use crate::trie::{
    ApplyStatePartResult, KeyForStateChanges, KeyLookupMode, NibbleSlice, PartialStorage,
    PrefetchApi, PrefetchError, RawTrieNode, RawTrieNodeWithSize, STATE_SNAPSHOT_COLUMNS,
    ShardTries, StateSnapshot, StateSnapshotConfig, Trie, TrieAccess, TrieCache,
    TrieCachingStorage, TrieChanges, TrieConfig, TrieDBStorage, TrieStorage, WrappedTrieChanges,
    estimator,
};
pub use crate::utils::*;
pub use near_primitives::errors::{MissingTrieValueContext, StorageError};
pub use near_primitives::shard_layout::ShardUId;

pub mod adapter;
pub mod archive;
mod columns;
pub mod config;
pub mod contract;
pub mod db;
pub mod flat;
pub mod genesis;
pub mod metrics;
pub mod migrations;
mod node_storage;
mod store;
pub mod trie;
mod utils;

#[cfg(test)]
mod tests {
    use super::{DBCol, NodeStorage, Store};

    fn test_clear_column(store: Store) {
        assert_eq!(store.get(DBCol::State, &[1; 8]).unwrap(), None);
        {
            let mut store_update = store.store_update();
            store_update.increment_refcount(DBCol::State, &[1; 8], &[1]);
            store_update.increment_refcount(DBCol::State, &[2; 8], &[2]);
            store_update.increment_refcount(DBCol::State, &[3; 8], &[3]);
            store_update.commit().unwrap();
        }
        assert_eq!(store.get(DBCol::State, &[1; 8]).unwrap().as_deref(), Some(&[1][..]));
        {
            let mut store_update = store.store_update();
            store_update.delete_all(DBCol::State);
            store_update.commit().unwrap();
        }
        assert_eq!(store.get(DBCol::State, &[1; 8]).unwrap(), None);
    }

    #[test]
    fn clear_column_rocksdb() {
        let (_tmp_dir, opener) = NodeStorage::test_opener();
        test_clear_column(opener.open().unwrap().get_hot_store());
    }

    #[test]
    fn clear_column_testdb() {
        test_clear_column(crate::test_utils::create_test_store());
    }

    /// Asserts that elements in the vector are sorted.
    #[track_caller]
    fn assert_sorted(want_count: usize, keys: Vec<Box<[u8]>>) {
        assert_eq!(want_count, keys.len());
        for (pos, pair) in keys.windows(2).enumerate() {
            let (fst, snd) = (&pair[0], &pair[1]);
            assert!(fst <= snd, "{fst:?} > {snd:?} at {pos}");
        }
    }

    /// Checks that keys are sorted when iterating.
    fn test_iter_order_impl(store: Store) {
        use rand::Rng;

        // An arbitrary non-rc non-insert-only column we can write data into.
        const COLUMN: DBCol = DBCol::RecentOutboundConnections;
        assert!(!COLUMN.is_rc());
        assert!(!COLUMN.is_insert_only());

        const COUNT: usize = 10_000;
        const PREFIXES: [[u8; 4]; 6] =
            [*b"foo0", *b"foo1", *b"foo2", *b"foo\xff", *b"fop\0", *b"\xff\xff\xff\xff"];

        // Fill column with random keys.  We're inserting multiple sets of keys
        // with different four-byte prefixes..  Each set is `COUNT` keys (for
        // total of `PREFIXES.len()*COUNT` keys).
        let mut rng: rand::rngs::StdRng = rand::SeedableRng::seed_from_u64(0x3243f6a8885a308d);
        let mut update = store.store_update();
        let mut buf = [0u8; 20];
        for prefix in &PREFIXES {
            buf[..prefix.len()].clone_from_slice(prefix);
            for _ in 0..COUNT {
                rng.fill(&mut buf[prefix.len()..]);
                update.set(COLUMN, &buf, &buf);
            }
        }
        update.commit().unwrap();

        fn collect<'a>(iter: crate::db::DBIterator<'a>) -> Vec<Box<[u8]>> {
            iter.map(Result::unwrap).map(|(key, _)| key).collect()
        }

        // Check that full scan produces keys in proper order.
        assert_sorted(PREFIXES.len() * COUNT, collect(store.iter(COLUMN)));
        assert_sorted(PREFIXES.len() * COUNT, collect(store.iter_raw_bytes(COLUMN)));
        assert_sorted(PREFIXES.len() * COUNT, collect(store.iter_prefix(COLUMN, b"")));

        // Check that prefix scan produces keys in proper order.
        for prefix in &PREFIXES {
            let keys = collect(store.iter_prefix(COLUMN, prefix));
            for (pos, key) in keys.iter().enumerate() {
                assert_eq!(
                    prefix,
                    &key[0..4],
                    "Expected {prefix:?} prefix but got {key:?} key at {pos}"
                );
            }
            assert_sorted(COUNT, keys);
        }
    }

    #[test]
    fn rocksdb_iter_order() {
        let (_tempdir, opener) = NodeStorage::test_opener();
        test_iter_order_impl(opener.open().unwrap().get_hot_store());
    }

    #[test]
    fn testdb_iter_order() {
        test_iter_order_impl(crate::test_utils::create_test_store());
    }

    /// Check saving and reading columns to/from a file.
    #[test]
    fn test_save_to_file() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();

        {
            let store = crate::test_utils::create_test_store();
            let mut store_update = store.store_update();
            store_update.increment_refcount(DBCol::State, &[1; 8], &[1]);
            store_update.increment_refcount(DBCol::State, &[2; 8], &[2]);
            store_update.increment_refcount(DBCol::State, &[2; 8], &[2]);
            store_update.commit().unwrap();
            store.save_state_to_file(tmp.path()).unwrap();
        }

        // Verify expected encoding.
        {
            let mut buffer = Vec::new();
            std::io::Read::read_to_end(tmp.as_file_mut(), &mut buffer).unwrap();
            #[rustfmt::skip]
            assert_eq!(&[
                /* column: */ 0, /* key len: */ 8, 0, 0, 0, /* key: */ 1, 1, 1, 1, 1, 1, 1, 1,
                                 /* val len: */ 9, 0, 0, 0, /* val: */ 1, 1, 0, 0, 0, 0, 0, 0, 0,
                /* column: */ 0, /* key len: */ 8, 0, 0, 0, /* key: */ 2, 2, 2, 2, 2, 2, 2, 2,
                                 /* val len: */ 9, 0, 0, 0, /* val: */ 2, 2, 0, 0, 0, 0, 0, 0, 0,
                /* end mark: */ 255,
            ][..], buffer.as_slice());
        }

        {
            // Fresh storage, should have no data.
            let store = crate::test_utils::create_test_store();
            assert_eq!(None, store.get(DBCol::State, &[1; 8]).unwrap());
            assert_eq!(None, store.get(DBCol::State, &[2; 8]).unwrap());

            // Read data from file.
            store.load_state_from_file(tmp.path()).unwrap();
            assert_eq!(Some(&[1u8][..]), store.get(DBCol::State, &[1; 8]).unwrap().as_deref());
            assert_eq!(Some(&[2u8][..]), store.get(DBCol::State, &[2; 8]).unwrap().as_deref());

            // Key &[2] should have refcount of two so once decreased it should
            // still exist.
            let mut store_update = store.store_update();
            store_update.decrement_refcount(DBCol::State, &[1; 8]);
            store_update.decrement_refcount(DBCol::State, &[2; 8]);
            store_update.commit().unwrap();
            assert_eq!(None, store.get(DBCol::State, &[1; 8]).unwrap());
            assert_eq!(Some(&[2u8][..]), store.get(DBCol::State, &[2; 8]).unwrap().as_deref());
        }

        // Verify detection of corrupt file.
        let file = std::fs::File::options().write(true).open(tmp.path()).unwrap();
        let len = file.metadata().unwrap().len();
        file.set_len(len.saturating_sub(1)).unwrap();
        core::mem::drop(file);
        let store = crate::test_utils::create_test_store();
        assert_eq!(
            std::io::ErrorKind::InvalidData,
            store.load_state_from_file(tmp.path()).unwrap_err().kind()
        );
    }
}
