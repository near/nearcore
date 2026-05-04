//! Real-RocksDB tests for `Store::multi_get` and `Store::multi_exists`.
//!
//! The cross-backend correctness contract is covered by
//! `database_conformance.rs`.  This file focuses on RocksDB-specific
//! behaviour: large batches, mixed present/absent keys at scale, refcount
//! handling in the actual RC merge path, ordering invariants under real
//! LSM-level reads, and the pinned-slice path through
//! `batched_multi_get_cf`.

use near_store::{DBCol, NodeStorage, Store};
use std::num::NonZeroU32;
use tempfile::TempDir;

const RC_COL: DBCol = DBCol::Receipts; // RC, cold-resident
const NON_RC_COL: DBCol = DBCol::Block; // non-RC, cold-resident

/// Open a fresh on-disk RocksDB-backed `Store`.  The `TempDir` must
/// outlive the `Store`.
fn fresh_rocksdb_store() -> (TempDir, Store) {
    let (db_tmp, opener) = NodeStorage::test_opener();
    let storage = opener.open().expect("open test store");
    let store = storage.get_hot_store();
    (db_tmp, store)
}

fn put_non_rc(store: &Store, col: DBCol, key: &[u8], value: &[u8]) {
    let mut update = store.store_update();
    // `Block` is an insert-only column, so use `insert` instead of `set`;
    // `set` panics on insert-only columns at store.rs:546.
    update.insert(col, key.to_vec(), value.to_vec());
    update.commit();
}

fn put_rc(store: &Store, col: DBCol, key: &[u8], value: &[u8], rc: u32) {
    let mut update = store.store_update();
    update.increment_refcount_by(col, key, value, NonZeroU32::new(rc).expect("rc > 0"));
    update.commit();
}

#[test]
fn empty_keys_returns_empty_vec() {
    let (_tmp, store) = fresh_rocksdb_store();
    let empty: &[&[u8]] = &[];
    assert!(store.multi_get(NON_RC_COL, empty).is_empty());
    assert!(store.multi_get(RC_COL, empty).is_empty());
    assert!(store.multi_exists(RC_COL, empty).is_empty());
}

#[test]
fn small_batch_present_and_absent_aligned() {
    let (_tmp, store) = fresh_rocksdb_store();
    put_non_rc(&store, NON_RC_COL, &[1u8; 32], b"v1");
    put_non_rc(&store, NON_RC_COL, &[3u8; 32], b"v3");

    let key1 = [1u8; 32];
    let key2 = [2u8; 32];
    let key3 = [3u8; 32];
    let keys: &[&[u8]] = &[&key1, &key2, &key3];
    let results = store.multi_get(NON_RC_COL, keys);
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].as_deref(), Some(b"v1".as_ref()));
    assert!(results[1].is_none(), "missing key must be None at the right index");
    assert_eq!(results[2].as_deref(), Some(b"v3".as_ref()));
}

#[test]
fn rc_column_strips_refcount() {
    let (_tmp, store) = fresh_rocksdb_store();
    let key = [42u8; 32];
    put_rc(&store, RC_COL, &key, b"payload", 1);

    let results = store.multi_get(RC_COL, &[&key]);
    assert_eq!(results[0].as_deref(), Some(b"payload".as_ref()), "refcount must be stripped");

    let exists = store.multi_exists(RC_COL, &[&key]);
    assert_eq!(exists, vec![true]);
}

#[test]
fn rc_column_decrement_then_lookup_returns_none() {
    let (_tmp, store) = fresh_rocksdb_store();
    let key = [7u8; 32];
    put_rc(&store, RC_COL, &key, b"payload", 1);

    // Decrement to refcount = 0.
    let mut update = store.store_update();
    update.decrement_refcount(RC_COL, &key);
    update.commit();

    let results = store.multi_get(RC_COL, &[&key]);
    assert!(results[0].is_none(), "decremented-to-zero key must be absent under rc-stripping");
    assert_eq!(store.multi_exists(RC_COL, &[&key]), vec![false]);
}

#[test]
fn large_batch_ordering_preserved() {
    let (_tmp, store) = fresh_rocksdb_store();
    // 1000 entries with deterministic 32-byte keys.
    let mut owned_keys: Vec<[u8; 32]> = Vec::with_capacity(1000);
    for i in 0..1000u32 {
        let mut k = [0u8; 32];
        k[..4].copy_from_slice(&i.to_be_bytes());
        owned_keys.push(k);
        let v = format!("value-{i}");
        put_non_rc(&store, NON_RC_COL, &k, v.as_bytes());
    }

    // Query in a non-sorted order: even keys first, then odd, plus 100 absent.
    let mut query_owned: Vec<[u8; 32]> = Vec::new();
    for i in (0..1000u32).step_by(2) {
        let mut k = [0u8; 32];
        k[..4].copy_from_slice(&i.to_be_bytes());
        query_owned.push(k);
    }
    for i in (1..1000u32).step_by(2) {
        let mut k = [0u8; 32];
        k[..4].copy_from_slice(&i.to_be_bytes());
        query_owned.push(k);
    }
    for i in 1000..1100u32 {
        let mut k = [0u8; 32];
        k[..4].copy_from_slice(&i.to_be_bytes());
        query_owned.push(k);
    }
    let query: Vec<&[u8]> = query_owned.iter().map(|k| k.as_slice()).collect();

    let results = store.multi_get(NON_RC_COL, &query);
    assert_eq!(results.len(), query.len());

    for (i, slot) in results.iter().enumerate() {
        let expected = if i < 1000 {
            // i < 500: even keys; i in [500, 1000): odd keys.
            let key_idx = if i < 500 { (i * 2) as u32 } else { (((i - 500) * 2) + 1) as u32 };
            Some(format!("value-{key_idx}"))
        } else {
            None
        };
        match (slot.as_deref(), expected.as_deref()) {
            (Some(actual), Some(expected)) => {
                assert_eq!(actual, expected.as_bytes(), "slot {i} mismatch")
            }
            (None, None) => {}
            (a, e) => {
                panic!(
                    "slot {i} presence mismatch: actual={:?}, expected={:?}",
                    a.is_some(),
                    e.is_some()
                )
            }
        }
    }
}

#[test]
fn batched_multi_get_returns_pinned_slices() {
    // The trait surface returns `Vec<Option<DBSlice>>`; pinned vs Vec is
    // an implementation detail.  Verify that a moderate-sized RC batch
    // round-trips correctly through `batched_multi_get_cf` (the path that
    // returns DBPinnableSlice and which `Store::multi_get` selects via
    // `batched_multi_get_with_rc_stripped`).
    let (_tmp, store) = fresh_rocksdb_store();
    let mut owned_keys: Vec<[u8; 32]> = Vec::with_capacity(200);
    for i in 0..200u32 {
        let mut k = [0u8; 32];
        k[..4].copy_from_slice(&i.to_be_bytes());
        owned_keys.push(k);
        let v = format!("rc-payload-{i}");
        put_rc(&store, RC_COL, &k, v.as_bytes(), 1);
    }
    let query: Vec<&[u8]> = owned_keys.iter().map(|k| k.as_slice()).collect();
    let results = store.multi_get(RC_COL, &query);
    assert_eq!(results.len(), 200);
    for (i, slot) in results.iter().enumerate() {
        let expected = format!("rc-payload-{i}");
        assert_eq!(slot.as_deref(), Some(expected.as_bytes()), "slot {i}");
    }
}

#[test]
fn multi_get_returns_same_as_scalar_get() {
    let (_tmp, store) = fresh_rocksdb_store();
    // Mix of present + missing in a non-trivial pattern.
    for i in 0..50u32 {
        if i % 3 != 0 {
            let mut k = [0u8; 32];
            k[..4].copy_from_slice(&i.to_be_bytes());
            put_rc(&store, RC_COL, &k, format!("v{i}").as_bytes(), 1);
        }
    }
    let mut query_owned: Vec<[u8; 32]> = Vec::new();
    for i in 0..60u32 {
        let mut k = [0u8; 32];
        k[..4].copy_from_slice(&i.to_be_bytes());
        query_owned.push(k);
    }
    let query: Vec<&[u8]> = query_owned.iter().map(|k| k.as_slice()).collect();

    let multi = store.multi_get(RC_COL, &query);
    for (i, key) in query.iter().enumerate() {
        let scalar = store.get(RC_COL, key);
        match (multi[i].as_deref(), scalar.as_deref()) {
            (Some(a), Some(e)) => assert_eq!(a, e, "slot {i} value mismatch"),
            (None, None) => {}
            (a, e) => panic!(
                "slot {i} presence mismatch: multi={:?} scalar={:?}",
                a.is_some(),
                e.is_some()
            ),
        }
    }
}
