//! Unit tests for [`Store::ingest_insert_only_sst`] against a real RocksDB tempdir.
//!
//! Driven by the SST ingest plan: the in-memory `TestDB` does not support
//! `ingest_external_sst_files`, so we exercise the helper directly here against
//! a real on-disk RocksDB instance to cover all the documented invariants.

use near_store::{DBCol, NodeStorage, Store};
use rand::RngCore;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use tempfile::TempDir;

/// Open a fresh on-disk RocksDB-backed `Store` and return it together with the
/// `TempDir` that owns its files. The `TempDir` must outlive the `Store`.
fn fresh_rocksdb_store() -> (TempDir, TempDir, Store) {
    let (db_tmp, opener) = NodeStorage::test_opener();
    let storage = opener.open().expect("open test store");
    let store = storage.get_hot_store();
    let sst_tmp = tempfile::tempdir().expect("create sst tempdir");
    (db_tmp, sst_tmp, store)
}

#[test]
fn empty_input_returns_ok_without_creating_a_file() {
    let (_db_tmp, sst_tmp, store) = fresh_rocksdb_store();
    let entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    store
        .ingest_insert_only_sst(DBCol::ReceiptToTx, &entries, sst_tmp.path())
        .expect("empty input should succeed");
    let any_entries = std::fs::read_dir(sst_tmp.path()).map(|d| d.count()).unwrap_or(0);
    assert_eq!(any_entries, 0, "no SST files should be created for empty input");
}

#[test]
fn sorted_unique_entries_ingest_and_become_readable() {
    let (_db_tmp, sst_tmp, store) = fresh_rocksdb_store();
    let entries: Vec<(Vec<u8>, Vec<u8>)> =
        (0u8..16).map(|i| (vec![i; 32], format!("value-{i}").into_bytes())).collect();
    store
        .ingest_insert_only_sst(DBCol::ReceiptToTx, &entries, sst_tmp.path())
        .expect("ingest should succeed");

    for (k, v) in &entries {
        let got = store.get(DBCol::ReceiptToTx, k).expect("entry should be readable after ingest");
        assert_eq!(&*got, v.as_slice());
    }
}

#[test]
fn non_sorted_entries_return_error() {
    let (_db_tmp, sst_tmp, store) = fresh_rocksdb_store();
    let entries: Vec<(Vec<u8>, Vec<u8>)> =
        vec![(vec![2; 32], b"a".to_vec()), (vec![1; 32], b"b".to_vec())];
    let err = store
        .ingest_insert_only_sst(DBCol::ReceiptToTx, &entries, sst_tmp.path())
        .expect_err("unsorted input must fail");
    let msg = format!("{err:#}");
    assert!(msg.contains("not sorted"), "error should mention sort order, got: {msg}");

    let leftover = std::fs::read_dir(sst_tmp.path()).map(|d| d.count()).unwrap_or(0);
    assert_eq!(leftover, 0, "no SST file should persist when entries are unsorted");
}

#[test]
fn duplicate_keys_same_value_return_error() {
    let (_db_tmp, sst_tmp, store) = fresh_rocksdb_store();
    let entries: Vec<(Vec<u8>, Vec<u8>)> =
        vec![(vec![1; 32], b"v".to_vec()), (vec![1; 32], b"v".to_vec())];
    let err = store
        .ingest_insert_only_sst(DBCol::ReceiptToTx, &entries, sst_tmp.path())
        .expect_err("duplicate keys must fail");
    let msg = format!("{err:#}");
    assert!(msg.contains("duplicate key"), "error should mention duplicate, got: {msg}");
}

#[test]
fn duplicate_keys_different_values_return_same_error() {
    let (_db_tmp, sst_tmp, store) = fresh_rocksdb_store();
    // The helper does not distinguish duplicate-with-different-values from
    // duplicate-with-same-values; dedupe with value-equality is the caller's job.
    let entries: Vec<(Vec<u8>, Vec<u8>)> =
        vec![(vec![1; 32], b"a".to_vec()), (vec![1; 32], b"b".to_vec())];
    let err = store
        .ingest_insert_only_sst(DBCol::ReceiptToTx, &entries, sst_tmp.path())
        .expect_err("duplicate keys must fail regardless of values");
    let msg = format!("{err:#}");
    assert!(msg.contains("duplicate key"), "error should mention duplicate, got: {msg}");
}

#[test]
fn entries_exceeding_size_cap_return_error_and_remove_file() {
    let (_db_tmp, sst_tmp, store) = fresh_rocksdb_store();
    // Build entries with genuinely random bytes so LZ4 cannot compress them
    // below the 256 MB hard cap. Using a seeded ChaCha rng keeps the test
    // deterministic; ChaCha output is statistically indistinguishable from
    // random and therefore incompressible in practice.
    let value_size: usize = 256 * 1024;
    let total_target: usize = 320 * 1024 * 1024;
    let count = total_target / value_size;
    let mut rng = ChaCha8Rng::seed_from_u64(0xDEAD_BEEF_C0DE_F00D);
    let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(count);
    for i in 0..count {
        let mut key = vec![0u8; 32];
        key[..8].copy_from_slice(&(i as u64).to_be_bytes());
        let mut value = vec![0u8; value_size];
        rng.fill_bytes(&mut value);
        entries.push((key, value));
    }

    let err = store
        .ingest_insert_only_sst(DBCol::ReceiptToTx, &entries, sst_tmp.path())
        .expect_err("oversized batch should fail");
    let msg = format!("{err:#}");
    assert!(msg.contains("exceeds cap"), "error should mention size cap, got: {msg}");

    let mut leftover_sst = std::fs::read_dir(sst_tmp.path())
        .expect("read sst tempdir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|x| x == "sst"));
    assert!(leftover_sst.next().is_none(), "oversized SST file should not persist");
}

#[test]
#[should_panic(expected = "ingest_insert_only_sst requires insert-only column")]
fn non_insert_only_column_panics() {
    let (_db_tmp, sst_tmp, store) = fresh_rocksdb_store();
    let entries: Vec<(Vec<u8>, Vec<u8>)> = vec![(vec![1; 8], b"v".to_vec())];
    // BlockMisc is `set`-able; not an insert-only column.
    let _ = store.ingest_insert_only_sst(DBCol::BlockMisc, &entries, sst_tmp.path());
}
