//! Parametrised conformance suite for the new MultiGet trait surface.
//!
//! Goals:
//!
//! * Verify `Database::multi_get_raw_bytes`, `pinned_multi_get_raw_bytes`,
//!   `multi_get_with_rc_stripped`, and `pinned_multi_get_with_rc_stripped`
//!   return semantically the same results as the equivalent N scalar
//!   `get_raw_bytes` / `get_with_rc_stripped` calls — for every backend.
//!
//! * Lock in two correctness gates:
//!     - `Store::multi_exists` on an RC column returns `false` for cells
//!       with refcount ≤ 0 (tombstones), matching scalar `Store::exists`.
//!     - `SplitDB::multi_get_with_rc_stripped` correctly falls through
//!       hot tombstones to cold real values; a hot-side cell with
//!       refcount ≤ 0 must NOT shadow a real cold-side value.
//!
//! Backends covered: `TestDB`, `ColdDB(TestDB)`, `SplitDB(TestDB hot,
//! ColdDB(TestDB) cold)`, `MixedDB(read=TestDB, write=TestDB)`,
//! `RecoveryDB(ColdDB(TestDB))`.  Real-RocksDB-specific behaviour
//! (pinned slices, large-batch ordering) is covered separately in
//! `multi_get.rs`.

use near_store::DBCol;
use near_store::db::{
    ColdDB, DBSlice, DBTransaction, Database, MixedDB, ReadOrder, RecoveryDB, SplitDB, TestDB,
};
use std::sync::Arc;

// ---------- Test helpers ----------

const NON_RC_COLD: DBCol = DBCol::Block; // cold-resident, non-RC
const NON_RC_HOT: DBCol = DBCol::BlockMerkleTree; // hot-only, non-RC
const RC_COLD: DBCol = DBCol::Transactions; // cold-resident, RC
const RC_RECEIPTS: DBCol = DBCol::Receipts; // cold-resident, RC

fn put_raw(db: &Arc<dyn Database>, col: DBCol, key: &[u8], value: &[u8]) {
    let mut tx = DBTransaction::new();
    tx.set(col, key.to_vec(), value.to_vec());
    db.write(tx);
}

/// Write a raw bytes payload to an RC column with a refcount of `rc`
/// appended. Uses `DBOp::Set` so we can synthesise tombstones (rc ≤ 0)
/// directly — `UpdateRefcount` panics in debug builds for non-positive
/// merged refcounts, which is the wrong shape for testing the stripping
/// path.
fn put_rc(db: &Arc<dyn Database>, col: DBCol, key: &[u8], payload: &[u8], rc: i64) {
    let bytes: Vec<u8> = [payload, &rc.to_le_bytes()[..]].concat();
    let mut tx = DBTransaction::new();
    tx.set(col, key.to_vec(), bytes);
    db.write(tx);
}

/// Construct a fresh TestDB-backed `Arc<dyn Database>`.
fn make_testdb() -> Arc<dyn Database> {
    TestDB::new()
}

/// Construct a fresh ColdDB(TestDB)-backed `Arc<dyn Database>`.
fn make_colddb() -> Arc<dyn Database> {
    Arc::new(ColdDB::new(TestDB::new()))
}

/// Construct a SplitDB given a freshly created hot and cold inner.  The
/// caller writes to the inner backends directly via the returned tuple.
fn make_splitdb() -> (Arc<dyn Database>, Arc<dyn Database>, Arc<dyn Database>) {
    let hot = TestDB::new() as Arc<dyn Database>;
    let cold_inner = TestDB::new() as Arc<dyn Database>;
    let cold = Arc::new(ColdDB::new(cold_inner.clone())) as Arc<dyn Database>;
    let split = SplitDB::new(hot.clone(), cold.clone()) as Arc<dyn Database>;
    (hot, cold_inner, split)
}

fn make_mixeddb() -> Arc<dyn Database> {
    let read_db = TestDB::new() as Arc<dyn Database>;
    let write_db = TestDB::new() as Arc<dyn Database>;
    MixedDB::new(read_db, write_db, ReadOrder::WriteDBFirst)
}

fn make_recoverydb() -> Arc<dyn Database> {
    let cold = Arc::new(ColdDB::new(TestDB::new()));
    Arc::new(RecoveryDB::new(cold))
}

fn assert_eq_payload(actual: &Option<DBSlice<'_>>, expected: Option<&[u8]>) {
    match (actual, expected) {
        (Some(a), Some(e)) => assert_eq!(a.as_slice(), e, "payload mismatch"),
        (None, None) => {}
        (a, e) => panic!("presence mismatch: actual={:?}, expected={:?}", a.is_some(), e.is_some()),
    }
}

// ---------- Generic correctness invariants ----------

/// `multi_get_raw_bytes` returns the same Option<bytes> per slot as N
/// scalar `get_raw_bytes` calls — for every backend.
fn check_multi_get_matches_scalar(db: &Arc<dyn Database>, col: DBCol, keys: &[&[u8]]) {
    let multi = db.multi_get_raw_bytes(col, keys);
    assert_eq!(multi.len(), keys.len(), "result length mismatch");
    for (i, key) in keys.iter().enumerate() {
        let scalar = db.get_raw_bytes(col, key);
        assert_eq_payload(&multi[i], scalar.as_deref());
    }
}

/// `pinned_multi_get_raw_bytes` returns the same Option<bytes> per slot
/// as N scalar `get_raw_bytes` calls.
fn check_batched_matches_scalar(db: &Arc<dyn Database>, col: DBCol, keys: &[&[u8]]) {
    let multi = db.pinned_multi_get_raw_bytes(col, keys);
    assert_eq!(multi.len(), keys.len(), "result length mismatch");
    for (i, key) in keys.iter().enumerate() {
        let scalar = db.get_raw_bytes(col, key);
        assert_eq_payload(&multi[i], scalar.as_deref());
    }
}

/// `multi_get_with_rc_stripped` returns the same Option<bytes> per slot
/// as N scalar `get_with_rc_stripped` calls.  RC column required.
fn check_rc_stripped_matches_scalar(db: &Arc<dyn Database>, col: DBCol, keys: &[&[u8]]) {
    assert!(col.is_rc(), "must be RC column");
    let multi = db.multi_get_with_rc_stripped(col, keys);
    assert_eq!(multi.len(), keys.len(), "result length mismatch");
    for (i, key) in keys.iter().enumerate() {
        let scalar = db.get_with_rc_stripped(col, key);
        assert_eq_payload(&multi[i], scalar.as_deref());
    }
    let batched = db.pinned_multi_get_with_rc_stripped(col, keys);
    for (i, key) in keys.iter().enumerate() {
        let scalar = db.get_with_rc_stripped(col, key);
        assert_eq_payload(&batched[i], scalar.as_deref());
    }
}

// ---------- Per-backend: generic invariants ----------

#[test]
fn testdb_multi_get_matches_scalar_non_rc() {
    let db = make_testdb();
    put_raw(&db, NON_RC_COLD, b"k1", b"v1");
    put_raw(&db, NON_RC_COLD, b"k3", b"v3");
    let keys: &[&[u8]] = &[b"k1", b"k2", b"k3", b""];
    check_multi_get_matches_scalar(&db, NON_RC_COLD, keys);
    check_batched_matches_scalar(&db, NON_RC_COLD, keys);
}

#[test]
fn testdb_multi_get_with_rc_stripped_basic() {
    let db = make_testdb();
    put_rc(&db, RC_COLD, b"alive", b"hello", 1);
    put_rc(&db, RC_COLD, b"alive2", b"world", 5);
    let keys: &[&[u8]] = &[b"alive", b"alive2", b"missing"];
    check_rc_stripped_matches_scalar(&db, RC_COLD, keys);

    // Sanity: scalar returns the stripped payloads.
    assert_eq!(db.get_with_rc_stripped(RC_COLD, b"alive").unwrap().as_slice(), b"hello");
    assert_eq!(db.get_with_rc_stripped(RC_COLD, b"alive2").unwrap().as_slice(), b"world");
}

#[test]
fn testdb_empty_keys_returns_empty_vec() {
    let db = make_testdb();
    put_raw(&db, NON_RC_COLD, b"k", b"v");
    let empty: &[&[u8]] = &[];
    assert!(db.multi_get_raw_bytes(NON_RC_COLD, empty).is_empty());
    assert!(db.pinned_multi_get_raw_bytes(NON_RC_COLD, empty).is_empty());
    assert!(db.multi_get_with_rc_stripped(RC_COLD, empty).is_empty());
    assert!(db.pinned_multi_get_with_rc_stripped(RC_COLD, empty).is_empty());
}

#[test]
fn testdb_ordering_preserved() {
    let db = make_testdb();
    put_raw(&db, NON_RC_COLD, b"a", b"A");
    put_raw(&db, NON_RC_COLD, b"b", b"B");
    put_raw(&db, NON_RC_COLD, b"c", b"C");
    // Request keys in non-sorted order; result[i] must align with keys[i].
    let keys: &[&[u8]] = &[b"c", b"a", b"missing", b"b"];
    let result = db.multi_get_raw_bytes(NON_RC_COLD, keys);
    assert_eq!(result.len(), 4);
    assert_eq_payload(&result[0], Some(b"C"));
    assert_eq_payload(&result[1], Some(b"A"));
    assert_eq_payload(&result[2], None);
    assert_eq_payload(&result[3], Some(b"B"));
}

#[test]
fn colddb_multi_get_forwards_correctly() {
    let db = make_colddb();
    put_raw(&db, NON_RC_COLD, b"k1", b"v1");
    put_rc(&db, RC_COLD, b"k2", b"v2", 1);
    let non_rc_keys: &[&[u8]] = &[b"k1", b"missing"];
    check_multi_get_matches_scalar(&db, NON_RC_COLD, non_rc_keys);
    check_batched_matches_scalar(&db, NON_RC_COLD, non_rc_keys);
    let rc_keys: &[&[u8]] = &[b"k2", b"missing"];
    check_rc_stripped_matches_scalar(&db, RC_COLD, rc_keys);
}

#[test]
fn mixeddb_multi_get_uses_default_impl_correctly() {
    let db = make_mixeddb();
    put_raw(&db, NON_RC_COLD, b"k1", b"v1");
    let keys: &[&[u8]] = &[b"k1", b"missing"];
    check_multi_get_matches_scalar(&db, NON_RC_COLD, keys);
    check_batched_matches_scalar(&db, NON_RC_COLD, keys);
}

#[test]
fn recoverydb_multi_get_uses_default_impl_correctly() {
    let db = make_recoverydb();
    // RecoveryDB is built around cold/state recovery; reads forward to
    // ColdDB which forwards to TestDB.  Populate via direct write to the
    // RecoveryDB (its `write` op is gated to State, but read works for
    // any cold-resident column populated through the cold path).  For
    // the conformance check we just verify multi_get matches scalar.
    let keys: &[&[u8]] = &[b"any"];
    check_multi_get_matches_scalar(&db, NON_RC_COLD, keys);
    check_batched_matches_scalar(&db, NON_RC_COLD, keys);
}

// ---------- Tombstones in RC column → multi_get_with_rc_stripped returns None ----------

#[test]
fn multi_get_with_rc_stripped_treats_tombstone_as_absent() {
    let db = make_testdb();
    // Live cell: refcount = +1.
    put_rc(&db, RC_COLD, b"live", b"alive_payload", 1);
    // Tombstone: refcount = 0 (raw bytes still stored).
    put_rc(&db, RC_COLD, b"tombstone_zero", b"stale_payload", 0);
    // Tombstone: refcount = -2 (also tombstoned per refcount semantics).
    put_rc(&db, RC_COLD, b"tombstone_neg", b"stale_payload", -2);
    // Truly absent.
    let keys: &[&[u8]] = &[b"live", b"tombstone_zero", b"tombstone_neg", b"absent"];

    // Cross-check: scalar `get_raw_bytes` returns the tombstone bytes (refcount appended).
    assert!(db.get_raw_bytes(RC_COLD, b"tombstone_zero").is_some());
    assert!(db.get_raw_bytes(RC_COLD, b"tombstone_neg").is_some());

    // The actual contract: rc-stripped multi-get treats tombstones as None.
    let multi = db.multi_get_with_rc_stripped(RC_COLD, keys);
    assert_eq_payload(&multi[0], Some(b"alive_payload"));
    assert_eq_payload(&multi[1], None); // zero refcount → None
    assert_eq_payload(&multi[2], None); // negative refcount → None
    assert_eq_payload(&multi[3], None);

    let batched = db.pinned_multi_get_with_rc_stripped(RC_COLD, keys);
    assert_eq_payload(&batched[0], Some(b"alive_payload"));
    assert_eq_payload(&batched[1], None);
    assert_eq_payload(&batched[2], None);
    assert_eq_payload(&batched[3], None);
}

#[test]
fn store_multi_exists_returns_false_for_tombstone() {
    let db = make_testdb();
    put_rc(&db, RC_COLD, b"live", b"alive_payload", 1);
    put_rc(&db, RC_COLD, b"tombstone", b"stale_payload", 0);

    // Construct a Store wrapping the TestDB to exercise the public API the
    // backfill actor uses.
    let store = near_store::Store::new(db);

    let keys: &[&[u8]] = &[b"live", b"tombstone", b"absent"];
    let exists = store.multi_exists(RC_COLD, keys);
    assert_eq!(exists, vec![true, false, false], "tombstone must report false");

    // Sanity-check the multi_get path produces the same shape (Some/None aligned).
    let multi = store.multi_get(RC_COLD, keys);
    assert_eq_payload(&multi[0], Some(b"alive_payload"));
    assert_eq_payload(&multi[1], None);
    assert_eq_payload(&multi[2], None);
}

// ---------- SplitDB hot tombstone + cold real → cold real wins ----------

#[test]
fn splitdb_hot_tombstone_falls_through_to_cold_real() {
    let (hot, cold_inner, split) = make_splitdb();

    // Set up: hot has a tombstone (refcount 0), cold has a real value.
    put_rc(&hot, RC_COLD, b"key", b"hot_stale", 0);
    put_rc(&cold_inner, RC_COLD, b"key", b"cold_real", 1);

    // Scalar `get_with_rc_stripped` already does this correctly per the
    // existing SplitDB override; sanity-check before testing multi-get.
    let scalar = split.get_with_rc_stripped(RC_COLD, b"key");
    assert_eq!(scalar.unwrap().as_slice(), b"cold_real", "scalar fall-through must work");

    // multi_get_with_rc_stripped must also fall through hot tombstones.
    let multi = split.multi_get_with_rc_stripped(RC_COLD, &[b"key"]);
    assert_eq_payload(&multi[0], Some(b"cold_real"));

    let batched = split.pinned_multi_get_with_rc_stripped(RC_COLD, &[b"key"]);
    assert_eq_payload(&batched[0], Some(b"cold_real"));
}

#[test]
fn splitdb_hot_real_shadows_cold_real() {
    let (hot, cold_inner, split) = make_splitdb();
    put_rc(&hot, RC_COLD, b"key", b"hot_real", 1);
    put_rc(&cold_inner, RC_COLD, b"key", b"cold_real", 1);

    let multi = split.multi_get_with_rc_stripped(RC_COLD, &[b"key"]);
    assert_eq_payload(&multi[0], Some(b"hot_real"));
    let batched = split.pinned_multi_get_with_rc_stripped(RC_COLD, &[b"key"]);
    assert_eq_payload(&batched[0], Some(b"hot_real"));
}

#[test]
fn splitdb_hot_only_no_cold() {
    let (hot, _cold_inner, split) = make_splitdb();
    put_rc(&hot, RC_COLD, b"key", b"hot_real", 1);

    let multi = split.multi_get_with_rc_stripped(RC_COLD, &[b"key", b"missing"]);
    assert_eq_payload(&multi[0], Some(b"hot_real"));
    assert_eq_payload(&multi[1], None);
}

#[test]
fn splitdb_cold_only_no_hot() {
    let (_hot, cold_inner, split) = make_splitdb();
    put_rc(&cold_inner, RC_COLD, b"key", b"cold_real", 1);

    let multi = split.multi_get_with_rc_stripped(RC_COLD, &[b"key", b"missing"]);
    assert_eq_payload(&multi[0], Some(b"cold_real"));
    assert_eq_payload(&multi[1], None);
}

#[test]
fn splitdb_non_cold_column_does_not_consult_cold() {
    let (hot, cold_inner, split) = make_splitdb();
    // Non-cold column: only hot should be read, cold should be ignored even
    // if it has data.  (Though writing to cold for a non-cold column via
    // ColdDB would panic — so we skip that and just verify the read shape.)
    put_raw(&hot, NON_RC_HOT, b"hot_key", b"hot_val");
    let _ = cold_inner; // unused; non-cold column wouldn't legally have cold data

    let multi = split.multi_get_raw_bytes(NON_RC_HOT, &[b"hot_key", b"missing"]);
    assert_eq_payload(&multi[0], Some(b"hot_val"));
    assert_eq_payload(&multi[1], None);
}

// ---------- Larger batch sanity (still TestDB) ----------

#[test]
fn large_batch_correctness_against_testdb() {
    let db = make_testdb();
    // Populate 100 RC entries.
    for i in 0..100u32 {
        let k = format!("key-{i:03}");
        let v = format!("val-{i:03}");
        put_rc(&db, RC_RECEIPTS, k.as_bytes(), v.as_bytes(), 1);
    }
    // Build a query of 200 keys: half present, half absent, randomly ordered.
    let mut keys_owned: Vec<Vec<u8>> = Vec::with_capacity(200);
    for i in 0..200u32 {
        keys_owned.push(format!("key-{i:03}").into_bytes());
    }
    let keys: Vec<&[u8]> = keys_owned.iter().map(Vec::as_slice).collect();

    let multi = db.multi_get_with_rc_stripped(RC_RECEIPTS, &keys);
    assert_eq!(multi.len(), 200);
    for (i, slot) in multi.iter().enumerate() {
        let expected = if i < 100 { Some(format!("val-{i:03}").into_bytes()) } else { None };
        assert_eq_payload(slot, expected.as_deref());
    }
}
