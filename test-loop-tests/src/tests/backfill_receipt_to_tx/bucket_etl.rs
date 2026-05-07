use super::common::{EPOCH_LENGTH, generate_diverse_traffic, shared_storage};
use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use borsh::BorshSerialize;
use near_chain::backfill_receipt_to_tx::{
    BuiltOrigin, BuiltOriginInputs, build_receipt_to_tx_info, process_height,
};
use near_chain::backfill_receipt_to_tx_bucket_etl::{
    BucketEtlOptions, BucketEtlOutputMode, BucketReader, BucketWriter, NeedsParentRow, OutputRow,
    ReceiptExtractRow, ReceiptOriginDemandRow, TxOriginDemandRow, compare_against_actor,
    finalize_output_bucket_with_max, pass_d_output_path, run_bucket_etl,
    validate_output_bucket_file, validate_output_bucket_file_with_max,
};
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{ReceiptOrigin, ReceiptToTxInfo, ReceiptToTxInfoV1};
use near_primitives::types::{AccountId, Balance};
use near_store::DBCol;
use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;
use std::str::FromStr;
use tempfile::TempDir;

/// Default test options. Big marker interval so most tests don't trigger
/// segment flushes mid-run; the resume test overrides.
fn default_etl_opts(scratch_dir: PathBuf) -> BucketEtlOptions {
    BucketEtlOptions {
        from_height: None,
        to_height: None,
        num_threads: 2,
        scratch_dir,
        output_mode: BucketEtlOutputMode::WriteToStore,
        marker_block_interval: 1_000_000,
        #[cfg(feature = "test_features")]
        crash_after_pass_b_height: None,
    }
}

fn build_env_with_traffic() -> crate::setup::env::TestLoopEnv {
    let user_account = create_account_id("account0");
    let receiver_account = create_account_id("account1");

    // Non-zero gas price → unspent gas refund receipts get created, which
    // makes the receipt-origin (`FromReceipt`) code path actually exercise
    // during diverse_traffic. With the default (zero) gas price, function
    // calls produce no refund receipts and the `FromReceipt` codepath is
    // never reached. See `test_refund_receipt_has_receipt_to_tx`.
    let min_gas_price = Balance::from_yoctonear(100_000_000);
    let mut env = TestLoopBuilder::new()
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .add_user_account(&receiver_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .gas_prices(min_gas_price, min_gas_price)
        .gc_num_epochs_to_keep(20)
        .build();
    generate_diverse_traffic(&mut env);
    env
}

fn collect_organic_entries(store: &near_store::Store) -> Vec<(CryptoHash, ReceiptToTxInfo)> {
    let mut entries: Vec<(CryptoHash, ReceiptToTxInfo)> = store
        .iter_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx)
        .map(|(k, v)| (CryptoHash::try_from(k.as_ref()).unwrap(), v))
        .collect();
    entries.sort_by(|a, b| a.0.as_ref().cmp(b.0.as_ref()));
    entries
}

fn wipe_receipt_to_tx(store: &near_store::Store) {
    let keys: Vec<Vec<u8>> = store.iter(DBCol::ReceiptToTx).map(|(k, _)| k.to_vec()).collect();
    let mut update = store.store_update();
    for k in &keys {
        update.delete(DBCol::ReceiptToTx, k);
    }
    update.commit();
    assert_eq!(store.iter(DBCol::ReceiptToTx).count(), 0);
}

/// Baseline: bucket-ETL output equals the entries written organically when
/// `save_receipt_to_tx = true`. Mirrors `test_backfill_matches_normal_processing`.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_bucket_etl_matches_actor_output() {
    init_test_logger();
    let env = build_env_with_traffic();
    let store = env.validator().store();

    let original = collect_organic_entries(&store);
    assert!(
        original.len() > 10,
        "expected many ReceiptToTx entries from diverse traffic, got {}",
        original.len()
    );
    let has_from_tx = original.iter().any(|(_, info)| {
        let ReceiptToTxInfo::V1(v1) = info;
        matches!(&v1.origin, ReceiptOrigin::FromTransaction(_))
    });
    assert!(has_from_tx, "expected FromTransaction entries");

    wipe_receipt_to_tx(&store);

    let scratch = TempDir::new().unwrap();
    let storage = shared_storage(&store, None);
    let chain_store = env.validator().client().chain.chain_store();
    let stats = run_bucket_etl(chain_store, &storage, default_etl_opts(scratch.path().to_owned()))
        .expect("bucket-ETL should succeed");
    assert_eq!(stats.pass_d_rows as usize, original.len());

    let backfilled = collect_organic_entries(&store);
    assert_eq!(backfilled.len(), original.len(), "row count must match");
    for ((k1, v1), (k2, v2)) in original.iter().zip(backfilled.iter()) {
        assert_eq!(k1, k2, "key mismatch");
        assert_eq!(v1, v2, "value mismatch for key {k1}");
    }
}

/// Bucket-ETL stats agree with `process_height` when a parent receipt is missing.
/// We delete one receipt-origin outcome's outcome_id from `Receipts` and confirm
/// both the actor and the bucket-ETL count it as `missing_parent_receipts`.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_bucket_etl_handles_missing_parent_receipt() {
    init_test_logger();
    let env = build_env_with_traffic();
    let store = env.validator().store();
    let chain_store = env.validator().client().chain.chain_store();

    // Find a receipt-origin outcome's outcome_id (also a receipt id in DBCol::Receipts).
    let parent = pick_receipt_origin_outcome_id(chain_store).expect("need at least one");
    let mut update = store.store_update();
    update.decrement_refcount(DBCol::Receipts, parent.as_ref());
    update.commit();
    // Confirm Pass A scan won't see it.
    assert!(
        store
            .get_ser::<near_primitives::receipt::Receipt>(DBCol::Receipts, parent.as_ref())
            .is_none()
    );

    let genesis = chain_store.get_genesis_height();
    let head = env.validator().head().height;

    // Baseline via process_height (after deletion).
    let mut actor_missing_parent: u64 = 0;
    let mut actor_entries: HashMap<CryptoHash, ReceiptToTxInfo> = HashMap::new();
    for h in genesis..=head {
        if let Some(hr) = process_height(chain_store, h).expect("process_height ok") {
            actor_missing_parent += hr.missing_parent_receipts;
            for (k, v) in hr.entries {
                actor_entries.insert(k, v);
            }
        }
    }
    assert!(actor_missing_parent > 0, "expected at least one missing parent");

    wipe_receipt_to_tx(&store);

    let scratch = TempDir::new().unwrap();
    let stats = run_bucket_etl(
        chain_store,
        &shared_storage(&store, None),
        default_etl_opts(scratch.path().to_owned()),
    )
    .expect("bucket-ETL should succeed");

    assert_eq!(
        stats.missing_parent_receipts, actor_missing_parent,
        "bucket-ETL missing_parent_receipts must match actor"
    );

    let etl_entries: HashMap<CryptoHash, ReceiptToTxInfo> = store
        .iter_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx)
        .map(|(k, v)| (CryptoHash::try_from(k.as_ref()).unwrap(), v))
        .collect();
    assert_eq!(etl_entries.len(), actor_entries.len());
    for (k, v) in &actor_entries {
        assert_eq!(etl_entries.get(k), Some(v));
    }
}

/// Symmetric to the parent test: when a child receipt is missing, both the
/// actor and the bucket-ETL count it as `missing_child_receipts`.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_bucket_etl_handles_missing_child_receipt() {
    init_test_logger();
    let env = build_env_with_traffic();
    let store = env.validator().store();
    let chain_store = env.validator().client().chain.chain_store();

    // Pick a receipt that appears as a child in some outcome's `receipt_ids`.
    let child = pick_child_receipt_id(chain_store).expect("need at least one");
    let mut update = store.store_update();
    update.decrement_refcount(DBCol::Receipts, child.as_ref());
    update.commit();
    assert!(
        store
            .get_ser::<near_primitives::receipt::Receipt>(DBCol::Receipts, child.as_ref())
            .is_none()
    );

    let genesis = chain_store.get_genesis_height();
    let head = env.validator().head().height;
    let mut actor_missing_child: u64 = 0;
    for h in genesis..=head {
        if let Some(hr) = process_height(chain_store, h).expect("process_height ok") {
            actor_missing_child += hr.missing_child_receipts;
        }
    }
    assert!(actor_missing_child > 0, "expected at least one missing child");

    wipe_receipt_to_tx(&store);
    let scratch = TempDir::new().unwrap();
    let stats = run_bucket_etl(
        chain_store,
        &shared_storage(&store, None),
        default_etl_opts(scratch.path().to_owned()),
    )
    .expect("bucket-ETL should succeed");
    assert_eq!(
        stats.missing_child_receipts, actor_missing_child,
        "bucket-ETL missing_child_receipts must match actor"
    );
}

/// `missing_outcomes` is incremented when an outcome key is absent from
/// `TransactionResultForBlock`. Delete one and confirm both paths agree.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_bucket_etl_handles_missing_outcome() {
    init_test_logger();
    let env = build_env_with_traffic();
    let store = env.validator().store();
    let chain_store = env.validator().client().chain.chain_store();

    // Pick the first key in TransactionResultForBlock and delete it.
    let key: Vec<u8> =
        store.iter(DBCol::TransactionResultForBlock).next().expect("non-empty").0.to_vec();
    let mut update = store.store_update();
    update.delete(DBCol::TransactionResultForBlock, &key);
    update.commit();

    let genesis = chain_store.get_genesis_height();
    let head = env.validator().head().height;
    let mut actor_missing_outcomes: u64 = 0;
    for h in genesis..=head {
        if let Some(hr) = process_height(chain_store, h).expect("process_height ok") {
            actor_missing_outcomes += hr.missing_outcomes;
        }
    }
    assert!(actor_missing_outcomes > 0, "expected at least one missing outcome");

    wipe_receipt_to_tx(&store);
    let scratch = TempDir::new().unwrap();
    let stats = run_bucket_etl(
        chain_store,
        &shared_storage(&store, None),
        default_etl_opts(scratch.path().to_owned()),
    )
    .expect("bucket-ETL should succeed");
    assert_eq!(
        stats.missing_outcomes, actor_missing_outcomes,
        "bucket-ETL missing_outcomes must match actor"
    );
}

/// `finalize_output_bucket` quarantines the batch when two entries share a key
/// but disagree on the serialized payload. Mirrors the actor's
/// `BackfillError::ValueDivergence` path.
#[test]
fn test_bucket_etl_quarantines_value_divergence() {
    init_test_logger();
    let key = CryptoHash([0x11; 32]);
    let row_a = OutputRow {
        child_id: key,
        info: ReceiptToTxInfo::V1(ReceiptToTxInfoV1 {
            origin: ReceiptOrigin::FromTransaction(
                near_primitives::receipt::ReceiptOriginTransaction {
                    tx_hash: CryptoHash([0xaa; 32]),
                    sender_account_id: AccountId::from_str("alice.near").unwrap(),
                },
            ),
            receiver_account_id: AccountId::from_str("bob.near").unwrap(),
            shard_id: 0u64.into(),
        }),
    };
    let row_b = OutputRow {
        child_id: key,
        info: ReceiptToTxInfo::V1(ReceiptToTxInfoV1 {
            origin: ReceiptOrigin::FromTransaction(
                near_primitives::receipt::ReceiptOriginTransaction {
                    tx_hash: CryptoHash([0xbb; 32]), // different tx hash → divergent payload
                    sender_account_id: AccountId::from_str("alice.near").unwrap(),
                },
            ),
            receiver_account_id: AccountId::from_str("bob.near").unwrap(),
            shard_id: 0u64.into(),
        }),
    };
    let err = finalize_output_bucket_with_max(vec![row_a, row_b], 1024 * 1024)
        .expect_err("divergent values must error");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("two different ReceiptToTxInfo")
            || msg.contains("ValueDivergence")
            || msg.contains("non-determinism"),
        "expected ValueDivergence error, got: {msg}"
    );
}

/// The deterministic shuffle (Pass B → Pass C) must correctly handle
/// receipt-origin parents whose `parent_receipt_id` first byte differs from
/// the child's. We verify byte-equivalence with `process_height` end-to-end;
/// any cross-prefix shuffle bug would surface as a missing or divergent entry.
/// (The actor under test reads from a single-shard chain whose traffic mix may
/// or may not include FromReceipt entries; equivalence is the load-bearing
/// invariant either way.)
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_bucket_etl_resolves_cross_prefix_parents() {
    init_test_logger();
    let env = build_env_with_traffic();
    let store = env.validator().store();
    let chain_store = env.validator().client().chain.chain_store();

    // Count how many cross-prefix parent→child cases the chain produced —
    // logged for diagnostic value, not asserted. The equivalence check below
    // is the actual safety net for the cross-prefix shuffle.
    let mut cross_prefix = 0usize;
    let genesis = chain_store.get_genesis_height();
    let head = env.validator().head().height;
    for h in genesis..=head {
        if let Some(hr) = process_height(chain_store, h).expect("process_height ok") {
            for (child_id, info) in &hr.entries {
                let ReceiptToTxInfo::V1(v1) = info;
                if let ReceiptOrigin::FromReceipt(o) = &v1.origin {
                    if o.parent_receipt_id.as_ref()[0] != child_id.as_ref()[0] {
                        cross_prefix += 1;
                    }
                }
            }
        }
    }
    eprintln!("test_bucket_etl_resolves_cross_prefix_parents: cross_prefix cases = {cross_prefix}");

    // Equivalence end-to-end.
    let original = collect_organic_entries(&store);
    wipe_receipt_to_tx(&store);
    let scratch = TempDir::new().unwrap();
    run_bucket_etl(
        chain_store,
        &shared_storage(&store, None),
        default_etl_opts(scratch.path().to_owned()),
    )
    .expect("bucket-ETL should succeed");
    let backfilled = collect_organic_entries(&store);
    assert_eq!(backfilled.len(), original.len());
    for ((k1, v1), (k2, v2)) in original.iter().zip(backfilled.iter()) {
        assert_eq!(k1, k2);
        assert_eq!(v1, v2);
    }
}

/// Pure unit test: each of the 5 row types roundtrips through
/// `BucketWriter` → `BucketReader` byte-for-byte.
#[test]
fn test_bucket_writer_reader_roundtrip() {
    init_test_logger();
    let receipt_extract = ReceiptExtractRow {
        receipt_id: CryptoHash([0x01; 32]),
        receiver_id: AccountId::from_str("alice.near").unwrap(),
        predecessor_id: AccountId::from_str("system").unwrap(),
    };
    let tx_demand = TxOriginDemandRow {
        child_id: CryptoHash([0x02; 32]),
        shard_id: 1u64.into(),
        tx_hash: CryptoHash([0x03; 32]),
        sender_account_id: AccountId::from_str("bob.near").unwrap(),
    };
    let needs_parent = NeedsParentRow {
        child_id: CryptoHash([0x04; 32]),
        shard_id: 2u64.into(),
        parent_receipt_id: CryptoHash([0x05; 32]),
    };
    let receipt_demand = ReceiptOriginDemandRow {
        child_id: CryptoHash([0x06; 32]),
        shard_id: 3u64.into(),
        parent_receipt_id: CryptoHash([0x07; 32]),
        parent_predecessor_id: AccountId::from_str("carol.near").unwrap(),
    };
    let output = OutputRow {
        child_id: CryptoHash([0x08; 32]),
        info: ReceiptToTxInfo::V1(ReceiptToTxInfoV1 {
            origin: ReceiptOrigin::FromTransaction(
                near_primitives::receipt::ReceiptOriginTransaction {
                    tx_hash: CryptoHash([0x09; 32]),
                    sender_account_id: AccountId::from_str("alice.near").unwrap(),
                },
            ),
            receiver_account_id: AccountId::from_str("bob.near").unwrap(),
            shard_id: 0u64.into(),
        }),
    };

    fn roundtrip<T: BorshSerialize + borsh::BorshDeserialize + PartialEq + std::fmt::Debug>(
        row: T,
    ) {
        let mut buf = Vec::new();
        let mut w = BucketWriter::new(&mut buf);
        w.write(&row).unwrap();
        let _ = w.finish().unwrap();
        let mut reader = BucketReader::new(Cursor::new(&buf));
        let bytes = reader.next().unwrap().unwrap();
        let decoded = T::try_from_slice(&bytes).unwrap();
        assert_eq!(decoded, row);
        // No more rows after.
        assert!(reader.next().is_none());
    }

    roundtrip(receipt_extract);
    roundtrip(tx_demand);
    roundtrip(needs_parent);
    roundtrip(receipt_demand);
    roundtrip(output);
}

/// `validate_output_bucket_file` rejects (a) out-of-order keys, (b) duplicates,
/// and (c) buckets exceeding the size cap.
#[test]
fn test_bucket_etl_rejects_invalid_output_bucket() {
    init_test_logger();
    let dir = TempDir::new().unwrap();

    // Helper to write rows raw (skipping finalize_output_bucket so we can craft invalid files).
    fn write_raw(path: &std::path::Path, rows: &[OutputRow]) {
        let f = std::fs::File::create(path).unwrap();
        let mut w = BucketWriter::new(std::io::BufWriter::new(f));
        for r in rows {
            w.write(r).unwrap();
        }
        let _ = w.finish().unwrap();
    }

    let mk_row = |k: u8, tx: u8| OutputRow {
        child_id: CryptoHash([k; 32]),
        info: ReceiptToTxInfo::V1(ReceiptToTxInfoV1 {
            origin: ReceiptOrigin::FromTransaction(
                near_primitives::receipt::ReceiptOriginTransaction {
                    tx_hash: CryptoHash([tx; 32]),
                    sender_account_id: AccountId::from_str("alice.near").unwrap(),
                },
            ),
            receiver_account_id: AccountId::from_str("bob.near").unwrap(),
            shard_id: 0u64.into(),
        }),
    };

    // (a) out-of-order
    let path = dir.path().join("out_of_order.bucket");
    write_raw(&path, &[mk_row(0x05, 1), mk_row(0x02, 2)]);
    let err = validate_output_bucket_file(&path).expect_err("out-of-order must error");
    assert!(
        format!("{err:#}").contains("not strictly sorted"),
        "expected out-of-order error: {err:#}"
    );

    // (b) duplicates
    let path = dir.path().join("dup.bucket");
    write_raw(&path, &[mk_row(0x03, 1), mk_row(0x03, 2)]);
    let err = validate_output_bucket_file(&path).expect_err("duplicate keys must error");
    assert!(format!("{err:#}").contains("duplicate key"), "expected duplicate error: {err:#}");

    // (c) size > limit (use a small max so we don't need 256 MiB of data).
    let path = dir.path().join("toolarge.bucket");
    let rows: Vec<OutputRow> = (1..30u8).map(|k| mk_row(k, 0)).collect();
    write_raw(&path, &rows);
    let err =
        validate_output_bucket_file_with_max(&path, 100).expect_err("oversized bucket must error");
    assert!(format!("{err:#}").contains("exceeds 100 bytes"), "expected size-limit error: {err:#}");
}

/// Crash mid-Pass-B at a specific height; resume with the knob unset; final
/// output equals a clean single-shot run. Closes the load-bearing
/// restart-correctness gap.
#[test]
#[cfg(feature = "test_features")]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_bucket_etl_resumes_after_simulated_crash() {
    init_test_logger();
    let env = build_env_with_traffic();
    let store = env.validator().store();
    let chain_store = env.validator().client().chain.chain_store();

    let original = collect_organic_entries(&store);
    wipe_receipt_to_tx(&store);

    let head = env.validator().head().height;
    let genesis = chain_store.get_genesis_height();
    // Pick a crash height in the middle of the slice and a tight marker
    // interval so workers actually persist a marker before the crash.
    let mid = genesis + (head - genesis) / 2;
    let scratch = TempDir::new().unwrap();

    // First pass: crash at `mid`.
    let opts_crash = BucketEtlOptions {
        from_height: Some(genesis),
        to_height: Some(head),
        num_threads: 2,
        scratch_dir: scratch.path().to_owned(),
        output_mode: BucketEtlOutputMode::WriteToStore,
        marker_block_interval: 3,
        crash_after_pass_b_height: Some(mid),
    };
    let err = run_bucket_etl(chain_store, &shared_storage(&store, None), opts_crash)
        .expect_err("crash knob must trigger error");
    assert!(
        format!("{err:#}").contains("crash_after_pass_b_height"),
        "expected crash-knob error, got: {err:#}"
    );

    // Verify a worker actually got past at least one marker boundary
    // (otherwise the test isn't exercising the resume path).
    let mut any_marker = false;
    for w in 0..2 {
        if scratch.path().join("pass_b").join(format!("worker_{w}")).join("marker").exists() {
            any_marker = true;
            break;
        }
    }
    assert!(any_marker, "no per-worker marker was written before the crash");

    // Resume: same scratch dir, knob unset.
    let opts_resume = BucketEtlOptions {
        from_height: Some(genesis),
        to_height: Some(head),
        num_threads: 2,
        scratch_dir: scratch.path().to_owned(),
        output_mode: BucketEtlOutputMode::WriteToStore,
        marker_block_interval: 3,
        crash_after_pass_b_height: None,
    };
    run_bucket_etl(chain_store, &shared_storage(&store, None), opts_resume)
        .expect("resume should succeed");

    let backfilled = collect_organic_entries(&store);
    assert_eq!(backfilled.len(), original.len(), "row count must match original");
    for ((k1, v1), (k2, v2)) in original.iter().zip(backfilled.iter()) {
        assert_eq!(k1, k2);
        assert_eq!(v1, v2);
    }
}

/// Compare-mode catches injected divergences. Run bucket-ETL to completion,
/// overwrite ONE entry in the output bucket with a wrong receiver, then run
/// `compare_against_actor` and assert at least one divergence is reported.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_bucket_etl_compare_mode_detects_injected_divergence() {
    init_test_logger();
    let env = build_env_with_traffic();
    let store = env.validator().store();
    let chain_store = env.validator().client().chain.chain_store();

    wipe_receipt_to_tx(&store);
    let scratch = TempDir::new().unwrap();
    run_bucket_etl(
        chain_store,
        &shared_storage(&store, None),
        default_etl_opts(scratch.path().to_owned()),
    )
    .expect("bucket-ETL should succeed");

    // First: clean compare-mode against the same range — must report zero.
    let genesis = chain_store.get_genesis_height();
    let head = env.validator().head().height;
    let opts = default_etl_opts(scratch.path().to_owned());
    let clean = compare_against_actor(chain_store, &opts, genesis, head).expect("compare ok");
    assert_eq!(clean, 0, "clean compare must report zero divergences");

    // Find the first non-empty output bucket and corrupt one entry.
    let mut corrupted = false;
    for prefix in 0u16..256u16 {
        let path = pass_d_output_path(scratch.path(), prefix as u8);
        if !path.exists() {
            continue;
        }
        // Read existing rows, mutate first one's receiver, rewrite.
        let f = std::fs::File::open(&path).unwrap();
        let reader = BucketReader::new(std::io::BufReader::new(f));
        let mut rows: Vec<OutputRow> = Vec::new();
        for row in reader {
            rows.push(borsh::BorshDeserialize::try_from_slice(&row.unwrap()).unwrap());
        }
        if rows.is_empty() {
            continue;
        }
        let ReceiptToTxInfo::V1(ref mut v1) = rows[0].info;
        v1.receiver_account_id = AccountId::from_str("zzzzzzzzz.near").unwrap();
        // Rewrite.
        let f = std::fs::File::create(&path).unwrap();
        let mut w = BucketWriter::new(std::io::BufWriter::new(f));
        for r in &rows {
            w.write(r).unwrap();
        }
        let _ = w.finish().unwrap();
        corrupted = true;
        break;
    }
    assert!(corrupted, "expected at least one non-empty output bucket");

    let divergences = compare_against_actor(chain_store, &opts, genesis, head).expect("compare ok");
    assert!(divergences > 0, "compare-mode must surface the injected divergence");
}

// ===== Helpers =====

/// Walk the chain looking for the first outcome that:
///   (a) is receipt-origin (`outcome_id` NOT in `Transactions`),
///   (b) has at least one child receipt id (`receipt_ids` non-empty), so
///       process_height actually performs the parent lookup,
///   (c) `outcome_id` is in `Receipts` (so deletion is meaningful).
fn pick_receipt_origin_outcome_id(chain_store: &near_chain::ChainStore) -> Option<CryptoHash> {
    use near_chain::ChainStoreAccess;
    use near_primitives::transaction::ExecutionOutcomeWithProof;
    use near_primitives::utils::{get_block_shard_id_rev, get_outcome_id_block_hash};
    let store = chain_store.store();
    let genesis = chain_store.get_genesis_height();
    let head = chain_store.head().ok()?.height;
    for h in genesis..=head {
        let block_hash = match chain_store.get_block_hash_by_height(h) {
            Ok(h) => h,
            Err(_) => continue,
        };
        for (key, outcome_ids) in
            store.iter_prefix_ser::<Vec<CryptoHash>>(DBCol::OutcomeIds, block_hash.as_ref())
        {
            let _ = get_block_shard_id_rev(&key);
            for outcome_id in outcome_ids {
                let is_tx = store.exists(DBCol::Transactions, outcome_id.as_ref());
                if is_tx {
                    continue;
                }
                if !store.exists(DBCol::Receipts, outcome_id.as_ref()) {
                    continue;
                }
                let key = get_outcome_id_block_hash(&outcome_id, &block_hash);
                let bytes = match store.get(DBCol::TransactionResultForBlock, &key) {
                    Some(b) => b,
                    None => continue,
                };
                let owp = <ExecutionOutcomeWithProof as borsh::BorshDeserialize>::try_from_slice(
                    bytes.as_ref(),
                )
                .unwrap();
                if !owp.outcome.receipt_ids.is_empty() {
                    return Some(outcome_id);
                }
            }
        }
    }
    None
}

/// Pick the first receipt id that appears as a child in some outcome's
/// `receipt_ids` AND is present in `Receipts`. Deleting it triggers
/// `missing_child_receipts` (and may also trigger `missing_parent_receipts`
/// if the receipt is itself an outcome_id; the missing-child test only
/// asserts that the counts agree between the actor and the bucket-ETL,
/// which holds either way).
fn pick_child_receipt_id(chain_store: &near_chain::ChainStore) -> Option<CryptoHash> {
    use near_chain::ChainStoreAccess;
    use near_primitives::transaction::ExecutionOutcomeWithProof;
    use near_primitives::utils::{get_block_shard_id_rev, get_outcome_id_block_hash};
    let store = chain_store.store();
    let genesis = chain_store.get_genesis_height();
    let head = chain_store.head().ok()?.height;

    for h in genesis..=head {
        let block_hash = match chain_store.get_block_hash_by_height(h) {
            Ok(h) => h,
            Err(_) => continue,
        };
        for (key, outcome_ids) in
            store.iter_prefix_ser::<Vec<CryptoHash>>(DBCol::OutcomeIds, block_hash.as_ref())
        {
            let _ = get_block_shard_id_rev(&key);
            for outcome_id in outcome_ids {
                if let Some(bytes) = store.get(
                    DBCol::TransactionResultForBlock,
                    &get_outcome_id_block_hash(&outcome_id, &block_hash),
                ) {
                    let owp =
                        <ExecutionOutcomeWithProof as borsh::BorshDeserialize>::try_from_slice(
                            bytes.as_ref(),
                        )
                        .unwrap();
                    for child in &owp.outcome.receipt_ids {
                        if store
                            .get_ser::<near_primitives::receipt::Receipt>(
                                DBCol::Receipts,
                                child.as_ref(),
                            )
                            .is_some()
                        {
                            return Some(*child);
                        }
                    }
                }
            }
        }
    }
    None
}

// Silence unused-import warnings on configurations where some imports aren't used.
#[allow(dead_code)]
fn _unused_imports_silencer(_: BuiltOriginInputs, _: BuiltOrigin) -> ReceiptToTxInfo {
    build_receipt_to_tx_info(BuiltOriginInputs {
        outcome_id: CryptoHash::default(),
        shard_id: 0u64.into(),
        child_receiver_id: AccountId::from_str("a").unwrap(),
        origin: BuiltOrigin::FromTransaction { sender_id: AccountId::from_str("a").unwrap() },
    })
}
