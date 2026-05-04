use super::common::{EPOCH_LENGTH, default_options, generate_diverse_traffic, shared_storage};
use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use near_async::time::Duration;
use near_chain::ChainGenesis;
use near_chain::backfill_receipt_to_tx::{
    BACKFILL_CHECKPOINT_KEY, BACKFILL_CHECKPOINT_KEY_LOW, BackfillStorage, process_height,
    process_one_batch,
};
use near_chain_configs::BackfillReceiptToTxConfig;
use near_client::backfill_receipt_to_tx_actor::BackfillReceiptToTxActor;
use near_database_tool::backfill_receipt_to_tx::{BackfillOptions, backfill_receipt_to_tx};
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{
    ReceiptOrigin, ReceiptOriginTransaction, ReceiptToTxInfo, ReceiptToTxInfoV1,
};
use near_primitives::types::{AccountId, Balance, BlockHeight, ShardId};
use near_store::test_utils::create_test_store;
use near_store::{DBCol, NodeStorage, Store};

/// Background actor backfills in descending order using BACKFILL_CHECKPOINT_KEY_LOW.
///
/// Simulates the actor's backward processing by calling process_height directly
/// (the same function the actor uses) and writing entries + checkpoint manually.
/// Verifies that the descending-order result matches forward-direction backfill.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_backward_backfill_matches_forward() {
    init_test_logger();

    let user_account = create_account_id("account0");
    let receiver_account = create_account_id("account1");

    let mut env = TestLoopBuilder::new()
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .add_user_account(&receiver_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .config_modifier(|config, _| {
            config.save_receipt_to_tx = false;
        })
        .gc_num_epochs_to_keep(20)
        .build();

    generate_diverse_traffic(&mut env);

    let store = env.validator().store();
    let chain_store = env.validator().client().chain.chain_store();
    let genesis_height = chain_store.get_genesis_height();
    let head_height = env.validator().head().height;

    // Run forward backfill to get the reference entries.
    let forward_stats = backfill_receipt_to_tx(
        chain_store,
        &shared_storage(&store, None),
        genesis_height,
        head_height,
        &default_options(),
        None,
    )
    .expect("forward backfill should succeed");

    let forward_entries: Vec<(Vec<u8>, ReceiptToTxInfo)> = store
        .iter_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx)
        .map(|(k, v)| (k.to_vec(), v))
        .collect();

    // Wipe and run backward (simulating the actor's logic).
    {
        let mut update = store.store_update();
        for (key, _) in &forward_entries {
            update.delete(DBCol::ReceiptToTx, key);
        }
        update.commit();
    }

    // Exercise the shared helper in descending order — same code path the actor uses.
    let pool = rayon::ThreadPoolBuilder::new().num_threads(1).build().unwrap();
    let heights: Vec<BlockHeight> = (genesis_height..=head_height).rev().collect();
    let backward_stats = process_one_batch(
        chain_store,
        &shared_storage(&store, None),
        &pool,
        &heights,
        Some((BACKFILL_CHECKPOINT_KEY_LOW, genesis_height)),
    )
    .expect("backward process_one_batch should succeed");

    // Verify backward checkpoint.
    let low_checkpoint = store
        .get_ser::<BlockHeight>(DBCol::Misc, BACKFILL_CHECKPOINT_KEY_LOW)
        .expect("backward checkpoint should exist");
    assert_eq!(low_checkpoint, genesis_height, "backward checkpoint should equal genesis_height");

    // Verify same number of entries.
    assert_eq!(
        backward_stats.entries_written, forward_stats.entries_written,
        "backward backfill should write the same number of entries as forward"
    );

    // Verify each entry matches.
    for (key, expected_info) in &forward_entries {
        let actual_info =
            store.get_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx, key).unwrap_or_else(|| {
                panic!("backward entry missing for key {:?}", key);
            });
        assert_eq!(
            expected_info, &actual_info,
            "backward entry should match forward for key {:?}",
            key
        );
    }
}

/// The BackfillReceiptToTxActor processes all heights via its batch loop.
///
/// Drives the actor's backfill_batch method manually in a loop to verify:
/// 1. Entries are written to ReceiptToTx
/// 2. Checkpoint reaches genesis
/// 3. Result matches forward-direction backfill
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_backfill_actor_processes_heights() {
    init_test_logger();

    let user_account = create_account_id("account0");
    let receiver_account = create_account_id("account1");

    let mut env = TestLoopBuilder::new()
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .add_user_account(&receiver_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .config_modifier(|config, _| {
            config.save_receipt_to_tx = false;
        })
        .gc_num_epochs_to_keep(20)
        .build();

    generate_diverse_traffic(&mut env);

    let store = env.validator().store();
    assert_eq!(store.iter(DBCol::ReceiptToTx).count(), 0, "no entries before actor runs");

    let chain_store = env.validator().client().chain.chain_store();
    let genesis_height = chain_store.get_genesis_height();

    // Create and run the actor's batch logic directly.
    let genesis = &env.shared_state.genesis;
    let chain_genesis = ChainGenesis::new(&genesis.config);
    let mut actor = BackfillReceiptToTxActor::new(
        shared_storage(&store, None),
        true,
        &chain_genesis,
        BackfillReceiptToTxConfig {
            enabled: true,
            batch_size: 100,
            batch_delay: Duration::milliseconds(1),
            num_threads: 4,
            start_height: None,
        },
    )
    .unwrap();

    // Drive the actor manually by calling backfill_batch in a loop.
    // This tests the real batch method (checkpoint management, height iteration).
    loop {
        match actor.backfill_batch() {
            Ok(true) => break,
            Ok(false) => continue,
            Err(e) => panic!("backfill_batch failed: {e}"),
        }
    }

    // Verify entries were written.
    let entry_count = store.iter(DBCol::ReceiptToTx).count();
    assert!(entry_count > 0, "actor should have written ReceiptToTx entries");

    // Verify checkpoint reached genesis.
    let checkpoint = store
        .get_ser::<BlockHeight>(DBCol::Misc, BACKFILL_CHECKPOINT_KEY_LOW)
        .expect("checkpoint should exist");
    assert!(
        checkpoint <= genesis_height,
        "backward checkpoint should reach genesis, got {} > {}",
        checkpoint,
        genesis_height
    );

    // Verify entries match forward backfill.
    let actor_entries: Vec<(Vec<u8>, ReceiptToTxInfo)> = store
        .iter_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx)
        .map(|(k, v)| (k.to_vec(), v))
        .collect();

    // Wipe and run forward backfill for comparison.
    {
        let mut update = store.store_update();
        for (key, _) in &actor_entries {
            update.delete(DBCol::ReceiptToTx, key);
        }
        update.commit();
    }

    let head_height = env.validator().head().height;
    let forward_stats = backfill_receipt_to_tx(
        chain_store,
        &shared_storage(&store, None),
        genesis_height,
        head_height,
        &default_options(),
        None,
    )
    .expect("forward backfill should succeed");

    assert_eq!(
        actor_entries.len(),
        forward_stats.entries_written as usize,
        "actor should produce the same number of entries as forward backfill"
    );

    for (key, expected_info) in &actor_entries {
        let actual_info =
            store.get_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx, key).unwrap_or_else(|| {
                panic!("forward entry missing for actor key {:?}", key);
            });
        assert_eq!(
            expected_info, &actual_info,
            "actor entry should match forward for key {:?}",
            key
        );
    }
}

/// Actor honors `start_height` on first run and stops at genesis.
///
/// Sets `start_height = mid_height`. After completion, no ReceiptToTx entries should
/// exist for heights above `mid_height`.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_actor_respects_start_height() {
    init_test_logger();

    let user_account = create_account_id("account0");
    let receiver_account = create_account_id("account1");

    let mut env = TestLoopBuilder::new()
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .add_user_account(&receiver_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .config_modifier(|config, _| {
            config.save_receipt_to_tx = false;
        })
        .gc_num_epochs_to_keep(20)
        .build();

    generate_diverse_traffic(&mut env);

    let store = env.validator().store();
    let chain_store = env.validator().client().chain.chain_store();
    let genesis_height = chain_store.get_genesis_height();
    let head_height = env.validator().head().height;
    let mid_height = genesis_height + (head_height - genesis_height) / 2;

    // Reference: which receipt_ids belong to heights strictly above mid_height?
    // We'll assert none of those appear in ReceiptToTx after the bounded backfill.
    let mut high_receipt_ids: Vec<Vec<u8>> = Vec::new();
    for h in (mid_height + 1)..=head_height {
        let result = process_height(chain_store, h).expect("process_height should succeed");
        if let Some(res) = result {
            for (receipt_id, _) in res.entries {
                high_receipt_ids.push(receipt_id.as_ref().to_vec());
            }
        }
    }
    assert!(
        !high_receipt_ids.is_empty(),
        "setup sanity: traffic should produce receipts above mid_height",
    );

    // Reset any state written by the reference pass above (process_height doesn't write,
    // but be defensive — ReceiptToTx should still be empty).
    assert_eq!(store.iter(DBCol::ReceiptToTx).count(), 0);

    let genesis = &env.shared_state.genesis;
    let chain_genesis = ChainGenesis::new(&genesis.config);
    let mut actor = BackfillReceiptToTxActor::new(
        shared_storage(&store, None),
        true,
        &chain_genesis,
        BackfillReceiptToTxConfig {
            enabled: true,
            batch_size: 100,
            batch_delay: Duration::milliseconds(1),
            num_threads: 1,
            start_height: Some(mid_height),
        },
    )
    .unwrap();

    loop {
        match actor.backfill_batch() {
            Ok(true) => break,
            Ok(false) => continue,
            Err(e) => panic!("backfill_batch failed: {e}"),
        }
    }

    let entry_count = store.iter(DBCol::ReceiptToTx).count();
    assert!(entry_count > 0, "actor should have written entries below mid_height");

    for id in &high_receipt_ids {
        assert!(
            store.get_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx, id).is_none(),
            "entry for receipt_id from height > mid_height should not be backfilled",
        );
    }

    let checkpoint = store
        .get_ser::<BlockHeight>(DBCol::Misc, BACKFILL_CHECKPOINT_KEY_LOW)
        .expect("checkpoint should exist");
    assert!(
        checkpoint <= genesis_height,
        "backward checkpoint should reach genesis, got {checkpoint}"
    );
}

/// Actor returns an error when `start_height` is outside the valid range.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_actor_rejects_invalid_start_height() {
    init_test_logger();

    let user_account = create_account_id("account0");

    let mut env = TestLoopBuilder::new()
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .config_modifier(|config, _| {
            config.save_receipt_to_tx = false;
        })
        .build();

    // Advance a few blocks so head > genesis.
    let target_height = env.validator().head().height + 2 * EPOCH_LENGTH;
    env.validator_runner().run_until_executed_height(target_height);

    let store = env.validator().store();
    let chain_store = env.validator().client().chain.chain_store();
    let genesis_height = chain_store.get_genesis_height();
    let head_height = env.validator().head().height;

    let genesis = &env.shared_state.genesis;
    let chain_genesis = ChainGenesis::new(&genesis.config);

    let make_actor = |start_height: BlockHeight| {
        BackfillReceiptToTxActor::new(
            shared_storage(&store, None),
            true,
            &chain_genesis,
            BackfillReceiptToTxConfig {
                enabled: true,
                batch_size: 100,
                batch_delay: Duration::milliseconds(1),
                num_threads: 1,
                start_height: Some(start_height),
            },
        )
        .unwrap()
    };

    // Above head.
    let mut actor_above = make_actor(head_height + 1_000);
    let err = actor_above.backfill_batch().expect_err("start_height > head must error");
    let msg = format!("{err:#}");
    assert!(msg.contains("exceeds chain head"), "error should mention head-exceeded, got: {msg}");

    // Below genesis. `genesis_height` is always >= 1 for testloop — if it's 0, use
    // saturating_sub to force a value that passes the > head check below genesis.
    if genesis_height > 0 {
        let mut actor_below = make_actor(genesis_height - 1);
        let err = actor_below.backfill_batch().expect_err("start_height < genesis must error");
        let msg = format!("{err:#}");
        assert!(msg.contains("is below genesis"), "error should mention below-genesis, got: {msg}");
    }

    // Ignore-warning case: checkpoint present ⇒ start_height silently ignored,
    // first batch must succeed. The WARN log itself is visual-verify only.
    {
        let mut update = store.store_update();
        update.set_ser(DBCol::Misc, BACKFILL_CHECKPOINT_KEY_LOW, &head_height);
        update.commit();
    }
    let mut actor_with_checkpoint = make_actor(head_height + 1_000);
    let _ = actor_with_checkpoint
        .backfill_batch()
        .expect("start_height must be ignored when a checkpoint exists");
}

/// Sequential forward (CLI) + backward (actor) backfill of overlapping ranges converges
/// to the same result as a single fresh forward run. Proves the separate-checkpoints /
/// insert-only safety claim without requiring concurrency.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_sequential_forward_then_backward_converge() {
    init_test_logger();

    let user_account = create_account_id("account0");
    let receiver_account = create_account_id("account1");

    let mut env = TestLoopBuilder::new()
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .add_user_account(&receiver_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .config_modifier(|config, _| {
            config.save_receipt_to_tx = false;
        })
        .gc_num_epochs_to_keep(20)
        .build();

    generate_diverse_traffic(&mut env);

    let store = env.validator().store();
    let chain_store = env.validator().client().chain.chain_store();
    let genesis_height = chain_store.get_genesis_height();
    let head_height = env.validator().head().height;
    let mid_height = genesis_height + (head_height - genesis_height) / 2;

    // First: CLI forward backfill over [genesis, mid_height].
    backfill_receipt_to_tx(
        chain_store,
        &shared_storage(&store, None),
        genesis_height,
        mid_height,
        &BackfillOptions { batch_size: 100, num_threads: 1, use_checkpoint: true },
        None,
    )
    .expect("forward CLI backfill should succeed");

    // Then: actor backward backfill from head down — overlaps the forward run in [genesis, mid_height].
    let genesis = &env.shared_state.genesis;
    let chain_genesis = ChainGenesis::new(&genesis.config);
    let mut actor = BackfillReceiptToTxActor::new(
        shared_storage(&store, None),
        true,
        &chain_genesis,
        BackfillReceiptToTxConfig {
            enabled: true,
            batch_size: 100,
            batch_delay: Duration::milliseconds(1),
            num_threads: 1,
            start_height: None,
        },
    )
    .unwrap();
    loop {
        match actor.backfill_batch() {
            Ok(true) => break,
            Ok(false) => continue,
            Err(e) => panic!("backfill_batch failed: {e}"),
        }
    }

    let combined_entries: Vec<(Vec<u8>, ReceiptToTxInfo)> = store
        .iter_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx)
        .map(|(k, v)| (k.to_vec(), v))
        .collect();

    // Reference: wipe and run a single fresh forward pass.
    {
        let mut update = store.store_update();
        for (key, _) in &combined_entries {
            update.delete(DBCol::ReceiptToTx, key);
        }
        update.delete(DBCol::Misc, BACKFILL_CHECKPOINT_KEY);
        update.delete(DBCol::Misc, BACKFILL_CHECKPOINT_KEY_LOW);
        update.commit();
    }
    backfill_receipt_to_tx(
        chain_store,
        &shared_storage(&store, None),
        genesis_height,
        head_height,
        &default_options(),
        None,
    )
    .expect("reference full forward backfill should succeed");

    let reference_entries: Vec<(Vec<u8>, ReceiptToTxInfo)> = store
        .iter_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx)
        .map(|(k, v)| (k.to_vec(), v))
        .collect();

    assert_eq!(
        combined_entries.len(),
        reference_entries.len(),
        "forward+backward should equal fresh full forward"
    );
    for ((ck, cv), (rk, rv)) in combined_entries.iter().zip(reference_entries.iter()) {
        assert_eq!(ck, rk, "entry key mismatch");
        assert_eq!(cv, rv, "entry value mismatch");
    }
}

/// Actor resumes from checkpoint after restart: partial run + fresh actor = complete backfill.
///
/// Drives `backfill_batch()` for a few iterations (partial progress), drops the actor,
/// builds a fresh actor with the same storage, drives to completion, and asserts the
/// combined output equals a from-scratch forward backfill.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_actor_resumes_from_checkpoint_after_restart() {
    init_test_logger();

    let user_account = create_account_id("account0");
    let receiver_account = create_account_id("account1");

    let mut env = TestLoopBuilder::new()
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .add_user_account(&receiver_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .config_modifier(|config, _| {
            config.save_receipt_to_tx = false;
        })
        .gc_num_epochs_to_keep(20)
        .build();

    generate_diverse_traffic(&mut env);

    let store = env.validator().store();
    let chain_store = env.validator().client().chain.chain_store();
    let genesis_height = chain_store.get_genesis_height();
    let head_height = env.validator().head().height;

    let genesis = &env.shared_state.genesis;
    let chain_genesis = ChainGenesis::new(&genesis.config);

    // First: run a fresh actor for a few batches (partial progress).
    {
        let mut actor = BackfillReceiptToTxActor::new(
            shared_storage(&store, None),
            true,
            &chain_genesis,
            BackfillReceiptToTxConfig {
                enabled: true,
                batch_size: 5,
                batch_delay: Duration::milliseconds(1),
                num_threads: 1,
                start_height: None,
            },
        )
        .unwrap();

        let mut batches_run = 0;
        loop {
            match actor.backfill_batch() {
                Ok(true) => panic!("backfill should not complete in only 2 batches"),
                Ok(false) => {
                    batches_run += 1;
                    if batches_run >= 2 {
                        break;
                    }
                }
                Err(e) => panic!("backfill_batch failed: {e}"),
            }
        }
    }
    // Actor is dropped; checkpoint persists in the store.

    let checkpoint_after_partial = store
        .get_ser::<BlockHeight>(DBCol::Misc, BACKFILL_CHECKPOINT_KEY_LOW)
        .expect("checkpoint should exist after partial run");
    assert!(
        checkpoint_after_partial > genesis_height,
        "checkpoint should be above genesis after partial run"
    );
    let partial_entry_count = store.iter(DBCol::ReceiptToTx).count();
    assert!(partial_entry_count > 0, "should have written some entries in partial run");

    // Then: create a fresh actor and drive to completion.
    {
        let mut actor = BackfillReceiptToTxActor::new(
            shared_storage(&store, None),
            true,
            &chain_genesis,
            BackfillReceiptToTxConfig {
                enabled: true,
                batch_size: 5,
                batch_delay: Duration::milliseconds(1),
                num_threads: 1,
                start_height: None,
            },
        )
        .unwrap();

        loop {
            match actor.backfill_batch() {
                Ok(true) => break,
                Ok(false) => continue,
                Err(e) => panic!("backfill_batch failed: {e}"),
            }
        }
    }

    // Verify checkpoint reached genesis.
    let checkpoint_final = store
        .get_ser::<BlockHeight>(DBCol::Misc, BACKFILL_CHECKPOINT_KEY_LOW)
        .expect("checkpoint should exist after completion");
    assert!(checkpoint_final <= genesis_height, "checkpoint should reach genesis after completion");

    // Verify combined result matches a fresh forward backfill.
    let combined_entries: Vec<(Vec<u8>, ReceiptToTxInfo)> = store
        .iter_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx)
        .map(|(k, v)| (k.to_vec(), v))
        .collect();

    // Wipe and run forward backfill for comparison.
    {
        let mut update = store.store_update();
        for (key, _) in &combined_entries {
            update.delete(DBCol::ReceiptToTx, key);
        }
        update.delete(DBCol::Misc, BACKFILL_CHECKPOINT_KEY_LOW);
        update.commit();
    }

    let forward_stats = backfill_receipt_to_tx(
        chain_store,
        &shared_storage(&store, None),
        genesis_height,
        head_height,
        &default_options(),
        None,
    )
    .expect("forward backfill should succeed");

    assert_eq!(
        combined_entries.len(),
        forward_stats.entries_written as usize,
        "restart-resume should produce the same number of entries as forward backfill"
    );

    for (key, expected_info) in &combined_entries {
        let actual_info =
            store.get_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx, key).unwrap_or_else(|| {
                panic!("forward entry missing for restart-resume key {:?}", key);
            });
        assert_eq!(
            expected_info, &actual_info,
            "restart-resume entry should match forward for key {:?}",
            key
        );
    }
}

/// Open a fresh on-disk RocksDB-backed `Store` for use as a `write_store`.
///
/// The returned `tempfile::TempDir` must outlive the `Store`; both are bound
/// in the caller's scope.
fn fresh_rocksdb_write_store() -> (tempfile::TempDir, Store) {
    let (tmp, opener) = NodeStorage::test_opener();
    let storage = opener.open().expect("open test rocksdb store");
    (tmp, storage.get_hot_store())
}

/// SST and legacy write paths produce byte-identical `ReceiptToTx` content.
///
/// Drives the same forward CLI backfill twice over the same chain data, with
/// only the `sst_temp_dir` field of `BackfillStorage` differing. Both runs
/// land in independent real-RocksDB write stores; iteration over `ReceiptToTx`
/// must be identical, key-for-key and value-for-value.
///
/// Catches: SST sort/dedup/ingest mechanics, borsh-serialization equivalence
/// between `insert_ser` (legacy) and `borsh::to_vec(&info)` (SST path),
/// multi-batch SST behavior, and checkpoint correctness in the SST mode.
///
/// Does NOT exercise cross-batch overlap with already-stored keys — that path
/// is explicitly out of the production assumption documented on
/// `process_one_batch`. Test 2 covers a separate divergent-value scenario
/// without depending on overlap correctness.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_sst_path_matches_legacy_forward_only() {
    init_test_logger();

    let user_account = create_account_id("account0");
    let receiver_account = create_account_id("account1");

    let mut env = TestLoopBuilder::new()
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .add_user_account(&receiver_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .config_modifier(|config, _| {
            config.save_receipt_to_tx = false;
        })
        .gc_num_epochs_to_keep(20)
        .build();

    generate_diverse_traffic(&mut env);

    let read_store = env.validator().store();
    let chain_store = env.validator().client().chain.chain_store();
    let genesis_height = chain_store.get_genesis_height();
    let head_height = env.validator().head().height;

    // Two independent RocksDB write stores. Bind both TempDirs to outer scope.
    let (_sst_db_tmp, sst_write_store) = fresh_rocksdb_write_store();
    let (_legacy_db_tmp, legacy_write_store) = fresh_rocksdb_write_store();
    let sst_temp_dir = tempfile::tempdir().expect("create sst staging dir");

    let sst_storage = BackfillStorage {
        read_store: read_store.clone(),
        write_store: sst_write_store.clone(),
        checkpoint_store: create_test_store(),
        sst_temp_dir: Some(sst_temp_dir.path().to_path_buf()),
    };
    let legacy_storage = BackfillStorage {
        read_store,
        write_store: legacy_write_store.clone(),
        checkpoint_store: create_test_store(),
        sst_temp_dir: None,
    };

    let options = BackfillOptions { batch_size: 100, num_threads: 1, use_checkpoint: true };

    let sst_stats = backfill_receipt_to_tx(
        chain_store,
        &sst_storage,
        genesis_height,
        head_height,
        &options,
        None,
    )
    .expect("SST forward backfill should succeed");
    let legacy_stats = backfill_receipt_to_tx(
        chain_store,
        &legacy_storage,
        genesis_height,
        head_height,
        &options,
        None,
    )
    .expect("legacy forward backfill should succeed");

    assert_eq!(sst_stats.entries_written, legacy_stats.entries_written, "entry counts must match",);

    // Both stores are RocksDB-backed, so iteration is sorted-by-key. Direct
    // Vec equality is sufficient: same key set, same value bytes, same order.
    let sst_entries: Vec<(Box<[u8]>, Box<[u8]>)> =
        sst_write_store.iter(DBCol::ReceiptToTx).collect();
    let legacy_entries: Vec<(Box<[u8]>, Box<[u8]>)> =
        legacy_write_store.iter(DBCol::ReceiptToTx).collect();
    assert!(!sst_entries.is_empty(), "diverse traffic must produce entries");
    assert_eq!(sst_entries, legacy_entries, "SST output must be byte-identical to legacy output",);

    // Checkpoint state must agree on the highest-completed height.
    let sst_checkpoint = sst_storage
        .checkpoint_store
        .get_ser::<BlockHeight>(DBCol::Misc, BACKFILL_CHECKPOINT_KEY)
        .expect("SST checkpoint must exist");
    let legacy_checkpoint = legacy_storage
        .checkpoint_store
        .get_ser::<BlockHeight>(DBCol::Misc, BACKFILL_CHECKPOINT_KEY)
        .expect("legacy checkpoint must exist");
    assert_eq!(sst_checkpoint, legacy_checkpoint, "checkpoint heights must match");
    assert_eq!(sst_checkpoint, head_height, "checkpoint must reach head");
}

/// Locks in cross-batch divergent-value behavior of the SST path.
///
/// A pre-existing `(K, V1)` is planted directly in `write_store` via
/// `set_raw_bytes` (bypassing the insert-only assertion). A single backfill
/// run then produces `(K, V2 != V1)`, simulating an unreachable-in-production
/// scenario the plan flags for explicit coverage.
///
/// Locked-in behavior on this scenario:
///
/// - SST path: silently overwrites V1 with V2. RocksDB's
///   `allow_global_seqno=true` (set in `Database::ingest_external_sst_files`)
///   makes the newly-ingested row visible in preference to the planted row.
/// - Legacy path in **debug** builds: panics with "write once column
///   overwritten" because `core/store/src/db/rocksdb.rs:354-358` runs
///   `assert_no_overwrite` only behind `cfg!(debug_assertions)`.
/// - Legacy path in **release** builds: silently overwrites V1 with V2,
///   matching the SST path.
///
/// Catching the divergence in debug is the point. If a future change makes
/// the SST path's behavior diverge from this contract — whether by adding a
/// guard, dropping the seqno-based ingest, or changing the dedup loop — this
/// test fails and the divergence is forced to the surface rather than
/// drifting silently.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_sst_path_handles_cross_batch_value_divergence() {
    init_test_logger();

    let user_account = create_account_id("account0");
    let receiver_account = create_account_id("account1");

    let mut env = TestLoopBuilder::new()
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .add_user_account(&receiver_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .config_modifier(|config, _| {
            config.save_receipt_to_tx = false;
        })
        .gc_num_epochs_to_keep(20)
        .build();

    generate_diverse_traffic(&mut env);

    let read_store = env.validator().store();
    let chain_store = env.validator().client().chain.chain_store();
    let genesis_height = chain_store.get_genesis_height();
    let head_height = env.validator().head().height;

    // Pick a real receipt id the backfill will produce by running it once
    // against a scratch store. The pre-existing-key scenario we want requires
    // a key the run will actually try to write.
    let scratch_store = create_test_store();
    let scratch_storage = BackfillStorage {
        read_store: read_store.clone(),
        write_store: scratch_store.clone(),
        checkpoint_store: create_test_store(),
        sst_temp_dir: None,
    };
    backfill_receipt_to_tx(
        chain_store,
        &scratch_storage,
        genesis_height,
        head_height,
        &BackfillOptions { batch_size: 1000, num_threads: 1, use_checkpoint: false },
        None,
    )
    .expect("scratch forward backfill should succeed");
    let (real_key_bytes, real_v_bytes): (Box<[u8]>, Box<[u8]>) = scratch_store
        .iter(DBCol::ReceiptToTx)
        .next()
        .expect("scratch backfill must produce at least one entry");
    let real_key = CryptoHash::try_from(real_key_bytes.as_ref()).expect("32-byte key");

    // Hand-built V1 byte-distinct from anything the chain produces. Writing
    // `set_raw_bytes` rather than `insert` is intentional: it bypasses the
    // insert-only assertion so we can plant stale state for the run to clobber.
    let synthetic_v1 = ReceiptToTxInfo::V1(ReceiptToTxInfoV1 {
        origin: ReceiptOrigin::FromTransaction(ReceiptOriginTransaction {
            tx_hash: CryptoHash::default(),
            sender_account_id: "synthetic-sender.test.near".parse::<AccountId>().unwrap(),
        }),
        receiver_account_id: "synthetic-receiver.test.near".parse::<AccountId>().unwrap(),
        shard_id: ShardId::new(0),
    });
    let synthetic_v1_bytes = borsh::to_vec(&synthetic_v1).expect("borsh serialize V1");
    assert_ne!(synthetic_v1_bytes, real_v_bytes.as_ref(), "V1 must differ from V2");

    // Helper: build a fresh RocksDB write_store with V1 planted at real_key,
    // then run forward backfill in either SST or legacy mode.
    let run_backfill = |sst_temp_dir: Option<std::path::PathBuf>| -> (tempfile::TempDir, Store) {
        let (db_tmp, write_store) = fresh_rocksdb_write_store();
        {
            let mut update = write_store.store_update();
            update.set_raw_bytes(DBCol::ReceiptToTx, real_key.as_ref(), &synthetic_v1_bytes);
            update.commit();
        }
        let pre: Vec<u8> = write_store
            .get(DBCol::ReceiptToTx, real_key.as_ref())
            .expect("V1 planted")
            .as_ref()
            .to_vec();
        assert_eq!(pre.as_slice(), synthetic_v1_bytes.as_slice(), "V1 must be visible pre-run");

        let storage = BackfillStorage {
            read_store: read_store.clone(),
            write_store: write_store.clone(),
            checkpoint_store: create_test_store(),
            sst_temp_dir,
        };
        backfill_receipt_to_tx(
            chain_store,
            &storage,
            genesis_height,
            head_height,
            &BackfillOptions { batch_size: 100, num_threads: 1, use_checkpoint: true },
            None,
        )
        .expect("backfill should succeed despite pre-existing key");
        (db_tmp, write_store)
    };

    // SST path: must silently overwrite V1 → V2. The seqno semantics of
    // `ingest_external_sst_files` make the newer entry shadow the older one.
    let sst_temp_dir = tempfile::tempdir().expect("create sst staging dir");
    let (_sst_db_tmp, sst_store) = run_backfill(Some(sst_temp_dir.path().to_path_buf()));
    let sst_observed: Vec<u8> = sst_store
        .get(DBCol::ReceiptToTx, real_key.as_ref())
        .expect("SST run leaves a value at the key")
        .as_ref()
        .to_vec();
    assert_eq!(
        sst_observed,
        real_v_bytes.as_ref(),
        "SST path must silently overwrite V1 with V2 (RocksDB seqno semantics)",
    );

    // Legacy path: panics in debug, silent overwrite in release. Capture the
    // panic via catch_unwind in debug; assert equivalence with SST in release.
    let legacy_result =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_backfill(None)));
    match legacy_result {
        Err(panic_payload) => {
            assert!(cfg!(debug_assertions), "legacy path panicked unexpectedly in release build",);
            // The panic message comes from `core/store/src/db/mod.rs::assert_no_overwrite`.
            let msg = panic_payload
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic_payload.downcast_ref::<&'static str>().copied())
                .unwrap_or("");
            assert!(
                msg.contains("write once column overwritten"),
                "expected assert_no_overwrite panic, got: {msg:?}",
            );
        }
        Ok((_legacy_db_tmp, legacy_store)) => {
            assert!(
                !cfg!(debug_assertions),
                "legacy path must panic in debug builds on cross-batch divergent value",
            );
            let legacy_observed: Vec<u8> = legacy_store
                .get(DBCol::ReceiptToTx, real_key.as_ref())
                .expect("legacy run leaves a value at the key")
                .as_ref()
                .to_vec();
            assert_eq!(
                legacy_observed, sst_observed,
                "in release, SST and legacy must agree on the final value",
            );
        }
    }
}

/// Byte-equivalence between the MultiGet `process_height` and a scalar
/// reference implementation. The reference walks the same chain state with
/// per-key Gets (the shape `process_height` collapses into batched calls).
/// For the same chain state, both implementations must produce identical
/// `(receipt_id, ReceiptToTxInfo)` entries — drift here would silently
/// corrupt `ReceiptToTx` data on archival nodes.
///
/// The scalar reference is intentionally verbose and parallel to the
/// per-key shape so future readers can spot divergence at the call-site
/// level.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_multi_get_path_matches_scalar_reference() {
    use near_chain::ChainStoreAccess;
    use near_primitives::receipt::{ReceiptOriginReceipt, ReceiptOriginTransaction};
    use near_primitives::utils::get_block_shard_id_rev;

    init_test_logger();

    let user_account = create_account_id("account0");
    let receiver_account = create_account_id("account1");

    let mut env = TestLoopBuilder::new()
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .add_user_account(&receiver_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .config_modifier(|config, _| {
            config.save_receipt_to_tx = false;
        })
        .gc_num_epochs_to_keep(20)
        .build();

    generate_diverse_traffic(&mut env);

    let chain_store = env.validator().client().chain.chain_store();
    let genesis_height = chain_store.get_genesis_height();
    let head_height = env.validator().head().height;

    /// Scalar reference implementation — mirrors the pre-Phase-1 shape of
    /// `process_height` exactly.  Returns the same `(receipt_id, info)`
    /// list the new MultiGet code is expected to produce.
    fn scalar_reference_process_height(
        chain_store: &near_chain::ChainStore,
        height: BlockHeight,
    ) -> Vec<(CryptoHash, ReceiptToTxInfo)> {
        let block_hash = match chain_store.get_block_hash_by_height(height) {
            Ok(hash) => hash,
            Err(_) => return Vec::new(),
        };
        let read_store = chain_store.store();
        let mut entries = Vec::new();
        for (key, outcome_ids) in
            read_store.iter_prefix_ser::<Vec<CryptoHash>>(DBCol::OutcomeIds, block_hash.as_ref())
        {
            let (_, shard_id) = get_block_shard_id_rev(&key).expect("invalid OutcomeIds key");
            for outcome_id in outcome_ids {
                let outcome_with_proof =
                    match chain_store.get_outcome_by_id_and_block_hash(&outcome_id, &block_hash) {
                        Some(o) => o,
                        None => continue,
                    };
                let outcome = &outcome_with_proof.outcome;
                if outcome.receipt_ids.is_empty() {
                    continue;
                }
                let is_tx = read_store.exists(DBCol::Transactions, outcome_id.as_ref());
                let parent_receipt =
                    if !is_tx { chain_store.get_receipt(&outcome_id) } else { None };
                for child_receipt_id in &outcome.receipt_ids {
                    let child_receipt = match chain_store.get_receipt(child_receipt_id) {
                        Some(r) => r,
                        None => continue,
                    };
                    let info = if is_tx {
                        ReceiptToTxInfo::V1(ReceiptToTxInfoV1 {
                            origin: ReceiptOrigin::FromTransaction(ReceiptOriginTransaction {
                                tx_hash: outcome_id,
                                sender_account_id: outcome.executor_id.clone(),
                            }),
                            receiver_account_id: child_receipt.receiver_id().clone(),
                            shard_id,
                        })
                    } else {
                        let parent = match &parent_receipt {
                            Some(r) => r,
                            None => continue,
                        };
                        ReceiptToTxInfo::V1(ReceiptToTxInfoV1 {
                            origin: ReceiptOrigin::FromReceipt(ReceiptOriginReceipt {
                                parent_receipt_id: outcome_id,
                                parent_predecessor_id: parent.predecessor_id().clone(),
                            }),
                            receiver_account_id: child_receipt.receiver_id().clone(),
                            shard_id,
                        })
                    };
                    entries.push((*child_receipt_id, info));
                }
            }
        }
        entries
    }

    // Walk every height; collect the entry set produced by each path.
    let mut multi_get_total: Vec<(CryptoHash, ReceiptToTxInfo)> = Vec::new();
    let mut scalar_total: Vec<(CryptoHash, ReceiptToTxInfo)> = Vec::new();
    let mut heights_with_entries = 0usize;
    for h in genesis_height..=head_height {
        if let Some(result) =
            process_height(chain_store, h).expect("process_height should not error on test chain")
        {
            if !result.entries.is_empty() {
                heights_with_entries += 1;
            }
            multi_get_total.extend(result.entries);
        }
        scalar_total.extend(scalar_reference_process_height(chain_store, h));
    }

    assert!(
        heights_with_entries > 0,
        "diverse traffic must produce at least one ReceiptToTx entry"
    );

    // Sort both by receipt_id to make the comparison order-independent
    // (the assembly loops walk in slightly different orders by construction).
    let key = |e: &(CryptoHash, ReceiptToTxInfo)| e.0;
    multi_get_total.sort_by_key(key);
    scalar_total.sort_by_key(key);

    // Compare via Borsh-serialised bytes — same equality semantics as the
    // SST-vs-legacy test (`ReceiptToTxInfo` is `BorshSerialize`).
    let to_bytes = |entries: &[(CryptoHash, ReceiptToTxInfo)]| {
        entries
            .iter()
            .map(|(k, v)| {
                let v = borsh::to_vec(v).expect("serialize ReceiptToTxInfo");
                (*k, v)
            })
            .collect::<Vec<(CryptoHash, Vec<u8>)>>()
    };
    let multi_bytes = to_bytes(&multi_get_total);
    let scalar_bytes = to_bytes(&scalar_total);

    assert_eq!(
        multi_bytes.len(),
        scalar_bytes.len(),
        "MultiGet and scalar paths must produce the same number of entries",
    );
    assert_eq!(
        multi_bytes, scalar_bytes,
        "MultiGet `process_height` must produce byte-identical entries to the scalar reference",
    );
}
