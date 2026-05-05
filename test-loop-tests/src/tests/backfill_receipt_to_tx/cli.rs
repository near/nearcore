use super::common::{EPOCH_LENGTH, default_options, generate_diverse_traffic, shared_storage};
use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use near_chain::backfill_receipt_to_tx::{
    BACKFILL_CHECKPOINT_KEY, BackfillStorage, process_one_batch,
};
use near_database_tool::backfill_receipt_to_tx::{BackfillOptions, backfill_receipt_to_tx};
use near_o11y::testonly::init_test_logger;
use near_primitives::receipt::{ReceiptOrigin, ReceiptToTxInfo};
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{Balance, BlockHeight};
use near_store::DBCol;
use near_store::test_utils::create_test_store;

/// Backfill produces the same ReceiptToTx entries that normal processing would.
///
/// Generates diverse traffic across multiple epochs:
/// - send_money transactions (simple transfers)
/// - deploy_contract + function calls (generates refund receipts)
/// - Multiple senders and receivers
///
/// 1. Run a node with `save_receipt_to_tx = true` and record the entries.
/// 2. Wipe the ReceiptToTx column.
/// 3. Run the backfill function.
/// 4. Verify that the backfilled entries match the originals.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_backfill_matches_normal_processing() {
    init_test_logger();

    let user_account = create_account_id("account0");
    let receiver_account = create_account_id("account1");

    // Build env with save_receipt_to_tx enabled (the default).
    // High gc_num_epochs_to_keep so blocks aren't GC'd during the test.
    let mut env = TestLoopBuilder::new()
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .add_user_account(&receiver_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .gc_num_epochs_to_keep(20)
        .build();

    generate_diverse_traffic(&mut env);

    // Collect all ReceiptToTx entries that were written during normal processing.
    let store = env.validator().store();
    let original_entries: Vec<(Vec<u8>, ReceiptToTxInfo)> = store
        .iter_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx)
        .map(|(k, v)| (k.to_vec(), v))
        .collect();
    assert!(
        original_entries.len() > 10,
        "expected many ReceiptToTx entries from diverse traffic, got {}",
        original_entries.len()
    );

    // Verify we have FromTransaction entries at minimum.
    let has_from_tx = original_entries.iter().any(|(_, info)| {
        let ReceiptToTxInfo::V1(v1) = info;
        matches!(&v1.origin, ReceiptOrigin::FromTransaction(_))
    });
    assert!(has_from_tx, "expected some FromTransaction entries");

    // Wipe the ReceiptToTx column.
    {
        let mut update = store.store_update();
        for (key, _) in &original_entries {
            update.delete(DBCol::ReceiptToTx, key);
        }
        update.commit();
    }

    assert_eq!(store.iter(DBCol::ReceiptToTx).count(), 0, "column should be empty after wipe");

    // Run the backfill.
    let chain_store = env.validator().client().chain.chain_store();
    let genesis_height = chain_store.get_genesis_height();
    let head_height = env.validator().head().height;

    let stats = backfill_receipt_to_tx(
        chain_store,
        &shared_storage(&store),
        genesis_height,
        head_height,
        &default_options(),
        None,
    )
    .expect("backfill should succeed");

    assert_eq!(
        stats.entries_written as usize,
        original_entries.len(),
        "backfill should write exactly as many entries as normal processing"
    );

    // Verify each entry matches.
    for (orig_key, orig_info) in &original_entries {
        let backfilled_info =
            store.get_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx, orig_key).unwrap_or_else(|| {
                panic!("backfilled entry missing for key {:?}", orig_key);
            });
        assert_eq!(
            orig_info, &backfilled_info,
            "backfilled entry should match original for key {:?}",
            orig_key
        );
    }
}

/// Backfill is idempotent — running it twice produces the same result.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_backfill_idempotent() {
    init_test_logger();

    let user_account = create_account_id("account0");

    let mut env = TestLoopBuilder::new()
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .config_modifier(|config, _| {
            config.save_receipt_to_tx = false;
        })
        .build();

    let signer = create_user_test_signer(&user_account);
    let block_hash = env.validator().head().last_block_hash;

    let tx = SignedTransaction::send_money(
        1,
        user_account.clone(),
        user_account,
        &signer,
        Balance::from_yoctonear(100),
        block_hash,
    );
    env.validator().submit_tx(tx);

    let target_height = env.validator().head().height + 2 * EPOCH_LENGTH;
    env.validator_runner().run_until_executed_height(target_height);

    let store = env.validator().store();
    let chain_store = env.validator().client().chain.chain_store();
    let genesis_height = chain_store.get_genesis_height();
    let head_height = env.validator().head().height;

    // No entries should exist (save_receipt_to_tx = false).
    assert_eq!(store.iter(DBCol::ReceiptToTx).count(), 0);

    // Run backfill once.
    let stats1 = backfill_receipt_to_tx(
        chain_store,
        &shared_storage(&store),
        genesis_height,
        head_height,
        &default_options(),
        None,
    )
    .expect("first backfill should succeed");

    let entries_after_first: Vec<(Vec<u8>, ReceiptToTxInfo)> = store
        .iter_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx)
        .map(|(k, v)| (k.to_vec(), v))
        .collect();
    assert!(stats1.entries_written > 0, "first backfill should write entries");

    // Run backfill again (idempotent).
    let stats2 = backfill_receipt_to_tx(
        chain_store,
        &shared_storage(&store),
        genesis_height,
        head_height,
        &default_options(),
        None,
    )
    .expect("second backfill should succeed");

    assert_eq!(
        stats2.entries_written, stats1.entries_written,
        "second backfill should re-write the same number of entries (insert-only column)"
    );

    let entries_after_second: Vec<(Vec<u8>, ReceiptToTxInfo)> = store
        .iter_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx)
        .map(|(k, v)| (k.to_vec(), v))
        .collect();

    // Same entries after both runs.
    assert_eq!(entries_after_first.len(), entries_after_second.len());
    for ((k1, v1), (k2, v2)) in entries_after_first.iter().zip(entries_after_second.iter()) {
        assert_eq!(k1, k2);
        assert_eq!(v1, v2);
    }
}

/// Checkpoint only records fully completed heights, not in-progress ones.
/// Checkpoint is stored in DBCol::Misc atomically with ReceiptToTx entries.
///
/// Uses batch_size=1 so every entry triggers a batch commit. Verifies that:
/// 1. After completion, checkpoint equals the last processed height
/// 2. Re-running from checkpoint processes 0 new entries (no skipped receipts)
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_checkpoint_does_not_skip_mid_height_receipts() {
    init_test_logger();

    let user_account = create_account_id("account0");

    let mut env = TestLoopBuilder::new()
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .config_modifier(|config, _| {
            config.save_receipt_to_tx = false;
        })
        .build();

    let signer = create_user_test_signer(&user_account);
    let block_hash = env.validator().head().last_block_hash;

    let tx = SignedTransaction::send_money(
        1,
        user_account.clone(),
        user_account,
        &signer,
        Balance::from_yoctonear(100),
        block_hash,
    );
    env.validator().submit_tx(tx);

    let target_height = env.validator().head().height + 2 * EPOCH_LENGTH;
    env.validator_runner().run_until_executed_height(target_height);

    let store = env.validator().store();
    let chain_store = env.validator().client().chain.chain_store();
    let genesis_height = chain_store.get_genesis_height();
    let head_height = env.validator().head().height;

    // Run backfill with batch_size=1 (commits after every single entry).
    // This maximizes the chance of mid-height commits.
    let options = BackfillOptions { batch_size: 1, num_threads: 1, use_checkpoint: true };
    let stats = backfill_receipt_to_tx(
        chain_store,
        &shared_storage(&store),
        genesis_height,
        head_height,
        &options,
        None,
    )
    .expect("backfill should succeed");

    assert!(stats.entries_written > 0, "backfill should have written entries");

    // Checkpoint should exist in DBCol::Misc and equal head_height.
    let checkpoint_value = store
        .get_ser::<BlockHeight>(DBCol::Misc, BACKFILL_CHECKPOINT_KEY)
        .expect("checkpoint should exist in DBCol::Misc");
    assert_eq!(checkpoint_value, head_height, "checkpoint should equal the last processed height");

    let entries_count = store.iter(DBCol::ReceiptToTx).count();

    // Now resume from checkpoint. Since checkpoint = head_height,
    // from_height = checkpoint+1 > head_height, so nothing to process.
    // This proves the checkpoint correctly represents "everything up to
    // this height is done" — no receipts were skipped mid-height.
    let stats2 = backfill_receipt_to_tx(
        chain_store,
        &shared_storage(&store),
        checkpoint_value + 1,
        head_height,
        &options,
        None,
    )
    .expect("resume backfill should succeed");

    assert_eq!(stats2.entries_written, 0, "resume should write 0 new entries");
    assert_eq!(
        store.iter(DBCol::ReceiptToTx).count(),
        entries_count,
        "entry count should not change after resume"
    );
}

/// Checkpoint enables correct resume after partial completion.
///
/// Runs backfill on the first half of heights, verifies checkpoint,
/// then resumes from checkpoint+1 to head. Verifies that the combined
/// result covers all heights with no gaps or duplicates.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_checkpoint_resume_after_partial_completion() {
    init_test_logger();

    let user_account = create_account_id("account0");

    let mut env = TestLoopBuilder::new()
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .config_modifier(|config, _| {
            config.save_receipt_to_tx = false;
        })
        .gc_num_epochs_to_keep(20)
        .build();

    let signer = create_user_test_signer(&user_account);

    // Generate traffic across multiple epochs to ensure entries span a wide range.
    let mut nonce = 1;
    for _ in 0..3 {
        for _ in 0..5 {
            let tx = SignedTransaction::send_money(
                nonce,
                user_account.clone(),
                user_account.clone(),
                &signer,
                Balance::from_yoctonear(100),
                env.validator().head().last_block_hash,
            );
            nonce += 1;
            env.validator().submit_tx(tx);
        }
        let target = env.validator().head().height + EPOCH_LENGTH;
        env.validator_runner().run_until_executed_height(target);
    }

    let store = env.validator().store();
    let chain_store = env.validator().client().chain.chain_store();
    let genesis_height = chain_store.get_genesis_height();
    let head_height = env.validator().head().height;
    let mid_height = genesis_height + (head_height - genesis_height) / 2;

    let options_with_checkpoint =
        BackfillOptions { batch_size: 1000, num_threads: 1, use_checkpoint: true };

    // Phase 1: backfill only the first half.
    let stats1 = backfill_receipt_to_tx(
        chain_store,
        &shared_storage(&store),
        genesis_height,
        mid_height,
        &options_with_checkpoint,
        None,
    )
    .expect("first half backfill should succeed");

    let checkpoint = store
        .get_ser::<BlockHeight>(DBCol::Misc, BACKFILL_CHECKPOINT_KEY)
        .expect("checkpoint should exist after first half");
    assert_eq!(checkpoint, mid_height, "checkpoint should equal mid_height");

    let entries_after_first_half = store.iter(DBCol::ReceiptToTx).count();

    // Phase 2: resume from checkpoint+1 to head.
    let stats2 = backfill_receipt_to_tx(
        chain_store,
        &shared_storage(&store),
        checkpoint + 1,
        head_height,
        &options_with_checkpoint,
        None,
    )
    .expect("second half backfill should succeed");

    let final_checkpoint = store
        .get_ser::<BlockHeight>(DBCol::Misc, BACKFILL_CHECKPOINT_KEY)
        .expect("checkpoint should exist after second half");
    assert_eq!(final_checkpoint, head_height, "checkpoint should equal head_height after resume");

    let entries_after_both = store.iter(DBCol::ReceiptToTx).count();
    assert_eq!(
        entries_after_both,
        entries_after_first_half + stats2.entries_written as usize,
        "second half should add new entries without duplicating first half"
    );

    // Verify combined result matches a full backfill.
    // Wipe and re-run from scratch.
    {
        let all_entries: Vec<Vec<u8>> =
            store.iter(DBCol::ReceiptToTx).map(|(k, _)| k.to_vec()).collect();
        let mut update = store.store_update();
        for key in &all_entries {
            update.delete(DBCol::ReceiptToTx, key);
        }
        update.commit();
    }

    let stats_full = backfill_receipt_to_tx(
        chain_store,
        &shared_storage(&store),
        genesis_height,
        head_height,
        &default_options(),
        None,
    )
    .expect("full backfill should succeed");

    assert_eq!(
        stats1.entries_written + stats2.entries_written,
        stats_full.entries_written,
        "partial runs combined should equal full run"
    );
}

/// Split-storage wiring writes entries and checkpoints to the correct physical store.
///
/// Builds a `BackfillStorage` with three distinct stores to stand in for
/// `(split_store, cold_store, hot_store)` on an archival node. After backfill:
/// - ReceiptToTx entries are only in `write_store` (the cold-store stand-in).
/// - The checkpoint is only in `checkpoint_store` (the hot-store stand-in).
/// - `read_store` receives nothing new — it is exclusively a read role.
///
/// Regressing this test means a future refactor has accidentally swapped the
/// three store roles — exactly the silent-corruption failure mode Decision 2-A
/// guards against.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_split_storage_wiring() {
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
    let write_store = create_test_store();
    let checkpoint_store = create_test_store();
    let storage = BackfillStorage {
        read_store: read_store.clone(),
        write_store: write_store.clone(),
        checkpoint_store: checkpoint_store.clone(),
    };

    let chain_store = env.validator().client().chain.chain_store();
    let genesis_height = chain_store.get_genesis_height();
    let head_height = env.validator().head().height;

    let stats = backfill_receipt_to_tx(
        chain_store,
        &storage,
        genesis_height,
        head_height,
        &BackfillOptions { batch_size: 100, num_threads: 1, use_checkpoint: true },
        None,
    )
    .expect("backfill should succeed");
    assert!(stats.entries_written > 0, "backfill should have written entries");

    // Entries landed only in write_store.
    assert!(
        write_store.iter(DBCol::ReceiptToTx).count() > 0,
        "write_store should contain backfilled entries"
    );
    assert_eq!(
        read_store.iter(DBCol::ReceiptToTx).count(),
        0,
        "read_store must NOT receive ReceiptToTx writes"
    );
    assert_eq!(
        checkpoint_store.iter(DBCol::ReceiptToTx).count(),
        0,
        "checkpoint_store must NOT receive ReceiptToTx writes"
    );

    // Checkpoint landed only in checkpoint_store.
    assert_eq!(
        checkpoint_store
            .get_ser::<BlockHeight>(DBCol::Misc, BACKFILL_CHECKPOINT_KEY)
            .expect("checkpoint should be in checkpoint_store"),
        head_height,
    );
    assert!(
        read_store.get_ser::<BlockHeight>(DBCol::Misc, BACKFILL_CHECKPOINT_KEY).is_none(),
        "read_store must NOT receive checkpoint writes"
    );
    assert!(
        write_store.get_ser::<BlockHeight>(DBCol::Misc, BACKFILL_CHECKPOINT_KEY).is_none(),
        "write_store must NOT receive checkpoint writes",
    );

    // Sanity: entries written to write_store are actually readable from write_store
    // (captures the view_runtime ↔ BackfillStorage invariant documented on
    // `BackfillStorage::for_node`: a real archival node reaches them via split_store's
    // hot-then-cold fall-through, which we don't simulate here — but the direct read
    // catches accidental no-op writes).
    let any_entry = write_store
        .iter_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx)
        .next()
        .expect("write_store should have at least one entry");
    let (key, expected) = any_entry;
    let actual = write_store
        .get_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx, &key)
        .expect("entry should be readable from write_store");
    assert_eq!(expected, actual);
}

/// Crash window between entries and checkpoint replays correctly.
///
/// Simulates a crash where entries committed but the checkpoint write didn't happen,
/// by running `process_one_batch` then deleting the checkpoint key. A full replay
/// from genesis must produce identical final content (insert-only idempotency).
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_crash_between_entries_and_checkpoint_replay() {
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

    // Run a partial batch: heights [genesis, mid_height] with a checkpoint.
    let pool = rayon::ThreadPoolBuilder::new().num_threads(1).build().unwrap();
    let partial_heights: Vec<BlockHeight> = (genesis_height..=mid_height).collect();
    let partial_stats = process_one_batch(
        chain_store,
        &shared_storage(&store),
        &pool,
        &partial_heights,
        Some((BACKFILL_CHECKPOINT_KEY, mid_height)),
    )
    .expect("partial batch should succeed");
    assert!(partial_stats.entries_written > 0, "partial run should have written entries");

    let partial_entry_count = store.iter(DBCol::ReceiptToTx).count();
    assert_eq!(partial_entry_count as u64, partial_stats.entries_written);

    // Simulate the crash window: entries persisted, checkpoint write never happened.
    {
        let mut update = store.store_update();
        update.delete(DBCol::Misc, BACKFILL_CHECKPOINT_KEY);
        update.commit();
    }
    assert!(store.get_ser::<BlockHeight>(DBCol::Misc, BACKFILL_CHECKPOINT_KEY).is_none());

    // Replay from genesis — no checkpoint present — covers the same heights and then some.
    let replay_stats = backfill_receipt_to_tx(
        chain_store,
        &shared_storage(&store),
        genesis_height,
        head_height,
        &BackfillOptions { batch_size: 100, num_threads: 1, use_checkpoint: true },
        None,
    )
    .expect("replay should succeed");
    assert!(replay_stats.entries_written > 0, "replay should have rewritten entries");

    let replay_entries: Vec<(Vec<u8>, ReceiptToTxInfo)> = store
        .iter_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx)
        .map(|(k, v)| (k.to_vec(), v))
        .collect();

    // Reference: wipe and run a clean single-pass backfill. Must match replay content.
    {
        let mut update = store.store_update();
        for (key, _) in &replay_entries {
            update.delete(DBCol::ReceiptToTx, key);
        }
        update.delete(DBCol::Misc, BACKFILL_CHECKPOINT_KEY);
        update.commit();
    }
    backfill_receipt_to_tx(
        chain_store,
        &shared_storage(&store),
        genesis_height,
        head_height,
        &default_options(),
        None,
    )
    .expect("reference backfill should succeed");

    let reference_entries: Vec<(Vec<u8>, ReceiptToTxInfo)> = store
        .iter_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx)
        .map(|(k, v)| (k.to_vec(), v))
        .collect();

    assert_eq!(
        replay_entries.len(),
        reference_entries.len(),
        "post-replay entry count should equal a clean run"
    );
    for ((replay_key, replay_val), (ref_key, ref_val)) in
        replay_entries.iter().zip(reference_entries.iter())
    {
        assert_eq!(replay_key, ref_key, "entry key mismatch between replay and reference");
        assert_eq!(replay_val, ref_val, "entry value mismatch between replay and reference");
    }
}
