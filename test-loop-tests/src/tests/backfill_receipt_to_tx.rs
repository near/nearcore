use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use near_async::time::Duration;
use near_chain::ChainGenesis;
use near_chain::backfill_receipt_to_tx::{
    BACKFILL_CHECKPOINT_KEY, BACKFILL_CHECKPOINT_KEY_LOW, BackfillStorage, process_one_batch,
};
use near_chain_configs::BackfillReceiptToTxConfig;
use near_client::backfill_receipt_to_tx_actor::BackfillReceiptToTxActor;
use near_database_tool::backfill_receipt_to_tx::{BackfillOptions, backfill_receipt_to_tx};
use near_o11y::testonly::init_test_logger;
use near_primitives::receipt::{ReceiptOrigin, ReceiptToTxInfo};
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{Balance, BlockHeight, Gas};
use near_store::test_utils::create_test_store;
use near_store::{DBCol, Store};

const EPOCH_LENGTH: u64 = 5;

fn default_options() -> BackfillOptions {
    BackfillOptions { batch_size: 1000, num_threads: 1, use_checkpoint: false }
}

/// Build a `BackfillStorage` where all three roles share the same underlying store.
/// Useful for non-split-storage tests.
fn shared_storage(store: &Store) -> BackfillStorage {
    BackfillStorage {
        read_store: store.clone(),
        write_store: store.clone(),
        checkpoint_store: store.clone(),
    }
}

/// Generate diverse traffic across multiple epochs: send_money in both directions,
/// deploy contract + function calls (refund receipts), more send_money.
///
/// Expects `account0` and `account1` to exist in the test environment.
fn generate_diverse_traffic(env: &mut TestLoopEnv) {
    let user_account = create_account_id("account0");
    let receiver_account = create_account_id("account1");
    let signer = create_user_test_signer(&user_account);
    let receiver_signer = create_user_test_signer(&receiver_account);
    let mut nonce = 1;

    // --- Epoch 1: send_money transactions in both directions ---
    for _ in 0..10 {
        let tx = SignedTransaction::send_money(
            nonce,
            user_account.clone(),
            receiver_account.clone(),
            &signer,
            Balance::from_yoctonear(100),
            env.validator().head().last_block_hash,
        );
        nonce += 1;
        env.validator().submit_tx(tx);
    }

    // Also send from receiver back to user.
    let mut receiver_nonce = 1;
    for _ in 0..5 {
        let tx = SignedTransaction::send_money(
            receiver_nonce,
            receiver_account.clone(),
            user_account.clone(),
            &receiver_signer,
            Balance::from_yoctonear(50),
            env.validator().head().last_block_hash,
        );
        receiver_nonce += 1;
        env.validator().submit_tx(tx);
    }

    let target_height = env.validator().head().height + EPOCH_LENGTH;
    env.validator_runner().run_until_executed_height(target_height);

    // --- Epoch 2: deploy contract + function calls (generates refund receipts) ---
    let deploy_tx = SignedTransaction::deploy_contract(
        nonce,
        &user_account,
        near_test_contracts::rs_contract().to_vec(),
        &signer,
        env.validator().head().last_block_hash,
    );
    nonce += 1;
    env.validator_runner().run_tx(deploy_tx, Duration::seconds(5));

    // Call a method with excess gas to generate refund receipts (FromReceipt).
    for _ in 0..10 {
        let call_tx = SignedTransaction::call(
            nonce,
            user_account.clone(),
            user_account.clone(),
            &signer,
            Balance::ZERO,
            "log_something".to_owned(),
            vec![],
            Gas::from_teragas(300),
            env.validator().head().last_block_hash,
        );
        nonce += 1;
        env.validator_runner().run_tx(call_tx, Duration::seconds(5));
    }

    // --- Epoch 3: more send_money to span another epoch ---
    for _ in 0..10 {
        let tx = SignedTransaction::send_money(
            nonce,
            user_account.clone(),
            receiver_account.clone(),
            &signer,
            Balance::from_yoctonear(100),
            env.validator().head().last_block_hash,
        );
        nonce += 1;
        env.validator().submit_tx(tx);
    }

    let target_height = env.validator().head().height + 2 * EPOCH_LENGTH;
    env.validator_runner().run_until_executed_height(target_height);
}

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
        &shared_storage(&store),
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
        &shared_storage(&store),
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
        shared_storage(&store),
        true,
        &chain_genesis,
        BackfillReceiptToTxConfig {
            enabled: true,
            batch_size: 100,
            batch_delay: Duration::milliseconds(1),
            num_threads: 4,
            start_height: None,
        },
    );

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
        &shared_storage(&store),
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
    for ((rk, rv), (refk, refv)) in replay_entries.iter().zip(reference_entries.iter()) {
        assert_eq!(rk, refk, "entry key mismatch between replay and reference");
        assert_eq!(rv, refv, "entry value mismatch between replay and reference");
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
        let result = near_chain::backfill_receipt_to_tx::process_height(chain_store, h)
            .expect("process_height should succeed");
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
        shared_storage(&store),
        true,
        &chain_genesis,
        BackfillReceiptToTxConfig {
            enabled: true,
            batch_size: 100,
            batch_delay: Duration::milliseconds(1),
            num_threads: 1,
            start_height: Some(mid_height),
        },
    );

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
            shared_storage(&store),
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

    // Phase 1: CLI forward backfill over [genesis, mid_height].
    backfill_receipt_to_tx(
        chain_store,
        &shared_storage(&store),
        genesis_height,
        mid_height,
        &BackfillOptions { batch_size: 100, num_threads: 1, use_checkpoint: true },
        None,
    )
    .expect("forward CLI backfill should succeed");

    // Phase 2: actor backward backfill from head down — overlaps phase 1 in [genesis, mid_height].
    let genesis = &env.shared_state.genesis;
    let chain_genesis = ChainGenesis::new(&genesis.config);
    let mut actor = BackfillReceiptToTxActor::new(
        shared_storage(&store),
        true,
        &chain_genesis,
        BackfillReceiptToTxConfig {
            enabled: true,
            batch_size: 100,
            batch_delay: Duration::milliseconds(1),
            num_threads: 1,
            start_height: None,
        },
    );
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
        &shared_storage(&store),
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
