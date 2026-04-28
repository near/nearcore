use super::common::{EPOCH_LENGTH, default_options, generate_diverse_traffic, shared_storage};
use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use near_async::time::Duration;
use near_chain::ChainGenesis;
use near_chain::backfill_receipt_to_tx::{
    BACKFILL_CHECKPOINT_KEY, BACKFILL_CHECKPOINT_KEY_LOW, process_height, process_one_batch,
};
use near_chain_configs::BackfillReceiptToTxConfig;
use near_client::backfill_receipt_to_tx_actor::BackfillReceiptToTxActor;
use near_database_tool::backfill_receipt_to_tx::{BackfillOptions, backfill_receipt_to_tx};
use near_o11y::testonly::init_test_logger;
use near_primitives::receipt::ReceiptToTxInfo;
use near_primitives::types::{Balance, BlockHeight};
use near_store::DBCol;

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

    // Phase 1: CLI forward backfill over [genesis, mid_height].
    backfill_receipt_to_tx(
        chain_store,
        &shared_storage(&store, None),
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

    // Phase 1: run a fresh actor for a few batches (partial progress).
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

    let checkpoint_after_phase1 = store
        .get_ser::<BlockHeight>(DBCol::Misc, BACKFILL_CHECKPOINT_KEY_LOW)
        .expect("checkpoint should exist after partial run");
    assert!(
        checkpoint_after_phase1 > genesis_height,
        "checkpoint should be above genesis after partial run"
    );
    let partial_entry_count = store.iter(DBCol::ReceiptToTx).count();
    assert!(partial_entry_count > 0, "should have written some entries in partial run");

    // Phase 2: create a fresh actor and drive to completion.
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
