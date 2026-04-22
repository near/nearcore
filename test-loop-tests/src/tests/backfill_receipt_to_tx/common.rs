use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use near_async::time::Duration;
use near_chain::backfill_receipt_to_tx::BackfillStorage;
use near_database_tool::backfill_receipt_to_tx::BackfillOptions;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{Balance, Gas};
use near_store::Store;

pub(super) const EPOCH_LENGTH: u64 = 5;

pub(super) fn default_options() -> BackfillOptions {
    BackfillOptions { batch_size: 1000, num_threads: 1, use_checkpoint: false }
}

/// Build a `BackfillStorage` where all three roles share the same underlying store.
/// Useful for non-split-storage tests.
pub(super) fn shared_storage(store: &Store) -> BackfillStorage {
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
pub(super) fn generate_diverse_traffic(env: &mut TestLoopEnv) {
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
