use std::collections::HashSet;

use near_async::time::Duration;
use near_client::pending_transaction_queue::P_MAX;
use near_crypto::{InMemorySigner, KeyType, Signer};
use near_o11y::testonly::init_test_logger;
use near_parameters::RuntimeConfigStore;
use near_primitives::account::AccessKey;
use near_primitives::action::{
    AddKeyAction, DeployContractAction, TransferToGasKeyAction, WithdrawFromGasKeyAction,
};
use near_primitives::errors::InvalidTxError;
use near_primitives::hash::CryptoHash;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::{Action, SignedTransaction, TransactionNonce, TransferAction};
use near_primitives::types::Nonce;
use near_primitives::types::{AccountId, Balance, NonceIndex};
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::{QueryRequest, QueryResponseKind};
use node_runtime::config::tx_cost;

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use crate::utils::node::TestLoopNode;

use super::spice_utils::delay_endorsements_propagation;

const TEST_GAS_PRICE: Balance = Balance::from_yoctonear(1);

fn gas_cost_per_transfer() -> Balance {
    let config_store = RuntimeConfigStore::new(None);
    let config = config_store.get_config(PROTOCOL_VERSION);
    let dummy_account = create_account_id("dummy");
    let sample_tx = SignedTransaction::send_money(
        1,
        dummy_account.clone(),
        dummy_account.clone(),
        &create_user_test_signer(&dummy_account),
        Balance::from_yoctonear(0),
        CryptoHash::default(),
    );
    tx_cost(&config, &sample_tx.transaction, TEST_GAS_PRICE).unwrap().gas_cost
}

/// Submit `count` transfer transactions from `sender` to `receiver`.
/// Returns the tx hashes. Increments `next_nonce`.
fn submit_transfers(
    env: &TestLoopEnv,
    sender: &AccountId,
    receiver: &AccountId,
    next_nonce: &mut u64,
    count: usize,
) -> Vec<CryptoHash> {
    let block_hash = env.validator().head().last_block_hash;
    let mut tx_hashes = Vec::new();
    for _ in 0..count {
        let tx = SignedTransaction::send_money(
            *next_nonce,
            sender.clone(),
            receiver.clone(),
            &create_user_test_signer(sender),
            Balance::from_millinear(1),
            block_hash,
        );
        *next_nonce += 1;
        tx_hashes.push(tx.get_hash());
        env.validator().submit_tx(tx);
    }
    tx_hashes
}

/// Deploy a contract on `account` and wait for certification to advance past
/// the deploy (so deploy exclusivity doesn't block subsequent txs).
fn deploy_contract_and_certify(env: &mut TestLoopEnv, account: &AccountId) {
    let deploy_tx = env.validator().tx_deploy_test_contract(account);
    env.validator_runner().run_tx(deploy_tx, Duration::seconds(20));
    let height = env.validator().head().height;
    env.validator_runner().run_until_certified(height);
}

/// P_MAX enforcement for contract accounts.
///
/// Deploy a contract, then submit P_MAX + 2 transactions. The pending
/// transaction queue should throttle inclusion to at most P_MAX at a time
/// from a contract account.
/// All transactions are eventually included after certification frees slots.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_ptq_p_max_contract_account() {
    init_test_logger();

    let contract_account = create_account_id("contract_account");
    let receiver = create_account_id("receiver");
    let mut env = TestLoopBuilder::new()
        .validators(1, 1)
        .add_user_account(&contract_account, Balance::from_near(1_000))
        .add_user_account(&receiver, Balance::from_near(0))
        .build();
    let execution_delay = 4;
    delay_endorsements_propagation(&mut env, execution_delay);
    let mut env = env.warmup();
    deploy_contract_and_certify(&mut env, &contract_account);

    // Submit P_MAX + 2 transfer transactions from the contract account.
    let num_txs = P_MAX + 2;
    let mut next_nonce = env.validator().get_next_nonce(&contract_account);
    let tx_hashes = submit_transfers(&env, &contract_account, &receiver, &mut next_nonce, num_txs);
    // Only P_MAX txs should be included before certification.
    env.validator_runner().run_until_included(&tx_hashes[..P_MAX]);
    for tx_hash in &tx_hashes[P_MAX..] {
        assert!(!is_included_in_head(&env.validator(), std::slice::from_ref(tx_hash)));
    }
    // The remaining txs are included after certification advances.
    let remaining: Vec<_> = tx_hashes[P_MAX..].to_vec();
    env.validator_runner().run_until_included(&remaining);

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// No P_MAX restriction for non-contract accounts.
///
/// Submit more than P_MAX transactions from an account without a contract.
/// All should be included without restriction.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_ptq_no_p_max_for_non_contract_account() {
    init_test_logger();

    let sender = create_account_id("sender");
    let receiver = create_account_id("receiver");
    let mut env = TestLoopBuilder::new()
        .validators(1, 1)
        .add_user_account(&sender, Balance::from_near(1_000))
        .add_user_account(&receiver, Balance::from_near(0))
        .build();
    let execution_delay = 4;
    delay_endorsements_propagation(&mut env, execution_delay);
    let mut env = env.warmup();

    let num_txs = P_MAX + 2;
    let mut next_nonce: u64 = 1;
    let tx_hashes = submit_transfers(&env, &sender, &receiver, &mut next_nonce, num_txs);
    // All should be included without P_MAX throttling.
    env.validator_runner().run_until_included(&tx_hashes);

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Nonce constraint from pending transaction queue.
///
/// Submit a transaction, wait for inclusion (but not certification). Then
/// submit another transaction reusing the same nonce. The certified state
/// still has the old nonce, but the pending transaction queue's max_nonce
/// should cause the RPC to reject the replay with InvalidNonce.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_ptq_nonce_constraint() {
    init_test_logger();

    let sender = create_account_id("sender");
    let receiver = create_account_id("receiver");
    let mut env = TestLoopBuilder::new()
        .validators(1, 1)
        .add_user_account(&sender, Balance::from_near(1_000))
        .add_user_account(&receiver, Balance::from_near(0))
        .build();
    let execution_delay = 4;
    delay_endorsements_propagation(&mut env, execution_delay);
    let mut env = env.warmup();

    // Submit a tx with nonce 1 and wait for inclusion.
    let block_hash = env.validator().head().last_block_hash;
    let tx = SignedTransaction::send_money(
        1,
        sender.clone(),
        receiver.clone(),
        &create_user_test_signer(&sender),
        Balance::from_millinear(1),
        block_hash,
    );
    env.validator().submit_tx(tx.clone());
    env.validator_runner().run_until_included(&[tx.get_hash()]);

    // Submit another tx reusing nonce 1. The certified state still has
    // nonce 0, but the pending transaction queue's max_nonce should cause
    // rejection.
    let block_hash = env.validator().head().last_block_hash;
    let replay_tx = SignedTransaction::send_money(
        1,
        sender.clone(),
        receiver,
        &create_user_test_signer(&sender),
        Balance::from_millinear(2),
        block_hash,
    );
    let result = env.validator_runner().execute_tx(replay_tx, Duration::seconds(5));
    assert!(matches!(result, Err(InvalidTxError::InvalidNonce { .. })));

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Deploy exclusivity.
///
/// Submit a DeployContract transaction and a transfer from the same account
/// simultaneously. The deploy should be included first, and the transfer
/// should be blocked while the deploy is uncertified. After certification,
/// the transfer is included.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_ptq_deploy_exclusivity() {
    init_test_logger();

    let account = create_account_id("deployer");
    let receiver = create_account_id("receiver");
    let mut env = TestLoopBuilder::new()
        .validators(1, 1)
        .add_user_account(&account, Balance::from_near(1_000))
        .add_user_account(&receiver, Balance::from_near(0))
        .build();
    let execution_delay = 4;
    delay_endorsements_propagation(&mut env, execution_delay);
    let mut env = env.warmup();

    // Submit a deploy tx and a transfer tx from the same account.
    let mut next_nonce: u64 = 1;
    let block_hash = env.validator().head().last_block_hash;
    let deploy_tx = SignedTransaction::from_actions(
        next_nonce,
        account.clone(),
        account.clone(),
        &create_user_test_signer(&account),
        vec![Action::DeployContract(DeployContractAction {
            code: near_test_contracts::rs_contract().to_vec(),
        })],
        block_hash,
    );
    next_nonce += 1;
    let transfer_tx = SignedTransaction::send_money(
        next_nonce,
        account.clone(),
        receiver,
        &create_user_test_signer(&account),
        Balance::from_millinear(1),
        block_hash,
    );
    let deploy_hash = deploy_tx.get_hash();
    let transfer_hash = transfer_tx.get_hash();

    env.validator().submit_tx(deploy_tx);
    env.validator().submit_tx(transfer_tx);

    // Deploy should be included first (lower nonce, picked first from pool).
    env.validator_runner().run_until_included(&[deploy_hash]);
    // Deploy exclusivity should prevent the transfer from being included
    // while the deploy is uncertified. Run up to just before certification.
    env.validator_runner().run_for_number_of_blocks(execution_delay as usize - 1);
    assert!(!is_included_in_head(&env.validator(), &[transfer_hash]));

    // Transfer should be included after certification advances.
    env.validator_runner().run_until_included(&[transfer_hash]);

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Pending transaction queue accumulates across blocks.
///
/// Submit transactions across multiple blocks. Verify that P_MAX counts
/// transactions from all uncertified blocks, not just the latest one.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_ptq_accumulates_across_blocks() {
    init_test_logger();

    let contract_account = create_account_id("contract_account");
    let receiver = create_account_id("receiver");
    let mut env = TestLoopBuilder::new()
        .validators(1, 1)
        .add_user_account(&contract_account, Balance::from_near(1_000))
        .add_user_account(&receiver, Balance::from_near(0))
        .build();
    let execution_delay = 4;
    delay_endorsements_propagation(&mut env, execution_delay);
    let mut env = env.warmup();
    deploy_contract_and_certify(&mut env, &contract_account);

    // Submit transactions in two batches, let the first batch get included,
    // then submit the rest. Total across uncertified blocks should be P_MAX.
    let first_batch_size = P_MAX / 2;
    let second_batch_size = P_MAX - first_batch_size;
    let mut next_nonce = env.validator().get_next_nonce(&contract_account);
    let first_batch =
        submit_transfers(&env, &contract_account, &receiver, &mut next_nonce, first_batch_size);
    let first_inclusion_height = env.validator_runner().run_until_included(&first_batch);

    // Submit and include the second batch.
    let second_batch =
        submit_transfers(&env, &contract_account, &receiver, &mut next_nonce, second_batch_size);
    env.validator_runner().run_until_included(&second_batch);

    // Submit a (P_MAX+1)th tx. This should be blocked by P_MAX since there
    // are already P_MAX uncertified txs from this contract account.
    let extra = submit_transfers(&env, &contract_account, &receiver, &mut next_nonce, 1);
    let extra_hash = extra[0];

    // Run blocks up to just before the first batch gets certified.
    // The extra tx should not get included while P_MAX uncertified txs are pending.
    let current_height = env.validator().head().height;
    let blocks_until_certification = first_inclusion_height + execution_delay - current_height - 1;
    env.validator_runner().run_for_number_of_blocks(blocks_until_certification as usize);
    assert!(!is_included_in_head(&env.validator(), &[extra_hash]));

    // Wait for the extra tx to be included after certification.
    env.validator_runner().run_until_included(&[extra_hash]);

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Pending transaction queue cleanup on certification.
///
/// Submit transactions, wait for certification to advance past them, then
/// verify new transactions from the same account are admitted freely.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_ptq_cleanup_on_certification() {
    init_test_logger();

    let contract_account = create_account_id("contract_account");
    let receiver = create_account_id("receiver");

    let mut env = TestLoopBuilder::new()
        .validators(1, 1)
        .add_user_account(&contract_account, Balance::from_near(1_000))
        .add_user_account(&receiver, Balance::from_near(0))
        .build();
    let execution_delay = 4;
    delay_endorsements_propagation(&mut env, execution_delay);
    let mut env = env.warmup();
    deploy_contract_and_certify(&mut env, &contract_account);

    // Submit P_MAX transactions and wait for inclusion + certification.
    let mut next_nonce = env.validator().get_next_nonce(&contract_account);
    let first_batch = submit_transfers(&env, &contract_account, &receiver, &mut next_nonce, P_MAX);
    env.validator_runner().run_until_included(&first_batch);
    let height = env.validator().head().height;
    env.validator_runner().run_until_certified(height);

    // Submit P_MAX more. All should be admitted since the first batch is certified.
    let second_batch = submit_transfers(&env, &contract_account, &receiver, &mut next_nonce, P_MAX);
    env.validator_runner().run_until_included(&second_batch);

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

struct GasKeySpiceEnv {
    env: TestLoopEnv,
    gas_key_signer: Signer,
    next_access_key_nonce: u64,
    /// Per-index gas key nonces (one per nonce index).
    gas_key_nonces: Vec<Nonce>,
}

/// Helper: set up a SPICE env with a gas key on `account`.
/// The gas key has `num_nonces` nonce indices and `fund_amount` balance.
fn setup_gas_key_spice_env(
    account: &AccountId,
    receiver: &AccountId,
    num_nonces: NonceIndex,
    fund_amount: Balance,
) -> GasKeySpiceEnv {
    let mut env = TestLoopBuilder::new()
        .validators(1, 1)
        .add_user_account(account, Balance::from_near(1_000))
        .add_user_account(receiver, Balance::from_near(0))
        .gas_prices(TEST_GAS_PRICE, TEST_GAS_PRICE)
        .build();
    let execution_delay = 4;
    delay_endorsements_propagation(&mut env, execution_delay);
    let mut env = env.warmup();
    let gas_key_signer: Signer =
        InMemorySigner::from_seed(account.clone(), KeyType::ED25519, "gas_key").into();
    let mut next_nonce: u64 = 1;

    // Add gas key.
    let block_hash = env.validator().head().last_block_hash;
    let add_key_tx = SignedTransaction::from_actions(
        next_nonce,
        account.clone(),
        account.clone(),
        &create_user_test_signer(account),
        vec![Action::AddKey(Box::new(AddKeyAction {
            public_key: gas_key_signer.public_key(),
            access_key: AccessKey::gas_key_full_access(num_nonces),
        }))],
        block_hash,
    );
    env.validator_runner().run_tx(add_key_tx, Duration::seconds(20));
    next_nonce += 1;

    // Fund the gas key.
    let block_hash = env.validator().head().last_block_hash;
    let fund_tx = SignedTransaction::from_actions(
        next_nonce,
        account.clone(),
        account.clone(),
        &create_user_test_signer(account),
        vec![Action::TransferToGasKey(Box::new(TransferToGasKeyAction {
            public_key: gas_key_signer.public_key(),
            deposit: fund_amount,
        }))],
        block_hash,
    );
    env.validator_runner().run_tx(fund_tx, Duration::seconds(20));
    next_nonce += 1;

    // Wait for certification to advance past the setup blocks so the certified
    // state includes the funded gas key (needed for RPC tx verification).
    let height = env.validator().head().height;
    env.validator_runner().run_until_certified(height);

    let response = env
        .validator()
        .runtime_query(QueryRequest::ViewGasKeyNonces {
            account_id: account.clone(),
            public_key: gas_key_signer.public_key(),
        })
        .unwrap();
    let QueryResponseKind::GasKeyNonces(view) = response.kind else {
        panic!("expected GasKeyNonces response");
    };
    GasKeySpiceEnv {
        env,
        gas_key_signer,
        next_access_key_nonce: next_nonce,
        gas_key_nonces: view.nonces,
    }
}

/// Gas key balance enforcement.
///
/// Create a gas key with enough balance for exactly 2 txs. Submit 2 gas key
/// txs and wait for inclusion (but not certification). Then submit one more
/// via execute_tx. The pending transaction queue tracks the first 2 as
/// pending, so the RPC rejects the third with NotEnoughGasKeyBalance.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_ptq_gas_key_balance_enforcement() {
    init_test_logger();

    let account = create_account_id("gas_key_account");
    let receiver = create_account_id("receiver");

    // Fund the gas key with enough for exactly 2 txs but not 3.
    let fund_amount = gas_cost_per_transfer().checked_mul(2).unwrap();
    let setup = setup_gas_key_spice_env(&account, &receiver, 1, fund_amount);
    let mut env = setup.env;
    let mut gas_key_nonce = setup.gas_key_nonces[0];

    // Submit the first 2 txs and wait for them to be included.
    let mut tx_hashes = Vec::new();
    let block_hash = env.validator().head().last_block_hash;
    for _ in 0..2 {
        gas_key_nonce += 1;
        let tx = SignedTransaction::from_actions_v1(
            TransactionNonce::from_nonce_and_index(gas_key_nonce, 0),
            account.clone(),
            receiver.clone(),
            &setup.gas_key_signer,
            vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(0) })],
            block_hash,
        );
        tx_hashes.push(tx.get_hash());
        env.validator().submit_tx(tx);
    }
    env.validator_runner().run_until_included(&tx_hashes);

    // Submit one more tx via execute_tx. The pending transaction queue
    // tracks the first 2 as uncertified, so the RPC handler should reject
    // it with NotEnoughGasKeyBalance.
    gas_key_nonce += 1;
    let block_hash = env.validator().head().last_block_hash;
    let tx = SignedTransaction::from_actions_v1(
        TransactionNonce::from_nonce_and_index(gas_key_nonce, 0),
        account,
        receiver,
        &setup.gas_key_signer,
        vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(0) })],
        block_hash,
    );
    let result = env.validator_runner().execute_tx(tx, Duration::seconds(5));
    assert!(matches!(result, Err(InvalidTxError::NotEnoughGasKeyBalance { .. })),);

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// WithdrawFromGasKey reduces available gas key balance.
///
/// Create a gas key with 1 milliNEAR, submit two access key txs each
/// withdrawing half the balance, and wait for inclusion. Then submit a gas
/// key tx via execute_tx. The pending transaction queue accumulates both
/// withdrawals, so the RPC rejects the gas key tx with
/// NotEnoughGasKeyBalance.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_ptq_withdraw_from_gas_key() {
    init_test_logger();

    let account = create_account_id("withdraw_account");
    let receiver = create_account_id("receiver");
    let fund_amount = Balance::from_millinear(1);
    let half = fund_amount.checked_div(2).unwrap();
    let setup = setup_gas_key_spice_env(&account, &receiver, 1, fund_amount);
    let mut env = setup.env;

    // Submit two access key txs, each withdrawing half the gas key balance.
    let block_hash = env.validator().head().last_block_hash;
    let mut next_nonce = setup.next_access_key_nonce;
    let withdraw_tx1 = SignedTransaction::from_actions(
        next_nonce,
        account.clone(),
        account.clone(),
        &create_user_test_signer(&account),
        vec![Action::WithdrawFromGasKey(Box::new(WithdrawFromGasKeyAction {
            public_key: setup.gas_key_signer.public_key(),
            amount: half,
        }))],
        block_hash,
    );
    next_nonce += 1;
    let withdraw_tx2 = SignedTransaction::from_actions(
        next_nonce,
        account.clone(),
        account.clone(),
        &create_user_test_signer(&account),
        vec![Action::WithdrawFromGasKey(Box::new(WithdrawFromGasKeyAction {
            public_key: setup.gas_key_signer.public_key(),
            amount: half,
        }))],
        block_hash,
    );
    let hashes = [withdraw_tx1.get_hash(), withdraw_tx2.get_hash()];
    env.validator().submit_tx(withdraw_tx1);
    env.validator().submit_tx(withdraw_tx2);
    env.validator_runner().run_until_included(&hashes);

    // Submit a gas key tx via execute_tx. The pending transaction queue
    // should accumulate both withdrawals against the gas key balance,
    // so the RPC handler should reject it with NotEnoughGasKeyBalance.
    let block_hash = env.validator().head().last_block_hash;
    let gas_key_tx = SignedTransaction::from_actions_v1(
        TransactionNonce::from_nonce_and_index(setup.gas_key_nonces[0] + 1, 0),
        account,
        receiver,
        &setup.gas_key_signer,
        vec![Action::Transfer(TransferAction { deposit: Balance::from_millinear(0) })],
        block_hash,
    );
    let result = env.validator_runner().execute_tx(gas_key_tx, Duration::seconds(5));
    assert!(matches!(result, Err(InvalidTxError::NotEnoughGasKeyBalance { .. })),);

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Gas key with multiple nonce indices shares balance.
///
/// Create a gas key with 4 nonce indices, funded for exactly 2 txs. Submit
/// 2 txs on different indices to exhaust the shared balance, then submit a
/// 3rd on yet another index. The 3rd should be rejected, proving the balance
/// is pooled across indices rather than tracked independently.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_ptq_gas_key_multiple_nonce_indices() {
    init_test_logger();

    let account = create_account_id("multi_nonce_account");
    let receiver = create_account_id("receiver");
    let num_nonces: NonceIndex = 4;

    // Fund gas key with enough for exactly 2 txs.
    let fund_amount = gas_cost_per_transfer().checked_mul(2).unwrap();
    let setup = setup_gas_key_spice_env(&account, &receiver, num_nonces, fund_amount);
    let mut env = setup.env;

    // Submit 2 txs on different nonce indices and wait for inclusion.
    let mut tx_hashes = Vec::new();
    let block_hash = env.validator().head().last_block_hash;
    for i in 0..2 {
        let tx = SignedTransaction::from_actions_v1(
            TransactionNonce::from_nonce_and_index(setup.gas_key_nonces[i] + 1, i as NonceIndex),
            account.clone(),
            receiver.clone(),
            &setup.gas_key_signer,
            vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(0) })],
            block_hash,
        );
        tx_hashes.push(tx.get_hash());
        env.validator().submit_tx(tx);
    }
    env.validator_runner().run_until_included(&tx_hashes);

    // Submit a 3rd tx on a different nonce index. The shared gas key balance
    // is exhausted, so the RPC should reject it.
    let block_hash = env.validator().head().last_block_hash;
    let tx = SignedTransaction::from_actions_v1(
        TransactionNonce::from_nonce_and_index(setup.gas_key_nonces[2] + 1, 2),
        account,
        receiver,
        &setup.gas_key_signer,
        vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(0) })],
        block_hash,
    );
    let result = env.validator_runner().execute_tx(tx, Duration::seconds(5));
    assert!(matches!(result, Err(InvalidTxError::NotEnoughGasKeyBalance { .. })),);

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Account balance enforcement across access key and gas key transactions.
///
/// Submit an access key tx with a large deposit that nearly exhausts the
/// account balance (1000 NEAR). Then submit a gas key tx whose deposit
/// exceeds the remaining balance. The gas key tx's deposit is paid from the
/// account balance, so the pending transaction queue's paid_from_balance
/// constraint causes the RPC to reject it with NotEnoughBalanceForDeposit.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_ptq_account_balance_access_key_gas_key_combined() {
    init_test_logger();

    let account = create_account_id("combo_account");
    let receiver = create_account_id("receiver");
    let fund_amount = Balance::from_millinear(100);
    let setup = setup_gas_key_spice_env(&account, &receiver, 1, fund_amount);
    let mut env = setup.env;

    // Submit an access key tx with a large deposit that nearly exhausts the
    // 1000 NEAR account balance.
    let block_hash = env.validator().head().last_block_hash;
    let access_key_tx = SignedTransaction::send_money(
        setup.next_access_key_nonce,
        account.clone(),
        receiver.clone(),
        &create_user_test_signer(&account),
        Balance::from_near(999),
        block_hash,
    );
    env.validator().submit_tx(access_key_tx.clone());

    // Wait for inclusion so the pending transaction queue tracks paid_from_balance.
    let access_key_hash = access_key_tx.get_hash();
    env.validator_runner().run_until_included(&[access_key_hash]);

    // Submit a gas key tx whose deposit exceeds the remaining balance.
    // Gas is paid from the gas key, but deposit is paid from the account.
    let block_hash = env.validator().head().last_block_hash;
    let gas_key_tx = SignedTransaction::from_actions_v1(
        TransactionNonce::from_nonce_and_index(setup.gas_key_nonces[0] + 1, 0),
        account,
        receiver,
        &setup.gas_key_signer,
        vec![Action::Transfer(TransferAction { deposit: Balance::from_near(2) })],
        block_hash,
    );
    let result = env.validator_runner().execute_tx(gas_key_tx, Duration::seconds(5));
    assert!(matches!(result, Err(InvalidTxError::NotEnoughBalanceForDeposit { .. })),);

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

fn is_included_in_head(node: &TestLoopNode<'_>, tx_hashes: &[CryptoHash]) -> bool {
    let client = node.client();
    let head = client.chain.head().unwrap();
    let block = client.chain.get_block(&head.last_block_hash).unwrap();
    let mut included: HashSet<CryptoHash> = HashSet::new();
    for chunk_header in block.chunks().iter() {
        let chunk = client.chain.get_chunk(&chunk_header.chunk_hash()).unwrap();
        for tx in chunk.to_transactions() {
            included.insert(tx.get_hash());
        }
    }
    tx_hashes.iter().all(|h| included.contains(h))
}
