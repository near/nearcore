use near_async::time::Duration;
use near_crypto::{InMemorySigner, KeyType, PublicKey, Signer};
use near_o11y::testonly::init_test_logger;
use near_primitives::account::AccessKey;
use near_primitives::action::{AddKeyAction, TransferToGasKeyAction};
use near_primitives::errors::{ActionError, ActionErrorKind, TxExecutionError};
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::{
    Action, FunctionCallAction, SignedTransaction, TransactionNonce, TransferAction,
};
use near_primitives::types::{AccountId, Balance, Gas, Nonce, NonceIndex};
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::{
    AccessKeyPermissionView, AccessKeyView, FinalExecutionOutcomeView, FinalExecutionStatus,
    QueryRequest, QueryResponseKind,
};
use testlib::fees_utils::FeeHelper;

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::{
    create_account_ids, create_validators_spec, validators_spec_clients_with_rpc,
};
use crate::utils::node::TestLoopNode;
use crate::utils::transactions::get_shared_block_hash;

fn query_gas_key_and_balance(
    node: &TestLoopNode<'_>,
    account_id: &AccountId,
    public_key: &PublicKey,
) -> (AccessKeyView, Balance) {
    let view = node.view_access_key_query(account_id, public_key).unwrap();
    let balance = match &view.permission {
        AccessKeyPermissionView::GasKeyFullAccess { balance, .. }
        | AccessKeyPermissionView::GasKeyFunctionCall { balance, .. } => *balance,
        _ => panic!("expected gas key, got {:?}", view.permission),
    };
    (view, balance)
}

fn assert_function_call_error(outcome: &FinalExecutionOutcomeView) {
    assert!(
        matches!(
            outcome.status,
            FinalExecutionStatus::Failure(TxExecutionError::ActionError(ActionError {
                kind: ActionErrorKind::FunctionCallError(_),
                ..
            }))
        ),
        "expected FunctionCallError, got {:?}",
        outcome.status,
    );
}

fn total_tokens_burnt(outcome: &FinalExecutionOutcomeView) -> Balance {
    std::iter::once(&outcome.transaction_outcome)
        .chain(&outcome.receipts_outcome)
        .map(|o| o.outcome.tokens_burnt)
        .fold(Balance::ZERO, |a, b| a.saturating_add(b))
}

/// Get the nonce for a gas key with specific nonce_index.
fn get_gas_key_nonce(
    env: &TestLoopEnv,
    account_id: &AccountId,
    public_key: &PublicKey,
    nonce_index: NonceIndex,
) -> Nonce {
    let response = env
        .rpc_node()
        .runtime_query(QueryRequest::ViewGasKeyNonces {
            account_id: account_id.clone(),
            public_key: public_key.clone(),
        })
        .unwrap();
    let QueryResponseKind::GasKeyNonces(view) = response.kind else {
        panic!("expected GasKeyNonces response");
    };
    view.nonces[nonce_index as usize]
}

#[test]
// TODO(gas-keys): Remove "nightly" once stable supports gas keys.
#[cfg_attr(not(feature = "nightly"), ignore)]
fn test_gas_key_transaction() {
    init_test_logger();

    let epoch_length = 10;
    let shard_layout = ShardLayout::single_shard();
    let user_accounts = create_account_ids(["account0", "account1", "account2", "account3"]);
    let initial_balance = Balance::from_near(1_000_000);
    let gas_price = Balance::from_yoctonear(1);
    let validators_spec = create_validators_spec(shard_layout.num_shards() as usize, 0);
    let clients = validators_spec_clients_with_rpc(&validators_spec);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&user_accounts, initial_balance)
        .gas_prices(gas_price, gas_price)
        .build();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .build()
        .warmup();

    let sender = &user_accounts[0];
    let receiver = &user_accounts[1];

    let gas_key_signer: Signer =
        InMemorySigner::from_seed(sender.clone(), KeyType::ED25519, "gas_key").into();
    let block_hash = get_shared_block_hash(&env.node_datas, &env.test_loop.data);
    let num_nonces = 3; // Arbitrary number of nonces for testing
    let add_key_tx = SignedTransaction::from_actions(
        1, // nonce
        sender.clone(),
        sender.clone(),
        &create_user_test_signer(sender),
        vec![Action::AddKey(Box::new(AddKeyAction {
            public_key: gas_key_signer.public_key(),
            access_key: AccessKey::gas_key_full_access(num_nonces),
        }))],
        block_hash,
    );
    env.rpc_runner().run_tx(add_key_tx, Duration::seconds(5));
    // Run for 1 more block for the access key to be reflected in chunks prev state root.
    env.rpc_runner().run_for_number_of_blocks(1);

    // Fund the gas key
    let gas_key_fund_amount = Balance::from_millinear(10);
    let block_hash = get_shared_block_hash(&env.node_datas, &env.test_loop.data);
    let fund_tx = SignedTransaction::from_actions(
        2, // nonce
        sender.clone(),
        sender.clone(),
        &create_user_test_signer(sender),
        vec![Action::TransferToGasKey(Box::new(TransferToGasKeyAction {
            public_key: gas_key_signer.public_key(),
            deposit: gas_key_fund_amount,
        }))],
        block_hash,
    );
    env.rpc_runner().run_tx(fund_tx, Duration::seconds(5));
    env.rpc_runner().run_for_number_of_blocks(1);

    // Record balances before the gas key transaction
    let sender_balance_before = env.rpc_node().view_account_query(sender).unwrap().amount;
    let (_, gas_key_balance_before) =
        query_gas_key_and_balance(&env.rpc_node(), sender, &gas_key_signer.public_key());

    // Send a transfer using the gas key
    let nonce_index = 0;
    let gas_key_nonce = get_gas_key_nonce(&env, sender, &gas_key_signer.public_key(), nonce_index);
    let block_hash = get_shared_block_hash(&env.node_datas, &env.test_loop.data);
    let transfer_amount = Balance::from_near(10);
    let gas_key_tx = SignedTransaction::from_actions_v1(
        TransactionNonce::from_nonce_and_index(gas_key_nonce + 1, nonce_index),
        sender.clone(),
        receiver.clone(),
        &gas_key_signer,
        vec![Action::Transfer(TransferAction { deposit: transfer_amount })],
        block_hash,
    );
    let outcome = env.rpc_runner().execute_tx(gas_key_tx, Duration::seconds(5)).unwrap();
    // Run for 1 more block for the transfer to be reflected in chunks prev state root.
    env.rpc_runner().run_for_number_of_blocks(1);

    // Check that the nonce for the gas key has been incremented
    let updated_gas_key_nonce =
        get_gas_key_nonce(&env, sender, &gas_key_signer.public_key(), nonce_index);
    assert_eq!(updated_gas_key_nonce, gas_key_nonce + 1);

    // Verify account balance pays for deposit, gas key balance pays for gas.
    let sender_balance_after = env.rpc_node().view_account_query(sender).unwrap().amount;
    assert_eq!(sender_balance_after, sender_balance_before.checked_sub(transfer_amount).unwrap());
    let gas_cost = total_tokens_burnt(&outcome);
    assert!(!gas_cost.is_zero());
    let (_, gas_key_balance_after) =
        query_gas_key_and_balance(&env.rpc_node(), sender, &gas_key_signer.public_key());
    assert_eq!(gas_key_balance_after, gas_key_balance_before.checked_sub(gas_cost).unwrap());

    // Verify receiver got the transfer
    let receiver_balance = env.rpc_node().view_account_query(receiver).unwrap().amount;
    assert_eq!(receiver_balance, initial_balance.checked_add(transfer_amount).unwrap());

    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}

#[test]
// TODO(gas-keys): Remove "nightly" once stable supports gas keys.
#[cfg_attr(not(feature = "nightly"), ignore)]
fn test_gas_key_refund() {
    init_test_logger();

    let epoch_length = 10;
    let shard_layout = ShardLayout::single_shard();
    let user_accounts = create_account_ids(["account0", "account1"]);
    let initial_balance = Balance::from_near(1_000_000);
    let gas_price = Balance::from_yoctonear(1);
    let validators_spec = create_validators_spec(shard_layout.num_shards() as usize, 0);
    let clients = validators_spec_clients_with_rpc(&validators_spec);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&user_accounts, initial_balance)
        .gas_prices(gas_price, gas_price)
        .build();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .build()
        .warmup();

    let sender = &user_accounts[0];
    let receiver = &user_accounts[1];

    let gas_key_signer: Signer =
        InMemorySigner::from_seed(sender.clone(), KeyType::ED25519, "gas_key").into();
    let block_hash = get_shared_block_hash(&env.node_datas, &env.test_loop.data);
    let num_nonces = 3;
    let add_key_tx = SignedTransaction::from_actions(
        1,
        sender.clone(),
        sender.clone(),
        &create_user_test_signer(sender),
        vec![Action::AddKey(Box::new(AddKeyAction {
            public_key: gas_key_signer.public_key(),
            access_key: AccessKey::gas_key_full_access(num_nonces),
        }))],
        block_hash,
    );
    env.rpc_runner().run_tx(add_key_tx, Duration::seconds(5));
    env.rpc_runner().run_for_number_of_blocks(1);

    // Fund the gas key
    let gas_key_fund_amount = Balance::from_millinear(10);
    let block_hash = get_shared_block_hash(&env.node_datas, &env.test_loop.data);
    let fund_tx = SignedTransaction::from_actions(
        2,
        sender.clone(),
        sender.clone(),
        &create_user_test_signer(sender),
        vec![Action::TransferToGasKey(Box::new(TransferToGasKeyAction {
            public_key: gas_key_signer.public_key(),
            deposit: gas_key_fund_amount,
        }))],
        block_hash,
    );
    env.rpc_runner().run_tx(fund_tx, Duration::seconds(5));
    env.rpc_runner().run_for_number_of_blocks(1);

    // Record balances before the gas key transaction
    let sender_balance_before = env.rpc_node().view_account_query(sender).unwrap().amount;
    let (_, gas_key_balance_before) =
        query_gas_key_and_balance(&env.rpc_node(), sender, &gas_key_signer.public_key());

    // Call a non-existing function on receiver (no contract deployed) with a deposit.
    // This will fail, producing both a balance refund (to account) and a gas refund (to gas key).
    let nonce_index = 0;
    let gas_key_nonce = get_gas_key_nonce(&env, sender, &gas_key_signer.public_key(), nonce_index);
    let block_hash = get_shared_block_hash(&env.node_datas, &env.test_loop.data);
    let prepaid_gas = near_primitives::types::Gas::from_teragas(100);
    let deposit_amount = Balance::from_near(5);
    let gas_key_tx = SignedTransaction::from_actions_v1(
        TransactionNonce::from_nonce_and_index(gas_key_nonce + 1, nonce_index),
        sender.clone(),
        receiver.clone(),
        &gas_key_signer,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "nonexistent_method".to_string(),
            args: vec![],
            gas: prepaid_gas,
            deposit: deposit_amount,
        }))],
        block_hash,
    );
    let outcome = env.rpc_runner().execute_tx(gas_key_tx, Duration::seconds(5)).unwrap();
    // Run for 1 more block for the refund to be reflected in queries.
    env.rpc_runner().run_for_number_of_blocks(1);

    // The function call should have failed (no contract on receiver).
    assert_function_call_error(&outcome);
    let tokens_burnt = total_tokens_burnt(&outcome);
    assert!(!tokens_burnt.is_zero());

    // Verify gas key balance: should be initial minus tokens_burnt (gas refund went back to gas key).
    let (_, gas_key_balance_after) =
        query_gas_key_and_balance(&env.rpc_node(), sender, &gas_key_signer.public_key());
    assert_eq!(gas_key_balance_after, gas_key_balance_before.checked_sub(tokens_burnt).unwrap());

    // Verify sender account balance is unchanged: deposit was deducted when the tx was
    // converted to a receipt, then refunded when the function call failed.
    let sender_balance_after = env.rpc_node().view_account_query(sender).unwrap().amount;
    assert_eq!(sender_balance_after, sender_balance_before);

    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}

/// Verify that when a gas key transaction's deposit exceeds the account balance, gas is still
/// charged from the gas key. We bypass RPC validation and chunk preparation by inserting the
/// tx directly into the pool and using ProduceWithoutTxVerification mode.
#[test]
#[cfg(feature = "test_features")]
#[cfg_attr(not(feature = "nightly"), ignore)]
fn test_gas_key_deposit_failed() {
    use near_client::NetworkAdversarialMessage;
    use near_client::client_actor::AdvProduceChunksMode;
    use near_primitives::transaction::ValidatedTransaction;

    init_test_logger();

    let epoch_length = 10;
    let shard_layout = ShardLayout::single_shard();
    let user_accounts = create_account_ids(["account0", "account1"]);
    let initial_balance = Balance::from_near(1_000_000);
    let gas_price = Balance::from_yoctonear(1);
    let validators_spec = create_validators_spec(shard_layout.num_shards() as usize, 0);
    let clients = validators_spec_clients_with_rpc(&validators_spec);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&user_accounts, initial_balance)
        .gas_prices(gas_price, gas_price)
        .build();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .build()
        .warmup();

    let sender = &user_accounts[0];
    let receiver = &user_accounts[1];

    // Add gas key
    let gas_key_signer: Signer =
        InMemorySigner::from_seed(sender.clone(), KeyType::ED25519, "gas_key").into();
    let block_hash = get_shared_block_hash(&env.node_datas, &env.test_loop.data);
    let num_nonces = 3;
    let add_key_tx = SignedTransaction::from_actions(
        1,
        sender.clone(),
        sender.clone(),
        &create_user_test_signer(sender),
        vec![Action::AddKey(Box::new(AddKeyAction {
            public_key: gas_key_signer.public_key(),
            access_key: AccessKey::gas_key_full_access(num_nonces),
        }))],
        block_hash,
    );
    env.rpc_runner().run_tx(add_key_tx, Duration::seconds(5));
    env.rpc_runner().run_for_number_of_blocks(1);

    // Fund the gas key
    let gas_key_fund_amount = Balance::from_millinear(10);
    let block_hash = get_shared_block_hash(&env.node_datas, &env.test_loop.data);
    let fund_tx = SignedTransaction::from_actions(
        2,
        sender.clone(),
        sender.clone(),
        &create_user_test_signer(sender),
        vec![Action::TransferToGasKey(Box::new(TransferToGasKeyAction {
            public_key: gas_key_signer.public_key(),
            deposit: gas_key_fund_amount,
        }))],
        block_hash,
    );
    env.rpc_runner().run_tx(fund_tx, Duration::seconds(5));
    env.rpc_runner().run_for_number_of_blocks(1);

    // Record balances before
    let sender_balance_before = env.rpc_node().view_account_query(sender).unwrap().amount;
    let (_, gas_key_balance_before) =
        query_gas_key_and_balance(&env.rpc_node(), sender, &gas_key_signer.public_key());
    let nonce_index: NonceIndex = 0;
    let gas_key_nonce = get_gas_key_nonce(&env, sender, &gas_key_signer.public_key(), nonce_index);

    // Enable adversarial mode: skip runtime verification during chunk preparation.
    env.node_runner(0).send_adversarial_message(NetworkAdversarialMessage::AdvProduceChunks(
        AdvProduceChunksMode::ProduceWithoutTxVerification,
    ));

    // Construct a gas key tx with deposit far exceeding account balance.
    let block_hash = get_shared_block_hash(&env.node_datas, &env.test_loop.data);
    let transfer_amount = Balance::from_near(1_000_000_000);
    let gas_key_tx = SignedTransaction::from_actions_v1(
        TransactionNonce::from_nonce_and_index(gas_key_nonce + 1, nonce_index),
        sender.clone(),
        receiver.clone(),
        &gas_key_signer,
        vec![Action::Transfer(TransferAction { deposit: transfer_amount })],
        block_hash,
    );
    let tx_hash = gas_key_tx.get_hash();

    // Insert directly into the tx pool.
    let shard_uid = shard_layout.account_id_to_shard_uid(sender);
    let validated_tx = ValidatedTransaction::new_for_test(gas_key_tx);
    env.node(0)
        .client()
        .chunk_producer
        .sharded_tx_pool
        .lock()
        .insert_transaction(shard_uid, validated_tx);

    // Wait for the tx to be included in a chunk and executed.
    let outcome = env.rpc_runner().run_until_outcome_available(tx_hash, Duration::seconds(10));
    env.rpc_runner().run_for_number_of_blocks(1);

    // Verify: tx failed with NotEnoughBalanceForDeposit
    assert!(
        matches!(
            &outcome.outcome_with_id.outcome.status,
            near_primitives::transaction::ExecutionStatus::Failure(
                TxExecutionError::InvalidTxError(
                    near_primitives::errors::InvalidTxError::NotEnoughBalanceForDeposit { .. },
                ),
            ),
        ),
        "expected NotEnoughBalanceForDeposit, got {:?}",
        outcome.outcome_with_id.outcome.status,
    );

    // Verify: all prepaid gas was burnt and tokens_burnt matches gas_burnt * gas_price
    let gas_burnt = outcome.outcome_with_id.outcome.gas_burnt;
    let tokens_burnt = outcome.outcome_with_id.outcome.tokens_burnt;
    assert!(gas_burnt.as_gas() > 0);
    assert_eq!(tokens_burnt, gas_price.checked_mul(u128::from(gas_burnt.as_gas())).unwrap());

    // Verify: no receipt was created (DepositFailed doesn't produce receipts)
    assert!(outcome.outcome_with_id.outcome.receipt_ids.is_empty());

    // Verify: gas key balance decreased by exactly tokens_burnt
    let (_, gas_key_balance_after) =
        query_gas_key_and_balance(&env.rpc_node(), sender, &gas_key_signer.public_key());
    assert_eq!(gas_key_balance_after, gas_key_balance_before.checked_sub(tokens_burnt).unwrap());

    // Verify: account balance unchanged
    let sender_balance_after = env.rpc_node().view_account_query(sender).unwrap().amount;
    assert_eq!(sender_balance_after, sender_balance_before);

    // Verify: gas key nonce incremented
    let gas_key_nonce_after =
        get_gas_key_nonce(&env, sender, &gas_key_signer.public_key(), nonce_index);
    assert_eq!(gas_key_nonce_after, gas_key_nonce + 1);

    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}

struct HostFunctionTestSetup {
    env: TestLoopEnv,
    account: AccountId,
    nonce: u64,
}

impl HostFunctionTestSetup {
    fn next_nonce(&mut self) -> u64 {
        self.nonce += 1;
        self.nonce
    }
}

fn setup_host_function_test() -> HostFunctionTestSetup {
    init_test_logger();

    let user_accounts = create_account_ids(["account0"]);
    let initial_balance = Balance::from_near(1_000_000);
    let gas_price = Balance::from_yoctonear(1);
    let mut env = TestLoopBuilder::new()
        .epoch_length(10)
        .gas_prices(gas_price, gas_price)
        .enable_rpc()
        .add_user_accounts(&user_accounts, initial_balance)
        .build()
        .warmup();

    let account = user_accounts[0].clone();
    let mut nonce = 0u64;
    let mut next_nonce = || {
        nonce += 1;
        nonce
    };

    // Deploy the nightly test contract
    let block_hash = env.rpc_node().head().last_block_hash;
    let deploy_tx = SignedTransaction::deploy_contract(
        next_nonce(),
        &account,
        near_test_contracts::nightly_rs_contract().to_vec(),
        &create_user_test_signer(&account),
        block_hash,
    );
    env.rpc_runner().run_tx(deploy_tx, Duration::seconds(5));
    env.rpc_runner().run_for_number_of_blocks(1);

    HostFunctionTestSetup { env, account, nonce }
}

/// Test that a contract can fund a gas key using the
/// `promise_batch_action_transfer_to_gas_key` host function.
#[test]
#[cfg_attr(not(feature = "nightly"), ignore)]
fn test_gas_key_transfer_host_function() {
    let setup = setup_host_function_test();
    let HostFunctionTestSetup { mut env, account, mut nonce } = setup;
    let mut next_nonce = || {
        nonce += 1;
        nonce
    };

    // Create a gas key on account
    let gas_key_signer: Signer =
        InMemorySigner::from_seed(account.clone(), KeyType::ED25519, "gas_key").into();
    let block_hash = env.rpc_node().head().last_block_hash;
    let add_key_tx = SignedTransaction::from_actions(
        next_nonce(),
        account.clone(),
        account.clone(),
        &create_user_test_signer(&account),
        vec![Action::AddKey(Box::new(AddKeyAction {
            public_key: gas_key_signer.public_key(),
            access_key: AccessKey::gas_key_full_access(3),
        }))],
        block_hash,
    );
    env.rpc_runner().run_tx(add_key_tx, Duration::seconds(5));
    env.rpc_runner().run_for_number_of_blocks(1);

    // Fund the gas key with an initial balance via TransferToGasKey transaction action
    let initial_gas_key_fund = Balance::from_millinear(100);
    let block_hash = env.rpc_node().head().last_block_hash;
    let fund_tx = SignedTransaction::from_actions(
        next_nonce(),
        account.clone(),
        account.clone(),
        &create_user_test_signer(&account),
        vec![Action::TransferToGasKey(Box::new(TransferToGasKeyAction {
            public_key: gas_key_signer.public_key(),
            deposit: initial_gas_key_fund,
        }))],
        block_hash,
    );
    env.rpc_runner().run_tx(fund_tx, Duration::seconds(5));
    env.rpc_runner().run_for_number_of_blocks(1);

    // Record gas key balance and account balance before the host function call
    let (_, gas_key_balance_before) =
        query_gas_key_and_balance(&env.rpc_node(), &account, &gas_key_signer.public_key());
    let account_balance_before = env.rpc_node().view_account_query(&account).unwrap().amount;

    // Call the contract's call_promise function to exercise the transfer_to_gas_key host function.
    let host_fn_deposit = Balance::from_millinear(10);
    let public_key_base64 = near_primitives_core::serialize::to_base64(
        &borsh::to_vec(&gas_key_signer.public_key()).unwrap(),
    );
    let input_data = serde_json::json!([
        {"batch_create": {"account_id": account.as_str()}, "id": 0},
        {"action_transfer_to_gas_key": {
            "promise_index": 0,
            "public_key": public_key_base64,
            "amount": host_fn_deposit.as_yoctonear().to_string(),
        }, "id": 0},
    ]);
    let input_data = serde_json::to_vec(&input_data).unwrap();

    let method_name = "call_promise";
    let fc_args_len = input_data.len();
    let block_hash = env.rpc_node().head().last_block_hash;
    let call_tx = SignedTransaction::from_actions(
        next_nonce(),
        account.clone(),
        account.clone(),
        &create_user_test_signer(&account),
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: method_name.to_string(),
            args: input_data,
            gas: Gas::from_teragas(100),
            deposit: Balance::ZERO,
        }))],
        block_hash,
    );
    let outcome = env.rpc_runner().execute_tx(call_tx, Duration::seconds(5)).unwrap();
    env.rpc_runner().run_for_number_of_blocks(1);

    assert!(
        matches!(outcome.status, FinalExecutionStatus::SuccessValue(_)),
        "expected success, got {:?}",
        outcome.status,
    );

    // Verify gas key balance increased by the deposit amount
    let (_, gas_key_balance_after) =
        query_gas_key_and_balance(&env.rpc_node(), &account, &gas_key_signer.public_key());
    assert_eq!(gas_key_balance_after, gas_key_balance_before.checked_add(host_fn_deposit).unwrap());

    // Verify account balance decreased by exactly deposit + tokens_burnt - reward.
    // The runtime gives 30% of gas_burnt_for_function_call * gas_price back to the account.
    // gas_burnt_for_function_call = receipt gas_burnt - exec overhead (new_action_receipt +
    // function_call action fees). The overhead uses method_name + args byte count.
    let account_balance_after = env.rpc_node().view_account_query(&account).unwrap().amount;
    let tokens_burnt = total_tokens_burnt(&outcome);
    let runtime_config =
        env.rpc_node().client().runtime_adapter.get_runtime_config(PROTOCOL_VERSION);
    let gas_price = Balance::from_yoctonear(1);
    let fee_helper = FeeHelper::new(runtime_config.clone(), gas_price);
    let fc_receipt_gas_burnt = outcome.receipts_outcome[0].outcome.gas_burnt;
    let fc_overhead = fee_helper.function_call_exec_gas((method_name.len() + fc_args_len) as u64);
    let gas_burnt_for_function_call = fc_receipt_gas_burnt.checked_sub(fc_overhead).unwrap();
    let reward = fee_helper.gas_burnt_to_reward(gas_burnt_for_function_call);
    assert_eq!(
        account_balance_after,
        account_balance_before
            .checked_sub(host_fn_deposit)
            .unwrap()
            .checked_sub(tokens_burnt)
            .unwrap()
            .checked_add(reward)
            .unwrap(),
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}

/// Test that a contract can create a gas key with full access using the host function.
#[test]
#[cfg_attr(not(feature = "nightly"), ignore)]
fn test_gas_key_add_full_access_host_function() {
    let mut setup = setup_host_function_test();
    let account = setup.account.clone();

    let gas_key_signer: Signer =
        InMemorySigner::from_seed(account.clone(), KeyType::ED25519, "new_gas_key").into();

    let num_nonces = 3u64;
    let public_key_base64 = near_primitives_core::serialize::to_base64(
        &borsh::to_vec(&gas_key_signer.public_key()).unwrap(),
    );
    let input_data = serde_json::json!([
        {"batch_create": {"account_id": account.as_str()}, "id": 0},
        {"action_add_gas_key_with_full_access": {
            "promise_index": 0,
            "public_key": public_key_base64,
            "num_nonces": num_nonces,
        }, "id": 0},
    ]);
    let input_data = serde_json::to_vec(&input_data).unwrap();

    let block_hash = get_shared_block_hash(&setup.env.node_datas, &setup.env.test_loop.data);
    let call_tx = SignedTransaction::from_actions(
        setup.next_nonce(),
        account.clone(),
        account.clone(),
        &create_user_test_signer(&account),
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: input_data,
            gas: Gas::from_teragas(100),
            deposit: Balance::ZERO,
        }))],
        block_hash,
    );
    let outcome = setup.env.rpc_runner().execute_tx(call_tx, Duration::seconds(5)).unwrap();
    setup.env.rpc_runner().run_for_number_of_blocks(1);

    assert!(
        matches!(outcome.status, FinalExecutionStatus::SuccessValue(_)),
        "expected success, got {:?}",
        outcome.status,
    );

    // Verify the gas key was created with correct properties
    let (view, balance) =
        query_gas_key_and_balance(&setup.env.rpc_node(), &account, &gas_key_signer.public_key());
    assert_eq!(balance, Balance::ZERO);
    assert_eq!(view.nonce, 0);
    assert!(matches!(
        view.permission,
        AccessKeyPermissionView::GasKeyFullAccess { num_nonces: 3, .. }
    ));

    // Verify nonces are initialized
    let response = setup
        .env
        .rpc_node()
        .runtime_query(QueryRequest::ViewGasKeyNonces {
            account_id: account,
            public_key: gas_key_signer.public_key(),
        })
        .unwrap();
    let QueryResponseKind::GasKeyNonces(nonces_view) = response.kind else {
        panic!("expected GasKeyNonces response");
    };
    assert_eq!(nonces_view.nonces.len(), num_nonces as usize);

    setup.env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}

/// Test that a contract can create a gas key with function call permission using the host function.
#[test]
#[cfg_attr(not(feature = "nightly"), ignore)]
fn test_gas_key_add_function_call_host_function() {
    let mut setup = setup_host_function_test();
    let account = setup.account.clone();

    let gas_key_signer: Signer =
        InMemorySigner::from_seed(account.clone(), KeyType::ED25519, "new_gas_key").into();

    let num_nonces = 2u64;
    let public_key_base64 = near_primitives_core::serialize::to_base64(
        &borsh::to_vec(&gas_key_signer.public_key()).unwrap(),
    );
    let input_data = serde_json::json!([
        {"batch_create": {"account_id": account.as_str()}, "id": 0},
        {"action_add_gas_key_with_function_call": {
            "promise_index": 0,
            "public_key": public_key_base64,
            "num_nonces": num_nonces,
            "allowance": "0",
            "receiver_id": account.as_str(),
            "method_names": "method1,method2",
        }, "id": 0},
    ]);
    let input_data = serde_json::to_vec(&input_data).unwrap();

    let block_hash = get_shared_block_hash(&setup.env.node_datas, &setup.env.test_loop.data);
    let call_tx = SignedTransaction::from_actions(
        setup.next_nonce(),
        account.clone(),
        account.clone(),
        &create_user_test_signer(&account),
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: input_data,
            gas: Gas::from_teragas(100),
            deposit: Balance::ZERO,
        }))],
        block_hash,
    );
    let outcome = setup.env.rpc_runner().execute_tx(call_tx, Duration::seconds(5)).unwrap();
    setup.env.rpc_runner().run_for_number_of_blocks(1);

    assert!(
        matches!(outcome.status, FinalExecutionStatus::SuccessValue(_)),
        "expected success, got {:?}",
        outcome.status,
    );

    // Verify the gas key was created with correct properties
    let (view, balance) =
        query_gas_key_and_balance(&setup.env.rpc_node(), &account, &gas_key_signer.public_key());
    assert_eq!(balance, Balance::ZERO);
    assert_eq!(view.nonce, 0);
    match &view.permission {
        AccessKeyPermissionView::GasKeyFunctionCall {
            num_nonces: n,
            allowance: a,
            receiver_id: r,
            method_names: m,
            ..
        } => {
            assert_eq!(*n, num_nonces as u16);
            assert!(a.is_none());
            assert_eq!(r.as_str(), account.as_str());
            assert_eq!(m, &vec!["method1".to_string(), "method2".to_string()]);
        }
        other => panic!("expected GasKeyFunctionCall, got {:?}", other),
    }

    // Verify nonces are initialized
    let response = setup
        .env
        .rpc_node()
        .runtime_query(QueryRequest::ViewGasKeyNonces {
            account_id: account,
            public_key: gas_key_signer.public_key(),
        })
        .unwrap();
    let QueryResponseKind::GasKeyNonces(nonces_view) = response.kind else {
        panic!("expected GasKeyNonces response");
    };
    assert_eq!(nonces_view.nonces.len(), num_nonces as usize);

    setup.env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}

/// Test that a nonzero allowance on a gas key function call is rejected by the verifier.
#[test]
#[cfg_attr(not(feature = "nightly"), ignore)]
fn test_gas_key_add_function_call_nonzero_allowance_rejected() {
    let mut setup = setup_host_function_test();
    let account = setup.account.clone();

    let gas_key_signer: Signer =
        InMemorySigner::from_seed(account.clone(), KeyType::ED25519, "new_gas_key").into();

    // Nonzero allowance should be rejected by verifier
    let public_key_base64 = near_primitives_core::serialize::to_base64(
        &borsh::to_vec(&gas_key_signer.public_key()).unwrap(),
    );
    let input_data = serde_json::json!([
        {"batch_create": {"account_id": account.as_str()}, "id": 0},
        {"action_add_gas_key_with_function_call": {
            "promise_index": 0,
            "public_key": public_key_base64,
            "num_nonces": 1,
            "allowance": "999",
            "receiver_id": account.as_str(),
            "method_names": "method1",
        }, "id": 0},
    ]);
    let input_data = serde_json::to_vec(&input_data).unwrap();

    let block_hash = get_shared_block_hash(&setup.env.node_datas, &setup.env.test_loop.data);
    let call_tx = SignedTransaction::from_actions(
        setup.next_nonce(),
        account.clone(),
        account.clone(),
        &create_user_test_signer(&account),
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: input_data,
            gas: Gas::from_teragas(100),
            deposit: Balance::ZERO,
        }))],
        block_hash,
    );
    let outcome = setup.env.rpc_runner().execute_tx(call_tx, Duration::seconds(5)).unwrap();
    setup.env.rpc_runner().run_for_number_of_blocks(1);

    // The receipt validation rejects the AddKey action with nonzero allowance on a gas key,
    // causing the function call to fail with GasKeyFunctionCallAllowanceNotAllowed.
    assert!(
        matches!(
            &outcome.status,
            FinalExecutionStatus::Failure(TxExecutionError::ActionError(ActionError {
                kind: ActionErrorKind::NewReceiptValidationError(_),
                ..
            }))
        ),
        "expected NewReceiptValidationError, got {:?}",
        outcome.status,
    );

    // Verify no gas key was created
    let result = setup.env.rpc_node().view_access_key_query(&account, &gas_key_signer.public_key());
    assert!(result.is_err(), "expected gas key to not exist after failed AddKey");

    setup.env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}

/// Test creating a gas key via host function, funding it, then using it to send a transaction.
#[test]
#[cfg_attr(not(feature = "nightly"), ignore)]
fn test_gas_key_add_then_fund_then_use() {
    let mut setup = setup_host_function_test();
    let account = setup.account.clone();

    let gas_key_signer: Signer =
        InMemorySigner::from_seed(account.clone(), KeyType::ED25519, "new_gas_key").into();

    // Create gas key via host function
    let num_nonces = 3u64;
    let public_key_base64 = near_primitives_core::serialize::to_base64(
        &borsh::to_vec(&gas_key_signer.public_key()).unwrap(),
    );
    let input_data = serde_json::json!([
        {"batch_create": {"account_id": account.as_str()}, "id": 0},
        {"action_add_gas_key_with_full_access": {
            "promise_index": 0,
            "public_key": public_key_base64,
            "num_nonces": num_nonces,
        }, "id": 0},
    ]);
    let input_data = serde_json::to_vec(&input_data).unwrap();

    let block_hash = get_shared_block_hash(&setup.env.node_datas, &setup.env.test_loop.data);
    let call_tx = SignedTransaction::from_actions(
        setup.next_nonce(),
        account.clone(),
        account.clone(),
        &create_user_test_signer(&account),
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: input_data,
            gas: Gas::from_teragas(100),
            deposit: Balance::ZERO,
        }))],
        block_hash,
    );
    let outcome = setup.env.rpc_runner().execute_tx(call_tx, Duration::seconds(5)).unwrap();
    setup.env.rpc_runner().run_for_number_of_blocks(1);
    assert!(
        matches!(outcome.status, FinalExecutionStatus::SuccessValue(_)),
        "expected success creating gas key, got {:?}",
        outcome.status,
    );

    // Fund the gas key via TransferToGasKey transaction
    let gas_key_fund_amount = Balance::from_millinear(10);
    let block_hash = get_shared_block_hash(&setup.env.node_datas, &setup.env.test_loop.data);
    let fund_tx = SignedTransaction::from_actions(
        setup.next_nonce(),
        account.clone(),
        account.clone(),
        &create_user_test_signer(&account),
        vec![Action::TransferToGasKey(Box::new(TransferToGasKeyAction {
            public_key: gas_key_signer.public_key(),
            deposit: gas_key_fund_amount,
        }))],
        block_hash,
    );
    setup.env.rpc_runner().run_tx(fund_tx, Duration::seconds(5));
    setup.env.rpc_runner().run_for_number_of_blocks(1);

    // Verify gas key is funded
    let (_, gas_key_balance) =
        query_gas_key_and_balance(&setup.env.rpc_node(), &account, &gas_key_signer.public_key());
    assert_eq!(gas_key_balance, gas_key_fund_amount);

    // Use the gas key to send a transfer transaction
    let nonce_index = 0;
    let gas_key_nonce =
        get_gas_key_nonce(&setup.env, &account, &gas_key_signer.public_key(), nonce_index);
    let block_hash = get_shared_block_hash(&setup.env.node_datas, &setup.env.test_loop.data);
    let transfer_amount = Balance::from_millinear(1);
    let gas_key_tx = SignedTransaction::from_actions_v1(
        TransactionNonce::from_nonce_and_index(gas_key_nonce + 1, nonce_index),
        account.clone(),
        account.clone(),
        &gas_key_signer,
        vec![Action::Transfer(TransferAction { deposit: transfer_amount })],
        block_hash,
    );
    let outcome = setup.env.rpc_runner().execute_tx(gas_key_tx, Duration::seconds(5)).unwrap();
    setup.env.rpc_runner().run_for_number_of_blocks(1);

    assert!(
        matches!(outcome.status, FinalExecutionStatus::SuccessValue(_)),
        "expected success using gas key, got {:?}",
        outcome.status,
    );

    // Verify gas key balance decreased (gas was charged)
    let gas_cost = total_tokens_burnt(&outcome);
    assert!(!gas_cost.is_zero());
    let (_, gas_key_balance_after) =
        query_gas_key_and_balance(&setup.env.rpc_node(), &account, &gas_key_signer.public_key());
    assert_eq!(gas_key_balance_after, gas_key_fund_amount.checked_sub(gas_cost).unwrap());

    // Verify nonce was updated
    let updated_nonce =
        get_gas_key_nonce(&setup.env, &account, &gas_key_signer.public_key(), nonce_index);
    assert_eq!(updated_nonce, gas_key_nonce + 1);

    setup.env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}
