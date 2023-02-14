//! DelegateAction is a type of action to support meta transactions.
//!
//! NEP: https://github.com/near/NEPs/pull/366
//! This is the module for its integration tests.

use crate::node::{Node, RuntimeNode};
use crate::tests::client::process_blocks::create_nightshade_runtimes;
use crate::tests::standard_cases::fee_helper;
use near_chain::ChainGenesis;
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_crypto::{KeyType, PublicKey};
use near_primitives::account::AccessKey;
use near_primitives::config::ActionCosts;
use near_primitives::errors::{ActionsValidationError, InvalidTxError, TxExecutionError};
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::{
    Action, AddKeyAction, DeleteAccountAction, DeleteKeyAction, DeployContractAction,
    FunctionCallAction, StakeAction, TransferAction,
};
use near_primitives::types::{AccountId, Balance};
use near_primitives::version::{ProtocolFeature, ProtocolVersion};
use near_primitives::views::{
    AccessKeyPermissionView, FinalExecutionOutcomeView, FinalExecutionStatus,
};
use near_test_contracts::smallest_rs_contract;
use nearcore::config::GenesisExt;
use nearcore::NEAR_BASE;
use testlib::runtime_utils::{
    add_contract, alice_account, bob_account, carol_account, eve_dot_alice_account,
};

fn exec_meta_transaction(
    actions: Vec<Action>,
    protocol_version: ProtocolVersion,
) -> FinalExecutionStatus {
    near_o11y::testonly::init_test_logger();
    let validator: AccountId = "test0".parse().unwrap();
    let user: AccountId = "alice.near".parse().unwrap();
    let receiver: AccountId = "bob.near".parse().unwrap();
    let relayer: AccountId = "relayer.near".parse().unwrap();
    let mut genesis =
        Genesis::test(vec![validator, user.clone(), receiver.clone(), relayer.clone()], 1);
    genesis.config.epoch_length = 1000;
    genesis.config.protocol_version = protocol_version;
    let mut env = TestEnv::builder(ChainGenesis::test())
        .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
        .build();

    let tx = env.meta_tx_from_actions(actions, user, relayer, receiver);

    match env.execute_tx(tx) {
        Ok(outcome) => outcome.status,
        Err(status) => FinalExecutionStatus::Failure(TxExecutionError::InvalidTxError(status)),
    }
}

/// Basic test to ensure the happy path works.
#[test]
fn accept_valid_meta_tx() {
    let protocol_version = ProtocolFeature::DelegateAction.protocol_version();
    let status = exec_meta_transaction(vec![], protocol_version);
    assert!(matches!(status, FinalExecutionStatus::SuccessValue(_)), "{status:?}",);
}

/// During the protocol upgrade phase, before the voting completes, we must not
/// include meta transaction on the chain.
///
/// Imagine a validator with an updated binary. A malicious node sends it a meta
/// transaction to execute before the upgrade has finished. We must ensure the
/// validator will not attempt adding it to the change unless the protocol
/// upgrade has completed.
///
/// Note: This does not prevent problems on the network layer that might arise
/// by having different interpretation of what a valid `SignedTransaction` might
/// be. We must catch that earlier.
#[test]
fn reject_valid_meta_tx_in_older_versions() {
    let protocol_version = ProtocolFeature::DelegateAction.protocol_version() - 1;

    let status = exec_meta_transaction(vec![], protocol_version);
    assert!(
        matches!(
                &status,
                FinalExecutionStatus::Failure(
                    TxExecutionError::InvalidTxError(
                        InvalidTxError::ActionsValidation(
                            ActionsValidationError::UnsupportedProtocolFeature{ protocol_feature, version }
                        )
                    )
                )
                if protocol_feature == "DelegateAction" && *version == ProtocolFeature::DelegateAction.protocol_version()
        ),
        "{status:?}",
    );
}

/// Take a list of actions and execute them as a meta transaction, check
/// everything executes successfully, return balance differences for the sender,
/// relayer, and receiver.
///
/// This is a common checker function used by the tests below.
fn check_meta_tx_execution(
    node: &impl Node,
    actions: Vec<Action>,
    sender: AccountId,
    relayer: AccountId,
    receiver: AccountId,
) -> (FinalExecutionOutcomeView, i128, i128, i128) {
    let node_user = node.user();

    assert_eq!(
        relayer,
        node.account_id().unwrap(),
        "the relayer must be the signer in meta transactions"
    );

    let sender_before = node_user.view_balance(&sender).unwrap();
    let relayer_before = node_user.view_balance(&relayer).unwrap();
    let receiver_before = node_user.view_balance(&receiver).unwrap();
    let relayer_nonce_before = node_user
        .get_access_key(&relayer, &PublicKey::from_seed(KeyType::ED25519, &relayer))
        .unwrap()
        .nonce;
    let user_nonce_before = node_user
        .get_access_key(&sender, &PublicKey::from_seed(KeyType::ED25519, &sender))
        .unwrap()
        .nonce;

    let tx_result = node_user
        .meta_tx(sender.clone(), receiver.clone(), relayer.clone(), actions.clone())
        .unwrap();

    // Execution of the transaction and all receipts should succeed
    tx_result.assert_success();

    // both nonces should be increased by 1
    let relayer_nonce = node_user
        .get_access_key(&relayer, &PublicKey::from_seed(KeyType::ED25519, &relayer))
        .unwrap()
        .nonce;
    assert_eq!(relayer_nonce, relayer_nonce_before + 1);
    // user key must be checked for existence (to test DeleteKey action)
    if let Ok(user_nonce) = node_user
        .get_access_key(&sender, &PublicKey::from_seed(KeyType::ED25519, &sender))
        .map(|key| key.nonce)
    {
        assert_eq!(user_nonce, user_nonce_before + 1);
    }

    let sender_after = node_user.view_balance(&sender).unwrap_or(0);
    let relayer_after = node_user.view_balance(&relayer).unwrap_or(0);
    let receiver_after = node_user.view_balance(&receiver).unwrap_or(0);

    let sender_diff = sender_after as i128 - sender_before as i128;
    let relayer_diff = relayer_after as i128 - relayer_before as i128;
    let receiver_diff = receiver_after as i128 - receiver_before as i128;
    (tx_result, sender_diff, relayer_diff, receiver_diff)
}

/// Call `check_meta_tx_execution` and perform gas checks for non function call actions.
///
/// This is a common checker function used by the tests below.
fn check_meta_tx_no_fn_call(
    node: &impl Node,
    actions: Vec<Action>,
    normal_tx_cost: Balance,
    tokens_transferred: Balance,
    sender: AccountId,
    relayer: AccountId,
    receiver: AccountId,
) -> FinalExecutionOutcomeView {
    let fee_helper = fee_helper(node);
    let gas_cost = normal_tx_cost + fee_helper.meta_tx_overhead_cost(&actions);

    let (tx_result, sender_diff, relayer_diff, receiver_diff) =
        check_meta_tx_execution(node, actions, sender, relayer, receiver);

    assert_eq!(sender_diff, 0, "sender should not pay for anything");
    assert_eq!(receiver_diff, tokens_transferred as i128, "unexpected receiver balance");
    assert_eq!(
        relayer_diff,
        -((gas_cost + tokens_transferred) as i128),
        "unexpected relayer balance"
    );

    tx_result
}

/// Call `check_meta_tx_execution` and perform gas checks specific to function calls.
///
/// This is a common checker function used by the tests below.
/// It works for action lists that consists multiple function calls but adding
/// other action will mess up the gas checks.
fn check_meta_tx_fn_call(
    node: &impl Node,
    actions: Vec<Action>,
    msg_len: u64,
    tokens_transferred: Balance,
    sender: AccountId,
    relayer: AccountId,
    receiver: AccountId,
) -> FinalExecutionOutcomeView {
    let fee_helper = fee_helper(node);
    let num_fn_calls = actions.len();
    let meta_tx_overhead_cost = fee_helper.meta_tx_overhead_cost(&actions);

    let (tx_result, sender_diff, relayer_diff, receiver_diff) =
        check_meta_tx_execution(node, actions, sender, relayer, receiver);

    assert_eq!(sender_diff, 0, "sender should not pay for anything");

    // Assertions on receiver and relayer are tricky because of dynamic gas
    // costs and contract reward. We need to check in the function call receipt
    // how much gas was spent and subtract the base cost that is not part of the
    // dynamic cost. The contract reward can be inferred from that.

    // static send gas is paid and burnt upfront
    let static_send_gas = fee_helper.cfg.fee(ActionCosts::new_action_receipt).send_fee(false)
        + num_fn_calls as u64 * fee_helper.cfg.fee(ActionCosts::function_call_base).send_fee(false)
        + msg_len * fee_helper.cfg.fee(ActionCosts::function_call_byte).send_fee(false);
    // static execution gas burnt in the same receipt as the function calls but
    // it doesn't contribute to the contract reward
    let static_exec_gas = fee_helper.cfg.fee(ActionCosts::new_action_receipt).exec_fee()
        + num_fn_calls as u64 * fee_helper.cfg.fee(ActionCosts::function_call_base).exec_fee()
        + msg_len * fee_helper.cfg.fee(ActionCosts::function_call_byte).exec_fee();

    // calculate contract rewards as reward("gas burnt in fn call receipt" - "static exec costs")
    let gas_burnt_for_function_call =
        tx_result.receipts_outcome[1].outcome.gas_burnt - static_send_gas;
    let dyn_cost = fee_helper.gas_to_balance(gas_burnt_for_function_call);
    let contract_reward = fee_helper.gas_burnt_to_reward(gas_burnt_for_function_call);

    // the relayer pays all gas and tokens
    let gas_cost =
        meta_tx_overhead_cost + fee_helper.gas_to_balance(static_exec_gas + static_send_gas);
    let expected_relayer_cost = (gas_cost + tokens_transferred + dyn_cost) as i128;
    assert_eq!(relayer_diff, -expected_relayer_cost, "unexpected relayer balance");

    // the receiver gains transferred tokens and the contract reward
    let expected_receiver_gain = (tokens_transferred + contract_reward) as i128;
    assert_eq!(receiver_diff, expected_receiver_gain, "unexpected receiver balance");

    tx_result
}

/// The simplest non-empty meta transaction: Transferring some NEAR tokens.
///
/// Note: The expectation is that the relayer pays for the tokens sent, as
/// specified in NEP-366.
#[test]
fn meta_tx_near_transfer() {
    let sender = bob_account();
    let relayer = alice_account();
    let receiver = carol_account();
    let node = RuntimeNode::new(&relayer);
    let fee_helper = fee_helper(&node);

    let amount = nearcore::NEAR_BASE;
    let actions = vec![Action::Transfer(TransferAction { deposit: amount })];
    let tx_cost = fee_helper.transfer_cost();
    check_meta_tx_no_fn_call(&node, actions, tx_cost, amount, sender, relayer, receiver);
}

/// Call a function on the test contract provided by default in the test environment.
#[test]
fn meta_tx_fn_call() {
    let sender = bob_account();
    let relayer = alice_account();
    let receiver = carol_account();
    let node = RuntimeNode::new(&relayer);

    let method_name = "log_something".to_owned();
    let method_name_len = method_name.len() as u64;
    let actions = vec![Action::FunctionCall(FunctionCallAction {
        method_name,
        args: vec![],
        gas: 30_000_000_000_000,
        deposit: 0,
    })];

    let outcome =
        check_meta_tx_fn_call(&node, actions, method_name_len, 0, sender, relayer, receiver);

    // Check that the function call was executed as expected
    let fn_call_logs = &outcome.receipts_outcome[1].outcome.logs;
    assert_eq!(fn_call_logs, &vec!["hello".to_owned()]);
}

#[test]
fn meta_tx_deploy() {
    let sender = bob_account();
    let relayer = alice_account();
    // Can only deploy on own account
    let receiver = sender.clone();
    let node = RuntimeNode::new(&relayer);
    let fee_helper = fee_helper(&node);

    let code = smallest_rs_contract().to_vec();
    let tx_cost = fee_helper.deploy_contract_cost(code.len() as u64);
    let actions = vec![Action::DeployContract(DeployContractAction { code })];
    check_meta_tx_no_fn_call(&node, actions, tx_cost, 0, sender, relayer, receiver);
}

#[test]
fn meta_tx_stake() {
    let sender = bob_account();
    let relayer = alice_account();
    // Can only stake on own account
    let receiver = sender.clone();
    let node = RuntimeNode::new(&relayer);
    let fee_helper = fee_helper(&node);

    let tx_cost = fee_helper.stake_cost();
    let public_key = create_user_test_signer(&sender).public_key;
    let actions = vec![Action::Stake(StakeAction { public_key, stake: 0 })];
    check_meta_tx_no_fn_call(
        &node,
        actions,
        tx_cost,
        0,
        sender.clone(),
        relayer.clone(),
        receiver.clone(),
    );
}

#[test]
fn meta_tx_add_key() {
    let sender = bob_account();
    let relayer = alice_account();
    // Can only add key on own account
    let receiver = sender.clone();
    let node = RuntimeNode::new(&relayer);
    let fee_helper = fee_helper(&node);

    let tx_cost = fee_helper.add_key_full_cost();
    // any public key works as long as it doesn't exists on the receiver, the
    // relayer public key is just handy
    let public_key = node.signer().public_key();
    let actions = vec![Action::AddKey(AddKeyAction {
        public_key: public_key.clone(),
        access_key: AccessKey::full_access(),
    })];
    check_meta_tx_no_fn_call(&node, actions, tx_cost, 0, sender, relayer, receiver.clone());

    let key_view = node
        .user()
        .get_access_key(&receiver, &public_key)
        .expect("looking up key that was just added failed");
    assert_eq!(
        key_view.permission,
        AccessKeyPermissionView::FullAccess,
        "wrong permissions for new key"
    );
}

#[test]
fn meta_tx_delete_key() {
    let sender = bob_account();
    let relayer = alice_account();
    // Can only delete keys on own account
    let receiver = sender.clone();
    let node = RuntimeNode::new(&relayer);
    let fee_helper = fee_helper(&node);

    let tx_cost = fee_helper.delete_key_cost();
    let public_key = PublicKey::from_seed(KeyType::ED25519, &receiver);
    let actions = vec![Action::DeleteKey(DeleteKeyAction { public_key: public_key.clone() })];
    check_meta_tx_no_fn_call(&node, actions, tx_cost, 0, sender, relayer, receiver.clone());

    let err = node
        .user()
        .get_access_key(&receiver, &public_key)
        .expect_err("key should have been deleted");
    assert_eq!(err, "Access key for public key #ed25519:4mhK4txd8Z5r71iCZ41UguSHuHFKUeCXPHv646DbQPYi does not exist");
}

#[test]
fn meta_tx_delete_account() {
    let relayer = alice_account();
    let sender = eve_dot_alice_account();
    let receiver = sender.clone();
    let node = RuntimeNode::new(&relayer);

    // setup: create new account because the standard accounts are validators (can't be deleted)
    let balance = NEAR_BASE;
    node.user()
        .create_account(
            relayer.clone(),
            sender.clone(),
            PublicKey::from_seed(KeyType::ED25519, &sender),
            balance,
        )
        .expect("account setup failed")
        .assert_success();

    let fee_helper = fee_helper(&node);

    let actions =
        vec![Action::DeleteAccount(DeleteAccountAction { beneficiary_id: relayer.clone() })];

    // special case balance check for deleting account
    let gas_cost =
        fee_helper.prepaid_delete_account_cost() + fee_helper.meta_tx_overhead_cost(&actions);
    let (_tx_result, sender_diff, relayer_diff, receiver_diff) =
        check_meta_tx_execution(&node, actions, sender, relayer, receiver.clone());

    assert_eq!(
        sender_diff,
        -(balance as i128),
        "sender should be deleted and thus have zero balance"
    );
    assert_eq!(sender_diff, receiver_diff);
    assert_eq!(relayer_diff, balance as i128 - (gas_cost as i128), "unexpected relayer balance");
    let err = node.view_account(&receiver).expect_err("account should have been deleted");
    assert_eq!(err, "Account ID #eve.alice.near does not exist");
}

/// Test the canonical example for meta transactions: A fungible token transfer.
///
/// Scenario: Bob sends some Carol-FT to David without requiring any NEAR tokens
/// to purchase gas. Alice acts as a relayer.
#[test]
fn meta_tx_ft_transfer() {
    let relayer = alice_account();
    let sender = bob_account();
    let ft_contract = carol_account();
    let receiver = "david.near";

    let mut genesis = Genesis::test(vec![alice_account(), bob_account(), carol_account()], 3);
    add_contract(&mut genesis, &ft_contract, near_test_contracts::ft_contract().to_vec());
    let node = RuntimeNode::new_from_genesis(&relayer, genesis);

    // A BUNCH OF TEST SETUP
    // initialize the contract
    node.user()
        .function_call(
            relayer.clone(),
            ft_contract.clone(),
            "new_default_meta",
            // make the relayer (alice) owner, makes initialization easier
            br#"{"owner_id": "alice.near", "total_supply": "1000000"}"#.to_vec(),
            30_000_000_000_000,
            0,
        )
        .expect("FT contract initialization failed")
        .assert_success();

    // register sender & receiver FT accounts
    let actions = vec![ft_register_action(&sender), ft_register_action(&receiver)];
    node.user()
        .sign_and_commit_actions(relayer.clone(), ft_contract.clone(), actions)
        .expect("registering FT accounts")
        .assert_success();
    // initialize sender balance
    let actions = vec![ft_transfer_action(&sender, 10_000).0];
    node.user()
        .sign_and_commit_actions(relayer.clone(), ft_contract.clone(), actions)
        .expect("initializing sender balance failed")
        .assert_success();

    // START OF META TRANSACTION
    // 1% fee to the relayer
    let (action0, bytes0) = ft_transfer_action(&relayer, 10);
    // the actual transfer
    let (action1, bytes1) = ft_transfer_action(receiver, 1000);
    let actions = vec![action0, action1];

    let outcome = check_meta_tx_fn_call(
        &node,
        actions,
        bytes0 + bytes1,
        2,
        sender.clone(),
        relayer.clone(),
        ft_contract.clone(),
    );

    // Check that the function call was executed as expected, according to NEP-141 events.
    let fn_call_logs = &outcome.receipts_outcome[1].outcome.logs;

    assert_eq!(2, fn_call_logs.len(), "expected 2 JSON events but found {fn_call_logs:?}");
    assert_eq!(
        fn_call_logs[0],
        ft_transfer_event(&sender, &relayer, 10),
        "relayer event looks wrong"
    );
    assert_eq!(
        fn_call_logs[1],
        ft_transfer_event(&sender, &receiver, 1000),
        "receiver event looks wrong"
    );

    // Also check FT balances
    assert_ft_balance(&node, &ft_contract, &receiver, 1000);
    assert_ft_balance(&node, &ft_contract, &sender, 10_000 - 1000 - 10);
    assert_ft_balance(&node, &ft_contract, &relayer, 1_000_000 - 10_000 + 10);
}

/// Construct an function call action with a FT transfer.
///
/// Returns the action and the number of bytes for gas charges.
fn ft_transfer_action(receiver: &str, amount: u128) -> (Action, u64) {
    let args: Vec<u8> = format!(
        r#"{{
        "receiver_id": "{receiver}",
        "amount": "{amount}"
    }}"#
    )
    .bytes()
    .collect();
    let method_name = "ft_transfer".to_owned();
    let num_bytes = method_name.len() + args.len();
    let action = Action::FunctionCall(FunctionCallAction {
        method_name,
        args,
        gas: 20_000_000_000_000,
        deposit: 1,
    });

    (action, num_bytes as u64)
}

/// Add NEAR token balance to maintain the storage of an account, which
/// registers the user in the fungible contract account.
fn ft_register_action(receiver: &str) -> Action {
    let args: Vec<u8> = format!(
        r#"{{
        "account_id": "{receiver}"
    }}"#
    )
    .bytes()
    .collect();
    Action::FunctionCall(FunctionCallAction {
        method_name: "storage_deposit".to_owned(),
        args,
        gas: 20_000_000_000_000,
        deposit: NEAR_BASE,
    })
}

/// Format a NEP-141 event for an ft transfer
fn ft_transfer_event(sender: &str, receiver: &str, amount: u128) -> String {
    // This part is valid JSON, I would like to use the json!() macro but it
    // produces the fields out of order. This is valid for JSON but it will fail
    // the string comparison.
    // (Note: parsing the logs as JSON and comparing serde_json::Value instead
    // of string is not possible because the logs are only partially valid
    // JOSN...)
    let data_json = format!(
        r#"[{{"old_owner_id":"{sender}","new_owner_id":"{receiver}","amount":"{amount}"}}]"#
    );
    let json = format!(
        r#"{{"standard":"nep141","version":"1.0.0","event":"ft_transfer","data":{data_json}}}"#
    );
    // this part isn't even valid JSON
    format!("EVENT_JSON:{json}")
}

/// Asserts an FT balance for an account.
fn assert_ft_balance(
    node: &RuntimeNode,
    ft_contract: &AccountId,
    user: &str,
    expected_balance: Balance,
) {
    let response = node
        .user()
        .view_call(ft_contract, "ft_balance_of", format!(r#"{{"account_id":"{user}"}}"#).as_bytes())
        .expect("view call failed");
    let balance = std::str::from_utf8(&response.result).expect("invalid UTF8");
    assert_eq!(format!("\"{expected_balance}\""), balance);
}
