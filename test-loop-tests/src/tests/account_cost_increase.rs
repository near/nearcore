//! Cost tests for the `AccountCostIncrease` protocol feature.
//!
//! The feature buys the gas attached to a receipt at an inflated `min_gas_purchase_price`
//! (~10x) while still burning it at the regular price; the price difference is refunded after
//! execution. The only exception is account creation: a fixed `account_creation_charge` is
//! withheld from that refund, making account creation more expensive.
//!
//! These tests run a *scenario* (a transaction to execute) both before the feature
//! (protocol version `AccountCostIncrease - 1`) and after the feature (the latest protocol version),
//! and compare the cost. The cost is measured as the net balance delta across the payer's accounts,
//! any gas key the gas is drawn from, and any accounts a deposit is transferred to (so deposits
//! net out). That equals the gross tokens the transaction reports burnt, which the test
//! cross-checks against the transaction outcome. A function call key's allowance is checked
//! separately (it should change by exactly the amount the account was debited), since counting
//! it in the balance would double-count the gas — it is paid from the account, not the allowance.
//!
//! Expectations:
//! - A scenario that creates no account costs the same before and after — up to a small
//!   meta-transaction adjustment, since the feature moved the create_account / DSI cost from the
//!   send fee to the exec fee and a relayed transaction charges the send fee twice.
//! - A scenario that creates an account costs `account_creation_charge` more (minus the
//!   create-account exec gas already burned at the regular price), per account created, plus the
//!   same meta-transaction adjustment for relayed methods. The expected difference is computed
//!   from the runtime configs (see [`run_cost_test`]).
//!
//! The [`run_cost_test`] wrapper runs a single scenario across a list of submission methods and
//! checks the cost difference for each automatically: signed directly with a full access key, a
//! function call access key, or a gas key (full / function-call); and relayed as a meta
//! transaction whose outer signature is a full access key or a full-access gas key. For the
//! relayed methods only the relayer's outer signature varies — the inner delegate action is
//! always signed by a full access key, since that is what determines how the meta transaction is
//! paid for.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use near_async::time::Duration;
use near_client::QueryError;
use near_crypto::{InMemorySigner, KeyType, PublicKey, Signer};
use near_o11y::testonly::init_test_logger;
use near_primitives::account::AccessKeyPermission;
use near_primitives::account::{AccessKey, FunctionCallPermission};
use near_primitives::action::delegate::{DelegateAction, NonDelegateAction, SignedDelegateAction};
use near_primitives::action::{
    AddKeyAction, CreateAccountAction, DeterministicStateInitAction, GlobalContractDeployMode,
    GlobalContractIdentifier, TransferToGasKeyAction, UseGlobalContractAction,
};
use near_primitives::deterministic_account_id::{
    DeterministicAccountStateInit, DeterministicAccountStateInitV1,
};
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::{
    Action, FunctionCallAction, SignedTransaction, TransactionNonce, TransferAction,
};
use near_primitives::types::{AccountId, Balance, Gas, Nonce, NonceIndex};
use near_primitives::utils::derive_near_deterministic_account_id;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_primitives::views::{
    AccessKeyPermissionView, ExecutionStatusView, FinalExecutionOutcomeView, FinalExecutionStatus,
};
use node_runtime::config::total_send_fees;
use std::collections::BTreeMap;
use testlib::fees_utils::FeeHelper;

/// Gas price, pinned (min == max) so it stays constant for a deterministic before/after
/// comparison. Set to the realistic mainnet minimum gas price of 100 MyoctoNEAR/gas, which is
/// exactly 10x below the feature's `min_gas_purchase_price` (1 GyoctoNEAR/gas) — i.e. the gas
/// attached to receipts is genuinely purchased at 10x the price it is burned at.
const GAS_PRICE: Balance = Balance::from_yoctonear(100_000_000);
/// Initial balance of every genesis account used by the tests.
const INITIAL_BALANCE: Balance = Balance::from_near(1_000_000);
/// How much to fund a gas key with. Plenty to cover the inflated gas purchase price plus the
/// account creation charge, the bulk of which is refunded.
const GAS_KEY_FUND: Balance = Balance::from_near(1);
/// Number of nonces to allocate for a gas key.
const NUM_NONCES: NonceIndex = 3;
/// Allowance granted to a (non-gas) function call access key. Generous enough to cover the gas
/// purchased at the inflated price; it is decremented on charge and restored by the gas refund.
const FUNCTION_CALL_KEY_ALLOWANCE: Balance = Balance::from_near(1);
/// Gas attached to function-call transactions.
const FUNCTION_CALL_GAS: Gas = Gas::from_teragas(30);
/// Gas attached to the `call_promise` transaction that spawns the account-creating receipts.
const CALL_PROMISE_GAS: Gas = Gas::from_teragas(100);
/// Amount transferred in transfer scenarios.
const TRANSFER_AMOUNT: Balance = Balance::from_millinear(1);
/// Large epoch length so the chain doesn't upgrade its protocol version away from the genesis
/// version while the (short) test runs.
const EPOCH_LENGTH: u64 = 100_000;

/// The different ways a transaction can be submitted, all of which the feature must treat
/// consistently.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SubmitMethod {
    /// Signed directly with the account's full access key.
    FullAccessKey,
    /// Signed directly with a function call access key (carrying a finite allowance).
    FunctionCallKey,
    /// Signed with a full-access gas key (gas paid from the gas key balance).
    GasKeyFullAccess,
    /// Signed with a function-call gas key.
    GasKeyFunctionCall,
    /// Relayer signs (and pays for) the outer transaction with a full access key; the inner
    /// delegate action is signed by the sender's full access key.
    DelegateFullAccessKey,
    /// Relayer signs (and pays for) the outer transaction with a full-access gas key; the inner
    /// delegate action is signed by the sender's full access key. Only the relayer's (outer)
    /// signature determines how the meta transaction is paid for, so the inner signature is held
    /// fixed at a full access key.
    DelegateGasKeyFullAccess,
}

use SubmitMethod::*;

const ALL_METHODS: &[SubmitMethod] = &[
    FullAccessKey,
    FunctionCallKey,
    GasKeyFullAccess,
    GasKeyFunctionCall,
    DelegateFullAccessKey,
    DelegateGasKeyFullAccess,
];

/// Methods able to submit arbitrary actions (everything that a function-call-restricted key
/// can't do, such as creating an account).
const FULL_ACCESS_METHODS: &[SubmitMethod] =
    &[FullAccessKey, GasKeyFullAccess, DelegateFullAccessKey, DelegateGasKeyFullAccess];

impl SubmitMethod {
    fn is_delegate(self) -> bool {
        matches!(self, DelegateFullAccessKey | DelegateGasKeyFullAccess)
    }
}

/// The gas key (account, public key) that pays for `method`'s transaction, whose balance must be
/// tracked when measuring cost. For the direct gas-key methods it is on the actor; for
/// [`DelegateGasKeyFullAccess`] it is on the relayer (which signs the outer transaction with it).
fn tracked_gas_key(
    method: SubmitMethod,
    accounts: &MethodAccounts,
) -> Option<(AccountId, PublicKey)> {
    match method {
        GasKeyFullAccess | GasKeyFunctionCall => {
            Some((accounts.actor.clone(), gas_key_signer(&accounts.actor).public_key()))
        }
        DelegateGasKeyFullAccess => {
            Some((accounts.relayer.clone(), gas_key_signer(&accounts.relayer).public_key()))
        }
        FullAccessKey | FunctionCallKey | DelegateFullAccessKey => None,
    }
}

/// A scenario is a transaction to run, described independently of how it is submitted.
struct Scenario {
    name: &'static str,
    /// Number of accounts the scenario creates. Determines the expected extra charge.
    accounts_created: u64,
    /// Whether the test contract must be deployed first (function-call / call_promise scenarios).
    needs_contract: bool,
    /// Whether a global contract must be deployed first (deterministic-state-init scenarios).
    needs_global_contract: bool,
    /// Builds the transaction from this method's accounts.
    build: fn(accounts: &MethodAccounts) -> ScenarioTx,
}

/// The account that holds the global contract referenced by deterministic-state-init scenarios.
fn global_contract_account() -> AccountId {
    create_account_id("global-contract")
}

/// The transaction a scenario produces for a given method.
struct ScenarioTx {
    /// The transaction receiver.
    receiver: AccountId,
    /// The actions to execute.
    actions: Vec<Action>,
    /// Accounts that receive a *deposit* from the transaction and must be tracked so the deposit
    /// nets out of the cost. The reported `tokens_burnt` is gross — it excludes deposits but
    /// includes the function-call reward — so reward recipients (e.g. the called contract) must
    /// NOT be listed here, only accounts a deposit is transferred to.
    deposit_recipients: Vec<AccountId>,
    /// Whether the transaction is expected to succeed.
    expect_success: bool,
    /// Actions run via the actor's full access key to `receiver` before measuring (not counted),
    /// e.g. to pre-create the receiver so the measured transaction does not create it.
    pre_setup: Vec<Action>,
}

impl ScenarioTx {
    /// A successful transaction with no deposit recipients and no pre-setup.
    fn new(receiver: AccountId, actions: Vec<Action>) -> Self {
        ScenarioTx {
            receiver,
            actions,
            deposit_recipients: vec![],
            expect_success: true,
            pre_setup: vec![],
        }
    }
}

/// The accounts a single submission method operates on. Each method gets its own set so they
/// don't interfere when sharing an environment.
struct MethodAccounts {
    /// The account whose key authorizes the inner actions (the receipt predecessor).
    actor: AccountId,
    /// Relayer that submits and pays for delegate actions (unused by direct methods).
    relayer: AccountId,
    /// Account where the test contract is deployed for function-call scenarios.
    contract: AccountId,
    /// A pre-existing named account, used as the recipient of a plain transfer.
    receiver: AccountId,
    /// Index of this method, used to derive unique implicit account ids per method.
    index: usize,
}

fn method_accounts(index: usize) -> MethodAccounts {
    MethodAccounts {
        actor: create_account_id(&format!("actor{index}")),
        relayer: create_account_id(&format!("relayer{index}")),
        contract: create_account_id(&format!("contract{index}")),
        receiver: create_account_id(&format!("receiver{index}")),
        index,
    }
}

/// The full-access key seeded for every actor by `create_user_test_signer`.
fn full_signer(account: &AccountId) -> Signer {
    create_user_test_signer(account)
}

/// A function-call access key, seeded deterministically per account.
fn function_call_signer(account: &AccountId) -> Signer {
    InMemorySigner::from_seed(account.clone(), KeyType::ED25519, "fc_key").into()
}

/// A gas key, seeded deterministically per account.
fn gas_key_signer(account: &AccountId) -> Signer {
    InMemorySigner::from_seed(account.clone(), KeyType::ED25519, "gas_key").into()
}

fn build_env(protocol_version: u32) -> TestLoopEnv {
    let mut accounts: Vec<AccountId> = (0..ALL_METHODS.len())
        .flat_map(|i| {
            let a = method_accounts(i);
            [a.actor, a.relayer, a.contract, a.receiver]
        })
        .collect();
    accounts.push(global_contract_account());
    TestLoopBuilder::new()
        .enable_rpc()
        .protocol_version(protocol_version)
        .epoch_length(EPOCH_LENGTH)
        .gas_prices(GAS_PRICE, GAS_PRICE)
        .add_user_accounts(&accounts, INITIAL_BALANCE)
        .build()
}

/// Assert that the chain is currently running with the expected `AccountCostIncrease` state.
/// Guards against the protocol version drifting (upgrading) mid-test.
fn assert_feature_state(env: &TestLoopEnv, expected_enabled: bool) {
    let head = env.rpc_node().head();
    let protocol_version =
        env.rpc_node().client().epoch_manager.get_epoch_protocol_version(&head.epoch_id).unwrap();
    assert_eq!(
        ProtocolFeature::AccountCostIncrease.enabled(protocol_version),
        expected_enabled,
        "unexpected AccountCostIncrease state at protocol version {protocol_version}",
    );
}

/// Balance of an account, treating a missing account as zero.
fn balance_or_zero(env: &TestLoopEnv, account: &AccountId) -> Balance {
    match env.rpc_node().view_account_query(account) {
        Ok(view) => view.amount,
        Err(QueryError::UnknownAccount { .. }) => Balance::ZERO,
        Err(err) => panic!("unexpected query error for {account}: {err:?}"),
    }
}

/// Balance held by a gas key (a real, separate pool of tokens the gas is paid from).
fn gas_key_balance(env: &TestLoopEnv, account: &AccountId, public_key: &PublicKey) -> Balance {
    let view = env.rpc_node().view_access_key_query(account, public_key).unwrap();
    match view.permission {
        AccessKeyPermissionView::GasKeyFullAccess { balance, .. }
        | AccessKeyPermissionView::GasKeyFunctionCall { balance, .. } => balance,
        other => panic!("expected gas key, got {other:?}"),
    }
}

/// Remaining allowance of a function call access key. The allowance is a spending budget that is
/// decremented when gas is charged and restored by the gas refund, so its net change equals the
/// gas actually spent. Unlike a gas key balance it is *not* a separate pool of tokens (the gas is
/// paid from the account), so it must not be added to [`total_user_balance`] — instead we check
/// it changed by exactly the measured cost.
fn function_call_key_allowance(
    env: &TestLoopEnv,
    account: &AccountId,
    public_key: &PublicKey,
) -> Balance {
    let view = env.rpc_node().view_access_key_query(account, public_key).unwrap();
    match view.permission {
        AccessKeyPermissionView::FunctionCall { allowance, .. } => {
            allowance.expect("function call key should have a finite allowance")
        }
        other => panic!("expected function call key, got {other:?}"),
    }
}

/// Sum of the balances the user controls: the main balance of every tracked account, plus the
/// balance of every tracked gas key.
fn total_user_balance(
    env: &TestLoopEnv,
    accounts: &[AccountId],
    gas_keys: &[(AccountId, PublicKey)],
) -> Balance {
    let mut sum = Balance::ZERO;
    for account in accounts {
        sum = sum.checked_add(balance_or_zero(env, account)).unwrap();
    }
    for (account, public_key) in gas_keys {
        sum = sum.checked_add(gas_key_balance(env, account, public_key)).unwrap();
    }
    sum
}

/// Next nonce for the access key identified by `public_key` on `account`.
fn next_key_nonce(env: &TestLoopEnv, account: &AccountId, public_key: &PublicKey) -> Nonce {
    env.rpc_node().view_access_key_query(account, public_key).unwrap().nonce + 1
}

/// Sum of tokens burnt across the transaction and all its receipt outcomes — the fees the
/// transaction actually cost (gas burnt + refund penalty + account-creation charge, minus the
/// function-call reward that is paid back to the contract).
fn total_tokens_burnt(outcome: &FinalExecutionOutcomeView) -> Balance {
    let mut sum = outcome.transaction_outcome.outcome.tokens_burnt;
    for receipt in &outcome.receipts_outcome {
        sum = sum.checked_add(receipt.outcome.tokens_burnt).unwrap();
    }
    sum
}

/// A distinct NEAR-implicit account id (64 lowercase hex chars) for the given method index.
fn near_implicit_account(index: usize) -> AccountId {
    format!("{:064x}", 0xa11ce0000u64 + index as u64).parse().unwrap()
}

/// A distinct ETH-implicit account id (`0x` + 40 lowercase hex chars) for the given method index.
fn eth_implicit_account(index: usize) -> AccountId {
    format!("0x{:040x}", 0xe1100000u64 + index as u64).parse().unwrap()
}

/// Deploy the test contract as a global contract (by account id) from [`global_contract_account`].
/// Setup, not measured. Deterministic-state-init scenarios reference it.
fn deploy_global_contract(env: &mut TestLoopEnv) {
    let account = global_contract_account();
    let tx = env.rpc_node().tx_deploy_global_contract(
        &account,
        near_test_contracts::rs_contract().to_vec(),
        GlobalContractDeployMode::AccountId,
    );
    env.rpc_runner().run_tx(tx, Duration::seconds(5));
    env.rpc_runner().run_for_number_of_blocks(1);
}

/// A `DeterministicStateInit` action and the deterministic account id it creates. The state is
/// kept tiny and varied per method index so each method gets a distinct (zero-balance) account.
fn deterministic_state_init(index: usize) -> (AccountId, Action) {
    let data = BTreeMap::from([(b"m".to_vec(), vec![index as u8])]);
    let state_init = DeterministicAccountStateInit::V1(DeterministicAccountStateInitV1 {
        code: GlobalContractIdentifier::AccountId(global_contract_account()),
        data,
    });
    let account = derive_near_deterministic_account_id(&state_init);
    let action = Action::DeterministicStateInit(Box::new(DeterministicStateInitAction {
        state_init,
        deposit: Balance::ZERO,
    }));
    (account, action)
}

/// Run a self-transaction signed by `signer_account`'s full access key (not measured). Used for
/// setup steps such as key creation and gas key funding.
fn run_full_key_tx(env: &mut TestLoopEnv, signer_account: &AccountId, actions: Vec<Action>) {
    run_full_key_tx_to(env, signer_account, signer_account, actions);
}

/// Run a transaction from `signer_account` (full access key) to `receiver`, waiting for success.
/// Not measured; used for setup such as pre-creating the receiver account.
fn run_full_key_tx_to(
    env: &mut TestLoopEnv,
    signer_account: &AccountId,
    receiver: &AccountId,
    actions: Vec<Action>,
) {
    let nonce = env.rpc_node().get_next_nonce(signer_account);
    let block_hash = env.rpc_node().head().last_block_hash;
    let tx = SignedTransaction::from_actions(
        nonce,
        signer_account.clone(),
        receiver.clone(),
        &full_signer(signer_account),
        actions,
        block_hash,
    );
    env.rpc_runner().run_tx(tx, Duration::seconds(5));
    env.rpc_runner().run_for_number_of_blocks(1);
}

/// Perform the on-chain setup a method needs before it can submit the scenario: deploy the
/// contract for function-call scenarios, add and fund the relevant key.
fn setup_method(
    env: &mut TestLoopEnv,
    method: SubmitMethod,
    accounts: &MethodAccounts,
    needs_contract: bool,
) {
    let actor = &accounts.actor;
    let contract = &accounts.contract;

    if needs_contract {
        let deploy_tx = env.rpc_node().tx_deploy_test_contract(contract);
        env.rpc_runner().run_tx(deploy_tx, Duration::seconds(5));
        env.rpc_runner().run_for_number_of_blocks(1);
    }

    // A function-call permission limited to the contract account.
    let fc_permission = |allowance: Option<Balance>| FunctionCallPermission {
        allowance,
        receiver_id: contract.to_string(),
        method_names: vec![],
    };

    match method {
        FunctionCallKey => {
            let access_key = AccessKey {
                nonce: 0,
                // A regular function call key carries a finite allowance.
                permission: AccessKeyPermission::FunctionCall(fc_permission(Some(
                    FUNCTION_CALL_KEY_ALLOWANCE,
                ))),
            };
            run_full_key_tx(
                env,
                actor,
                vec![Action::AddKey(Box::new(AddKeyAction {
                    public_key: function_call_signer(actor).public_key(),
                    access_key,
                }))],
            );
        }
        GasKeyFullAccess => {
            add_gas_key(env, actor, AccessKey::gas_key_full_access(NUM_NONCES));
        }
        GasKeyFunctionCall => {
            // Gas keys require an unlimited (None) allowance.
            add_gas_key(
                env,
                actor,
                AccessKey::gas_key_function_call(NUM_NONCES, fc_permission(None)),
            );
        }
        // The relayer signs and pays for the meta transaction with a funded full-access gas key.
        DelegateGasKeyFullAccess => {
            add_gas_key(env, &accounts.relayer, AccessKey::gas_key_full_access(NUM_NONCES));
        }
        FullAccessKey | DelegateFullAccessKey => {}
    }
}

/// Add a gas key to `actor` and fund it with [`GAS_KEY_FUND`].
fn add_gas_key(env: &mut TestLoopEnv, actor: &AccountId, access_key: AccessKey) {
    let public_key = gas_key_signer(actor).public_key();
    run_full_key_tx(
        env,
        actor,
        vec![Action::AddKey(Box::new(AddKeyAction { public_key: public_key.clone(), access_key }))],
    );
    run_full_key_tx(
        env,
        actor,
        vec![Action::TransferToGasKey(Box::new(TransferToGasKeyAction {
            public_key,
            deposit: GAS_KEY_FUND,
        }))],
    );
}

/// Submit the scenario via `method`, assert its success matches `expect_success`, wait for its
/// refund receipts to settle, and return the final outcome.
fn submit_scenario(
    env: &mut TestLoopEnv,
    method: SubmitMethod,
    accounts: &MethodAccounts,
    receiver: &AccountId,
    actions: Vec<Action>,
    expect_success: bool,
) -> FinalExecutionOutcomeView {
    let actor = &accounts.actor;
    let block_hash = env.rpc_node().head().last_block_hash;

    let tx = match method {
        FullAccessKey => {
            let nonce = env.rpc_node().get_next_nonce(actor);
            SignedTransaction::from_actions(
                nonce,
                actor.clone(),
                receiver.clone(),
                &full_signer(actor),
                actions,
                block_hash,
            )
        }
        FunctionCallKey => {
            let signer = function_call_signer(actor);
            let nonce = next_key_nonce(env, actor, &signer.public_key());
            SignedTransaction::from_actions(
                nonce,
                actor.clone(),
                receiver.clone(),
                &signer,
                actions,
                block_hash,
            )
        }
        GasKeyFullAccess | GasKeyFunctionCall => {
            let signer = gas_key_signer(actor);
            let nonces =
                env.rpc_node().view_gas_key_nonces_query(actor, &signer.public_key()).unwrap();
            SignedTransaction::from_actions_v1(
                TransactionNonce::from_nonce_and_index(nonces[0] + 1, 0),
                actor.clone(),
                receiver.clone(),
                &signer,
                actions,
                block_hash,
            )
        }
        DelegateFullAccessKey | DelegateGasKeyFullAccess => {
            // The inner delegate action is always signed by the sender's full access key; only the
            // relayer's outer signature varies between the two delegate methods.
            let inner_signer = full_signer(actor);
            let relayer = &accounts.relayer;
            let tip = env.rpc_node().head();
            let delegate_action = DelegateAction {
                sender_id: actor.clone(),
                receiver_id: receiver.clone(),
                actions: actions
                    .into_iter()
                    .map(|action| NonDelegateAction::try_from(action).unwrap())
                    .collect(),
                nonce: next_key_nonce(env, actor, &inner_signer.public_key()),
                max_block_height: tip.height + 100,
                public_key: inner_signer.public_key(),
            };
            let signature = inner_signer.sign(delegate_action.get_nep461_hash().as_bytes());
            let signed_delegate_action = SignedDelegateAction { delegate_action, signature };
            let outer_actions = vec![Action::Delegate(Box::new(signed_delegate_action))];

            if method == DelegateGasKeyFullAccess {
                // The relayer signs (and pays for) the outer transaction with its gas key.
                let relayer_gas_signer = gas_key_signer(relayer);
                let nonces = env
                    .rpc_node()
                    .view_gas_key_nonces_query(relayer, &relayer_gas_signer.public_key())
                    .unwrap();
                SignedTransaction::from_actions_v1(
                    TransactionNonce::from_nonce_and_index(nonces[0] + 1, 0),
                    relayer.clone(),
                    actor.clone(),
                    &relayer_gas_signer,
                    outer_actions,
                    block_hash,
                )
            } else {
                let relayer_nonce = env.rpc_node().get_next_nonce(relayer);
                SignedTransaction::from_actions(
                    relayer_nonce,
                    relayer.clone(),
                    actor.clone(),
                    &full_signer(relayer),
                    outer_actions,
                    block_hash,
                )
            }
        }
    };

    let outcome = env.rpc_runner().execute_tx(tx, Duration::seconds(5)).unwrap();
    // "Succeeded" means nothing failed anywhere in the receipt tree. We can't use the top-level
    // status alone: for a meta transaction whose inner action fails, the outer delegate receipt
    // (and thus the transaction) still reports success while an inner receipt reports failure.
    let tx_failed = matches!(outcome.status, FinalExecutionStatus::Failure(_));
    let any_receipt_failed = outcome
        .receipts_outcome
        .iter()
        .any(|r| matches!(r.outcome.status, ExecutionStatusView::Failure(_)));
    let succeeded = !tx_failed && !any_receipt_failed;
    assert_eq!(
        succeeded, expect_success,
        "unexpected scenario outcome (status {:?})",
        outcome.status,
    );

    // Let the gas-refund (and any other) receipts settle into the queried state.
    env.rpc_runner().run_for_number_of_blocks(3);
    outcome
}

/// Run `scenario` via `method` in `env` and return the cost: the net decrease across every
/// account the user controls (payer + relayer + receiver + any extra recipients), which equals
/// the tokens burnt by the transaction. Cross-checks that against the transaction outcome, and
/// for the function-call-key method checks the allowance changed by the account's gas debit.
fn measure_cost(env: &mut TestLoopEnv, method: SubmitMethod, scenario: &Scenario) -> Balance {
    let index = ALL_METHODS.iter().position(|m| *m == method).unwrap();
    let accounts = method_accounts(index);

    setup_method(env, method, &accounts, scenario.needs_contract);

    let tx = (scenario.build)(&accounts);

    // Pre-setup, e.g. to pre-create the receiver. Run via the actor's full key, not measured.
    if !tx.pre_setup.is_empty() {
        run_full_key_tx_to(env, &accounts.actor, &tx.receiver, tx.pre_setup.clone());
    }

    // Track the payer's accounts plus the accounts a deposit is transferred to. Deposits net out
    // (they leave the payer and arrive at the recipient, both tracked), so the cost reduces to the
    // gross tokens burnt — which the reported `tokens_burnt` also is (it excludes deposits but
    // includes the function-call reward, so reward recipients are deliberately not tracked).
    let mut tracked = vec![accounts.actor.clone()];
    if method.is_delegate() {
        tracked.push(accounts.relayer.clone());
    }
    tracked.extend(tx.deposit_recipients.iter().cloned());
    tracked.sort();
    tracked.dedup();

    let gas_keys: Vec<_> = tracked_gas_key(method, &accounts).into_iter().collect();

    // A function call access key's allowance is decremented by the gas charged to the account and
    // restored by the gas refund, so it changes by exactly the account's net debit. Track it
    // separately (counting it in the balance would double-count the gas).
    let fc_allowance_key = (method == FunctionCallKey)
        .then(|| (accounts.actor.clone(), function_call_signer(&accounts.actor).public_key()));

    let actor_before = balance_or_zero(env, &accounts.actor);
    let allowance_before =
        fc_allowance_key.as_ref().map(|(a, pk)| function_call_key_allowance(env, a, pk));
    let before = total_user_balance(env, &tracked, &gas_keys);

    let outcome =
        submit_scenario(env, method, &accounts, &tx.receiver, tx.actions, tx.expect_success);

    let after = total_user_balance(env, &tracked, &gas_keys);
    let actor_after = balance_or_zero(env, &accounts.actor);
    let allowance_after =
        fc_allowance_key.as_ref().map(|(a, pk)| function_call_key_allowance(env, a, pk));

    let cost = before
        .checked_sub(after)
        .unwrap_or_else(|| panic!("cost is negative: balance grew from {before} to {after}"));

    // The measured balance delta must equal the tokens the transaction reported burnt.
    let burnt = total_tokens_burnt(&outcome);
    assert_eq!(
        cost, burnt,
        "scenario {} via {method:?}: balance delta {cost} != tokens burnt {burnt}",
        scenario.name,
    );

    if let (Some(allowance_before), Some(allowance_after)) = (allowance_before, allowance_after) {
        let allowance_spent = allowance_before.checked_sub(allowance_after).unwrap();
        let account_spent = actor_before.checked_sub(actor_after).unwrap();
        assert_eq!(
            allowance_spent, account_spent,
            "function call key allowance changed by {allowance_spent} but the account was debited {account_spent}",
        );
    }

    cost
}

/// Run a single scenario across multiple submission methods, comparing the cost before and
/// after the `AccountCostIncrease` feature and asserting the difference matches the expected
/// account-creation charge.
fn run_cost_test(scenario: Scenario, methods: &[SubmitMethod]) {
    init_test_logger();

    // The feature is only enabled on nightly today. Skip when it isn't enabled in this build; the
    // tests then re-enable themselves automatically once it is enabled at PROTOCOL_VERSION (e.g.
    // when it is stabilized).
    if !ProtocolFeature::AccountCostIncrease.enabled(PROTOCOL_VERSION) {
        return;
    }

    let before_version = ProtocolFeature::AccountCostIncrease.protocol_version() - 1;
    let after_version = PROTOCOL_VERSION;
    assert!(
        !ProtocolFeature::AccountCostIncrease.enabled(before_version),
        "before_version should not enable the feature",
    );

    let mut before_env = build_env(before_version);
    let mut after_env = build_env(after_version);

    if scenario.needs_global_contract {
        deploy_global_contract(&mut before_env);
        deploy_global_contract(&mut after_env);
    }

    let before_config =
        before_env.rpc_node().client().runtime_adapter.get_runtime_config(before_version).clone();
    let after_config =
        after_env.rpc_node().client().runtime_adapter.get_runtime_config(after_version).clone();

    // Expected extra charge per created account, derived from the post-feature config.
    let charge_per_account =
        FeeHelper::new(after_config.clone(), GAS_PRICE).extra_account_creation_charge();
    let charge_total = charge_per_account
        .checked_mul(u128::from(scenario.accounts_created))
        .unwrap()
        .as_yoctonear() as i128;
    let price = GAS_PRICE.as_yoctonear() as i128;

    for &method in methods {
        assert_feature_state(&before_env, false);
        assert_feature_state(&after_env, true);

        // Meta-transaction adjustment. A relayed transaction charges each inner action's send fee
        // one extra time (the relayer->sender hop) versus a direct transaction. The feature
        // redistributed the create_account / deterministic_state_init fees from send to exec
        // (same total), so for delegate methods that extra send hop costs `send_after -
        // send_before` less after the feature. Computed from the configs for this scenario's
        // actual inner actions, so it is automatically 0 when no changed send fee is in the
        // transaction (function calls, and `call_promise` whose account creation happens in
        // receipts the contract spawns rather than in the transaction's actions).
        let expected_diff: i128 = if method.is_delegate() {
            let accounts = method_accounts(index_of(method));
            let tx = (scenario.build)(&accounts);
            let sir = accounts.actor == tx.receiver;
            let send_before = total_send_fees(&before_config, sir, &tx.actions, &tx.receiver)
                .unwrap()
                .gas
                .as_gas();
            let send_after = total_send_fees(&after_config, sir, &tx.actions, &tx.receiver)
                .unwrap()
                .gas
                .as_gas();
            charge_total + (send_after as i128 - send_before as i128) * price
        } else {
            charge_total
        };

        let cost_before = measure_cost(&mut before_env, method, &scenario);
        let cost_after = measure_cost(&mut after_env, method, &scenario);
        assert!(
            cost_before > Balance::ZERO && cost_after > Balance::ZERO,
            "scenario {} via {method:?}: cost must be positive (before {cost_before}, after {cost_after})",
            scenario.name,
        );
        let actual_diff = cost_after.as_yoctonear() as i128 - cost_before.as_yoctonear() as i128;

        tracing::info!(
            target: "test",
            "scenario {} via {method:?}: before={cost_before} after={cost_after} diff={actual_diff} expected={expected_diff}",
            scenario.name,
        );
        assert_eq!(
            actual_diff, expected_diff,
            "scenario {} via {method:?}: cost difference {actual_diff} != expected {expected_diff} \
             (before {cost_before}, after {cost_after})",
            scenario.name,
        );
    }
}

/// Index of `method` within [`ALL_METHODS`] (used to derive its dedicated accounts).
fn index_of(method: SubmitMethod) -> usize {
    ALL_METHODS.iter().position(|m| *m == method).unwrap()
}

/// A single zero-deposit function call to `log_something` on the test contract.
fn log_something_action() -> Action {
    Action::FunctionCall(Box::new(FunctionCallAction {
        method_name: "log_something".to_string(),
        args: vec![],
        gas: FUNCTION_CALL_GAS,
        deposit: Balance::ZERO,
    }))
}

fn transfer_action() -> Action {
    Action::Transfer(TransferAction { deposit: TRANSFER_AMOUNT })
}

/// A transfer to `receiver`, tracked as a deposit recipient so the deposit nets out of the cost.
fn transfer_to(receiver: AccountId) -> ScenarioTx {
    ScenarioTx {
        receiver: receiver.clone(),
        actions: vec![transfer_action()],
        deposit_recipients: vec![receiver],
        expect_success: true,
        pre_setup: vec![],
    }
}

#[test]
fn test_create_account() {
    // A single `CreateAccount` action creating a fresh sub-account of the actor.
    run_cost_test(
        Scenario {
            name: "create_account",
            accounts_created: 1,
            needs_contract: false,
            needs_global_contract: false,
            build: |accounts| {
                let new_account = create_account_id(&format!("sub.{}", accounts.actor));
                ScenarioTx::new(new_account, vec![Action::CreateAccount(CreateAccountAction {})])
            },
        },
        FULL_ACCESS_METHODS,
    );
}

#[test]
fn test_function_call() {
    // A single function call; creates no account, so cost is identical before and after.
    run_cost_test(
        Scenario {
            name: "function_call",
            accounts_created: 0,
            needs_contract: true,
            needs_global_contract: false,
            build: |accounts| {
                ScenarioTx::new(accounts.contract.clone(), vec![log_something_action()])
            },
        },
        ALL_METHODS,
    );
}

#[test]
fn test_transfer_to_named_account() {
    // Transfer to an existing named account; no account is created, so cost is unchanged.
    run_cost_test(
        Scenario {
            name: "transfer_to_named",
            accounts_created: 0,
            needs_contract: false,
            needs_global_contract: false,
            build: |accounts| transfer_to(accounts.receiver.clone()),
        },
        FULL_ACCESS_METHODS,
    );
}

#[test]
fn test_transfer_creating_near_implicit_account() {
    // Transfer to a non-existent NEAR-implicit account, which creates it.
    run_cost_test(
        Scenario {
            name: "transfer_creating_near_implicit",
            accounts_created: 1,
            needs_contract: false,
            needs_global_contract: false,
            build: |accounts| transfer_to(near_implicit_account(accounts.index)),
        },
        FULL_ACCESS_METHODS,
    );
}

#[test]
fn test_transfer_to_existing_near_implicit_account() {
    // Transfer to a NEAR-implicit account that already exists (pre-created in setup), so no
    // account is created and no creation charge applies.
    run_cost_test(
        Scenario {
            name: "transfer_to_existing_near_implicit",
            accounts_created: 0,
            needs_contract: false,
            needs_global_contract: false,
            build: |accounts| {
                let mut tx = transfer_to(near_implicit_account(accounts.index));
                // Pre-create the implicit account with an initial transfer (not measured).
                tx.pre_setup = vec![transfer_action()];
                tx
            },
        },
        FULL_ACCESS_METHODS,
    );
}

#[test]
fn test_transfer_creating_eth_implicit_account() {
    // Transfer to a non-existent ETH-implicit account, which creates it.
    run_cost_test(
        Scenario {
            name: "transfer_creating_eth_implicit",
            accounts_created: 1,
            needs_contract: false,
            needs_global_contract: false,
            build: |accounts| transfer_to(eth_implicit_account(accounts.index)),
        },
        FULL_ACCESS_METHODS,
    );
}

#[test]
fn test_create_account_then_failing_call() {
    // `CreateAccount` followed by a `FunctionCall` on the new (contract-less) account, which
    // fails — rolling back the whole receipt, so the account is NOT created and no creation
    // charge is applied. Cost is therefore identical before and after.
    run_cost_test(
        Scenario {
            name: "create_account_then_failing_call",
            accounts_created: 0,
            needs_contract: false,
            needs_global_contract: false,
            build: |accounts| {
                let new_account = create_account_id(&format!("sub.{}", accounts.actor));
                ScenarioTx {
                    receiver: new_account,
                    actions: vec![
                        Action::CreateAccount(CreateAccountAction {}),
                        log_something_action(),
                    ],
                    deposit_recipients: vec![],
                    expect_success: false,
                    pre_setup: vec![],
                }
            },
        },
        FULL_ACCESS_METHODS,
    );
}

#[test]
fn test_create_account_then_successful_call() {
    // `CreateAccount`, point the new account at the global contract with `UseGlobalContract`, then
    // call a method on it that succeeds. The account is created (charge applies); using a global
    // contract keeps the account zero-balance, so no funding transfer is needed and the only thing
    // the new account receives is the function-call reward (left in the gross cost, not tracked).
    run_cost_test(
        Scenario {
            name: "create_account_then_successful_call",
            accounts_created: 1,
            needs_contract: false,
            needs_global_contract: true,
            build: |accounts| {
                let new_account = create_account_id(&format!("sub.{}", accounts.actor));
                ScenarioTx::new(
                    new_account,
                    vec![
                        Action::CreateAccount(CreateAccountAction {}),
                        Action::UseGlobalContract(Box::new(UseGlobalContractAction {
                            contract_identifier: GlobalContractIdentifier::AccountId(
                                global_contract_account(),
                            ),
                        })),
                        log_something_action(),
                    ],
                )
            },
        },
        FULL_ACCESS_METHODS,
    );
}

#[test]
fn test_create_two_accounts_via_call_promise() {
    // A single `call_promise` function call that spawns two promises, each creating a new
    // sub-account of the contract. Two accounts are created, so the creation charge applies twice.
    run_cost_test(
        Scenario {
            name: "create_two_accounts_via_call_promise",
            accounts_created: 2,
            needs_contract: true,
            needs_global_contract: false,
            build: |accounts| {
                let first = create_account_id(&format!("a.{}", accounts.contract));
                let second = create_account_id(&format!("b.{}", accounts.contract));
                let args = serde_json::json!([
                    { "batch_create": { "account_id": first.as_str() }, "id": 0 },
                    { "action_create_account": { "promise_index": 0 }, "id": 0 },
                    { "batch_create": { "account_id": second.as_str() }, "id": 1 },
                    { "action_create_account": { "promise_index": 1 }, "id": 1 },
                ]);
                ScenarioTx {
                    receiver: accounts.contract.clone(),
                    actions: vec![Action::FunctionCall(Box::new(FunctionCallAction {
                        method_name: "call_promise".to_string(),
                        args: serde_json::to_vec(&args).unwrap(),
                        gas: CALL_PROMISE_GAS,
                        deposit: Balance::ZERO,
                    }))],
                    // The two created accounts get no deposit (bare creation), and the contract
                    // only receives the function-call reward, so none are deposit recipients.
                    deposit_recipients: vec![],
                    expect_success: true,
                    pre_setup: vec![],
                }
            },
        },
        ALL_METHODS,
    );
}

#[test]
fn test_deterministic_state_init_creating_account() {
    // A `DeterministicStateInit` action that creates a new (zero-balance) deterministic account.
    run_cost_test(
        Scenario {
            name: "deterministic_state_init_creating_account",
            accounts_created: 1,
            needs_contract: false,
            needs_global_contract: true,
            build: |accounts| {
                let (account, action) = deterministic_state_init(accounts.index);
                ScenarioTx::new(account, vec![action])
            },
        },
        FULL_ACCESS_METHODS,
    );
}

#[test]
fn test_deterministic_state_init_to_existing_account() {
    // `DeterministicStateInit` on a deterministic account that already exists (pre-created in
    // setup). Re-initialization creates no account, so no creation charge applies.
    run_cost_test(
        Scenario {
            name: "deterministic_state_init_to_existing_account",
            accounts_created: 0,
            needs_contract: false,
            needs_global_contract: true,
            build: |accounts| {
                let (account, action) = deterministic_state_init(accounts.index);
                ScenarioTx {
                    receiver: account,
                    actions: vec![action.clone()],
                    deposit_recipients: vec![],
                    expect_success: true,
                    // Create the deterministic account first (not measured).
                    pre_setup: vec![action],
                }
            },
        },
        FULL_ACCESS_METHODS,
    );
}
