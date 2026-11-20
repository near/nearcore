//! Cross-cutting cost invariants that every supported way to pay for a transaction must satisfy.
//!
//! Each test runs a [`Scenario`] (a transaction to execute) and checks:
//!   - The net balance decrease across the payer's accounts and any accounts a deposit is
//!     transferred to equals the tokens the transaction reports burnt (deposits net out).
//!   - For the [`FunctionCallKey`] method, the function-call key's allowance drops by exactly
//!     the amount the account was debited (allowance is a spending budget tied to the account,
//!     not a separate pool - counting it in the balance would double-count the gas).
//!
//! These invariants are consensus-neutral - any mismatch indicates a bug in how fees, refunds,
//! or relayer accounting are propagated.
//!
//! Each scenario runs through a list of [`SubmitMethod`]s, covering every way the gas can be paid
//! for: signed directly with a full access key, a function call access key or relayed as a meta
//! transaction paid by the relayer's full access key. All methods share one set of accounts
//! ([`actor`], [`relayer`], [`contract`], [`receiver`]) in an environment provisioned up front so
//! that every method can run every scenario (see [`build_env`]): scenarios only differ in the
//! transaction they build, methods only in how it is signed and paid for. Accounts a scenario
//! *creates* get names derived from the method's index, since an account can only be created once
//! per environment.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use near_async::time::Duration;
use near_client::QueryError;
use near_crypto::{InMemorySigner, KeyType, PublicKey, Signer};
use near_o11y::testonly::init_test_logger;
use near_primitives::account::{AccessKey, AccessKeyPermission, FunctionCallPermission};
use near_primitives::action::delegate::{DelegateAction, NonDelegateAction, SignedDelegateAction};
use near_primitives::action::{
    AddKeyAction, CreateAccountAction, DeterministicStateInitAction, GlobalContractDeployMode,
    GlobalContractIdentifier, UseGlobalContractAction,
};
use near_primitives::deterministic_account_id::{
    DeterministicAccountStateInit, DeterministicAccountStateInitV1,
};
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::{Action, SignedTransaction, TransferAction};
use near_primitives::types::{AccountId, Balance, Gas, Nonce};
use near_primitives::utils::derive_near_deterministic_account_id;
use near_primitives::views::{
    AccessKeyPermissionView, ExecutionStatusView, FinalExecutionOutcomeView, FinalExecutionStatus,
};
use near_test_contracts::backwards_compatible_rs_contract;
use std::collections::BTreeMap;

/// Gas price, pinned (min == max) so it stays constant across the test.
const GAS_PRICE: Balance = Balance::from_yoctonear(100_000_000);
/// Initial balance of every genesis account used by the tests.
const INITIAL_BALANCE: Balance = Balance::from_near(1_000_000);
/// Allowance granted to a (non-gas) function call access key. Generous enough to cover the gas
/// purchased at the inflated price; it is decremented on charge and restored by the gas refund.
const FUNCTION_CALL_KEY_ALLOWANCE: Balance = Balance::from_near(1);
/// Gas attached to function-call transactions.
const FUNCTION_CALL_GAS: Gas = Gas::from_teragas(30);
/// Gas attached to the `call_promise` transaction that spawns the account-creating receipts.
const CALL_PROMISE_GAS: Gas = Gas::from_teragas(100);
/// Amount transferred in transfer scenarios.
const TRANSFER_AMOUNT: Balance = Balance::from_millinear(1);
/// Large epoch length so the chain doesn't upgrade its protocol version while the test runs.
const EPOCH_LENGTH: u64 = 100_000;

/// The different ways a transaction can be submitted, all of which must satisfy the same
/// cost invariants. The delegate variant differs only in the relayer's outer signature - the
/// inner delegate action is always signed by the sender's full access key, since the outer
/// signature is what determines how the meta transaction is paid for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SubmitMethod {
    /// Signed directly with the account's full access key.
    FullAccessKey,
    /// Signed directly with a function call access key (carrying a finite allowance).
    FunctionCallKey,
    /// Relayed as a meta transaction; the relayer signs (and pays for) the outer transaction
    /// with a full access key.
    DelegateFullAccessKey,
}

use SubmitMethod::*;

const ALL_METHODS: &[SubmitMethod] = &[FullAccessKey, FunctionCallKey, DelegateFullAccessKey];

/// Methods able to submit arbitrary actions (everything a function-call-restricted key can't
/// do, such as creating an account).
const FULL_ACCESS_METHODS: &[SubmitMethod] = &[FullAccessKey, DelegateFullAccessKey];

/// Methods used by the deterministic-state-init scenarios.
const DETERMINISTIC_STATE_INIT_METHODS: &[SubmitMethod] = &[FullAccessKey];

impl SubmitMethod {
    fn is_delegate(self) -> bool {
        match self {
            DelegateFullAccessKey => true,
            FullAccessKey | FunctionCallKey => false,
        }
    }

    /// Index of this method in [`ALL_METHODS`]. Scenarios use it to derive unique names for the
    /// accounts they create.
    fn index(self) -> usize {
        ALL_METHODS.iter().position(|method| *method == self).unwrap()
    }
}

/// The account whose key authorizes the inner actions (the receipt predecessor). Depending on
/// the method, the gas is paid from its main balance or its function call key allowance.
fn actor() -> AccountId {
    create_account_id("actor")
}

/// Relayer that submits and pays for delegate actions (unused by direct methods).
fn relayer() -> AccountId {
    create_account_id("relayer")
}

/// Account holding the test contract, deployed both to the account itself (for function-call
/// scenarios) and as a global contract by account id (for use-global-contract and
/// deterministic-state-init scenarios).
fn contract() -> AccountId {
    create_account_id("contract")
}

/// A pre-existing named account, used as the recipient of a plain transfer.
fn receiver() -> AccountId {
    create_account_id("receiver")
}

/// A transaction whose cost invariants to check, described independently of how it is submitted.
struct Scenario {
    /// Builds the transaction. Receives the submitting method's index, used to derive unique
    /// names for the accounts the scenario creates.
    build: fn(index: usize) -> ScenarioTx,
}

/// The transaction a scenario produces for a given method.
struct ScenarioTx {
    /// The transaction receiver.
    receiver: AccountId,
    /// The actions to execute.
    actions: Vec<Action>,
    /// Accounts that receive a *deposit* from the transaction and must be tracked so the
    /// deposit nets out of the cost. The reported `tokens_burnt` is gross - it excludes deposits
    /// but includes the function-call reward - so reward recipients (e.g. the called contract)
    /// must NOT be listed here, only accounts a deposit is transferred to.
    deposit_recipients: Vec<AccountId>,
    /// Whether the transaction is expected to succeed.
    expect_success: bool,
    /// Actions run via the actor's full access key to `receiver` before measuring (not
    /// counted), e.g. to pre-create the receiver so the measured transaction does not create it.
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

/// The full-access key seeded for every genesis account.
fn full_signer(account: &AccountId) -> Signer {
    create_user_test_signer(account)
}

/// A function-call access key, seeded deterministically per account.
fn function_call_signer(account: &AccountId) -> Signer {
    InMemorySigner::from_seed(account.clone(), KeyType::ED25519, "fc_key").into()
}

/// Build an environment at the current protocol version, provisioned so that every submission
/// method can run every scenario: the test contract is deployed (directly and as a global
/// contract), the actor gets a function call key.
fn build_env() -> TestLoopEnv {
    let accounts = [actor(), relayer(), contract(), receiver()];
    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .epoch_length(EPOCH_LENGTH)
        .gas_prices(GAS_PRICE, GAS_PRICE)
        .add_user_accounts(&accounts, INITIAL_BALANCE)
        .build();
    provision_env(&mut env);
    env
}

/// Deploy the contracts and add the keys described in [`build_env`], all transactions run in
/// parallel. Setup, not measured.
fn provision_env(env: &mut TestLoopEnv) {
    let txs = {
        let node = env.rpc_node();

        // A function-call permission limited to the contract account. A regular function call
        // key carries a finite allowance.
        let fc_permission = |allowance: Option<Balance>| FunctionCallPermission {
            allowance,
            receiver_id: contract().to_string(),
            method_names: vec![],
        };
        let fc_key = AccessKey {
            nonce: 0,
            permission: AccessKeyPermission::FunctionCall(fc_permission(Some(
                FUNCTION_CALL_KEY_ALLOWANCE,
            ))),
        };
        let actor_actions = vec![Action::AddKey(Box::new(AddKeyAction {
            public_key: function_call_signer(&actor()).public_key(),
            access_key: fc_key,
        }))];

        vec![
            node.tx_deploy_contract(&contract(), backwards_compatible_rs_contract().to_vec()),
            node.tx_deploy_global_contract(
                &contract(),
                backwards_compatible_rs_contract().to_vec(),
                GlobalContractDeployMode::AccountId,
            ),
            node.tx_from_actions(&actor(), &actor(), actor_actions),
        ]
    };
    env.node_runner(0).run_txs_parallel(txs, Duration::seconds(30));
    // Run one extra block to make the state final, some functions can't read data from the latest block.
    env.rpc_runner().run_for_number_of_blocks(1);
}

/// Balance of an account, treating a missing account as zero.
fn balance_or_zero(env: &TestLoopEnv, account: &AccountId) -> Balance {
    match env.rpc_node().view_account_query(account) {
        Ok(view) => view.amount,
        Err(QueryError::UnknownAccount { .. }) => Balance::ZERO,
        Err(err) => panic!("unexpected query error for {account}: {err:?}"),
    }
}

/// Remaining allowance of a function call access key. The allowance is a spending budget that is
/// decremented when gas is charged and restored by the gas refund, so its net change equals the
/// gas actually spent. Unlike a gas key balance it is *not* a separate pool of tokens (the gas
/// is paid from the account), so it must not be summed with the balances - instead we check it
/// changed by exactly the measured cost.
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

/// Sum of the balances the user controls: the main balance of every tracked account.
fn total_user_balance(env: &TestLoopEnv, accounts: &[AccountId]) -> Balance {
    let mut sum = Balance::ZERO;
    for account in accounts {
        sum = sum.checked_add(balance_or_zero(env, account)).unwrap();
    }
    sum
}

/// Next nonce for the access key identified by `public_key` on `account`.
fn next_key_nonce(env: &TestLoopEnv, account: &AccountId, public_key: &PublicKey) -> Nonce {
    env.rpc_node().view_access_key_query(account, public_key).unwrap().nonce + 1
}

/// Sum of tokens burnt across the transaction and all its receipt outcomes - the fees the
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

/// A `DeterministicStateInit` action and the deterministic account id it creates. The state is
/// kept tiny and varied per method index so each method gets a distinct (zero-balance) account.
fn deterministic_state_init(index: usize) -> (AccountId, Action) {
    let data = BTreeMap::from([(b"m".to_vec(), vec![index as u8])]);
    let state_init = DeterministicAccountStateInit::V1(DeterministicAccountStateInitV1 {
        code: GlobalContractIdentifier::AccountId(contract()),
        data,
    });
    let account = derive_near_deterministic_account_id(&state_init);
    let action = Action::DeterministicStateInit(Box::new(DeterministicStateInitAction {
        state_init,
        deposit: Balance::ZERO,
    }));
    (account, action)
}

/// Run a transaction signed by `signer`'s full access key and wait for it to complete. Used for
/// a scenario's pre-setup actions, which are not measured.
fn run_setup_tx(
    env: &mut TestLoopEnv,
    signer: &AccountId,
    receiver: &AccountId,
    actions: Vec<Action>,
) {
    let tx = env.rpc_node().tx_from_actions(signer, receiver, actions);
    env.rpc_runner().run_tx(tx, Duration::seconds(5));
    env.rpc_runner().run_for_number_of_blocks(1);
}

/// Wrap `actions` into a delegate action signed by the actor's full access key. The result is
/// submitted by the relayer in an outer transaction addressed to the actor.
fn signed_delegate_action(env: &TestLoopEnv, receiver: &AccountId, actions: Vec<Action>) -> Action {
    let actor = actor();
    let signer = full_signer(&actor);
    let delegate_action = DelegateAction {
        sender_id: actor.clone(),
        receiver_id: receiver.clone(),
        actions: actions
            .into_iter()
            .map(|action| NonDelegateAction::try_from(action).unwrap())
            .collect(),
        nonce: env.rpc_node().get_next_nonce(&actor),
        max_block_height: env.rpc_node().head().height + 100,
        public_key: signer.public_key(),
    };
    let signature = signer.sign(delegate_action.get_nep461_hash().as_bytes());
    Action::Delegate(Box::new(SignedDelegateAction { delegate_action, signature }))
}

/// Build the scenario transaction, signed according to `method`.
fn build_tx(
    env: &TestLoopEnv,
    method: SubmitMethod,
    receiver: &AccountId,
    actions: Vec<Action>,
) -> SignedTransaction {
    let actor = actor();
    match method {
        FullAccessKey => env.rpc_node().tx_from_actions(&actor, receiver, actions),
        FunctionCallKey => {
            let signer = function_call_signer(&actor);
            SignedTransaction::from_actions(
                next_key_nonce(env, &actor, &signer.public_key()),
                actor.clone(),
                receiver.clone(),
                &signer,
                actions,
                env.rpc_node().head().last_block_hash,
            )
        }
        DelegateFullAccessKey => {
            let outer_actions = vec![signed_delegate_action(env, receiver, actions)];
            // The outer transaction goes from the relayer to the actor.
            env.rpc_node().tx_from_actions(&relayer(), &actor, outer_actions)
        }
    }
}

/// Submit the scenario transaction via `method`, assert its success matches `expect_success`,
/// wait for its refund receipts to settle, and return the final outcome.
fn submit_scenario(
    env: &mut TestLoopEnv,
    method: SubmitMethod,
    tx: &ScenarioTx,
) -> FinalExecutionOutcomeView {
    let signed_tx = build_tx(env, method, &tx.receiver, tx.actions.clone());
    let outcome = env.rpc_runner().execute_tx(signed_tx, Duration::seconds(5)).unwrap();

    // "Succeeded" means nothing failed anywhere in the receipt tree. The top-level status alone
    // is not enough: for a meta transaction whose inner action fails, the outer delegate receipt
    // (and thus the transaction) still reports success while an inner receipt reports failure.
    let tx_failed = matches!(outcome.status, FinalExecutionStatus::Failure(_));
    let any_receipt_failed = outcome
        .receipts_outcome
        .iter()
        .any(|receipt| matches!(receipt.outcome.status, ExecutionStatusView::Failure(_)));
    assert_eq!(
        !tx_failed && !any_receipt_failed,
        tx.expect_success,
        "unexpected scenario outcome (status {:?})",
        outcome.status,
    );

    // Run for one more height to finalize the state. RPC functions sometimes use prev_state_root to
    // read data, which doesn't work with the latest block.
    env.rpc_runner().run_for_number_of_blocks(1);
    outcome
}

/// Balances captured before and after the scenario runs.
struct BalanceSnapshot {
    /// Sum across all tracked accounts.
    total: Balance,
    /// The actor's own balance.
    actor: Balance,
    /// Remaining allowance of the actor's function call key ([`FunctionCallKey`] method only).
    fc_allowance: Option<Balance>,
}

/// Run `scenario` via `method` in `env` and check:
///   - The net decrease across every account the user controls (payer + relayer + any deposit
///     recipients) equals the tokens burnt by the transaction.
///   - For the [`FunctionCallKey`] method, the allowance changed by exactly the account's
///     gas debit.
fn check_cost_invariants(env: &mut TestLoopEnv, method: SubmitMethod, scenario: &Scenario) {
    let tx = (scenario.build)(method.index());

    // Pre-setup, e.g. to pre-create the receiver. Run via the actor's full key, not measured.
    if !tx.pre_setup.is_empty() {
        run_setup_tx(env, &actor(), &tx.receiver, tx.pre_setup.clone());
    }

    // Track the payer's accounts plus the accounts a deposit is transferred to. Deposits net out
    // (they leave the payer and arrive at the recipient, both tracked), so the balance delta
    // reduces to the gross tokens burnt - which the reported `tokens_burnt` also is (it excludes
    // deposits but includes the function-call reward, so reward recipients are deliberately not
    // tracked).
    let mut tracked = vec![actor()];
    if method.is_delegate() {
        tracked.push(relayer());
    }
    tracked.extend(tx.deposit_recipients.iter().cloned());
    tracked.sort();
    tracked.dedup();

    // A function call access key's allowance is decremented by the gas charged to the account
    // and restored by the gas refund, so it changes by exactly the account's net debit. Track it
    // separately (counting it in the balance would double-count the gas).
    let fc_key =
        (method == FunctionCallKey).then(|| (actor(), function_call_signer(&actor()).public_key()));

    let snapshot = |env: &TestLoopEnv| BalanceSnapshot {
        total: total_user_balance(env, &tracked),
        actor: balance_or_zero(env, &actor()),
        fc_allowance: fc_key
            .as_ref()
            .map(|(account, key)| function_call_key_allowance(env, account, key)),
    };

    let before = snapshot(env);
    let outcome = submit_scenario(env, method, &tx);
    let after = snapshot(env);

    let cost = before.total.checked_sub(after.total).unwrap_or_else(|| {
        panic!("cost is negative: balance grew from {} to {}", before.total, after.total)
    });

    // The measured balance delta must equal the tokens the transaction reported burnt.
    let burnt = total_tokens_burnt(&outcome);
    assert_eq!(cost, burnt, "{method:?}: balance delta {cost} != tokens burnt {burnt}");

    if let Some((allowance_before, allowance_after)) = before.fc_allowance.zip(after.fc_allowance) {
        let allowance_spent = allowance_before.checked_sub(allowance_after).unwrap();
        let account_spent = before.actor.checked_sub(after.actor).unwrap();
        assert_eq!(
            allowance_spent, account_spent,
            "function call key allowance changed by {allowance_spent} but the account was debited {account_spent}",
        );
    }

    assert!(cost > Balance::ZERO, "{method:?}: cost must be positive, got {cost}");
}

/// Run a single scenario across multiple submission methods, checking the cost invariants hold
/// for each.
fn run_cost_test(scenario: Scenario, methods: &[SubmitMethod]) {
    init_test_logger();

    let mut env = build_env();
    for &method in methods {
        check_cost_invariants(&mut env, method, &scenario);
    }
}

/// A fresh sub-account of the actor, used as the target of `CreateAccount` scenarios. Unique
/// per method index.
fn sub_account(index: usize) -> AccountId {
    create_account_id(&format!("sub{index}.{}", actor()))
}

/// A single zero-deposit function call to `log_something` on the test contract.
fn log_something_action() -> Action {
    Action::function_call("log_something", vec![], FUNCTION_CALL_GAS)
}

fn transfer_action() -> Action {
    Action::Transfer(TransferAction { deposit: TRANSFER_AMOUNT })
}

/// A transfer to `receiver`, tracked as a deposit recipient so the deposit nets out of the cost.
fn transfer_to(receiver: AccountId) -> ScenarioTx {
    ScenarioTx {
        deposit_recipients: vec![receiver.clone()],
        ..ScenarioTx::new(receiver, vec![transfer_action()])
    }
}

#[test]
fn test_create_account() {
    // A single `CreateAccount` action creating a fresh sub-account of the actor.
    run_cost_test(
        Scenario {
            build: |index| {
                ScenarioTx::new(
                    sub_account(index),
                    vec![Action::CreateAccount(CreateAccountAction {})],
                )
            },
        },
        FULL_ACCESS_METHODS,
    );
}

#[test]
fn test_function_call() {
    // A single function call.
    run_cost_test(
        Scenario { build: |_| ScenarioTx::new(contract(), vec![log_something_action()]) },
        ALL_METHODS,
    );
}

#[test]
fn test_transfer_to_named_account() {
    // Transfer to an existing named account.
    run_cost_test(Scenario { build: |_| transfer_to(receiver()) }, FULL_ACCESS_METHODS);
}

#[test]
fn test_transfer_creating_near_implicit_account() {
    // Transfer to a non-existent NEAR-implicit account, which creates it.
    run_cost_test(
        Scenario { build: |index| transfer_to(near_implicit_account(index)) },
        FULL_ACCESS_METHODS,
    );
}

#[test]
fn test_transfer_to_existing_near_implicit_account() {
    // Transfer to a NEAR-implicit account that already exists (pre-created in setup), so no
    // account is created and no creation charge applies.
    run_cost_test(
        Scenario {
            build: |index| ScenarioTx {
                // Pre-create the implicit account with an initial transfer (not measured).
                pre_setup: vec![transfer_action()],
                ..transfer_to(near_implicit_account(index))
            },
        },
        FULL_ACCESS_METHODS,
    );
}

#[test]
fn test_transfer_creating_eth_implicit_account() {
    // Transfer to a non-existent ETH-implicit account, which creates it.
    run_cost_test(
        Scenario { build: |index| transfer_to(eth_implicit_account(index)) },
        FULL_ACCESS_METHODS,
    );
}

#[test]
fn test_create_account_then_failing_call() {
    // `CreateAccount` followed by a `FunctionCall` on the new (contract-less) account, which
    // fails - rolling back the whole receipt, so the account is NOT created.
    run_cost_test(
        Scenario {
            build: |index| ScenarioTx {
                expect_success: false,
                ..ScenarioTx::new(
                    sub_account(index),
                    vec![Action::CreateAccount(CreateAccountAction {}), log_something_action()],
                )
            },
        },
        FULL_ACCESS_METHODS,
    );
}

#[test]
fn test_create_account_then_successful_call() {
    // `CreateAccount`, point the new account at the global contract with `UseGlobalContract`,
    // then call a method on it that succeeds.
    run_cost_test(
        Scenario {
            build: |index| {
                let use_global_contract =
                    Action::UseGlobalContract(Box::new(UseGlobalContractAction {
                        contract_identifier: GlobalContractIdentifier::AccountId(contract()),
                    }));
                ScenarioTx::new(
                    sub_account(index),
                    vec![
                        Action::CreateAccount(CreateAccountAction {}),
                        use_global_contract,
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
    // sub-account of the contract.
    run_cost_test(
        Scenario {
            build: |index| {
                let first = create_account_id(&format!("a{index}.{}", contract()));
                let second = create_account_id(&format!("b{index}.{}", contract()));
                let args = serde_json::json!([
                    { "batch_create": { "account_id": first.as_str() }, "id": 0 },
                    { "action_create_account": { "promise_index": 0 }, "id": 0 },
                    { "batch_create": { "account_id": second.as_str() }, "id": 1 },
                    { "action_create_account": { "promise_index": 1 }, "id": 1 },
                ]);
                let call = Action::function_call(
                    "call_promise",
                    serde_json::to_vec(&args).unwrap(),
                    CALL_PROMISE_GAS,
                );
                ScenarioTx::new(contract(), vec![call])
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
            build: |index| {
                let (account, action) = deterministic_state_init(index);
                ScenarioTx::new(account, vec![action])
            },
        },
        DETERMINISTIC_STATE_INIT_METHODS,
    );
}

#[test]
fn test_deterministic_state_init_to_existing_account() {
    // `DeterministicStateInit` on a deterministic account that already exists (pre-created in
    // setup). Re-initialization creates no account.
    run_cost_test(
        Scenario {
            build: |index| {
                let (account, action) = deterministic_state_init(index);
                ScenarioTx {
                    // Create the deterministic account first (not measured).
                    pre_setup: vec![action.clone()],
                    ..ScenarioTx::new(account, vec![action])
                }
            },
        },
        DETERMINISTIC_STATE_INIT_METHODS,
    );
}
