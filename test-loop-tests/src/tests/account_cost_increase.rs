use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_primitives::account::id::AccountType;
use near_primitives::action::{
    CreateAccountAction, DeterministicStateInitAction, FunctionCallAction,
    GlobalContractDeployMode, GlobalContractIdentifier, TransferAction, UseGlobalContractAction,
};
use near_primitives::deterministic_account_id::{
    DeterministicAccountStateInit, DeterministicAccountStateInitV1,
};
use near_primitives::gas::Gas;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::transaction::{Action, SignedTransaction};
use near_primitives::types::{Balance, ProtocolVersion};
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::utils::derive_near_deterministic_account_id;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_primitives::views::{
    ExecutionStatusView, FinalExecutionOutcomeView, FinalExecutionStatus,
};
use std::collections::BTreeMap;

const GAS_PRICE: Balance = Balance::from_yoctonear(100_000_000);

fn measure_transaction_cost(env: &mut TestLoopEnv, transaction: SignedTransaction) -> Balance {
    measure_transaction_cost_with_success(env, transaction, true)
}

fn measure_transaction_cost_with_success(
    env: &mut TestLoopEnv,
    transaction: SignedTransaction,
    should_succeed: bool,
) -> Balance {
    let signer = transaction.transaction.signer_id().clone();
    let initial_balance = env.rpc_node().query_balance(&signer);

    // `execute_tx` returns Err only for transactions rejected before execution; on-chain
    // execution failures are reported inside the outcome.
    let outcome = env.rpc_runner().execute_tx(transaction, Duration::seconds(5)).unwrap();
    assert_outcome_success(&outcome, should_succeed);

    // `execute_tx` waits until every receipt in the transaction's tree (refunds included) has
    // executed, but balance queries read the state root referenced by the head block, which
    // lags one block behind chunk execution. Advance a block so the refunds become visible.
    env.rpc_runner().run_for_number_of_blocks(1);

    let final_balance = env.rpc_node().query_balance(&signer);
    let cost = initial_balance.checked_sub(final_balance).unwrap();
    assert!(cost > Balance::ZERO);
    cost
}

/// Assert whether the transaction outcome matches the expected success. "Succeeded" means
/// nothing failed anywhere in the receipt tree. The top-level status alone is not enough:
/// receipts spawned through promises (e.g. by `call_promise`) don't propagate their failures to
/// the transaction status.
fn assert_outcome_success(outcome: &FinalExecutionOutcomeView, should_succeed: bool) {
    let tx_failed = matches!(outcome.status, FinalExecutionStatus::Failure(_));
    let any_receipt_failed = outcome
        .receipts_outcome
        .iter()
        .any(|receipt| matches!(receipt.outcome.status, ExecutionStatusView::Failure(_)));
    assert_eq!(
        !tx_failed && !any_receipt_failed,
        should_succeed,
        "unexpected transaction outcome (status {:?})",
        outcome.status,
    );
}

/// Test that account_creation_charge is properly applied in various scenarios.
/// Tests ProtocolFeature::AccountCostIncrease.
#[test]
fn test_create_account_cost() {
    init_test_logger();

    let signer = create_account_id("alice");
    let global_contract_account = create_account_id("global_contract");

    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .gas_prices(GAS_PRICE, GAS_PRICE)
        .add_user_account(&signer, Balance::from_near(100_000))
        .add_user_account(&global_contract_account, Balance::from_near(100_000))
        .build();

    // Deploy a global contract
    let deploy_global_tx = env.rpc_node().tx_deploy_global_contract(
        &global_contract_account,
        near_test_contracts::rs_contract().to_vec(),
        GlobalContractDeployMode::AccountId,
    );
    env.rpc_runner().run_tx(deploy_global_tx, Duration::seconds(5));

    // Deploying a global contract only publishes the code, it doesn't deploy it to the account.
    // Deploy the test contract to the account itself so it can be called (`call_promise` below).
    let deploy_tx = env.rpc_node().tx_deploy_test_contract(&global_contract_account);
    env.rpc_runner().run_tx(deploy_tx, Duration::seconds(5));

    // The fixed charge for creating an account (in earlier configs the parameter is zero).
    let account_creation_charge = env
        .rpc_node()
        .client()
        .runtime_adapter
        .get_runtime_config(PROTOCOL_VERSION)
        .account_creation_charge;

    if ProtocolFeature::AccountCostIncrease.enabled(PROTOCOL_VERSION) {
        assert_eq!(account_creation_charge, Balance::from_millinear(7));
    }

    // [CreateAccount] should cost at least account_creation_charge
    let create_account_tx = env.rpc_node().tx_from_actions(
        &signer,
        &create_account_id("account1.alice"),
        vec![Action::CreateAccount(CreateAccountAction {})],
    );
    let create_account_tx_cost = measure_transaction_cost(&mut env, create_account_tx);
    assert!(create_account_tx_cost > account_creation_charge);

    // [Transfer] to a new implicit account should cost at least account_creation_charge
    let implicit_account_id =
        create_account_id("8bca86065be487de45e795b2c3154fe834d53ffa07e0a44f29e76a2a5f075df8");
    assert!(matches!(implicit_account_id.get_account_type(), AccountType::NearImplicitAccount));
    let transfer_to_implicit_tx = env.rpc_node().tx_from_actions(
        &signer,
        &implicit_account_id,
        vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(1) })],
    );
    let transfer_to_implicit_tx_cost = measure_transaction_cost(&mut env, transfer_to_implicit_tx);
    assert!(transfer_to_implicit_tx_cost > account_creation_charge);

    // Another [Transfer] to the same implicit account should cost less
    let second_transfer_to_implicit_tx = env.rpc_node().tx_from_actions(
        &signer,
        &implicit_account_id,
        vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(1) })],
    );
    let second_transfer_to_implicit_tx_cost =
        measure_transaction_cost(&mut env, second_transfer_to_implicit_tx);
    if ProtocolFeature::AccountCostIncrease.enabled(PROTOCOL_VERSION) {
        assert!(second_transfer_to_implicit_tx_cost < transfer_to_implicit_tx_cost);
        assert!(second_transfer_to_implicit_tx_cost < account_creation_charge);
    } else {
        assert_eq!(second_transfer_to_implicit_tx_cost, transfer_to_implicit_tx_cost);
    }

    // [Transfer] to a new eth implicit account should cost at least account_creation_charge
    let eth_implicit_account_id = create_account_id("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    assert!(matches!(eth_implicit_account_id.get_account_type(), AccountType::EthImplicitAccount));
    let transfer_to_eth_implicit_tx = env.rpc_node().tx_from_actions(
        &signer,
        &eth_implicit_account_id,
        vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(1) })],
    );
    let transfer_to_eth_implicit_tx_cost =
        measure_transaction_cost(&mut env, transfer_to_eth_implicit_tx);
    assert!(transfer_to_eth_implicit_tx_cost > account_creation_charge);

    // Another [Transfer] to the same eth implicit account should cost less
    let second_transfer_to_eth_implicit_tx = env.rpc_node().tx_from_actions(
        &signer,
        &eth_implicit_account_id,
        vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(1) })],
    );
    let second_transfer_to_eth_implicit_tx_cost =
        measure_transaction_cost(&mut env, second_transfer_to_eth_implicit_tx);
    if ProtocolFeature::AccountCostIncrease.enabled(PROTOCOL_VERSION) {
        assert!(second_transfer_to_eth_implicit_tx_cost < transfer_to_eth_implicit_tx_cost);
        assert!(second_transfer_to_eth_implicit_tx_cost < account_creation_charge);
    } else {
        assert_eq!(second_transfer_to_eth_implicit_tx_cost, transfer_to_eth_implicit_tx_cost);
    }

    // [Transfer] to a new deterministic account should cost at least account_creation_charge
    let deterministic_account_id = create_account_id("0s0000000000000000000000000000000000000000");
    assert!(matches!(
        deterministic_account_id.get_account_type(),
        AccountType::NearDeterministicAccount
    ));
    let transfer_to_deterministic_tx = env.rpc_node().tx_from_actions(
        &signer,
        &deterministic_account_id,
        vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(1) })],
    );
    let transfer_to_deterministic_tx_cost =
        measure_transaction_cost(&mut env, transfer_to_deterministic_tx);
    assert!(transfer_to_deterministic_tx_cost > account_creation_charge);

    // Another [Transfer] to the same deterministic account should cost less
    let second_transfer_to_deterministic_tx = env.rpc_node().tx_from_actions(
        &signer,
        &deterministic_account_id,
        vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(1) })],
    );
    let second_transfer_to_deterministic_tx_cost =
        measure_transaction_cost(&mut env, second_transfer_to_deterministic_tx);
    if ProtocolFeature::AccountCostIncrease.enabled(PROTOCOL_VERSION) {
        assert!(second_transfer_to_deterministic_tx_cost < transfer_to_deterministic_tx_cost);
        assert!(second_transfer_to_deterministic_tx_cost < account_creation_charge);
    } else {
        assert_eq!(second_transfer_to_deterministic_tx_cost, transfer_to_deterministic_tx_cost);
    }

    // [DeterministicStateInit] to a new account should cost at least account_creation_charge
    let state_init = DeterministicAccountStateInit::V1(DeterministicAccountStateInitV1 {
        code: GlobalContractIdentifier::AccountId(global_contract_account.clone()),
        data: BTreeMap::new(),
    });
    let derived_deterministic_account = derive_near_deterministic_account_id(&state_init);
    let deterministic_state_init_tx = env.rpc_node().tx_from_actions(
        &signer,
        &derived_deterministic_account,
        vec![Action::DeterministicStateInit(Box::new(DeterministicStateInitAction {
            state_init,
            deposit: Balance::ZERO,
        }))],
    );
    let deterministic_state_init_tx_cost =
        measure_transaction_cost(&mut env, deterministic_state_init_tx);
    assert!(deterministic_state_init_tx_cost > account_creation_charge);

    // Creating multiple accounts should charge multiple times. The contract spawns promises that
    // create sub-accounts of itself - a `CreateAccount` action can't create implicit accounts
    // (those are created only by transfers) and only the account's parent can create it.
    let create_three_tx = env.rpc_node().tx_from_actions(
        &signer,
        &global_contract_account,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&serde_json::json!([
                { "batch_create": { "account_id": "a.global_contract" }, "id": 0 },
                { "action_create_account": { "promise_index": 0 }, "id": 0 },
                { "batch_create": { "account_id": "b.global_contract" }, "id": 1 },
                { "action_create_account": { "promise_index": 1 }, "id": 1 },
                { "batch_create": { "account_id": "c.global_contract" }, "id": 2 },
                { "action_create_account": { "promise_index": 2 }, "id": 2 },
            ]))
            .unwrap(),
            gas: Gas::from_teragas(300),
            deposit: Balance::ZERO,
        }))],
    );
    let create_three_tx_cost = measure_transaction_cost(&mut env, create_three_tx);
    assert!(create_three_tx_cost > account_creation_charge.checked_mul(3).unwrap());

    // [CreateAccount, DeployContract, FunctionCall] where the function call fails should not charge for the new account
    let create_fail_tx = env.rpc_node().tx_from_actions(
        &signer,
        &create_account_id("account2.alice"),
        vec![
            Action::CreateAccount(CreateAccountAction {}),
            Action::UseGlobalContract(Box::new(UseGlobalContractAction {
                contract_identifier: GlobalContractIdentifier::AccountId(global_contract_account),
            })),
            Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "panic_with_message".to_string(),
                args: Vec::new(),
                gas: Gas::from_teragas(100),
                deposit: Balance::ZERO,
            })),
        ],
    );
    let create_fail_tx_cost =
        measure_transaction_cost_with_success(&mut env, create_fail_tx, false);
    if ProtocolFeature::AccountCostIncrease.enabled(PROTOCOL_VERSION) {
        assert!(create_fail_tx_cost < account_creation_charge);
    }
}

/// Sum of tokens burnt across the transaction and all its receipt outcomes - the fees the
/// transaction cost.
fn total_tokens_burnt(outcome: &FinalExecutionOutcomeView) -> Balance {
    let mut sum = outcome.transaction_outcome.outcome.tokens_burnt;
    for receipt in &outcome.receipts_outcome {
        sum = sum.checked_add(receipt.outcome.tokens_burnt).unwrap();
    }
    sum
}

/// Protocol version of the epoch the chain head belongs to.
fn protocol_version_at_head(env: &TestLoopEnv) -> ProtocolVersion {
    let head = env.rpc_node().head();
    env.rpc_node().client().epoch_manager.get_epoch_protocol_version(&head.epoch_id).unwrap()
}

/// Whether `AccountCostIncrease` was enabled in the block with the given hash.
fn block_has_feature(env: &TestLoopEnv, block_hash: &CryptoHash) -> bool {
    let client = env.rpc_node().client();
    let header = client.chain.get_block_header(block_hash).unwrap();
    let version = client.epoch_manager.get_epoch_protocol_version(header.epoch_id()).unwrap();
    ProtocolFeature::AccountCostIncrease.enabled(version)
}

/// Upgrade the network to a protocol version with `AccountCostIncrease` enabled while a stream
/// of account-creating transactions runs through the upgrade boundary.
///
/// The interesting case is a receipt that crosses the boundary: its gas was purchased before
/// the upgrade at the old (10x lower) price, but it executes after the upgrade, where a created
/// account should burn `account_creation_charge` out of the gas price surplus refund. Such a
/// receipt has no surplus to take the charge from, so the runtime charges only what's available
/// (nothing) and the account is still created successfully.
///
/// The signer and the created accounts are placed on different shards so that the
/// account-creating receipt executes one block after the transaction is converted - a
/// same-shard receipt would execute in the same block as the conversion and could never cross
/// the upgrade boundary.
// TODO(spice-test): This test can be removed once the AccountCostIncrease feature is stabilized.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
#[test]
fn test_create_account_cost_protocol_upgrade() {
    init_test_logger();

    // The test upgrades to PROTOCOL_VERSION, which must have the feature enabled.
    if !ProtocolFeature::AccountCostIncrease.enabled(PROTOCOL_VERSION) {
        return;
    }

    let signer = create_account_id("alice");
    // Boundary "mm": "alice" goes to the first shard, "subN.alice" to the second.
    let shard_layout = ShardLayout::multi_shard_custom(vec![create_account_id("mm")], 1);
    let epoch_length = 10;

    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .protocol_version(ProtocolFeature::AccountCostIncrease.protocol_version() - 1)
        .protocol_upgrade_schedule(ProtocolUpgradeVotingSchedule::new_immediate(PROTOCOL_VERSION))
        .epoch_length(epoch_length)
        .shard_layout(shard_layout)
        .gas_prices(GAS_PRICE, GAS_PRICE)
        .add_user_account(&signer, Balance::from_near(1_000))
        .build();

    let account_creation_charge = env
        .rpc_node()
        .client()
        .runtime_adapter
        .get_runtime_config(PROTOCOL_VERSION)
        .account_creation_charge;

    // Send a CreateAccount transaction at every height until the network upgrades, then keep
    // sending for a few more blocks under the new version.
    let mut tx_hashes = Vec::new();
    let mut blocks_after_upgrade = 0;
    while blocks_after_upgrade < 5 {
        assert!(tx_hashes.len() < 10 * epoch_length as usize, "the upgrade never happened");
        let new_account = create_account_id(&format!("sub{}.alice", tx_hashes.len()));
        let tx = env.rpc_node().tx_from_actions(
            &signer,
            &new_account,
            vec![Action::CreateAccount(CreateAccountAction {})],
        );
        tx_hashes.push(env.rpc_node().submit_tx(tx));
        env.rpc_runner().run_for_number_of_blocks(1);
        if protocol_version_at_head(&env) == PROTOCOL_VERSION {
            blocks_after_upgrade += 1;
        }
    }
    // Let the last transactions and all of their receipts finish executing.
    env.rpc_runner().run_for_number_of_blocks(5);

    let (mut before, mut crossing, mut after) = (0, 0, 0);
    for tx_hash in tx_hashes {
        let outcome = env.rpc_node().client().chain.get_final_transaction_result(&tx_hash).unwrap();
        // Every transaction must succeed: before, across, and after the upgrade.
        assert_outcome_success(&outcome, true);
        let cost = total_tokens_burnt(&outcome);

        // The transaction is converted to a receipt in one block, and the receipt may execute
        // in another. `receipts_outcome[0]` is the account-creating receipt, the rest are
        // refunds.
        let tx_with_feature = block_has_feature(&env, &outcome.transaction_outcome.block_hash);
        let receipt_with_feature = block_has_feature(&env, &outcome.receipts_outcome[0].block_hash);
        match (tx_with_feature, receipt_with_feature) {
            // Fully before the upgrade: the old, cheap account creation.
            (false, false) => {
                assert!(cost < account_creation_charge);
                before += 1;
            }
            // The receipt crossed the upgrade boundary: its gas was purchased at the old price,
            // so there is no price surplus to take the account creation charge from. The
            // account is created anyway and only the available surplus (nothing) is charged.
            (false, true) => {
                assert!(cost < account_creation_charge);
                crossing += 1;
            }
            (true, false) => panic!("receipt executed at an older version than its transaction"),
            // Fully after the upgrade: the account creation charge applies.
            (true, true) => {
                assert!(cost > account_creation_charge);
                after += 1;
            }
        }
    }
    tracing::info!(target: "test", before, crossing, after, "transactions per upgrade stage");
    assert!(
        before > 0 && crossing > 0 && after > 0,
        "the test should cover all three cases (before: {before}, crossing: {crossing}, after: {after})",
    );
}
