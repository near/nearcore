//! Regression tests: a receipt that crosses a change of the fee schedule must be refunded
//! exactly what it paid, not what its actions cost under the new schedule.
//!
//! The failed-receipt tests use synthetic fee schedules (the current config vs. the same config
//! with a scaled `Transfer` execution fee), so they do not depend on which real fees a protocol
//! version changes. The successful-receipt tests use the real configs around the `0u` upgrade.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use near_crypto::{KeyType, PublicKeyHandle, SecretKey};
use near_o11y::testonly::init_test_logger;
use near_parameters::{ActionCosts, Fee, RuntimeConfigStore};
use near_primitives::gas::Gas;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::transaction::{Action, FunctionCallAction, TransferAction};
use near_primitives::types::{AccountId, Balance, ProtocolVersion};
use near_primitives::universal_state_init::{UniversalStateInit, UniversalStateInitV1};
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_primitives::views::{ExecutionStatusView, FinalExecutionOutcomeView};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

const GAS_PRICE: Balance = Balance::from_yoctonear(100_000_000);
const TRAILING_TRANSFERS: usize = 99;
const EPOCH_LENGTH: u64 = 10;

fn total_tokens_burnt(outcome: &FinalExecutionOutcomeView) -> Balance {
    let mut sum = outcome.transaction_outcome.outcome.tokens_burnt;
    for receipt in &outcome.receipts_outcome {
        sum = sum.checked_add(receipt.outcome.tokens_burnt).unwrap();
    }
    sum
}

fn setup_env(
    store: RuntimeConfigStore,
    old_version: ProtocolVersion,
    new_version: ProtocolVersion,
    signer: &AccountId,
) -> TestLoopEnv {
    TestLoopBuilder::new()
        .enable_rpc()
        .protocol_version(old_version)
        .protocol_upgrade_schedule(ProtocolUpgradeVotingSchedule::new_immediate(new_version))
        .runtime_config_store(store)
        .epoch_length(EPOCH_LENGTH)
        .shard_layout(ShardLayout::multi_shard_custom(vec![create_account_id("mm")], 1))
        .gas_prices(GAS_PRICE, GAS_PRICE)
        .add_user_account(signer, Balance::from_near(1_000))
        .build()
}

/// Runs blocks until a transaction submitted now is converted to a receipt in the last block of
/// an epoch of `funding_version`, followed by an epoch of `execution_version`. The receipt is then
/// executed in the first block of that next epoch.
fn wait_for_epoch_boundary(
    env: &mut TestLoopEnv,
    funding_version: ProtocolVersion,
    execution_version: ProtocolVersion,
) {
    let mut blocks = 0;
    loop {
        assert!(blocks < 10 * EPOCH_LENGTH, "the epoch boundary was never reached");
        let head = env.rpc_node().head();
        let epoch_manager = &env.rpc_node().client().epoch_manager;
        let boundary_follows = epoch_manager.get_epoch_protocol_version(&head.epoch_id).unwrap()
            == funding_version
            && epoch_manager.get_next_epoch_protocol_version(&head.last_block_hash).unwrap()
                == execution_version;
        let epoch_start = epoch_manager.get_epoch_start_height(&head.last_block_hash).unwrap();
        // The transaction is converted two blocks after the head.
        if boundary_follows && head.height + 3 == epoch_start + EPOCH_LENGTH {
            break;
        }
        env.rpc_runner().run_for_number_of_blocks(1);
        blocks += 1;
    }
}

/// Submits `actions` so that the receipt is funded under `funding_version` and executed under
/// `execution_version`, and returns the final outcome.
fn submit_across_boundary(
    env: &mut TestLoopEnv,
    signer: &AccountId,
    receiver: &AccountId,
    actions: Vec<Action>,
    funding_version: ProtocolVersion,
    execution_version: ProtocolVersion,
) -> FinalExecutionOutcomeView {
    wait_for_epoch_boundary(env, funding_version, execution_version);
    let tx = env.rpc_node().tx_from_actions(signer, receiver, actions);
    let tx_hash = env.rpc_node().submit_tx(tx);
    env.rpc_runner().run_for_number_of_blocks(10);

    let client = env.rpc_node().client();
    let version_of = |block_hash: CryptoHash| {
        let header = client.chain.get_block_header(&block_hash).unwrap();
        client.epoch_manager.get_epoch_protocol_version(header.epoch_id()).unwrap()
    };
    let outcome = client.chain.get_final_transaction_result(&tx_hash).unwrap();
    assert_eq!(version_of(outcome.transaction_outcome.block_hash), funding_version);
    assert_eq!(version_of(outcome.receipts_outcome[0].block_hash), execution_version);
    outcome
}

fn balance_change(before: Balance, after: Balance) -> i128 {
    after.as_yoctonear() as i128 - before.as_yoctonear() as i128
}

/// Sends a receipt that fails on its first action and leaves `TRAILING_TRANSFERS` transfers
/// unexecuted, so that it is funded under one fee schedule and executed under another whose
/// `Transfer` execution fee is `old * num / den`. Returns `tokens burnt + signer balance change`
/// over all transactions, which is zero iff no tokens were created or destroyed.
fn run_fee_schedule_crossing(num: u64, den: u64) -> i128 {
    init_test_logger();

    let old_version: ProtocolVersion = PROTOCOL_VERSION - 1;
    let new_version: ProtocolVersion = PROTOCOL_VERSION;
    let signer = create_account_id("alice");
    let missing_receiver = create_account_id("missing");

    let base_store = RuntimeConfigStore::new(None);
    let old_config = base_store.get_config(old_version).clone();
    let mut new_config = old_config.as_ref().clone();
    let old_transfer = old_config.fees.fee(ActionCosts::transfer).clone();
    let scaled_exec = old_transfer.exec_fee().gas.as_gas() * num / den;
    Arc::make_mut(&mut new_config.fees).action_fees[ActionCosts::transfer] = Fee::new(
        old_transfer.send_fee(true).gas.as_gas(),
        old_transfer.send_fee(false).gas.as_gas(),
        scaled_exec,
    );
    let store = RuntimeConfigStore::new_custom(BTreeMap::from([
        (0, old_config),
        (new_version, Arc::new(new_config)),
    ]));

    let mut env = setup_env(store, old_version, new_version, &signer);

    let runtime = &env.rpc_node().client().runtime_adapter;
    assert_eq!(
        runtime.get_runtime_config(new_version).fees.fee(ActionCosts::transfer).exec_fee().gas,
        Gas::from_gas(scaled_exec)
    );

    let balance_before = env.rpc_node().query_balance(&signer);
    let mut actions = vec![Action::FunctionCall(Box::new(FunctionCallAction {
        method_name: "fail-first".to_string(),
        args: Vec::new(),
        gas: Gas::from_teragas(1),
        deposit: Balance::ZERO,
    }))];
    actions.extend(
        (0..TRAILING_TRANSFERS)
            .map(|_| Action::Transfer(TransferAction { deposit: Balance::ZERO })),
    );
    let outcome = submit_across_boundary(
        &mut env,
        &signer,
        &missing_receiver,
        actions,
        old_version,
        new_version,
    );
    let action_outcome = &outcome.receipts_outcome[0];
    // the first action must fail, otherwise the trailing transfers would execute
    assert!(
        matches!(action_outcome.outcome.status, ExecutionStatusView::Failure(_)),
        "expected the first action to fail, got {:?}",
        action_outcome.outcome.status
    );

    let balance_after = env.rpc_node().query_balance(&signer);
    total_tokens_burnt(&outcome).as_yoctonear() as i128
        + balance_change(balance_before, balance_after)
}

/// Control: the same fee schedule on both sides of the upgrade conserves tokens.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
#[test]
fn test_failed_receipt_refund_unchanged_fees() {
    assert_eq!(run_fee_schedule_crossing(1, 1), 0);
}

/// A fee increase must not mint tokens through the refund of unexecuted actions.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
#[test]
fn test_failed_receipt_refund_after_fee_increase() {
    let created = run_fee_schedule_crossing(3, 1);
    assert_eq!(created, 0, "tokens created (+) or destroyed (-) by the refund, in yoctoNEAR");
}

/// A fee decrease must not destroy tokens through the refund of unexecuted actions.
/// TODO: CapRefundAtPrevEpochFees does not prevent this. Un-ignore when a full fix is in place.
#[ignore = "fee decreases are not compensated"]
#[test]
fn test_failed_receipt_refund_after_fee_decrease() {
    let created = run_fee_schedule_crossing(1, 3);
    assert_eq!(created, 0, "tokens created (+) or destroyed (-) by the refund, in yoctoNEAR");
}

/// Sends a receipt of `TRAILING_TRANSFERS + 1` transfers with no attached gas to a fresh `0u`
/// account, funded under `funding_version` and executed under `execution_version`, using the
/// real runtime configs. All the transfers succeed: the first one creates the account.
///
/// Gas is purchased at `min_gas_purchase_price` and burnt at the lower `GAS_PRICE`, so the
/// refund includes the price difference of every unit of gas burnt. Returns
/// `tokens burnt + signer and receiver balance changes`, which is zero iff no tokens were
/// created or destroyed.
fn run_universal_transfers(funding_version: ProtocolVersion) -> i128 {
    init_test_logger();

    let execution_version = ProtocolFeature::UniversalAccounts.protocol_version();
    let signer = create_account_id("alice");
    let public_key = SecretKey::from_seed(KeyType::ED25519, "surplus-mint").public_key();
    let receiver = UniversalStateInit::V1(UniversalStateInitV1 {
        code: None,
        data: BTreeMap::new(),
        access_keys: BTreeSet::from([PublicKeyHandle::from(public_key)]),
    })
    .derive_account_id();

    let store = RuntimeConfigStore::new(None);
    let config = store.get_config(execution_version).clone();
    assert!(config.min_gas_purchase_price > GAS_PRICE, "the burn price must be the lower one");
    let mut env = setup_env(store, funding_version, execution_version, &signer);

    let balance_before = env.rpc_node().query_balance(&signer);
    let deposit = Balance::from_near(1);
    let actions =
        (0..TRAILING_TRANSFERS + 1).map(|_| Action::Transfer(TransferAction { deposit })).collect();
    let outcome = submit_across_boundary(
        &mut env,
        &signer,
        &receiver,
        actions,
        funding_version,
        execution_version,
    );
    let action_outcome = &outcome.receipts_outcome[0];
    assert!(
        matches!(action_outcome.outcome.status, ExecutionStatusView::SuccessValue(_)),
        "expected the transfers to succeed, got {:?}",
        action_outcome.outcome.status
    );

    let signer_change = balance_change(balance_before, env.rpc_node().query_balance(&signer));
    let receiver_balance = env.rpc_node().query_balance(&receiver);
    let created = total_tokens_burnt(&outcome).as_yoctonear() as i128
        + signer_change
        + receiver_balance.as_yoctonear() as i128;
    tracing::info!(
        target: "test",
        gas_burnt = %action_outcome.outcome.gas_burnt,
        created_near = created as f64 / 1e24,
        "universal transfers"
    );
    created
}

/// Control: the transfers funded and executed with the `0u` fees conserve tokens.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
#[test]
fn test_successful_receipt_refund_unchanged_fees() {
    let version = ProtocolFeature::UniversalAccounts.protocol_version();
    assert_eq!(run_universal_transfers(version), 0);
}

/// A fee increase must not mint tokens through the price difference refunded for the burnt gas.
/// Funded before the `0u` upgrade, each transfer prepays only the transfer fee. Executed after
/// it, each also burns the account creation fee, so far more gas is burnt than was purchased,
/// and the price difference is refunded for all of it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
#[test]
fn test_successful_receipt_refund_after_fee_increase() {
    let version = ProtocolFeature::UniversalAccounts.protocol_version();
    let created = run_universal_transfers(version - 1);
    assert_eq!(created, 0, "tokens created (+) or destroyed (-) by the refund, in yoctoNEAR");
}
