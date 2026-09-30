//! Regression test: a failed receipt that crosses a change of the fee schedule must be refunded
//! exactly what it paid, not what its unexecuted actions cost under the new schedule.
//!
//! The fee schedules are synthetic (the current config vs. the same config with a scaled
//! `Transfer` execution fee), so the test does not depend on which real fees a protocol version
//! changes.

use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use near_o11y::testonly::init_test_logger;
use near_parameters::{ActionCosts, Fee, RuntimeConfigStore};
use near_primitives::gas::Gas;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::transaction::{Action, FunctionCallAction, TransferAction};
use near_primitives::types::{Balance, ProtocolVersion};
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::{ExecutionStatusView, FinalExecutionOutcomeView};
use std::collections::BTreeMap;
use std::sync::Arc;

const GAS_PRICE: Balance = Balance::from_yoctonear(100_000_000);
const TRAILING_TRANSFERS: usize = 99;

fn total_tokens_burnt(outcome: &FinalExecutionOutcomeView) -> Balance {
    let mut sum = outcome.transaction_outcome.outcome.tokens_burnt;
    for receipt in &outcome.receipts_outcome {
        sum = sum.checked_add(receipt.outcome.tokens_burnt).unwrap();
    }
    sum
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
    let epoch_length: u64 = 10;

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

    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .protocol_version(old_version)
        .protocol_upgrade_schedule(ProtocolUpgradeVotingSchedule::new_immediate(new_version))
        .runtime_config_store(store)
        .epoch_length(epoch_length)
        .shard_layout(ShardLayout::multi_shard_custom(vec![create_account_id("mm")], 1))
        .gas_prices(GAS_PRICE, GAS_PRICE)
        .add_user_account(&signer, Balance::from_near(1_000))
        .build();

    let runtime = &env.rpc_node().client().runtime_adapter;
    assert_eq!(
        runtime.get_runtime_config(new_version).fees.fee(ActionCosts::transfer).exec_fee().gas,
        Gas::from_gas(scaled_exec)
    );

    // Wait for the last epoch before the upgrade and send one transaction that gets
    // converted to a receipt in its last block. The receipt is then executed in the
    // first block of the upgraded epoch.
    let mut blocks = 0;
    loop {
        assert!(blocks < 10 * epoch_length, "the upgrade never happened");
        let head = env.rpc_node().head();
        let epoch_manager = &env.rpc_node().client().epoch_manager;
        let upgrade_follows = epoch_manager.get_epoch_protocol_version(&head.epoch_id).unwrap()
            == old_version
            && epoch_manager.get_next_epoch_protocol_version(&head.last_block_hash).unwrap()
                == new_version;
        let epoch_start = epoch_manager.get_epoch_start_height(&head.last_block_hash).unwrap();
        // The transaction is converted two blocks after the head.
        if upgrade_follows && head.height + 3 == epoch_start + epoch_length {
            break;
        }
        env.rpc_runner().run_for_number_of_blocks(1);
        blocks += 1;
    }
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
    let tx = env.rpc_node().tx_from_actions(&signer, &missing_receiver, actions);
    let tx_hash = env.rpc_node().submit_tx(tx);
    env.rpc_runner().run_for_number_of_blocks(10);

    let client = env.rpc_node().client();
    let version_of = |block_hash: CryptoHash| {
        let header = client.chain.get_block_header(&block_hash).unwrap();
        client.epoch_manager.get_epoch_protocol_version(header.epoch_id()).unwrap()
    };
    let outcome = client.chain.get_final_transaction_result(&tx_hash).unwrap();
    let action_outcome = &outcome.receipts_outcome[0];
    // the first action must fail, otherwise the trailing transfers would execute
    assert!(
        matches!(action_outcome.outcome.status, ExecutionStatusView::Failure(_)),
        "expected the first action to fail, got {:?}",
        action_outcome.outcome.status
    );
    assert_eq!(version_of(outcome.transaction_outcome.block_hash), old_version);
    assert_eq!(version_of(action_outcome.block_hash), new_version);

    let balance_after = env.rpc_node().query_balance(&signer);
    let net_change = balance_after.as_yoctonear() as i128 - balance_before.as_yoctonear() as i128;
    total_tokens_burnt(&outcome).as_yoctonear() as i128 + net_change
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
