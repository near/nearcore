//! A receipt funded under one fee schedule and executed under another must spend exactly the
//! gas that was purchased for it: it must neither mint tokens after a fee increase nor take
//! more than it paid after a fee decrease.
//!
//! The fee schedules are synthetic: the config of the creation version, and the same config with
//! a scaled `Transfer` execution fee at the execution version. Each test converts a transaction
//! in the last block before the version change and executes its receipt in the first block
//! after it, then checks that the tokens paid by the transaction equal the tokens burnt.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use near_o11y::testonly::init_test_logger;
use near_parameters::{ActionCosts, Fee, RuntimeConfigStore};
use near_primitives::action::delegate::{DelegateAction, NonDelegateAction, SignedDelegateAction};
use near_primitives::gas::Gas;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::{Action, FunctionCallAction, TransferAction};
use near_primitives::types::{AccountId, Balance, ProtocolVersion};
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_primitives::views::{ExecutionStatusView, FinalExecutionOutcomeView};
use std::collections::BTreeMap;
use std::iter::once;
use std::sync::Arc;

/// Below `min_gas_purchase_price`, so all gas is purchased at a higher price than it is burnt
/// at, and every refund includes the price difference of the burnt gas.
const GAS_PRICE: Balance = Balance::from_yoctonear(100_000_000);
/// With the failing action, the maximum number of actions in a receipt.
const NUM_TRANSFERS: usize = 99;
const EPOCH_LENGTH: u64 = 10;

/// The version that `PinReceiptFees` is enabled at.
const PINNED_VERSION: ProtocolVersion = ProtocolFeature::PinReceiptFees.protocol_version();

/// A change of the `Transfer` execution fee at `new_version`, to `old * num / den`.
struct FeeChange {
    old_version: ProtocolVersion,
    new_version: ProtocolVersion,
    num: u64,
    den: u64,
}

impl FeeChange {
    fn new(old_version: ProtocolVersion, num: u64, den: u64) -> Option<Self> {
        let new_version = old_version + 1;
        if new_version > PROTOCOL_VERSION {
            tracing::info!(
                target: "test",
                new_version,
                protocol_version = PROTOCOL_VERSION,
                "skipping: the binary does not support the new version"
            );
            return None;
        }
        Some(Self { old_version, new_version, num, den })
    }

    fn config_store(&self) -> RuntimeConfigStore {
        let old_config = RuntimeConfigStore::new().get_config(self.old_version).clone();
        let mut new_config = old_config.as_ref().clone();
        let old_transfer = old_config.fees.fee(ActionCosts::transfer).clone();
        Arc::make_mut(&mut new_config.fees).action_fees[ActionCosts::transfer] = Fee::new(
            old_transfer.send_fee(true).gas.as_gas(),
            old_transfer.send_fee(false).gas.as_gas(),
            old_transfer.exec_fee().gas.as_gas() * self.num / self.den,
        );
        RuntimeConfigStore::new_custom(BTreeMap::from([
            (0, old_config),
            (self.new_version, Arc::new(new_config)),
        ]))
    }

    fn setup_env(&self, accounts: &[&AccountId]) -> TestLoopEnv {
        let store = self.config_store();
        assert!(store.get_config(self.new_version).min_gas_purchase_price > GAS_PRICE);
        let mut builder = TestLoopBuilder::new()
            .enable_rpc()
            .protocol_version(self.old_version)
            .protocol_upgrade_schedule(ProtocolUpgradeVotingSchedule::new_immediate(
                self.new_version,
            ))
            .runtime_config_store(store)
            .epoch_length(EPOCH_LENGTH)
            // Accounts below "mm" and above it are on different shards, so a receipt to the
            // other shard executes one block after its transaction is converted.
            .shard_layout(ShardLayout::multi_shard_custom(vec![create_account_id("mm")], 1))
            .gas_prices(GAS_PRICE, GAS_PRICE);
        for account in accounts {
            builder = builder.add_user_account(account, Balance::from_near(1_000));
        }
        builder.build()
    }

    /// Runs blocks until a transaction submitted now is converted to a receipt in the last block
    /// of the old version, so that the receipt executes in the first block of the new version.
    fn wait_for_version_change(&self, env: &mut TestLoopEnv) {
        for _ in 0..10 * EPOCH_LENGTH {
            let head = env.rpc_node().head();
            let epoch_manager = &env.rpc_node().client().epoch_manager;
            let change_follows = epoch_manager.get_epoch_protocol_version(&head.epoch_id).unwrap()
                == self.old_version
                && epoch_manager.get_next_epoch_protocol_version(&head.last_block_hash).unwrap()
                    == self.new_version;
            let epoch_start = epoch_manager.get_epoch_start_height(&head.last_block_hash).unwrap();
            // The transaction is converted two blocks after the head.
            if change_follows && head.height + 3 == epoch_start + EPOCH_LENGTH {
                return;
            }
            env.rpc_runner().run_for_number_of_blocks(1);
        }
        panic!("the version change was never reached");
    }

    /// Submits `actions` from `signer` to `receiver` so that the receipt is converted at the old
    /// version and executed at the new one. Returns the final outcome and the number of tokens
    /// created, i.e. the tokens burnt plus the change of the balances of `accounts`.
    fn run(
        &self,
        env: &mut TestLoopEnv,
        signer: &AccountId,
        receiver: &AccountId,
        actions: Vec<Action>,
        accounts: &[&AccountId],
    ) -> (FinalExecutionOutcomeView, i128) {
        self.wait_for_version_change(env);
        let balances_before: Vec<_> =
            accounts.iter().map(|account| env.rpc_node().query_balance(account)).collect();
        let tx = env.rpc_node().tx_from_actions(signer, receiver, actions);
        let tx_hash = env.rpc_node().submit_tx(tx);
        env.rpc_runner().run_for_number_of_blocks(10);

        let client = env.rpc_node().client();
        let version_of = |block_hash: CryptoHash| {
            let header = client.chain.get_block_header(&block_hash).unwrap();
            client.epoch_manager.get_epoch_protocol_version(header.epoch_id()).unwrap()
        };
        let outcome = client.chain.get_final_transaction_result(&tx_hash).unwrap();
        assert_eq!(version_of(outcome.transaction_outcome.block_hash), self.old_version);
        assert_eq!(version_of(outcome.receipts_outcome[0].block_hash), self.new_version);

        let mut created = outcome.transaction_outcome.outcome.tokens_burnt.as_yoctonear() as i128;
        for receipt_outcome in &outcome.receipts_outcome {
            created += receipt_outcome.outcome.tokens_burnt.as_yoctonear() as i128;
        }
        for (account, before) in accounts.iter().zip(balances_before) {
            let after = env.rpc_node().query_balance(account);
            created += after.as_yoctonear() as i128 - before.as_yoctonear() as i128;
        }
        tracing::info!(target: "test", created, "tokens created (+) or destroyed (-)");
        (outcome, created)
    }
}

/// A function call to an account without a contract, so the receipt fails before its other
/// actions execute.
fn failing_function_call() -> Action {
    Action::FunctionCall(Box::new(FunctionCallAction {
        method_name: "fail-first".to_string(),
        args: Vec::new(),
        gas: Gas::from_teragas(1),
        deposit: Balance::ZERO,
    }))
}

fn transfers(deposit: Balance) -> impl Iterator<Item = Action> {
    (0..NUM_TRANSFERS).map(move |_| Action::Transfer(TransferAction { deposit }))
}

fn assert_failed(outcome: &FinalExecutionOutcomeView) {
    let status = &outcome.receipts_outcome[0].outcome.status;
    assert!(
        matches!(status, ExecutionStatusView::Failure(_)),
        "expected a failure, got {status:?}"
    );
}

/// Sends a receipt whose first action fails, so its transfers are refunded unexecuted.
/// Returns the number of tokens created.
fn run_failed_receipt(fee_change: FeeChange) -> i128 {
    init_test_logger();
    let signer = create_account_id("alice");
    let receiver = create_account_id("zed");
    let mut env = fee_change.setup_env(&[&signer, &receiver]);
    let actions = once(failing_function_call()).chain(transfers(Balance::ZERO)).collect();
    let (outcome, created) = fee_change.run(&mut env, &signer, &receiver, actions, &[&signer]);
    assert_failed(&outcome);
    created
}

/// Control: a receipt that crosses a version without a fee change conserves tokens.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
#[test]
fn test_failed_receipt_unchanged_fees() {
    let Some(fee_change) = FeeChange::new(PINNED_VERSION, 1, 1) else { return };
    assert_eq!(run_failed_receipt(fee_change), 0);
}

/// Control: without `PinReceiptFees`, the refund of the unexecuted transfers is priced with the
/// increased fee and mints tokens. Guards that the other tests exercise the minting scenario.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
#[test]
fn test_failed_receipt_after_fee_increase_without_pinning() {
    let Some(fee_change) = FeeChange::new(PINNED_VERSION - 2, 3, 1) else { return };
    assert!(run_failed_receipt(fee_change) > 0);
}

/// The unexecuted transfers are refunded at the fees they were purchased at.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
#[test]
fn test_failed_receipt_after_fee_increase() {
    let Some(fee_change) = FeeChange::new(PINNED_VERSION, 3, 1) else { return };
    assert_eq!(run_failed_receipt(fee_change), 0);
}

/// After a fee decrease the refund still covers all the gas purchased.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
#[test]
fn test_failed_receipt_after_fee_decrease() {
    let Some(fee_change) = FeeChange::new(PINNED_VERSION, 1, 3) else { return };
    assert_eq!(run_failed_receipt(fee_change), 0);
}

/// All transfers execute and burn their execution fees. Only the purchased gas is refunded the
/// difference between the purchase and the burn price.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
#[test]
fn test_successful_receipt_after_fee_increase() {
    init_test_logger();
    let Some(fee_change) = FeeChange::new(PINNED_VERSION, 3, 1) else { return };
    let signer = create_account_id("alice");
    let receiver = create_account_id("zed");
    let mut env = fee_change.setup_env(&[&signer, &receiver]);
    let actions = transfers(Balance::from_yoctonear(1)).collect();
    let (outcome, created) =
        fee_change.run(&mut env, &signer, &receiver, actions, &[&signer, &receiver]);
    let status = &outcome.receipts_outcome[0].outcome.status;
    assert!(matches!(status, ExecutionStatusView::SuccessValue(_)), "got {status:?}");
    assert_eq!(created, 0);
}

/// The relayer purchases the gas of the inner actions of a meta transaction. The receipt of the
/// relayer forwards it to the receipt of the inner actions at the fees it was purchased at.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
#[test]
fn test_meta_transaction_after_fee_increase() {
    init_test_logger();
    let Some(fee_change) = FeeChange::new(PINNED_VERSION, 3, 1) else { return };
    let relayer = create_account_id("alice");
    let sender = create_account_id("zed");
    let receiver = create_account_id("zoe");
    let mut env = fee_change.setup_env(&[&relayer, &sender, &receiver]);

    let sender_signer = create_user_test_signer(&sender);
    let inner_actions = once(failing_function_call())
        .chain(transfers(Balance::ZERO))
        .map(|action| NonDelegateAction::try_from(action).unwrap())
        .collect();
    let delegate_action = DelegateAction {
        sender_id: sender.clone(),
        receiver_id: receiver.clone(),
        actions: inner_actions,
        nonce: env.rpc_node().get_next_nonce(&sender),
        max_block_height: env.rpc_node().head().height + 10 * EPOCH_LENGTH,
        public_key: sender_signer.public_key(),
    };
    let signed_delegate_action = SignedDelegateAction::sign(&sender_signer, delegate_action);

    let (outcome, created) = fee_change.run(
        &mut env,
        &relayer,
        &sender,
        vec![signed_delegate_action.into()],
        &[&relayer, &sender, &receiver],
    );
    // The receipt of the relayer succeeds, the receipt of the inner actions fails.
    let statuses: Vec<_> =
        outcome.receipts_outcome.iter().map(|outcome| &outcome.outcome.status).collect();
    assert!(
        statuses.iter().any(|status| matches!(status, ExecutionStatusView::Failure(_))),
        "expected the inner actions to fail, got {statuses:?}"
    );
    assert_eq!(created, 0);
}
