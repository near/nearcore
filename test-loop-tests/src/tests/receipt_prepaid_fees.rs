use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use assert_matches::assert_matches;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_parameters::parameter_table::FeeComponent;
use near_parameters::{ActionCosts, RuntimeConfig, RuntimeConfigStore};
use near_primitives::gas::Gas;
use near_primitives::hash::CryptoHash;
use near_primitives::state_record::StateRecord;
use near_primitives::types::{Balance, ProtocolVersion};
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::version::ProtocolFeature;
use near_primitives::views::{FinalExecutionOutcomeView, FinalExecutionStatus};
use std::collections::BTreeMap;
use std::sync::Arc;

const EPOCH_LENGTH: u64 = 10;
// Long enough that a yield created before the upgrade times out after it.
const YIELD_TIMEOUT_LENGTH: u64 = 50;
const GAS_PRICE: Balance = Balance::from_yoctonear(100_000_000);

fn old_protocol_version() -> ProtocolVersion {
    ProtocolFeature::ReceiptPrepaidFees.protocol_version()
}

fn new_protocol_version() -> ProtocolVersion {
    old_protocol_version() + 1
}

/// The config of `protocol_version` with a short yield timeout, and the exec fee
/// of a function call changed by `function_call_exec_change`.
fn runtime_config(
    protocol_version: ProtocolVersion,
    function_call_exec_change: impl Fn(Gas) -> Gas,
) -> Arc<RuntimeConfig> {
    let mut config = RuntimeConfigStore::new().get_config(protocol_version).as_ref().clone();
    Arc::make_mut(&mut config.wasm_config).limit_config.yield_timeout_length_in_blocks =
        YIELD_TIMEOUT_LENGTH;
    let fee = &mut Arc::make_mut(&mut config.fees).action_fees[ActionCosts::function_call_base];
    let exec_gas = function_call_exec_change(fee.exec_fee().gas);
    fee.execution = FeeComponent::GasAndCompute { gas: exec_gas, compute: fee.exec_fee().compute };
    Arc::new(config)
}

fn protocol_version_of_block(env: &TestLoopEnv, block_hash: &CryptoHash) -> ProtocolVersion {
    let client = env.validator().client();
    let header = client.chain.get_block_header(block_hash).unwrap();
    client.epoch_manager.get_epoch_protocol_version(header.epoch_id()).unwrap()
}

/// The sum of all account balances, staked balance included. With no receipt in
/// flight, it equals the total supply.
fn sum_of_balances(env: &TestLoopEnv) -> Balance {
    let mut account_ids = vec![];
    env.shared_state.genesis.for_each_record(|record| {
        if let StateRecord::Account { account_id, .. } = record {
            account_ids.push(account_id.clone());
        }
    });
    account_ids.iter().fold(Balance::ZERO, |sum, account_id| {
        let account = env.validator().view_account_query(account_id).unwrap();
        sum.checked_add(account.amount).unwrap().checked_add(account.locked).unwrap()
    })
}

fn total_supply(env: &TestLoopEnv) -> Balance {
    let client = env.validator().client();
    let head = client.chain.head().unwrap();
    client.chain.get_block_header(&head.last_block_hash).unwrap().total_supply()
}

/// Deploys the test contract to `user` and starts a yield that nobody resumes.
/// Its callback runs when the yield times out, after the protocol upgrade.
fn run_yield_across_upgrade(
    function_call_exec_change: impl Fn(Gas) -> Gas,
) -> (TestLoopEnv, FinalExecutionOutcomeView) {
    let user = create_account_id("user");
    let runtime_config_store = RuntimeConfigStore::new_custom(BTreeMap::from([
        (old_protocol_version(), runtime_config(old_protocol_version(), |gas| gas)),
        (new_protocol_version(), runtime_config(new_protocol_version(), function_call_exec_change)),
    ]));
    let mut env = TestLoopBuilder::new()
        .protocol_version(old_protocol_version())
        .protocol_upgrade_schedule(ProtocolUpgradeVotingSchedule::new_immediate(
            new_protocol_version(),
        ))
        .epoch_length(EPOCH_LENGTH)
        .gas_prices(GAS_PRICE, GAS_PRICE)
        .add_user_account(&user, Balance::from_near(1_000_000))
        .runtime_config_store(runtime_config_store)
        // Keep the outcomes of the yield transaction until its callback runs.
        .gc_num_epochs_to_keep(100)
        .build();

    let deploy_tx = env.validator().tx_deploy_test_contract(&user);
    env.validator_runner().run_tx(deploy_tx, Duration::seconds(5));
    let yield_tx = env.validator().tx_call(
        &user,
        &user,
        "call_yield_create_return_promise",
        vec![],
        Balance::ZERO,
        Gas::from_teragas(300),
    );
    let outcome = env.validator_runner().execute_tx(yield_tx, Duration::seconds(200)).unwrap();
    // Let the refund receipts land, so no receipt carries tokens any more.
    env.validator_runner().run_for_number_of_blocks(5);
    (env, outcome)
}

#[test]
#[cfg_attr(not(feature = "nightly"), ignore)]
fn test_yield_callback_runs_when_its_fee_rose_after_creation() {
    init_test_logger();

    let (env, outcome) =
        run_yield_across_upgrade(|gas| gas.checked_add(Gas::from_teragas(1)).unwrap());

    // Check: the yield was created before the upgrade and its callback ran after it.
    let [yield_create_receipt, callback_receipt, ..] = outcome.receipts_outcome.as_slice() else {
        panic!("expected the yield and its callback, got {:?}", outcome.receipts_outcome)
    };
    assert_eq!(
        protocol_version_of_block(&env, &yield_create_receipt.block_hash),
        old_protocol_version()
    );
    assert_eq!(
        protocol_version_of_block(&env, &callback_receipt.block_hash),
        new_protocol_version()
    );
    // Check: the callback succeeds, and the protocol pays the part of the risen
    // function call fee that the prepaid fees do not cover.
    assert_matches!(outcome.status, FinalExecutionStatus::SuccessValue(_));
    // Check: no tokens were minted or lost.
    assert_eq!(sum_of_balances(&env), total_supply(&env));
}

#[test]
#[cfg_attr(not(feature = "nightly"), ignore)]
fn test_yield_callback_refunds_prepaid_fees_left_when_its_fee_fell_after_creation() {
    init_test_logger();

    let (env, outcome) =
        run_yield_across_upgrade(|gas| gas.checked_sub(Gas::from_gigagas(1)).unwrap());

    // Check: the yield was created before the upgrade and its callback ran after it.
    let [yield_create_receipt, callback_receipt, ..] = outcome.receipts_outcome.as_slice() else {
        panic!("expected the yield and its callback, got {:?}", outcome.receipts_outcome)
    };
    assert_eq!(
        protocol_version_of_block(&env, &yield_create_receipt.block_hash),
        old_protocol_version()
    );
    assert_eq!(
        protocol_version_of_block(&env, &callback_receipt.block_hash),
        new_protocol_version()
    );
    // Check: the callback succeeds at the lower fee.
    assert_matches!(outcome.status, FinalExecutionStatus::SuccessValue(_));
    // Check: no tokens were minted or lost.
    assert_eq!(sum_of_balances(&env), total_supply(&env));
}
