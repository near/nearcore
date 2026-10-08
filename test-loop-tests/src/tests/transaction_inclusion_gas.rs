use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_parameters::RuntimeConfigStore;
use near_primitives::types::{Balance, Gas};
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::FinalExecutionStatus;
use std::sync::Arc;

#[test]
fn transfer_burns_transaction_inclusion_gas_at_gas_price_and_conserves_balance() {
    init_test_logger();
    let sender = create_account_id("sender");
    let receiver = create_account_id("receiver");
    let initial_balance = Balance::from_near(1);
    let deposit = Balance::from_millinear(1);
    let gas_price = Balance::from_yoctonear(100_000_000);
    let transaction_inclusion_gas_per_byte = Gas::from_gigagas(10);

    let mut runtime_config =
        RuntimeConfigStore::new().get_config(PROTOCOL_VERSION).as_ref().clone();
    Arc::make_mut(&mut runtime_config.fees).transaction_inclusion_gas_per_byte =
        transaction_inclusion_gas_per_byte;
    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .gas_prices(gas_price, gas_price)
        .runtime_config_store(RuntimeConfigStore::with_one_config(runtime_config))
        .add_user_accounts([&sender, &receiver], initial_balance)
        .build();

    let tx = env.rpc_node().tx_send_money(&sender, &receiver, deposit);
    let tx_hash = tx.get_hash();
    let tx_size = tx.size_for_limits(PROTOCOL_VERSION);
    env.rpc_runner().run_tx(tx, Duration::seconds(5));
    env.rpc_runner().run_for_number_of_blocks(1);

    let result = env.rpc_node().client().chain.get_final_transaction_result(&tx_hash).unwrap();
    assert!(matches!(result.status, FinalExecutionStatus::SuccessValue(_)));
    let transaction_inclusion_gas =
        transaction_inclusion_gas_per_byte.checked_mul(tx_size).unwrap();
    assert!(transaction_inclusion_gas > result.transaction_outcome.outcome.gas_burnt);
    let transaction_inclusion_amount =
        gas_price.checked_mul(u128::from(transaction_inclusion_gas.as_gas())).unwrap();
    assert_eq!(result.transaction_outcome.outcome.tokens_burnt, transaction_inclusion_amount);

    let tokens_burnt_by_all_outcomes = result
        .receipts_outcome
        .iter()
        .map(|outcome| outcome.outcome.tokens_burnt)
        .fold(result.transaction_outcome.outcome.tokens_burnt, |sum, tokens_burnt| {
            sum.checked_add(tokens_burnt).unwrap()
        });
    let sender_balance_drop =
        initial_balance.checked_sub(env.rpc_node().query_balance(&sender)).unwrap();
    assert_eq!(sender_balance_drop, deposit.checked_add(tokens_burnt_by_all_outcomes).unwrap());
    assert_eq!(
        env.rpc_node().query_balance(&receiver),
        initial_balance.checked_add(deposit).unwrap()
    );
}
