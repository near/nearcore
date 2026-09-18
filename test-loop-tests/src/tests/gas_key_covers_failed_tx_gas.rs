use crate::tests::gas_keys::{
    GasKeyEnv, get_gas_key_nonce, query_gas_key_and_balance, setup_funded_gas_key,
    total_tokens_burnt,
};
use crate::utils::transactions::get_shared_block_hash;
use near_async::time::Duration;
use near_primitives::errors::{InvalidTxError, TxExecutionError};
use near_primitives::transaction::{
    Action, ExecutionStatus, FunctionCallAction, SignedTransaction, TransactionNonce,
    TransferAction,
};
use near_primitives::types::{Balance, Gas, NonceIndex};
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_primitives::views::FinalExecutionStatus;

/// Genesis at the protocol version this build runs, which must already charge a
/// gas key transaction's gas to the account. Every test here pairs this with
/// `#[cfg_attr(not(feature = "nightly"), ignore)]`, because a stable build
/// panics on a chain whose protocol version it does not support.
fn setup(sender_balance: Balance, gas_key_balance: Balance, num_nonces: NonceIndex) -> GasKeyEnv {
    assert!(
        ProtocolFeature::GasKeyCoversFailedTxGas.enabled(PROTOCOL_VERSION),
        "a gas key still prepays its gas at protocol version {PROTOCOL_VERSION}"
    );
    setup_funded_gas_key(PROTOCOL_VERSION, sender_balance, gas_key_balance, num_nonces)
}

#[test]
#[cfg_attr(not(feature = "nightly"), ignore)]
fn test_account_pays_the_gas_and_the_key_stays_funded() {
    let sender_balance = Balance::from_near(100);
    let gas_key_balance = Balance::from_millinear(1);
    let num_nonces = 3;
    let GasKeyEnv { mut env, sender, receiver, gas_key_signer, .. } =
        setup(sender_balance, gas_key_balance, num_nonces);

    let sender_before = env.rpc_node().view_account_query(&sender).unwrap().amount;
    let receiver_before = env.rpc_node().view_account_query(&receiver).unwrap().amount;
    let (_, key_before) =
        query_gas_key_and_balance(&env.rpc_node(), &sender, &gas_key_signer.public_key());

    let nonce_index = 0;
    let gas_key_nonce = get_gas_key_nonce(&env, &sender, &gas_key_signer.public_key(), nonce_index);
    let block_hash = get_shared_block_hash(&env.node_datas, &env.test_loop.data);
    let transfer_amount = Balance::from_near(1);
    let gas_key_tx = SignedTransaction::from_actions_v1(
        TransactionNonce::from_nonce_and_index(gas_key_nonce + 1, nonce_index),
        sender.clone(),
        receiver.clone(),
        &gas_key_signer,
        vec![Action::Transfer(TransferAction { deposit: transfer_amount })],
        block_hash,
    );
    let outcome = env.rpc_runner().execute_tx(gas_key_tx, Duration::seconds(5)).unwrap();
    env.rpc_runner().run_for_number_of_blocks(1);

    assert!(
        matches!(outcome.status, FinalExecutionStatus::SuccessValue(_)),
        "expected success, got {:?}",
        outcome.status,
    );
    let tokens_burnt = total_tokens_burnt(&outcome);
    assert!(!tokens_burnt.is_zero());

    let sender_after = env.rpc_node().view_account_query(&sender).unwrap().amount;
    assert_eq!(
        sender_after,
        sender_before.checked_sub(transfer_amount).unwrap().checked_sub(tokens_burnt).unwrap()
    );
    let (_, key_after) =
        query_gas_key_and_balance(&env.rpc_node(), &sender, &gas_key_signer.public_key());
    assert_eq!(key_after, key_before);

    let receiver_after = env.rpc_node().view_account_query(&receiver).unwrap().amount;
    assert_eq!(receiver_after, receiver_before.checked_add(transfer_amount).unwrap());
    assert_eq!(
        get_gas_key_nonce(&env, &sender, &gas_key_signer.public_key(), nonce_index),
        gas_key_nonce + 1
    );
}

#[test]
#[cfg_attr(not(feature = "nightly"), ignore)]
fn test_gas_refund_credits_the_account() {
    let sender_balance = Balance::from_near(100);
    let gas_key_balance = Balance::from_millinear(1);
    let num_nonces = 3;
    let GasKeyEnv { mut env, sender, receiver, gas_key_signer, .. } =
        setup(sender_balance, gas_key_balance, num_nonces);

    let sender_before = env.rpc_node().view_account_query(&sender).unwrap().amount;
    let (_, key_before) =
        query_gas_key_and_balance(&env.rpc_node(), &sender, &gas_key_signer.public_key());

    // The receiver has no contract, so the call fails and refunds both the
    // deposit and the unused gas.
    let nonce_index = 0;
    let gas_key_nonce = get_gas_key_nonce(&env, &sender, &gas_key_signer.public_key(), nonce_index);
    let block_hash = get_shared_block_hash(&env.node_datas, &env.test_loop.data);
    let gas_key_tx = SignedTransaction::from_actions_v1(
        TransactionNonce::from_nonce_and_index(gas_key_nonce + 1, nonce_index),
        sender.clone(),
        receiver.clone(),
        &gas_key_signer,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "nonexistent_method".to_string(),
            args: vec![],
            gas: Gas::from_teragas(100),
            deposit: Balance::from_near(1),
        }))],
        block_hash,
    );
    let outcome = env.rpc_runner().execute_tx(gas_key_tx, Duration::seconds(5)).unwrap();
    env.rpc_runner().run_for_number_of_blocks(1);

    let tokens_burnt = total_tokens_burnt(&outcome);
    assert!(!tokens_burnt.is_zero());

    let sender_after = env.rpc_node().view_account_query(&sender).unwrap().amount;
    assert_eq!(sender_after, sender_before.checked_sub(tokens_burnt).unwrap());
    let (_, key_after) =
        query_gas_key_and_balance(&env.rpc_node(), &sender, &gas_key_signer.public_key());
    assert_eq!(key_after, key_before);
}

#[test]
#[cfg_attr(not(feature = "nightly"), ignore)]
#[cfg(feature = "test_features")]
fn test_key_pays_tokens_burnt_when_account_cannot_pay() {
    use near_client::NetworkAdversarialMessage;
    use near_client::client_actor::AdvProduceChunksMode;
    use near_primitives::transaction::ValidatedTransaction;

    let sender_balance = Balance::from_near(100);
    let gas_key_balance = Balance::from_millinear(1);
    let num_nonces = 3;
    let GasKeyEnv { mut env, sender, receiver, gas_key_signer, gas_price } =
        setup(sender_balance, gas_key_balance, num_nonces);

    let sender_balance_before = env.rpc_node().view_account_query(&sender).unwrap().amount;
    let (_, gas_key_balance_before) =
        query_gas_key_and_balance(&env.rpc_node(), &sender, &gas_key_signer.public_key());
    let nonce_index: NonceIndex = 0;
    let gas_key_nonce = get_gas_key_nonce(&env, &sender, &gas_key_signer.public_key(), nonce_index);

    // Skip runtime verification during chunk preparation, so a transaction the
    // account cannot pay for still reaches a chunk and is charged.
    env.node_runner(0).send_adversarial_message(NetworkAdversarialMessage::AdvProduceChunks(
        AdvProduceChunksMode::ProduceWithoutTxVerification,
    ));

    let block_hash = get_shared_block_hash(&env.node_datas, &env.test_loop.data);
    let transfer_amount = sender_balance.checked_mul(2).unwrap();
    let gas_key_tx = SignedTransaction::from_actions_v1(
        TransactionNonce::from_nonce_and_index(gas_key_nonce + 1, nonce_index),
        sender.clone(),
        receiver.clone(),
        &gas_key_signer,
        vec![Action::Transfer(TransferAction { deposit: transfer_amount })],
        block_hash,
    );
    let tx_hash = gas_key_tx.get_hash();

    let epoch_id = env.validator().head().epoch_id;
    let shard_layout = env.validator().client().epoch_manager.get_shard_layout(&epoch_id).unwrap();
    let shard_uid = shard_layout.account_id_to_shard_uid(&sender);
    let validated_tx = ValidatedTransaction::new_for_test(gas_key_tx);
    env.node(0)
        .client()
        .chunk_producer
        .sharded_tx_pool
        .lock()
        .insert_transaction(shard_uid, validated_tx);

    let outcome = env.rpc_runner().run_until_outcome_available(tx_hash, Duration::seconds(10));
    env.rpc_runner().run_for_number_of_blocks(1);

    let status = &outcome.outcome_with_id.outcome.status;
    assert!(
        matches!(
            status,
            ExecutionStatus::Failure(TxExecutionError::InvalidTxError(
                InvalidTxError::NotEnoughBalance { .. }
            )),
        ),
        "expected NotEnoughBalance, got {status:?}",
    );

    let gas_burnt = outcome.outcome_with_id.outcome.gas_burnt;
    let tokens_burnt = outcome.outcome_with_id.outcome.tokens_burnt;
    assert!(gas_burnt.as_gas() > 0);
    assert_eq!(tokens_burnt, gas_price.checked_mul(u128::from(gas_burnt.as_gas())).unwrap());
    assert!(outcome.outcome_with_id.outcome.receipt_ids.is_empty());

    let (_, gas_key_balance_after) =
        query_gas_key_and_balance(&env.rpc_node(), &sender, &gas_key_signer.public_key());
    assert_eq!(gas_key_balance_after, gas_key_balance_before.checked_sub(tokens_burnt).unwrap());

    let sender_balance_after = env.rpc_node().view_account_query(&sender).unwrap().amount;
    assert_eq!(sender_balance_after, sender_balance_before);

    assert_eq!(
        get_gas_key_nonce(&env, &sender, &gas_key_signer.public_key(), nonce_index),
        gas_key_nonce + 1
    );
}
