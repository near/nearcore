use crate::tests::gas_keys::{
    GasKeyEnv, get_gas_key_nonce, query_gas_key_and_balance, setup_funded_gas_key,
};
use crate::utils::transactions::get_shared_block_hash;
use assert_matches::assert_matches;
use near_async::time::Duration;
use near_primitives::action::delegate::{DelegateAction, SignedDelegateAction};
use near_primitives::transaction::{Action, SignedTransaction, TransferAction};
use near_primitives::types::{Balance, NonceIndex};
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_primitives::views::FinalExecutionStatus;

#[test]
#[cfg_attr(not(feature = "nightly"), ignore)]
fn test_gas_key_delegate_uses_access_key_nonce() {
    assert!(ProtocolFeature::GasKeyDelegateUsesAccessKeyNonce.enabled(PROTOCOL_VERSION));
    let sender_balance = Balance::from_near(100);
    let gas_key_balance = Balance::from_millinear(1);
    let num_nonces = 2;
    let GasKeyEnv { mut env, sender, receiver, gas_key_signer, .. } =
        setup_funded_gas_key(PROTOCOL_VERSION, sender_balance, gas_key_balance, num_nonces);
    let relayer = receiver.clone();

    let other_nonce_index: NonceIndex = 1;
    let other_gas_key_nonce =
        get_gas_key_nonce(&env, &sender, &gas_key_signer.public_key(), other_nonce_index);
    let access_key_view =
        env.rpc_node().view_access_key_query(&sender, &gas_key_signer.public_key()).unwrap();
    let delegate_nonce = access_key_view.nonce + 1;
    let transfer_amount = Balance::from_near(1);
    let delegate_action = DelegateAction {
        sender_id: sender.clone(),
        receiver_id: receiver.clone(),
        actions: vec![
            Action::Transfer(TransferAction { deposit: transfer_amount }).try_into().unwrap(),
        ],
        nonce: delegate_nonce,
        max_block_height: env.rpc_node().head().height + 100,
        public_key: gas_key_signer.public_key(),
    };
    let signed_delegate_action = SignedDelegateAction::sign(&gas_key_signer, delegate_action);

    let sender_before = env.rpc_node().view_account_query(&sender).unwrap().amount;
    let meta_tx =
        env.rpc_node().tx_from_actions(&relayer, &sender, vec![signed_delegate_action.into()]);
    let outcome = env.rpc_runner().execute_tx(meta_tx, Duration::seconds(5)).unwrap();
    env.rpc_runner().run_for_number_of_blocks(1);
    assert_matches!(outcome.status, FinalExecutionStatus::SuccessValue(_));
    // The relayer pays the inner deposit and the gas.
    let sender_after = env.rpc_node().view_account_query(&sender).unwrap().amount;
    assert_eq!(sender_after, sender_before);

    let access_key_view =
        env.rpc_node().view_access_key_query(&sender, &gas_key_signer.public_key()).unwrap();
    assert_eq!(access_key_view.nonce, delegate_nonce);
    let gas_key_nonces =
        env.rpc_node().view_gas_key_nonces_query(&sender, &gas_key_signer.public_key()).unwrap();
    assert_eq!(gas_key_nonces, vec![delegate_nonce, other_gas_key_nonce]);

    let block_hash = get_shared_block_hash(&env.node_datas, &env.test_loop.data);
    let v0_tx = SignedTransaction::send_money(
        access_key_view.nonce + 1,
        sender.clone(),
        receiver,
        &gas_key_signer,
        transfer_amount,
        block_hash,
    );
    let outcome = env.rpc_runner().execute_tx(v0_tx, Duration::seconds(5)).unwrap();
    env.rpc_runner().run_for_number_of_blocks(1);
    assert_matches!(outcome.status, FinalExecutionStatus::SuccessValue(_));
    let access_key_view =
        env.rpc_node().view_access_key_query(&sender, &gas_key_signer.public_key()).unwrap();
    assert_eq!(access_key_view.nonce, delegate_nonce + 1);
}

#[test]
#[cfg_attr(not(feature = "nightly"), ignore)]
#[cfg(feature = "test_features")]
fn test_tx_jumped_by_gas_key_delegate_charges_gas_key() {
    use near_client::NetworkAdversarialMessage;
    use near_client::client_actor::AdvProduceChunksMode;
    use near_primitives::errors::{InvalidTxError, TxExecutionError};
    use near_primitives::transaction::{ExecutionStatus, ValidatedTransaction};

    assert!(ProtocolFeature::GasKeyDelegateUsesAccessKeyNonce.enabled(PROTOCOL_VERSION));
    let sender_balance = Balance::from_near(100);
    let gas_key_balance = Balance::from_millinear(1);
    let num_nonces = 2;
    let GasKeyEnv { mut env, sender, receiver, gas_key_signer, gas_price } =
        setup_funded_gas_key(PROTOCOL_VERSION, sender_balance, gas_key_balance, num_nonces);
    let relayer = receiver.clone();

    let implicit_nonce_index: NonceIndex = 0;
    let gas_key_nonce =
        get_gas_key_nonce(&env, &sender, &gas_key_signer.public_key(), implicit_nonce_index);
    let delegate_nonce = gas_key_nonce + 5;
    let transfer_amount = Balance::from_near(1);
    let delegate_action = DelegateAction {
        sender_id: sender.clone(),
        receiver_id: receiver.clone(),
        actions: vec![
            Action::Transfer(TransferAction { deposit: transfer_amount }).try_into().unwrap(),
        ],
        nonce: delegate_nonce,
        max_block_height: env.rpc_node().head().height + 100,
        public_key: gas_key_signer.public_key(),
    };
    let signed_delegate_action = SignedDelegateAction::sign(&gas_key_signer, delegate_action);
    let meta_tx =
        env.rpc_node().tx_from_actions(&relayer, &sender, vec![signed_delegate_action.into()]);
    env.rpc_runner().run_tx(meta_tx, Duration::seconds(5));
    env.rpc_runner().run_for_number_of_blocks(1);

    let sender_balance_before = env.rpc_node().view_account_query(&sender).unwrap().amount;
    let (_, gas_key_balance_before) =
        query_gas_key_and_balance(&env.rpc_node(), &sender, &gas_key_signer.public_key());

    let jumped_nonce = gas_key_nonce + 1;
    let block_hash = get_shared_block_hash(&env.node_datas, &env.test_loop.data);
    let v0_tx = SignedTransaction::send_money(
        jumped_nonce,
        sender.clone(),
        receiver,
        &gas_key_signer,
        transfer_amount,
        block_hash,
    );
    let tx_hash = v0_tx.get_hash();
    let rpc_error = env.rpc_runner().execute_tx(v0_tx.clone(), Duration::seconds(5)).unwrap_err();
    assert_eq!(
        rpc_error,
        InvalidTxError::InvalidNonce { tx_nonce: jumped_nonce, ak_nonce: delegate_nonce }
    );

    // Skip runtime verification during chunk preparation, so the chunk includes
    // the tx the way a producer that has not seen the delegate would.
    env.node_runner(0).send_adversarial_message(NetworkAdversarialMessage::AdvProduceChunks(
        AdvProduceChunksMode::ProduceWithoutTxVerification,
    ));
    let epoch_id = env.validator().head().epoch_id;
    let shard_layout = env.validator().client().epoch_manager.get_shard_layout(&epoch_id).unwrap();
    let shard_uid = shard_layout.account_id_to_shard_uid(&sender);
    env.node(0)
        .client()
        .chunk_producer
        .sharded_tx_pool
        .lock()
        .insert_transaction(shard_uid, ValidatedTransaction::new_for_test(v0_tx));

    let outcome = env.rpc_runner().run_until_outcome_available(tx_hash, Duration::seconds(10));
    env.rpc_runner().run_for_number_of_blocks(1);

    let status = &outcome.outcome_with_id.outcome.status;
    assert_eq!(
        status,
        &ExecutionStatus::Failure(TxExecutionError::InvalidTxError(InvalidTxError::InvalidNonce {
            tx_nonce: jumped_nonce,
            ak_nonce: delegate_nonce,
        })),
    );
    let gas_burnt = outcome.outcome_with_id.outcome.gas_burnt;
    let tokens_burnt = outcome.outcome_with_id.outcome.tokens_burnt;
    assert_eq!(tokens_burnt, gas_price.checked_mul(u128::from(gas_burnt.as_gas())).unwrap());
    assert!(!tokens_burnt.is_zero());

    let (_, gas_key_balance_after) =
        query_gas_key_and_balance(&env.rpc_node(), &sender, &gas_key_signer.public_key());
    assert_eq!(gas_key_balance_after, gas_key_balance_before.checked_sub(tokens_burnt).unwrap());
    let sender_balance_after = env.rpc_node().view_account_query(&sender).unwrap().amount;
    assert_eq!(sender_balance_after, sender_balance_before);
}

#[test]
#[cfg_attr(not(feature = "nightly"), ignore)]
#[cfg(feature = "test_features")]
fn test_strict_tx_jumped_by_second_gas_key_delegate_charges_gas_key() {
    use near_client::NetworkAdversarialMessage;
    use near_client::client_actor::AdvProduceChunksMode;
    use near_primitives::errors::{InvalidTxError, TxExecutionError};
    use near_primitives::transaction::{ExecutionStatus, TransactionNonce, ValidatedTransaction};

    assert!(ProtocolFeature::GasKeyDelegateUsesAccessKeyNonce.enabled(PROTOCOL_VERSION));
    let sender_balance = Balance::from_near(100);
    let gas_key_balance = Balance::from_millinear(1);
    let num_nonces = 2;
    let GasKeyEnv { mut env, sender, receiver, gas_key_signer, gas_price } =
        setup_funded_gas_key(PROTOCOL_VERSION, sender_balance, gas_key_balance, num_nonces);
    let relayer = receiver.clone();

    let implicit_nonce_index: NonceIndex = 0;
    let gas_key_nonce =
        get_gas_key_nonce(&env, &sender, &gas_key_signer.public_key(), implicit_nonce_index);
    let first_delegate_nonce = gas_key_nonce + 5;
    let second_delegate_nonce = gas_key_nonce + 10;
    let transfer_amount = Balance::from_near(1);
    for delegate_nonce in [first_delegate_nonce, second_delegate_nonce] {
        let delegate_action = DelegateAction {
            sender_id: sender.clone(),
            receiver_id: receiver.clone(),
            actions: vec![
                Action::Transfer(TransferAction { deposit: transfer_amount }).try_into().unwrap(),
            ],
            nonce: delegate_nonce,
            max_block_height: env.rpc_node().head().height + 100,
            public_key: gas_key_signer.public_key(),
        };
        let signed_delegate_action = SignedDelegateAction::sign(&gas_key_signer, delegate_action);
        let meta_tx =
            env.rpc_node().tx_from_actions(&relayer, &sender, vec![signed_delegate_action.into()]);
        env.rpc_runner().run_tx(meta_tx, Duration::seconds(5));
        env.rpc_runner().run_for_number_of_blocks(1);
    }

    let sender_balance_before = env.rpc_node().view_account_query(&sender).unwrap().amount;
    let (_, gas_key_balance_before) =
        query_gas_key_and_balance(&env.rpc_node(), &sender, &gas_key_signer.public_key());

    // Valid after the first delegate, jumped by the second.
    let jumped_nonce = first_delegate_nonce + 1;
    let block_hash = get_shared_block_hash(&env.node_datas, &env.test_loop.data);
    let strict_tx = SignedTransaction::from_actions_v1_strict(
        TransactionNonce::from_nonce(jumped_nonce),
        sender.clone(),
        receiver,
        &gas_key_signer,
        vec![Action::Transfer(TransferAction { deposit: transfer_amount })],
        block_hash,
    );
    let tx_hash = strict_tx.get_hash();
    let rpc_error =
        env.rpc_runner().execute_tx(strict_tx.clone(), Duration::seconds(5)).unwrap_err();
    assert_eq!(
        rpc_error,
        InvalidTxError::InvalidNonce { tx_nonce: jumped_nonce, ak_nonce: second_delegate_nonce }
    );

    // Skip runtime verification during chunk preparation, so the chunk includes
    // the tx the way a producer that admitted it before the second delegate would.
    env.node_runner(0).send_adversarial_message(NetworkAdversarialMessage::AdvProduceChunks(
        AdvProduceChunksMode::ProduceWithoutTxVerification,
    ));
    let epoch_id = env.validator().head().epoch_id;
    let shard_layout = env.validator().client().epoch_manager.get_shard_layout(&epoch_id).unwrap();
    let shard_uid = shard_layout.account_id_to_shard_uid(&sender);
    env.node(0)
        .client()
        .chunk_producer
        .sharded_tx_pool
        .lock()
        .insert_transaction(shard_uid, ValidatedTransaction::new_for_test(strict_tx));

    let outcome = env.rpc_runner().run_until_outcome_available(tx_hash, Duration::seconds(10));
    env.rpc_runner().run_for_number_of_blocks(1);

    assert_eq!(
        outcome.outcome_with_id.outcome.status,
        ExecutionStatus::Failure(TxExecutionError::InvalidTxError(InvalidTxError::InvalidNonce {
            tx_nonce: jumped_nonce,
            ak_nonce: second_delegate_nonce,
        })),
    );
    let gas_burnt = outcome.outcome_with_id.outcome.gas_burnt;
    let tokens_burnt = outcome.outcome_with_id.outcome.tokens_burnt;
    assert_eq!(tokens_burnt, gas_price.checked_mul(u128::from(gas_burnt.as_gas())).unwrap());
    assert!(!tokens_burnt.is_zero());

    let (_, gas_key_balance_after) =
        query_gas_key_and_balance(&env.rpc_node(), &sender, &gas_key_signer.public_key());
    assert_eq!(gas_key_balance_after, gas_key_balance_before.checked_sub(tokens_burnt).unwrap());
    let sender_balance_after = env.rpc_node().view_account_query(&sender).unwrap().amount;
    assert_eq!(sender_balance_after, sender_balance_before);
}
