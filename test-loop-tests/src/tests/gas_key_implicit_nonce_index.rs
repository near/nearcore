use crate::tests::gas_keys::{
    GasKeyEnv, get_gas_key_nonce, query_gas_key_and_balance, setup_funded_gas_key,
    total_tokens_burnt,
};
use crate::utils::transactions::get_shared_block_hash;
use near_async::time::Duration;
use near_primitives::errors::InvalidTxError;
use near_primitives::transaction::{Action, SignedTransaction, TransferAction};
use near_primitives::types::Balance;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_primitives::views::FinalExecutionStatus;

#[test]
#[cfg_attr(not(feature = "nightly"), ignore)]
fn test_v0_tx_on_gas_key_uses_implicit_nonce_index() {
    assert!(ProtocolFeature::GasKeyImplicitNonceIndex.enabled(PROTOCOL_VERSION));
    let sender_balance = Balance::from_near(100);
    let gas_key_balance = Balance::from_millinear(1);
    let num_nonces = 3;
    let GasKeyEnv { mut env, sender, receiver, gas_key_signer, .. } =
        setup_funded_gas_key(PROTOCOL_VERSION, sender_balance, gas_key_balance, num_nonces);

    let implicit_nonce_index = 0;
    let gas_key_nonce =
        get_gas_key_nonce(&env, &sender, &gas_key_signer.public_key(), implicit_nonce_index);
    let access_key_view =
        env.rpc_node().view_access_key_query(&sender, &gas_key_signer.public_key()).unwrap();
    assert_eq!(access_key_view.nonce, gas_key_nonce);

    let sender_before = env.rpc_node().view_account_query(&sender).unwrap().amount;
    let (_, key_before) =
        query_gas_key_and_balance(&env.rpc_node(), &sender, &gas_key_signer.public_key());
    let block_hash = get_shared_block_hash(&env.node_datas, &env.test_loop.data);
    let transfer_amount = Balance::from_near(1);
    let v0_tx = SignedTransaction::from_actions(
        access_key_view.nonce + 1,
        sender.clone(),
        receiver,
        &gas_key_signer,
        vec![Action::Transfer(TransferAction { deposit: transfer_amount })],
        block_hash,
    );
    let outcome = env.rpc_runner().execute_tx(v0_tx, Duration::seconds(5)).unwrap();
    env.rpc_runner().run_for_number_of_blocks(1);

    assert!(
        matches!(outcome.status, FinalExecutionStatus::SuccessValue(_)),
        "expected success, got {:?}",
        outcome.status,
    );
    assert_eq!(
        get_gas_key_nonce(&env, &sender, &gas_key_signer.public_key(), implicit_nonce_index),
        gas_key_nonce + 1
    );
    let access_key_view_after =
        env.rpc_node().view_access_key_query(&sender, &gas_key_signer.public_key()).unwrap();
    assert_eq!(access_key_view_after.nonce, gas_key_nonce + 1);

    let tokens_burnt = total_tokens_burnt(&outcome);
    let sender_after = env.rpc_node().view_account_query(&sender).unwrap().amount;
    assert_eq!(
        sender_after,
        sender_before.checked_sub(transfer_amount).unwrap().checked_sub(tokens_burnt).unwrap()
    );
    let (_, key_after) =
        query_gas_key_and_balance(&env.rpc_node(), &sender, &gas_key_signer.public_key());
    assert_eq!(key_after, key_before);
}

#[test]
fn test_v0_tx_on_gas_key_rejected_before_implicit_nonce_index() {
    let protocol_version =
        (ProtocolFeature::GasKeyImplicitNonceIndex.protocol_version() - 1).min(PROTOCOL_VERSION);
    let sender_balance = Balance::from_near(100);
    let gas_key_balance = Balance::from_millinear(1);
    let num_nonces = 3;
    let GasKeyEnv { mut env, sender, receiver, gas_key_signer, .. } =
        setup_funded_gas_key(protocol_version, sender_balance, gas_key_balance, num_nonces);

    let gas_key_access_key_nonce = 0;
    let access_key_view =
        env.rpc_node().view_access_key_query(&sender, &gas_key_signer.public_key()).unwrap();
    assert_eq!(access_key_view.nonce, gas_key_access_key_nonce);

    let block_hash = get_shared_block_hash(&env.node_datas, &env.test_loop.data);
    let v0_tx = SignedTransaction::from_actions(
        access_key_view.nonce + 1,
        sender,
        receiver,
        &gas_key_signer,
        vec![Action::Transfer(TransferAction { deposit: Balance::from_near(1) })],
        block_hash,
    );
    let err = env.rpc_runner().execute_tx(v0_tx, Duration::seconds(5)).unwrap_err();
    assert_eq!(err, InvalidTxError::InvalidNonceIndex { tx_nonce_index: None, num_nonces });
}
