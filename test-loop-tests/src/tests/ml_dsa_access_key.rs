use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use assert_matches::assert_matches;
use near_async::time::Duration;
use near_client::QueryError;
use near_crypto::{InMemorySigner, KeyType, PublicKey, Signer};
use near_o11y::testonly::init_test_logger;
use near_primitives::account::{AccessKey, AccessKeyPermission, FunctionCallPermission};
use near_primitives::action::{AddKeyAction, DeleteKeyAction};
use near_primitives::errors::{InvalidAccessKeyError, InvalidTxError};
use near_primitives::gas::Gas;
use near_primitives::transaction::{Action, FunctionCallAction, SignedTransaction, TransferAction};
use near_primitives::types::{AccountId, Balance};
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_primitives::views::{AccessKeyPermissionView, FinalExecutionStatus};

/// Submit an `AddKey` action for `public_key` on `account` (signed by the account's default
/// ed25519 key) and wait for it to land.
fn add_key(
    env: &mut TestLoopEnv,
    account: &AccountId,
    public_key: PublicKey,
    access_key: AccessKey,
) {
    let tx = env.rpc_node().tx_from_actions(
        account,
        account,
        vec![Action::AddKey(Box::new(AddKeyAction { public_key, access_key }))],
    );
    env.rpc_runner().run_tx(tx, Duration::seconds(5));
    env.rpc_runner().run_for_number_of_blocks(1);
}

/// End-to-end happy path for an ML-DSA-65 full-access key:
///   1. add the PQ key via ed25519-signed `AddKey`
///   2. confirm the key is visible with FullAccess permission
///   3. sign and execute a transfer using the PQ key
///   4. delete the PQ key
///   5. confirm the key is gone
#[test]
fn test_ml_dsa_65_full_access_key() {
    init_test_logger();
    if !ProtocolFeature::PostQuantumSignatures.enabled(PROTOCOL_VERSION) {
        tracing::info!("skipping: PostQuantumSignatures not enabled");
        return;
    }

    let sender = create_account_id("sender");
    let receiver = create_account_id("receiver");
    let initial_balance = Balance::from_near(1_000);
    let transfer_amount = Balance::from_near(42);

    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .add_user_accounts([&sender, &receiver], initial_balance)
        .build();

    let pq_signer: Signer =
        InMemorySigner::from_seed(sender.clone(), KeyType::MLDSA65, "pq-full").into();

    // 1. AddKey for the ML-DSA-65 pubkey, signed with the account's ed25519 key.
    add_key(&mut env, &sender, pq_signer.public_key(), AccessKey::full_access());

    // 2. View the new access key.
    let view = env.rpc_node().view_access_key_query(&sender, &pq_signer.public_key()).unwrap();
    assert_eq!(view.permission, AccessKeyPermissionView::FullAccess);

    // 3. Send a transfer signed with the ML-DSA-65 key.
    let block_hash = env.rpc_node().head().last_block_hash;
    let pq_transfer = SignedTransaction::from_actions(
        view.nonce + 1,
        sender.clone(),
        receiver.clone(),
        &pq_signer,
        vec![Action::Transfer(TransferAction { deposit: transfer_amount })],
        block_hash,
    );
    env.rpc_runner().run_tx(pq_transfer, Duration::seconds(5));
    env.rpc_runner().run_for_number_of_blocks(1);

    assert_eq!(
        env.rpc_node().query_balance(&receiver),
        initial_balance.checked_add(transfer_amount).unwrap(),
    );

    // 4. Delete the ML-DSA-65 key (via the account's ed25519 key).
    let delete_key_tx = env.rpc_node().tx_from_actions(
        &sender,
        &sender,
        vec![Action::DeleteKey(Box::new(DeleteKeyAction { public_key: pq_signer.public_key() }))],
    );
    env.rpc_runner().run_tx(delete_key_tx, Duration::seconds(5));
    env.rpc_runner().run_for_number_of_blocks(1);

    // 5. Confirm the key is gone.
    assert_matches!(
        env.rpc_node().view_access_key_query(&sender, &pq_signer.public_key()),
        Err(QueryError::UnknownAccessKey { .. })
    );
}

/// End-to-end for an ML-DSA-65 function-call access key restricted to a single
/// method on a single receiver:
///   1. deploy the standard test contract
///   2. add an ML-DSA-65 function-call key allowing only `log_something`
///   3. confirm the key view reflects the permission
///   4. call the allowed method with the PQ key - succeeds
///   5. call a different method with the PQ key - rejected by the access-key
///      verifier with `MethodNameMismatch`
#[test]
fn test_ml_dsa_65_function_call_key() {
    init_test_logger();
    if !ProtocolFeature::PostQuantumSignatures.enabled(PROTOCOL_VERSION) {
        tracing::info!("skipping: PostQuantumSignatures not enabled");
        return;
    }

    let user = create_account_id("user");
    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .add_user_account(&user, Balance::from_near(100))
        .build();

    // 1. Deploy the standard test contract on `user`.
    let deploy_tx = env.rpc_node().tx_deploy_test_contract(&user);
    env.rpc_runner().run_tx(deploy_tx, Duration::seconds(5));

    // 2. Add an ML-DSA-65 function-call key allowing only `log_something` on `user`.
    let pq_signer: Signer =
        InMemorySigner::from_seed(user.clone(), KeyType::MLDSA65, "pq-fc").into();
    let access_key = AccessKey {
        nonce: 0,
        permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
            allowance: None,
            receiver_id: user.to_string(),
            method_names: vec!["log_something".to_string()],
        }),
    };
    add_key(&mut env, &user, pq_signer.public_key(), access_key);

    // 3. View the new access key.
    let view = env.rpc_node().view_access_key_query(&user, &pq_signer.public_key()).unwrap();
    let AccessKeyPermissionView::FunctionCall { allowance, receiver_id, method_names } =
        view.permission.clone()
    else {
        panic!("expected FunctionCall permission, got {:?}", view.permission);
    };
    assert_eq!(allowance, None);
    assert_eq!(receiver_id, user.to_string());
    assert_eq!(method_names, vec!["log_something".to_string()]);

    // 4. Call the allowed method - should succeed.
    let block_hash = env.rpc_node().head().last_block_hash;
    let allowed_nonce = view.nonce + 1;
    let allowed_call = SignedTransaction::from_actions(
        allowed_nonce,
        user.clone(),
        user.clone(),
        &pq_signer,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "log_something".to_string(),
            args: vec![],
            gas: Gas::from_teragas(100),
            deposit: Balance::ZERO,
        }))],
        block_hash,
    );
    let outcome = env.rpc_runner().execute_tx(allowed_call, Duration::seconds(5)).unwrap();
    assert_matches!(outcome.status, FinalExecutionStatus::SuccessValue(_));
    env.rpc_runner().run_for_number_of_blocks(1);

    // 5. Call a different method - the access-key verifier rejects it before
    //    the tx is admitted, with `MethodNameMismatch`.
    let block_hash = env.rpc_node().head().last_block_hash;
    let disallowed_call = SignedTransaction::from_actions(
        allowed_nonce + 1,
        user.clone(),
        user,
        &pq_signer,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "loop_forever".to_string(),
            args: vec![],
            gas: Gas::from_teragas(100),
            deposit: Balance::ZERO,
        }))],
        block_hash,
    );
    let err = env.rpc_runner().execute_tx(disallowed_call, Duration::seconds(5)).unwrap_err();
    assert_matches!(
        err,
        InvalidTxError::InvalidAccessKeyError(InvalidAccessKeyError::MethodNameMismatch { .. })
    );
}
