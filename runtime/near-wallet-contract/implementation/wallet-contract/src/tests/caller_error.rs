//! A suite of tests for code paths handling error cases where the `rlp_execute` function
//! is called by an external account (as opposed to the Wallet Contract calling itself).
//! Since `rlp_execute` is public, it must be impossible for an external account to
//! cause harm to the Wallet Contract by calling this function.

use crate::{
    error::{CallerError, Error},
    internal::MAX_YOCTO_NEAR,
    tests::{
        utils::{self, codec, test_context::TestContext},
        RLP_EXECUTE,
    },
    types::{Action, ExecuteResponse},
};
use aurora_engine_types::types::Wei;
use near_workspaces::types::NearToken;

// If an external account is submitting a valid Ethereum transaction signed by
// the user then it is expected that this external account should cover the entire
// cost of that transaction (including any attached $NEAR).
#[tokio::test]
async fn test_insufficient_value() -> anyhow::Result<()> {
    let TestContext { worker, wallet_contract, wallet_sk, .. } = TestContext::new().await?;

    let external_account = worker.dev_create_account().await?;

    // Create a transaction (NEP-141 transfer) that requires 1 yoctoNear attached.
    let account_id = "aurora";
    let action = Action::FunctionCall {
        receiver_id: account_id.into(),
        method_name: "ft_transfer".into(),
        args: r#"{"receiver_id": "some.account", "amount": "1"}"#.into(),
        gas: 5,
        yocto_near: 1,
    };
    let signed_transaction = utils::create_signed_transaction(
        0,
        &account_id.parse().unwrap(),
        Wei::zero(),
        action,
        &wallet_sk,
    );

    let result = wallet_contract
        .external_rlp_execute(&external_account, account_id, &signed_transaction)
        .await?;

    assert!(!result.success);
    assert_eq!(
        result.error,
        Some(Error::Caller(CallerError::InsufficientAttachedValue).to_string())
    );

    // Try again with a transaction that has some attached Wei
    let transfer_amount = NearToken::from_near(1).as_yoctonear();
    let action = Action::Transfer { receiver_id: account_id.into(), yocto_near: 0 };
    let signed_transaction = utils::create_signed_transaction(
        1,
        &account_id.parse().unwrap(),
        Wei::new_u128(transfer_amount / (MAX_YOCTO_NEAR as u128)),
        action,
        &wallet_sk,
    );
    let result = wallet_contract
        .external_rlp_execute(&external_account, account_id, &signed_transaction)
        .await?;

    assert!(!result.success);
    assert_eq!(
        result.error,
        Some(Error::Caller(CallerError::InsufficientAttachedValue).to_string())
    );

    // It works if we attach the right amount of Near and does not
    // spend any tokens from the Wallet Contract.
    let initial_wallet_balance = wallet_contract.inner.as_account().view_account().await?.balance;
    let action = Action::Transfer { receiver_id: external_account.id().to_string(), yocto_near: 0 };
    let signed_transaction = utils::create_signed_transaction(
        2,
        external_account.id(),
        Wei::new_u128(transfer_amount / (MAX_YOCTO_NEAR as u128)),
        action,
        &wallet_sk,
    );
    let result: ExecuteResponse = external_account
        .call(wallet_contract.inner.id(), RLP_EXECUTE)
        .args_json(serde_json::json!({
            "target": external_account.id(),
            "tx_bytes_b64": codec::encode_b64(&codec::rlp_encode(&signed_transaction))
        }))
        .max_gas()
        .deposit(NearToken::from_yoctonear(transfer_amount))
        .transact()
        .await?
        .into_result()?
        .json()?;

    assert!(result.success);

    let final_wallet_balance = wallet_contract.inner.as_account().view_account().await?.balance;

    assert!(final_wallet_balance >= initial_wallet_balance);

    Ok(())
}
