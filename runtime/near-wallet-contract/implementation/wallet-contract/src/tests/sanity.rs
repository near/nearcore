use aurora_engine_types::types::Wei;
use near_sdk::NearToken;

use crate::{
    internal::MAX_YOCTO_NEAR,
    tests::utils::{self, test_context::TestContext},
    types::Action,
};

// The initial nonce value for a Wallet Contract should be 0.
#[tokio::test]
async fn test_initial_nonce() -> anyhow::Result<()> {
    let TestContext { wallet_contract, .. } = TestContext::new().await?;

    let nonce = wallet_contract.get_nonce().await?;
    assert_eq!(nonce, 0);

    Ok(())
}

// The Wallet Contract should be able to call other Near smart contracts
#[tokio::test]
async fn test_function_call_action_success() -> anyhow::Result<()> {
    let TestContext { worker, wallet_contract, wallet_sk, .. } = TestContext::new().await?;

    utils::deploy_and_call_hello(&worker, &wallet_contract, &wallet_sk, 0).await?;

    // After the transaction the nonce is incremented
    let nonce = wallet_contract.get_nonce().await?;
    assert_eq!(nonce, 1);

    Ok(())
}

// The Wallet Contract should be able to send $NEAR to other Near accounts.
#[tokio::test]
async fn test_base_token_transfer_success() -> anyhow::Result<()> {
    let TestContext { worker, wallet_contract, wallet_sk, .. } = TestContext::new().await?;

    let transfer_amount = NearToken::from_near(2).as_yoctonear() + 1;
    let receiver_account = worker.root_account().unwrap();

    let initial_wallet_balance =
        wallet_contract.inner.as_account().view_account().await.unwrap().balance;
    let initial_receiver_balance = receiver_account.view_account().await.unwrap().balance;

    let receiver_id = receiver_account.id().as_str().into();
    let action = Action::Transfer { receiver_id, yocto_near: 1 };
    let value = Wei::new_u128(transfer_amount / (MAX_YOCTO_NEAR as u128));
    let signed_transaction =
        utils::create_signed_transaction(0, receiver_account.id(), value, action, &wallet_sk);

    let result =
        wallet_contract.rlp_execute(receiver_account.id().as_str(), &signed_transaction).await?;
    assert!(result.success);

    let final_wallet_balance =
        wallet_contract.inner.as_account().view_account().await.unwrap().balance;
    let final_receiver_balance = receiver_account.view_account().await.unwrap().balance;

    // Check token balances
    assert_eq!(
        final_receiver_balance.as_yoctonear() - initial_receiver_balance.as_yoctonear(),
        transfer_amount
    );
    // Wallet loses a little more $NEAR than the transfer amount
    // due to gas spent on the transaction.
    let diff = initial_wallet_balance.as_yoctonear()
        - final_wallet_balance.as_yoctonear()
        - transfer_amount;
    assert!(diff < NearToken::from_millinear(2).as_yoctonear());

    Ok(())
}
