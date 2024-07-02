use aurora_engine_types::types::Wei;
use near_sdk::NearToken;

use crate::{
    internal::MAX_YOCTO_NEAR,
    tests::utils::{self, codec, test_context::TestContext},
    types::{Action, ExecuteResponse},
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

/// Only one transaction can be in flight at a time.
#[tokio::test]
async fn test_simultaneous_transactions() -> anyhow::Result<()> {
    let TestContext { worker, wallet_contract, wallet_sk, .. } = TestContext::new().await?;

    let receiver_account = worker.root_account().unwrap();

    let initial_receiver_balance = receiver_account.view_account().await.unwrap().balance;

    let receiver_id = receiver_account.id().as_str().into();
    let action = Action::Transfer { receiver_id, yocto_near: 1 };
    let signed_transaction =
        utils::create_signed_transaction(0, receiver_account.id(), Wei::zero(), action, &wallet_sk);
    let wallet_method_call_1 = near_workspaces::operations::Function::new("rlp_execute")
        .args_json(serde_json::json!({
            "target": receiver_account.id(),
            "tx_bytes_b64": codec::encode_b64(&codec::rlp_encode(&signed_transaction))
        }))
        .gas(near_workspaces::types::Gas::from_tgas(100));
    let wallet_method_call_2 = near_workspaces::operations::Function::new("rlp_execute")
        .args_json(serde_json::json!({
            "target": receiver_account.id(),
            "tx_bytes_b64": codec::encode_b64(&codec::rlp_encode(&signed_transaction))
        }))
        .gas(near_workspaces::types::Gas::from_tgas(100));

    let near_transaction = wallet_contract
        .inner
        .as_account()
        .batch(wallet_contract.inner.id())
        .call(wallet_method_call_1)
        .call(wallet_method_call_2)
        .transact()
        .await?;

    let result: ExecuteResponse = near_transaction.json()?;

    // The second transaction in the batch fails and this is returned as the
    // result of the Near transaction. But the first transaction in the batch
    // spawns promises that resolve, so the transfer was will successful.
    assert!(!result.success);
    assert!(result.error.unwrap().contains("transaction already in progress"));

    let final_receiver_balance = receiver_account.view_account().await.unwrap().balance;
    assert_eq!(final_receiver_balance.as_yoctonear() - initial_receiver_balance.as_yoctonear(), 1,);

    Ok(())
}

/// Test asserting that eth-implicit accounts cannot be registered.
#[tokio::test]
async fn test_register_eth_implicit_account() -> anyhow::Result<()> {
    let TestContext { address_registrar, .. } = TestContext::new().await?;

    let args = br#"{"account_id": "0x8fd379246834eac74b8419ffda202cf8051f7a03"}"#;
    let result = address_registrar.call("register").args(args.to_vec()).transact().await?;

    let logs = result.logs();
    assert_eq!(logs.len(), 1);
    assert!(logs[0].contains("Refuse to register eth-implicit account"));

    let output: Option<String> = result.json()?;
    assert_eq!(output, None);

    Ok(())
}

/// Test asserting the address registrar requires a deposit.
#[tokio::test]
async fn test_register_without_deposit() -> anyhow::Result<()> {
    let TestContext { worker, address_registrar, .. } = TestContext::new().await?;

    let method = "register";
    let args = br#"{"account_id": "birchmd.near"}"#;
    let result = address_registrar.call(method).args(args.to_vec()).transact().await?;
    assert!(result.is_failure(), "Call without deposit must fail");

    let result = worker
        .root_account()?
        .call(address_registrar.id(), method)
        .args(args.to_vec())
        .deposit(NearToken::from_yoctonear(320000000000000000000))
        .transact()
        .await?;

    let output: Option<String> = result.json()?;
    assert_eq!(output.as_deref(), Some("0x4bfcff9a964925adf801c866f6ada98bd7ec40ca"));

    Ok(())
}
