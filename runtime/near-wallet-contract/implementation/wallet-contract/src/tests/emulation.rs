use crate::{
    internal::{account_id_to_address, CHAIN_ID, MAX_YOCTO_NEAR},
    tests::utils::{crypto, nep141, test_context::TestContext},
};
use aurora_engine_types::types::{Address, Wei};
use near_sdk::json_types::U128;
use near_workspaces::types::NearToken;

// The Wallet Contract should understand that transactions to other Wallet
// Contract instances are base token transactions.
#[tokio::test]
async fn test_base_token_transfer() -> anyhow::Result<()> {
    const TRANSFER_AMOUNT: NearToken = NearToken::from_near(2);

    let TestContext { worker, wallet_contract, wallet_sk, wallet_contract_bytes, .. } =
        TestContext::new().await?;

    let (other_wallet, other_address) =
        TestContext::deploy_wallet(&worker, &wallet_contract_bytes).await?;

    let initial_wallet_balance = wallet_contract.inner.as_account().view_account().await?.balance;
    let initial_other_balance = other_wallet.inner.as_account().view_account().await?.balance;

    let transaction = aurora_engine_transactions::eip_2930::Transaction2930 {
        nonce: 0.into(),
        gas_price: 0.into(),
        gas_limit: 0.into(),
        to: Some(Address::new(other_address)),
        value: Wei::new_u128(TRANSFER_AMOUNT.as_yoctonear() / u128::from(MAX_YOCTO_NEAR)),
        data: b"A message for the recipient".to_vec(),
        chain_id: CHAIN_ID,
        access_list: Vec::new(),
    };
    let signed_transaction = crypto::sign_transaction(transaction, &wallet_sk);

    let result =
        wallet_contract.rlp_execute(other_wallet.inner.id().as_str(), &signed_transaction).await?;

    assert!(result.success);

    let final_wallet_balance = wallet_contract.inner.as_account().view_account().await?.balance;
    let final_other_balance = other_wallet.inner.as_account().view_account().await?.balance;

    // Receiver balance increases
    assert_eq!(
        final_other_balance.as_yoctonear(),
        initial_other_balance.as_yoctonear() + TRANSFER_AMOUNT.as_yoctonear()
    );
    // Sender balance decreases (by a little more than the
    // `TRANSFER_AMOUNT` due to gas spent to execute the transaction)
    let diff = NearToken::from_yoctonear(
        initial_wallet_balance.as_yoctonear()
            - (final_wallet_balance.as_yoctonear() + TRANSFER_AMOUNT.as_yoctonear()),
    );
    assert!(diff < NearToken::from_millinear(2));

    // If the relayer adds an account suffix different from the wallet contract,
    // then it the transaction is rejected.
    let transaction = aurora_engine_transactions::eip_2930::Transaction2930 {
        nonce: 1.into(),
        gas_price: 0.into(),
        gas_limit: 0.into(),
        to: Some(Address::new(other_address)),
        value: Wei::new_u128(TRANSFER_AMOUNT.as_yoctonear() / u128::from(MAX_YOCTO_NEAR)),
        data: b"A message for the recipient".to_vec(),
        chain_id: CHAIN_ID,
        access_list: Vec::new(),
    };
    let signed_transaction = crypto::sign_transaction(transaction, &wallet_sk);

    let target = format!("0x{}.wrong.suffix", hex::encode(other_address));
    wallet_contract.rlp_execute(&target, &signed_transaction).await?;

    let final_other_balance = other_wallet.inner.as_account().view_account().await?.balance;

    // Receiver balance remains unchanged
    assert_eq!(
        final_other_balance.as_yoctonear(),
        initial_other_balance.as_yoctonear() + TRANSFER_AMOUNT.as_yoctonear()
    );

    Ok(())
}

// The Wallet Contract should understand the ERC-20 standard and map
// it to NEP-141 function calls.
#[tokio::test]
async fn test_erc20_emulation() -> anyhow::Result<()> {
    const MINT_AMOUNT: NearToken = NearToken::from_near(100);
    const TRANSFER_AMOUNT: NearToken = NearToken::from_near(32);

    let TestContext {
        worker,
        wallet_contract,
        wallet_sk,
        wallet_address,
        wallet_contract_bytes,
        ..
    } = TestContext::new().await?;

    let token_contract = nep141::Nep141::deploy(&worker).await?;
    token_contract.mint(wallet_contract.inner.id(), MINT_AMOUNT.as_yoctonear()).await?;

    // Check balance
    let transaction = aurora_engine_transactions::eip_2930::Transaction2930 {
        nonce: 0.into(),
        gas_price: 0.into(),
        gas_limit: 0.into(),
        to: Some(Address::new(account_id_to_address(
            &token_contract.contract.id().as_str().parse().unwrap(),
        ))),
        value: Wei::zero(),
        data: [
            crate::eth_emulation::ERC20_BALANCE_OF_SELECTOR.to_vec(),
            ethabi::encode(&[ethabi::Token::Address(wallet_address)]),
        ]
        .concat(),
        chain_id: CHAIN_ID,
        access_list: Vec::new(),
    };
    let signed_transaction = crypto::sign_transaction(transaction, &wallet_sk);

    let result = wallet_contract
        .rlp_execute(token_contract.contract.id().as_str(), &signed_transaction)
        .await?;

    let balance: U128 = serde_json::from_slice(result.success_value.as_ref().unwrap())?;
    assert_eq!(balance.0, token_contract.ft_balance_of(wallet_contract.inner.id()).await?);

    // Do a transfer to another account. Note that the other account has never
    // held this token before so it will require a storage deposit, but this is
    // handled automatically by the wallet contract.
    let (other_wallet, other_address) =
        TestContext::deploy_wallet(&worker, &wallet_contract_bytes).await?;
    let transaction = aurora_engine_transactions::eip_2930::Transaction2930 {
        nonce: 1.into(),
        gas_price: 0.into(),
        gas_limit: 0.into(),
        to: Some(Address::new(account_id_to_address(
            &token_contract.contract.id().as_str().parse().unwrap(),
        ))),
        value: Wei::zero(),
        data: [
            crate::eth_emulation::ERC20_TRANSFER_SELECTOR.to_vec(),
            ethabi::encode(&[
                ethabi::Token::Address(other_address),
                ethabi::Token::Uint(TRANSFER_AMOUNT.as_yoctonear().into()),
            ]),
        ]
        .concat(),
        chain_id: CHAIN_ID,
        access_list: Vec::new(),
    };
    let signed_transaction = crypto::sign_transaction(transaction, &wallet_sk);

    let result = wallet_contract
        .rlp_execute(token_contract.contract.id().as_str(), &signed_transaction)
        .await?;

    assert!(result.success);
    assert_eq!(
        MINT_AMOUNT.as_yoctonear() - TRANSFER_AMOUNT.as_yoctonear(),
        token_contract.ft_balance_of(wallet_contract.inner.id()).await?
    );
    assert_eq!(
        TRANSFER_AMOUNT.as_yoctonear(),
        token_contract.ft_balance_of(other_wallet.inner.id()).await?
    );

    // Now send a second transfer. This time the storage deposit is not needed
    // and this case is still handled well by the wallet contract.
    let transaction = aurora_engine_transactions::eip_2930::Transaction2930 {
        nonce: 2.into(),
        gas_price: 0.into(),
        gas_limit: 0.into(),
        to: Some(Address::new(account_id_to_address(
            &token_contract.contract.id().as_str().parse().unwrap(),
        ))),
        value: Wei::zero(),
        data: [
            crate::eth_emulation::ERC20_TRANSFER_SELECTOR.to_vec(),
            ethabi::encode(&[
                ethabi::Token::Address(other_address),
                ethabi::Token::Uint(TRANSFER_AMOUNT.as_yoctonear().into()),
            ]),
        ]
        .concat(),
        chain_id: CHAIN_ID,
        access_list: Vec::new(),
    };
    let signed_transaction = crypto::sign_transaction(transaction, &wallet_sk);

    let result = wallet_contract
        .rlp_execute(token_contract.contract.id().as_str(), &signed_transaction)
        .await?;

    assert!(result.success);
    assert_eq!(
        MINT_AMOUNT.as_yoctonear() - (2 * TRANSFER_AMOUNT.as_yoctonear()),
        token_contract.ft_balance_of(wallet_contract.inner.id()).await?
    );
    assert_eq!(
        2 * TRANSFER_AMOUNT.as_yoctonear(),
        token_contract.ft_balance_of(other_wallet.inner.id()).await?
    );
    assert_eq!(
        nep141::STORAGE_DEPOSIT_AMOUNT,
        token_contract
            .storage_balance_of(other_wallet.inner.id())
            .await?
            .unwrap()
            .total
            .as_yoctonear(),
    );

    Ok(())
}
