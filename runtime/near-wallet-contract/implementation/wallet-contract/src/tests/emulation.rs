use crate::{
    internal::{account_id_to_address, CHAIN_ID, MAX_YOCTO_NEAR},
    tests::utils::{crypto, nep141, test_context::TestContext},
    types::ExecuteResponse,
};
use aurora_engine_types::types::{Address, Wei};
use near_sdk::json_types::U128;
use near_workspaces::{result::ValueOrReceiptId, types::NearToken};

// The Wallet Contract should understand that transactions to other Wallet
// Contract instances are base token transactions.
#[tokio::test]
async fn test_base_token_transfer() -> anyhow::Result<()> {
    const TRANSFER_AMOUNT: NearToken = NearToken::from_near(2);

    let TestContext {
        worker,
        wallet_contract,
        wallet_address,
        wallet_sk,
        wallet_contract_bytes,
        ..
    } = TestContext::new().await?;

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

    // Base token transfers to self are also allowed, but do not actually move any funds.
    let transaction = aurora_engine_transactions::eip_2930::Transaction2930 {
        nonce: 1.into(),
        gas_price: 0.into(),
        gas_limit: 0.into(),
        to: Some(Address::new(wallet_address)),
        value: Wei::new_u128(TRANSFER_AMOUNT.as_yoctonear() / u128::from(MAX_YOCTO_NEAR)),
        data: b"Note to self".to_vec(),
        chain_id: CHAIN_ID,
        access_list: Vec::new(),
    };
    let signed_transaction = crypto::sign_transaction(transaction, &wallet_sk);

    let result = wallet_contract
        .rlp_execute(wallet_contract.inner.id().as_str(), &signed_transaction)
        .await?;

    assert!(result.success);

    let initial_wallet_balance = final_wallet_balance;
    let final_wallet_balance = wallet_contract.inner.as_account().view_account().await?.balance;
    let diff = NearToken::from_yoctonear(
        initial_wallet_balance.as_yoctonear() - final_wallet_balance.as_yoctonear(),
    );
    assert!(diff < NearToken::from_millinear(2));

    // If the relayer adds an account suffix different from the wallet contract,
    // then the transaction is rejected.
    let transaction = aurora_engine_transactions::eip_2930::Transaction2930 {
        nonce: 2.into(),
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
    let result = wallet_contract.rlp_execute_with_receipts(&target, &signed_transaction).await?;

    // Transaction is rejected for a wrong namespace so there is a faulty relayer error.
    for r in result.receipt_outcomes() {
        let response: ExecuteResponse = match r.clone().into_result().unwrap() {
            ValueOrReceiptId::ReceiptId(_) => continue,
            ValueOrReceiptId::Value(value) => match value.json() {
                Err(_) => continue,
                Ok(x) => x,
            },
        };
        assert!(!response.success, "Expected failure");
        assert_eq!(response.error.as_deref(), Some("Error: faulty relayer"));
    }

    let initial_wallet_balance = final_wallet_balance;
    let final_wallet_balance = wallet_contract.inner.as_account().view_account().await?.balance;

    // Sender balance does not decrease (other than the gas spent to execute the transaction)
    let diff = NearToken::from_yoctonear(
        initial_wallet_balance.as_yoctonear() - final_wallet_balance.as_yoctonear(),
    );
    assert!(diff < NearToken::from_millinear(2));

    Ok(())
}

// Relayers are paid for base token transfers.
#[tokio::test]
async fn test_base_token_transfer_with_relayer_refund() -> anyhow::Result<()> {
    const TRANSFER_AMOUNT: NearToken = NearToken::from_near(2);
    const RELAYER_REFUND: NearToken = NearToken::from_millinear(1);
    const GAS_LIMIT: u64 = 100_000;

    let TestContext { worker, wallet_contract, wallet_sk, wallet_contract_bytes, .. } =
        TestContext::new().await?;

    let relayer = worker.root_account()?;

    let (other_wallet, other_address) =
        TestContext::deploy_wallet(&worker, &wallet_contract_bytes).await?;

    let initial_relayer_balance = relayer.view_account().await?.balance;
    let initial_wallet_balance = wallet_contract.inner.as_account().view_account().await?.balance;
    let initial_other_balance = other_wallet.inner.as_account().view_account().await?.balance;

    let transaction = aurora_engine_transactions::eip_2930::Transaction2930 {
        nonce: 0.into(),
        gas_price: (RELAYER_REFUND.as_yoctonear()
            / ((GAS_LIMIT as u128) * (MAX_YOCTO_NEAR as u128)))
            .into(),
        gas_limit: GAS_LIMIT.into(),
        to: Some(Address::new(other_address)),
        value: Wei::new_u128(TRANSFER_AMOUNT.as_yoctonear() / u128::from(MAX_YOCTO_NEAR)),
        data: Vec::new(),
        chain_id: CHAIN_ID,
        access_list: Vec::new(),
    };
    let signed_transaction = crypto::sign_transaction(transaction, &wallet_sk);

    let result = wallet_contract
        .rlp_execute_from(
            &relayer,
            other_wallet.inner.id().as_str(),
            &signed_transaction,
            NearToken::from_yoctonear(0),
        )
        .await?;

    assert!(result.success);

    let final_relayer_balance = relayer.view_account().await?.balance;
    let final_wallet_balance = wallet_contract.inner.as_account().view_account().await?.balance;
    let final_other_balance = other_wallet.inner.as_account().view_account().await?.balance;

    // Receiver balance increases
    assert_eq!(
        final_other_balance.as_yoctonear(),
        initial_other_balance.as_yoctonear() + TRANSFER_AMOUNT.as_yoctonear()
    );

    // Wallet balance decreases (round to milliNEAR to account for funds
    // received for calling the contract).
    assert_eq!(
        final_wallet_balance.as_millinear(),
        initial_wallet_balance.as_millinear()
            - TRANSFER_AMOUNT.as_millinear()
            - RELAYER_REFUND.as_millinear()
    );

    // Relayer balance stays the same (rounded to the nearest milliNEAR) since the
    // wallet refunded the relayer approximately equal to the transaction gas cost.
    assert_eq!(final_relayer_balance.as_millinear(), initial_relayer_balance.as_millinear());

    Ok(())
}

// The Wallet Contract should understand the ERC-20 standard and map
// it to NEP-141 function calls.
#[tokio::test]
async fn test_erc20_emulation() -> anyhow::Result<()> {
    const MINT_AMOUNT: NearToken = NearToken::from_near(100);
    const TRANSFER_AMOUNT: NearToken = NearToken::from_near(32);
    const RELAYER_REFUND: NearToken = NearToken::from_millinear(3);
    const GAS_LIMIT: u64 = 100_000;

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
            crate::eth_emulation::ERC20_TRANSFER_SELECTOR,
            ethabi::encode(&[
                ethabi::Token::Address(other_address),
                ethabi::Token::Uint(TRANSFER_AMOUNT.as_yoctonear().into()),
            ])
            .as_slice(),
            b"hello_world", // Include a memo
        ]
        .concat(),
        chain_id: CHAIN_ID,
        access_list: Vec::new(),
    };
    let signed_transaction = crypto::sign_transaction(transaction, &wallet_sk);

    let result = wallet_contract
        .rlp_execute_with_receipts(token_contract.contract.id().as_str(), &signed_transaction)
        .await?;

    assert_eq!(
        get_nep_141_memo(result.logs().first().unwrap())?.as_deref(),
        Some("0x68656c6c6f5f776f726c64")
    );

    let result: ExecuteResponse = result.json()?;
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
        .rlp_execute_with_receipts(token_contract.contract.id().as_str(), &signed_transaction)
        .await?;

    assert_eq!(get_nep_141_memo(result.logs().first().unwrap())?, None);

    let result: ExecuteResponse = result.json()?;
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

    // Check total supply also works
    let transaction = aurora_engine_transactions::eip_2930::Transaction2930 {
        nonce: 3.into(),
        gas_price: 0.into(),
        gas_limit: 0.into(),
        to: Some(Address::new(account_id_to_address(
            &token_contract.contract.id().as_str().parse().unwrap(),
        ))),
        value: Wei::zero(),
        data: crate::eth_emulation::ERC20_TOTAL_SUPPLY_SELECTOR.to_vec(),
        chain_id: CHAIN_ID,
        access_list: Vec::new(),
    };
    let signed_transaction = crypto::sign_transaction(transaction, &wallet_sk);

    let result = wallet_contract
        .rlp_execute(token_contract.contract.id().as_str(), &signed_transaction)
        .await?;

    let balance: U128 = serde_json::from_slice(result.success_value.as_ref().unwrap())?;
    assert_eq!(balance.0, MINT_AMOUNT.as_yoctonear());

    // If an external relayer triggers the transaction then it is
    // compensated for the Near gas.
    let transaction = aurora_engine_transactions::eip_2930::Transaction2930 {
        nonce: 4.into(),
        gas_price: (RELAYER_REFUND.as_yoctonear()
            / ((GAS_LIMIT as u128) * (MAX_YOCTO_NEAR as u128)))
            .into(),
        gas_limit: GAS_LIMIT.into(),
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

    let relayer = worker.root_account()?;
    let initial_relayer_balance = relayer.view_account().await?.balance;
    let initial_wallet_balance = wallet_contract.inner.as_account().view_account().await?.balance;

    let result = wallet_contract
        .rlp_execute_from(
            &relayer,
            token_contract.contract.id().as_str(),
            &signed_transaction,
            NearToken::from_yoctonear(0),
        )
        .await?;

    assert!(result.success);
    assert_eq!(
        MINT_AMOUNT.as_yoctonear() - (3 * TRANSFER_AMOUNT.as_yoctonear()),
        token_contract.ft_balance_of(wallet_contract.inner.id()).await?
    );
    assert_eq!(
        3 * TRANSFER_AMOUNT.as_yoctonear(),
        token_contract.ft_balance_of(other_wallet.inner.id()).await?
    );

    let final_relayer_balance = relayer.view_account().await?.balance;
    let final_wallet_balance = wallet_contract.inner.as_account().view_account().await?.balance;

    // Relayer balance stays the same (rounded to the nearest milliNEAR) since the
    // wallet refunded the relayer approximately equal to the transaction gas cost.
    assert_eq!(final_relayer_balance.as_millinear(), initial_relayer_balance.as_millinear());

    // Wallet balance decreases (round to milliNEAR to account for funds
    // received for calling the contract).
    assert_eq!(
        final_wallet_balance.as_millinear(),
        initial_wallet_balance.as_millinear() - RELAYER_REFUND.as_millinear()
    );

    Ok(())
}

fn get_nep_141_memo(log: &str) -> anyhow::Result<Option<String>> {
    let log = log
        .strip_prefix("EVENT_JSON:")
        .ok_or_else(|| anyhow::Error::msg("Expected `EVENT_JSON` log"))?;
    let log: TransferLog = serde_json::from_str(log)?;
    Ok(log.data[0].memo.clone())
}

#[allow(dead_code)]
#[derive(Debug, serde::Deserialize)]
struct TransferLog {
    standard: String,
    version: String,
    event: String,
    data: [TransferData; 1],
}

#[allow(dead_code)]
#[derive(Debug, serde::Deserialize)]
struct TransferData {
    old_owner_id: String,
    new_owner_id: String,
    amount: String,
    memo: Option<String>,
}
