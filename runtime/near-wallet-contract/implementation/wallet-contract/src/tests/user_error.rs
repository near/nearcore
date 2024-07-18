//! A suite of tests for code paths handling cases where the user signed transaction
//! data that is invalid in some way. This is as opposed to errors which arise
//! from faulty relayers.

use crate::{
    error::{Error, UnsupportedAction, UserError},
    internal::{account_id_to_address, CHAIN_ID, MAX_YOCTO_NEAR},
    tests::utils::{
        self, crypto,
        test_context::{TestContext, WalletContract},
    },
    types::{Action, FUNCTION_CALL_SELECTOR},
};
use aurora_engine_types::types::{Address, Wei};
use near_workspaces::types::{KeyType, SecretKey};

// Transactions which would deploy an EVM contract are not allowed because
// there is no native EVM bytecode interpreter on Near.
#[tokio::test]
async fn test_evm_deploy() -> anyhow::Result<()> {
    let TestContext { wallet_contract, wallet_sk, .. } = TestContext::new().await?;

    let transaction = aurora_engine_transactions::eip_2930::Transaction2930 {
        nonce: 0.into(),
        gas_price: 0.into(),
        gas_limit: 0.into(),
        to: None,
        value: Wei::zero(),
        data: Vec::new(),
        chain_id: CHAIN_ID,
        access_list: Vec::new(),
    };
    let signed_transaction = crypto::sign_transaction(transaction, &wallet_sk);

    let result = wallet_contract.rlp_execute("aurora", &signed_transaction).await?;

    assert!(!result.success);
    assert_eq!(result.error, Some(Error::User(UserError::EvmDeployDisallowed).to_string()));

    Ok(())
}

// The Near value of a transaction is equal to `tx.value * 1e6 + action.yocto_near`.
// Near values must be 128-bit numbers. Therefore `tx.value` cannot be larger than
// `u128::MAX // 1e6`.
#[tokio::test]
async fn test_value_too_large() -> anyhow::Result<()> {
    let TestContext { wallet_contract, wallet_sk, .. } = TestContext::new().await?;

    let account_id = "aurora";
    let action = Action::Transfer { receiver_id: account_id.into(), yocto_near: 0 };
    let signed_transaction = utils::create_signed_transaction(
        0,
        &account_id.parse().unwrap(),
        Wei::new_u128(u128::MAX),
        action,
        &wallet_sk,
    );

    let result = wallet_contract.rlp_execute(account_id, &signed_transaction).await?;

    assert!(!result.success);
    assert_eq!(result.error, Some(Error::User(UserError::ValueTooLarge).to_string()));

    Ok(())
}

// Test case where `AddKey`/`DeleteKey` action contains an unknown public key kind
#[tokio::test]
async fn test_unknown_public_key_kind() -> anyhow::Result<()> {
    let TestContext { wallet_contract, wallet_sk, .. } = TestContext::new().await?;

    let account_id = "aurora";
    let action = Action::DeleteKey { public_key_kind: 2, public_key: b"a_new_key_type".to_vec() };
    let signed_transaction = utils::create_signed_transaction(
        0,
        &account_id.parse().unwrap(),
        Wei::zero(),
        action,
        &wallet_sk,
    );

    let result = wallet_contract.rlp_execute(account_id, &signed_transaction).await?;

    assert!(!result.success);
    assert_eq!(result.error, Some(Error::User(UserError::UnknownPublicKeyKind).to_string()));

    let action = Action::AddKey {
        public_key_kind: 2,
        public_key: b"some_key".to_vec(),
        nonce: 0,
        is_full_access: false,
        is_limited_allowance: false,
        allowance: 0,
        receiver_id: account_id.into(),
        method_names: Vec::new(),
    };
    let signed_transaction = utils::create_signed_transaction(
        1,
        &account_id.parse().unwrap(),
        Wei::zero(),
        action,
        &wallet_sk,
    );

    let result = wallet_contract.rlp_execute(account_id, &signed_transaction).await?;

    assert!(!result.success);
    assert_eq!(result.error, Some(Error::User(UserError::UnknownPublicKeyKind).to_string()));

    Ok(())
}

// Test case where `AddKey`/`DeleteKey` action contains invalid public key bytes
#[tokio::test]
async fn test_invalid_public_key() -> anyhow::Result<()> {
    async fn assert_invalid_pk(
        ctx: &TestContext,
        public_key_kind: u8,
        public_key: Vec<u8>,
        expected_error: UserError,
    ) -> anyhow::Result<()> {
        let wallet_contract = &ctx.wallet_contract;
        let wallet_sk = &ctx.wallet_sk;

        let nonce = wallet_contract.get_nonce().await?;
        let account_id = "aurora";
        let action = Action::DeleteKey { public_key_kind, public_key: public_key.clone() };
        let signed_transaction = utils::create_signed_transaction(
            nonce,
            &account_id.parse().unwrap(),
            Wei::zero(),
            action,
            wallet_sk,
        );

        let result = wallet_contract.rlp_execute(account_id, &signed_transaction).await?;

        assert!(!result.success);
        assert_eq!(result.error, Some(Error::User(expected_error.clone()).to_string()));

        let action = Action::AddKey {
            public_key_kind,
            public_key,
            nonce: 0,
            is_full_access: false,
            is_limited_allowance: false,
            allowance: 0,
            receiver_id: account_id.into(),
            method_names: Vec::new(),
        };
        let signed_transaction = utils::create_signed_transaction(
            nonce + 1,
            &account_id.parse().unwrap(),
            Wei::zero(),
            action,
            wallet_sk,
        );

        let result = wallet_contract.rlp_execute(account_id, &signed_transaction).await?;

        assert!(!result.success);
        assert_eq!(result.error, Some(Error::User(expected_error).to_string()));

        Ok(())
    }

    let ctx = TestContext::new().await?;

    assert_invalid_pk(&ctx, 0, Vec::new(), UserError::InvalidEd25519Key).await?;
    assert_invalid_pk(&ctx, 0, b"wrong_length".to_vec(), UserError::InvalidEd25519Key).await?;

    assert_invalid_pk(&ctx, 1, Vec::new(), UserError::InvalidSecp256k1Key).await?;
    assert_invalid_pk(&ctx, 1, b"wrong_length".to_vec(), UserError::InvalidSecp256k1Key).await?;

    Ok(())
}

// Tests case where we try to add an access key with an invalid `receiver_id`
#[tokio::test]
async fn test_invalid_public_key_account_id() -> anyhow::Result<()> {
    let TestContext { wallet_contract, wallet_sk, .. } = TestContext::new().await?;

    let key = SecretKey::from_random(KeyType::ED25519);
    let account_id = "aurora";
    let non_account_id = "---***---";
    let action = Action::AddKey {
        public_key_kind: 0,
        public_key: key.public_key().key_data().to_vec(),
        nonce: 0,
        is_full_access: false,
        is_limited_allowance: false,
        allowance: 0,
        receiver_id: non_account_id.into(),
        method_names: Vec::new(),
    };
    let signed_transaction = utils::create_signed_transaction(
        0,
        &account_id.parse().unwrap(),
        Wei::zero(),
        action,
        &wallet_sk,
    );

    let result = wallet_contract.rlp_execute(account_id, &signed_transaction).await?;

    assert!(!result.success);
    assert_eq!(result.error, Some(Error::User(UserError::InvalidAccessKeyAccountId).to_string()));

    Ok(())
}

// User's are not allowed to add full access keys to the account.
// This would be too dangerous as it could allow for undefined behaviour
// such as deploying a different contract to an Eth implicit address.
#[tokio::test]
async fn test_cannot_add_full_access_key() -> anyhow::Result<()> {
    let TestContext { wallet_contract, wallet_sk, .. } = TestContext::new().await?;

    let key = SecretKey::from_random(KeyType::ED25519);
    let action = Action::AddKey {
        public_key_kind: 0,
        public_key: key.public_key().key_data().to_vec(),
        nonce: 0,
        is_full_access: true,
        is_limited_allowance: false,
        allowance: 0,
        receiver_id: String::new(),
        method_names: Vec::new(),
    };
    let signed_transaction = utils::create_signed_transaction(
        0,
        wallet_contract.inner.id(),
        Wei::zero(),
        action,
        &wallet_sk,
    );

    let result = wallet_contract
        .rlp_execute(wallet_contract.inner.id().as_str(), &signed_transaction)
        .await?;

    assert!(!result.success);
    assert_eq!(
        result.error,
        Some(
            Error::User(UserError::UnsupportedAction(UnsupportedAction::AddFullAccessKey))
                .to_string()
        )
    );

    Ok(())
}

// Cases where `tx.data` cannot be parsed into a known
// Action or emulated Ethereum standard.
#[tokio::test]
async fn test_bad_data() -> anyhow::Result<()> {
    let TestContext { wallet_contract, wallet_sk, .. } = TestContext::new().await?;

    let account_id = "aurora";
    let to = Address::new(account_id_to_address(&account_id.parse().unwrap()));
    let transaction = aurora_engine_transactions::eip_2930::Transaction2930 {
        nonce: 0.into(),
        gas_price: 0.into(),
        gas_limit: 0.into(),
        to: Some(to),
        value: Wei::zero(),
        data: hex::decode("deadbeef").unwrap(),
        chain_id: CHAIN_ID,
        access_list: Vec::new(),
    };
    let signed_transaction = crypto::sign_transaction(transaction, &wallet_sk);

    let result = wallet_contract.rlp_execute(account_id, &signed_transaction).await?;

    assert!(!result.success);
    assert_eq!(result.error, Some(Error::User(UserError::UnknownFunctionSelector).to_string()));

    let transaction = aurora_engine_transactions::eip_2930::Transaction2930 {
        nonce: 1.into(),
        gas_price: 0.into(),
        gas_limit: 0.into(),
        to: Some(to),
        value: Wei::zero(),
        data: [
            FUNCTION_CALL_SELECTOR.to_vec(),
            hex::decode("0000000000000000000000000000000000000000000000000000000000000000")
                .unwrap(),
        ]
        .concat(),
        chain_id: CHAIN_ID,
        access_list: Vec::new(),
    };
    let signed_transaction = crypto::sign_transaction(transaction, &wallet_sk);

    let result = wallet_contract.rlp_execute(account_id, &signed_transaction).await?;

    assert!(!result.success);
    assert_eq!(result.error, Some(Error::User(UserError::InvalidAbiEncodedData).to_string()));

    let transaction = aurora_engine_transactions::eip_2930::Transaction2930 {
        nonce: 2.into(),
        gas_price: 0.into(),
        gas_limit: 0.into(),
        to: Some(to),
        value: Wei::zero(),
        data: Vec::new(),
        chain_id: CHAIN_ID,
        access_list: Vec::new(),
    };
    let signed_transaction = crypto::sign_transaction(transaction, &wallet_sk);

    let result = wallet_contract.rlp_execute(account_id, &signed_transaction).await?;

    assert!(!result.success);
    assert_eq!(result.error, Some(Error::User(UserError::InvalidAbiEncodedData).to_string()));

    Ok(())
}

// Test case where the action contains greater than or equal to 1_000_000 yoctoNear directly.
#[tokio::test]
async fn test_excess_yocto() -> anyhow::Result<()> {
    let TestContext { wallet_contract, wallet_sk, .. } = TestContext::new().await?;

    assert_excess_yocto_err(&wallet_contract, &wallet_sk, 0, MAX_YOCTO_NEAR).await?;
    assert_excess_yocto_err(&wallet_contract, &wallet_sk, 1, MAX_YOCTO_NEAR + 1).await?;

    Ok(())
}

async fn assert_excess_yocto_err(
    wallet_contract: &WalletContract,
    wallet_sk: &near_crypto::SecretKey,
    nonce: u64,
    yocto_near: u32,
) -> anyhow::Result<()> {
    let account_id = "aurora";
    let action = Action::Transfer { receiver_id: account_id.into(), yocto_near };
    let signed_transaction = utils::create_signed_transaction(
        nonce,
        &account_id.parse().unwrap(),
        Wei::new_u64(1),
        action,
        wallet_sk,
    );

    let result = wallet_contract.rlp_execute(account_id, &signed_transaction).await?;

    assert!(!result.success);
    assert_eq!(result.error, Some(Error::User(UserError::ExcessYoctoNear).to_string()));

    Ok(())
}
