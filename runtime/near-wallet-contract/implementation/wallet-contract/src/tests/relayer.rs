use crate::{
    internal::{account_id_to_address, CHAIN_ID},
    tests::{
        utils::{
            self, codec, crypto, nep141,
            test_context::{TestContext, WalletContract},
        },
        RLP_EXECUTE,
    },
    types::{Action, ExecuteResponse},
};
use aurora_engine_types::types::{Address, Wei};
use near_workspaces::{
    network::Sandbox,
    types::{AccessKeyPermission, SecretKey},
    Contract, Worker,
};

// A relayer can use its own Near account to send a transaction containing data
// signed by the user which adds a FunctionCall access key to the Wallet
// Contract account. This allows the relayer to send transactions on the user's
// behalf while the user covers the gas costs.
#[tokio::test]
async fn test_register_relayer() -> anyhow::Result<()> {
    let TestContext { worker, mut wallet_contract, wallet_sk, .. } = TestContext::new().await?;

    let relayer_pk = wallet_contract.register_relayer(&worker).await?;
    let key = wallet_contract.inner.as_account().view_access_key(&relayer_pk).await?;
    match &key.permission {
        AccessKeyPermission::FunctionCall(access) => {
            assert_eq!(access.allowance, None);
            assert_eq!(access.receiver_id.as_str(), wallet_contract.inner.id().as_str());
            assert_eq!(&access.method_names, &[RLP_EXECUTE]);
        }
        _ => panic!("Unexpected full access key"),
    }

    // Should be able to submit transactions using the new key
    utils::deploy_and_call_hello(&worker, &wallet_contract, &wallet_sk, 1).await?;

    // If the relayer is dishonest then its key is revoked.
    // In this case the relayer will try to repeat a nonce value.
    let result = utils::deploy_and_call_hello(&worker, &wallet_contract, &wallet_sk, 1).await;
    let error_message = format!("{:?}", result.unwrap_err());
    assert!(error_message.contains("faulty relayer"));

    assert_revoked_key(&wallet_contract.inner, &relayer_pk).await;

    Ok(())
}

// If the relayer sends garbage data to the Wallet Contract then it is banned.
#[tokio::test]
async fn test_relayer_invalid_tx_data() -> anyhow::Result<()> {
    let TestContext { worker, mut wallet_contract, .. } = TestContext::new().await?;

    async fn new_relayer(
        worker: &Worker<Sandbox>,
        wc: &mut WalletContract,
    ) -> anyhow::Result<SecretKey> {
        wc.register_relayer(worker).await?;
        let sk = wc.inner.as_account().secret_key().clone();
        Ok(sk)
    }

    async fn rlp_execute(
        relayer_key: &SecretKey,
        wc: &WalletContract,
        tx_bytes: &[u8],
    ) -> anyhow::Result<()> {
        let relayer_pk = relayer_key.public_key();

        let result: ExecuteResponse = wc
            .inner
            .call(RLP_EXECUTE)
            .args_json(serde_json::json!({
                "target": "some.account.near",
                "tx_bytes_b64": codec::encode_b64(tx_bytes)
            }))
            .max_gas()
            .transact()
            .await?
            .into_result()?
            .json()?;

        assert!(!result.success);
        assert_eq!(result.error.as_deref(), Some("Error: faulty relayer"));

        assert_revoked_key(&wc.inner, &relayer_pk).await;

        Ok(())
    }

    let inputs: [&[u8]; 2] = [b"random_garbage_data", &[]];
    let relayer_keys = {
        // Need to generate all the relayer keys first because they are
        // going to get banned as we run the different inputs in the later loop.
        let mut tmp = Vec::new();
        for _ in 0..(inputs.len()) {
            tmp.push(new_relayer(&worker, &mut wallet_contract).await?);
        }
        tmp
    };

    for (input, sk) in inputs.into_iter().zip(relayer_keys) {
        wallet_contract.inner.as_account_mut().set_secret_key(sk.clone());
        rlp_execute(&sk, &wallet_contract, input).await?;
    }

    Ok(())
}

// Tests case where relayer sends a transaction signed by the wrong account.
#[tokio::test]
async fn test_relayer_invalid_sender() -> anyhow::Result<()> {
    let TestContext { worker, mut wallet_contract, wallet_contract_bytes, .. } =
        TestContext::new().await?;

    let wrong_wallet_sk = TestContext::deploy_wallet(&worker, &wallet_contract_bytes).await?.0.sk;
    let relayer_pk = wallet_contract.register_relayer(&worker).await?;

    let target = "aurora";
    let action = Action::Transfer { receiver_id: target.into(), yocto_near: 0 };
    // Transaction signed by wrong secret key
    let signed_transaction = utils::create_signed_transaction(
        0,
        &target.parse().unwrap(),
        Wei::zero(),
        action,
        &wrong_wallet_sk,
    );

    let result = wallet_contract.rlp_execute(target, &signed_transaction).await?;

    assert!(!result.success);
    assert_eq!(result.error.as_deref(), Some("Error: faulty relayer"));

    assert_revoked_key(&wallet_contract.inner, &relayer_pk).await;

    Ok(())
}

// Tests the case where the relayer sets the `target` to a named account which does not
// hash to the `to` field of the user's signed Ethereum transaction.
#[tokio::test]
async fn test_relayer_invalid_target() -> anyhow::Result<()> {
    let TestContext { worker, mut wallet_contract, wallet_sk, .. } = TestContext::new().await?;

    let relayer_pk = wallet_contract.register_relayer(&worker).await?;

    let real_target = "aurora";
    let action = Action::Transfer { receiver_id: real_target.into(), yocto_near: 0 };
    let signed_transaction = utils::create_signed_transaction(
        0,
        &real_target.parse().unwrap(),
        Wei::zero(),
        action,
        &wallet_sk,
    );

    let result =
        wallet_contract.rlp_execute(&format!("other.{real_target}"), &signed_transaction).await?;

    assert!(!result.success);
    assert_eq!(result.error.as_deref(), Some("Error: faulty relayer"));

    assert_revoked_key(&wallet_contract.inner, &relayer_pk).await;

    Ok(())
}

// Tests the situation where the relayer sets `target == tx.to` when it should have
// looked up the named account corresponding to `tx.to`. In this case the relayer
// should be banned for being lazy.
#[tokio::test]
async fn test_relayer_invalid_address_target() -> anyhow::Result<()> {
    let TestContext {
        worker,
        mut wallet_contract,
        wallet_sk,
        wallet_address,
        address_registrar,
        ..
    } = TestContext::new().await?;

    // Deploy a NEP-141 contract and register its address.
    // Registering should prevent a lazy relayer from setting the target incorrectly.
    let token_contract = nep141::Nep141::deploy(&worker).await?;
    let register_output: Option<String> = address_registrar
        .call("register")
        .args_json(serde_json::json!({
            "account_id": token_contract.contract.id().as_str()
        }))
        .max_gas()
        .transact()
        .await?
        .json()?;
    let token_address: [u8; 20] =
        hex::decode(register_output.as_ref().unwrap().strip_prefix("0x").unwrap())?
            .try_into()
            .unwrap();
    assert_eq!(
        token_address,
        account_id_to_address(&token_contract.contract.id().as_str().parse().unwrap(),).0
    );

    // Set up a relayer with control to send transactions via the Wallet Contract account.
    let relayer_pk = wallet_contract.register_relayer(&worker).await?;

    // The user submits a transaction to interact with the NEP-141 contract.
    let transaction = aurora_engine_transactions::eip_2930::Transaction2930 {
        nonce: 0.into(),
        gas_price: 0.into(),
        gas_limit: 0.into(),
        to: Some(Address::from_array(token_address)),
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

    // Relayer fails to set `target` correctly
    let result =
        wallet_contract.rlp_execute(register_output.unwrap().as_str(), &signed_transaction).await?;

    assert!(!result.success);
    assert_eq!(result.error.as_deref(), Some("Error: faulty relayer"));

    assert_revoked_key(&wallet_contract.inner, &relayer_pk).await;

    Ok(())
}

// A relayer sending a transaction signed with the wrong chain id is a ban-worthy offense.
#[tokio::test]
async fn test_relayer_wrong_chain_id() -> anyhow::Result<()> {
    let TestContext { worker, mut wallet_contract, wallet_sk, wallet_address, .. } =
        TestContext::new().await?;

    let relayer_pk = wallet_contract.register_relayer(&worker).await?;

    let transaction = aurora_engine_transactions::eip_2930::Transaction2930 {
        nonce: 0.into(),
        gas_price: 0.into(),
        gas_limit: 0.into(),
        to: Some(Address::new(wallet_address)),
        value: Wei::zero(),
        data: [
            crate::eth_emulation::ERC20_BALANCE_OF_SELECTOR.to_vec(),
            ethabi::encode(&[ethabi::Token::Address(wallet_address)]),
        ]
        .concat(),
        chain_id: CHAIN_ID + 1,
        access_list: Vec::new(),
    };
    let signed_transaction = crypto::sign_transaction(transaction, &wallet_sk);

    let result = wallet_contract
        .rlp_execute(wallet_contract.inner.id().as_str(), &signed_transaction)
        .await?;

    assert!(!result.success);
    assert_eq!(result.error.as_deref(), Some("Error: faulty relayer"));

    assert_revoked_key(&wallet_contract.inner, &relayer_pk).await;

    Ok(())
}

async fn assert_revoked_key(
    wallet_contract: &Contract,
    relayer_pk: &near_workspaces::types::PublicKey,
) {
    let key_query = wallet_contract.as_account().view_access_key(relayer_pk).await;

    let error_message = format!("{:?}", key_query.unwrap_err());
    assert!(error_message.contains("UnknownAccessKey"));
}
