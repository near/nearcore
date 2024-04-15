use crate::{
    internal::{account_id_to_address, CHAIN_ID},
    tests::utils::test_context::WalletContract,
    types::Action,
};
use aurora_engine_transactions::EthTransactionKind;
use aurora_engine_types::types::{Address, Wei};
use near_crypto::SecretKey;
use near_workspaces::{network::Sandbox, AccountId, Worker};

pub mod codec;
pub mod crypto;
pub mod nep141;
pub mod test_context;

pub async fn deploy_and_call_hello(
    worker: &Worker<Sandbox>,
    wallet_contract: &WalletContract,
    wallet_sk: &SecretKey,
    nonce: u64,
) -> anyhow::Result<()> {
    let hello_bytes = tokio::fs::read("src/tests/res/hello.wasm").await?;
    let hello_contract = worker.dev_deploy(&hello_bytes).await?;

    let action = Action::FunctionCall {
        receiver_id: hello_contract.id().to_string(),
        method_name: "greet".into(),
        args: br#"{"name": "Aurora"}"#.to_vec(),
        gas: 5_000_000_000_000,
        yocto_near: 0,
    };
    let signed_transaction =
        create_signed_transaction(nonce, hello_contract.id(), Wei::zero(), action, wallet_sk);

    let result =
        wallet_contract.rlp_execute(hello_contract.id().as_str(), &signed_transaction).await?;

    if result.success_value.as_deref() != Some(br#""Hello, Aurora!""#.as_slice()) {
        anyhow::bail!("Call to hello contract failed: {:?}", result.error);
    }

    Ok(())
}

pub fn create_signed_transaction(
    nonce: u64,
    target: &AccountId,
    value: Wei,
    action: Action,
    wallet_sk: &SecretKey,
) -> EthTransactionKind {
    let transaction = aurora_engine_transactions::eip_2930::Transaction2930 {
        nonce: nonce.into(),
        gas_price: 0.into(),
        gas_limit: 0.into(),
        to: Some(Address::new(account_id_to_address(&target.as_str().parse().unwrap()))),
        value,
        data: codec::abi_encode(action),
        chain_id: CHAIN_ID,
        access_list: Vec::new(),
    };
    crypto::sign_transaction(transaction, wallet_sk)
}
