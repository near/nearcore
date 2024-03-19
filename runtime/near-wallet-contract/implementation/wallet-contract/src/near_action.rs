//! Definition of `Action` for Near protocol.
//! Unfortunately we cannot use `near-primitives` directly in the contract
//! because it uses dependencies that do not compile to Wasm (at least
//! not without some extra feature flags that `near-primitives` currently
//! does not include).

use near_sdk::{AccountId, Gas, NearToken, PublicKey};

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub enum Action {
    CreateAccount(CreateAccountAction),
    DeployContract(DeployContractAction),
    FunctionCall(FunctionCallAction),
    Transfer(TransferAction),
    Stake(StakeAction),
    AddKey(AddKeyAction),
    DeleteKey(DeleteKeyAction),
    DeleteAccount(DeleteAccountAction),
    Delegate(SignedDelegateAction),
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct CreateAccountAction {}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct DeployContractAction {
    pub code: Vec<u8>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct FunctionCallAction {
    pub method_name: String,
    pub args: Vec<u8>,
    pub gas: Gas,
    pub deposit: NearToken,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct TransferAction {
    pub deposit: NearToken,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct StakeAction {
    pub stake: NearToken,
    pub public_key: PublicKey,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct AddKeyAction {
    pub public_key: PublicKey,
    pub access_key: AccessKey,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct AccessKey {
    pub nonce: u64,
    pub permission: AccessKeyPermission,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub enum AccessKeyPermission {
    FullAccess,
    FunctionCall(FunctionCallPermission),
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct FunctionCallPermission {
    pub allowance: Option<NearToken>,
    pub receiver_id: AccountId,
    pub method_names: Vec<String>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct DeleteKeyAction {
    pub public_key: PublicKey,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct DeleteAccountAction {
    pub beneficiary_id: AccountId,
}

/// This is just a placeholder for now since Delegate actions will
/// not be supported by the Wallet Contract in V1.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct SignedDelegateAction;
