//! Partial definition of `Action` for Near protocol.
//! Unfortunately we cannot use `near-primitives` directly in the contract
//! because it uses dependencies that do not compile to Wasm (at least
//! not without some extra feature flags that `near-primitives` currently
//! does not include).
//! Some variants of `near_primitives::Action` are intentionally left out
//! because they are not possible to do with the wallet contract
//! (e.g. `DeleteAccount`).

use near_sdk::{AccountId, Gas, NearToken, PublicKey};

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub enum Action {
    FunctionCall(FunctionCallAction),
    Transfer(TransferAction),
    AddKey(AddKeyAction),
    DeleteKey(DeleteKeyAction),
}

impl Action {
    pub fn gas(&self) -> Gas {
        match self {
            Self::FunctionCall(fn_call) => fn_call.gas,
            // 2 Tgas is sufficient for any non-function call action
            Self::Transfer(_) | Self::AddKey(_) | Self::DeleteKey(_) => Gas::from_tgas(2),
        }
    }
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
