use primitives::hash::bs58_format;
use primitives::types::{AccountId, TransactionBody, Balance};

#[derive(Serialize, Deserialize)]
pub struct SendMoneyRequest {
    pub nonce: u64,
    #[serde(with = "bs58_format")]
    pub sender_account_id: AccountId,
    #[serde(with = "bs58_format")]
    pub receiver_account_id: AccountId,
    pub amount: Balance,
}

#[derive(Serialize, Deserialize)]
pub struct StakeRequest {
    pub nonce: u64,
    #[serde(with = "bs58_format")]
    pub staker_account_id: AccountId,
    pub amount: Balance,
}

#[derive(Serialize, Deserialize)]
pub struct DeployContractRequest {
    pub nonce: u64,
    #[serde(with = "bs58_format")]
    pub contract_account_id: AccountId,
    pub wasm_byte_array: Vec<u8>,
    pub public_key: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
pub struct CreateAccountRequest {
    pub nonce: u64,
    #[serde(with = "bs58_format")]
    pub sender: AccountId,
    #[serde(with = "bs58_format")]
    pub new_account_id: AccountId,
    pub amount: u64,
    pub public_key: Vec<u8>
}

#[derive(Serialize, Deserialize)]
pub struct SwapKeyRequest {
    pub nonce: u64,
    #[serde(with = "bs58_format")]
    pub account: AccountId,
    pub cur_key: Vec<u8>,
    pub new_key: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
pub struct ScheduleFunctionCallRequest {
    pub nonce: u64,
    #[serde(with = "bs58_format")]
    pub originator_account_id: AccountId,
    #[serde(with = "bs58_format")]
    pub contract_account_id: AccountId,
    pub method_name: String,
    pub args: Vec<Vec<u8>>,
}

#[derive(Serialize, Deserialize)]
pub struct ViewAccountRequest {
    #[serde(with = "bs58_format")]
    pub account_id: AccountId,
}

#[derive(Serialize, Deserialize)]
pub struct ViewAccountResponse {
    #[serde(with = "bs58_format")]
    pub account_id: AccountId,
    pub amount: Balance,
    pub nonce: u64,
}

#[derive(Serialize, Deserialize)]
pub struct CallViewFunctionRequest {
    #[serde(with = "bs58_format")]
    pub contract_account_id: AccountId,
    pub method_name: String,
    pub args: Vec<Vec<u8>>,
}

#[derive(Serialize, Deserialize)]
pub struct CallViewFunctionResponse {
    #[serde(with = "bs58_format")]
    pub account_id: AccountId,
    pub amount: Balance,
    pub nonce: u64,
    pub result: Vec<u8>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct PreparedTransactionBodyResponse {
    pub body: TransactionBody,
}
