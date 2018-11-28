use primitives::types::AccountId;

#[derive(Serialize, Deserialize)]
pub struct SendMoneyRequest {
    pub nonce: u64,
    pub sender_account_id: AccountId,
    pub receiver_account_id: AccountId,
    pub amount: u64,
}

#[derive(Serialize, Deserialize)]
pub struct DeployContractRequest {
    pub nonce: u64,
    pub sender_account_id: AccountId,
    pub contract_account_id: AccountId,
    pub wasm_byte_array: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
pub struct CallMethodRequest {
    pub nonce: u64,
    pub sender_account_id: AccountId,
    pub contract_account_id: AccountId,
    pub method_name: String,
    pub args: Vec<Vec<u8>>,
}

#[derive(Serialize, Deserialize)]
pub struct ViewAccountRequest {
    pub account_id: AccountId,
}
