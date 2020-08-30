use borsh::BorshDeserialize;

use near_primitives::types::Balance;

pub type RawAddress = [u8; 20];
pub type RawHash = [u8; 32];

#[derive(BorshDeserialize)]
pub struct GetCodeArgs {
    pub address: RawAddress,
}

#[derive(BorshDeserialize)]
pub struct GetStorageAtArgs {
    pub address: RawAddress,
    pub key: RawHash,
}

#[derive(BorshDeserialize)]
pub struct WithdrawNearArgs {
    pub account_id: String,
    pub amount: Balance,
}
