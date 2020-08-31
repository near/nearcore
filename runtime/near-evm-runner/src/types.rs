use borsh::{BorshDeserialize, BorshSerialize};

use near_primitives::types::Balance;

pub type RawAddress = [u8; 20];
pub type RawHash = [u8; 32];

#[derive(BorshSerialize, BorshDeserialize)]
pub struct GetCodeArgs {
    pub address: RawAddress,
}

#[derive(BorshSerialize, BorshDeserialize)]
pub struct GetStorageAtArgs {
    pub address: RawAddress,
    pub key: RawHash,
}

#[derive(BorshSerialize, BorshDeserialize)]
pub struct WithdrawArgs {
    pub account_id: String,
    pub amount: Balance,
}

#[derive(BorshSerialize, BorshDeserialize)]
pub struct TransferArgs {
    pub account_id: RawAddress,
    pub amount: Balance,
}
