use borsh::{BorshDeserialize, BorshSerialize};

use near_vm_errors::VMLogicError;
use near_vm_logic::types::{AccountId, Balance};

pub type RawAddress = [u8; 20];
pub type RawHash = [u8; 32];

pub type Result<T> = std::result::Result<T, VMLogicError>;

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
    pub account_id: AccountId,
    pub amount: Balance,
}

#[derive(BorshSerialize, BorshDeserialize)]
pub struct TransferArgs {
    pub account_id: RawAddress,
    pub amount: Balance,
}
