use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::types::AccountId;

/// Request contains a list of AccountIds for which we want to get the contract code.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ContractCodeRequest {
    pub requester: AccountId,
    pub account_ids: Vec<AccountId>,
}

/// A list of this struct is contained in the ContractCodeResponse.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ContractCodeResponseItem {
    pub account_id: AccountId,
    pub code: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ContractCodeResponse {
    pub items: Vec<ContractCodeResponseItem>,
}
