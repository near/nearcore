use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::types::AccountId;

/// Request to retrieve the contract code for a list of AccountIds.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ContractCodeRequest {
    /// AccountId of the requester. Used to send the response back to the originating node.
    pub requester: AccountId,
    /// List of AccountIds for which the contract code is requested.
    pub account_ids: Vec<AccountId>,
}

/// Represents a response to the contract code request for a given AccountId.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ContractCodeResponseItem {
    /// AccountId that the contract belongs to.
    pub account_id: AccountId,
    /// Uncompiled contract code.
    pub code: Vec<u8>,
}

/// Contains contract code responses for a list of AccountIds.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ContractCodeResponse {
    /// List of responses, one per AccountId.
    pub items: Vec<ContractCodeResponseItem>,
}
