use std::collections::{HashMap, HashSet};

use crate::challenge::PartialState;
use crate::sharding::{ChunkHash, ReceiptProof, ShardChunkHeader, ShardChunkHeaderV3};
use crate::transaction::SignedTransaction;
use crate::types::EpochId;
use crate::validator_signer::{EmptyValidatorSigner, ValidatorSigner};
use borsh::{BorshDeserialize, BorshSerialize};
use bytes::BufMut;
use near_crypto::{PublicKey, Signature};
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::{AccountId, Balance, BlockHeight, ShardId};

/// Request contains a list of AccountIds for which we want to get the contract code.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ContractCodeRequest {
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