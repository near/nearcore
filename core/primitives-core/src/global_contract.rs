use borsh::{BorshDeserialize, BorshSerialize};
use near_account_id::AccountId;
use near_schema_checker_lib::ProtocolSchema;
use serde_with::serde_as;

use crate::hash::CryptoHash;

#[serde_as]
#[derive(
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    Hash,
    PartialEq,
    Eq,
    Clone,
    ProtocolSchema,
    Debug,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum GlobalContractIdentifier {
    CodeHash(CryptoHash) = 0,
    AccountId(AccountId) = 1,
}

impl GlobalContractIdentifier {
    pub fn len(&self) -> usize {
        match self {
            GlobalContractIdentifier::CodeHash(_) => 32,
            GlobalContractIdentifier::AccountId(account_id) => account_id.len(),
        }
    }
}
