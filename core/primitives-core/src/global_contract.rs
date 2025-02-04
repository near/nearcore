use crate::{hash::CryptoHash, types::AccountId};
use borsh::{BorshDeserialize, BorshSerialize};
use near_schema_checker_lib::ProtocolSchema;
use serde_with::serde_as;

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
pub enum GlobalContractIdentifier {
    CodeHash(CryptoHash),
    AccountId(AccountId),
}
