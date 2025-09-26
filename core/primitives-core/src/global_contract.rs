use crate::account::AccountContract;
use crate::hash::CryptoHash;
use borsh::{BorshDeserialize, BorshSerialize};
use near_account_id::AccountId;
use near_schema_checker_lib::ProtocolSchema;
use serde_with::serde_as;
use std::fmt;

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

/// Extract [`GlobalContractIdentifier`] out of [`AccountContract`] if it represents a global
/// contract.
///
/// If conversion is not possible, the conversion error can be inspected to obtain information
/// about the local error.
impl TryFrom<AccountContract> for GlobalContractIdentifier {
    type Error = ContractIsLocalError;
    fn try_from(value: AccountContract) -> Result<Self, Self::Error> {
        match value {
            AccountContract::None => Err(ContractIsLocalError::NotDeployed),
            AccountContract::Local(h) => Err(ContractIsLocalError::Deployed(h)),
            AccountContract::Global(h) => Ok(GlobalContractIdentifier::CodeHash(h)),
            AccountContract::GlobalByAccount(a) => Ok(GlobalContractIdentifier::AccountId(a)),
        }
    }
}

#[derive(Debug)]
pub enum ContractIsLocalError {
    NotDeployed,
    Deployed(CryptoHash),
}

impl std::error::Error for ContractIsLocalError {}

impl fmt::Display for ContractIsLocalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ContractIsLocalError::NotDeployed => "contract is not deployed",
            ContractIsLocalError::Deployed(_) => "a locally deployed contract is deployed",
        })
    }
}
