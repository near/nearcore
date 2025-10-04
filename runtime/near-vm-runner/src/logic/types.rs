use near_primitives_core::hash::CryptoHash;
pub use near_primitives_core::types::*;

pub type PublicKey = Vec<u8>;
pub type PromiseIndex = u64;
pub type ReceiptIndex = u64;
pub type IteratorIndex = u64;
pub type ActionIndex = u64;

#[derive(Debug, PartialEq, Clone)]
pub enum ReturnData {
    /// Method returned some value or data.
    Value(Vec<u8>),

    /// The return value of the method should be taken from the return value of another method
    /// identified through receipt index.
    ReceiptIndex(ReceiptIndex),

    /// Method hasn't returned any data or promise.
    None,
}

impl ReturnData {
    /// Function to extract value from ReturnData.
    pub fn as_value(self) -> Option<Vec<u8>> {
        match self {
            ReturnData::Value(value) => Some(value),
            _ => None,
        }
    }
}

/// When there is a callback attached to one or more contract calls the execution results of these
/// calls are available to the contract invoked through the callback.
#[derive(Debug, PartialEq)]
pub enum PromiseResult {
    /// Current version of the protocol never returns `PromiseResult::NotReady`.
    NotReady,
    Successful(Vec<u8>),
    Failed,
}

#[derive(Debug, Clone, serde::Serialize)]
pub enum GlobalContractDeployMode {
    CodeHash,
    AccountId,
}

#[derive(Debug, Clone, serde::Serialize)]
pub enum GlobalContractIdentifier {
    CodeHash(CryptoHash),
    AccountId(AccountId),
}

impl GlobalContractIdentifier {
    /// Corresponds to `near_primitives::action::GlobalContractIdentifier::len` impl
    pub fn len(&self) -> usize {
        match &self {
            Self::CodeHash(_) => 32,
            Self::AccountId(account_id) => account_id.len(),
        }
    }
}

impl From<GlobalContractIdentifier>
    for near_primitives_core::global_contract::GlobalContractIdentifier
{
    fn from(other: GlobalContractIdentifier) -> Self {
        match other {
            GlobalContractIdentifier::CodeHash(crypto_hash) => Self::CodeHash(crypto_hash),
            GlobalContractIdentifier::AccountId(account_id) => Self::AccountId(account_id),
        }
    }
}
