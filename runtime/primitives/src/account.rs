use crate::crypto::signature::PublicKey;
use crate::hash::CryptoHash;
use crate::types::{Balance, Nonce};

/// Per account information stored in the state.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct Account {
    pub public_keys: Vec<PublicKey>,
    pub nonce: Nonce,
    // amount + staked is the total value of the account
    pub amount: Balance,
    pub staked: Balance,
    pub code_hash: CryptoHash,
}

impl Account {
    pub fn new(public_keys: Vec<PublicKey>, amount: Balance, code_hash: CryptoHash) -> Self {
        Account { public_keys, nonce: 0, amount, staked: 0, code_hash }
    }
}
