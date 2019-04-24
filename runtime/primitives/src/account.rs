use crate::crypto::signature::PublicKey;
use crate::hash::CryptoHash;
use crate::types::{AccountId, Balance, Nonce};

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

/// Limited Access key to use owner's account with the fixed public_key.
/// Access Key is stored under the key of owner's `account_id` and the `public_key`.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct AccessKey {
    /// Balance amount on this Access Key. Can be used to pay for the transactions.
    pub amount: Balance,
    /// Owner of the balance of this Access Key. None means the account owner.
    pub balance_owner: Option<AccountId>,
    /// Contract ID that can be called with this Access Key. None means the account owner.
    /// Access key only allows to call given contract_id.
    pub contract_id: Option<AccountId>,
    /// The only method name that can be called with this Access Key. None means any method name.
    pub method_name: Option<Vec<u8>>,
}
