use std::fmt;

use crate::crypto::signature::PublicKey;
use crate::hash::CryptoHash;
use crate::logging;
use crate::serialize::{u128_dec_format, vec_base_format};
use crate::types::{AccountId, Balance, BlockIndex, Nonce, StorageUsage};

/// Per account information stored in the state.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct Account {
    pub public_keys: Vec<PublicKey>,
    pub nonce: Nonce,
    /// The sum of `amount` and `staked` is the total value of the account.
    pub amount: Balance,
    /// The amount staked by given account.
    pub staked: Balance,
    pub code_hash: CryptoHash,
    /// Storage used by the given account.
    pub storage_usage: StorageUsage,
    /// Last block index at which the storage was paid for.
    pub storage_paid_at: BlockIndex,
}

impl Account {
    pub fn new(
        public_keys: Vec<PublicKey>,
        amount: Balance,
        code_hash: CryptoHash,
        storage_paid_at: BlockIndex,
    ) -> Self {
        Account {
            public_keys,
            nonce: 0,
            amount,
            staked: 0,
            code_hash,
            storage_usage: 0,
            storage_paid_at,
        }
    }

    /// Try debiting the balance by the given amount.
    pub fn checked_sub(&mut self, amount: Balance) -> Result<(), String> {
        self.amount = self.amount.checked_sub(amount).ok_or_else(|| {
            format!(
                "Sender does not have enough balance {} for operation costing {}",
                self.amount, amount
            )
        })?;
        Ok(())
    }
}

/// Limited Access key to use owner's account with the fixed public_key.
/// Access Key is stored under the key of owner's `account_id` and the `public_key`.
#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
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

impl fmt::Debug for AccessKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("AccessKey")
            .field("amount", &self.amount)
            .field("balance_owner", &self.balance_owner)
            .field("contract_id", &self.contract_id)
            .field("method_name", &self.method_name.as_ref().map(|v| logging::pretty_utf8(&v)))
            .finish()
    }
}
