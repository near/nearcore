use std::convert::{TryFrom, TryInto};
use std::fmt;

use protobuf::well_known_types::BytesValue;
use protobuf::well_known_types::StringValue;
use protobuf::SingularPtrField;

use near_protos::access_key as access_key_proto;

use crate::crypto::signature::PublicKey;
use crate::hash::CryptoHash;
use crate::logging;
use crate::types::{AccountId, Balance, BlockIndex, Nonce, StorageUsage};

/// Per account information stored in the state.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct Account {
    pub public_keys: Vec<PublicKey>,
    pub nonce: Nonce,
    // amount + staked is the total value of the account
    pub amount: Balance,
    pub staked: Balance,
    pub code_hash: CryptoHash,
    /// Storage used by the given account.
    pub storage_usage: StorageUsage,
    /// Last block index at which the storage was paid for.
    pub storage_paid_at: BlockIndex,
}

impl Account {
    pub fn new(public_keys: Vec<PublicKey>, amount: Balance, code_hash: CryptoHash) -> Self {
        Account {
            public_keys,
            nonce: 0,
            amount,
            staked: 0,
            code_hash,
            storage_usage: 0,
            storage_paid_at: 0,
        }
    }

    /// Try debiting the balance by the given amount.
    pub fn checked_sub(&mut self, amount: Balance) -> Result<(), String> {
        self.amount = self
            .amount
            .checked_sub(amount)
            .ok_or_else(|| {
                format!(
                    "Sender does not have enough balance {} for operation costing {}",
                    self.amount, amount
                )
            })?
            .into();
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

impl TryFrom<access_key_proto::AccessKey> for AccessKey {
    type Error = Box<dyn std::error::Error>;

    fn try_from(access_key: access_key_proto::AccessKey) -> Result<Self, Self::Error> {
        Ok(AccessKey {
            amount: access_key.amount.unwrap_or_default().try_into()?,
            balance_owner: access_key.balance_owner.into_option().map(|s| s.value),
            contract_id: access_key.contract_id.into_option().map(|s| s.value),
            method_name: access_key.method_name.into_option().map(|s| s.value),
        })
    }
}

impl From<AccessKey> for access_key_proto::AccessKey {
    fn from(access_key: AccessKey) -> access_key_proto::AccessKey {
        access_key_proto::AccessKey {
            amount: SingularPtrField::some(access_key.amount.into()),
            balance_owner: SingularPtrField::from_option(access_key.balance_owner.map(|v| {
                let mut res = StringValue::new();
                res.set_value(v);
                res
            })),
            contract_id: SingularPtrField::from_option(access_key.contract_id.map(|v| {
                let mut res = StringValue::new();
                res.set_value(v);
                res
            })),
            method_name: SingularPtrField::from_option(access_key.method_name.map(|v| {
                let mut res = BytesValue::new();
                res.set_value(v);
                res
            })),
            ..Default::default()
        }
    }
}
