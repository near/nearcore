use std::convert::{TryFrom, TryInto};

use protobuf::{RepeatedField, SingularPtrField};

use near_protos::access_key as access_key_proto;
use near_protos::account as account_proto;

use crate::hash::CryptoHash;
use crate::serialize::{option_u128_dec_format, u128_dec_format};
use crate::types::{AccountId, Balance, BlockIndex, Nonce, StorageUsage};
use crate::utils::proto_to_type;

/// Per account information stored in the state.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct Account {
    // amount + staked is the total value of the account
    #[serde(with = "u128_dec_format")]
    pub amount: Balance,
    #[serde(with = "u128_dec_format")]
    pub staked: Balance,
    pub code_hash: CryptoHash,
    /// Storage used by the given account.
    pub storage_usage: StorageUsage,
    /// Last block index at which the storage was paid for.
    pub storage_paid_at: BlockIndex,
}

impl Account {
    pub fn new(amount: Balance, code_hash: CryptoHash, storage_paid_at: BlockIndex) -> Self {
        Account { amount, staked: 0, code_hash, storage_usage: 0, storage_paid_at }
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

impl TryFrom<account_proto::Account> for Account {
    type Error = Box<dyn std::error::Error>;

    fn try_from(account: account_proto::Account) -> Result<Self, Self::Error> {
        Ok(Account {
            amount: proto_to_type(account.amount)?,
            staked: proto_to_type(account.staked)?,
            code_hash: account.code_hash.try_into()?,
            storage_usage: account.storage_usage,
            storage_paid_at: account.storage_paid_at,
        })
    }
}

impl From<Account> for account_proto::Account {
    fn from(account: Account) -> Self {
        account_proto::Account {
            amount: SingularPtrField::some(account.amount.into()),
            staked: SingularPtrField::some(account.staked.into()),
            code_hash: account.code_hash.into(),
            storage_usage: account.storage_usage,
            storage_paid_at: account.storage_paid_at,

            cached_size: Default::default(),
            unknown_fields: Default::default(),
        }
    }
}

/// Access key provides limited access to an account. Each access key belongs to some account and
/// is identified by a unique (within the account) public key. One account may have large number of
/// access keys. Access keys allow to act on behalf of the account by restricting transactions
/// that can be issued.
/// `account_id,public_key` is a key in the state
#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone, Debug)]
pub struct AccessKey {
    /// The nonce for this access key.
    /// NOTE: In some cases the access key needs to be recreated. If the new access key reuses the
    /// same public key, the nonce of the new access key should be equal to the nonce of the old
    /// access key. It's required to avoid replaying old transactions again.
    pub nonce: Nonce,

    /// Defines permissions for this access key.
    pub permission: AccessKeyPermission,
}

impl AccessKey {
    pub fn full_access() -> Self {
        Self { nonce: 0, permission: AccessKeyPermission::FullAccess }
    }
}

/// Defines permissions for AccessKey
#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone, Debug)]
pub enum AccessKeyPermission {
    FunctionCall(FunctionCallPermission),

    /// Grants full access to the account.
    /// NOTE: It's used to replace account-level public keys.
    FullAccess,
}

impl TryFrom<access_key_proto::AccessKey> for AccessKey {
    type Error = Box<dyn std::error::Error>;

    fn try_from(proto: access_key_proto::AccessKey) -> Result<Self, Self::Error> {
        Ok(AccessKey {
            nonce: proto.nonce,
            permission: match proto.permission {
                Some(access_key_proto::AccessKey_oneof_permission::full_access(_)) => {
                    AccessKeyPermission::FullAccess
                }
                Some(access_key_proto::AccessKey_oneof_permission::function_call(fc)) => {
                    AccessKeyPermission::FunctionCall(FunctionCallPermission::try_from(fc)?)
                }
                None => return Err("No such permission".into()),
            },
        })
    }
}

impl From<AccessKey> for access_key_proto::AccessKey {
    fn from(access_key: AccessKey) -> Self {
        let permission = match access_key.permission {
            AccessKeyPermission::FullAccess => {
                access_key_proto::AccessKey_oneof_permission::full_access(
                    access_key_proto::FullAccessPermission::default(),
                )
            }
            AccessKeyPermission::FunctionCall(fc) => {
                access_key_proto::AccessKey_oneof_permission::function_call(fc.into())
            }
        };
        access_key_proto::AccessKey {
            nonce: access_key.nonce,
            permission: Some(permission),

            cached_size: Default::default(),
            unknown_fields: Default::default(),
        }
    }
}

/// Grants limited permission to issue transactions with a single function call action.
/// Those function calls can't have attached balance.
/// The permission can limit the allowed balance to be spent on the prepaid gas.
/// It also restrict the account ID of the receiver for this function call.
/// It also can restrict the method name for the allowed function calls.
#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone, Debug)]
pub struct FunctionCallPermission {
    /// Allowance is a balance limit to use by this access key to pay for function call gas and
    /// transaction fees. When this access key is used, both account balance and the allowance is
    /// decreased by the same value.
    /// `None` means unlimited allowance.
    /// NOTE: To change or increase the allowance, the old access key needs to be deleted and a new
    /// access key should be created.
    #[serde(with = "option_u128_dec_format")]
    pub allowance: Option<Balance>,

    /// The access key only allows transactions with the given receiver's account id.
    pub receiver_id: AccountId,

    /// A list of method names that can be used. The access key only allows transactions with the
    /// function call of one of the given method names.
    /// Empty list means any method name can be used.
    pub method_names: Vec<String>,
}

impl TryFrom<access_key_proto::FunctionCallPermission> for FunctionCallPermission {
    type Error = Box<dyn std::error::Error>;

    fn try_from(proto: access_key_proto::FunctionCallPermission) -> Result<Self, Self::Error> {
        Ok(FunctionCallPermission {
            allowance: proto.allowance.into_option().map(|a| a.try_into()).transpose()?,
            receiver_id: proto.receiver_id,
            method_names: proto.method_names.to_vec(),
        })
    }
}

impl From<FunctionCallPermission> for access_key_proto::FunctionCallPermission {
    fn from(f: FunctionCallPermission) -> Self {
        access_key_proto::FunctionCallPermission {
            allowance: SingularPtrField::from_option(f.allowance.map(|a| a.into())),
            receiver_id: f.receiver_id,
            method_names: RepeatedField::from_vec(f.method_names),

            cached_size: Default::default(),
            unknown_fields: Default::default(),
        }
    }
}
