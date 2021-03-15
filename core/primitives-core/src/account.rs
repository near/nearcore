use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

use crate::hash::CryptoHash;
use crate::serialize::{option_u128_dec_format, u128_dec_format_compatible};
use crate::types::{AccountId, Balance, Nonce, StorageUsage};

/// Per account information stored in the state.
#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct Account {
    /// The total not locked tokens.
    #[serde(with = "u128_dec_format_compatible")]
    pub amount: Balance,
    /// The amount locked due to staking.
    #[serde(with = "u128_dec_format_compatible")]
    pub locked: Balance,
    /// Hash of the code stored in the storage for this account.
    pub code_hash: CryptoHash,
    /// Storage used by the given account, includes account id, this struct, access keys and other data.
    pub storage_usage: StorageUsage,
}

impl Account {
    /// Max number of bytes an account can have in its state (excluding contract code)
    /// before it is infeasible to delete.
    pub const MAX_ACCOUNT_DELETION_STORAGE_USAGE: u64 = 10_000;
}

/// Access key provides limited access to an account. Each access key belongs to some account and
/// is identified by a unique (within the account) public key. One account may have large number of
/// access keys. Access keys allow to act on behalf of the account by restricting transactions
/// that can be issued.
/// `account_id,public_key` is a key in the state
#[derive(
    BorshSerialize, BorshDeserialize, Serialize, Deserialize, PartialEq, Eq, Hash, Clone, Debug,
)]
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
    pub const ACCESS_KEY_NONCE_RANGE_MULTIPLIER: u64 = 1_000_000;

    pub fn full_access() -> Self {
        Self { nonce: 0, permission: AccessKeyPermission::FullAccess }
    }
}

/// Defines permissions for AccessKey
#[derive(
    BorshSerialize, BorshDeserialize, Serialize, Deserialize, PartialEq, Eq, Hash, Clone, Debug,
)]
pub enum AccessKeyPermission {
    FunctionCall(FunctionCallPermission),

    /// Grants full access to the account.
    /// NOTE: It's used to replace account-level public keys.
    FullAccess,
}

/// Grants limited permission to make transactions with FunctionCallActions
/// The permission can limit the allowed balance to be spent on the prepaid gas.
/// It also restrict the account ID of the receiver for this function call.
/// It also can restrict the method name for the allowed function calls.
#[derive(
    BorshSerialize, BorshDeserialize, Serialize, Deserialize, PartialEq, Eq, Hash, Clone, Debug,
)]
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

#[cfg(test)]
mod tests {
    use borsh::BorshSerialize;

    use crate::hash::hash;
    use crate::serialize::to_base;

    use super::*;

    #[test]
    fn test_account_serialization() {
        let acc = Account {
            amount: 1_000_000,
            locked: 1_000_000,
            code_hash: CryptoHash::default(),
            storage_usage: 100,
        };
        let bytes = acc.try_to_vec().unwrap();
        assert_eq!(to_base(&hash(&bytes)), "EVk5UaxBe8LQ8r8iD5EAxVBs6TJcMDKqyH7PBuho6bBJ");
    }
}
