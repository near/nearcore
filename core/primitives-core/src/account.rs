use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

use crate::hash::CryptoHash;
use crate::serialize::{option_u128_dec_format, u128_dec_format_compatible};
use crate::types::{AccountId, Balance, Nonce, StorageUsage};
#[cfg(feature = "protocol_feature_add_account_versions")]
use borsh::maybestd::io::Error;

#[cfg(not(feature = "protocol_feature_add_account_versions"))]
/// Per account information stored in the state.
#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct Account {
    /// The total not locked tokens.
    #[serde(with = "u128_dec_format_compatible")]
    amount: Balance,
    /// The amount locked due to staking.
    #[serde(with = "u128_dec_format_compatible")]
    locked: Balance,
    /// Hash of the code stored in the storage for this account.
    code_hash: CryptoHash,
    /// Storage used by the given account, includes account id, this struct, access keys and other data.
    storage_usage: StorageUsage,
}

#[cfg(feature = "protocol_feature_add_account_versions")]
#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct AccountV1 {
    /// The total not locked tokens.
    #[serde(with = "u128_dec_format_compatible")]
    amount: Balance,
    /// The amount locked due to staking.
    #[serde(with = "u128_dec_format_compatible")]
    locked: Balance,
    /// Hash of the code stored in the storage for this account.
    code_hash: CryptoHash,
    /// Storage used by the given account, includes account id, this struct, access keys and other data.
    storage_usage: StorageUsage,
}

#[cfg(feature = "protocol_feature_add_account_versions")]
#[derive(BorshSerialize, Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub enum Account {
    AccountV1(AccountV1),
}

impl Account {
    #[cfg(not(feature = "protocol_feature_add_account_versions"))]
    pub fn new(
        amount: Balance,
        locked: Balance,
        code_hash: CryptoHash,
        storage_usage: StorageUsage,
    ) -> Self {
        Account { amount, locked, code_hash, storage_usage }
    }

    #[cfg(feature = "protocol_feature_add_account_versions")]
    pub fn new(
        amount: Balance,
        locked: Balance,
        code_hash: CryptoHash,
        storage_usage: StorageUsage,
    ) -> Self {
        Account::AccountV1(AccountV1 { amount, locked, code_hash, storage_usage })
    }

    #[cfg(not(feature = "protocol_feature_add_account_versions"))]
    #[inline]
    pub fn amount(&self) -> Balance {
        self.amount
    }

    #[cfg(feature = "protocol_feature_add_account_versions")]
    #[inline]
    pub fn amount(&self) -> Balance {
        match self {
            Account::AccountV1(acc) => acc.amount,
        }
    }

    #[cfg(not(feature = "protocol_feature_add_account_versions"))]
    #[inline]
    pub fn locked(&self) -> Balance {
        self.locked
    }

    #[cfg(feature = "protocol_feature_add_account_versions")]
    #[inline]
    pub fn locked(&self) -> Balance {
        match self {
            Account::AccountV1(acc) => acc.locked,
        }
    }

    #[cfg(not(feature = "protocol_feature_add_account_versions"))]
    #[inline]
    pub fn code_hash(&self) -> CryptoHash {
        self.code_hash
    }

    #[cfg(feature = "protocol_feature_add_account_versions")]
    #[inline]
    pub fn code_hash(&self) -> CryptoHash {
        match self {
            Account::AccountV1(acc) => acc.code_hash,
        }
    }

    #[cfg(not(feature = "protocol_feature_add_account_versions"))]
    #[inline]
    pub fn storage_usage(&self) -> StorageUsage {
        self.storage_usage
    }

    #[cfg(feature = "protocol_feature_add_account_versions")]
    #[inline]
    pub fn storage_usage(&self) -> StorageUsage {
        match self {
            Account::AccountV1(acc) => acc.storage_usage,
        }
    }

    #[cfg(not(feature = "protocol_feature_add_account_versions"))]
    #[inline]
    pub fn set_amount(&mut self, amount: Balance) {
        self.amount = amount;
    }

    #[cfg(feature = "protocol_feature_add_account_versions")]
    #[inline]
    pub fn set_amount(&mut self, amount: Balance) {
        match self {
            Account::AccountV1(acc) => {
                acc.amount = amount;
            }
        }
    }

    #[cfg(not(feature = "protocol_feature_add_account_versions"))]
    #[inline]
    pub fn set_locked(&mut self, locked: Balance) {
        self.locked = locked;
    }

    #[cfg(feature = "protocol_feature_add_account_versions")]
    #[inline]
    pub fn set_locked(&mut self, locked: Balance) {
        match self {
            Account::AccountV1(acc) => {
                acc.locked = locked;
            }
        }
    }

    #[cfg(not(feature = "protocol_feature_add_account_versions"))]
    #[inline]
    pub fn set_code_hash(&mut self, code_hash: CryptoHash) {
        self.code_hash = code_hash;
    }

    #[cfg(feature = "protocol_feature_add_account_versions")]
    #[inline]
    pub fn set_code_hash(&mut self, code_hash: CryptoHash) {
        match self {
            Account::AccountV1(acc) => {
                acc.code_hash = code_hash;
            }
        }
    }

    #[cfg(not(feature = "protocol_feature_add_account_versions"))]
    #[inline]
    pub fn set_storage_usage(&mut self, storage_usage: StorageUsage) {
        self.storage_usage = storage_usage;
    }

    #[cfg(feature = "protocol_feature_add_account_versions")]
    #[inline]
    pub fn set_storage_usage(&mut self, storage_usage: StorageUsage) {
        match self {
            Account::AccountV1(acc) => {
                acc.storage_usage = storage_usage;
            }
        }
    }
}

#[cfg(feature = "protocol_feature_add_account_versions")]
impl BorshDeserialize for Account {
    fn deserialize(buf: &mut &[u8]) -> Result<Self, Error> {
        if buf.len() == std::mem::size_of::<AccountV1>() {
            // This should only ever happen if we have pre-transition account serialized in state
            // See test_account_size
            Ok(Account::AccountV1(<AccountV1 as BorshDeserialize>::deserialize(buf)?))
        } else {
            #[derive(BorshDeserialize)]
            enum DeserializableAccount {
                AccountV1(AccountV1),
            };
            let deserialized_account = DeserializableAccount::deserialize(buf)?;
            match deserialized_account {
                DeserializableAccount::AccountV1(account) => Ok(Account::AccountV1(account)),
            }
        }
    }
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
        let acc = Account::new(1_000_000, 1_000_000, CryptoHash::default(), 100);
        let bytes = acc.try_to_vec().unwrap();
        #[cfg(not(feature = "protocol_feature_add_account_versions"))]
        assert_eq!(to_base(&hash(&bytes)), "EVk5UaxBe8LQ8r8iD5EAxVBs6TJcMDKqyH7PBuho6bBJ");
        #[cfg(feature = "protocol_feature_add_account_versions")]
        {
            assert_eq!(to_base(&hash(&bytes)), "7HBKnu8VPDaVgj6jbGvdVgTzPG3uBdZ97WGhoYpKT7hZ");
            match acc {
                Account::AccountV1(account) => {
                    let pbytes = account.try_to_vec().unwrap();
                    assert_eq!(
                        to_base(&hash(&pbytes)),
                        "EVk5UaxBe8LQ8r8iD5EAxVBs6TJcMDKqyH7PBuho6bBJ"
                    );
                }
            }
        }
    }

    #[test]
    #[cfg(feature = "protocol_feature_add_account_versions")]
    fn test_account_size() {
        let new_account = Account::new(0, 0, CryptoHash::default(), 0);
        let old_account =
            AccountV1 { amount: 0, locked: 0, code_hash: CryptoHash::default(), storage_usage: 0 };
        let new_bytes = new_account.try_to_vec().unwrap();
        let old_bytes = old_account.try_to_vec().unwrap();
        assert!(new_bytes.len() > old_bytes.len());
        assert_eq!(old_bytes.len(), std::mem::size_of::<AccountV1>());
    }

    #[test]
    #[cfg(feature = "protocol_feature_add_account_versions")]
    fn test_account_deserialization() {
        let old_account = AccountV1 {
            amount: 100,
            locked: 200,
            code_hash: CryptoHash::default(),
            storage_usage: 300,
        };
        let mut old_bytes = &old_account.try_to_vec().unwrap()[..];
        let new_account = <Account as BorshDeserialize>::deserialize(&mut old_bytes).unwrap();
        assert_eq!(new_account, Account::AccountV1(old_account));
        let mut new_bytes = &new_account.try_to_vec().unwrap()[..];
        let deserialized_account =
            <Account as BorshDeserialize>::deserialize(&mut new_bytes).unwrap();
        assert_eq!(deserialized_account, new_account);
    }
}
