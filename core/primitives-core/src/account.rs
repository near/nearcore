use crate::checked_feature;
use crate::hash::CryptoHash;
use crate::serialize::dec_format;
use crate::types::{Balance, Nonce, ProtocolVersion, StorageUsage};
use borsh::{BorshDeserialize, BorshSerialize};
pub use near_account_id as id;
use near_schema_checker_lib::ProtocolSchema;
use std::io;

#[derive(
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    PartialOrd,
    Eq,
    Clone,
    Copy,
    Debug,
    Default,
    serde::Serialize,
    serde::Deserialize,
    ProtocolSchema,
)]
pub enum AccountVersion {
    #[default]
    V1,
    V2,
}

/// Per account information stored in the state.
/// When introducing new version:
/// - introduce new AccountV[NewVersion] struct
/// - add new Account enum option V[NewVersion](AccountV[NewVersion])
/// - add new BorshVersionedAccount enum option V[NewVersion](AccountV[NewVersion])
/// - update SerdeAccount with newly added fields
/// - update serde ser/deser to properly handle conversions
#[derive(PartialEq, Eq, Debug, Clone, ProtocolSchema)]
pub enum Account {
    V1(AccountV1),
    V2(AccountV2),
}

// Original representation of the account.
#[derive(
    BorshSerialize,
    serde::Serialize,
    serde::Deserialize,
    PartialEq,
    Eq,
    Debug,
    Clone,
    ProtocolSchema,
)]
pub struct AccountV1 {
    /// The total not locked tokens.
    amount: Balance,
    /// The amount locked due to staking.
    locked: Balance,
    /// Hash of the code stored in the storage for this account.
    code_hash: CryptoHash,
    /// Storage used by the given account, includes account id, this struct, access keys and other data.
    storage_usage: StorageUsage,
}

impl AccountV1 {
    fn to_v2(&self, permanent_storage_bytes: StorageUsage) -> AccountV2 {
        AccountV2 {
            amount: self.amount,
            locked: self.locked,
            code_hash: self.code_hash,
            storage_usage: self.storage_usage,
            permanent_storage_bytes,
        }
    }
}

/// V2 introduces permanent_storage_bytes
#[derive(
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    PartialEq,
    Eq,
    Debug,
    Clone,
    ProtocolSchema,
)]
pub struct AccountV2 {
    /// The total not locked tokens.
    amount: Balance,
    /// The amount locked due to staking.
    locked: Balance,
    /// Hash of the code stored in the storage for this account.
    code_hash: CryptoHash,
    /// Storage used by the given account, includes account id, this struct, access keys and other data.
    storage_usage: StorageUsage,
    /// Permanent storage allowance, additional to what storage staking gives.
    permanent_storage_bytes: StorageUsage,
}

impl Account {
    /// Max number of bytes an account can have in its state (excluding contract code)
    /// before it is infeasible to delete.
    pub const MAX_ACCOUNT_DELETION_STORAGE_USAGE: u64 = 10_000;
    /// HACK: Using u128::MAX as a sentinel value, there are not enough tokens
    /// in total supply which makes it an invalid value. We use it to
    /// differentiate AccountVersion V1 from newer versions.
    const SERIALIZATION_SENTINEL: u128 = u128::MAX;

    // TODO(nonrefundable) Consider using consider some additional new types
    // or a different way to write down constructor (e.g. builder pattern.)
    pub fn new(
        amount: Balance,
        locked: Balance,
        permanent_storage_bytes: StorageUsage,
        code_hash: CryptoHash,
        storage_usage: StorageUsage,
        protocol_version: ProtocolVersion,
    ) -> Self {
        if permanent_storage_bytes > 0 {
            assert!(checked_feature!(
                "protocol_feature_nonrefundable_transfer_nep491",
                NonrefundableStorage,
                protocol_version
            ));
            Self::V2(AccountV2 {
                amount,
                locked,
                permanent_storage_bytes,
                code_hash,
                storage_usage,
            })
        } else {
            Self::V1(AccountV1 { amount, locked, code_hash, storage_usage })
        }
    }

    #[inline]
    pub fn amount(&self) -> Balance {
        match self {
            Self::V1(account) => account.amount,
            Self::V2(account) => account.amount,
        }
    }

    #[inline]
    pub fn permanent_storage_bytes(&self) -> StorageUsage {
        match self {
            Self::V1(_) => 0,
            Self::V2(account) => account.permanent_storage_bytes,
        }
    }

    #[inline]
    pub fn locked(&self) -> Balance {
        match self {
            Self::V1(account) => account.locked,
            Self::V2(account) => account.locked,
        }
    }

    #[inline]
    pub fn code_hash(&self) -> CryptoHash {
        match self {
            Self::V1(account) => account.code_hash,
            Self::V2(account) => account.code_hash,
        }
    }

    #[inline]
    pub fn storage_usage(&self) -> StorageUsage {
        match self {
            Self::V1(account) => account.storage_usage,
            Self::V2(account) => account.storage_usage,
        }
    }

    #[inline]
    pub fn version(&self) -> AccountVersion {
        match self {
            Self::V1(_) => AccountVersion::V1,
            Self::V2(_) => AccountVersion::V2,
        }
    }

    #[inline]
    pub fn set_amount(&mut self, amount: Balance) {
        match self {
            Self::V1(account) => account.amount = amount,
            Self::V2(account) => account.amount = amount,
        }
    }

    #[inline]
    pub fn set_permanent_storage_bytes(&mut self, permanent_storage_bytes: StorageUsage) {
        match self {
            Self::V1(v1) => {
                *self = Account::V2(v1.to_v2(permanent_storage_bytes));
            }
            Self::V2(v2) => v2.permanent_storage_bytes = permanent_storage_bytes,
        }
    }

    #[inline]
    pub fn set_locked(&mut self, locked: Balance) {
        match self {
            Self::V1(account) => account.locked = locked,
            Self::V2(account) => account.locked = locked,
        }
    }

    #[inline]
    pub fn set_code_hash(&mut self, code_hash: CryptoHash) {
        match self {
            Self::V1(account) => account.code_hash = code_hash,
            Self::V2(account) => account.code_hash = code_hash,
        }
    }

    #[inline]
    pub fn set_storage_usage(&mut self, storage_usage: StorageUsage) {
        match self {
            Self::V1(account) => account.storage_usage = storage_usage,
            Self::V2(account) => account.storage_usage = storage_usage,
        }
    }
}

/// Account representation for serde ser/deser that maintains both backward
/// and forward compatibility.
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug, Clone, ProtocolSchema)]
struct SerdeAccount {
    #[serde(with = "dec_format")]
    amount: Balance,
    #[serde(with = "dec_format")]
    locked: Balance,
    #[serde(with = "dec_format", default, skip_serializing_if = "Option::is_none")]
    permanent_storage_bytes: Option<StorageUsage>,
    code_hash: CryptoHash,
    storage_usage: StorageUsage,
    /// Version of Account in re migrations and similar.
    #[serde(default)]
    version: AccountVersion,
}

impl<'de> serde::Deserialize<'de> for Account {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let account_data = SerdeAccount::deserialize(deserializer)?;
        match account_data.version {
            AccountVersion::V1 => {
                if account_data.permanent_storage_bytes.is_some() {
                    return Err(serde::de::Error::custom(
                        "permanent storage bytes should not be set for V1 account",
                    ));
                }
                Ok(Account::V1(AccountV1 {
                    amount: account_data.amount,
                    locked: account_data.locked,
                    code_hash: account_data.code_hash,
                    storage_usage: account_data.storage_usage,
                }))
            }
            AccountVersion::V2 => {
                let Some(permanent_storage_bytes) = account_data.permanent_storage_bytes else {
                    return Err(serde::de::Error::custom(
                        "permanent storage bytes is missing for V2",
                    ));
                };
                Ok(Account::V2(AccountV2 {
                    amount: account_data.amount,
                    locked: account_data.locked,
                    permanent_storage_bytes,
                    code_hash: account_data.code_hash,
                    storage_usage: account_data.storage_usage,
                }))
            }
        }
    }
}

impl serde::Serialize for Account {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let version = self.version();
        let permanent_storage_bytes = match version {
            AccountVersion::V1 => None,
            AccountVersion::V2 => Some(self.permanent_storage_bytes()),
        };
        let repr = SerdeAccount {
            amount: self.amount(),
            locked: self.locked(),
            permanent_storage_bytes,
            code_hash: self.code_hash(),
            storage_usage: self.storage_usage(),
            version,
        };
        repr.serialize(serializer)
    }
}

#[derive(BorshSerialize, BorshDeserialize)]
enum BorshVersionedAccount {
    // V1 is not included since it is serialized directly without being wrapped in enum
    V2(AccountV2),
}

impl BorshDeserialize for Account {
    fn deserialize_reader<R: io::Read>(rd: &mut R) -> io::Result<Self> {
        // The first value of all Account serialization formats is a u128,
        // either a sentinel or a balance.
        let sentinel_or_amount = u128::deserialize_reader(rd)?;
        if sentinel_or_amount == Account::SERIALIZATION_SENTINEL {
            let versioned_account = BorshVersionedAccount::deserialize_reader(rd)?;
            let account = match versioned_account {
                BorshVersionedAccount::V2(account_v2) => Account::V2(account_v2),
            };
            Ok(account)
        } else {
            // Legacy unversioned representation of Account
            let locked = u128::deserialize_reader(rd)?;
            let code_hash = CryptoHash::deserialize_reader(rd)?;
            let storage_usage = StorageUsage::deserialize_reader(rd)?;

            Ok(Account::V1(AccountV1 {
                amount: sentinel_or_amount,
                locked,
                code_hash,
                storage_usage,
            }))
        }
    }
}

impl BorshSerialize for Account {
    fn serialize<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        let versioned_account = match self {
            Account::V1(account_v1) => return account_v1.serialize(writer),
            Account::V2(account_v2) => BorshVersionedAccount::V2(account_v2.clone()),
        };
        let sentinel = Account::SERIALIZATION_SENTINEL;
        BorshSerialize::serialize(&sentinel, writer)?;
        BorshSerialize::serialize(&versioned_account, writer)
    }
}

/// Access key provides limited access to an account. Each access key belongs to some account and
/// is identified by a unique (within the account) public key. One account may have large number of
/// access keys. Access keys allow to act on behalf of the account by restricting transactions
/// that can be issued.
/// `account_id,public_key` is a key in the state
#[derive(
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    Hash,
    Clone,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    ProtocolSchema,
)]
pub struct AccessKey {
    /// Nonce for this access key, used for tx nonce generation. When access key is created, nonce
    /// is set to `(block_height - 1) * 1e6` to avoid tx hash collision on access key re-creation.
    /// See <https://github.com/near/nearcore/issues/3779> for more details.
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
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    Hash,
    Clone,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    ProtocolSchema,
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
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    PartialEq,
    Eq,
    Hash,
    Clone,
    Debug,
    ProtocolSchema,
)]
pub struct FunctionCallPermission {
    /// Allowance is a balance limit to use by this access key to pay for function call gas and
    /// transaction fees. When this access key is used, both account balance and the allowance is
    /// decreased by the same value.
    /// `None` means unlimited allowance.
    /// NOTE: To change or increase the allowance, the old access key needs to be deleted and a new
    /// access key should be created.
    #[serde(with = "dec_format")]
    pub allowance: Option<Balance>,

    // This isn't an AccountId because already existing records in testnet genesis have invalid
    // values for this field (see: https://github.com/near/nearcore/pull/4621#issuecomment-892099860)
    // we accommodate those by using a string, allowing us to read and parse genesis.
    /// The access key only allows transactions with the given receiver's account id.
    pub receiver_id: String,

    /// A list of method names that can be used. The access key only allows transactions with the
    /// function call of one of the given method names.
    /// Empty list means any method name can be used.
    pub method_names: Vec<String>,
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
    use crate::version::ProtocolFeature;

    use super::*;

    #[test]
    #[should_panic]
    fn test_v1_account_cannot_have_permanent_storage_bytes() {
        #[cfg(not(feature = "protocol_feature_nonrefundable_transfer_nep491"))]
        let protocol_version = crate::version::PROTOCOL_VERSION;

        #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
        let protocol_version = ProtocolFeature::NonrefundableStorage.protocol_version() - 1;

        Account::new(0, 0, 1, CryptoHash::default(), 0, protocol_version);
    }

    #[test]
    fn test_v1_account_serde_serialization() {
        let old_account = AccountV1 {
            amount: 1_000_000,
            locked: 1_000_000,
            code_hash: CryptoHash::hash_bytes(&[42]),
            storage_usage: 100,
        };

        let serialized_account = serde_json::to_string(&old_account).unwrap();
        let expected_serde_repr = SerdeAccount {
            amount: old_account.amount,
            locked: old_account.locked,
            permanent_storage_bytes: None,
            code_hash: old_account.code_hash,
            storage_usage: old_account.storage_usage,
            version: AccountVersion::V1,
        };
        let actual_serde_repr: SerdeAccount = serde_json::from_str(&serialized_account).unwrap();
        assert_eq!(actual_serde_repr, expected_serde_repr);

        let new_account: Account = serde_json::from_str(&serialized_account).unwrap();
        assert_eq!(new_account, Account::V1(old_account));

        let new_serialized_account = serde_json::to_string(&new_account).unwrap();
        let deserialized_account: Account = serde_json::from_str(&new_serialized_account).unwrap();
        assert_eq!(deserialized_account, new_account);
    }

    #[test]
    fn test_v1_account_borsh_serialization() {
        let old_account = AccountV1 {
            amount: 100,
            locked: 200,
            code_hash: CryptoHash::hash_bytes(&[42]),
            storage_usage: 300,
        };
        let old_bytes = borsh::to_vec(&old_account).unwrap();
        let new_account = <Account as BorshDeserialize>::deserialize(&mut &old_bytes[..]).unwrap();
        assert_eq!(new_account, Account::V1(old_account));

        let new_bytes = borsh::to_vec(&new_account).unwrap();
        assert_eq!(new_bytes, old_bytes);
        let deserialized_account =
            <Account as BorshDeserialize>::deserialize(&mut &new_bytes[..]).unwrap();
        assert_eq!(deserialized_account, new_account);
    }

    #[test]
    fn test_account_v2_serde_serialization() {
        let account_v2 = AccountV2 {
            amount: 10_000_000,
            locked: 100_000,
            permanent_storage_bytes: 2000,
            code_hash: CryptoHash::hash_bytes(&[42]),
            storage_usage: 1000,
        };
        let account = Account::V2(account_v2.clone());

        let serialized_account = serde_json::to_string(&account).unwrap();
        let expected_serde_repr = SerdeAccount {
            amount: account_v2.amount,
            locked: account_v2.locked,
            permanent_storage_bytes: Some(account_v2.permanent_storage_bytes),
            code_hash: account_v2.code_hash,
            storage_usage: account_v2.storage_usage,
            version: AccountVersion::V2,
        };
        let actual_serde_repr: SerdeAccount = serde_json::from_str(&serialized_account).unwrap();
        assert_eq!(actual_serde_repr, expected_serde_repr);

        let deserialized_account: Account = serde_json::from_str(&serialized_account).unwrap();
        assert_eq!(deserialized_account, account);
    }

    /// It is impossible to construct V1 account with permanent_storage_bytes greater than 0.
    /// So the situation in this test is theoretical.
    ///
    /// Serialization of V1 account with permanent_storage_bytes amount greater than 0 would pass without an error,
    /// but an error would be raised on deserialization of such invalid data.
    #[test]
    fn test_account_v2_serde_serialization_invalid_data() {
        let account = SerdeAccount {
            amount: 10_000_000,
            locked: 100_000,
            permanent_storage_bytes: Some(1),
            code_hash: CryptoHash::default(),
            storage_usage: 1000,
            version: AccountVersion::V1,
        };
        let serialized_account = serde_json::to_string(&account).unwrap();
        let deserialization_result: Result<Account, serde_json::Error> =
            serde_json::from_str(&serialized_account);
        assert!(deserialization_result.is_err());
    }

    #[test]
    fn test_account_v2_borsh_serialization() {
        let account_v2 = AccountV2 {
            amount: 10_000_000,
            locked: 100_000,
            permanent_storage_bytes: 2000,
            code_hash: CryptoHash::hash_bytes(&[42]),
            storage_usage: 1000,
        };
        let account = Account::V2(account_v2);
        let serialized_account = borsh::to_vec(&account).unwrap();
        let deserialized_account =
            <Account as BorshDeserialize>::deserialize(&mut &serialized_account[..]).unwrap();
        assert_eq!(deserialized_account, account);
    }
}
