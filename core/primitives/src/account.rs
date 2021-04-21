#[cfg(feature = "protocol_feature_add_account_versions")]
use crate::checked_feature;
use crate::serialize::u128_dec_format_compatible;
use borsh::{BorshDeserialize, BorshSerialize};
#[cfg(feature = "protocol_feature_add_account_versions")]
use core::default::Default;
#[cfg(feature = "protocol_feature_add_account_versions")]
use core::result::Result;
#[cfg(feature = "protocol_feature_add_account_versions")]
use core::result::Result::Ok;
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::{Balance, ProtocolVersion, StorageUsage};
use serde;
use serde::{Deserialize, Serialize};
#[cfg(feature = "protocol_feature_add_account_versions")]
use std::io;

#[cfg(feature = "protocol_feature_add_account_versions")]
#[derive(
    BorshSerialize, BorshDeserialize, Serialize, Deserialize, PartialEq, Eq, Debug, Clone, Copy,
)]
pub enum AccountVersion {
    /// Versioning release
    V1,
    /// Recalculated storage usage due to previous bug, see
    /// https://github.com/near/nearcore/issues/3824
    V2,
}

#[cfg(feature = "protocol_feature_add_account_versions")]
impl Default for AccountVersion {
    fn default() -> Self {
        AccountVersion::V1
    }
}

/// Per account information stored in the state.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
#[cfg_attr(
    not(feature = "protocol_feature_add_account_versions"),
    derive(BorshSerialize, BorshDeserialize)
)]
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
    /// Version of Account in re migrations and similar
    #[cfg(feature = "protocol_feature_add_account_versions")]
    #[serde(default)]
    version: AccountVersion,
}

impl Account {
    /// Max number of bytes an account can have in its state (excluding contract code)
    /// before it is infeasible to delete.
    pub const MAX_ACCOUNT_DELETION_STORAGE_USAGE: u64 = 10_000;

    pub fn new(
        amount: Balance,
        locked: Balance,
        code_hash: CryptoHash,
        storage_usage: StorageUsage,
        protocol_version: ProtocolVersion,
    ) -> Self {
        #[cfg(not(feature = "protocol_feature_add_account_versions"))]
        let _ = protocol_version;
        Self {
            amount,
            locked,
            code_hash,
            storage_usage,
            #[cfg(feature = "protocol_feature_add_account_versions")]
            version: if checked_feature!(
                "protocol_feature_add_account_versions",
                AccountVersions,
                protocol_version
            ) {
                AccountVersion::V2
            } else {
                AccountVersion::V1
            },
        }
    }

    #[inline]
    pub fn amount(&self) -> Balance {
        self.amount
    }

    #[inline]
    pub fn locked(&self) -> Balance {
        self.locked
    }

    #[inline]
    pub fn code_hash(&self) -> CryptoHash {
        self.code_hash
    }

    #[inline]
    pub fn storage_usage(&self) -> StorageUsage {
        self.storage_usage
    }

    #[inline]
    #[cfg(feature = "protocol_feature_add_account_versions")]
    pub fn version(&self) -> AccountVersion {
        self.version
    }

    #[inline]
    pub fn set_amount(&mut self, amount: Balance) {
        self.amount = amount;
    }

    #[inline]
    pub fn set_locked(&mut self, locked: Balance) {
        self.locked = locked;
    }

    #[inline]
    pub fn set_code_hash(&mut self, code_hash: CryptoHash) {
        self.code_hash = code_hash;
    }

    #[inline]
    pub fn set_storage_usage(&mut self, storage_usage: StorageUsage) {
        self.storage_usage = storage_usage;
    }

    #[inline]
    #[cfg(feature = "protocol_feature_add_account_versions")]
    pub fn set_version(&mut self, version: AccountVersion) {
        self.version = version;
    }
}

#[cfg(feature = "protocol_feature_add_account_versions")]
#[derive(BorshSerialize, BorshDeserialize)]
struct LegacyAccount {
    amount: Balance,
    locked: Balance,
    code_hash: CryptoHash,
    storage_usage: StorageUsage,
}

#[cfg(feature = "protocol_feature_add_account_versions")]
#[derive(BorshSerialize, BorshDeserialize)]
struct SerializableAccount {
    amount: Balance,
    locked: Balance,
    code_hash: CryptoHash,
    storage_usage: StorageUsage,
    version: AccountVersion,
}

#[cfg(feature = "protocol_feature_add_account_versions")]
impl BorshDeserialize for Account {
    fn deserialize(buf: &mut &[u8]) -> Result<Self, io::Error> {
        if buf.len() == std::mem::size_of::<LegacyAccount>() {
            // This should only ever happen if we have pre-transition account serialized in state
            // See test_account_size
            let deserialized_account = LegacyAccount::deserialize(buf)?;
            Ok(Account {
                amount: deserialized_account.amount,
                locked: deserialized_account.locked,
                code_hash: deserialized_account.code_hash,
                storage_usage: deserialized_account.storage_usage,
                version: AccountVersion::V1,
            })
        } else {
            let deserialized_account = SerializableAccount::deserialize(buf)?;
            Ok(Account {
                amount: deserialized_account.amount,
                locked: deserialized_account.locked,
                code_hash: deserialized_account.code_hash,
                storage_usage: deserialized_account.storage_usage,
                version: deserialized_account.version,
            })
        }
    }
}

#[cfg(feature = "protocol_feature_add_account_versions")]
impl BorshSerialize for Account {
    fn serialize<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        match self.version {
            AccountVersion::V1 => LegacyAccount {
                amount: self.amount,
                locked: self.locked,
                code_hash: self.code_hash,
                storage_usage: self.storage_usage,
            }
            .serialize(writer),
            _ => SerializableAccount {
                amount: self.amount,
                locked: self.locked,
                code_hash: self.code_hash,
                storage_usage: self.storage_usage,
                version: self.version,
            }
            .serialize(writer),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::account::Account;
    #[cfg(feature = "protocol_feature_add_account_versions")]
    use crate::account::{AccountVersion, LegacyAccount};
    #[cfg(feature = "protocol_feature_add_account_versions")]
    use crate::borsh::BorshDeserialize;
    use crate::hash::CryptoHash;
    #[cfg(feature = "protocol_feature_add_account_versions")]
    use crate::version::ProtocolFeature;
    #[cfg(not(feature = "protocol_feature_add_account_versions"))]
    use crate::version::PROTOCOL_VERSION;
    use borsh::BorshSerialize;
    use near_primitives_core::hash::hash;
    use near_primitives_core::serialize::to_base;

    #[test]
    fn test_account_serialization() {
        #[cfg(feature = "protocol_feature_add_account_versions")]
        let acc = Account::new(
            1_000_000,
            1_000_000,
            CryptoHash::default(),
            100,
            ProtocolFeature::AccountVersions.protocol_version() - 1,
        );
        #[cfg(not(feature = "protocol_feature_add_account_versions"))]
        let acc = Account::new(1_000_000, 1_000_000, CryptoHash::default(), 100, PROTOCOL_VERSION);
        let bytes = acc.try_to_vec().unwrap();
        assert_eq!(to_base(&hash(&bytes)), "EVk5UaxBe8LQ8r8iD5EAxVBs6TJcMDKqyH7PBuho6bBJ");
    }

    #[test]
    #[cfg(feature = "protocol_feature_add_account_versions")]
    fn test_account_size() {
        let new_account = Account::new(
            0,
            0,
            CryptoHash::default(),
            0,
            ProtocolFeature::AccountVersions.protocol_version(),
        );
        let old_account = Account::new(
            0,
            0,
            CryptoHash::default(),
            0,
            ProtocolFeature::AccountVersions.protocol_version() - 1,
        );
        let new_bytes = new_account.try_to_vec().unwrap();
        let old_bytes = old_account.try_to_vec().unwrap();
        assert!(new_bytes.len() > old_bytes.len());
        assert_eq!(old_bytes.len(), std::mem::size_of::<LegacyAccount>());
    }

    #[test]
    #[cfg(feature = "protocol_feature_add_account_versions")]
    fn test_account_deserialization() {
        let old_account = LegacyAccount {
            amount: 100,
            locked: 200,
            code_hash: CryptoHash::default(),
            storage_usage: 300,
        };
        let mut old_bytes = &old_account.try_to_vec().unwrap()[..];
        let new_account = <Account as BorshDeserialize>::deserialize(&mut old_bytes).unwrap();
        assert_eq!(new_account.amount, old_account.amount);
        assert_eq!(new_account.locked, old_account.locked);
        assert_eq!(new_account.code_hash, old_account.code_hash);
        assert_eq!(new_account.storage_usage, old_account.storage_usage);
        assert_eq!(new_account.version, AccountVersion::V1);
        let mut new_bytes = &new_account.try_to_vec().unwrap()[..];
        let deserialized_account =
            <Account as BorshDeserialize>::deserialize(&mut new_bytes).unwrap();
        assert_eq!(deserialized_account, new_account);
    }
}
