#[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
use crate::checked_feature;
use crate::hash::CryptoHash;
use crate::serialize::dec_format;
use crate::types::{Balance, Nonce, ProtocolVersion, StorageUsage};
use borsh::{BorshDeserialize, BorshSerialize};
pub use near_account_id as id;
use near_structs_checker::ProtocolStruct;
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
)]
pub enum AccountVersion {
    #[cfg_attr(not(feature = "protocol_feature_nonrefundable_transfer_nep491"), default)]
    V1,
    #[default]
    #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
    V2,
}

impl TryFrom<u8> for AccountVersion {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(AccountVersion::V1),
            #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
            2 => Ok(AccountVersion::V2),
            _ => Err(()),
        }
    }
}

/// Per account information stored in the state.
#[cfg_attr(
    not(feature = "protocol_feature_nonrefundable_transfer_nep491"),
    derive(serde::Deserialize)
)]
#[derive(serde::Serialize, PartialEq, Eq, Debug, Clone)]
pub struct Account {
    /// The total not locked tokens.
    #[serde(with = "dec_format")]
    amount: Balance,
    /// The amount locked due to staking.
    #[serde(with = "dec_format")]
    locked: Balance,
    /// Permanent storage allowance, additional to what storage staking gives.
    #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
    #[serde(with = "dec_format")]
    permanent_storage_bytes: StorageUsage,
    /// Hash of the code stored in the storage for this account.
    code_hash: CryptoHash,
    /// Storage used by the given account, includes account id, this struct, access keys and other data.
    storage_usage: StorageUsage,
    /// Version of Account in re migrations and similar.
    #[serde(default)]
    version: AccountVersion,
}

impl Account {
    /// Max number of bytes an account can have in its state (excluding contract code)
    /// before it is infeasible to delete.
    pub const MAX_ACCOUNT_DELETION_STORAGE_USAGE: u64 = 10_000;
    /// HACK: Using u128::MAX as a sentinel value, there are not enough tokens
    /// in total supply which makes it an invalid value. We use it to
    /// differentiate AccountVersion V1 from newer versions.
    const SERIALIZATION_SENTINEL: u128 = u128::MAX;

    // TODO(nonrefundable) Consider using consider some additional newtypes
    // or a different way to write down constructor (e.g. builder pattern.)
    pub fn new(
        amount: Balance,
        locked: Balance,
        permanent_storage_bytes: StorageUsage,
        code_hash: CryptoHash,
        storage_usage: StorageUsage,
        #[cfg_attr(not(feature = "protocol_feature_nonrefundable_transfer_nep491"), allow(unused))]
        protocol_version: ProtocolVersion,
    ) -> Self {
        #[cfg(not(feature = "protocol_feature_nonrefundable_transfer_nep491"))]
        let account_version = AccountVersion::V1;

        #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
        let account_version = if checked_feature!("stable", NonrefundableStorage, protocol_version)
        {
            AccountVersion::V2
        } else {
            AccountVersion::V1
        };
        if account_version == AccountVersion::V1 {
            assert_eq!(permanent_storage_bytes, 0);
        }
        Account {
            amount,
            locked,
            #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
            permanent_storage_bytes,
            code_hash,
            storage_usage,
            version: account_version,
        }
    }

    #[inline]
    pub fn amount(&self) -> Balance {
        self.amount
    }

    #[inline]
    #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
    pub fn permanent_storage_bytes(&self) -> StorageUsage {
        self.permanent_storage_bytes
    }

    #[inline]
    #[cfg(not(feature = "protocol_feature_nonrefundable_transfer_nep491"))]
    pub fn permanent_storage_bytes(&self) -> StorageUsage {
        0
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
    pub fn version(&self) -> AccountVersion {
        self.version
    }

    #[inline]
    pub fn set_amount(&mut self, amount: Balance) {
        self.amount = amount;
    }

    #[inline]
    #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
    pub fn set_permanent_storage_bytes(&mut self, permanent_storage_bytes: StorageUsage) {
        self.permanent_storage_bytes = permanent_storage_bytes;
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

    pub fn set_version(&mut self, version: AccountVersion) {
        self.version = version;
    }
}

/// These accounts are serialized in merklized state.
/// We keep old accounts in the old format to avoid migration of the MPT.
#[derive(BorshSerialize, serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug, Clone)]
struct LegacyAccount {
    amount: Balance,
    locked: Balance,
    code_hash: CryptoHash,
    storage_usage: StorageUsage,
}

/// We only allow nonrefundable storage on new accounts (see `LegacyAccount`).
#[derive(BorshSerialize, BorshDeserialize)]
struct AccountV2 {
    amount: Balance,
    locked: Balance,
    code_hash: CryptoHash,
    storage_usage: StorageUsage,
    #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
    permanent_storage_bytes: StorageUsage,
}

/// We need custom serde deserialization in order to parse mainnet genesis accounts (LegacyAccounts)
/// as accounts V1. This preserves the mainnet genesis hash.
#[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
impl<'de> serde::Deserialize<'de> for Account {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        struct AccountData {
            #[serde(with = "dec_format")]
            amount: Balance,
            #[serde(with = "dec_format")]
            locked: Balance,
            // If the field is missing, serde will use None as the default.
            #[serde(default, with = "dec_format")]
            permanent_storage_bytes: Option<StorageUsage>,
            code_hash: CryptoHash,
            storage_usage: StorageUsage,
            #[serde(default)]
            version: Option<AccountVersion>,
        }

        let account_data = AccountData::deserialize(deserializer)?;

        match account_data.permanent_storage_bytes {
            Some(permanent_storage_bytes) => {
                // Given that the `permanent_storage_bytes` field has been serialized, the `version` field must has been serialized too.
                let version = match account_data.version {
                    Some(version) => version,
                    None => {
                        return Err(serde::de::Error::custom("missing `version` field"));
                    }
                };

                #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
                if version < AccountVersion::V2 && permanent_storage_bytes > 0 {
                    return Err(serde::de::Error::custom(
                        "permanent storage bytes positive amount exists for account version older than V2",
                    ));
                }

                Ok(Account {
                    amount: account_data.amount,
                    locked: account_data.locked,
                    code_hash: account_data.code_hash,
                    storage_usage: account_data.storage_usage,
                    permanent_storage_bytes,
                    version,
                })
            }
            None => Ok(Account {
                amount: account_data.amount,
                locked: account_data.locked,
                code_hash: account_data.code_hash,
                storage_usage: account_data.storage_usage,
                permanent_storage_bytes: 0,
                version: AccountVersion::V1,
            }),
        }
    }
}

impl BorshDeserialize for Account {
    fn deserialize_reader<R: io::Read>(rd: &mut R) -> io::Result<Self> {
        // The first value of all Account serialization formats is a u128,
        // either a sentinel or a balance.
        let sentinel_or_amount = u128::deserialize_reader(rd)?;
        if sentinel_or_amount == Account::SERIALIZATION_SENTINEL {
            if cfg!(not(feature = "protocol_feature_nonrefundable_transfer_nep491")) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("account serialization sentinel not allowed for AccountV1"),
                ));
            }

            // Account v2 or newer.
            let version_byte = u8::deserialize_reader(rd)?;
            let version = AccountVersion::try_from(version_byte).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "error deserializing account: invalid account version {}",
                        version_byte
                    ),
                )
            })?;
            #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
            if version < AccountVersion::V2 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("expected account version 2 or higher, got {:?}", version),
                ));
            }
            let account = AccountV2::deserialize_reader(rd)?;

            Ok(Account {
                amount: account.amount,
                locked: account.locked,
                #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
                permanent_storage_bytes: account.permanent_storage_bytes,
                code_hash: account.code_hash,
                storage_usage: account.storage_usage,
                version,
            })
        } else {
            // Account v1
            let locked = u128::deserialize_reader(rd)?;
            let code_hash = CryptoHash::deserialize_reader(rd)?;
            let storage_usage = StorageUsage::deserialize_reader(rd)?;

            Ok(Account {
                amount: sentinel_or_amount,
                locked,
                code_hash,
                storage_usage,
                version: AccountVersion::V1,
                #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
                permanent_storage_bytes: 0,
            })
        }
    }
}

impl BorshSerialize for Account {
    fn serialize<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        let legacy_account = LegacyAccount {
            amount: self.amount(),
            locked: self.locked(),
            code_hash: self.code_hash(),
            storage_usage: self.storage_usage(),
        };

        #[cfg(not(feature = "protocol_feature_nonrefundable_transfer_nep491"))]
        {
            legacy_account.serialize(writer)
        }

        #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
        {
            match self.version {
                // It might be tempting to lazily convert old V1 to V2
                // while serializing. But that would break the borsh assumptions
                // of unique binary representation.
                AccountVersion::V1 => {
                    if self.permanent_storage_bytes > 0 {
                        panic!("Trying to serialize V1 account with permanent_storage_bytes");
                    }
                    legacy_account.serialize(writer)
                }
                AccountVersion::V2 => {
                    let account = AccountV2 {
                        amount: self.amount(),
                        locked: self.locked(),
                        code_hash: self.code_hash(),
                        storage_usage: self.storage_usage(),
                        permanent_storage_bytes: self.permanent_storage_bytes(),
                    };
                    let sentinel = Account::SERIALIZATION_SENTINEL;
                    // For now a constant, but if we need V3 later we can use this
                    // field instead of sentinel magic.
                    let version = 2u8;
                    BorshSerialize::serialize(&sentinel, writer)?;
                    BorshSerialize::serialize(&version, writer)?;
                    account.serialize(writer)
                }
            }
        }
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
    // we accomodate those by using a string, allowing us to read and parse genesis.
    /// The access key only allows transactions with the given receiver's account id.
    pub receiver_id: String,

    /// A list of method names that can be used. The access key only allows transactions with the
    /// function call of one of the given method names.
    /// Empty list means any method name can be used.
    pub method_names: Vec<String>,
}

#[cfg(test)]
mod tests {

    use crate::hash::hash;
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
    fn test_legacy_account_serde_serialization() {
        let old_account = LegacyAccount {
            amount: 1_000_000,
            locked: 1_000_000,
            code_hash: CryptoHash::default(),
            storage_usage: 100,
        };

        let serialized_account = serde_json::to_string(&old_account).unwrap();
        let new_account: Account = serde_json::from_str(&serialized_account).unwrap();
        assert_eq!(new_account.amount(), old_account.amount);
        assert_eq!(new_account.locked(), old_account.locked);
        assert_eq!(new_account.code_hash(), old_account.code_hash);
        assert_eq!(new_account.storage_usage(), old_account.storage_usage);
        assert_eq!(new_account.permanent_storage_bytes(), 0);
        assert_eq!(new_account.version, AccountVersion::V1);

        let new_serialized_account = serde_json::to_string(&new_account).unwrap();
        let deserialized_account: Account = serde_json::from_str(&new_serialized_account).unwrap();
        assert_eq!(deserialized_account, new_account);
    }

    #[test]
    fn test_legacy_account_borsh_serialization() {
        let old_account = LegacyAccount {
            amount: 100,
            locked: 200,
            code_hash: CryptoHash::default(),
            storage_usage: 300,
        };
        let mut old_bytes = &borsh::to_vec(&old_account).unwrap()[..];
        let new_account = <Account as BorshDeserialize>::deserialize(&mut old_bytes).unwrap();

        assert_eq!(new_account.amount(), old_account.amount);
        assert_eq!(new_account.locked(), old_account.locked);
        assert_eq!(new_account.code_hash(), old_account.code_hash);
        assert_eq!(new_account.storage_usage(), old_account.storage_usage);
        assert_eq!(new_account.version, AccountVersion::V1);

        let mut new_bytes = &borsh::to_vec(&new_account).unwrap()[..];
        let deserialized_account =
            <Account as BorshDeserialize>::deserialize(&mut new_bytes).unwrap();
        assert_eq!(deserialized_account, new_account);
    }

    #[test]
    fn test_account_v1_serde_serialization() {
        let account = Account {
            amount: 10_000_000,
            locked: 100_000,
            #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
            permanent_storage_bytes: 0,
            code_hash: CryptoHash::default(),
            storage_usage: 1000,
            version: AccountVersion::V1,
        };
        let serialized_account = serde_json::to_string(&account).unwrap();
        let deserialized_account: Account = serde_json::from_str(&serialized_account).unwrap();
        assert_eq!(deserialized_account, account);
    }

    #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
    /// It is impossible to construct V1 account with permanent_storage_bytes greater than 0.
    /// So the situation in this test is theoretical.
    ///
    /// Serialization of account V1 with permanent_storage_bytes amount greater than 0 would pass without an error,
    /// but an error would be raised on deserialization of such invalid data.
    #[test]
    fn test_account_v1_serde_serialization_invalid_data() {
        let account = Account {
            amount: 10_000_000,
            locked: 100_000,
            permanent_storage_bytes: 1,
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
    fn test_account_v1_borsh_serialization() {
        let account = Account {
            amount: 1_000_000,
            locked: 1_000_000,
            #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
            permanent_storage_bytes: 0,
            code_hash: CryptoHash::default(),
            storage_usage: 100,
            version: AccountVersion::V1,
        };
        let serialized_account = borsh::to_vec(&account).unwrap();
        assert_eq!(
            &hash(&serialized_account).to_string(),
            "EVk5UaxBe8LQ8r8iD5EAxVBs6TJcMDKqyH7PBuho6bBJ"
        );
        let deserialized_account =
            <Account as BorshDeserialize>::deserialize(&mut &serialized_account[..]).unwrap();
        assert_eq!(deserialized_account, account);
    }

    #[cfg(not(feature = "protocol_feature_nonrefundable_transfer_nep491"))]
    #[test]
    #[should_panic(expected = "account serialization sentinel not allowed for AccountV1")]
    fn test_account_v1_borsh_serialization_sentinel() {
        let account = Account {
            amount: Account::SERIALIZATION_SENTINEL,
            locked: 1_000_000,
            code_hash: CryptoHash::default(),
            storage_usage: 100,
            version: AccountVersion::V1,
        };
        let serialized_account = borsh::to_vec(&account).unwrap();
        <Account as BorshDeserialize>::deserialize(&mut &serialized_account[..]).unwrap();
    }

    #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
    /// It is impossible to construct V1 account with permanent_storage_bytes greater than 0.
    /// So the situation in this test is theoretical.
    ///
    /// If a V1 account had permanent_storage_bytes greater than zero, it would panic during Borsh serialization.
    #[test]
    #[should_panic(expected = "Trying to serialize V1 account with permanent_storage_bytes")]
    fn test_account_v1_borsh_serialization_invalid_data() {
        let account = Account {
            amount: 1_000_000,
            locked: 1_000_000,
            permanent_storage_bytes: 1,
            code_hash: CryptoHash::default(),
            storage_usage: 100,
            version: AccountVersion::V1,
        };
        let _ = borsh::to_vec(&account);
    }

    #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
    #[test]
    fn test_account_v2_serde_serialization() {
        let account = Account {
            amount: 10_000_000,
            locked: 100_000,
            permanent_storage_bytes: 37,
            code_hash: CryptoHash::default(),
            storage_usage: 1000,
            version: AccountVersion::V2,
        };
        let serialized_account = serde_json::to_string(&account).unwrap();
        let deserialized_account: Account = serde_json::from_str(&serialized_account).unwrap();
        assert_eq!(deserialized_account, account);
    }

    #[cfg(feature = "protocol_feature_nonrefundable_transfer_nep491")]
    #[test]
    fn test_account_v2_borsh_serialization() {
        let account = Account {
            amount: 1_000_000,
            locked: 1_000_000,
            permanent_storage_bytes: 42,
            code_hash: CryptoHash::default(),
            storage_usage: 100,
            version: AccountVersion::V2,
        };
        let serialized_account = borsh::to_vec(&account).unwrap();
        if cfg!(feature = "protocol_feature_nonrefundable_transfer_nep491") {
            expect_test::expect!("G2Dn8ABMPqsoXuQCPR9yV19HgBi3ZJNqEMEntyLqveDR")
        } else {
            expect_test::expect!("EVk5UaxBe8LQ8r8iD5EAxVBs6TJcMDKqyH7PBuho6bBJ")
        }
        .assert_eq(&hash(&serialized_account).to_string());
        let deserialized_account =
            <Account as BorshDeserialize>::deserialize(&mut &serialized_account[..]).unwrap();
        assert_eq!(deserialized_account, account);
    }
}
