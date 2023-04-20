use crate::hash::CryptoHash;
use crate::serialize::dec_format;
use crate::types::{Balance, Nonce, StorageUsage};
use borsh::{BorshDeserialize, BorshSerialize};
pub use near_account_id as id;
use std::io;

#[derive(
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    Clone,
    Copy,
    Debug,
    Default,
    serde::Serialize,
    serde::Deserialize,
)]
pub enum AccountVersion {
    #[default]
    V1,
}

/// Per account information stored in the state.
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct Account {
    /// The total not locked tokens.
    #[serde(with = "dec_format")]
    amount: Balance,
    /// The amount locked due to staking.
    #[serde(with = "dec_format")]
    locked: Balance,
    /// Hash of the code stored in the storage for this account.
    code_hash: CryptoHash,
    /// Storage used by the given account, includes account id, this struct, access keys and other data.
    storage_usage: StorageUsage,
    /// Version of Account in re migrations and similar
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
    ) -> Self {
        Account { amount, locked, code_hash, storage_usage, version: AccountVersion::V1 }
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

    pub fn set_version(&mut self, version: AccountVersion) {
        self.version = version;
    }
}

#[derive(BorshSerialize, BorshDeserialize)]
struct LegacyAccount {
    amount: Balance,
    locked: Balance,
    code_hash: CryptoHash,
    storage_usage: StorageUsage,
}

impl BorshDeserialize for Account {
    fn deserialize_reader<R: io::Read>(rd: &mut R) -> io::Result<Self> {
        // This should only ever happen if we have pre-transition account serialized in state
        // See test_account_size
        let deserialized_account = LegacyAccount::deserialize_reader(rd)?;
        Ok(Account {
            amount: deserialized_account.amount,
            locked: deserialized_account.locked,
            code_hash: deserialized_account.code_hash,
            storage_usage: deserialized_account.storage_usage,
            version: AccountVersion::V1,
        })
    }
}

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
    use borsh::BorshSerialize;

    use crate::hash::hash;

    use super::*;

    #[test]
    fn test_account_serialization() {
        let acc = Account::new(1_000_000, 1_000_000, CryptoHash::default(), 100);
        let bytes = acc.try_to_vec().unwrap();
        assert_eq!(hash(&bytes).to_string(), "EVk5UaxBe8LQ8r8iD5EAxVBs6TJcMDKqyH7PBuho6bBJ");
    }

    #[test]
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
