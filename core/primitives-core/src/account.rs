use crate::hash::CryptoHash;
use crate::types::{Balance, Nonce, NonceIndex, StorageUsage};
use borsh::{BorshDeserialize, BorshSerialize};
pub use near_account_id as id;
use near_account_id::AccountId;
use near_schema_checker_lib::ProtocolSchema;
use std::borrow::Cow;
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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum AccountVersion {
    #[default]
    V1,
    V2,
}

/// Whether an account's state has been installed.
///
/// Only universal accounts can be uninitialized: they come into existence when
/// a transfer funds a `0u` id whose state init has not been applied yet. A
/// deterministic `0s` account waiting for its state init is an ordinary V1
/// account with no contract, not this.
#[derive(
    PartialEq, Eq, Clone, Copy, Debug, Default, serde::Serialize, serde::Deserialize, ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum AccountState {
    #[default]
    Initialized,
    Uninitialized,
}

impl AccountState {
    fn is_initialized(&self) -> bool {
        match self {
            Self::Initialized => true,
            Self::Uninitialized => false,
        }
    }
}

/// Error returned when a change does not fit the account's [`AccountState`].
///
/// Unreachable on a healthy chain: an uninitialized account has no access keys,
/// so no receipt can name one as its actor. Seeing this means corrupted state
/// or a bug, not anything a user did.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum InvalidAccountState {
    #[error("account state has not been installed yet")]
    Uninitialized,
    #[error("account state has already been installed")]
    AlreadyInitialized,
}

/// Per account information stored in the state.
///
/// Two independent axes: whether the account's state has been installed, and,
/// for an initialized account, the version of its storage layout.
/// When introducing new version:
/// - introduce new AccountV[NewVersion] struct
/// - add new InitializedAccount enum option V[NewVersion](AccountV[NewVersion])
/// - add new BorshVersionedAccount enum option V[NewVersion](AccountV[NewVersion])
/// - update SerdeAccount with newly added fields
/// - update serde ser/deser to properly handle conversions
#[derive(PartialEq, Eq, Debug, Clone, ProtocolSchema)]
pub enum Account {
    Uninitialized(UninitializedAccountV1),
    Initialized(InitializedAccount),
}

/// An account with its state in place. It can hold a contract, access keys and
/// contract data, and it can stake.
#[derive(PartialEq, Eq, Debug, Clone, ProtocolSchema)]
pub enum InitializedAccount {
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

#[allow(dead_code)]
impl AccountV1 {
    fn to_v2(&self) -> AccountV2 {
        AccountV2 {
            amount: self.amount,
            locked: self.locked,
            storage_usage: self.storage_usage,
            contract: AccountContract::from_local_code_hash(self.code_hash),
        }
    }
}

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
pub enum AccountContract {
    None,
    Local(CryptoHash),
    Global(CryptoHash),
    GlobalByAccount(AccountId),
}

impl AccountContract {
    pub fn local_code(&self) -> Option<CryptoHash> {
        match self {
            AccountContract::None
            | AccountContract::GlobalByAccount(_)
            | AccountContract::Global(_) => None,
            AccountContract::Local(hash) => Some(*hash),
        }
    }

    pub fn from_local_code_hash(code_hash: CryptoHash) -> AccountContract {
        if code_hash == CryptoHash::default() {
            AccountContract::None
        } else {
            AccountContract::Local(code_hash)
        }
    }

    pub fn is_none(&self) -> bool {
        match self {
            Self::None => true,
            Self::Local(_) | Self::Global(_) | Self::GlobalByAccount(_) => false,
        }
    }

    pub fn is_some(&self) -> bool {
        !self.is_none()
    }

    pub fn is_local(&self) -> bool {
        match self {
            Self::Local(_) => true,
            Self::None | Self::Global(_) | Self::GlobalByAccount(_) => false,
        }
    }

    pub fn identifier_storage_usage(&self) -> u64 {
        match self {
            AccountContract::None | AccountContract::Local(_) => 0u64,
            AccountContract::Global(_) => 32u64,
            AccountContract::GlobalByAccount(id) => id.len() as u64,
        }
    }
}

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
    /// Storage used by the given account, includes account id, this struct, access keys and other data.
    storage_usage: StorageUsage,
    /// Type of contract deployed to this account, if any.
    contract: AccountContract,
}

/// A universal account funded before its state init was installed.
///
/// It carries nothing but balance: no contract, no access keys and no data.
/// Installing the state init is the only thing that can add any of those, and
/// doing so moves the account out of this state, so everything else that writes
/// to an account is unreachable while it stays uninitialized.
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Debug, Clone, ProtocolSchema)]
pub struct UninitializedAccountV1 {
    /// The total not locked tokens.
    amount: Balance,
    /// Storage used by the account record itself.
    storage_usage: StorageUsage,
}

impl Account {
    /// Max number of bytes an account can have in its state (excluding contract code)
    /// before it is infeasible to delete.
    pub const MAX_ACCOUNT_DELETION_STORAGE_USAGE: u64 = 10_000;
    /// HACK: Using u128::MAX as a sentinel value, there are not enough tokens
    /// in total supply which makes it an invalid value. We use it to
    /// differentiate AccountVersion V1 from newer versions.
    const SERIALIZATION_SENTINEL: Balance = Balance::MAX;

    pub fn new(
        amount: Balance,
        locked: Balance,
        contract: AccountContract,
        storage_usage: StorageUsage,
    ) -> Self {
        let account = match contract {
            AccountContract::None => InitializedAccount::V1(AccountV1 {
                amount,
                locked,
                code_hash: CryptoHash::default(),
                storage_usage,
            }),
            AccountContract::Local(code_hash) => {
                InitializedAccount::V1(AccountV1 { amount, locked, code_hash, storage_usage })
            }
            AccountContract::Global(_) | AccountContract::GlobalByAccount(_) => {
                InitializedAccount::V2(AccountV2 { amount, locked, storage_usage, contract })
            }
        };
        Self::Initialized(account)
    }

    /// A universal account funded before its state init was installed. Holds
    /// nothing but balance until [`Self::initialize`] is called.
    pub fn new_uninitialized(amount: Balance, storage_usage: StorageUsage) -> Self {
        Self::Uninitialized(UninitializedAccountV1 { amount, storage_usage })
    }

    /// Whether the account's state has been installed.
    ///
    /// This is about the [`Account::Uninitialized`] representation, which only
    /// universal accounts use. A deterministic `0s` account still waiting for
    /// its state init is an ordinary V1 account with no contract, so this
    /// returns `true` for it.
    #[inline]
    pub fn is_initialized(&self) -> bool {
        match self {
            Self::Initialized(_) => true,
            Self::Uninitialized(_) => false,
        }
    }

    /// Move an uninitialized account to the initialized state, keeping its
    /// balance and storage usage. The caller then installs the state itself.
    pub fn initialize(&mut self) -> Result<(), InvalidAccountState> {
        let Self::Uninitialized(account) = self else {
            return Err(InvalidAccountState::AlreadyInitialized);
        };
        *self = Self::Initialized(InitializedAccount::V1(AccountV1 {
            amount: account.amount,
            locked: Balance::ZERO,
            code_hash: CryptoHash::default(),
            storage_usage: account.storage_usage,
        }));
        Ok(())
    }

    #[inline]
    fn state(&self) -> AccountState {
        match self {
            Self::Uninitialized(_) => AccountState::Uninitialized,
            Self::Initialized(_) => AccountState::Initialized,
        }
    }

    #[inline]
    pub fn amount(&self) -> Balance {
        match self {
            Self::Uninitialized(account) => account.amount,
            Self::Initialized(account) => account.amount(),
        }
    }

    /// Always zero while uninitialized: nothing can stake on the account's
    /// behalf before its state init is installed.
    #[inline]
    pub fn locked(&self) -> Balance {
        match self {
            Self::Uninitialized(_) => Balance::ZERO,
            Self::Initialized(account) => account.locked(),
        }
    }

    #[inline]
    pub fn contract(&self) -> Cow<'_, AccountContract> {
        // NOTE: Returning `AccountContract::None` for uninitialized accounts is
        // a conscious choice. It's almost always treated by the callers exactly
        // the same way as an initialized account with no contract.
        match self {
            Self::Uninitialized(_) => Cow::Owned(AccountContract::None),
            Self::Initialized(account) => account.contract(),
        }
    }

    #[inline]
    pub fn storage_usage(&self) -> StorageUsage {
        match self {
            Self::Uninitialized(account) => account.storage_usage,
            Self::Initialized(account) => account.storage_usage(),
        }
    }

    /// Layout version within the account's [`AccountState`].
    #[inline]
    pub fn version(&self) -> AccountVersion {
        match self {
            Self::Uninitialized(_) => AccountVersion::V1,
            Self::Initialized(account) => account.version(),
        }
    }

    #[inline]
    pub fn global_contract_hash(&self) -> Option<CryptoHash> {
        match self {
            Self::Uninitialized(_) => None,
            Self::Initialized(account) => account.global_contract_hash(),
        }
    }

    #[inline]
    pub fn global_contract_account_id(&self) -> Option<&AccountId> {
        match self {
            Self::Uninitialized(_) => None,
            Self::Initialized(account) => account.global_contract_account_id(),
        }
    }

    #[inline]
    pub fn local_contract_hash(&self) -> Option<CryptoHash> {
        match self {
            Self::Uninitialized(_) => None,
            Self::Initialized(account) => account.local_contract_hash(),
        }
    }

    #[inline]
    pub fn set_amount(&mut self, amount: Balance) {
        match self {
            Self::Uninitialized(account) => account.amount = amount,
            Self::Initialized(account) => account.set_amount(amount),
        }
    }

    /// Fails on an uninitialized account, which can never have locked balance.
    #[inline]
    pub fn set_locked(&mut self, locked: Balance) -> Result<(), InvalidAccountState> {
        match self {
            Self::Uninitialized(_) => Err(InvalidAccountState::Uninitialized),
            Self::Initialized(account) => {
                account.set_locked(locked);
                Ok(())
            }
        }
    }

    /// Fails on an uninitialized account. Call [`Self::initialize`] first.
    #[inline]
    pub fn set_contract(&mut self, contract: AccountContract) -> Result<(), InvalidAccountState> {
        match self {
            Self::Uninitialized(_) => Err(InvalidAccountState::Uninitialized),
            Self::Initialized(account) => {
                account.set_contract(contract);
                Ok(())
            }
        }
    }

    #[inline]
    pub fn set_storage_usage(&mut self, storage_usage: StorageUsage) {
        match self {
            Self::Uninitialized(account) => account.storage_usage = storage_usage,
            Self::Initialized(account) => account.set_storage_usage(storage_usage),
        }
    }
}

impl InitializedAccount {
    #[inline]
    fn amount(&self) -> Balance {
        match self {
            Self::V1(account) => account.amount,
            Self::V2(account) => account.amount,
        }
    }

    #[inline]
    fn locked(&self) -> Balance {
        match self {
            Self::V1(account) => account.locked,
            Self::V2(account) => account.locked,
        }
    }

    #[inline]
    fn contract(&self) -> Cow<'_, AccountContract> {
        match self {
            Self::V1(account) => {
                Cow::Owned(AccountContract::from_local_code_hash(account.code_hash))
            }
            Self::V2(account) => Cow::Borrowed(&account.contract),
        }
    }

    #[inline]
    fn storage_usage(&self) -> StorageUsage {
        match self {
            Self::V1(account) => account.storage_usage,
            Self::V2(account) => account.storage_usage,
        }
    }

    #[inline]
    fn version(&self) -> AccountVersion {
        match self {
            Self::V1(_) => AccountVersion::V1,
            Self::V2(_) => AccountVersion::V2,
        }
    }

    #[inline]
    fn global_contract_hash(&self) -> Option<CryptoHash> {
        match self {
            Self::V2(AccountV2 { contract: AccountContract::Global(hash), .. }) => Some(*hash),
            Self::V1(_) | Self::V2(_) => None,
        }
    }

    #[inline]
    fn global_contract_account_id(&self) -> Option<&AccountId> {
        match self {
            Self::V2(AccountV2 { contract: AccountContract::GlobalByAccount(account), .. }) => {
                Some(account)
            }
            Self::V1(_) | Self::V2(_) => None,
        }
    }

    #[inline]
    fn local_contract_hash(&self) -> Option<CryptoHash> {
        match self {
            Self::V1(account) => {
                AccountContract::from_local_code_hash(account.code_hash).local_code()
            }
            Self::V2(AccountV2 { contract: AccountContract::Local(hash), .. }) => Some(*hash),
            Self::V2(AccountV2 { contract: AccountContract::None, .. })
            | Self::V2(AccountV2 { contract: AccountContract::Global(_), .. })
            | Self::V2(AccountV2 { contract: AccountContract::GlobalByAccount(_), .. }) => None,
        }
    }

    #[inline]
    fn set_amount(&mut self, amount: Balance) {
        match self {
            Self::V1(account) => account.amount = amount,
            Self::V2(account) => account.amount = amount,
        }
    }

    #[inline]
    fn set_locked(&mut self, locked: Balance) {
        match self {
            Self::V1(account) => account.locked = locked,
            Self::V2(account) => account.locked = locked,
        }
    }

    #[inline]
    fn set_contract(&mut self, contract: AccountContract) {
        match self {
            Self::V1(account) => match contract {
                AccountContract::None | AccountContract::Local(_) => {
                    account.code_hash = contract.local_code().unwrap_or_default();
                }
                AccountContract::Global(_) | AccountContract::GlobalByAccount(_) => {
                    let mut account_v2 = account.to_v2();
                    account_v2.contract = contract;
                    *self = Self::V2(account_v2);
                }
            },
            Self::V2(account) => {
                account.contract = contract;
            }
        }
    }

    #[inline]
    fn set_storage_usage(&mut self, storage_usage: StorageUsage) {
        match self {
            Self::V1(account) => account.storage_usage = storage_usage,
            Self::V2(account) => account.storage_usage = storage_usage,
        }
    }
}

/// Account representation for serde ser/deser that maintains both backward
/// and forward compatibility.
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug, Clone, ProtocolSchema)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
struct SerdeAccount {
    amount: Balance,
    locked: Balance,
    code_hash: CryptoHash,
    storage_usage: StorageUsage,
    /// Account storage layout version, used for migrations and similar.
    /// Scoped to `state`.
    #[serde(default)]
    version: AccountVersion,
    /// Whether the account's state has been installed. Absent from documents
    /// written before universal accounts existed, which are all initialized.
    #[serde(default, skip_serializing_if = "AccountState::is_initialized")]
    state: AccountState,
    /// Global contracts fields
    #[serde(default, skip_serializing_if = "Option::is_none")]
    global_contract_hash: Option<CryptoHash>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    global_contract_account_id: Option<AccountId>,
}

impl<'de> serde::Deserialize<'de> for Account {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let account_data = SerdeAccount::deserialize(deserializer)?;
        if account_data.code_hash != CryptoHash::default()
            && (account_data.global_contract_hash.is_some()
                || account_data.global_contract_account_id.is_some())
        {
            return Err(serde::de::Error::custom(
                "An Account can't contain both a local and global contract",
            ));
        }
        if account_data.global_contract_hash.is_some()
            && account_data.global_contract_account_id.is_some()
        {
            return Err(serde::de::Error::custom(
                "An Account can't contain both types of global contracts",
            ));
        }

        if !account_data.state.is_initialized() {
            return uninitialized_account_from_serde(account_data)
                .map_err(serde::de::Error::custom);
        }

        match account_data.version {
            AccountVersion::V1 => Ok(Account::Initialized(InitializedAccount::V1(AccountV1 {
                amount: account_data.amount,
                locked: account_data.locked,
                code_hash: account_data.code_hash,
                storage_usage: account_data.storage_usage,
            }))),
            AccountVersion::V2 => {
                let contract = match account_data.global_contract_account_id {
                    Some(account_id) => AccountContract::GlobalByAccount(account_id),
                    None => match account_data.global_contract_hash {
                        Some(hash) => AccountContract::Global(hash),
                        None => AccountContract::from_local_code_hash(account_data.code_hash),
                    },
                };

                Ok(Account::Initialized(InitializedAccount::V2(AccountV2 {
                    amount: account_data.amount,
                    locked: account_data.locked,
                    storage_usage: account_data.storage_usage,
                    contract,
                })))
            }
        }
    }
}

/// Rebuild an uninitialized account, rejecting the fields it can never carry.
fn uninitialized_account_from_serde(account: SerdeAccount) -> Result<Account, &'static str> {
    if account.version != AccountVersion::V1 {
        return Err("an uninitialized account must be version V1");
    }
    if account.locked != Balance::ZERO {
        return Err("an uninitialized account can't have locked balance");
    }
    if account.code_hash != CryptoHash::default()
        || account.global_contract_hash.is_some()
        || account.global_contract_account_id.is_some()
    {
        return Err("an uninitialized account can't have a contract");
    }
    Ok(Account::new_uninitialized(account.amount, account.storage_usage))
}

impl serde::Serialize for Account {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let version = self.version();
        let code_hash = self.local_contract_hash().unwrap_or_default();
        let repr = SerdeAccount {
            amount: self.amount(),
            locked: self.locked(),
            code_hash,
            storage_usage: self.storage_usage(),
            version,
            state: self.state(),
            global_contract_hash: self.global_contract_hash(),
            global_contract_account_id: self.global_contract_account_id().cloned(),
        };
        repr.serialize(serializer)
    }
}

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for Account {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Account".to_string().into()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        SerdeAccount::json_schema(generator)
    }
}

#[derive(BorshSerialize, BorshDeserialize)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
enum BorshVersionedAccount {
    // V1 is not included since it is serialized directly without being wrapped in enum
    V2(AccountV2) = 0,
    Uninitialized(UninitializedAccountV1) = 1,
}

impl BorshDeserialize for Account {
    fn deserialize_reader<R: io::Read>(rd: &mut R) -> io::Result<Self> {
        // The first value of all Account serialization formats is a u128,
        // either a sentinel or a balance.
        let sentinel_or_amount = Balance::deserialize_reader(rd)?;
        if sentinel_or_amount == Account::SERIALIZATION_SENTINEL {
            let versioned_account = BorshVersionedAccount::deserialize_reader(rd)?;
            let account = match versioned_account {
                BorshVersionedAccount::V2(account_v2) => {
                    Account::Initialized(InitializedAccount::V2(account_v2))
                }
                BorshVersionedAccount::Uninitialized(account) => Account::Uninitialized(account),
            };
            Ok(account)
        } else {
            // Legacy unversioned representation of Account
            let locked = Balance::deserialize_reader(rd)?;
            let code_hash = CryptoHash::deserialize_reader(rd)?;
            let storage_usage = StorageUsage::deserialize_reader(rd)?;

            let account_v1 =
                AccountV1 { amount: sentinel_or_amount, locked, code_hash, storage_usage };
            Ok(Account::Initialized(InitializedAccount::V1(account_v1)))
        }
    }
}

impl BorshSerialize for Account {
    fn serialize<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        let versioned_account = match self {
            Account::Initialized(InitializedAccount::V1(account_v1)) => {
                return account_v1.serialize(writer);
            }
            Account::Initialized(InitializedAccount::V2(account_v2)) => {
                BorshVersionedAccount::V2(account_v2.clone())
            }
            Account::Uninitialized(account) => {
                BorshVersionedAccount::Uninitialized(account.clone())
            }
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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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

    /// Borsh-serialized size of a Nonce value (u64), stored as gas key nonce trie values.
    pub const NONCE_VALUE_LEN: usize = std::mem::size_of::<Nonce>();

    /// Minimum borsh-serialized size of an AccessKey with a gas key permission.
    /// This is the size for GasKeyFullAccess (the smallest gas key variant).
    pub fn min_gas_key_borsh_len() -> usize {
        borsh::object_length(&Self::gas_key_full_access(0)).unwrap()
    }

    pub fn full_access() -> Self {
        Self { nonce: 0, permission: AccessKeyPermission::FullAccess }
    }

    pub fn gas_key_full_access(num_nonces: NonceIndex) -> Self {
        Self {
            nonce: 0,
            permission: AccessKeyPermission::GasKeyFullAccess(GasKeyInfo {
                balance: Balance::from_yoctonear(0),
                num_nonces,
            }),
        }
    }

    pub fn gas_key_function_call(
        num_nonces: NonceIndex,
        function_call_permission: FunctionCallPermission,
    ) -> Self {
        Self {
            nonce: 0,
            permission: AccessKeyPermission::GasKeyFunctionCall(
                GasKeyInfo { balance: Balance::from_yoctonear(0), num_nonces },
                function_call_permission,
            ),
        }
    }

    pub fn gas_key_info(&self) -> Option<&GasKeyInfo> {
        match &self.permission {
            AccessKeyPermission::GasKeyFunctionCall(gas_key_info, _)
            | AccessKeyPermission::GasKeyFullAccess(gas_key_info) => Some(gas_key_info),
            AccessKeyPermission::FunctionCall(_) | AccessKeyPermission::FullAccess => None,
        }
    }

    pub fn gas_key_info_mut(&mut self) -> Option<&mut GasKeyInfo> {
        match &mut self.permission {
            AccessKeyPermission::GasKeyFunctionCall(gas_key_info, _)
            | AccessKeyPermission::GasKeyFullAccess(gas_key_info) => Some(gas_key_info),
            AccessKeyPermission::FunctionCall(_) | AccessKeyPermission::FullAccess => None,
        }
    }
}

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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct GasKeyInfo {
    pub balance: Balance,
    pub num_nonces: NonceIndex,
}

impl GasKeyInfo {
    /// Maximum gas key balance that can be burned during key or account deletion.
    /// Deletion fails if the (sum of) gas key balance(s) exceeds this threshold.
    pub const MAX_BALANCE_TO_BURN: Balance = Balance::from_near(1);

    pub fn borsh_len() -> usize {
        borsh::object_length(&Self { balance: Balance::from_yoctonear(0), num_nonces: 0 }).unwrap()
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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum AccessKeyPermission {
    FunctionCall(FunctionCallPermission),
    /// Grants full access to the account.
    /// NOTE: It's used to replace account-level public keys.
    FullAccess,
    /// Gas key with limited permission to make transactions with FunctionCallActions
    /// Gas keys are a kind of access keys with a prepaid balance to pay for gas.
    GasKeyFunctionCall(GasKeyInfo, FunctionCallPermission),
    /// Gas key with full access to the account.
    /// Gas keys are a kind of access keys with a prepaid balance to pay for gas.
    GasKeyFullAccess(GasKeyInfo),
}

impl AccessKeyPermission {
    pub const MAX_NONCES_FOR_GAS_KEY: NonceIndex = 1024;

    pub fn function_call_permission(&self) -> Option<&FunctionCallPermission> {
        match self {
            AccessKeyPermission::FunctionCall(permission)
            | AccessKeyPermission::GasKeyFunctionCall(_, permission) => Some(permission),
            AccessKeyPermission::FullAccess | AccessKeyPermission::GasKeyFullAccess(_) => None,
        }
    }

    pub fn function_call_permission_mut(&mut self) -> Option<&mut FunctionCallPermission> {
        match self {
            AccessKeyPermission::FunctionCall(permission)
            | AccessKeyPermission::GasKeyFunctionCall(_, permission) => Some(permission),
            AccessKeyPermission::FullAccess | AccessKeyPermission::GasKeyFullAccess(_) => None,
        }
    }
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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct FunctionCallPermission {
    /// Allowance is a balance limit to use by this access key to pay for function call gas and
    /// transaction fees. When this access key is used, both account balance and the allowance is
    /// decreased by the same value.
    /// `None` means unlimited allowance.
    /// NOTE: To change or increase the allowance, the old access key needs to be deleted and a new
    /// access key should be created.
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
    use super::*;
    use expect_test::expect;

    fn initialized_v1(account: AccountV1) -> Account {
        Account::Initialized(InitializedAccount::V1(account))
    }

    fn initialized_v2(account: AccountV2) -> Account {
        Account::Initialized(InitializedAccount::V2(account))
    }

    fn create_serde_account(
        code_hash: CryptoHash,
        global_contract_hash: Option<CryptoHash>,
        global_contract_account_id: Option<AccountId>,
    ) -> SerdeAccount {
        SerdeAccount {
            amount: Balance::from_yoctonear(10_000_000),
            locked: Balance::from_yoctonear(100_000),
            code_hash,
            storage_usage: 1000,
            version: AccountVersion::V2,
            state: AccountState::Initialized,
            global_contract_hash,
            global_contract_account_id,
        }
    }

    #[test]
    fn test_v1_account_serde_serialization() {
        let old_account = AccountV1 {
            amount: Balance::from_yoctonear(1_000_000),
            locked: Balance::from_yoctonear(1_000_000),
            code_hash: CryptoHash::hash_bytes(&[42]),
            storage_usage: 100,
        };

        let serialized_account = serde_json::to_string(&old_account).unwrap();
        let expected_serde_repr = SerdeAccount {
            amount: old_account.amount,
            locked: old_account.locked,
            code_hash: old_account.code_hash,
            storage_usage: old_account.storage_usage,
            version: AccountVersion::V1,
            state: AccountState::Initialized,
            global_contract_hash: None,
            global_contract_account_id: None,
        };
        let actual_serde_repr: SerdeAccount = serde_json::from_str(&serialized_account).unwrap();
        assert_eq!(actual_serde_repr, expected_serde_repr);

        let new_account: Account = serde_json::from_str(&serialized_account).unwrap();
        assert_eq!(new_account, initialized_v1(old_account));

        let new_serialized_account = serde_json::to_string(&new_account).unwrap();
        let deserialized_account: Account = serde_json::from_str(&new_serialized_account).unwrap();
        assert_eq!(deserialized_account, new_account);
    }

    #[test]
    fn test_v1_account_borsh_serialization() {
        let old_account = AccountV1 {
            amount: Balance::from_yoctonear(100),
            locked: Balance::from_yoctonear(200),
            code_hash: CryptoHash::hash_bytes(&[42]),
            storage_usage: 300,
        };
        let old_bytes = borsh::to_vec(&old_account).unwrap();
        let new_account = <Account as BorshDeserialize>::deserialize(&mut &old_bytes[..]).unwrap();
        assert_eq!(new_account, initialized_v1(old_account));

        let new_bytes = borsh::to_vec(&new_account).unwrap();
        assert_eq!(new_bytes, old_bytes);
        let deserialized_account =
            <Account as BorshDeserialize>::deserialize(&mut &new_bytes[..]).unwrap();
        assert_eq!(deserialized_account, new_account);
    }

    #[test]
    fn test_account_v2_serde_serialization() {
        let account_v2 = AccountV2 {
            amount: Balance::from_yoctonear(10_000_000),
            locked: Balance::from_yoctonear(100_000),
            storage_usage: 1000,
            contract: AccountContract::Local(CryptoHash::hash_bytes(&[42])),
        };
        let account = initialized_v2(account_v2.clone());

        let serialized_account = serde_json::to_string(&account).unwrap();
        let expected_serde_repr = SerdeAccount {
            amount: account_v2.amount,
            locked: account_v2.locked,
            code_hash: account_v2.contract.local_code().unwrap_or_default(),
            storage_usage: account_v2.storage_usage,
            version: AccountVersion::V2,
            state: AccountState::Initialized,
            global_contract_hash: None,
            global_contract_account_id: None,
        };
        let actual_serde_repr: SerdeAccount = serde_json::from_str(&serialized_account).unwrap();
        assert_eq!(actual_serde_repr, expected_serde_repr);

        let deserialized_account: Account = serde_json::from_str(&serialized_account).unwrap();
        assert_eq!(deserialized_account, account);
    }

    #[test]
    fn test_account_v2_borsh_serialization() {
        let account_v2 = AccountV2 {
            amount: Balance::from_yoctonear(10_000_000),
            locked: Balance::from_yoctonear(100_000),
            storage_usage: 1000,
            contract: AccountContract::Global(CryptoHash::hash_bytes(&[42])),
        };
        let account = initialized_v2(account_v2);
        let serialized_account = borsh::to_vec(&account).unwrap();
        let deserialized_account =
            <Account as BorshDeserialize>::deserialize(&mut &serialized_account[..]).unwrap();
        assert_eq!(deserialized_account, account);
    }

    #[test]
    fn test_account_v2_serde_deserialization_fails_with_local_hash_and_global_account_id() {
        let id = AccountId::try_from("test.near".to_string()).unwrap();
        let code_hash = CryptoHash::hash_bytes(&[42]);

        let serde_repr = create_serde_account(code_hash, None, Some(id));

        let serde_string = serde_json::to_string(&serde_repr).unwrap();
        let deserialization_attempt: Result<Account, _> = serde_json::from_str(&serde_string);
        assert!(deserialization_attempt.is_err());
    }

    #[test]
    fn test_account_v2_serde_deserialization_fails_with_local_and_global_hashes() {
        let code_hash = CryptoHash::hash_bytes(&[42]);

        let serde_repr = create_serde_account(code_hash, Some(code_hash), None);

        let serde_string = serde_json::to_string(&serde_repr).unwrap();
        let deserialization_attempt: Result<Account, _> = serde_json::from_str(&serde_string);
        assert!(deserialization_attempt.is_err());
    }

    #[test]
    fn test_account_v2_serde_deserialization_fails_if_both_types_of_global_contract_are_present() {
        let id = AccountId::try_from("test.near".to_string()).unwrap();
        let serde_repr = create_serde_account(
            CryptoHash::default(),
            Some(CryptoHash::hash_bytes(&[42])),
            Some(id),
        );

        let serde_string = serde_json::to_string(&serde_repr).unwrap();
        let deserialization_attempt: Result<Account, _> = serde_json::from_str(&serde_string);
        assert!(deserialization_attempt.is_err());
    }

    #[test]
    fn test_account_version_upgrade_behaviour() {
        let account_v1 = AccountV1 {
            amount: Balance::from_yoctonear(100),
            locked: Balance::from_yoctonear(200),
            code_hash: CryptoHash::hash_bytes(&[42]),
            storage_usage: 300,
        };
        let mut account = initialized_v1(account_v1);
        let contract = AccountContract::Local(CryptoHash::hash_bytes(&[42]));
        account.set_contract(contract).unwrap();
        assert!(matches!(account, Account::Initialized(InitializedAccount::V1(_))));

        let contract = AccountContract::None;
        account.set_contract(contract).unwrap();
        assert!(matches!(account, Account::Initialized(InitializedAccount::V1(_))));

        let contract = AccountContract::Global(CryptoHash::hash_bytes(&[42]));
        account.set_contract(contract).unwrap();
        assert!(matches!(account, Account::Initialized(InitializedAccount::V2(_))));
    }

    fn borsh_hex(account: &Account) -> String {
        borsh::to_vec(account).unwrap().iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn uninitialized_serde_account() -> SerdeAccount {
        SerdeAccount {
            amount: Balance::from_yoctonear(100),
            locked: Balance::ZERO,
            code_hash: CryptoHash::default(),
            storage_usage: 300,
            version: AccountVersion::V1,
            state: AccountState::Uninitialized,
            global_contract_hash: None,
            global_contract_account_id: None,
        }
    }

    fn deserialize_account(repr: &SerdeAccount) -> Result<Account, serde_json::Error> {
        serde_json::from_str(&serde_json::to_string(repr).unwrap())
    }

    /// Pins the on-wire encoding of every representation, so a change to the
    /// `Account` shape that also moves the bytes can't pass unnoticed.
    #[test]
    fn borsh_encoding_is_stable() {
        let v1 = initialized_v1(AccountV1 {
            amount: Balance::from_yoctonear(100),
            locked: Balance::from_yoctonear(200),
            code_hash: CryptoHash::hash_bytes(&[42]),
            storage_usage: 300,
        });
        expect!["64000000000000000000000000000000c8000000000000000000000000000000684888c0ebb17f374298b65ee2807526c066094c701bcc7ebbe1c1095f494fc12c01000000000000"].assert_eq(&borsh_hex(&v1));

        let v2 = initialized_v2(AccountV2 {
            amount: Balance::from_yoctonear(100),
            locked: Balance::from_yoctonear(200),
            storage_usage: 300,
            contract: AccountContract::Global(CryptoHash::hash_bytes(&[42])),
        });
        expect!["ffffffffffffffffffffffffffffffff0064000000000000000000000000000000c80000000000000000000000000000002c0100000000000002684888c0ebb17f374298b65ee2807526c066094c701bcc7ebbe1c1095f494fc1"].assert_eq(&borsh_hex(&v2));

        let uninitialized = Account::new_uninitialized(Balance::from_yoctonear(100), 300);
        expect![
            "ffffffffffffffffffffffffffffffff01640000000000000000000000000000002c01000000000000"
        ]
        .assert_eq(&borsh_hex(&uninitialized));
    }

    #[test]
    fn uninitialized_account_accessors() {
        let account = Account::new_uninitialized(Balance::from_yoctonear(100), 300);

        assert!(!account.is_initialized());
        assert_eq!(account.amount(), Balance::from_yoctonear(100));
        assert_eq!(account.locked(), Balance::ZERO);
        assert_eq!(account.storage_usage(), 300);
        assert_eq!(account.version(), AccountVersion::V1);
        assert!(account.contract().is_none());
        assert_eq!(account.local_contract_hash(), None);
        assert_eq!(account.global_contract_hash(), None);
        assert_eq!(account.global_contract_account_id(), None);
    }

    #[test]
    fn initialize_keeps_balance_and_storage_usage() {
        let mut account = Account::new_uninitialized(Balance::from_yoctonear(100), 300);
        // A transfer can credit the account while it is still uninitialized.
        account.set_amount(Balance::from_yoctonear(500));

        account.initialize().unwrap();

        assert!(account.is_initialized());
        assert_eq!(account.amount(), Balance::from_yoctonear(500));
        assert_eq!(account.locked(), Balance::ZERO);
        assert_eq!(account.storage_usage(), 300);
        assert_eq!(account.version(), AccountVersion::V1);
        assert!(account.contract().is_none());
    }

    #[test]
    fn initialize_twice_fails() {
        let mut account = Account::new_uninitialized(Balance::from_yoctonear(100), 300);
        assert_eq!(account.initialize(), Ok(()));
        assert_eq!(account.initialize(), Err(InvalidAccountState::AlreadyInitialized));
    }

    #[test]
    fn uninitialized_account_rejects_locked_and_contract() {
        let mut account = Account::new_uninitialized(Balance::from_yoctonear(100), 300);
        let contract = AccountContract::Local(CryptoHash::hash_bytes(&[42]));

        assert_eq!(
            account.set_locked(Balance::from_yoctonear(1)),
            Err(InvalidAccountState::Uninitialized)
        );
        assert_eq!(account.set_contract(contract), Err(InvalidAccountState::Uninitialized));
    }

    #[test]
    fn initialized_account_accepts_locked_and_contract() {
        let mut account = Account::new_uninitialized(Balance::from_yoctonear(100), 300);
        let contract = AccountContract::Local(CryptoHash::hash_bytes(&[42]));

        account.initialize().unwrap();
        account.set_locked(Balance::from_yoctonear(7)).unwrap();
        account.set_contract(contract.clone()).unwrap();

        assert_eq!(account.locked(), Balance::from_yoctonear(7));
        assert_eq!(account.contract().as_ref(), &contract);
    }

    #[test]
    fn uninitialized_account_borsh_serialization() {
        let account = Account::new_uninitialized(Balance::from_yoctonear(100), 300);

        let bytes = borsh::to_vec(&account).unwrap();
        let deserialized = <Account as BorshDeserialize>::deserialize(&mut &bytes[..]).unwrap();
        assert_eq!(deserialized, account);
    }

    /// An unknown wrapper discriminant must fail loudly rather than decode as
    /// something else. This is what stops an older binary from misreading an
    /// account variant it does not know about.
    #[test]
    fn borsh_deserialization_rejects_unknown_discriminant() {
        let account = Account::new_uninitialized(Balance::from_yoctonear(100), 300);
        let mut bytes = borsh::to_vec(&account).unwrap();

        // The discriminant sits right after the sentinel.
        let sentinel_len = borsh::to_vec(&Account::SERIALIZATION_SENTINEL).unwrap().len();
        assert_eq!(bytes[sentinel_len], 1, "uninitialized accounts are discriminant 1");
        bytes[sentinel_len] = 2;

        assert!(<Account as BorshDeserialize>::deserialize(&mut &bytes[..]).is_err());
    }

    #[test]
    fn uninitialized_account_serde_serialization() {
        let account = Account::new_uninitialized(Balance::from_yoctonear(100), 300);

        let serialized_account = serde_json::to_string(&account).unwrap();
        expect![[r#"{"amount":"100","locked":"0","code_hash":"11111111111111111111111111111111","storage_usage":300,"version":"V1","state":"uninitialized"}"#]].assert_eq(&serialized_account);

        let deserialized_account: Account = serde_json::from_str(&serialized_account).unwrap();
        assert_eq!(deserialized_account, account);
    }

    /// Existing accounts must serialize exactly as before, so that documents
    /// written by older binaries and by this one stay interchangeable.
    #[test]
    fn initialized_account_serde_serialization_omits_state() {
        let account = Account::new(
            Balance::from_yoctonear(100),
            Balance::from_yoctonear(200),
            AccountContract::Local(CryptoHash::hash_bytes(&[42])),
            300,
        );

        let serialized_account = serde_json::to_string(&account).unwrap();
        assert!(!serialized_account.contains("\"state\""), "{serialized_account}");
    }

    #[test]
    fn uninitialized_account_serde_deserialization_rejects_impossible_fields() {
        assert!(deserialize_account(&uninitialized_serde_account()).is_ok());

        let mut wrong_version = uninitialized_serde_account();
        wrong_version.version = AccountVersion::V2;
        assert!(deserialize_account(&wrong_version).is_err());

        let mut locked = uninitialized_serde_account();
        locked.locked = Balance::from_yoctonear(1);
        assert!(deserialize_account(&locked).is_err());

        let mut local_contract = uninitialized_serde_account();
        local_contract.code_hash = CryptoHash::hash_bytes(&[42]);
        assert!(deserialize_account(&local_contract).is_err());

        let mut global_contract = uninitialized_serde_account();
        global_contract.global_contract_hash = Some(CryptoHash::hash_bytes(&[42]));
        assert!(deserialize_account(&global_contract).is_err());

        let mut global_by_account = uninitialized_serde_account();
        global_by_account.global_contract_account_id =
            Some(AccountId::try_from("test.near".to_string()).unwrap());
        assert!(deserialize_account(&global_by_account).is_err());
    }
}
