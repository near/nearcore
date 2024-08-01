use crate::account::{AccessKey, Account};
use crate::challenge::ChallengesResult;
use crate::errors::EpochError;
use crate::hash::CryptoHash;
use crate::receipt::{PromiseYieldTimeout, Receipt};
use crate::serialize::dec_format;
use crate::trie_key::TrieKey;
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::PublicKey;
/// Reexport primitive types
pub use near_primitives_core::types::*;
use near_schema_checker_lib::ProtocolSchema;
use serde_with::base64::Base64;
use serde_with::serde_as;
use std::sync::Arc;
use std::sync::LazyLock;

mod chunk_validator_stats;

pub use chunk_validator_stats::ChunkStats;

/// Hash used by to store state root.
pub type StateRoot = CryptoHash;

/// Different types of finality.
#[derive(
    serde::Serialize, serde::Deserialize, Default, Clone, Debug, PartialEq, Eq, arbitrary::Arbitrary,
)]
pub enum Finality {
    #[serde(rename = "optimistic")]
    None,
    #[serde(rename = "near-final")]
    DoomSlug,
    #[serde(rename = "final")]
    #[default]
    Final,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct AccountWithPublicKey {
    pub account_id: AccountId,
    pub public_key: PublicKey,
}

/// Account info for validators
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct AccountInfo {
    pub account_id: AccountId,
    pub public_key: PublicKey,
    #[serde(with = "dec_format")]
    pub amount: Balance,
}

/// This type is used to mark keys (arrays of bytes) that are queried from store.
///
/// NOTE: Currently, this type is only used in the view_client and RPC to be able to transparently
/// pretty-serialize the bytes arrays as base64-encoded strings (see `serialize.rs`).
#[serde_as]
#[derive(
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Debug,
    PartialEq,
    Eq,
    derive_more::Deref,
    derive_more::From,
    derive_more::Into,
    BorshSerialize,
    BorshDeserialize,
)]
#[serde(transparent)]
pub struct StoreKey(#[serde_as(as = "Base64")] Vec<u8>);

/// This type is used to mark values returned from store (arrays of bytes).
///
/// NOTE: Currently, this type is only used in the view_client and RPC to be able to transparently
/// pretty-serialize the bytes arrays as base64-encoded strings (see `serialize.rs`).
#[serde_as]
#[derive(
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Debug,
    PartialEq,
    Eq,
    derive_more::Deref,
    derive_more::From,
    derive_more::Into,
    BorshSerialize,
    BorshDeserialize,
)]
#[serde(transparent)]
pub struct StoreValue(#[serde_as(as = "Base64")] Vec<u8>);

/// This type is used to mark function arguments.
///
/// NOTE: The main reason for this to exist (except the type-safety) is that the value is
/// transparently serialized and deserialized as a base64-encoded string when serde is used
/// (serde_json).
#[serde_as]
#[derive(
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Debug,
    PartialEq,
    Eq,
    derive_more::Deref,
    derive_more::From,
    derive_more::Into,
    BorshSerialize,
    BorshDeserialize,
)]
#[serde(transparent)]
pub struct FunctionArgs(#[serde_as(as = "Base64")] Vec<u8>);

/// A structure used to indicate the kind of state changes due to transaction/receipt processing, etc.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub enum StateChangeKind {
    AccountTouched { account_id: AccountId },
    AccessKeyTouched { account_id: AccountId },
    DataTouched { account_id: AccountId },
    ContractCodeTouched { account_id: AccountId },
}

pub type StateChangesKinds = Vec<StateChangeKind>;

#[easy_ext::ext(StateChangesKindsExt)]
impl StateChangesKinds {
    pub fn from_changes(
        raw_changes: &mut dyn Iterator<Item = Result<RawStateChangesWithTrieKey, std::io::Error>>,
    ) -> Result<StateChangesKinds, std::io::Error> {
        raw_changes
            .filter_map(|raw_change| {
                let RawStateChangesWithTrieKey { trie_key, .. } = match raw_change {
                    Ok(p) => p,
                    Err(e) => return Some(Err(e)),
                };
                match trie_key {
                    TrieKey::Account { account_id } => {
                        Some(Ok(StateChangeKind::AccountTouched { account_id }))
                    }
                    TrieKey::ContractCode { account_id } => {
                        Some(Ok(StateChangeKind::ContractCodeTouched { account_id }))
                    }
                    TrieKey::AccessKey { account_id, .. } => {
                        Some(Ok(StateChangeKind::AccessKeyTouched { account_id }))
                    }
                    TrieKey::ContractData { account_id, .. } => {
                        Some(Ok(StateChangeKind::DataTouched { account_id }))
                    }
                    _ => None,
                }
            })
            .collect()
    }
}

/// A structure used to index state changes due to transaction/receipt processing and other things.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, PartialEq, ProtocolSchema)]
pub enum StateChangeCause {
    /// A type of update that does not get finalized. Used for verification and execution of
    /// immutable smart contract methods. Attempt fo finalize a `TrieUpdate` containing such
    /// change will lead to panic.
    NotWritableToDisk,
    /// A type of update that is used to mark the initial storage update, e.g. during genesis
    /// or in tests setup.
    InitialState,
    /// Processing of a transaction.
    TransactionProcessing { tx_hash: CryptoHash },
    /// Before the receipt is going to be processed, inputs get drained from the state, which
    /// causes state modification.
    ActionReceiptProcessingStarted { receipt_hash: CryptoHash },
    /// Computation of gas reward.
    ActionReceiptGasReward { receipt_hash: CryptoHash },
    /// Processing of a receipt.
    ReceiptProcessing { receipt_hash: CryptoHash },
    /// The given receipt was postponed. This is either a data receipt or an action receipt.
    /// A `DataReceipt` can be postponed if the corresponding `ActionReceipt` is not received yet,
    /// or other data dependencies are not satisfied.
    /// An `ActionReceipt` can be postponed if not all data dependencies are received.
    PostponedReceipt { receipt_hash: CryptoHash },
    /// Updated delayed receipts queue in the state.
    /// We either processed previously delayed receipts or added more receipts to the delayed queue.
    UpdatedDelayedReceipts,
    /// State change that happens when we update validator accounts. Not associated with any
    /// specific transaction or receipt.
    ValidatorAccountsUpdate,
    /// State change that is happens due to migration that happens in first block of an epoch
    /// after protocol upgrade
    Migration,
    /// State changes for building states for re-sharding
    Resharding,
}

/// This represents the committed changes in the Trie with a change cause.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct RawStateChange {
    pub cause: StateChangeCause,
    pub data: Option<Vec<u8>>,
}

/// List of committed changes with a cause for a given TrieKey
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct RawStateChangesWithTrieKey {
    pub trie_key: TrieKey,
    pub changes: Vec<RawStateChange>,
}

/// Consolidate state change of trie_key and the final value the trie key will be changed to
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct ConsolidatedStateChange {
    pub trie_key: TrieKey,
    pub value: Option<Vec<u8>>,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct StateChangesForResharding {
    pub changes: Vec<ConsolidatedStateChange>,
    // For DelayedReceipt and for PromiseYieldTimeout, account information is kept in the trie
    // value rather than in the trie key. When such a key is erased, we need to know the erased
    // value so that the change can be propagated to the correct child trie.
    pub processed_delayed_receipts: Vec<Receipt>,
    pub processed_yield_timeouts: Vec<PromiseYieldTimeout>,
}

impl StateChangesForResharding {
    pub fn from_raw_state_changes(
        changes: &[RawStateChangesWithTrieKey],
        processed_delayed_receipts: Vec<Receipt>,
        processed_yield_timeouts: Vec<PromiseYieldTimeout>,
    ) -> Self {
        let changes = changes
            .iter()
            .map(|RawStateChangesWithTrieKey { trie_key, changes }| {
                let value = changes.last().expect("state_changes must not be empty").data.clone();
                ConsolidatedStateChange { trie_key: trie_key.clone(), value }
            })
            .collect();
        Self { changes, processed_delayed_receipts, processed_yield_timeouts }
    }
}

/// key that was updated -> list of updates with the corresponding indexing event.
pub type RawStateChanges = std::collections::BTreeMap<Vec<u8>, RawStateChangesWithTrieKey>;

#[derive(Debug)]
pub enum StateChangesRequest {
    AccountChanges { account_ids: Vec<AccountId> },
    SingleAccessKeyChanges { keys: Vec<AccountWithPublicKey> },
    AllAccessKeyChanges { account_ids: Vec<AccountId> },
    ContractCodeChanges { account_ids: Vec<AccountId> },
    DataChanges { account_ids: Vec<AccountId>, key_prefix: StoreKey },
}

#[derive(Debug)]
pub enum StateChangeValue {
    AccountUpdate { account_id: AccountId, account: Account },
    AccountDeletion { account_id: AccountId },
    AccessKeyUpdate { account_id: AccountId, public_key: PublicKey, access_key: AccessKey },
    AccessKeyDeletion { account_id: AccountId, public_key: PublicKey },
    DataUpdate { account_id: AccountId, key: StoreKey, value: StoreValue },
    DataDeletion { account_id: AccountId, key: StoreKey },
    ContractCodeUpdate { account_id: AccountId, code: Vec<u8> },
    ContractCodeDeletion { account_id: AccountId },
}

impl StateChangeValue {
    pub fn affected_account_id(&self) -> &AccountId {
        match &self {
            StateChangeValue::AccountUpdate { account_id, .. }
            | StateChangeValue::AccountDeletion { account_id }
            | StateChangeValue::AccessKeyUpdate { account_id, .. }
            | StateChangeValue::AccessKeyDeletion { account_id, .. }
            | StateChangeValue::DataUpdate { account_id, .. }
            | StateChangeValue::DataDeletion { account_id, .. }
            | StateChangeValue::ContractCodeUpdate { account_id, .. }
            | StateChangeValue::ContractCodeDeletion { account_id } => account_id,
        }
    }
}

#[derive(Debug)]
pub struct StateChangeWithCause {
    pub cause: StateChangeCause,
    pub value: StateChangeValue,
}

pub type StateChanges = Vec<StateChangeWithCause>;

#[easy_ext::ext(StateChangesExt)]
impl StateChanges {
    pub fn from_changes(
        raw_changes: impl Iterator<Item = Result<RawStateChangesWithTrieKey, std::io::Error>>,
    ) -> Result<StateChanges, std::io::Error> {
        let mut state_changes = Self::new();

        for raw_change in raw_changes {
            let RawStateChangesWithTrieKey { trie_key, changes } = raw_change?;

            match trie_key {
                TrieKey::Account { account_id } => state_changes.extend(changes.into_iter().map(
                    |RawStateChange { cause, data }| StateChangeWithCause {
                        cause,
                        value: if let Some(change_data) = data {
                            StateChangeValue::AccountUpdate {
                                account_id: account_id.clone(),
                                account: <_>::try_from_slice(&change_data).expect(
                                    "Failed to parse internally stored account information",
                                ),
                            }
                        } else {
                            StateChangeValue::AccountDeletion { account_id: account_id.clone() }
                        },
                    },
                )),
                TrieKey::AccessKey { account_id, public_key } => {
                    state_changes.extend(changes.into_iter().map(
                        |RawStateChange { cause, data }| StateChangeWithCause {
                            cause,
                            value: if let Some(change_data) = data {
                                StateChangeValue::AccessKeyUpdate {
                                    account_id: account_id.clone(),
                                    public_key: public_key.clone(),
                                    access_key: <_>::try_from_slice(&change_data)
                                        .expect("Failed to parse internally stored access key"),
                                }
                            } else {
                                StateChangeValue::AccessKeyDeletion {
                                    account_id: account_id.clone(),
                                    public_key: public_key.clone(),
                                }
                            },
                        },
                    ))
                }
                TrieKey::ContractCode { account_id } => {
                    state_changes.extend(changes.into_iter().map(
                        |RawStateChange { cause, data }| StateChangeWithCause {
                            cause,
                            value: match data {
                                Some(change_data) => StateChangeValue::ContractCodeUpdate {
                                    account_id: account_id.clone(),
                                    code: change_data,
                                },
                                None => StateChangeValue::ContractCodeDeletion {
                                    account_id: account_id.clone(),
                                },
                            },
                        },
                    ));
                }
                TrieKey::ContractData { account_id, key } => {
                    state_changes.extend(changes.into_iter().map(
                        |RawStateChange { cause, data }| StateChangeWithCause {
                            cause,
                            value: if let Some(change_data) = data {
                                StateChangeValue::DataUpdate {
                                    account_id: account_id.clone(),
                                    key: key.to_vec().into(),
                                    value: change_data.into(),
                                }
                            } else {
                                StateChangeValue::DataDeletion {
                                    account_id: account_id.clone(),
                                    key: key.to_vec().into(),
                                }
                            },
                        },
                    ));
                }
                // The next variants considered as unnecessary as too low level
                TrieKey::ReceivedData { .. } => {}
                TrieKey::PostponedReceiptId { .. } => {}
                TrieKey::PendingDataCount { .. } => {}
                TrieKey::PostponedReceipt { .. } => {}
                TrieKey::DelayedReceiptIndices => {}
                TrieKey::DelayedReceipt { .. } => {}
                TrieKey::PromiseYieldIndices => {}
                TrieKey::PromiseYieldTimeout { .. } => {}
                TrieKey::PromiseYieldReceipt { .. } => {}
                TrieKey::BufferedReceiptIndices => {}
                TrieKey::BufferedReceipt { .. } => {}
            }
        }

        Ok(state_changes)
    }
    pub fn from_account_changes(
        raw_changes: impl Iterator<Item = Result<RawStateChangesWithTrieKey, std::io::Error>>,
    ) -> Result<StateChanges, std::io::Error> {
        let state_changes = Self::from_changes(raw_changes)?;

        Ok(state_changes
            .into_iter()
            .filter(|state_change| {
                matches!(
                    state_change.value,
                    StateChangeValue::AccountUpdate { .. }
                        | StateChangeValue::AccountDeletion { .. }
                )
            })
            .collect())
    }

    pub fn from_access_key_changes(
        raw_changes: impl Iterator<Item = Result<RawStateChangesWithTrieKey, std::io::Error>>,
    ) -> Result<StateChanges, std::io::Error> {
        let state_changes = Self::from_changes(raw_changes)?;

        Ok(state_changes
            .into_iter()
            .filter(|state_change| {
                matches!(
                    state_change.value,
                    StateChangeValue::AccessKeyUpdate { .. }
                        | StateChangeValue::AccessKeyDeletion { .. }
                )
            })
            .collect())
    }

    pub fn from_contract_code_changes(
        raw_changes: impl Iterator<Item = Result<RawStateChangesWithTrieKey, std::io::Error>>,
    ) -> Result<StateChanges, std::io::Error> {
        let state_changes = Self::from_changes(raw_changes)?;

        Ok(state_changes
            .into_iter()
            .filter(|state_change| {
                matches!(
                    state_change.value,
                    StateChangeValue::ContractCodeUpdate { .. }
                        | StateChangeValue::ContractCodeDeletion { .. }
                )
            })
            .collect())
    }

    pub fn from_data_changes(
        raw_changes: impl Iterator<Item = Result<RawStateChangesWithTrieKey, std::io::Error>>,
    ) -> Result<StateChanges, std::io::Error> {
        let state_changes = Self::from_changes(raw_changes)?;

        Ok(state_changes
            .into_iter()
            .filter(|state_change| {
                matches!(
                    state_change.value,
                    StateChangeValue::DataUpdate { .. } | StateChangeValue::DataDeletion { .. }
                )
            })
            .collect())
    }
}

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize, serde::Serialize)]
pub struct StateRootNode {
    /// In Nightshade, data is the serialized TrieNodeWithSize.
    ///
    /// Beware that hash of an empty state root (i.e. once who’s data is an
    /// empty byte string) **does not** equal hash of an empty byte string.
    /// Instead, an all-zero hash indicates an empty node.
    pub data: Arc<[u8]>,

    /// In Nightshade, memory_usage is a field of TrieNodeWithSize.
    pub memory_usage: u64,
}

impl StateRootNode {
    pub fn empty() -> Self {
        static EMPTY: LazyLock<Arc<[u8]>> = LazyLock::new(|| Arc::new([]));
        StateRootNode { data: EMPTY.clone(), memory_usage: 0 }
    }
}

/// Epoch identifier -- wrapped hash, to make it easier to distinguish.
/// EpochId of epoch T is the hash of last block in T-2
/// EpochId of first two epochs is 0
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    Hash,
    Eq,
    PartialEq,
    PartialOrd,
    Ord,
    derive_more::AsRef,
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    arbitrary::Arbitrary,
    ProtocolSchema,
)]
#[as_ref(forward)]
pub struct EpochId(pub CryptoHash);

impl std::str::FromStr for EpochId {
    type Err = Box<dyn std::error::Error + Send + Sync>;

    /// Decodes base58-encoded string into a 32-byte crypto hash.
    fn from_str(epoch_id_str: &str) -> Result<Self, Self::Err> {
        Ok(EpochId(CryptoHash::from_str(epoch_id_str)?))
    }
}

/// Stores validator and its stake for two consecutive epochs.
/// It is necessary because the blocks on the epoch boundary need to contain approvals from both
/// epochs.
#[derive(BorshSerialize, BorshDeserialize, serde::Serialize, Debug, Clone, PartialEq, Eq)]
pub struct ApprovalStake {
    /// Account that stakes money.
    pub account_id: AccountId,
    /// Public key of the proposed validator.
    pub public_key: PublicKey,
    /// Stake / weight of the validator.
    pub stake_this_epoch: Balance,
    pub stake_next_epoch: Balance,
}

pub mod validator_stake {
    use crate::types::ApprovalStake;
    use borsh::{BorshDeserialize, BorshSerialize};
    use near_crypto::{KeyType, PublicKey};
    use near_primitives_core::types::{AccountId, Balance};
    use serde::Serialize;

    pub use super::ValidatorStakeV1;

    /// Stores validator and its stake.
    #[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, PartialEq, Eq)]
    #[serde(tag = "validator_stake_struct_version")]
    pub enum ValidatorStake {
        V1(ValidatorStakeV1),
    }

    pub struct ValidatorStakeIter<'a> {
        collection: ValidatorStakeIterSource<'a>,
        curr_index: usize,
        len: usize,
    }

    impl<'a> ValidatorStakeIter<'a> {
        pub fn empty() -> Self {
            Self { collection: ValidatorStakeIterSource::V2(&[]), curr_index: 0, len: 0 }
        }

        pub fn v1(collection: &'a [ValidatorStakeV1]) -> Self {
            Self {
                collection: ValidatorStakeIterSource::V1(collection),
                curr_index: 0,
                len: collection.len(),
            }
        }

        pub fn new(collection: &'a [ValidatorStake]) -> Self {
            Self {
                collection: ValidatorStakeIterSource::V2(collection),
                curr_index: 0,
                len: collection.len(),
            }
        }

        pub fn len(&self) -> usize {
            self.len
        }
    }

    impl<'a> Iterator for ValidatorStakeIter<'a> {
        type Item = ValidatorStake;

        fn next(&mut self) -> Option<Self::Item> {
            if self.curr_index < self.len {
                let item = match self.collection {
                    ValidatorStakeIterSource::V1(collection) => {
                        ValidatorStake::V1(collection[self.curr_index].clone())
                    }
                    ValidatorStakeIterSource::V2(collection) => collection[self.curr_index].clone(),
                };
                self.curr_index += 1;
                Some(item)
            } else {
                None
            }
        }
    }

    enum ValidatorStakeIterSource<'a> {
        V1(&'a [ValidatorStakeV1]),
        V2(&'a [ValidatorStake]),
    }

    impl ValidatorStake {
        pub fn new_v1(account_id: AccountId, public_key: PublicKey, stake: Balance) -> Self {
            Self::V1(ValidatorStakeV1 { account_id, public_key, stake })
        }

        pub fn new(account_id: AccountId, public_key: PublicKey, stake: Balance) -> Self {
            Self::new_v1(account_id, public_key, stake)
        }

        pub fn test(account_id: AccountId) -> Self {
            Self::new_v1(account_id, PublicKey::empty(KeyType::ED25519), 0)
        }

        pub fn into_v1(self) -> ValidatorStakeV1 {
            match self {
                Self::V1(v1) => v1,
            }
        }

        #[inline]
        pub fn account_and_stake(self) -> (AccountId, Balance) {
            match self {
                Self::V1(v1) => (v1.account_id, v1.stake),
            }
        }

        #[inline]
        pub fn destructure(self) -> (AccountId, PublicKey, Balance) {
            match self {
                Self::V1(v1) => (v1.account_id, v1.public_key, v1.stake),
            }
        }

        #[inline]
        pub fn take_account_id(self) -> AccountId {
            match self {
                Self::V1(v1) => v1.account_id,
            }
        }

        #[inline]
        pub fn account_id(&self) -> &AccountId {
            match self {
                Self::V1(v1) => &v1.account_id,
            }
        }

        #[inline]
        pub fn take_public_key(self) -> PublicKey {
            match self {
                Self::V1(v1) => v1.public_key,
            }
        }

        #[inline]
        pub fn public_key(&self) -> &PublicKey {
            match self {
                Self::V1(v1) => &v1.public_key,
            }
        }

        #[inline]
        pub fn stake(&self) -> Balance {
            match self {
                Self::V1(v1) => v1.stake,
            }
        }

        #[inline]
        pub fn stake_mut(&mut self) -> &mut Balance {
            match self {
                Self::V1(v1) => &mut v1.stake,
            }
        }

        pub fn get_approval_stake(&self, is_next_epoch: bool) -> ApprovalStake {
            ApprovalStake {
                account_id: self.account_id().clone(),
                public_key: self.public_key().clone(),
                stake_this_epoch: if is_next_epoch { 0 } else { self.stake() },
                stake_next_epoch: if is_next_epoch { self.stake() } else { 0 },
            }
        }

        /// Returns the validator's number of mandates (rounded down) at `stake_per_seat`.
        ///
        /// It returns `u16` since it allows infallible conversion to `usize` and with [`u16::MAX`]
        /// equalling 65_535 it should be sufficient to hold the number of mandates per validator.
        ///
        /// # Why `u16` should be sufficient
        ///
        /// As of October 2023, a [recommended lower bound] for the stake required per mandate is
        /// 25k $NEAR. At this price, the validator with highest stake would have 1_888 mandates,
        /// which is well below `u16::MAX`.
        ///
        /// From another point of view, with more than `u16::MAX` mandates for validators, sampling
        /// mandates might become computationally too expensive. This might trigger an increase in
        /// the required stake per mandate, bringing down the number of mandates per validator.
        ///
        /// [recommended lower bound]: https://near.zulipchat.com/#narrow/stream/407237-pagoda.2Fcore.2Fstateless-validation/topic/validator.20seat.20assignment/near/393792901
        ///
        /// # Panics
        ///
        /// Panics if the number of mandates overflows `u16`.
        pub fn num_mandates(&self, stake_per_mandate: Balance) -> u16 {
            // Integer division in Rust returns the floor as described here
            // https://doc.rust-lang.org/std/primitive.u64.html#method.div_euclid
            u16::try_from(self.stake() / stake_per_mandate)
                .expect("number of mandats should fit u16")
        }

        /// Returns the weight attributed to the validator's partial mandate.
        ///
        /// A validator has a partial mandate if its stake cannot be divided evenly by
        /// `stake_per_mandate`. The remainder of that division is the weight of the partial
        /// mandate.
        ///
        /// Due to this definintion a validator has exactly one partial mandate with `0 <= weight <
        /// stake_per_mandate`.
        ///
        /// # Example
        ///
        /// Let `V` be a validator with stake of 12. If `stake_per_mandate` equals 5 then the weight
        /// of `V`'s partial mandate is `12 % 5 = 2`.
        pub fn partial_mandate_weight(&self, stake_per_mandate: Balance) -> Balance {
            self.stake() % stake_per_mandate
        }
    }
}

/// Stores validator and its stake.
#[derive(
    BorshSerialize, BorshDeserialize, serde::Serialize, Debug, Clone, PartialEq, Eq, ProtocolSchema,
)]
pub struct ValidatorStakeV1 {
    /// Account that stakes money.
    pub account_id: AccountId,
    /// Public key of the proposed validator.
    pub public_key: PublicKey,
    /// Stake / weight of the validator.
    pub stake: Balance,
}

/// Information after block was processed.
#[derive(Debug, PartialEq, BorshSerialize, BorshDeserialize, Clone, Eq, ProtocolSchema)]
pub struct BlockExtra {
    pub challenges_result: ChallengesResult,
}

pub mod chunk_extra {
    use crate::congestion_info::CongestionInfo;
    use crate::types::validator_stake::{ValidatorStake, ValidatorStakeIter};
    use crate::types::StateRoot;
    use borsh::{BorshDeserialize, BorshSerialize};
    use near_primitives_core::hash::CryptoHash;
    use near_primitives_core::types::{Balance, Gas, ProtocolVersion};
    use near_primitives_core::version::{ProtocolFeature, PROTOCOL_VERSION};

    pub use super::ChunkExtraV1;

    /// Information after chunk was processed, used to produce or check next chunk.
    #[derive(Debug, PartialEq, BorshSerialize, BorshDeserialize, Clone, Eq)]
    pub enum ChunkExtra {
        V1(ChunkExtraV1),
        V2(ChunkExtraV2),
        V3(ChunkExtraV3),
    }

    #[derive(Debug, PartialEq, BorshSerialize, BorshDeserialize, Clone, Eq)]
    pub struct ChunkExtraV2 {
        /// Post state root after applying give chunk.
        pub state_root: StateRoot,
        /// Root of merklizing results of receipts (transactions) execution.
        pub outcome_root: CryptoHash,
        /// Validator proposals produced by given chunk.
        pub validator_proposals: Vec<ValidatorStake>,
        /// Actually how much gas were used.
        pub gas_used: Gas,
        /// Gas limit, allows to increase or decrease limit based on expected time vs real time for computing the chunk.
        pub gas_limit: Gas,
        /// Total balance burnt after processing the current chunk.
        pub balance_burnt: Balance,
    }

    /// V2 -> V3: add congestion info fields.
    #[derive(Debug, PartialEq, BorshSerialize, BorshDeserialize, Clone, Eq)]
    pub struct ChunkExtraV3 {
        /// Post state root after applying give chunk.
        pub state_root: StateRoot,
        /// Root of merklizing results of receipts (transactions) execution.
        pub outcome_root: CryptoHash,
        /// Validator proposals produced by given chunk.
        pub validator_proposals: Vec<ValidatorStake>,
        /// Actually how much gas were used.
        pub gas_used: Gas,
        /// Gas limit, allows to increase or decrease limit based on expected time vs real time for computing the chunk.
        pub gas_limit: Gas,
        /// Total balance burnt after processing the current chunk.
        pub balance_burnt: Balance,
        /// Congestion info about this shard after the chunk was applied.
        congestion_info: CongestionInfo,
    }

    impl ChunkExtra {
        /// This method creates a slimmed down and invalid ChunkExtra. It's used
        /// for resharding where we only need the state root. This should not be
        /// used as part of regular processing.
        pub fn new_with_only_state_root(state_root: &StateRoot) -> Self {
            // TODO(congestion_control) - integration with resharding
            let congestion_control = if ProtocolFeature::CongestionControl.enabled(PROTOCOL_VERSION)
            {
                Some(CongestionInfo::default())
            } else {
                None
            };
            Self::new(
                PROTOCOL_VERSION,
                state_root,
                CryptoHash::default(),
                vec![],
                0,
                0,
                0,
                congestion_control,
            )
        }

        pub fn new(
            protocol_version: ProtocolVersion,
            state_root: &StateRoot,
            outcome_root: CryptoHash,
            validator_proposals: Vec<ValidatorStake>,
            gas_used: Gas,
            gas_limit: Gas,
            balance_burnt: Balance,
            congestion_info: Option<CongestionInfo>,
        ) -> Self {
            if ProtocolFeature::CongestionControl.enabled(protocol_version) {
                assert!(congestion_info.is_some());
                Self::V3(ChunkExtraV3 {
                    state_root: *state_root,
                    outcome_root,
                    validator_proposals,
                    gas_used,
                    gas_limit,
                    balance_burnt,
                    congestion_info: congestion_info.unwrap(),
                })
            } else {
                assert!(congestion_info.is_none());
                Self::V2(ChunkExtraV2 {
                    state_root: *state_root,
                    outcome_root,
                    validator_proposals,
                    gas_used,
                    gas_limit,
                    balance_burnt,
                })
            }
        }

        #[inline]
        pub fn outcome_root(&self) -> &StateRoot {
            match self {
                Self::V1(v1) => &v1.outcome_root,
                Self::V2(v2) => &v2.outcome_root,
                Self::V3(v3) => &v3.outcome_root,
            }
        }

        #[inline]
        pub fn state_root(&self) -> &StateRoot {
            match self {
                Self::V1(v1) => &v1.state_root,
                Self::V2(v2) => &v2.state_root,
                Self::V3(v3) => &v3.state_root,
            }
        }

        #[inline]
        pub fn state_root_mut(&mut self) -> &mut StateRoot {
            match self {
                Self::V1(v1) => &mut v1.state_root,
                Self::V2(v2) => &mut v2.state_root,
                Self::V3(v3) => &mut v3.state_root,
            }
        }

        #[inline]
        pub fn validator_proposals(&self) -> ValidatorStakeIter {
            match self {
                Self::V1(v1) => ValidatorStakeIter::v1(&v1.validator_proposals),
                Self::V2(v2) => ValidatorStakeIter::new(&v2.validator_proposals),
                Self::V3(v3) => ValidatorStakeIter::new(&v3.validator_proposals),
            }
        }

        #[inline]
        pub fn gas_limit(&self) -> Gas {
            match self {
                Self::V1(v1) => v1.gas_limit,
                Self::V2(v2) => v2.gas_limit,
                Self::V3(v3) => v3.gas_limit,
            }
        }

        #[inline]
        pub fn gas_used(&self) -> Gas {
            match self {
                Self::V1(v1) => v1.gas_used,
                Self::V2(v2) => v2.gas_used,
                Self::V3(v3) => v3.gas_used,
            }
        }

        #[inline]
        pub fn balance_burnt(&self) -> Balance {
            match self {
                Self::V1(v1) => v1.balance_burnt,
                Self::V2(v2) => v2.balance_burnt,
                Self::V3(v3) => v3.balance_burnt,
            }
        }

        #[inline]
        pub fn congestion_info(&self) -> Option<CongestionInfo> {
            match self {
                Self::V1(_) => None,
                Self::V2(_) => None,
                Self::V3(v3) => v3.congestion_info.into(),
            }
        }
    }
}

/// Information after chunk was processed, used to produce or check next chunk.
#[derive(Debug, PartialEq, BorshSerialize, BorshDeserialize, Clone, Eq, ProtocolSchema)]
pub struct ChunkExtraV1 {
    /// Post state root after applying give chunk.
    pub state_root: StateRoot,
    /// Root of merklizing results of receipts (transactions) execution.
    pub outcome_root: CryptoHash,
    /// Validator proposals produced by given chunk.
    pub validator_proposals: Vec<ValidatorStakeV1>,
    /// Actually how much gas were used.
    pub gas_used: Gas,
    /// Gas limit, allows to increase or decrease limit based on expected time vs real time for computing the chunk.
    pub gas_limit: Gas,
    /// Total balance burnt after processing the current chunk.
    pub balance_burnt: Balance,
}

#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, arbitrary::Arbitrary,
)]
#[serde(untagged)]
pub enum BlockId {
    Height(BlockHeight),
    Hash(CryptoHash),
}

pub type MaybeBlockId = Option<BlockId>;

#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, arbitrary::Arbitrary,
)]
#[serde(rename_all = "snake_case")]
pub enum SyncCheckpoint {
    Genesis,
    EarliestAvailable,
}

#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, arbitrary::Arbitrary,
)]
#[serde(rename_all = "snake_case")]
pub enum BlockReference {
    BlockId(BlockId),
    Finality(Finality),
    SyncCheckpoint(SyncCheckpoint),
}

impl BlockReference {
    pub fn latest() -> Self {
        Self::Finality(Finality::None)
    }
}

impl From<BlockId> for BlockReference {
    fn from(block_id: BlockId) -> Self {
        Self::BlockId(block_id)
    }
}

impl From<Finality> for BlockReference {
    fn from(finality: Finality) -> Self {
        Self::Finality(finality)
    }
}

#[derive(
    Default, BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq, ProtocolSchema,
)]
pub struct ValidatorStats {
    pub produced: NumBlocks,
    pub expected: NumBlocks,
}

impl ValidatorStats {
    /// Compare stats with threshold which is an expected percentage from 0 to
    /// 100.
    pub fn less_than(&self, threshold: u8) -> bool {
        self.produced * 100 < u64::from(threshold) * self.expected
    }
}

#[derive(Debug, BorshSerialize, BorshDeserialize, PartialEq, Eq, ProtocolSchema)]
pub struct BlockChunkValidatorStats {
    pub block_stats: ValidatorStats,
    pub chunk_stats: ChunkStats,
}

#[derive(serde::Deserialize, Debug, arbitrary::Arbitrary, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EpochReference {
    EpochId(EpochId),
    BlockId(BlockId),
    Latest,
}

impl serde::Serialize for EpochReference {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            EpochReference::EpochId(epoch_id) => {
                s.serialize_newtype_variant("EpochReference", 0, "epoch_id", epoch_id)
            }
            EpochReference::BlockId(block_id) => {
                s.serialize_newtype_variant("EpochReference", 1, "block_id", block_id)
            }
            EpochReference::Latest => {
                s.serialize_newtype_variant("EpochReference", 2, "latest", &())
            }
        }
    }
}

/// Either an epoch id or latest block hash.  When `EpochId` variant is used it
/// must be an identifier of a past epoch.  When `BlockHeight` is used it must
/// be hash of the latest block in the current epoch.  Using current epoch id
/// with `EpochId` or arbitrary block hash in past or present epochs will result
/// in errors.
#[derive(Debug)]
pub enum ValidatorInfoIdentifier {
    EpochId(EpochId),
    BlockHash(CryptoHash),
}

/// Reasons for removing a validator from the validator set.
#[derive(
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Debug,
    PartialEq,
    Eq,
    ProtocolSchema,
)]
pub enum ValidatorKickoutReason {
    /// Slashed validators are kicked out.
    Slashed,
    /// Validator didn't produce enough blocks.
    NotEnoughBlocks { produced: NumBlocks, expected: NumBlocks },
    /// Validator didn't produce enough chunks.
    NotEnoughChunks { produced: NumBlocks, expected: NumBlocks },
    /// Validator unstaked themselves.
    Unstaked,
    /// Validator stake is now below threshold
    NotEnoughStake {
        #[serde(with = "dec_format", rename = "stake_u128")]
        stake: Balance,
        #[serde(with = "dec_format", rename = "threshold_u128")]
        threshold: Balance,
    },
    /// Enough stake but is not chosen because of seat limits.
    DidNotGetASeat,
    /// Validator didn't produce enough chunk endorsements.
    NotEnoughChunkEndorsements { produced: NumBlocks, expected: NumBlocks },
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TransactionOrReceiptId {
    Transaction { transaction_hash: CryptoHash, sender_id: AccountId },
    Receipt { receipt_id: CryptoHash, receiver_id: AccountId },
}

/// Provides information about current epoch validators.
/// Used to break dependency between epoch manager and runtime.
pub trait EpochInfoProvider: Send + Sync {
    /// Get current stake of a validator in the given epoch.
    /// If the account is not a validator, returns `None`.
    fn validator_stake(
        &self,
        epoch_id: &EpochId,
        last_block_hash: &CryptoHash,
        account_id: &AccountId,
    ) -> Result<Option<Balance>, EpochError>;

    /// Get the total stake of the given epoch.
    fn validator_total_stake(
        &self,
        epoch_id: &EpochId,
        last_block_hash: &CryptoHash,
    ) -> Result<Balance, EpochError>;

    fn minimum_stake(&self, prev_block_hash: &CryptoHash) -> Result<Balance, EpochError>;

    /// Get the chain_id of the chain this epoch belongs to
    fn chain_id(&self) -> String;

    /// Which shard the account belongs to in the given epoch.
    fn account_id_to_shard_id(
        &self,
        account_id: &AccountId,
        epoch_id: &EpochId,
    ) -> Result<ShardId, EpochError>;
}

/// Mode of the trie cache.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum TrieCacheMode {
    /// In this mode we put each visited node to LRU cache to optimize performance.
    /// Presence of any exact node is not guaranteed.
    CachingShard,
    /// In this mode we put each visited node to the chunk cache which is a hash map.
    /// This is needed to guarantee that all nodes for which we charged a touching trie node cost are retrieved from DB
    /// only once during a single chunk processing. Such nodes remain in cache until the chunk processing is finished,
    /// and thus users (potentially different) are not required to pay twice for retrieval of the same node.
    CachingChunk,
}

/// State changes for a range of blocks.
/// Expects that a block is present at most once in the list.
#[derive(borsh::BorshDeserialize, borsh::BorshSerialize)]
pub struct StateChangesForBlockRange {
    pub blocks: Vec<StateChangesForBlock>,
}

/// State changes for a single block.
/// Expects that a shard is present at most once in the list of state changes.
#[derive(borsh::BorshDeserialize, borsh::BorshSerialize)]
pub struct StateChangesForBlock {
    pub block_hash: CryptoHash,
    pub state_changes: Vec<StateChangesForShard>,
}

/// Key and value of a StateChanges column.
#[derive(borsh::BorshDeserialize, borsh::BorshSerialize)]
pub struct StateChangesForShard {
    pub shard_id: ShardId,
    pub state_changes: Vec<RawStateChangesWithTrieKey>,
}

#[cfg(test)]
mod tests {
    use near_crypto::{KeyType, PublicKey};
    use near_primitives_core::types::Balance;

    use super::validator_stake::ValidatorStake;

    fn new_validator_stake(stake: Balance) -> ValidatorStake {
        ValidatorStake::new(
            "test_account".parse().unwrap(),
            PublicKey::empty(KeyType::ED25519),
            stake,
        )
    }

    #[test]
    fn test_validator_stake_num_mandates() {
        assert_eq!(new_validator_stake(0).num_mandates(5), 0);
        assert_eq!(new_validator_stake(10).num_mandates(5), 2);
        assert_eq!(new_validator_stake(12).num_mandates(5), 2);
    }

    #[test]
    fn test_validator_partial_mandate_weight() {
        assert_eq!(new_validator_stake(0).partial_mandate_weight(5), 0);
        assert_eq!(new_validator_stake(10).partial_mandate_weight(5), 0);
        assert_eq!(new_validator_stake(12).partial_mandate_weight(5), 2);
    }
}
