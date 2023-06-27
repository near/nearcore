use crate::account::{AccessKey, Account};
use crate::challenge::ChallengesResult;
use crate::errors::EpochError;
use crate::hash::CryptoHash;
use crate::receipt::Receipt;
use crate::serialize::dec_format;
use crate::trie_key::TrieKey;
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::PublicKey;
/// Reexport primitive types
pub use near_primitives_core::types::*;
pub use near_vm_runner::logic::TrieNodesCount;
use once_cell::sync::Lazy;
use serde_with::base64::Base64;
use serde_with::serde_as;
use std::sync::Arc;

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
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
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
    /// State change that happens when we update validator accounts. Not associated with with any
    /// specific transaction or receipt.
    ValidatorAccountsUpdate,
    /// State change that is happens due to migration that happens in first block of an epoch
    /// after protocol upgrade
    Migration,
    /// State changes for building states for re-sharding
    Resharding,
}

/// This represents the committed changes in the Trie with a change cause.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct RawStateChange {
    pub cause: StateChangeCause,
    pub data: Option<Vec<u8>>,
}

/// List of committed changes with a cause for a given TrieKey
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
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
pub struct StateChangesForSplitStates {
    pub changes: Vec<ConsolidatedStateChange>,
    // we need to store deleted receipts here because StateChanges will only include
    // trie keys for removed values and account information can not be inferred from
    // trie key for delayed receipts
    pub processed_delayed_receipts: Vec<Receipt>,
}

impl StateChangesForSplitStates {
    pub fn from_raw_state_changes(
        changes: &[RawStateChangesWithTrieKey],
        processed_delayed_receipts: Vec<Receipt>,
    ) -> Self {
        let changes = changes
            .iter()
            .map(|RawStateChangesWithTrieKey { trie_key, changes }| {
                let value = changes.last().expect("state_changes must not be empty").data.clone();
                ConsolidatedStateChange { trie_key: trie_key.clone(), value }
            })
            .collect();
        Self { changes, processed_delayed_receipts }
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
        static EMPTY: Lazy<Arc<[u8]>> = Lazy::new(|| Arc::new([]));
        StateRootNode { data: EMPTY.clone(), memory_usage: 0 }
    }
}

/// Epoch identifier -- wrapped hash, to make it easier to distinguish.
/// EpochId of epoch T is the hash of last block in T-2
/// EpochId of first two epochs is 0
#[derive(
    Debug,
    Clone,
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
)]
#[as_ref(forward)]
pub struct EpochId(pub CryptoHash);

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
    use near_crypto::PublicKey;
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
    }
}

/// Stores validator and its stake.
#[derive(BorshSerialize, BorshDeserialize, serde::Serialize, Debug, Clone, PartialEq, Eq)]
pub struct ValidatorStakeV1 {
    /// Account that stakes money.
    pub account_id: AccountId,
    /// Public key of the proposed validator.
    pub public_key: PublicKey,
    /// Stake / weight of the validator.
    pub stake: Balance,
}

/// Information after block was processed.
#[derive(Debug, PartialEq, BorshSerialize, BorshDeserialize, Clone, Eq)]
pub struct BlockExtra {
    pub challenges_result: ChallengesResult,
}

pub mod chunk_extra {
    use crate::types::validator_stake::{ValidatorStake, ValidatorStakeIter};
    use crate::types::StateRoot;
    use borsh::{BorshDeserialize, BorshSerialize};
    use near_primitives_core::hash::CryptoHash;
    use near_primitives_core::types::{Balance, Gas};

    pub use super::ChunkExtraV1;

    /// Information after chunk was processed, used to produce or check next chunk.
    #[derive(Debug, PartialEq, BorshSerialize, BorshDeserialize, Clone, Eq)]
    pub enum ChunkExtra {
        V1(ChunkExtraV1),
        V2(ChunkExtraV2),
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

    impl ChunkExtra {
        pub fn new_with_only_state_root(state_root: &StateRoot) -> Self {
            Self::new(state_root, CryptoHash::default(), vec![], 0, 0, 0)
        }

        pub fn new(
            state_root: &StateRoot,
            outcome_root: CryptoHash,
            validator_proposals: Vec<ValidatorStake>,
            gas_used: Gas,
            gas_limit: Gas,
            balance_burnt: Balance,
        ) -> Self {
            Self::V2(ChunkExtraV2 {
                state_root: *state_root,
                outcome_root,
                validator_proposals,
                gas_used,
                gas_limit,
                balance_burnt,
            })
        }

        #[inline]
        pub fn outcome_root(&self) -> &StateRoot {
            match self {
                Self::V1(v1) => &v1.outcome_root,
                Self::V2(v2) => &v2.outcome_root,
            }
        }

        #[inline]
        pub fn state_root(&self) -> &StateRoot {
            match self {
                Self::V1(v1) => &v1.state_root,
                Self::V2(v2) => &v2.state_root,
            }
        }

        #[inline]
        pub fn state_root_mut(&mut self) -> &mut StateRoot {
            match self {
                Self::V1(v1) => &mut v1.state_root,
                Self::V2(v2) => &mut v2.state_root,
            }
        }

        #[inline]
        pub fn validator_proposals(&self) -> ValidatorStakeIter {
            match self {
                Self::V1(v1) => ValidatorStakeIter::v1(&v1.validator_proposals),
                Self::V2(v2) => ValidatorStakeIter::new(&v2.validator_proposals),
            }
        }

        #[inline]
        pub fn gas_limit(&self) -> Gas {
            match self {
                Self::V1(v1) => v1.gas_limit,
                Self::V2(v2) => v2.gas_limit,
            }
        }

        #[inline]
        pub fn gas_used(&self) -> Gas {
            match self {
                Self::V1(v1) => v1.gas_used,
                Self::V2(v2) => v2.gas_used,
            }
        }

        #[inline]
        pub fn balance_burnt(&self) -> Balance {
            match self {
                Self::V1(v1) => v1.balance_burnt,
                Self::V2(v2) => v2.balance_burnt,
            }
        }
    }
}

/// Information after chunk was processed, used to produce or check next chunk.
#[derive(Debug, PartialEq, BorshSerialize, BorshDeserialize, Clone, Eq)]
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

#[derive(Default, BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq)]
pub struct ValidatorStats {
    pub produced: NumBlocks,
    pub expected: NumBlocks,
}

#[derive(Debug, BorshSerialize, BorshDeserialize, PartialEq, Eq)]
pub struct BlockChunkValidatorStats {
    pub block_stats: ValidatorStats,
    pub chunk_stats: ValidatorStats,
}

#[derive(serde::Deserialize, Debug, arbitrary::Arbitrary)]
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
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TransactionOrReceiptId {
    Transaction { transaction_hash: CryptoHash, sender_id: AccountId },
    Receipt { receipt_id: CryptoHash, receiver_id: AccountId },
}

/// Provides information about current epoch validators.
/// Used to break dependency between epoch manager and runtime.
pub trait EpochInfoProvider {
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
}

/// Mode of the trie cache.
#[derive(Debug, Copy, Clone)]
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
