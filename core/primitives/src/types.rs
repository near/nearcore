use borsh::{BorshDeserialize, BorshSerialize};
use derive_more::{AsRef as DeriveAsRef, From as DeriveFrom};
use serde::{Deserialize, Serialize};

use near_crypto::PublicKey;

use crate::account::{AccessKey, Account};
use crate::challenge::ChallengesResult;
use crate::errors::EpochError;
use crate::hash::CryptoHash;
use crate::serialize::u128_dec_format;
use crate::trie_key::TrieKey;

/// Account identifier. Provides access to user's state.
pub type AccountId = String;
/// Hash used by a struct implementing the Merkle tree.
pub type MerkleHash = CryptoHash;
/// Validator identifier in current group.
pub type ValidatorId = u64;
/// Mask which validators participated in multi sign.
pub type ValidatorMask = Vec<bool>;
/// StorageUsage is used to count the amount of storage used by a contract.
pub type StorageUsage = u64;
/// StorageUsageChange is used to count the storage usage within a single contract call.
pub type StorageUsageChange = i64;
/// Nonce for transactions.
pub type Nonce = u64;
/// Height of the block.
pub type BlockHeight = u64;
/// Height of the epoch.
pub type EpochHeight = u64;
/// Shard index, from 0 to NUM_SHARDS - 1.
pub type ShardId = u64;
/// Balance is type for storing amounts of tokens.
pub type Balance = u128;
/// Gas is a type for storing amount of gas.
pub type Gas = u64;

/// Number of blocks in current group.
pub type NumBlocks = u64;
/// Number of shards in current group.
pub type NumShards = u64;
/// Number of seats of validators (block producer or hidden ones) in current group (settlement).
pub type NumSeats = u64;
/// Block height delta that measures the difference between `BlockHeight`s.
pub type BlockHeightDelta = u64;

pub type GCCount = u64;

pub type ReceiptIndex = usize;
pub type PromiseId = Vec<ReceiptIndex>;

/// Hash used by to store state root.
pub type StateRoot = CryptoHash;

/// Different types of finality.
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq)]
pub enum Finality {
    #[serde(rename = "optimistic")]
    None,
    #[serde(rename = "near-final")]
    DoomSlug,
    #[serde(rename = "final")]
    Final,
}

impl Default for Finality {
    fn default() -> Self {
        Finality::Final
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AccountWithPublicKey {
    pub account_id: AccountId,
    pub public_key: PublicKey,
}

/// Account info for validators
#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct AccountInfo {
    pub account_id: AccountId,
    pub public_key: PublicKey,
    #[serde(with = "u128_dec_format")]
    pub amount: Balance,
}

/// This type is used to mark keys (arrays of bytes) that are queried from store.
///
/// NOTE: Currently, this type is only used in the view_client and RPC to be able to transparently
/// pretty-serialize the bytes arrays as base64-encoded strings (see `serialize.rs`).
#[derive(Debug, Clone, PartialEq, Eq, DeriveAsRef, DeriveFrom, BorshSerialize, BorshDeserialize)]
#[as_ref(forward)]
pub struct StoreKey(Vec<u8>);

/// This type is used to mark values returned from store (arrays of bytes).
///
/// NOTE: Currently, this type is only used in the view_client and RPC to be able to transparently
/// pretty-serialize the bytes arrays as base64-encoded strings (see `serialize.rs`).
#[derive(Debug, Clone, PartialEq, Eq, DeriveAsRef, DeriveFrom, BorshSerialize, BorshDeserialize)]
#[as_ref(forward)]
pub struct StoreValue(Vec<u8>);

/// This type is used to mark function arguments.
///
/// NOTE: The main reason for this to exist (except the type-safety) is that the value is
/// transparently serialized and deserialized as a base64-encoded string when serde is used
/// (serde_json).
#[derive(Debug, Clone, PartialEq, Eq, DeriveAsRef, DeriveFrom, BorshSerialize, BorshDeserialize)]
#[as_ref(forward)]
pub struct FunctionArgs(Vec<u8>);

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

#[derive(Debug)]
pub struct StateChangeWithCause {
    pub cause: StateChangeCause,
    pub value: StateChangeValue,
}

pub type StateChanges = Vec<StateChangeWithCause>;

#[easy_ext::ext(StateChangesExt)]
impl StateChanges {
    pub fn from_account_changes(
        raw_changes: impl Iterator<Item = Result<RawStateChangesWithTrieKey, std::io::Error>>,
    ) -> Result<StateChanges, std::io::Error> {
        let mut state_changes = Self::new();

        for raw_change in raw_changes {
            let RawStateChangesWithTrieKey { trie_key, changes } = raw_change?;
            let account_id = match trie_key {
                TrieKey::Account { account_id } => account_id,
                _ => panic!("Conflicting data stored under TrieKey::Account prefix"),
            };
            state_changes.extend(changes.into_iter().map(|RawStateChange { cause, data }| {
                StateChangeWithCause {
                    cause,
                    value: if let Some(change_data) = data {
                        StateChangeValue::AccountUpdate {
                            account_id: account_id.clone(),
                            account: <_>::try_from_slice(&change_data)
                                .expect("Failed to parse internally stored account information"),
                        }
                    } else {
                        StateChangeValue::AccountDeletion { account_id: account_id.clone() }
                    },
                }
            }));
        }

        Ok(state_changes)
    }

    pub fn from_access_key_changes(
        raw_changes: impl Iterator<Item = Result<RawStateChangesWithTrieKey, std::io::Error>>,
    ) -> Result<StateChanges, std::io::Error> {
        let mut state_changes = Self::new();

        for raw_change in raw_changes {
            let RawStateChangesWithTrieKey { trie_key, changes } = raw_change?;
            let (account_id, public_key) = match trie_key {
                TrieKey::AccessKey { account_id, public_key } => (account_id, public_key),
                _ => panic!("Conflicting data stored under TrieKey::AccessKey prefix"),
            };
            state_changes.extend(changes.into_iter().map(|RawStateChange { cause, data }| {
                StateChangeWithCause {
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
                }
            }));
        }

        Ok(state_changes)
    }

    pub fn from_contract_code_changes(
        raw_changes: impl Iterator<Item = Result<RawStateChangesWithTrieKey, std::io::Error>>,
    ) -> Result<StateChanges, std::io::Error> {
        let mut state_changes = Self::new();

        for raw_change in raw_changes {
            let RawStateChangesWithTrieKey { trie_key, changes } = raw_change?;
            let account_id = match trie_key {
                TrieKey::ContractCode { account_id } => account_id,
                _ => panic!("Conflicting data stored under TrieKey::ContractCode prefix"),
            };
            state_changes.extend(changes.into_iter().map(|RawStateChange { cause, data }| {
                StateChangeWithCause {
                    cause,
                    value: if let Some(change_data) = data {
                        StateChangeValue::ContractCodeUpdate {
                            account_id: account_id.clone(),
                            code: change_data.into(),
                        }
                    } else {
                        StateChangeValue::ContractCodeDeletion { account_id: account_id.clone() }
                    },
                }
            }));
        }

        Ok(state_changes)
    }

    pub fn from_data_changes(
        raw_changes: impl Iterator<Item = Result<RawStateChangesWithTrieKey, std::io::Error>>,
    ) -> Result<StateChanges, std::io::Error> {
        let mut state_changes = Self::new();

        for raw_change in raw_changes {
            let RawStateChangesWithTrieKey { trie_key, changes } = raw_change?;
            let (account_id, data_key) = match trie_key {
                TrieKey::ContractData { account_id, key } => (account_id, key),
                _ => panic!("Conflicting data stored under TrieKey::ContractData prefix"),
            };
            state_changes.extend(changes.into_iter().map(|RawStateChange { cause, data }| {
                StateChangeWithCause {
                    cause,
                    value: if let Some(change_data) = data {
                        StateChangeValue::DataUpdate {
                            account_id: account_id.clone(),
                            key: data_key.to_vec().into(),
                            value: change_data.into(),
                        }
                    } else {
                        StateChangeValue::DataDeletion {
                            account_id: account_id.clone(),
                            key: data_key.to_vec().into(),
                        }
                    },
                }
            }));
        }

        Ok(state_changes)
    }
}

#[derive(PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize, Serialize)]
pub struct StateRootNode {
    /// in Nightshade, data is the serialized TrieNodeWithSize
    pub data: Vec<u8>,
    /// in Nightshade, memory_usage is a field of TrieNodeWithSize
    pub memory_usage: u64,
}

impl StateRootNode {
    pub fn empty() -> Self {
        StateRootNode { data: vec![], memory_usage: 0 }
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
    DeriveAsRef,
    BorshSerialize,
    BorshDeserialize,
    Serialize,
)]
#[as_ref(forward)]
pub struct EpochId(pub CryptoHash);

/// Stores validator and its stake.
#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct ValidatorStake {
    /// Account that stakes money.
    pub account_id: AccountId,
    /// Public key of the proposed validator.
    pub public_key: PublicKey,
    /// Stake / weight of the validator.
    pub stake: Balance,
}

/// Stores validator and its stake for two consecutive epochs.
/// It is necessary because the blocks on the epoch boundary need to contain approvals from both
/// epochs.
#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct ApprovalStake {
    /// Account that stakes money.
    pub account_id: AccountId,
    /// Public key of the proposed validator.
    pub public_key: PublicKey,
    /// Stake / weight of the validator.
    pub stake_this_epoch: Balance,
    pub stake_next_epoch: Balance,
}

impl ValidatorStake {
    pub fn new(account_id: AccountId, public_key: PublicKey, stake: Balance) -> Self {
        ValidatorStake { account_id, public_key, stake }
    }

    pub fn get_approval_stake(&self, is_next_epoch: bool) -> ApprovalStake {
        ApprovalStake {
            account_id: self.account_id.clone(),
            public_key: self.public_key.clone(),
            stake_this_epoch: if is_next_epoch { 0 } else { self.stake },
            stake_next_epoch: if is_next_epoch { self.stake } else { 0 },
        }
    }
}

/// Information after block was processed.
#[derive(Debug, PartialEq, BorshSerialize, BorshDeserialize, Serialize, Clone, Eq)]
pub struct BlockExtra {
    pub challenges_result: ChallengesResult,
}

/// Information after chunk was processed, used to produce or check next chunk.
#[derive(Debug, PartialEq, BorshSerialize, BorshDeserialize, Serialize, Clone, Eq)]
pub struct ChunkExtra {
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
    pub fn new(
        state_root: &StateRoot,
        outcome_root: CryptoHash,
        validator_proposals: Vec<ValidatorStake>,
        gas_used: Gas,
        gas_limit: Gas,
        balance_burnt: Balance,
    ) -> Self {
        Self {
            state_root: state_root.clone(),
            outcome_root,
            validator_proposals,
            gas_used,
            gas_limit,
            balance_burnt,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BlockId {
    Height(BlockHeight),
    Hash(CryptoHash),
}

pub type MaybeBlockId = Option<BlockId>;

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncCheckpoint {
    Genesis,
    EarliestAvailable,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, Serialize, Deserialize)]
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

#[derive(Default, BorshSerialize, BorshDeserialize, Serialize, Clone, Debug, PartialEq)]
pub struct ValidatorStats {
    pub produced: NumBlocks,
    pub expected: NumBlocks,
}

#[derive(Debug)]
pub struct BlockChunkValidatorStats {
    pub block_stats: ValidatorStats,
    pub chunk_stats: ValidatorStats,
}

/// Reasons for removing a validator from the validator set.
#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
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
        #[serde(with = "u128_dec_format", rename = "stake_u128")]
        stake: Balance,
        #[serde(with = "u128_dec_format", rename = "threshold_u128")]
        threshold: Balance,
    },
    /// Enough stake but is not chosen because of seat limits.
    DidNotGetASeat,
}

#[derive(Serialize, Deserialize, Clone)]
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
