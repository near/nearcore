use borsh::{BorshDeserialize, BorshSerialize};

use crate::hash::CryptoHash;
use near_crypto::PublicKey;

/// Account identifier. Provides access to user's state.
pub type AccountId = String;
/// Hash used by a struct implementing the Merkle tree.
pub type MerkleHash = CryptoHash;
/// Validator identifier in current group.
pub type ValidatorId = usize;
/// Mask which validators participated in multi sign.
pub type ValidatorMask = Vec<bool>;
/// StorageUsage is used to count the amount of storage used by a contract.
pub type StorageUsage = u64;
/// StorageUsageChange is used to count the storage usage within a single contract call.
pub type StorageUsageChange = i64;
/// Nonce for transactions.
pub type Nonce = u64;
/// Index of the block.
pub type BlockIndex = u64;
/// Shard index, from 0 to NUM_SHARDS - 1.
pub type ShardId = u64;
/// Balance is type for storing amounts of tokens.
pub type Balance = u128;
/// Gas is a type for storing amount of gas.
pub type Gas = u64;

pub type ReceiptIndex = usize;
pub type PromiseId = Vec<ReceiptIndex>;

/// Hash used by to store state root and the number of parts the state is divided.
#[derive(Hash, Eq, PartialEq, Clone, Debug, BorshSerialize, BorshDeserialize, Default)]
pub struct StateRoot {
    pub hash: CryptoHash,
    pub num_parts: u64,
}

/// Epoch identifier -- wrapped hash, to make it easier to distinguish.
#[derive(Hash, Eq, PartialEq, Clone, Debug, BorshSerialize, BorshDeserialize, Default)]
pub struct EpochId(pub CryptoHash);

impl AsRef<[u8]> for EpochId {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

#[derive(Hash, Eq, PartialEq, Clone, Debug, BorshSerialize, BorshDeserialize, Default)]
pub struct Range(pub u64, pub u64);

/// Stores validator and its stake.
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct ValidatorStake {
    /// Account that stakes money.
    pub account_id: AccountId,
    /// Public key of the proposed validator.
    pub public_key: PublicKey,
    /// Stake / weight of the validator.
    pub amount: Balance,
}

impl ValidatorStake {
    pub fn new(account_id: AccountId, public_key: PublicKey, amount: Balance) -> Self {
        ValidatorStake { account_id, public_key, amount }
    }
}

/// Information after chunk was processed, used to produce or check next chunk.
#[derive(Debug, PartialEq, BorshSerialize, BorshDeserialize, Clone, Eq)]
pub struct ChunkExtra {
    /// Post state root after applying give chunk.
    pub state_root: StateRoot,
    /// Validator proposals produced by given chunk.
    pub validator_proposals: Vec<ValidatorStake>,
    /// Actually how much gas were used.
    pub gas_used: Gas,
    /// Gas limit, allows to increase or decrease limit based on expected time vs real time for computing the chunk.
    pub gas_limit: Gas,
    /// Total rent paid after processing the current chunk
    pub rent_paid: Balance,
}

impl ChunkExtra {
    pub fn new(
        state_root: &StateRoot,
        validator_proposals: Vec<ValidatorStake>,
        gas_used: Gas,
        gas_limit: Gas,
        rent_paid: Balance,
    ) -> Self {
        Self { state_root: state_root.clone(), validator_proposals, gas_used, gas_limit, rent_paid }
    }
}

/// Data structure for semver version and github tag or commit.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Version {
    pub version: String,
    pub build: String,
}
