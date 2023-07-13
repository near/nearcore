use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::state::FlatStateValue;
use near_primitives::types::BlockHeight;

#[derive(BorshSerialize, BorshDeserialize, Debug, Copy, Clone, PartialEq, Eq)]
pub struct BlockInfo {
    pub hash: CryptoHash,
    pub height: BlockHeight,
    pub prev_hash: CryptoHash,
}

impl BlockInfo {
    pub fn genesis(hash: CryptoHash, height: BlockHeight) -> BlockInfo {
        BlockInfo { hash, height, prev_hash: CryptoHash::default() }
    }
}

#[derive(strum::AsRefStr, Debug, PartialEq, Eq)]
pub enum FlatStorageError {
    /// This means we can't find a path from `flat_head` to the block. Includes
    /// `flat_head` hash and block hash, respectively.
    /// Should not result in node panic, because flat head can move during processing
    /// of some chunk.
    BlockNotSupported((CryptoHash, CryptoHash)),
    /// Internal error, caused by DB or in-memory data corruption. Should result
    /// in panic, because correctness of flat storage is not guaranteed afterwards.
    StorageInternalError(String),
}

impl From<FlatStorageError> for StorageError {
    fn from(err: FlatStorageError) -> Self {
        match err {
            FlatStorageError::BlockNotSupported((head_hash, block_hash)) => {
                StorageError::FlatStorageBlockNotSupported(format!(
                    "FlatStorage with head {:?} does not support this block {:?}",
                    head_hash, block_hash
                ))
            }
            FlatStorageError::StorageInternalError(_) => StorageError::StorageInternalError,
        }
    }
}

pub type FlatStorageResult<T> = Result<T, FlatStorageError>;

#[derive(BorshSerialize, BorshDeserialize, Debug, PartialEq, Eq)]
pub enum FlatStateValuesInliningMigrationStatus {
    Empty,
    InProgress,
    Finished,
}

#[derive(BorshSerialize, BorshDeserialize, Debug, PartialEq, Eq)]
pub enum FlatStorageStatus {
    /// Flat Storage is not supported.
    Disabled,
    /// Flat Storage is empty: either wasn't created yet or was deleted.
    Empty,
    /// Flat Storage is in the process of being created.
    Creation(FlatStorageCreationStatus),
    /// Flat Storage is ready to be used.
    Ready(FlatStorageReadyStatus),
}

impl Into<i64> for &FlatStorageStatus {
    /// Converts status to integer to export to prometheus later.
    /// Cast inside enum does not work because it is not fieldless.
    fn into(self) -> i64 {
        match self {
            FlatStorageStatus::Disabled => 0,
            FlatStorageStatus::Empty => 1,
            FlatStorageStatus::Ready(_) => 2,
            // 10..20 is reserved for creation statuses
            FlatStorageStatus::Creation(creation_status) => match creation_status {
                FlatStorageCreationStatus::SavingDeltas => 10,
                FlatStorageCreationStatus::FetchingState(_) => 11,
                FlatStorageCreationStatus::CatchingUp(_) => 12,
            },
        }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Debug, PartialEq, Eq)]
pub struct FlatStorageReadyStatus {
    pub flat_head: BlockInfo,
}

/// If a node has flat storage enabled but it didn't have flat storage data on disk, its creation should be initiated.
/// Because this is a heavy work requiring ~5h for testnet rpc node and ~10h for testnet archival node, we do it on
/// background during regular block processing.
/// This struct reveals what is the current status of creating flat storage data on disk.
#[derive(BorshSerialize, BorshDeserialize, Copy, Clone, Debug, PartialEq, Eq)]
pub enum FlatStorageCreationStatus {
    /// Flat storage state does not exist. We are saving `FlatStorageDelta`s to disk.
    /// During this step, we save current chain head, start saving all deltas for blocks after chain head and wait until
    /// final chain head moves after saved chain head.
    SavingDeltas,
    /// Flat storage state misses key-value pairs. We need to fetch Trie state to fill flat storage for some final chain
    /// head. It is the heaviest work, so it is done in multiple steps, see comment for `FetchingStateStatus` for more
    /// details.
    /// During each step we spawn background threads to fill some contiguous range of state keys.
    /// Status contains block hash for which we fetch the shard state and number of current step. Progress of each step
    /// is saved to disk, so if creation is interrupted during some step, we don't repeat previous steps, starting from
    /// the saved step again.
    FetchingState(FetchingStateStatus),
    /// Flat storage data exists on disk but block which is corresponds to is earlier than chain final head.
    /// We apply deltas from disk until the head reaches final head.
    /// Includes block hash of flat storage head.
    CatchingUp(CryptoHash),
}

/// Current step of fetching state to fill flat storage.
#[derive(BorshSerialize, BorshDeserialize, Copy, Clone, Debug, PartialEq, Eq)]
pub struct FetchingStateStatus {
    /// Hash of block on top of which we create flat storage.
    pub block_hash: CryptoHash,
    /// Number of the first state part to be fetched in this step.
    pub part_id: u64,
    /// Number of parts fetched in one step.
    pub num_parts_in_step: u64,
    /// Total number of state parts.
    pub num_parts: u64,
}

pub type FlatStateIterator<'a> =
    Box<dyn Iterator<Item = FlatStorageResult<(Vec<u8>, FlatStateValue)>> + 'a>;
