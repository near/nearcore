use std::io;

use chrono::{DateTime, Utc};
use log::error;
use serde::{Deserialize, Serialize};

use near_primitives::block::BlockValidityError;
use near_primitives::challenge::{ChunkProofs, ChunkState};
use near_primitives::errors::{EpochError, StorageError};
use near_primitives::hash::CryptoHash;
use near_primitives::serialize::to_base;
use near_primitives::sharding::{ChunkHash, ShardChunkHeader};
use near_primitives::types::{BlockHeight, ShardId};

#[derive(thiserror::Error, Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub enum GCError {
    #[error("invalid gc stop height {0}")]
    InvalidGCStopHeight(BlockHeight),
    #[error("block {0} on canonical chain shouldn't have refcount 0")]
    InvalidBlockRefCount(CryptoHash),
    #[error("transaction {0} shouldn't have refcount 0")]
    InvalidTransactionRefCount(CryptoHash),
}

#[derive(thiserror::Error, Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub enum BlockProofError {
    #[error("block {0} is ahead of head block {1}")]
    BlockAheadOfHead(CryptoHash, CryptoHash),
    #[error("merkle tree node missing")]
    MerkleTreeNodeMissing,
}

/// Block or block header doesn't fit into the current chain
#[derive(thiserror::Error, Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub enum UnfitError {
    #[error("header already known")]
    HeaderAlreadyKnown,
    #[error("already known in head")]
    KnownInHead,
    #[error("already known in orphans")]
    KnownInOrphans,
    #[error("already known in blocks with missing chunks")]
    KnownInBlocksWithMissingChunks,
    #[error("already known in store")]
    KnownInStore,
}

#[derive(thiserror::Error, Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "content")]
pub enum Error {
    /// The block doesn't fit anywhere in our chain.
    #[error("Block is unfit: {0}")]
    Unfit(#[from] UnfitError),
    /// Orphan block.
    #[error("Orphan")]
    Orphan,
    /// Block is not available (e.g. garbage collected)
    #[error("Block Missing (unavailable on the node): {0}")]
    BlockMissing(CryptoHash),
    /// Chunk is missing.
    #[error("Chunk Missing (unavailable on the node): {0:?}")]
    ChunkMissing(ChunkHash),
    /// Chunks missing with header info.
    #[error("Chunks Missing: {0:?}")]
    ChunksMissing(Vec<ShardChunkHeader>),
    /// Block time is before parent block time.
    #[error("Invalid Block Time: block time {cur_timestamp} before previous {prev_timestamp}")]
    InvalidBlockPastTime { prev_timestamp: DateTime<Utc>, cur_timestamp: DateTime<Utc> },
    /// Block time is from too much in the future.
    #[error("Invalid Block Time: Too far in the future: {0}")]
    InvalidBlockFutureTime(DateTime<Utc>),
    /// Block height is invalid (not previous + 1).
    #[error("Invalid Block Height {0}")]
    InvalidBlockHeight(BlockHeight),
    /// Invalid block proposed signature.
    #[error("Invalid Block Proposer Signature")]
    InvalidBlockProposer,
    /// Invalid state root hash.
    #[error("Invalid State Root Hash")]
    InvalidStateRoot,
    /// Invalid block tx root hash.
    #[error("Invalid Block Tx Root Hash")]
    InvalidTxRoot,
    /// Invalid chunk receipts root hash.
    #[error("Invalid Chunk Receipts Root Hash")]
    InvalidChunkReceiptsRoot,
    /// Invalid chunk headers root hash.
    #[error("Invalid Chunk Headers Root Hash")]
    InvalidChunkHeadersRoot,
    /// Invalid chunk tx root hash.
    #[error("Invalid Chunk Tx Root Hash")]
    InvalidChunkTxRoot,
    /// Invalid receipts proof.
    #[error("Invalid Receipts Proof")]
    InvalidReceiptsProof,
    /// Invalid outcomes proof.
    #[error("Invalid Outcomes Proof")]
    InvalidOutcomesProof,
    /// Invalid state payload on state sync.
    #[error("Invalid State Payload")]
    InvalidStatePayload,
    /// Invalid transactions in the block.
    #[error("Invalid Transactions")]
    InvalidTransactions,
    /// Invalid Challenge Root (doesn't match actual challenge)
    #[error("Invalid Challenge Root")]
    InvalidChallengeRoot,
    /// Invalid challenge (wrong signature or format).
    #[error("Invalid Challenge")]
    InvalidChallenge,
    /// Incorrect (malicious) challenge (slash the sender).
    #[error("Malicious Challenge")]
    MaliciousChallenge,
    /// Incorrect number of chunk headers
    #[error("Incorrect Number of Chunk Headers")]
    IncorrectNumberOfChunkHeaders,
    /// Invalid chunk.
    #[error("Invalid Chunk")]
    InvalidChunk,
    /// One of the chunks has invalid proofs
    #[error("Invalid Chunk Proofs")]
    #[serde(rename = "InvalidChunkProofs")]
    #[serde(skip_deserializing)]
    InvalidChunkProofs(Box<ChunkProofs>),
    /// Invalid chunk state.
    #[error("Invalid Chunk State")]
    #[serde(rename = "InvalidChunkState")]
    #[serde(skip_deserializing)]
    InvalidChunkState(Box<ChunkState>),
    /// Invalid chunk mask
    #[error("Invalid Chunk Mask")]
    InvalidChunkMask,
    /// The chunk height is outside of the horizon
    #[error("Invalid Chunk Height")]
    InvalidChunkHeight,
    /// Invalid epoch hash
    #[error("Invalid Epoch Hash")]
    InvalidEpochHash,
    /// `next_bps_hash` doens't correspond to the actual next block producers set
    #[error("Invalid Next BP Hash")]
    InvalidNextBPHash,
    /// The block doesn't have approvals from 50% of the block producers
    #[error("Not enough approvals")]
    NotEnoughApprovals,
    /// The information about the last final block is incorrect
    #[error("Invalid finality info")]
    InvalidFinalityInfo,
    /// Invalid validator proposals in the block.
    #[error("Invalid Validator Proposals")]
    InvalidValidatorProposals,
    /// Invalid Signature
    #[error("Invalid Signature")]
    InvalidSignature,
    /// Invalid Approvals
    #[error("Invalid Approvals")]
    InvalidApprovals,
    /// Invalid Gas Limit
    #[error("Invalid Gas Limit")]
    InvalidGasLimit,
    /// Invalid Gas Limit
    #[error("Invalid Gas Price")]
    InvalidGasPrice,
    /// Invalid Gas Used
    #[error("Invalid Gas Used")]
    InvalidGasUsed,
    /// Invalid Balance Burnt
    #[error("Invalid Balance Burnt")]
    InvalidBalanceBurnt,
    /// Invalid shard id
    #[error("Shard id {0} does not exist")]
    InvalidShardId(ShardId),
    /// Invalid shard id
    #[error("Invalid state request: {0}")]
    InvalidStateRequest(String),
    /// Invalid VRF proof, or incorrect random_output in the header
    #[error("Invalid Randomness Beacon Output")]
    InvalidRandomnessBeaconOutput,
    /// Invalid block merkle root.
    #[error("Invalid Block Merkle Root")]
    InvalidBlockMerkleRoot,
    /// Someone is not a validator. Usually happens in signature verification
    #[error("Not A Validator")]
    NotAValidator,
    /// Validator error.
    #[error("Validator Error: {0}")]
    ValidatorError(String),
    /// Epoch out of bounds. Usually if received block is too far in the future or alternative fork.
    #[error("Epoch Out Of Bounds")]
    EpochOutOfBounds,
    /// A challenged block is on the chain that was attempted to become the head
    #[error("Challenged block on chain")]
    ChallengedBlockOnChain,
    /// IO Error.
    #[error("IO Error: {0}")]
    IOErr(String),
    /// Not found record in the DB.
    #[error("DB Not Found Error: {0}")]
    DBNotFoundErr(String),
    /// Storage error. Used for internal passing the error.
    #[error("Storage Error: {0}")]
    StorageError(#[from] StorageError),
    /// GC error.
    #[error("GC Error: {0}")]
    GCError(#[from] GCError),
    #[error("block proof error: {0}")]
    BlockProofError(#[from] BlockProofError),
    /// Anything else
    #[error("Other Error: {0}")]
    Other(String),
}

/// For now StorageError can happen at any time from ViewClient because of
/// the used isolation level + running ViewClient in a separate thread.
pub trait LogTransientStorageError {
    fn log_storage_error(self, message: &str) -> Self;
}

impl<T> LogTransientStorageError for Result<T, Error> {
    fn log_storage_error(self, message: &str) -> Self {
        if let Err(ref e) = self {
            error!(target: "client",
                   "Transient storage error: {}, {}",
                   message, e
            );
        }
        self
    }
}

impl Error {
    pub fn is_bad_data(&self) -> bool {
        match self {
            Error::Unfit(_)
            | Error::Orphan
            | Error::BlockMissing(_)
            | Error::ChunkMissing(_)
            | Error::ChunksMissing(_)
            | Error::InvalidChunkHeight
            | Error::IOErr(_)
            | Error::Other(_)
            | Error::ValidatorError(_)
            | Error::BlockProofError(_)
            // TODO: can be either way?
            | Error::EpochOutOfBounds
            | Error::ChallengedBlockOnChain
            | Error::GCError(_)
            | Error::DBNotFoundErr(_) => false,
            Error::InvalidBlockPastTime { .. }
            | Error::InvalidBlockFutureTime(_)
            | Error::InvalidBlockHeight(_)
            | Error::InvalidBlockProposer
            | Error::InvalidChunk
            | Error::InvalidChunkProofs(_)
            | Error::InvalidChunkState(_)
            | Error::InvalidChunkMask
            | Error::InvalidStateRoot
            | Error::InvalidTxRoot
            | Error::InvalidChunkReceiptsRoot
            | Error::InvalidOutcomesProof
            | Error::InvalidChunkHeadersRoot
            | Error::InvalidChunkTxRoot
            | Error::InvalidReceiptsProof
            | Error::InvalidStatePayload
            | Error::InvalidTransactions
            | Error::InvalidChallenge
            | Error::MaliciousChallenge
            | Error::IncorrectNumberOfChunkHeaders
            | Error::InvalidEpochHash
            | Error::InvalidNextBPHash
            | Error::NotEnoughApprovals
            | Error::InvalidFinalityInfo
            | Error::InvalidValidatorProposals
            | Error::InvalidSignature
            | Error::InvalidApprovals
            | Error::InvalidGasLimit
            | Error::InvalidGasPrice
            | Error::InvalidGasUsed
            | Error::InvalidBalanceBurnt
            | Error::InvalidShardId(_)
            | Error::InvalidStateRequest(_)
            | Error::InvalidRandomnessBeaconOutput
            | Error::InvalidBlockMerkleRoot
            | Error::NotAValidator
            | Error::InvalidChallengeRoot
            | Error::StorageError(_)=> true,
        }
    }

    pub fn is_error(&self) -> bool {
        match self {
            Error::IOErr(_) | Error::Other(_) | Error::DBNotFoundErr(_) => true,
            _ => false,
        }
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Error {
        Error::IOErr(error.to_string())
    }
}

impl From<String> for Error {
    fn from(error: String) -> Error {
        Error::Other(error)
    }
}

impl From<EpochError> for Error {
    fn from(error: EpochError) -> Self {
        match error {
            EpochError::EpochOutOfBounds => Error::EpochOutOfBounds,
            EpochError::MissingBlock(h) => Error::DBNotFoundErr(to_base(&h)),
            err => Error::ValidatorError(err.to_string()),
        }
        .into()
    }
}

impl From<BlockValidityError> for Error {
    fn from(error: BlockValidityError) -> Self {
        match error {
            BlockValidityError::InvalidStateRoot => Error::InvalidStateRoot,
            BlockValidityError::InvalidReceiptRoot => Error::InvalidChunkReceiptsRoot,
            BlockValidityError::InvalidTransactionRoot => Error::InvalidTxRoot,
            BlockValidityError::InvalidChunkHeaderRoot => Error::InvalidChunkHeadersRoot,
            BlockValidityError::InvalidNumChunksIncluded => Error::InvalidChunkMask,
            BlockValidityError::InvalidChallengeRoot => Error::InvalidChallengeRoot,
        }
        .into()
    }
}
