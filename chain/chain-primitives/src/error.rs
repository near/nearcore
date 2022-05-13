use std::fmt::{self, Display};
use std::io;

use chrono::DateTime;
use near_primitives::time::Utc;

use tracing::error;

use near_primitives::block::BlockValidityError;
use near_primitives::challenge::{ChunkProofs, ChunkState};
use near_primitives::errors::{EpochError, StorageError};
use near_primitives::serialize::to_base;
use near_primitives::shard_layout::ShardLayoutError;
use near_primitives::sharding::{ChunkHash, ShardChunkHeader};
use near_primitives::types::{BlockHeight, EpochId, ShardId};

#[derive(thiserror::Error, Debug)]
pub enum QueryError {
    #[error("Account ID {requested_account_id} is invalid")]
    InvalidAccount {
        requested_account_id: near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    },
    #[error("Account {requested_account_id} does not exist while viewing")]
    UnknownAccount {
        requested_account_id: near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    },
    #[error(
        "Contract code for contract ID {contract_account_id} has never been observed on the node"
    )]
    NoContractCode {
        contract_account_id: near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    },
    #[error("Access key for public key {public_key} does not exist while viewing")]
    UnknownAccessKey {
        public_key: near_crypto::PublicKey,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    },
    #[error("Internal error occurred: {error_message}")]
    InternalError {
        error_message: String,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    },
    #[error("Function call returned an error: {error_message}")]
    ContractExecutionError {
        error_message: String,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    },
    #[error("The state of account {requested_account_id} is too large")]
    TooLargeContractState {
        requested_account_id: near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_primitives::hash::CryptoHash,
    },
}

#[derive(Debug)]
pub struct Error {
    inner: anyhow::Error,
}

#[derive(Clone, Eq, PartialEq, Debug, thiserror::Error)]
pub enum ErrorKind {
    /// The block is already known
    #[error("Block is known: {0}")]
    BlockKnown(#[from] BlockKnownError),
    /// Orphan block.
    #[error("Orphan")]
    Orphan,
    /// Chunk is missing.
    #[error("Chunk Missing (unavailable on the node): {0:?}")]
    ChunkMissing(ChunkHash),
    /// Chunks missing with header info.
    #[error("Chunks Missing: {0:?}")]
    ChunksMissing(Vec<ShardChunkHeader>),
    /// Block time is before parent block time.
    #[error("Invalid Block Time: block time {1} before previous {0}")]
    InvalidBlockPastTime(DateTime<Utc>, DateTime<Utc>),
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
    InvalidChunkProofs(Box<ChunkProofs>),
    /// Invalid chunk state.
    #[error("Invalid Chunk State")]
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
    #[error("Epoch Out Of Bounds: {:?}", _0)]
    EpochOutOfBounds(EpochId),
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
    GCError(String),
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

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

impl Error {
    /// Return the [`ErrorKind`] of the error.
    pub fn kind(&self) -> ErrorKind {
        match self.inner.downcast_ref::<ErrorKind>() {
            Some(kind) => kind.clone(),
            None => ErrorKind::Other(self.inner.to_string()),
        }
    }

    /// Return the root cause of the error.
    pub fn cause(&self) -> &(dyn std::error::Error + 'static) {
        self.inner.root_cause()
    }

    /// Return the error's backtrace.
    pub fn backtrace(&self) -> impl std::fmt::Debug + Display + '_ {
        self.inner.backtrace()
    }

    pub fn is_bad_data(&self) -> bool {
        match self.kind() {
            ErrorKind::BlockKnown(_)
            | ErrorKind::Orphan
            | ErrorKind::ChunkMissing(_)
            | ErrorKind::ChunksMissing(_)
            | ErrorKind::InvalidChunkHeight
            | ErrorKind::IOErr(_)
            | ErrorKind::Other(_)
            | ErrorKind::ValidatorError(_)
            | ErrorKind::EpochOutOfBounds(_)
            | ErrorKind::ChallengedBlockOnChain
            | ErrorKind::StorageError(_)
            | ErrorKind::GCError(_)
            | ErrorKind::DBNotFoundErr(_) => false,
            ErrorKind::InvalidBlockPastTime(_, _)
            | ErrorKind::InvalidBlockFutureTime(_)
            | ErrorKind::InvalidBlockHeight(_)
            | ErrorKind::InvalidBlockProposer
            | ErrorKind::InvalidChunk
            | ErrorKind::InvalidChunkProofs(_)
            | ErrorKind::InvalidChunkState(_)
            | ErrorKind::InvalidChunkMask
            | ErrorKind::InvalidStateRoot
            | ErrorKind::InvalidTxRoot
            | ErrorKind::InvalidChunkReceiptsRoot
            | ErrorKind::InvalidOutcomesProof
            | ErrorKind::InvalidChunkHeadersRoot
            | ErrorKind::InvalidChunkTxRoot
            | ErrorKind::InvalidReceiptsProof
            | ErrorKind::InvalidStatePayload
            | ErrorKind::InvalidTransactions
            | ErrorKind::InvalidChallenge
            | ErrorKind::MaliciousChallenge
            | ErrorKind::IncorrectNumberOfChunkHeaders
            | ErrorKind::InvalidEpochHash
            | ErrorKind::InvalidNextBPHash
            | ErrorKind::NotEnoughApprovals
            | ErrorKind::InvalidFinalityInfo
            | ErrorKind::InvalidValidatorProposals
            | ErrorKind::InvalidSignature
            | ErrorKind::InvalidApprovals
            | ErrorKind::InvalidGasLimit
            | ErrorKind::InvalidGasPrice
            | ErrorKind::InvalidGasUsed
            | ErrorKind::InvalidBalanceBurnt
            | ErrorKind::InvalidShardId(_)
            | ErrorKind::InvalidStateRequest(_)
            | ErrorKind::InvalidRandomnessBeaconOutput
            | ErrorKind::InvalidBlockMerkleRoot
            | ErrorKind::NotAValidator
            | ErrorKind::InvalidChallengeRoot => true,
        }
    }

    pub fn is_error(&self) -> bool {
        match self.kind() {
            ErrorKind::IOErr(_) | ErrorKind::Other(_) | ErrorKind::DBNotFoundErr(_) => true,
            _ => false,
        }
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self { inner: anyhow::Error::new(kind) }
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self { inner: anyhow::Error::new(ErrorKind::IOErr(error.to_string())) }
    }
}

impl std::error::Error for Error {}

impl From<EpochError> for Error {
    fn from(error: EpochError) -> Self {
        match error {
            EpochError::EpochOutOfBounds(epoch_id) => ErrorKind::EpochOutOfBounds(epoch_id),
            EpochError::MissingBlock(h) => ErrorKind::DBNotFoundErr(to_base(&h)),
            err => ErrorKind::ValidatorError(err.to_string()),
        }
        .into()
    }
}

impl From<ShardLayoutError> for Error {
    fn from(error: ShardLayoutError) -> Self {
        match error {
            ShardLayoutError::InvalidShardIdError { shard_id } => {
                ErrorKind::InvalidShardId(shard_id)
            }
        }
        .into()
    }
}

impl From<BlockValidityError> for Error {
    fn from(error: BlockValidityError) -> Self {
        match error {
            BlockValidityError::InvalidStateRoot => ErrorKind::InvalidStateRoot,
            BlockValidityError::InvalidReceiptRoot => ErrorKind::InvalidChunkReceiptsRoot,
            BlockValidityError::InvalidTransactionRoot => ErrorKind::InvalidTxRoot,
            BlockValidityError::InvalidChunkHeaderRoot => ErrorKind::InvalidChunkHeadersRoot,
            BlockValidityError::InvalidChunkMask => ErrorKind::InvalidChunkMask,
            BlockValidityError::InvalidChallengeRoot => ErrorKind::InvalidChallengeRoot,
        }
        .into()
    }
}

impl From<StorageError> for Error {
    fn from(error: StorageError) -> Self {
        ErrorKind::StorageError(error).into()
    }
}

#[derive(Clone, Eq, PartialEq, Debug, thiserror::Error)]
pub enum BlockKnownError {
    #[error("already known in header")]
    KnownInHeader,
    #[error("already known in head")]
    KnownInHead,
    #[error("already known in orphan")]
    KnownInOrphan,
    #[error("already known in missing chunks")]
    KnownInMissingChunks,
    #[error("already known in store")]
    KnownInStore,
}
