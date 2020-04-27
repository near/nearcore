use std::fmt::{self, Display};
use std::io;

use chrono::{DateTime, Utc};
use failure::{Backtrace, Context, Fail};

use near_primitives::challenge::{ChunkProofs, ChunkState};
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::{ChunkHash, ShardChunkHeader};
use near_primitives::types::ShardId;

#[derive(Debug)]
pub struct Error {
    inner: Context<ErrorKind>,
}

#[derive(Clone, Eq, PartialEq, Debug, Fail)]
pub enum ErrorKind {
    /// The block doesn't fit anywhere in our chain.
    #[fail(display = "Block is unfit: {}", _0)]
    Unfit(String),
    /// Orphan block.
    #[fail(display = "Orphan")]
    Orphan,
    /// Block is not availiable (e.g. garbage collected)
    #[fail(display = "Block Missing (unavailable on the node): {}", _0)]
    BlockMissing(CryptoHash),
    /// Chunk is missing.
    #[fail(display = "Chunk Missing (unavailable on the node): {:?}", _0)]
    ChunkMissing(ChunkHash),
    /// Chunks missing with header info.
    #[fail(display = "Chunks Missing: {:?}", _0)]
    ChunksMissing(Vec<ShardChunkHeader>),
    /// Block time is before parent block time.
    #[fail(display = "Invalid Block Time: block time {} before previous {}", _1, _0)]
    InvalidBlockPastTime(DateTime<Utc>, DateTime<Utc>),
    /// Block time is from too much in the future.
    #[fail(display = "Invalid Block Time: Too far in the future: {}", _0)]
    InvalidBlockFutureTime(DateTime<Utc>),
    /// Block height is invalid (not previous + 1).
    #[fail(display = "Invalid Block Height")]
    InvalidBlockHeight,
    /// Invalid block proposed signature.
    #[fail(display = "Invalid Block Proposer Signature")]
    InvalidBlockProposer,
    /// Invalid block confirmation signature.
    #[fail(display = "Invalid Block Confirmation Signature")]
    InvalidBlockConfirmation,
    /// Invalid state root hash.
    #[fail(display = "Invalid State Root Hash")]
    InvalidStateRoot,
    /// Invalid block tx root hash.
    #[fail(display = "Invalid Block Tx Root Hash")]
    InvalidTxRoot,
    /// Invalid chunk receipts root hash.
    #[fail(display = "Invalid Chunk Receipts Root Hash")]
    InvalidChunkReceiptsRoot,
    /// Invalid chunk headers root hash.
    #[fail(display = "Invalid Chunk Headers Root Hash")]
    InvalidChunkHeadersRoot,
    /// Invalid chunk tx root hash.
    #[fail(display = "Invalid Chunk Tx Root Hash")]
    InvalidChunkTxRoot,
    /// Invalid receipts proof.
    #[fail(display = "Invalid Receipts Proof")]
    InvalidReceiptsProof,
    /// Invalid outcomes proof.
    #[fail(display = "Invalid Outcomes Proof")]
    InvalidOutcomesProof,
    /// Invalid state payload on state sync.
    #[fail(display = "Invalid State Payload")]
    InvalidStatePayload,
    /// Invalid transactions in the block.
    #[fail(display = "Invalid Transactions")]
    InvalidTransactions,
    /// Invalid challenge (wrong signature or format).
    #[fail(display = "Invalid Challenge")]
    InvalidChallenge,
    /// Incorrect (malicious) challenge (slash the sender).
    #[fail(display = "Malicious Challenge")]
    MaliciousChallenge,
    /// Incorrect number of chunk headers
    #[fail(display = "Incorrect Number of Chunk Headers")]
    IncorrectNumberOfChunkHeaders,
    /// Invalid chunk.
    #[fail(display = "Invalid Chunk")]
    InvalidChunk,
    /// One of the chunks has invalid proofs
    #[fail(display = "Invalid Chunk Proofs")]
    InvalidChunkProofs(ChunkProofs),
    /// Invalid chunk state.
    #[fail(display = "Invalid Chunk State")]
    InvalidChunkState(ChunkState),
    /// Invalid chunk mask
    #[fail(display = "Invalid Chunk Mask")]
    InvalidChunkMask,
    /// The chunk height is outside of the horizon
    #[fail(display = "Invalid Chunk Height")]
    InvalidChunkHeight,
    /// Invalid epoch hash
    #[fail(display = "Invalid Epoch Hash")]
    InvalidEpochHash,
    /// `next_bps_hash` doens't correspond to the actual next block producers set
    #[fail(display = "Invalid Next BP Hash")]
    InvalidNextBPHash,
    /// The block doesn't have approvals from 50% of the block producers
    #[fail(display = "Not enough approvals")]
    NotEnoughApprovals,
    /// The information about the last final block is incorrect
    #[fail(display = "Invalid finality info")]
    InvalidFinalityInfo,
    /// Invalid validator proposals in the block.
    #[fail(display = "Invalid Validator Proposals")]
    InvalidValidatorProposals,
    /// Invalid Signature
    #[fail(display = "Invalid Signature")]
    InvalidSignature,
    /// Invalid Approvals
    #[fail(display = "Invalid Approvals")]
    InvalidApprovals,
    /// Invalid Gas Limit
    #[fail(display = "Invalid Gas Limit")]
    InvalidGasLimit,
    /// Invalid Gas Limit
    #[fail(display = "Invalid Gas Price")]
    InvalidGasPrice,
    /// Invalid Gas Used
    #[fail(display = "Invalid Gas Used")]
    InvalidGasUsed,
    /// Invalid Validator Reward
    #[fail(display = "Invalid Validator Reward")]
    InvalidReward,
    /// Invalid Balance Burnt
    #[fail(display = "Invalid Balance Burnt")]
    InvalidBalanceBurnt,
    /// Invalid shard id
    #[fail(display = "Shard id {} does not exist", _0)]
    InvalidShardId(ShardId),
    /// Invalid shard id
    #[fail(display = "Invalid state request: {}", _0)]
    InvalidStateRequest(String),
    /// Invalid VRF proof, or incorrect random_output in the header
    #[fail(display = "Invalid Randomness Beacon Output")]
    InvalidRandomnessBeaconOutput,
    /// Someone is not a validator. Usually happens in signature verification
    #[fail(display = "Not A Validator")]
    NotAValidator,
    /// Validator error.
    #[fail(display = "Validator Error: {}", _0)]
    ValidatorError(String),
    /// Epoch out of bounds. Usually if received block is too far in the future or alternative fork.
    #[fail(display = "Epoch Out Of Bounds")]
    EpochOutOfBounds,
    /// A challenged block is on the chain that was attempted to become the head
    #[fail(display = "Challenged block on chain")]
    ChallengedBlockOnChain,
    /// IO Error.
    #[fail(display = "IO Error: {}", _0)]
    IOErr(String),
    /// Not found record in the DB.
    #[fail(display = "DB Not Found Error: {}", _0)]
    DBNotFoundErr(String),
    /// Storage error. Used for internal passing the error.
    #[fail(display = "Storage Error")]
    StorageError,
    /// GC error.
    #[fail(display = "GC Error: {}", _0)]
    GCError(String),
    /// Anything else
    #[fail(display = "Other Error: {}", _0)]
    Other(String),
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let cause = match self.cause() {
            Some(c) => format!("{}", c),
            None => String::from("Unknown"),
        };
        let backtrace = match self.backtrace() {
            Some(b) => format!("{}", b),
            None => String::from("Unknown"),
        };
        let output = format!("{} \n Cause: {} \n Backtrace: {}", self.inner, cause, backtrace);
        Display::fmt(&output, f)
    }
}

impl Error {
    pub fn kind(&self) -> ErrorKind {
        self.inner.get_context().clone()
    }

    pub fn cause(&self) -> Option<&dyn Fail> {
        self.inner.cause()
    }

    pub fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.backtrace()
    }

    pub fn is_bad_data(&self) -> bool {
        match self.kind() {
            ErrorKind::Unfit(_)
            | ErrorKind::Orphan
            | ErrorKind::BlockMissing(_)
            | ErrorKind::ChunkMissing(_)
            | ErrorKind::ChunksMissing(_)
            | ErrorKind::InvalidChunkHeight
            | ErrorKind::IOErr(_)
            | ErrorKind::Other(_)
            | ErrorKind::ValidatorError(_)
            // TODO: can be either way?
            | ErrorKind::EpochOutOfBounds
            | ErrorKind::ChallengedBlockOnChain
            | ErrorKind::StorageError
            | ErrorKind::GCError(_)
            | ErrorKind::DBNotFoundErr(_) => false,
            ErrorKind::InvalidBlockPastTime(_, _)
            | ErrorKind::InvalidBlockFutureTime(_)
            | ErrorKind::InvalidBlockHeight
            | ErrorKind::InvalidBlockProposer
            | ErrorKind::InvalidBlockConfirmation
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
            | ErrorKind::InvalidReward
            | ErrorKind::InvalidBalanceBurnt
            | ErrorKind::InvalidShardId(_)
            | ErrorKind::InvalidStateRequest(_)
            | ErrorKind::InvalidRandomnessBeaconOutput
            | ErrorKind::NotAValidator => true,
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
    fn from(kind: ErrorKind) -> Error {
        Error { inner: Context::new(kind) }
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Error {
        Error { inner: Context::new(ErrorKind::IOErr(error.to_string())) }
    }
}

impl From<String> for Error {
    fn from(error: String) -> Error {
        Error { inner: Context::new(ErrorKind::Other(error)) }
    }
}

impl std::error::Error for Error {}
