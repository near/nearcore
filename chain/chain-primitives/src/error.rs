use near_primitives::block::BlockValidityError;
use near_primitives::challenge::{ChunkProofs, MaybeEncodedShardChunk};
use near_primitives::errors::{ChunkAccessError, EpochError, StorageError};
use near_primitives::shard_layout::ShardLayoutError;
use near_primitives::sharding::{BadHeaderForProtocolVersionError, ChunkHash, ShardChunkHeader};
use near_primitives::types::{BlockHeight, EpochId, ShardId, ShardIndex};
use near_time::Utc;
use std::io;

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

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The block is already known
    #[error("Block is known: {0}")]
    BlockKnown(#[from] BlockKnownError),
    #[error("Too many blocks being processed")]
    TooManyProcessingBlocks,
    /// Orphan block.
    #[error("Orphan")]
    Orphan,
    /// Chunk is missing.
    #[error("Chunk Missing (unavailable on the node): {0:?}")]
    ChunkMissing(ChunkHash),
    /// Chunks missing with header info.
    #[error("Chunks Missing: {0:?}")]
    ChunksMissing(Vec<ShardChunkHeader>),
    /// Optimistic block in processing.
    #[error("Optimistic block in processing")]
    OptimisticBlockInProcessing,
    /// Block time is before parent block time.
    #[error("Invalid Block Time: block time {1} before previous {0}")]
    InvalidBlockPastTime(Utc, Utc),
    /// Block time is from too much in the future.
    #[error("Invalid Block Time: Too far in the future: {0}")]
    InvalidBlockFutureTime(Utc),
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
    /// Invalid challenge (wrong signature or format).
    #[error("Invalid Challenge")]
    InvalidChallenge,
    /// Incorrect number of chunk headers
    #[error("Incorrect Number of Chunk Headers")]
    IncorrectNumberOfChunkHeaders,
    /// Invalid chunk.
    #[error("Invalid Chunk")]
    InvalidChunk(String),
    /// One of the chunks has invalid transactions order
    #[error("Invalid Chunk Transactions Order")]
    InvalidChunkTransactionsOrder(Box<MaybeEncodedShardChunk>),
    /// One of the chunks has invalid proofs
    #[error("Invalid Chunk Proofs")]
    InvalidChunkProofs(Box<ChunkProofs>),
    #[error("Invalid Chunk State Witness: {0}")]
    InvalidChunkStateWitness(String),
    #[error("Invalid Partial Chunk State Witness: {0}")]
    InvalidPartialChunkStateWitness(String),
    #[error("Invalid Chunk Endorsement")]
    InvalidChunkEndorsement,
    /// Invalid chunk mask
    #[error("Invalid Chunk Endorsement Bitmap")]
    InvalidChunkEndorsementBitmap(String),
    /// Invalid chunk mask
    #[error("Invalid Chunk Mask")]
    InvalidChunkMask,
    /// The chunk height is outside of the horizon
    #[error("Invalid Chunk Height")]
    InvalidChunkHeight,
    /// Invalid epoch hash
    #[error("Invalid Epoch Hash")]
    InvalidEpochHash,
    /// `next_bps_hash` doesn't correspond to the actual next block producers set
    #[error("Invalid Next BP Hash")]
    InvalidNextBPHash,
    /// The block has a protocol version that's outdated
    #[error("Invalid protocol version")]
    InvalidProtocolVersion,
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
    /// Invalid Gas Price
    #[error("Invalid Gas Price")]
    InvalidGasPrice,
    /// Invalid Gas Used
    #[error("Invalid Gas Used")]
    InvalidGasUsed,
    /// Invalid Balance Burnt
    #[error("Invalid Balance Burnt")]
    InvalidBalanceBurnt,
    /// Invalid Congestion Info
    #[error("Invalid Congestion Info: {0}")]
    InvalidCongestionInfo(String),
    /// Invalid bandwidth requests
    #[error("Invalid bandwidth requests - chunk extra doesn't match chunk header: {0}")]
    InvalidBandwidthRequests(String),
    /// Invalid shard id
    #[error("Shard id {0} does not exist")]
    InvalidShardId(ShardId),
    #[error("Shard index {0} does not exist")]
    InvalidShardIndex(ShardIndex),
    #[error("Shard id {0} does not have a parent")]
    NoParentShardId(ShardId),
    /// Invalid shard id
    #[error("Invalid state request: {0}")]
    InvalidStateRequest(String),
    /// Invalid VRF proof, or incorrect random_output in the header
    #[error("Invalid Randomness Beacon Output")]
    InvalidRandomnessBeaconOutput,
    /// Invalid block merkle root.
    #[error("Invalid Block Merkle Root")]
    InvalidBlockMerkleRoot,
    /// Invalid split shard ids.
    #[error("Invalid Split Shard Ids when resharding. shard_id: {0}, parent_shard_id: {1}")]
    InvalidSplitShardsIds(ShardId, ShardId),
    /// Someone is not a validator. Usually happens in signature verification
    #[error("Not A Validator: {0}")]
    NotAValidator(String),
    /// Someone is not a chunk validator. Happens if we're asked to validate a chunk we're not
    /// supposed to validate, or to verify a chunk approval signed by a validator that isn't
    /// supposed to validate the chunk.
    #[error("Not A Chunk Validator")]
    NotAChunkValidator,
    /// Validator error.
    #[error("Validator Error: {0}")]
    ValidatorError(String),
    /// Epoch out of bounds. Usually if received block is too far in the future or alternative fork.
    #[error("Epoch Out Of Bounds: {:?}", _0)]
    EpochOutOfBounds(EpochId),
    /// Block cannot be finalized.
    #[error("Block cannot be finalized")]
    CannotBeFinalized,
    /// IO Error.
    #[error("IO Error: {0}")]
    IOErr(#[from] io::Error),
    /// Not found record in the DB.
    #[error("DB Not Found Error: {0}")]
    DBNotFoundErr(String),
    /// Storage error. Used for internal passing the error.
    #[error("Storage Error: {0}")]
    StorageError(#[from] StorageError),
    /// GC error.
    #[error("GC Error: {0}")]
    GCError(String),
    /// Resharding error.
    #[error("Resharding Error: {0}")]
    ReshardingError(String),
    /// EpochSyncProof validation error.
    #[error("EpochSyncProof Validation Error: {0}")]
    InvalidEpochSyncProof(String),
    /// Invalid chunk header version for protocol version
    #[error(transparent)]
    BadHeaderForProtocolVersion(#[from] BadHeaderForProtocolVersionError),
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
        if let Err(err) = &self {
            tracing::error!(target: "chain", "Transient storage error: {message}, {err}");
        }
        self
    }
}

impl Error {
    pub fn is_bad_data(&self) -> bool {
        match self {
            Error::BlockKnown(_)
            | Error::TooManyProcessingBlocks
            | Error::Orphan
            | Error::ChunkMissing(_)
            | Error::ChunksMissing(_)
            | Error::OptimisticBlockInProcessing
            | Error::InvalidChunkHeight
            | Error::IOErr(_)
            | Error::Other(_)
            | Error::ValidatorError(_)
            | Error::EpochOutOfBounds(_)
            | Error::CannotBeFinalized
            | Error::StorageError(_)
            | Error::GCError(_)
            | Error::ReshardingError(_)
            | Error::DBNotFoundErr(_) => false,
            Error::InvalidBlockPastTime(_, _)
            | Error::InvalidBlockFutureTime(_)
            | Error::InvalidBlockHeight(_)
            | Error::InvalidBlockProposer
            | Error::InvalidChunk(_)
            | Error::InvalidChunkTransactionsOrder(_)
            | Error::InvalidChunkProofs(_)
            | Error::InvalidChunkStateWitness(_)
            | Error::InvalidPartialChunkStateWitness(_)
            | Error::InvalidChunkEndorsement
            | Error::InvalidChunkEndorsementBitmap(_)
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
            | Error::InvalidSplitShardsIds(_, _)
            | Error::IncorrectNumberOfChunkHeaders
            | Error::InvalidEpochHash
            | Error::InvalidEpochSyncProof(_)
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
            | Error::InvalidCongestionInfo(_)
            | Error::InvalidBandwidthRequests(_)
            | Error::InvalidShardId(_)
            | Error::InvalidShardIndex(_)
            | Error::NoParentShardId(_)
            | Error::InvalidStateRequest(_)
            | Error::InvalidRandomnessBeaconOutput
            | Error::InvalidBlockMerkleRoot
            | Error::InvalidProtocolVersion
            | Error::NotAValidator(_)
            | Error::NotAChunkValidator
            | Error::BadHeaderForProtocolVersion(_) => true,
        }
    }

    pub fn is_error(&self) -> bool {
        match self {
            Error::IOErr(_) | Error::Other(_) | Error::DBNotFoundErr(_) => true,
            _ => false,
        }
    }

    /// Some blockchain errors are reported in the prometheus metrics. In such cases a report might
    /// contain a label that specifies the type of error that has occurred. For example when the node
    /// receives a block with an invalid signature this would be reported as:
    ///  `near_num_invalid_blocks{error="invalid_signature"}`.
    /// This function returns the value of the error label for a specific instance of Error.
    pub fn prometheus_label_value(&self) -> &'static str {
        match self {
            Error::BlockKnown(_) => "block_known",
            Error::TooManyProcessingBlocks => "too_many_processing_blocks",
            Error::Orphan => "orphan",
            Error::ChunkMissing(_) => "chunk_missing",
            Error::ChunksMissing(_) => "chunks_missing",
            Error::OptimisticBlockInProcessing => "optimistic_block_in_processing",
            Error::InvalidChunkHeight => "invalid_chunk_height",
            Error::IOErr(_) => "io_err",
            Error::Other(_) => "other",
            Error::ValidatorError(_) => "validator_error",
            Error::EpochOutOfBounds(_) => "epoch_out_of_bounds",
            Error::CannotBeFinalized => "cannot_be_finalized",
            Error::StorageError(_) => "storage_error",
            Error::GCError(_) => "gc_error",
            Error::DBNotFoundErr(_) => "db_not_found_err",
            Error::InvalidBlockPastTime(_, _) => "invalid_block_past_time",
            Error::InvalidBlockFutureTime(_) => "invalid_block_future_time",
            Error::InvalidBlockHeight(_) => "invalid_block_height",
            Error::InvalidBlockProposer => "invalid_block_proposer",
            Error::InvalidChunk(_) => "invalid_chunk",
            Error::InvalidChunkTransactionsOrder(_) => "invalid_chunk_transactions_order",
            Error::InvalidChunkProofs(_) => "invalid_chunk_proofs",
            Error::InvalidChunkStateWitness(_) => "invalid_chunk_state_witness",
            Error::InvalidPartialChunkStateWitness(_) => "invalid_partial_chunk_state_witness",
            Error::InvalidChunkEndorsement => "invalid_chunk_endorsement",
            Error::InvalidChunkEndorsementBitmap(_) => "invalid_chunk_endorsement_bitmap",
            Error::InvalidChunkMask => "invalid_chunk_mask",
            Error::InvalidStateRoot => "invalid_state_root",
            Error::InvalidTxRoot => "invalid_tx_root",
            Error::InvalidChunkReceiptsRoot => "invalid_chunk_receipts_root",
            Error::InvalidOutcomesProof => "invalid_outcomes_proof",
            Error::InvalidChunkHeadersRoot => "invalid_chunk_headers_root",
            Error::InvalidChunkTxRoot => "invalid_chunk_tx_root",
            Error::InvalidReceiptsProof => "invalid_receipts_proof",
            Error::InvalidStatePayload => "invalid_state_payload",
            Error::InvalidTransactions => "invalid_transactions",
            Error::InvalidChallenge => "invalid_challenge",
            Error::InvalidSplitShardsIds(_, _) => "invalid_split_shard_ids",
            Error::IncorrectNumberOfChunkHeaders => "incorrect_number_of_chunk_headers",
            Error::InvalidEpochHash => "invalid_epoch_hash",
            Error::InvalidEpochSyncProof(_) => "invalid_epoch_sync_proof",
            Error::InvalidNextBPHash => "invalid_next_bp_hash",
            Error::NotEnoughApprovals => "not_enough_approvals",
            Error::InvalidFinalityInfo => "invalid_finality_info",
            Error::InvalidValidatorProposals => "invalid_validator_proposals",
            Error::InvalidSignature => "invalid_signature",
            Error::InvalidApprovals => "invalid_approvals",
            Error::InvalidGasLimit => "invalid_gas_limit",
            Error::InvalidGasPrice => "invalid_gas_price",
            Error::InvalidGasUsed => "invalid_gas_used",
            Error::InvalidBalanceBurnt => "invalid_balance_burnt",
            Error::InvalidCongestionInfo(_) => "invalid_congestion_info",
            Error::InvalidBandwidthRequests(_) => "invalid_bandwidth_requests",
            Error::InvalidShardId(_) => "invalid_shard_id",
            Error::InvalidShardIndex(_) => "invalid_shard_index",
            Error::NoParentShardId(_) => "no_parent_shard_id",
            Error::InvalidStateRequest(_) => "invalid_state_request",
            Error::InvalidRandomnessBeaconOutput => "invalid_randomness_beacon_output",
            Error::InvalidBlockMerkleRoot => "invalid_block_merkle_root",
            Error::InvalidProtocolVersion => "invalid_protocol_version",
            Error::NotAValidator(_) => "not_a_validator",
            Error::NotAChunkValidator => "not_a_chunk_validator",
            Error::ReshardingError(_) => "resharding_error",
            Error::BadHeaderForProtocolVersion(_) => "bad_header_for_protocol_version",
        }
    }
}

impl From<EpochError> for Error {
    fn from(error: EpochError) -> Self {
        match error {
            EpochError::EpochOutOfBounds(epoch_id) => Error::EpochOutOfBounds(epoch_id),
            EpochError::MissingBlock(h) => Error::DBNotFoundErr(format!("epoch block: {h}")),
            EpochError::NotAValidator(account_id, epoch_id) => {
                Error::NotAValidator(format!("account_id: {account_id}, epoch_id: {epoch_id:?}"))
            }
            err => Error::ValidatorError(err.to_string()),
        }
    }
}

pub trait EpochErrorResultToChainError<T> {
    fn into_chain_error(self) -> Result<T, Error>;
}

impl<T> EpochErrorResultToChainError<T> for Result<T, EpochError> {
    fn into_chain_error(self: Result<T, EpochError>) -> Result<T, Error> {
        self.map_err(|err| err.into())
    }
}

impl From<ShardLayoutError> for Error {
    fn from(error: ShardLayoutError) -> Self {
        match error {
            ShardLayoutError::InvalidShardIdError { shard_id } => Error::InvalidShardId(shard_id),
            ShardLayoutError::InvalidShardIndexError { shard_index } => {
                Error::InvalidShardIndex(shard_index)
            }
            ShardLayoutError::NoParentError { shard_id } => Error::NoParentShardId(shard_id),
        }
    }
}

impl From<BlockValidityError> for Error {
    fn from(error: BlockValidityError) -> Self {
        match error {
            BlockValidityError::InvalidStateRoot => Error::InvalidStateRoot,
            BlockValidityError::InvalidReceiptRoot => Error::InvalidChunkReceiptsRoot,
            BlockValidityError::InvalidTransactionRoot => Error::InvalidTxRoot,
            BlockValidityError::InvalidChunkHeaderRoot => Error::InvalidChunkHeadersRoot,
            BlockValidityError::InvalidChunkMask => Error::InvalidChunkMask,
        }
    }
}

impl From<ChunkAccessError> for Error {
    fn from(error: ChunkAccessError) -> Self {
        match error {
            ChunkAccessError::ChunkMissing(chunk_hash) => Error::ChunkMissing(chunk_hash),
        }
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
    #[error("already known in blocks in processing")]
    KnownInProcessing,
    #[error("already known in invalid blocks")]
    KnownAsInvalid,
}
