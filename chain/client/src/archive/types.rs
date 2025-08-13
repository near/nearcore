use near_primitives::errors::EpochError;
use std::fmt;

/// The ColdStoreCopyResult indicates if and what block was copied.
#[derive(Debug)]
pub enum ColdStoreCopyResult {
    // No block was copied. The cold head is up to date with the final head.
    NoBlockCopied,
    /// The final head block was copied. This is the latest block
    /// that could be copied until new block is finalized.
    LatestBlockCopied,
    /// A block older than the final head block was copied. There
    /// are more blocks that can be copied immediately.
    OtherBlockCopied,
}

/// The ColdStoreError indicates what errors were encountered while copying a blocks and running sanity checks.
#[derive(Debug)]
pub enum ColdStoreError {
    ColdHeadAheadOfFinalHeadError {
        cold_head_height: u64,
        hot_final_head_height: u64,
    },
    ColdHeadBehindHotTailError {
        cold_head_height: u64,
        hot_tail_height: u64,
    },
    SkippedBlocksBetweenColdHeadAndNextHeightError {
        cold_head_height: u64,
        next_height: u64,
        hot_final_head_height: u64,
    },
    ColdHeadHashReadError {
        cold_head_height: u64,
    },
    EpochError {
        e: EpochError,
    },
    Error {
        message: String,
    },
}

impl fmt::Display for ColdStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColdStoreError::ColdHeadAheadOfFinalHeadError {
                cold_head_height,
                hot_final_head_height,
            } => {
                write!(
                    f,
                    "Cold head is ahead of final head. cold head height: {cold_head_height} final head height {hot_final_head_height}"
                )
            }
            ColdStoreError::ColdHeadBehindHotTailError { cold_head_height, hot_tail_height } => {
                write!(
                    f,
                    "Cold head is behind hot tail. cold head height: {cold_head_height} hot tail height {hot_tail_height}"
                )
            }
            ColdStoreError::SkippedBlocksBetweenColdHeadAndNextHeightError {
                cold_head_height,
                next_height,
                hot_final_head_height,
            } => {
                write!(
                    f,
                    "All blocks between cold head and next height were skipped, but next height > hot final head. cold head {cold_head_height} next height to copy: {next_height} final head height {hot_final_head_height}"
                )
            }
            ColdStoreError::ColdHeadHashReadError { cold_head_height } => {
                write!(f, "Failed to read the cold head hash at height {cold_head_height}")
            }
            ColdStoreError::EpochError { e } => {
                write!(f, "Cold store copy error: {e}")
            }
            ColdStoreError::Error { message } => {
                write!(f, "Cold store copy error: {message}")
            }
        }
    }
}

impl std::error::Error for ColdStoreError {}

impl From<std::io::Error> for ColdStoreError {
    fn from(error: std::io::Error) -> Self {
        ColdStoreError::Error { message: error.to_string() }
    }
}

impl From<EpochError> for ColdStoreError {
    fn from(error: EpochError) -> Self {
        ColdStoreError::EpochError { e: error }
    }
}

#[derive(Debug)]
pub enum ColdStoreMigrationResult {
    /// Cold storage was already initialized
    NoNeedForMigration,
    /// Performed a successful cold storage migration
    SuccessfulMigration,
    /// Migration was interrupted by keep_going flag
    MigrationInterrupted,
}

pub fn cold_store_copy_result_to_string(
    result: &anyhow::Result<ColdStoreCopyResult, ColdStoreError>,
) -> &str {
    match result {
        Err(ColdStoreError::ColdHeadBehindHotTailError { .. }) => "cold_head_behind_hot_tail_error",
        Err(ColdStoreError::ColdHeadAheadOfFinalHeadError { .. }) => {
            "cold_head_ahead_of_final_head_error"
        }
        Err(ColdStoreError::SkippedBlocksBetweenColdHeadAndNextHeightError { .. }) => {
            "skipped_blocks_between_cold_head_and_next_height_error"
        }
        Err(ColdStoreError::ColdHeadHashReadError { .. }) => "cold_head_hash_read_error",
        Err(ColdStoreError::EpochError { .. }) => "epoch_error",
        Err(ColdStoreError::Error { .. }) => "error",
        Ok(ColdStoreCopyResult::NoBlockCopied) => "no_block_copied",
        Ok(ColdStoreCopyResult::LatestBlockCopied) => "latest_block_copied",
        Ok(ColdStoreCopyResult::OtherBlockCopied) => "other_block_copied",
    }
}
