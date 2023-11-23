//! This module contains iterators that can be used to iterate over blocks in the database

pub mod last_blocks;

/// Iterate over the last N blocks in the blockchain
pub use last_blocks::LastNBlocksIterator;
