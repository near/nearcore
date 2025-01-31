//! This module contains iterators that can be used to iterate over blocks in the database

mod height_range;
mod last_blocks;

use std::rc::Rc;

use near_chain::{Block, ChainStore};
use near_primitives::types::BlockHeight;

/// Iterate over blocks between two block heights.
/// `from_height` and `to_height` are inclusive
pub use height_range::BlockHeightRangeIterator;

/// Iterate over the last N blocks in the blockchain
pub use last_blocks::LastNBlocksIterator;

/// Arguments that user can pass to a command to choose some subset of blocks
pub struct CommandArgs {
    /// Analyze the last N blocks
    pub last_blocks: Option<u64>,

    /// Analyze blocks from the given block height, inclusive
    pub from_block_height: Option<BlockHeight>,

    /// Analyze blocks up to the given block height, inclusive
    pub to_block_height: Option<BlockHeight>,
}

/// Produce to right iterator for a given set of command line arguments
pub fn make_block_iterator_from_command_args(
    command_args: CommandArgs,
    chain_store: Rc<ChainStore>,
) -> Option<Box<dyn Iterator<Item = Block>>> {
    // Make sure that only one type of argument is used (there is no mixing of last_blocks and from_block_height)
    let mut arg_types_used: u64 = 0;
    if command_args.last_blocks.is_some() {
        arg_types_used += 1;
    }
    if command_args.from_block_height.is_some() || command_args.from_block_height.is_some() {
        arg_types_used += 1;
    }

    if arg_types_used > 1 {
        panic!("It is illegal to mix multiple types of arguments specifying a subset of blocks");
    }

    if let Some(last_blocks) = command_args.last_blocks {
        return Some(Box::new(LastNBlocksIterator::new(last_blocks, chain_store)));
    }

    if command_args.from_block_height.is_some() || command_args.to_block_height.is_some() {
        return Some(Box::new(BlockHeightRangeIterator::new(
            command_args.from_block_height,
            command_args.to_block_height,
            chain_store,
        )));
    }

    None
}
