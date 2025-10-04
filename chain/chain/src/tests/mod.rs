mod doomslug;
mod garbage_collection;
mod simple_chain;
mod spice_core;
mod sync_chain;

use crate::block_processing_utils::BlockProcessingArtifact;
use crate::test_utils::process_block_sync;
use crate::{Block, Chain, Error, Provenance};
use near_primitives::utils::MaybeValidated;
use std::sync::Arc;

impl Chain {
    /// A wrapper function around process_block that doesn't trigger all the callbacks
    /// Only used in tests
    pub(crate) fn process_block_test(&mut self, block: Arc<Block>) -> Result<(), Error> {
        let mut block_processing_artifacts = BlockProcessingArtifact::default();
        process_block_sync(
            self,
            MaybeValidated::from(block),
            Provenance::PRODUCED,
            &mut block_processing_artifacts,
        )
        .map(|_| {})
    }
}
