mod challenges;
mod doomslug;
mod gc;
mod simple_chain;
mod sync_chain;

use crate::block_processing_utils::BlockProcessingArtifact;
use crate::test_utils::{process_block_sync, wait_for_all_blocks_in_processing};
use crate::{Block, Chain, Error, Provenance};
use near_primitives::account::id::AccountId;
use near_primitives::utils::MaybeValidated;
use std::sync::Arc;

impl Chain {
    /// A wrapper function around process_block that doesn't trigger all the callbacks
    /// Only used in tests
    pub(crate) fn process_block_test(
        &mut self,
        me: &Option<AccountId>,
        block: Block,
    ) -> Result<(), Error> {
        let mut block_processing_artifacts = BlockProcessingArtifact::default();
        process_block_sync(
            self,
            me,
            MaybeValidated::from(block),
            Provenance::PRODUCED,
            &mut block_processing_artifacts,
        )
        .map(|_| {})
    }

    pub(crate) fn finish_processing_remaining_blocks(&mut self, me: &Option<AccountId>) {
        while wait_for_all_blocks_in_processing(self) {
            let mut block_processing_artifacts = BlockProcessingArtifact::default();
            let _ = self.postprocess_ready_blocks(
                me,
                &mut block_processing_artifacts,
                Arc::new(|_| {}),
            );
        }
    }
}
