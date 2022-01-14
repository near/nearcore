mod challenges;
mod doomslug;
mod gc;
mod simple_chain;
mod sync_chain;

use crate::types::Tip;
use crate::{Block, Chain, Error, Provenance};
use near_primitives::account::id::AccountId;
use near_primitives::utils::MaybeValidated;

impl Chain {
    /// A wrapper function around process_block that doesn't trigger all the callbacks
    /// Only used in tests
    pub(crate) fn process_block_test(
        &mut self,
        me: &Option<AccountId>,
        block: Block,
    ) -> Result<Option<Tip>, Error> {
        self.process_block(
            me,
            MaybeValidated::from(block),
            Provenance::PRODUCED,
            |_| {},
            |_| {},
            |_| {},
            |_| {},
        )
    }
}
