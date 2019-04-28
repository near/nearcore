use std::sync::Arc;

use near_chain::{Block, ValidTransaction};
use primitives::transaction::SignedTransaction;

use crate::types::{Error};

pub mod types;

/// Transaction pool: keeps track of transactions that were not yet accepted into the block chain.
pub struct TransactionPool {
    // TODO: replace with proper map.
    pub transactions: Vec<SignedTransaction>,
}

impl TransactionPool {
    pub fn new() -> Self {
        TransactionPool { transactions: vec![] }
    }

    /// Insert new transaction into the pool that passed validation.
    pub fn insert_transaction(&mut self, valid_transaction: ValidTransaction) {
        self.transactions.push(valid_transaction.transaction);
    }

    /// Take transactions from the pool, in the appropriate order to be put in a new block.
    /// Ensure that on average they will fit into expected weight.
    pub fn prepare_transactions(
        &mut self,
        expected_weight: u32,
    ) -> Result<Vec<SignedTransaction>, Error> {
        let result = self.transactions.drain(..).collect();
        Ok(result)
    }

    /// Quick reconciliation step - evict all transactions that already in the block
    /// or became invalid after it.
    pub fn reconcile_block(&mut self, block: &Block) {
        // TODO
    }
}
