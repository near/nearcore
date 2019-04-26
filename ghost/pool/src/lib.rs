use std::sync::Arc;

use primitives::transaction::SignedTransaction;
use near_chain::Block;

use crate::types::{ChainAdapter, Error};

mod types;

/// Transaction pool: keeps track of transactions that were not yet accepted into the block chain.
pub struct TransactionPool {
    pub chain_adapter: Arc<ChainAdapter>,
    // TODO: replace with proper map.
    pub transactions: Vec<SignedTransaction>,
}

impl TransactionPool {
    pub fn new(chain_adapter: Arc<ChainAdapter>) -> Self {
        TransactionPool {
            chain_adapter,
            transactions: vec![],
        }
    }

    /// Insert new transaction into the pool if didn't exist yet and passes validation.
    pub fn insert_transaction(&mut self, transaction: Vec<u8>) -> Result<(), Error> {
        let (transaction, ) = self.chain_adapter.validate_tx(&transaction)?;
        self.transactions.push(transaction);
        Ok(())
    }

    /// Take transactions from the pool, in the appropriate order to be put in a new block.
    /// Ensure that on average they will fit into expected weight.
    pub fn prepare_transactions(&mut self, expected_weight: u32) -> Result<Vec<SignedTransaction>, Error> {
        let result = self.transactions.drain(..).collect();
        Ok(result)
    }

    /// Quick reconciliation step - evict all transactions that already in the block
    /// or became invalid after it.
    pub fn reconcile_block(&mut self, block: Block) {
        // TODO
    }
}