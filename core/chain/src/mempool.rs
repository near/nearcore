use std::collections::HashSet;
use transaction::SignedTransaction;
use crate::types::{ReceiptBlock, ChainPayload, SignedShardBlock};

/// mempool that stores transactions and receipts for a chain
pub struct Pool {
    transactions: HashSet<SignedTransaction>,
    receipts: HashSet<ReceiptBlock>,
}

impl Pool {
    pub fn new() -> Self {
        Pool { 
            transactions: HashSet::new(),
            receipts: HashSet::new(),
        }
    }

    pub fn add_transaction(&mut self, transaction: SignedTransaction) {
        self.transactions.insert(transaction);
    }

    pub fn add_receipt(&mut self, receipt: ReceiptBlock) {
        self.receipts.insert(receipt);
    }

    pub fn produce_payload(&mut self) -> ChainPayload {
        let transactions: Vec<_> = self.transactions.drain().collect();
        let receipts: Vec<_> = self.receipts.drain().collect();
        ChainPayload { transactions, receipts }
    }

    pub fn import_block(&mut self, block: &SignedShardBlock) {
        for transaction in block.body.transactions.iter() {
            self.transactions.remove(transaction);
        }
        for receipt in block.body.receipts.iter() {
            self.receipts.remove(receipt);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use primitives::hash::CryptoHash;
    use primitives::signature::DEFAULT_SIGNATURE;
    use transaction::{TransactionBody, SendMoneyTransaction};

    #[test]
    fn test_import_block() {
        let mut pool = Pool::new();
        let transaction = SignedTransaction::new(
            DEFAULT_SIGNATURE,
            TransactionBody::SendMoney(SendMoneyTransaction {
                nonce: 0,
                originator: "alice.near".to_string(),
                receiver: "bob.near".to_string(),
                amount: 1,
            })
        );
        pool.add_transaction(transaction.clone());
        assert_eq!(pool.transactions.len(), 1);
        let block = SignedShardBlock::new(
            0,
            0,
            CryptoHash::default(),
            CryptoHash::default(),
            vec![transaction],
            vec![],
            CryptoHash::default()
        );
        pool.import_block(&block);
        assert_eq!(pool.transactions.len(), 0);
    }
}