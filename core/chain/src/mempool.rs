use std::collections::HashSet;
use std::sync::Arc;
use parking_lot::RwLock;
use tokio::sync::mpsc::{Sender, Receiver};
use futures::{Future, Stream};
use log::error;
use transaction::SignedTransaction;
use crate::types::{ReceiptBlock, ChainPayload, SignedShardBlock};

/// mempool that stores transactions and receipts for a chain
#[derive(Default)]
struct Pool {
    transactions: HashSet<SignedTransaction>,
    receipts: HashSet<ReceiptBlock>,
}

impl Pool {
    fn add_transaction(&mut self, transaction: SignedTransaction) {
        self.transactions.insert(transaction);
    }

    fn add_receipt(&mut self, receipt: ReceiptBlock) {
        self.receipts.insert(receipt);
    }

    #[allow(unused)]
    fn produce_payload(&mut self) -> ChainPayload {
        let transactions: Vec<_> = self.transactions.drain().collect();
        let receipts: Vec<_> = self.receipts.drain().collect();
        ChainPayload { transactions, receipts }
    }

    fn import_block(&mut self, block: &SignedShardBlock) {
        for transaction in block.body.transactions.iter() {
            self.transactions.remove(transaction);
        }
        for receipt in block.body.receipts.iter() {
            self.receipts.remove(receipt);
        }
    }
}

/// spawns mempool and use channels to communicate to other parts of the code
// TODO: use payload_tx to send payload when we receive some sort of signal
pub fn spawn_mempool(
    block_rx: Receiver<SignedShardBlock>,
    _payload_tx: Sender<ChainPayload>,
    transaction_rx: Receiver<SignedTransaction>,
    receipt_rx: Receiver<ReceiptBlock>
) {
    let pool = Arc::new(RwLock::new(Pool::default()));
    let block_task = block_rx.for_each({
        let pool = pool.clone();
        move |b| {
            pool.write().import_block(&b);
            Ok(())
        }
    }).map_err(|e| error!("Error in receiving block: {:?}", e));
    tokio::spawn(block_task);
    let transaction_task = transaction_rx.for_each({
        let pool = pool.clone();
        move |t| {
            pool.write().add_transaction(t);
            Ok(())
        }
    }).map_err(|e| error!("Error in receiving transaction {:?}", e));
    tokio::spawn(transaction_task);
    let receipt_task = receipt_rx.for_each({
        let pool = pool.clone();
        move |r| {
            pool.write().add_receipt(r);
            Ok(())
        }
    }).map_err(|e| error!("Error in receiving receipt {:?}", e));
    tokio::spawn(receipt_task);
    
}

#[cfg(test)]
mod tests {
    use super::*;
    use primitives::hash::CryptoHash;
    use primitives::signature::DEFAULT_SIGNATURE;
    use transaction::{TransactionBody, SendMoneyTransaction};

    #[test]
    fn test_import_block() {
        let mut pool = Pool::default();
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