use primitives::types::{SignedTransaction, ReceiptTransaction};

pub struct TransactionPool {
    signed_txs: Vec<SignedTransaction>,
    receipts: Vec<ReceiptTransaction>,
}

impl TransactionPool {
    pub fn new() -> Self {
        TransactionPool {
            signed_txs: vec![],
            receipts: vec![],
        }
    }

    pub fn insert_signed_tx(&mut self, t: SignedTransaction) {
        self.signed_txs.push(t);
    }

    pub fn insert_receipt(&mut self, receipt: ReceiptTransaction) {
        self.receipts.push(receipt);
    }

    pub fn drain(&mut self) -> (Vec<SignedTransaction>, Vec<ReceiptTransaction>) {
        let signed_txs = self.signed_txs.drain(..).collect();
        let receipts = self.receipts.drain(..).collect();
        (signed_txs, receipts)
    }

    #[allow(unused)]
    pub fn len(&self) -> usize {
        self.signed_txs.len() + self.receipts.len()
    }
}