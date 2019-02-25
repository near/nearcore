use bit_set::BitSet;
use primitives::hash::CryptoHash;
use primitives::transaction::SignedTransaction;
use std::collections::HashMap;

struct TxExchangeEntry {
    tx: SignedTransaction,
    known_to: BitSet,
}

struct TxExchange {
    num_authorities: i64,
    entries: HashMap<CryptoHash, TxExchangeEntry>,
}

impl TxExchangeEntry {
    fn new(tx: SignedTransaction, known_to: BitSet) -> Self {
        TxExchangeEntry { tx, known_to }
    }
}

impl TxExchange {
    fn add_transaction(&mut self, tx: SignedTransaction) {
        self.entries.insert(tx.get_hash(), TxExchangeEntry::new(tx, BitSet::new()));
    }
}
