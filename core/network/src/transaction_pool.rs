use protocol::TransactionPool;
use parking_lot::RwLock;
use std::mem;

pub struct Pool<T> {
    data: RwLock<Vec<T>>,
}

impl<T> Pool<T> {
    pub fn new() -> Pool<T> {
        Pool {
            data: RwLock::new(Vec::new())
        }
    }

    pub fn size(&self) -> usize {
        self.data.read().len()
    }
}

impl<T: Clone> Pool<T> {
    pub fn peek(&self) -> Vec<T> {
        self.data.read().iter().cloned().collect()
    }
}

impl<T> TransactionPool<T> for Pool<T> where T: Sync + Send {
    // get tx from pool. Should we clone or move?
    fn get(&self) -> Vec<T> {
        mem::replace(&mut self.data.write(), Vec::new())
    }

    fn put(&self, tx: T) {
        self.data.write().push(tx);
    }

    fn put_many(&self, mut txs: Vec<T>) {
        self.data.write().append(&mut txs);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use primitives::types::SignedTransaction;
    
    #[test]
    fn test_get_and_put() {
        let tx_pool = Pool::new();
        let txs: Vec<SignedTransaction> = (1..10).map(|_| SignedTransaction::default()).collect();
        let txs_cloned: Vec<SignedTransaction> = txs.iter().cloned().collect();
        tx_pool.put_many(txs);
        let tx_from_pool = tx_pool.peek();
        assert_eq!(tx_from_pool, txs_cloned);
    }

    #[test]
    fn test_put_one() {
        let tx_pool = Pool::new();
        let tx = SignedTransaction::default();
        tx_pool.put(tx);
        assert_eq!(tx_pool.size(), 1);
    }
}
