use protocol::TransactionPool;
use parking_lot::RwLock;
use std::mem;

pub struct Pool<T> {
    data: RwLock<Vec<T>>,
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

impl<T> Pool<T> {
    pub fn new() -> Pool<T> {
        Pool {
            data: RwLock::new(Vec::new())
        }
    }
}
