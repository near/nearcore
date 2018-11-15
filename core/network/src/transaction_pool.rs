use protocol::TransactionPool;

pub struct Pool<T>(Vec<T>);

impl<T> TransactionPool<T> for Pool<T>
where
    T: Sync + Send,
{
    fn get(&mut self) -> Vec<T> {
        self.0.drain(0..).collect()
    }

    fn put(&mut self, tx: T) {
        self.0.push(tx);
    }

    fn put_many(&mut self, mut txs: Vec<T>) {
        self.0.append(&mut txs);
    }
}

impl<T> Default for Pool<T> {
    fn default() -> Self {
        Pool(Vec::new())
    }
}
