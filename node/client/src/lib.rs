extern crate primitives;
extern crate storage;
extern crate node_runtime;

use primitives::types::TransactionBody;
use storage::Storage;
use node_runtime::Runtime;

pub struct Client {
    storage: Storage,
    runtime: Runtime,
}

impl Client {
    pub fn new() -> Self {
        Client {
            storage: Storage::new("storage/db/"),
            runtime: Runtime::default()
        }
    }

    pub fn receive_transaction(&self, t: &TransactionBody) {
        println!("{:?}", t);
    }
}
