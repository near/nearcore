extern crate network;
extern crate node_runtime;
extern crate parking_lot;
extern crate primitives;
extern crate storage;

use network::protocol::Transaction;
use node_runtime::Runtime;
use parking_lot::RwLock;
use primitives::traits::GenericResult;
use primitives::types::{MerkleHash, SignedTransaction, ViewCall, ViewCallResult};
use storage::{StateDb, Storage};

pub struct Client {
    state_db: RwLock<StateDb>,
    runtime: Runtime,
    last_root: RwLock<MerkleHash>,
}

impl Client {
    pub fn new(storage: Storage) -> Self {
        let state_db = StateDb::new(storage);
        let state_view = state_db.get_state_view();
        Client {
            runtime: Runtime::default(),
            state_db: RwLock::new(state_db),
            last_root: RwLock::new(state_view),
        }
    }

    pub fn receive_transaction(&self, t: SignedTransaction) {
        println!("{:?}", t);
        // TODO: Put into the non-existent pool or TxFlow?
        let mut state_db = self.state_db.write();
        let (_, new_root) = self
            .runtime
            .apply(&mut state_db, &self.last_root.read(), vec![t]);
        *self.last_root.write() = new_root;
    }

    pub fn view_call(&self, view_call: &ViewCall) -> ViewCallResult {
        let mut state_db = self.state_db.write();
        self.runtime
            .view(&mut state_db, &self.last_root.read(), view_call)
    }

    pub fn handle_signed_transaction<T: Transaction>(&self, t: &T) -> GenericResult {
        println!("{:?}", t);
        Ok(())
    }
}
