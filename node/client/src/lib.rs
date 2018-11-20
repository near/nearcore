extern crate node_runtime;
extern crate parking_lot;
extern crate primitives;
extern crate storage;

use node_runtime::Runtime;
use parking_lot::RwLock;
use primitives::types::{MerkleHash, SignedTransaction, ViewCall, ViewCallResult};
use storage::{StateDb, Storage};
use storage::{MemoryDB, TestBackend, TestBackendTransaction, TestChangesTrieStorage, TestExt};
use storage::{Externalities, OverlayedChanges, Backend};
use std::cell::RefCell;


pub struct Client {
    backend: RwLock<TestBackend>,
    runtime: Runtime,
}

impl Client {
    pub fn new(storage: Storage) -> Self {
        let state_db = MemoryDB::default();
        let last_root = Default::default();
        Client {
            runtime: Runtime::default(),
            backend: RwLock::new(TestBackend::new(state_db, last_root)),
        }
    }

    pub fn receive_transaction(&self, t: SignedTransaction) {
        println!("{:?}", t);
        // TODO: Put into the non-existent pool or TxFlow?
        let guard = self.backend.write();
        let (_, backend_transaction, new_root) = self
            .runtime
            .apply(&guard, vec![t]);

        // TODO: apply backend transaction and modify backend
        //self.state_db.replace(backend.into_storage());
        //self.last_root = new_root;
    }

    pub fn view_call(&self, view_call: &ViewCall) -> ViewCallResult {
        //let backend = TestBackend::new(self.state_db, self.last_root);
        let guard = self.backend.write();
        let ret = self.runtime
            .view(&guard, view_call);
        ret
    }
}
