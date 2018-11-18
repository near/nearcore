extern crate node_runtime;
extern crate parking_lot;
extern crate primitives;
extern crate storage;

use node_runtime::Runtime;
use parking_lot::RwLock;
use primitives::traits::StateTransitionRuntime;
use primitives::types::{SignedTransaction, ViewCall, ViewCallResult};
use storage::{StateDb, StateDbView, Storage};

pub struct Client {
    runtime: Runtime,
    last_state_view: RwLock<StateDbView>,
}

impl Client {
    pub fn new(db_path: &str) -> Self {
        let storage = Storage::new(db_path);
        let state_db = StateDb::new(storage);
        let state_view = state_db.get_state_view();
        Client {
            runtime: Runtime::default(),
            last_state_view: RwLock::new(state_view),
        }
    }

    pub fn receive_transaction(&self, t: SignedTransaction) {
        println!("{:?}", t);
        // TODO: Put into the non-existent pool or TxFlow?
        let mut last_state_view = self.last_state_view.write();
        let (_, new_state_view) = self.runtime.apply(&mut last_state_view, vec![t]);
        *last_state_view = new_state_view;
    }

    pub fn view_call(&self, v: &ViewCall) -> ViewCallResult {
        match self
            .runtime
            .get_account::<storage::StateDbView>(&self.last_state_view.read(), v.account)
        {
            Some(account) => ViewCallResult {
                account: v.account,
                amount: account.amount,
            },
            None => ViewCallResult {
                account: v.account,
                amount: 0,
            },
        }
    }
}
