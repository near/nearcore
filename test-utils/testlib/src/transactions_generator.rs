//! Set of methods that construct transactions of various kind.

use std::sync::{Arc, RwLock};
use primitives::types::AccountId;
use crate::node::Node;
use std::collections::HashMap;
use primitives::transaction::SignedTransaction;

/// Tracks nonces for the accounts.
type Nonces = RwLock<HashMap<AccountId, u64>>;
/// Nodes that can be used to generate nonces
type Nodes = Vec<Arc<RwLock<dyn Node>>>;

/// Keeps the context that is needed to generate a random transaction.
struct Generator {
    pub nodes: Nodes,
    pub nonces: Nonces,
}

impl Generator {
    pub fn send_money(&mut self) -> SignedTransaction {
        unimplemented!()
    }
}
