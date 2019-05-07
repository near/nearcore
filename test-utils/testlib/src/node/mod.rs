use std::panic;
use std::sync::Arc;

use node_runtime::chain_spec::ChainSpec;
use primitives::crypto::signer::InMemorySigner;
use primitives::transaction::SignedTransaction;
use primitives::types::{AccountId, Balance};

use crate::user::{AsyncUser, User};
use std::sync::RwLock;
pub mod runtime_node;
pub use self::runtime_node::RuntimeNode;

pub const TEST_BLOCK_FETCH_LIMIT: u64 = 5;
pub const TEST_BLOCK_MAX_SIZE: u32 = 1000;

pub fn configure_chain_spec() -> ChainSpec {
    ChainSpec::default_poa()
}

/// Config that can be used to start a node or connect to an existing node.
#[allow(clippy::large_enum_variant)]
pub enum NodeConfig {
    /// A node with only runtime and state that is used to run runtime tests
    Runtime { account_id: AccountId },
}

pub trait Node: Send + Sync {
    fn account_id(&self) -> Option<&AccountId>;

    fn start(&mut self);

    fn kill(&mut self);

    fn view_balance(&self, account_id: &AccountId) -> Result<Balance, String> {
        self.user().view_balance(account_id)
    }

    fn add_transaction(&self, transaction: SignedTransaction) -> Result<(), String> {
        self.user().add_transaction(transaction)
    }

    fn get_account_nonce(&self, account_id: &AccountId) -> Option<u64> {
        self.user().get_account_nonce(account_id)
    }

    fn signer(&self) -> Arc<InMemorySigner>;

    fn is_running(&self) -> bool;

    fn user(&self) -> Box<dyn User>;

    fn async_user(&self) -> Box<dyn AsyncUser> {
        unimplemented!()
    }
}

impl Node {
    pub fn new_sharable(config: NodeConfig) -> Arc<RwLock<dyn Node>> {
        match config {
            NodeConfig::Runtime { account_id } => {
                Arc::new(RwLock::new(RuntimeNode::new(&account_id)))
            }
        }
    }

    pub fn new(config: NodeConfig) -> Box<dyn Node> {
        match config {
            NodeConfig::Runtime { account_id } => Box::new(RuntimeNode::new(&account_id)),
        }
    }
}

pub fn sample_two_nodes(num_nodes: usize) -> (usize, usize) {
    let i = rand::random::<usize>() % num_nodes;
    // Should be a different node.
    let mut j = rand::random::<usize>() % (num_nodes - 1);
    if j >= i {
        j += 1;
    }
    (i, j)
}

/// Sample a node for sending a transaction/checking balance
pub fn sample_queryable_node(nodes: &[Arc<RwLock<Node>>]) -> usize {
    let num_nodes = nodes.len();
    let mut k = rand::random::<usize>() % num_nodes;
    while !nodes[k].read().unwrap().is_running() {
        k = rand::random::<usize>() % num_nodes;
    }
    k
}
