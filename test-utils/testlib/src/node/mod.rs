use std::sync::Arc;
use std::sync::RwLock;

use near_chain_configs::Genesis;
use near_crypto::{InMemorySigner, Signer};
use near_jsonrpc_primitives::errors::ServerError;
use near_primitives::state_record::StateRecord;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, Balance, NumSeats};
use near_primitives::validator_signer::InMemoryValidatorSigner;
use near_primitives::views::AccountView;
use neard::config::{
    create_testnet_configs, create_testnet_configs_from_seeds, Config, GenesisExt,
};
use neard::NearConfig;

pub use crate::node::process_node::ProcessNode;
pub use crate::node::runtime_node::RuntimeNode;
pub use crate::node::thread_node::ThreadNode;
use crate::user::{AsyncUser, User};
use near_primitives::contract::ContractCode;
use num_rational::Rational;

mod process_node;
mod runtime_node;
mod thread_node;

pub const TEST_BLOCK_FETCH_LIMIT: u64 = 5;
pub const TEST_BLOCK_MAX_SIZE: u32 = 1000;

pub fn configure_chain_spec() -> Genesis {
    Genesis::test(vec!["alice.near", "bob.near"], 2)
}

/// Config that can be used to start a node or connect to an existing node.
#[allow(clippy::large_enum_variant)]
pub enum NodeConfig {
    /// A node with only runtime and state that is used to run runtime tests.
    Runtime { account_id: AccountId },
    /// A complete node with network, RPC, client, consensus and all tasks running in a thead.
    /// Should be the default choice for the tests, since it provides the most control through the
    /// internal access.
    Thread(NearConfig),
    /// A complete noe running in a subprocess. Can be started and stopped, but besides that all
    /// interactions are limited to what is exposed through RPC.
    Process(NearConfig),
}

pub trait Node: Send + Sync {
    fn genesis(&self) -> &Genesis;

    fn account_id(&self) -> Option<AccountId>;

    fn start(&mut self);

    fn kill(&mut self);

    fn view_account(&self, account_id: &AccountId) -> Result<AccountView, String> {
        self.user().view_account(account_id)
    }

    fn get_access_key_nonce_for_signer(&self, account_id: &AccountId) -> Result<u64, String> {
        self.user().get_access_key_nonce_for_signer(account_id)
    }

    fn view_balance(&self, account_id: &AccountId) -> Result<Balance, String> {
        self.user().view_balance(account_id)
    }

    fn add_transaction(&self, transaction: SignedTransaction) -> Result<(), ServerError> {
        self.user().add_transaction(transaction)
    }

    fn signer(&self) -> Arc<dyn Signer>;

    fn block_signer(&self) -> Arc<dyn Signer> {
        unimplemented!()
    }

    fn is_running(&self) -> bool;

    fn user(&self) -> Box<dyn User>;

    fn async_user(&self) -> Box<dyn AsyncUser> {
        unimplemented!()
    }

    fn as_thread_ref(&self) -> &ThreadNode {
        unimplemented!()
    }

    fn as_thread_mut(&mut self) -> &mut ThreadNode {
        unimplemented!()
    }

    fn as_process_ref(&self) -> &ProcessNode {
        unimplemented!()
    }

    fn as_process_mut(&mut self) -> &mut ProcessNode {
        unimplemented!()
    }
}

impl dyn Node {
    pub fn new_sharable(config: NodeConfig) -> Arc<RwLock<dyn Node>> {
        match config {
            NodeConfig::Runtime { account_id } => {
                Arc::new(RwLock::new(RuntimeNode::new(&account_id)))
            }
            NodeConfig::Thread(config) => Arc::new(RwLock::new(ThreadNode::new(config))),
            NodeConfig::Process(config) => Arc::new(RwLock::new(ProcessNode::new(config))),
        }
    }

    pub fn new(config: NodeConfig) -> Box<dyn Node> {
        match config {
            NodeConfig::Runtime { account_id } => Box::new(RuntimeNode::new(&account_id)),
            NodeConfig::Thread(config) => Box::new(ThreadNode::new(config)),
            NodeConfig::Process(config) => Box::new(ProcessNode::new(config)),
        }
    }
}

fn near_configs_to_node_configs(
    configs: Vec<Config>,
    validator_signers: Vec<InMemoryValidatorSigner>,
    network_signers: Vec<InMemorySigner>,
    genesis: Genesis,
) -> Vec<NodeConfig> {
    let mut result = vec![];
    for i in 0..configs.len() {
        result.push(NodeConfig::Thread(NearConfig::new(
            configs[i].clone(),
            genesis.clone(),
            (&network_signers[i]).into(),
            Some(Arc::new(validator_signers[i].clone())),
        )))
    }
    result
}

pub fn create_nodes(num_nodes: usize, prefix: &str) -> Vec<NodeConfig> {
    let (configs, validator_signers, network_signers, genesis) =
        create_testnet_configs(1, num_nodes as NumSeats, 0, prefix, true, false);
    near_configs_to_node_configs(configs, validator_signers, network_signers, genesis)
}

pub fn create_nodes_from_seeds(seeds: Vec<String>) -> Vec<NodeConfig> {
    let code = near_test_contracts::rs_contract();
    let (configs, validator_signers, network_signers, mut genesis) =
        create_testnet_configs_from_seeds(seeds.clone(), 1, 0, true, false);
    genesis.config.gas_price_adjustment_rate = Rational::from_integer(0);
    for seed in seeds {
        let mut is_account_record_found = false;
        for record in genesis.records.as_mut() {
            if let StateRecord::Account { account_id: record_account_id, ref mut account } = record
            {
                if *record_account_id == seed {
                    is_account_record_found = true;
                    account.set_code_hash(ContractCode::new(code.to_vec(), None).get_hash());
                }
            }
        }
        assert!(is_account_record_found);
        genesis
            .records
            .as_mut()
            .push(StateRecord::Contract { account_id: seed, code: code.to_vec() });
    }
    near_configs_to_node_configs(configs, validator_signers, network_signers, genesis)
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
pub fn sample_queryable_node(nodes: &[Arc<RwLock<dyn Node>>]) -> usize {
    let num_nodes = nodes.len();
    let mut k = rand::random::<usize>() % num_nodes;
    while !nodes[k].read().unwrap().is_running() {
        k = rand::random::<usize>() % num_nodes;
    }
    k
}
