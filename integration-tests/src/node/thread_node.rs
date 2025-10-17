use std::sync::Arc;

use near_chain_configs::Genesis;
use near_crypto::{InMemorySigner, Signer};
use near_primitives::types::AccountId;
use nearcore::{NearConfig, start_with_config};

use crate::node::Node;
use crate::user::User;
use crate::user::rpc_user::RpcUser;
use near_async::ActorSystem;

pub enum ThreadNodeState {
    Stopped,
    Running(ActorSystem),
}

pub struct ThreadNode {
    pub config: NearConfig,
    pub state: ThreadNodeState,
    pub signer: Arc<Signer>,
    pub dir: tempfile::TempDir,
    account_id: AccountId,
    runtime: tokio::runtime::Runtime,
}

impl Drop for ThreadNode {
    fn drop(&mut self) {
        if let ThreadNodeState::Running(handle) = &self.state {
            handle.stop();
        }
    }
}

impl Node for ThreadNode {
    fn genesis(&self) -> &Genesis {
        &self.config.genesis
    }

    fn account_id(&self) -> Option<AccountId> {
        self.config.validator_signer.get().map(|vs| vs.validator_id().clone())
    }

    fn start(&mut self) {
        let handle = ActorSystem::new();
        self.runtime
            .block_on(start_with_config(self.dir.path(), self.config.clone(), handle.clone()))
            .expect("Failed to start ThreadNode");
        self.state = ThreadNodeState::Running(handle);
    }

    fn kill(&mut self) {
        let state = std::mem::replace(&mut self.state, ThreadNodeState::Stopped);
        match state {
            ThreadNodeState::Stopped => panic!("Node is not running"),
            ThreadNodeState::Running(handle) => {
                handle.stop();
                self.state = ThreadNodeState::Stopped;
            }
        }
    }

    fn signer(&self) -> Arc<Signer> {
        self.signer.clone()
    }

    fn is_running(&self) -> bool {
        match self.state {
            ThreadNodeState::Stopped => false,
            ThreadNodeState::Running(_) => true,
        }
    }

    fn user(&self) -> Box<dyn User> {
        Box::new(RpcUser::new(
            &self.config.rpc_addr().unwrap(),
            self.account_id.clone(),
            self.signer.clone(),
        ))
    }

    fn as_thread_ref(&self) -> &ThreadNode {
        self
    }

    fn as_thread_mut(&mut self) -> &mut ThreadNode {
        self
    }
}

impl ThreadNode {
    /// Side effects: create storage, open database, lock database
    pub fn new(config: NearConfig) -> ThreadNode {
        let account_id = config.validator_signer.get().unwrap().validator_id().clone();
        let signer = Arc::new(InMemorySigner::test_signer(&account_id));
        ThreadNode {
            config,
            state: ThreadNodeState::Stopped,
            signer,
            dir: tempfile::Builder::new().prefix("thread_node").tempdir().unwrap(),
            account_id,
            runtime: tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(1)
                .build()
                .expect("Failed to create Tokio runtime"),
        }
    }
}
