use std::path::PathBuf;
use std::sync::Arc;

use near_actix_test_utils::ShutdownableThread;
use near_chain_configs::Genesis;
use near_crypto::{InMemorySigner, KeyType, Signer};
use near_primitives::types::AccountId;
use nearcore::{start_with_config, NearConfig};

use crate::node::Node;
use crate::user::rpc_user::RpcUser;
use crate::user::User;

pub enum ThreadNodeState {
    Stopped,
    Running(ShutdownableThread),
}

pub struct ThreadNode {
    pub config: NearConfig,
    pub state: ThreadNodeState,
    pub signer: Arc<Signer>,
    pub dir: tempfile::TempDir,
    account_id: AccountId,
}

fn start_thread(config: NearConfig, path: PathBuf) -> ShutdownableThread {
    ShutdownableThread::start("test", move || {
        start_with_config(&path, config).expect("start_with_config");
    })
}

impl Node for ThreadNode {
    fn genesis(&self) -> &Genesis {
        &self.config.genesis
    }

    fn account_id(&self) -> Option<AccountId> {
        self.config.validator_signer.as_ref().map(|vs| vs.validator_id().clone())
    }

    fn start(&mut self) {
        let handle = start_thread(self.config.clone(), self.dir.path().to_path_buf());
        self.state = ThreadNodeState::Running(handle);
    }

    fn kill(&mut self) {
        let state = std::mem::replace(&mut self.state, ThreadNodeState::Stopped);
        match state {
            ThreadNodeState::Stopped => panic!("Node is not running"),
            ThreadNodeState::Running(handle) => {
                handle.shutdown();
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
        Box::new(RpcUser::new(&self.config.rpc_addr().unwrap(), self.account_id.clone(), self.signer.clone()))
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
        let account_id = config.validator_signer.as_ref().unwrap().validator_id().clone();
        let in_memory_signer = InMemorySigner::from_seed(
            account_id.clone(),
            KeyType::ED25519,
            account_id.as_ref(),
        );
        ThreadNode {
            config,
            state: ThreadNodeState::Stopped,
            signer: Arc::new(Signer::InMemorySigner(in_memory_signer)),
            dir: tempfile::Builder::new().prefix("thread_node").tempdir().unwrap(),
            account_id,
        }
    }
}
