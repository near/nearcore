use std::path::PathBuf;
use std::sync::Arc;

use near::{start_with_config, NearConfig};
use near_chain_configs::GenesisConfig;
use near_crypto::{InMemorySigner, KeyType, Signer};
use near_primitives::types::AccountId;

use crate::actix_utils::ShutdownableThread;
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
    pub signer: Arc<InMemorySigner>,
    pub dir: tempdir::TempDir,
}

fn start_thread(config: NearConfig, path: PathBuf) -> ShutdownableThread {
    ShutdownableThread::start("test", move || {
        start_with_config(&path, config);
    })
}

impl Node for ThreadNode {
    fn genesis_config(&self) -> &GenesisConfig {
        &self.config.genesis_config
    }

    fn account_id(&self) -> Option<AccountId> {
        match &self.config.validator_signer {
            Some(vs) => Some(vs.validator_id().clone()),
            None => None,
        }
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
            }
        }
    }

    fn signer(&self) -> Arc<dyn Signer> {
        self.signer.clone()
    }

    fn is_running(&self) -> bool {
        match self.state {
            ThreadNodeState::Stopped => false,
            ThreadNodeState::Running(_) => true,
        }
    }

    fn user(&self) -> Box<dyn User> {
        let account_id = self.signer.account_id.clone();
        Box::new(RpcUser::new(&self.config.rpc_config.addr, account_id, self.signer.clone()))
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
        let signer = Arc::new(InMemorySigner::from_seed(
            &config.validator_signer.as_ref().unwrap().validator_id(),
            KeyType::ED25519,
            &config.validator_signer.as_ref().unwrap().validator_id(),
        ));
        ThreadNode {
            config,
            state: ThreadNodeState::Stopped,
            signer,
            dir: tempdir::TempDir::new("thread_node").unwrap(),
        }
    }
}
