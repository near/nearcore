use std::sync::Arc;

use near::{start_with_config, NearConfig};
use near_primitives::crypto::signer::EDSigner;
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
}

fn start_thread(config: NearConfig) -> ShutdownableThread {
    ShutdownableThread::start("test", move || {
        let tmp_dir = tempdir::TempDir::new("thread_node").unwrap();
        start_with_config(tmp_dir.path(), config);
    })
}

impl Node for ThreadNode {
    fn account_id(&self) -> Option<AccountId> {
        match &self.config.block_producer {
            Some(bp) => Some(bp.account_id.clone()),
            None => None,
        }
    }

    fn start(&mut self) {
        let handle = start_thread(self.config.clone());
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

    fn signer(&self) -> Arc<dyn EDSigner> {
        self.config.block_producer.clone().unwrap().signer.clone()
    }

    fn is_running(&self) -> bool {
        match self.state {
            ThreadNodeState::Stopped => false,
            ThreadNodeState::Running(_) => true,
        }
    }

    fn user(&self) -> Box<dyn User> {
        Box::new(RpcUser::new(&self.config.rpc_config.addr))
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
        ThreadNode { config, state: ThreadNodeState::Stopped }
    }
}
