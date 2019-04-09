use crate::node::{LocalNodeConfig, Node};
use crate::user::{ThreadUser, User};
use client::Client;
use primitives::crypto::signer::InMemorySigner;
use primitives::types::AccountId;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio_utils::ShutdownableThread;

pub enum ThreadNodeState {
    Stopped,
    Running(ShutdownableThread),
}

pub struct ThreadNode {
    pub config: LocalNodeConfig,
    pub client: Arc<Client<InMemorySigner>>,
    pub state: ThreadNodeState,
}

impl Node for ThreadNode {
    fn account_id(&self) -> Option<&AccountId> {
        self.config.client_cfg.account_id.as_ref()
    }

    fn start(&mut self) {
        let client = self.client.clone();
        let network_cfg = self.config.network_cfg.clone();
        let rpc_cfg = self.config.rpc_cfg.clone();
        let client_cfg = self.config.client_cfg.clone();
        let proxy_handlers = self.config.proxy_handlers.clone();
        let handle =
            alphanet::start_from_client(client, network_cfg, rpc_cfg, client_cfg, proxy_handlers);
        self.state = ThreadNodeState::Running(handle);
        thread::sleep(Duration::from_secs(1));
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

    fn signer(&self) -> Arc<InMemorySigner> {
        self.client.signer.clone().expect("Must have a signer")
    }

    fn as_thread_ref(&self) -> &ThreadNode {
        self
    }

    fn as_thread_mut(&mut self) -> &mut ThreadNode {
        self
    }

    fn is_running(&self) -> bool {
        match self.state {
            ThreadNodeState::Stopped => false,
            ThreadNodeState::Running(_) => true,
        }
    }

    fn user(&self) -> Box<dyn User> {
        Box::new(ThreadUser::new(self.client.clone()))
    }
}

impl ThreadNode {
    /// Side effects: create storage, open database, lock database
    pub fn new(config: LocalNodeConfig) -> ThreadNode {
        let signer = match &config.client_cfg.account_id {
            Some(account_id) => Some(Arc::new(InMemorySigner::from_seed(&account_id, &account_id))),
            None => None,
        };
        let client = Arc::new(Client::new(&config.client_cfg, signer));
        let state = ThreadNodeState::Stopped;
        ThreadNode { config, client, state }
    }
}
