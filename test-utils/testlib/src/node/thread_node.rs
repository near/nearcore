use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use actix::System;

use near::config::Config;
use near::{start_with_config, NearConfig};
use near_client::ClientActor;
use near_primitives::crypto::signer::InMemorySigner;
use near_primitives::types::AccountId;

use crate::node::Node;
use crate::tokio_utils::ShutdownableThread;
use crate::user::User;

pub enum ThreadNodeState {
    Stopped,
    Running(ShutdownableThread),
}

pub struct ThreadNode {
    pub config: NearConfig,
    pub state: ThreadNodeState,
    use_rpc_user: bool,
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
        // self.config.block_producer.clone().expect("Must have a signer").signer
        Arc::new(InMemorySigner::from_seed(
            &self.account_id().unwrap(),
            &self.account_id().unwrap(),
        ))
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
        unimplemented!();
        //        if self.use_rpc_user {
        //            Box::new(RpcUser::new(SocketAddr::new(
        //                IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
        //                self.config.rpc_cfg.rpc_port,
        //            )))
        //        } else {
        //            Box::new(ThreadUser::new(self.client.clone()))
        //        }
    }
}

impl ThreadNode {
    /// Side effects: create storage, open database, lock database
    pub fn new(config: NearConfig) -> ThreadNode {
        ThreadNode { config, state: ThreadNodeState::Stopped, use_rpc_user: false }
    }

    /// Enables or disables using `RPCUser` instead of `ThreadUser`.
    pub fn use_rpc_user(&mut self, value: bool) {
        self.use_rpc_user = value;
    }
}
