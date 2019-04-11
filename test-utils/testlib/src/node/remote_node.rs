use crate::node::Node;
use crate::user::rpc_user::RpcUser;
use crate::user::User;
use primitives::crypto::signer::InMemorySigner;
use std::net::SocketAddr;
use std::sync::Arc;

pub struct RemoteNode {
    pub addr: SocketAddr,
    pub signer: Arc<InMemorySigner>,
}

impl RemoteNode {
    pub fn new(addr: SocketAddr, signer: Arc<InMemorySigner>) -> Self {
        Self { addr, signer }
    }
}

impl Node for RemoteNode {
    fn account_id(&self) -> Option<&String> {
        Some(&self.signer.account_id)
    }

    fn start(&mut self) {
        unimplemented!()
    }

    fn kill(&mut self) {
        unimplemented!()
    }

    fn signer(&self) -> Arc<InMemorySigner> {
        self.signer.clone()
    }

    fn is_running(&self) -> bool {
        self.user().get_best_block_index().is_some()
    }

    fn user(&self) -> Box<dyn User> {
        Box::new(RpcUser::new(self.addr))
    }
}
