use crate::node::{Node, NodeType, NodeConfig};
use primitives::crypto::signer::InMemorySigner;
use std::sync::Arc;
use crate::user::User;
use crate::user::rpc_user::RpcUser;
use std::net::SocketAddr;

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
        unimplemented!()
    }

    fn config(&self) -> &NodeConfig {
        unimplemented!()
    }

    fn node_type(&self) -> NodeType {
        NodeType::RemoteNode
    }

    fn start(&mut self) {
        unimplemented!()
    }

    fn kill(&mut self) {
        unimplemented!()
    }

    fn signer(&self) -> Arc<InMemorySigner> {
        unimplemented!()
    }

    fn is_running(&self) -> bool {
        unimplemented!()
    }

    fn user(&self) -> Box<dyn User> {
        Box::new(RpcUser::new(self.addr))
    }
}
