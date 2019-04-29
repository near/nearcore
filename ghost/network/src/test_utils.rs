use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::Duration;

use primitives::crypto::signature::get_key_pair;
use primitives::test_utils::get_key_pair_from_seed;

use crate::types::PeerInfo;
use crate::NetworkConfig;

/// Returns available port.
pub fn open_port() -> u16 {
    // use port 0 to allow the OS to assign an open port
    // TcpListener's Drop impl will unbind the port as soon as
    // listener goes out of scope
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

impl NetworkConfig {
    /// Returns network config with given seed used for peer id.
    pub fn from_seed(seed: &str, port: u16) -> Self {
        let (public_key, private_key) = get_key_pair_from_seed(seed);
        NetworkConfig {
            public_key,
            private_key,
            addr: Some(format!("0.0.0.0:{}", port).parse().unwrap()),
            boot_nodes: vec![],
            handshake_timeout: Duration::from_secs(60),
            reconnect_delay: Duration::from_secs(60),
            bootstrap_peers_period: Duration::from_secs(60),
            peer_max_count: 3,
        }
    }
}

pub fn convert_boot_nodes(boot_nodes: Vec<(&str, u16)>) -> Vec<PeerInfo> {
    let mut result = vec![];
    for (peer_seed, port) in boot_nodes {
        let (id, _) = get_key_pair_from_seed(peer_seed);
        result.push(PeerInfo::new(id.into(), format!("127.0.0.1:{}", port).parse().unwrap()))
    }
    result
}

impl PeerInfo {
    /// Creates random peer info.
    pub fn random() -> Self {
        let (id, _) = get_key_pair();
        PeerInfo {
            id: id.into(),
            addr: None,
            account_id: None
        }
    }
}