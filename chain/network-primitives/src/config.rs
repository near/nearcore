use crate::network_protocol::PeerInfo;
use crate::types::ROUTED_MESSAGE_TTL;
use near_crypto::{KeyType, PublicKey, SecretKey};
use near_primitives::types::AccountId;
use std::collections::{HashMap, HashSet};
use std::net::{AddrParseError, IpAddr, SocketAddr};
use std::str::FromStr;
use std::time::Duration;

/// Configuration for the peer-to-peer manager.
#[derive(Clone)]
pub struct NetworkConfig {
    pub public_key: PublicKey,
    pub secret_key: SecretKey,
    pub account_id: Option<AccountId>,
    pub addr: Option<SocketAddr>,
    pub boot_nodes: Vec<PeerInfo>,
    pub handshake_timeout: Duration,
    pub reconnect_delay: Duration,
    pub bootstrap_peers_period: Duration,
    /// Maximum number of active peers. Hard limit.
    pub max_num_peers: u32,
    /// Minimum outbound connections a peer should have to avoid eclipse attacks.
    pub minimum_outbound_peers: u32,
    /// Lower bound of the ideal number of connections.
    pub ideal_connections_lo: u32,
    /// Upper bound of the ideal number of connections.
    pub ideal_connections_hi: u32,
    /// Peers which last message is was within this period of time are considered active recent peers.
    pub peer_recent_time_window: Duration,
    /// Number of peers to keep while removing a connection.
    /// Used to avoid disconnecting from peers we have been connected since long time.
    pub safe_set_size: u32,
    /// Lower bound of the number of connections to archival peers to keep
    /// if we are an archival node.
    pub archival_peer_connections_lower_bound: u32,
    /// Duration of the ban for misbehaving peers.
    pub ban_window: Duration,
    /// Remove expired peers.
    pub peer_expiration_duration: Duration,
    /// Maximum number of peer addresses we should ever send on PeersRequest.
    pub max_send_peers: u32,
    /// Duration for checking on stats from the peers.
    pub peer_stats_period: Duration,
    /// Time to persist Accounts Id in the router without removing them.
    pub ttl_account_id_router: Duration,
    /// Number of hops a message is allowed to travel before being dropped.
    /// This is used to avoid infinite loop because of inconsistent view of the network
    /// by different nodes.
    pub routed_message_ttl: u8,
    /// Maximum number of routes that we should keep track for each Account id in the Routing Table.
    pub max_routes_to_store: usize,
    /// Height horizon for highest height peers
    /// For example if one peer is 1 height away from max height peer,
    /// we still want to use the rest to query for state/headers/blocks.
    pub highest_peer_horizon: u64,
    /// Period between pushing network info to client
    pub push_info_period: Duration,
    /// Peers on blacklist by IP:Port.
    /// Nodes will not accept or try to establish connection to such peers.
    pub blacklist: HashMap<IpAddr, BlockedPorts>,
    /// Flag to disable outbound connections. When this flag is active, nodes will not try to
    /// establish connection with other nodes, but will accept incoming connection if other requirements
    /// are satisfied.
    /// This flag should be ALWAYS FALSE. Only set to true for testing purposes.
    pub outbound_disabled: bool,
    /// Not clear old data, set `true` for archive nodes.
    pub archive: bool,
}

impl NetworkConfig {
    /// Returns network config with given seed used for peer id.
    pub fn from_seed(seed: &str, port: u16) -> Self {
        let secret_key = SecretKey::from_seed(KeyType::ED25519, seed);
        let public_key = secret_key.public_key();
        NetworkConfig {
            public_key,
            secret_key,
            account_id: Some(seed.parse().unwrap()),
            addr: Some(format!("0.0.0.0:{}", port).parse().unwrap()),
            boot_nodes: vec![],
            handshake_timeout: Duration::from_secs(60),
            reconnect_delay: Duration::from_secs(60),
            bootstrap_peers_period: Duration::from_millis(100),
            max_num_peers: 40,
            minimum_outbound_peers: 5,
            ideal_connections_lo: 30,
            ideal_connections_hi: 35,
            peer_recent_time_window: Duration::from_secs(600),
            safe_set_size: 20,
            archival_peer_connections_lower_bound: 10,
            ban_window: Duration::from_secs(1),
            peer_expiration_duration: Duration::from_secs(60 * 60),
            max_send_peers: 512,
            peer_stats_period: Duration::from_secs(5),
            ttl_account_id_router: Duration::from_secs(60 * 60),
            routed_message_ttl: ROUTED_MESSAGE_TTL,
            max_routes_to_store: 1,
            highest_peer_horizon: 5,
            push_info_period: Duration::from_millis(100),
            blacklist: HashMap::new(),
            outbound_disabled: false,
            archive: false,
        }
    }

    pub fn verify(&self) -> Result<(), anyhow::Error> {
        if !(self.ideal_connections_lo <= self.ideal_connections_hi) {
            anyhow::bail!(
                "Invalid ideal_connections values. lo({}) > hi({}).",
                self.ideal_connections_lo,
                self.ideal_connections_hi
            );
        }

        if !(self.ideal_connections_hi < self.max_num_peers) {
            anyhow::bail!(
                "max_num_peers({}) is below ideal_connections_hi({}) which may lead to connection saturation and declining new connections.",
                self.max_num_peers, self.ideal_connections_hi
            );
        }

        if self.outbound_disabled {
            anyhow::bail!("Outbound connections are disabled.");
        }

        if !(self.safe_set_size > self.minimum_outbound_peers) {
            anyhow::bail!(
                "safe_set_size({}) must be larger than minimum_outbound_peers({}).",
                self.safe_set_size,
                self.minimum_outbound_peers
            );
        }

        if UPDATE_INTERVAL_LAST_TIME_RECEIVED_MESSAGE * 2 > self.peer_recent_time_window {
            anyhow::bail!(
                "Very short peer_recent_time_window({}). it should be at least twice update_interval_last_time_received_message({}).",
                self.peer_recent_time_window.as_secs(), UPDATE_INTERVAL_LAST_TIME_RECEIVED_MESSAGE.as_secs()
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum BlockedPorts {
    All,
    Some(HashSet<u16>),
}

/// converts list of addresses represented by strings to <IpAddr, BlockedPorts> HashMap
///
/// Arguments:
/// - `blacklist`- list of strings in following formats:
///    - "IP" - for example 127.0.0.1 - if only IP is provided we will block all ports
///    - "IP:PORT - for example 127.0.0.1:2134
pub fn blacklist_from_iter<T>(blacklist: T) -> HashMap<IpAddr, BlockedPorts>
where
    T: IntoIterator<Item = String>,
{
    let mut blacklist_map = HashMap::new();
    for addr in blacklist {
        if let Ok(res) = addr.parse::<PatternAddr>() {
            match res {
                PatternAddr::Ip(addr) => {
                    blacklist_map
                        .entry(addr)
                        .and_modify(|blocked_ports| *blocked_ports = BlockedPorts::All)
                        .or_insert(BlockedPorts::All);
                }
                PatternAddr::IpPort(addr) => {
                    blacklist_map
                        .entry(addr.ip())
                        .and_modify(|blocked_ports| {
                            if let BlockedPorts::Some(ports) = blocked_ports {
                                ports.insert(addr.port());
                            }
                        })
                        .or_insert_with(|| {
                            BlockedPorts::Some(HashSet::from_iter(vec![addr.port()]))
                        });
                }
            }
        }
    }

    blacklist_map
}

/// Used to match a socket addr by IP:Port or only by IP
#[derive(Clone, Debug)]
pub enum PatternAddr {
    Ip(IpAddr),
    IpPort(SocketAddr),
}

impl FromStr for PatternAddr {
    type Err = AddrParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(pattern) = s.parse::<IpAddr>() {
            return Ok(PatternAddr::Ip(pattern));
        }

        s.parse::<SocketAddr>().map(PatternAddr::IpPort)
    }
}

/// On every message from peer don't update `last_time_received_message`
/// but wait some "small" timeout between updates to avoid a lot of messages between
/// Peer and PeerManager.
pub const UPDATE_INTERVAL_LAST_TIME_RECEIVED_MESSAGE: Duration = Duration::from_secs(60);

#[cfg(test)]
mod test {
    use crate::types::{NetworkConfig, UPDATE_INTERVAL_LAST_TIME_RECEIVED_MESSAGE};

    #[test]
    fn test_network_config() {
        let nc = NetworkConfig::from_seed("123", 213);
        assert!(nc.verify().is_ok());

        let mut nc = NetworkConfig::from_seed("123", 213);
        nc.ideal_connections_lo = nc.ideal_connections_hi + 1;
        let res = nc.verify();
        assert!(res.is_err(), "{:?}", res);

        let mut nc = NetworkConfig::from_seed("123", 213);
        nc.ideal_connections_hi = nc.max_num_peers;
        let res = nc.verify();
        assert!(res.is_err(), "{:?}", res);

        let mut nc = NetworkConfig::from_seed("123", 213);
        nc.safe_set_size = nc.minimum_outbound_peers;
        let res = nc.verify();
        assert!(res.is_err(), "{:?}", res);

        let mut nc = NetworkConfig::from_seed("123", 213);
        nc.peer_recent_time_window = UPDATE_INTERVAL_LAST_TIME_RECEIVED_MESSAGE;
        let res = nc.verify();
        assert!(res.is_err(), "{:?}", res);
    }
}
