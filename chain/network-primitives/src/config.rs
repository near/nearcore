use crate::network_protocol::PeerInfo;
use crate::types::{Blacklist, ROUTED_MESSAGE_TTL};
use near_crypto::{KeyType, SecretKey};
use near_primitives::network::PeerId;
use near_primitives::types::AccountId;
use near_primitives::validator_signer::{InMemoryValidatorSigner, ValidatorSigner};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;

/// How much height horizon to give to consider peer up to date.
pub const HIGHEST_PEER_HORIZON: u64 = 5;

/// Maximum amount of routes to store for each account id.
pub const MAX_ROUTES_TO_STORE: usize = 5;

/// ValidatorEndpoints are the endpoints that peers should connect to, to send messages to this
/// validator. Validator will sign the endpoints and broadcast them to the network.
/// For a static setup (a static IP, or a list of relay nodes with static IPs) use PublicAddrs.
/// For a dynamic setup (with a single dynamic/ephemeral IP), use TrustedStunServers.
#[derive(Clone)]
pub enum ValidatorEndpoints {
    /// Single public address of this validator, or a list of public addresses of trusted nodes
    /// willing to route messages to this validator. Validator will connect to the listed relay
    /// nodes on startup.
    PublicAddrs(Vec<SocketAddr>),
    /// Addresses of the format "<domain/ip>:<port>" of STUN servers.
    /// The IP of the validator will be determined dynamically by querying all the STUN servers on
    /// the list.
    TrustedStunServers(Vec<String>),
}

#[derive(Clone)]
pub struct ValidatorConfig {
    pub signer: Arc<dyn ValidatorSigner>,
    pub endpoints: ValidatorEndpoints,
}

impl ValidatorConfig {
    pub fn account_id(&self) -> AccountId {
        self.signer.validator_id().clone()
    }
}

/// Configuration for the peer-to-peer manager.
#[derive(Clone)]
pub struct NetworkConfig {
    pub node_addr: Option<SocketAddr>,
    pub node_key: SecretKey,
    pub validator: Option<ValidatorConfig>,

    pub boot_nodes: Vec<PeerInfo>,
    pub whitelist_nodes: Vec<PeerInfo>,
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
    /// Nodes will not accept or try to establish connection to such peers.
    pub blacklist: Blacklist,
    /// Flag to disable outbound connections. When this flag is active, nodes will not try to
    /// establish connection with other nodes, but will accept incoming connection if other requirements
    /// are satisfied.
    /// This flag should be ALWAYS FALSE. Only set to true for testing purposes.
    pub outbound_disabled: bool,
    /// Not clear old data, set `true` for archive nodes.
    pub archive: bool,
}

impl NetworkConfig {
    pub fn new(
        cfg: crate::config_json::Config,
        node_key: SecretKey,
        validator_signer: Option<Arc<dyn ValidatorSigner>>,
        archive: bool,
    ) -> Self {
        Self {
            node_key,
            validator: validator_signer.as_ref().map(|signer| ValidatorConfig {
                signer: signer.clone(),
                endpoints: if cfg.public_addrs.len() > 0 {
                    ValidatorEndpoints::PublicAddrs(
                        cfg.public_addrs
                            .into_iter()
                            .map(|addr| addr.parse().expect("Failed to parse SocketAddr"))
                            .collect(),
                    )
                } else {
                    ValidatorEndpoints::TrustedStunServers(cfg.trusted_stun_servers)
                },
            }),
            node_addr: match cfg.addr.as_str() {
                "" => None,
                addr => Some(addr.parse().expect("Failed to parse SocketAddr")),
            },
            boot_nodes: if cfg.boot_nodes.is_empty() {
                vec![]
            } else {
                cfg.boot_nodes
                    .split(',')
                    .map(|chunk| chunk.try_into().expect("Failed to parse PeerInfo"))
                    .collect()
            },
            whitelist_nodes: (|| -> Vec<_> {
                let w = &cfg.whitelist_nodes;
                if w.is_empty() {
                    return vec![];
                }
                let mut peers = vec![];
                for peer in w.split(',') {
                    let peer: PeerInfo = peer.try_into().expect("Failed to parse PeerInfo");
                    if peer.addr.is_none() {
                        panic!("whitelist_nodes are required to specify both PeerId and IP:port")
                    }
                    peers.push(peer);
                }
                peers
            }()),
            handshake_timeout: cfg.handshake_timeout,
            reconnect_delay: cfg.reconnect_delay,
            bootstrap_peers_period: Duration::from_secs(60),
            max_num_peers: cfg.max_num_peers,
            minimum_outbound_peers: cfg.minimum_outbound_peers,
            ideal_connections_lo: cfg.ideal_connections_lo,
            ideal_connections_hi: cfg.ideal_connections_hi,
            peer_recent_time_window: cfg.peer_recent_time_window,
            safe_set_size: cfg.safe_set_size,
            archival_peer_connections_lower_bound: cfg.archival_peer_connections_lower_bound,
            ban_window: cfg.ban_window,
            max_send_peers: 512,
            peer_expiration_duration: Duration::from_secs(7 * 24 * 60 * 60),
            peer_stats_period: Duration::from_secs(5),
            ttl_account_id_router: cfg.ttl_account_id_router,
            routed_message_ttl: ROUTED_MESSAGE_TTL,
            max_routes_to_store: MAX_ROUTES_TO_STORE,
            highest_peer_horizon: HIGHEST_PEER_HORIZON,
            push_info_period: Duration::from_millis(100),
            blacklist: cfg
                .blacklist
                .iter()
                .map(|e| e.parse().expect("failed to parse blacklist"))
                .collect(),
            outbound_disabled: false,
            archive,
        }
    }

    pub fn node_id(&self) -> PeerId {
        PeerId::new(self.node_key.public_key())
    }

    /// Returns network config with given seed used for peer id.
    pub fn from_seed(seed: &str, port: u16) -> Self {
        let node_key = SecretKey::from_seed(KeyType::ED25519, seed);
        let node_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port));
        let account_id = seed.parse().unwrap();
        let validator = ValidatorConfig {
            signer: Arc::new(InMemoryValidatorSigner::from_seed(
                account_id,
                KeyType::ED25519,
                seed,
            )),
            endpoints: ValidatorEndpoints::PublicAddrs(vec![node_addr]),
        };
        NetworkConfig {
            node_addr: Some(node_addr),
            node_key,
            validator: Some(validator),
            boot_nodes: vec![],
            whitelist_nodes: vec![],
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
            blacklist: Blacklist::default(),
            outbound_disabled: false,
            archive: false,
        }
    }

    pub fn verify(&self) -> anyhow::Result<()> {
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
