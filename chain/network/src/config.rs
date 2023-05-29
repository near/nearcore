use crate::blacklist;
use crate::concurrency::rate;
use crate::network_protocol::PeerAddr;
use crate::network_protocol::PeerInfo;
use crate::peer_manager::peer_manager_actor::Event;
use crate::peer_manager::peer_store;
use crate::sink::Sink;
use crate::stun;
use crate::tcp;
use crate::types::ROUTED_MESSAGE_TTL;
use anyhow::Context;
use near_async::time;
use near_crypto::{KeyType, SecretKey};
use near_primitives::network::PeerId;
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::AccountId;
use near_primitives::validator_signer::ValidatorSigner;
use std::collections::HashSet;
use std::sync::Arc;

/// How much height horizon to give to consider peer up to date.
pub const HIGHEST_PEER_HORIZON: u64 = 5;

/// Maximum amount of routes to store for each account id.
pub const MAX_ROUTES_TO_STORE: usize = 5;

/// Maximum number of PeerAddrs in the ValidatorConfig::endpoints field.
pub const MAX_PEER_ADDRS: usize = 10;

/// Maximum number of peers to include in a PeersResponse message.
pub const PEERS_RESPONSE_MAX_PEERS: u32 = 512;

/// ValidatorProxies are nodes with public IP (aka proxies) that this validator trusts to be honest
/// and willing to forward traffic to this validator. Whenever this node is a TIER1 validator
/// (i.e. whenever it is a block producer/chunk producer/approver for the given epoch),
/// it will connect to all the proxies in this config and advertise a signed list of proxies that
/// it has established a connection to.
///
/// Once other TIER1 nodes learn the list of proxies, they will maintain a connection to a random
/// proxy on this list. This way a message from any TIER1 node to this node will require at most 2
/// hops.
///
/// neard supports 2 modes for configuring proxy addresses:
/// * [recommended] `Static` list of proxies (public SocketAddr + PeerId), supports up to 10 proxies.
///   It is a totally valid setup for a TIER1 validator to be its own (perahaps only) proxy:
///   to achieve that, add an entry with the public address of this node to the Static list.
/// * [discouraged] `Dynamic` proxy - in case you want this validator to be its own and only proxy,
///   instead of adding the public address explicitly to the `Static` list, you can specify a STUN
///   server address (or a couple of them) which will be used to dynamically resolve the public IP
///   of this validator. Note that in this case the validator trusts the STUN servers to correctly
///   resolve the public IP.
#[derive(Clone)]
pub enum ValidatorProxies {
    Static(Vec<PeerAddr>),
    Dynamic(Vec<stun::ServerAddr>),
}

#[derive(Clone)]
pub struct ValidatorConfig {
    pub signer: Arc<dyn ValidatorSigner>,
    pub proxies: ValidatorProxies,
}

impl ValidatorConfig {
    pub fn account_id(&self) -> AccountId {
        self.signer.validator_id().clone()
    }
}

#[derive(Clone)]
pub struct Tier1 {
    /// Interval between attempts to connect to proxies of other TIER1 nodes.
    pub connect_interval: time::Duration,
    /// Maximal number of new connections established every connect_interval.
    /// TIER1 can consists of hundreds of nodes, so it is not feasible to connect to all of them at
    /// once.
    pub new_connections_per_attempt: u64,
    /// Interval between broacasts of the list of validator's proxies.
    /// Before the broadcast, validator tries to establish all the missing connections to proxies.
    pub advertise_proxies_interval: time::Duration,
    /// Support for gradual TIER1 feature rollout:
    /// - establishing connection to node's own proxies is always enabled (it is a part of peer
    ///   discovery mechanism). Note that unless the proxy has enable_inbound set, establishing
    ///   those connections will fail anyway.
    /// - a node will start accepting TIER1 inbound connections iff `enable_inbound` is true.
    /// - a node will try to start outbound TIER1 connections iff `enable_outbound` is true.
    pub enable_inbound: bool,
    pub enable_outbound: bool,
}

/// Validated configuration for the peer-to-peer manager.
#[derive(Clone)]
pub struct NetworkConfig {
    pub node_addr: Option<tcp::ListenerAddr>,
    pub node_key: SecretKey,
    pub validator: Option<ValidatorConfig>,

    pub peer_store: peer_store::Config,
    pub whitelist_nodes: Vec<PeerInfo>,
    pub handshake_timeout: time::Duration,

    /// Whether to re-establish connection to known reliable peers from previous neard run(s).
    /// See near_network::peer_manager::connection_store for details.
    pub connect_to_reliable_peers_on_startup: bool,
    /// Maximum time between refreshing the peer list.
    pub monitor_peers_max_period: time::Duration,
    /// Maximum number of active peers. Hard limit.
    pub max_num_peers: u32,
    /// Minimum outbound connections a peer should have to avoid eclipse attacks.
    pub minimum_outbound_peers: u32,
    /// Lower bound of the ideal number of connections.
    pub ideal_connections_lo: u32,
    /// Upper bound of the ideal number of connections.
    pub ideal_connections_hi: u32,
    /// Peers which last message is was within this period of time are considered active recent peers.
    pub peer_recent_time_window: time::Duration,
    /// Number of peers to keep while removing a connection.
    /// Used to avoid disconnecting from peers we have been connected since long time.
    pub safe_set_size: u32,
    /// Lower bound of the number of connections to archival peers to keep
    /// if we are an archival node.
    pub archival_peer_connections_lower_bound: u32,
    /// Maximum number of peer addresses we should ever send on PeersRequest.
    pub max_send_peers: u32,
    /// Duration for checking on stats from the peers.
    pub peer_stats_period: time::Duration,
    /// Time to persist Accounts Id in the router without removing them.
    pub ttl_account_id_router: time::Duration,
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
    pub push_info_period: time::Duration,
    /// Flag to disable outbound connections. When this flag is active, nodes will not try to
    /// establish connection with other nodes, but will accept incoming connection if other requirements
    /// are satisfied.
    /// This flag should be ALWAYS FALSE. Only set to true for testing purposes.
    pub outbound_disabled: bool,
    /// Flag to disable inbound connections. When true, all the incoming handshake/connection requests will be rejected.
    pub inbound_disabled: bool,
    /// Whether this is an archival node.
    pub archive: bool,
    /// Maximal rate at which SyncAccountsData can be broadcasted.
    pub accounts_data_broadcast_rate_limit: rate::Limit,
    /// Maximal rate at which RoutingTable can be recomputed.
    pub routing_table_update_rate_limit: rate::Limit,
    /// Config of the TIER1 network.
    pub tier1: Option<Tier1>,

    // Whether to ignore tombstones some time after startup.
    //
    // Ignoring tombstones means:
    //   * not broadcasting deleted edges
    //   * ignoring received deleted edges as well
    pub skip_tombstones: Option<time::Duration>,

    /// TEST-ONLY
    /// TODO(gprusak): make it pub(crate), once all integration tests
    /// are merged into near_network.
    pub event_sink: Sink<Event>,
}

impl NetworkConfig {
    /// Overrides values of NetworkConfig with values for the JSON config.
    /// We need all the values from NetworkConfig to be configurable.
    /// We need this in case of emergency. It is faster to change the config than to recompile.
    fn override_config(&mut self, overrides: crate::config_json::NetworkConfigOverrides) {
        if let Some(connect_to_reliable_peers_on_startup) =
            overrides.connect_to_reliable_peers_on_startup
        {
            self.connect_to_reliable_peers_on_startup = connect_to_reliable_peers_on_startup
        }
        if let Some(max_send_peers) = overrides.max_send_peers {
            self.max_send_peers = max_send_peers
        }
        if let Some(routed_message_ttl) = overrides.routed_message_ttl {
            self.routed_message_ttl = routed_message_ttl
        }
        if let Some(max_routes_to_store) = overrides.max_routes_to_store {
            self.max_routes_to_store = max_routes_to_store
        }
        if let Some(highest_peer_horizon) = overrides.highest_peer_horizon {
            self.highest_peer_horizon = highest_peer_horizon
        }
        if let Some(millis) = overrides.push_info_period_millis {
            self.push_info_period = time::Duration::milliseconds(millis)
        }
        if let Some(outbound_disabled) = overrides.outbound_disabled {
            self.outbound_disabled = outbound_disabled
        }
        if let (Some(qps), Some(burst)) = (
            overrides.accounts_data_broadcast_rate_limit_qps,
            overrides.accounts_data_broadcast_rate_limit_burst,
        ) {
            self.accounts_data_broadcast_rate_limit = rate::Limit { qps, burst }
        }
        if let (Some(qps), Some(burst)) = (
            overrides.routing_table_update_rate_limit_qps,
            overrides.routing_table_update_rate_limit_burst,
        ) {
            self.routing_table_update_rate_limit = rate::Limit { qps, burst }
        }
    }

    pub fn new(
        cfg: crate::config_json::Config,
        node_key: SecretKey,
        validator_signer: Option<Arc<dyn ValidatorSigner>>,
        archive: bool,
    ) -> anyhow::Result<Self> {
        if cfg.public_addrs.len() > MAX_PEER_ADDRS {
            anyhow::bail!(
                "public_addrs has {} entries, limit is {MAX_PEER_ADDRS}",
                cfg.public_addrs.len()
            );
        }
        let mut proxies = HashSet::new();
        for proxy in &cfg.public_addrs {
            if proxies.contains(&proxy.peer_id) {
                anyhow::bail!("public_addrs: found multiple entries with peer_id {}. Only 1 entry per peer_id is supported.",proxy.peer_id);
            }
            proxies.insert(proxy.peer_id.clone());
            let ip = proxy.addr.ip();
            if cfg.allow_private_ip_in_public_addrs {
                if ip.is_unspecified() {
                    anyhow::bail!("public_addrs: {ip} is not a valid IP. If you wanted to specify a loopback IP, use 127.0.0.1 instead.");
                }
            } else {
                // TODO(gprusak): use !ip.is_global() instead, once it is stable.
                if ip.is_loopback()
                    || ip.is_unspecified()
                    || match ip {
                        std::net::IpAddr::V4(ip) => ip.is_private(),
                        // TODO(gprusak): use ip.is_unique_local() once stable.
                        std::net::IpAddr::V6(_) => false,
                    }
                {
                    anyhow::bail!("public_addrs: {ip} is not a public IP.");
                }
            }
        }
        let mut this = Self {
            node_key,
            validator: validator_signer.map(|signer| ValidatorConfig {
                signer,
                proxies: if !cfg.public_addrs.is_empty() {
                    ValidatorProxies::Static(cfg.public_addrs)
                } else {
                    ValidatorProxies::Dynamic(cfg.trusted_stun_servers)
                },
            }),
            node_addr: match cfg.addr.as_str() {
                "" => None,
                addr => Some(tcp::ListenerAddr::new(
                    addr.parse().context("Failed to parse SocketAddr")?,
                )),
            },
            peer_store: peer_store::Config {
                boot_nodes: if cfg.boot_nodes.is_empty() {
                    vec![]
                } else {
                    cfg.boot_nodes
                        .split(',')
                        .map(|chunk| chunk.parse())
                        .collect::<Result<_, _>>()
                        .context("boot_nodes")?
                },
                blacklist: cfg
                    .blacklist
                    .iter()
                    .map(|e| e.parse())
                    .collect::<Result<_, _>>()
                    .context("failed to parse blacklist")?,
                peer_states_cache_size: cfg.peer_states_cache_size,
                connect_only_to_boot_nodes: cfg.experimental.connect_only_to_boot_nodes,
                ban_window: cfg.ban_window.try_into()?,
                peer_expiration_duration: cfg.peer_expiration_duration.try_into()?,
            },
            whitelist_nodes: if cfg.whitelist_nodes.is_empty() {
                vec![]
            } else {
                cfg.whitelist_nodes
                    .split(',')
                    .map(|peer| match peer.parse::<PeerInfo>() {
                        Ok(peer) if peer.addr.is_none() => anyhow::bail!(
                            "whitelist_nodes are required to specify both PeerId and IP:port"
                        ),
                        Ok(peer) => Ok(peer),
                        Err(err) => Err(err.into()),
                    })
                    .collect::<anyhow::Result<_>>()
                    .context("whitelist_nodes")?
            },
            connect_to_reliable_peers_on_startup: true,
            handshake_timeout: cfg.handshake_timeout.try_into()?,
            monitor_peers_max_period: cfg.monitor_peers_max_period.try_into()?,
            max_num_peers: cfg.max_num_peers,
            minimum_outbound_peers: cfg.minimum_outbound_peers,
            ideal_connections_lo: cfg.ideal_connections_lo,
            ideal_connections_hi: cfg.ideal_connections_hi,
            peer_recent_time_window: cfg.peer_recent_time_window.try_into()?,
            safe_set_size: cfg.safe_set_size,
            archival_peer_connections_lower_bound: cfg.archival_peer_connections_lower_bound,
            max_send_peers: PEERS_RESPONSE_MAX_PEERS,
            peer_stats_period: cfg.peer_stats_period.try_into()?,
            ttl_account_id_router: cfg.ttl_account_id_router.try_into()?,
            routed_message_ttl: ROUTED_MESSAGE_TTL,
            max_routes_to_store: MAX_ROUTES_TO_STORE,
            highest_peer_horizon: HIGHEST_PEER_HORIZON,
            push_info_period: time::Duration::milliseconds(100),
            outbound_disabled: false,
            archive,
            accounts_data_broadcast_rate_limit: rate::Limit { qps: 0.1, burst: 1 },
            routing_table_update_rate_limit: rate::Limit { qps: 1., burst: 1 },
            tier1: Some(Tier1 {
                connect_interval: cfg.experimental.tier1_connect_interval.try_into()?,
                new_connections_per_attempt: cfg.experimental.tier1_new_connections_per_attempt,
                advertise_proxies_interval: time::Duration::minutes(15),
                enable_inbound: cfg.experimental.tier1_enable_inbound,
                enable_outbound: cfg.experimental.tier1_enable_outbound,
            }),
            inbound_disabled: cfg.experimental.inbound_disabled,
            skip_tombstones: if cfg.experimental.skip_sending_tombstones_seconds > 0 {
                Some(time::Duration::seconds(cfg.experimental.skip_sending_tombstones_seconds))
            } else {
                None
            },
            event_sink: Sink::null(),
        };
        this.override_config(cfg.experimental.network_config_overrides);
        Ok(this)
    }

    pub fn node_id(&self) -> PeerId {
        PeerId::new(self.node_key.public_key())
    }

    /// TEST-ONLY: Returns network config with given seed used for peer id.
    pub fn from_seed(seed: &str, node_addr: tcp::ListenerAddr) -> Self {
        let node_key = SecretKey::from_seed(KeyType::ED25519, seed);
        let validator = ValidatorConfig {
            signer: Arc::new(create_test_signer(seed)),
            proxies: ValidatorProxies::Static(vec![PeerAddr {
                addr: *node_addr,
                peer_id: PeerId::new(node_key.public_key()),
            }]),
        };
        NetworkConfig {
            node_addr: Some(node_addr),
            node_key,
            validator: Some(validator),
            peer_store: peer_store::Config {
                boot_nodes: vec![],
                blacklist: blacklist::Blacklist::default(),
                peer_states_cache_size: 1000,
                ban_window: time::Duration::seconds(1),
                peer_expiration_duration: time::Duration::seconds(60 * 60),
                connect_only_to_boot_nodes: false,
            },
            whitelist_nodes: vec![],
            handshake_timeout: time::Duration::seconds(5),
            connect_to_reliable_peers_on_startup: true,
            monitor_peers_max_period: time::Duration::seconds(100),
            max_num_peers: 40,
            minimum_outbound_peers: 5,
            ideal_connections_lo: 30,
            ideal_connections_hi: 35,
            peer_recent_time_window: time::Duration::seconds(600),
            safe_set_size: 20,
            archival_peer_connections_lower_bound: 10,
            max_send_peers: PEERS_RESPONSE_MAX_PEERS,
            peer_stats_period: time::Duration::seconds(5),
            ttl_account_id_router: time::Duration::seconds(60 * 60),
            routed_message_ttl: ROUTED_MESSAGE_TTL,
            max_routes_to_store: 1,
            highest_peer_horizon: 5,
            push_info_period: time::Duration::milliseconds(100),
            outbound_disabled: false,
            inbound_disabled: false,
            archive: false,
            accounts_data_broadcast_rate_limit: rate::Limit { qps: 100., burst: 1000000 },
            routing_table_update_rate_limit: rate::Limit { qps: 10., burst: 1 },
            tier1: Some(Tier1 {
                // Interval is very large, so that it doesn't happen spontaneously in tests.
                // It should rather be triggered manually in tests.
                connect_interval: time::Duration::hours(1000),
                new_connections_per_attempt: 10000,
                advertise_proxies_interval: time::Duration::hours(1000),
                enable_inbound: true,
                enable_outbound: true,
            }),
            skip_tombstones: None,
            event_sink: Sink::null(),
        }
    }

    pub fn verify(self) -> anyhow::Result<VerifiedConfig> {
        if !(self.ideal_connections_lo <= self.ideal_connections_hi) {
            anyhow::bail!(
                "Invalid ideal_connections values. lo({}) > hi({}).",
                self.ideal_connections_lo,
                self.ideal_connections_hi
            );
        }

        if !(self.ideal_connections_hi <= self.max_num_peers) {
            anyhow::bail!(
                "max_num_peers({}) < ideal_connections_hi({}) which may lead to connection saturation and declining new connections.",
                self.max_num_peers, self.ideal_connections_hi
            );
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
                self.peer_recent_time_window, UPDATE_INTERVAL_LAST_TIME_RECEIVED_MESSAGE
            );
        }

        if !(self.max_send_peers <= PEERS_RESPONSE_MAX_PEERS) {
            anyhow::bail!(
                "max_send_peers({}) can be at most {}",
                self.max_send_peers,
                PEERS_RESPONSE_MAX_PEERS
            );
        }

        self.accounts_data_broadcast_rate_limit
            .validate()
            .context("accounts_Data_broadcast_rate_limit")?;
        self.routing_table_update_rate_limit
            .validate()
            .context("routing_table_update_rate_limit")?;
        Ok(VerifiedConfig { node_id: self.node_id(), inner: self })
    }
}

/// On every message from peer don't update `last_time_received_message`
/// but wait some "small" timeout between updates to avoid a lot of messages between
/// Peer and PeerManager.
pub const UPDATE_INTERVAL_LAST_TIME_RECEIVED_MESSAGE: time::Duration = time::Duration::seconds(60);

#[derive(Clone)]
pub struct VerifiedConfig {
    inner: NetworkConfig,
    /// Cached inner.node_id().
    /// It allows to avoid recomputing the public key every time.
    node_id: PeerId,
}

impl VerifiedConfig {
    pub fn node_id(&self) -> PeerId {
        self.node_id.clone()
    }
}

impl std::ops::Deref for VerifiedConfig {
    type Target = NetworkConfig;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[cfg(test)]
mod test {
    use super::UPDATE_INTERVAL_LAST_TIME_RECEIVED_MESSAGE;
    use crate::config;
    use crate::config_json::NetworkConfigOverrides;
    use crate::network_protocol;
    use crate::network_protocol::testonly as data;
    use crate::network_protocol::{AccountData, VersionedAccountData};
    use crate::tcp;
    use crate::testonly::make_rng;
    use near_async::time;

    #[test]
    fn test_network_config() {
        let nc = config::NetworkConfig::from_seed("123", tcp::ListenerAddr::reserve_for_test());
        assert!(nc.verify().is_ok());

        let mut nc = config::NetworkConfig::from_seed("123", tcp::ListenerAddr::reserve_for_test());
        nc.ideal_connections_lo = nc.ideal_connections_hi + 1;
        assert!(nc.verify().is_err());

        let mut nc = config::NetworkConfig::from_seed("123", tcp::ListenerAddr::reserve_for_test());
        nc.ideal_connections_hi = nc.max_num_peers + 1;
        assert!(nc.verify().is_err());

        let mut nc = config::NetworkConfig::from_seed("123", tcp::ListenerAddr::reserve_for_test());
        nc.safe_set_size = nc.minimum_outbound_peers;
        assert!(nc.verify().is_err());

        let mut nc = config::NetworkConfig::from_seed("123", tcp::ListenerAddr::reserve_for_test());
        nc.peer_recent_time_window = UPDATE_INTERVAL_LAST_TIME_RECEIVED_MESSAGE;
        assert!(nc.verify().is_err());
    }

    #[test]
    fn test_network_config_override() {
        fn check_override_field<T: std::cmp::PartialEq>(
            before: &T,
            after: &T,
            override_val: &Option<T>,
        ) -> bool {
            if let Some(val) = override_val {
                return after == val;
            } else {
                return after == before;
            }
        }
        let check_fields = |before: &config::NetworkConfig,
                            after: &config::NetworkConfig,
                            overrides: &NetworkConfigOverrides| {
            assert!(check_override_field(
                &before.connect_to_reliable_peers_on_startup,
                &after.connect_to_reliable_peers_on_startup,
                &overrides.connect_to_reliable_peers_on_startup
            ));
            assert!(check_override_field(
                &before.max_send_peers,
                &after.max_send_peers,
                &overrides.max_send_peers
            ));
            assert!(check_override_field(
                &before.routed_message_ttl,
                &after.routed_message_ttl,
                &overrides.routed_message_ttl
            ));
            assert!(check_override_field(
                &before.max_routes_to_store,
                &after.max_routes_to_store,
                &overrides.max_routes_to_store
            ));
            assert!(check_override_field(
                &before.highest_peer_horizon,
                &after.highest_peer_horizon,
                &overrides.highest_peer_horizon
            ));
            assert!(check_override_field(
                &before.push_info_period,
                &after.push_info_period,
                &overrides
                    .push_info_period_millis
                    .map(|millis| time::Duration::milliseconds(millis))
            ));
            assert!(check_override_field(
                &before.outbound_disabled,
                &after.outbound_disabled,
                &overrides.outbound_disabled
            ));
            assert!(check_override_field(
                &before.accounts_data_broadcast_rate_limit.burst,
                &after.accounts_data_broadcast_rate_limit.burst,
                &overrides.accounts_data_broadcast_rate_limit_burst
            ));
            assert!(check_override_field(
                &before.accounts_data_broadcast_rate_limit.qps,
                &after.accounts_data_broadcast_rate_limit.qps,
                &overrides.accounts_data_broadcast_rate_limit_qps
            ));
        };
        let no_overrides = NetworkConfigOverrides::default();
        let mut overrides = NetworkConfigOverrides::default();
        overrides.connect_to_reliable_peers_on_startup = Some(false);
        overrides.max_send_peers = Some(42);
        overrides.routed_message_ttl = Some(43);
        overrides.accounts_data_broadcast_rate_limit_burst = Some(44);
        overrides.accounts_data_broadcast_rate_limit_qps = Some(45.0);

        let nc_before =
            config::NetworkConfig::from_seed("123", tcp::ListenerAddr::reserve_for_test());

        let mut nc_after = nc_before.clone();
        nc_after.override_config(no_overrides.clone());
        check_fields(&nc_before, &nc_after, &no_overrides);
        assert!(nc_after.verify().is_ok());

        nc_after = nc_before.clone();
        nc_after.override_config(overrides.clone());
        check_fields(&nc_before, &nc_after, &overrides);
        assert!(nc_after.verify().is_ok());
    }

    // Check that MAX_PEER_ADDRS limit is consistent with the
    // network_protocol::MAX_ACCOUNT_DATA_SIZE_BYTES limit
    #[test]
    fn accounts_data_size_limit() {
        let mut rng = make_rng(39521947542);
        let clock = time::FakeClock::default();
        let signer = data::make_validator_signer(&mut rng);

        let ad = VersionedAccountData {
            data: AccountData {
                proxies: (0..config::MAX_PEER_ADDRS)
                    .map(|_| {
                        // Using IPv6 gives maximal size of the resulting config.
                        let ip = data::make_ipv6(&mut rng);
                        data::make_peer_addr(&mut rng, ip)
                    })
                    .collect(),
                peer_id: data::make_peer_id(&mut rng),
            },
            account_key: signer.public_key(),
            version: 0,
            timestamp: clock.now_utc(),
        };
        let sad = ad.sign(&signer).unwrap();
        assert!(sad.payload().len() <= network_protocol::MAX_ACCOUNT_DATA_SIZE_BYTES);
    }
}
