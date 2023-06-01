use crate::accounts_data::{AccountDataCacheSnapshot, LocalAccountData};
use crate::config;
use crate::network_protocol::{
    AccountData, PeerAddr, PeerInfo, PeerMessage, SignedAccountData, SyncAccountsData,
};
use crate::peer::peer_actor::PeerActor;
use crate::peer_manager::connection;
use crate::stun;
use crate::tcp;
use crate::types::PeerType;
use near_async::time;
use near_crypto::PublicKey;
use near_o11y::log_assert;
use near_primitives::network::PeerId;
use rand::seq::IteratorRandom as _;
use rand::seq::SliceRandom as _;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

impl super::NetworkState {
    // Returns ValidatorConfig of this node iff it belongs to TIER1 according to `accounts_data`.
    pub fn tier1_validator_config(
        &self,
        accounts_data: &AccountDataCacheSnapshot,
    ) -> Option<&config::ValidatorConfig> {
        if self.config.tier1.is_none() {
            return None;
        }
        self.config
            .validator
            .as_ref()
            .filter(|cfg| accounts_data.keys.contains(&cfg.signer.public_key()))
    }

    async fn tier1_connect_to_my_proxies(
        self: &Arc<Self>,
        clock: &time::Clock,
        proxies: &[PeerAddr],
    ) {
        let tier1 = self.tier1.load();
        // Try to connect to all proxies in parallel.
        let mut handles = vec![];
        for proxy in proxies {
            // Skip the proxies we are already connected to.
            if tier1.ready.contains_key(&proxy.peer_id) {
                continue;
            }
            handles.push(async move {
                let res = async {
                    let stream = tcp::Stream::connect(
                        &PeerInfo {
                            id: proxy.peer_id.clone(),
                            addr: Some(proxy.addr),
                            account_id: None,
                        },
                        tcp::Tier::T1,
                    )
                    .await?;
                    anyhow::Ok(PeerActor::spawn_and_handshake(clock.clone(), stream, None, self.clone()).await?)
                }.await;
                if let Err(err) = res {
                    tracing::warn!(target:"network", ?err, "failed to establish connection to TIER1 proxy {:?}",proxy);
                }
            });
        }
        futures_util::future::join_all(handles).await;
    }

    /// Requests direct peers for accounts data full sync.
    /// Should be called whenever the accounts_data.keys changes, and
    /// periodically just in case.
    pub fn tier1_request_full_sync(&self) {
        self.tier2.broadcast_message(Arc::new(PeerMessage::SyncAccountsData(SyncAccountsData {
            incremental: true,
            requesting_full_sync: true,
            accounts_data: vec![],
        })));
    }

    /// Tries to connect to ALL trusted proxies from the config, then broadcasts AccountData with
    /// the set of proxies it managed to connect to. This way other TIER1 nodes can just connect
    /// to ANY proxy of this node.
    pub async fn tier1_advertise_proxies(
        self: &Arc<Self>,
        clock: &time::Clock,
    ) -> Option<Arc<SignedAccountData>> {
        // Tier1 advertise proxies calls should be disjoint,
        // to avoid a race condition while connecting to the proxies.
        // TODO(gprusak): there are more corner cases to cover, because
        // tier1_connect may also spawn TIER1 connections conflicting with
        // tier1_advertise_proxies. It would be better to be able to await
        // handshake on connection attempts, even if another call spawned them.
        let _lock = self.tier1_advertise_proxies_mutex.lock().await;
        let accounts_data = self.accounts_data.load();

        let vc = self.tier1_validator_config(&accounts_data)?;
        let proxies = match (&self.config.node_addr, &vc.proxies) {
            (None, _) => vec![],
            (_, config::ValidatorProxies::Static(peer_addrs)) => peer_addrs.clone(),
            // If Dynamic are specified,
            // it means that this node is its own proxy.
            // Discover the public IP of this node using those STUN servers.
            // We do not require all stun servers to be available, but
            // we require the received responses to be consistent.
            (Some(node_addr), config::ValidatorProxies::Dynamic(stun_servers)) => {
                // Query all the STUN servers in parallel.
                let queries = stun_servers.iter().map(|addr| {
                    let clock = clock.clone();
                    let addr = addr.clone();
                    self.spawn(async move {
                        match stun::query(&clock, &addr).await {
                            Ok(ip) => Some(ip),
                            Err(err) => {
                                tracing::warn!(target:"network", "STUN lookup failed for {addr}: {err}");
                                None
                            }
                        }
                    })
                });
                let mut node_ips = vec![];
                for q in queries {
                    node_ips.extend(q.await.unwrap());
                }
                // Check that we have received non-zero responses and that they are consistent.
                if node_ips.is_empty() {
                    vec![]
                } else if !node_ips.iter().all(|ip| ip == &node_ips[0]) {
                    tracing::warn!(target:"network", "received inconsistent responses from the STUN servers");
                    vec![]
                } else {
                    vec![PeerAddr {
                        peer_id: self.config.node_id(),
                        addr: std::net::SocketAddr::new(node_ips[0], node_addr.port()),
                    }]
                }
            }
        };
        self.tier1_connect_to_my_proxies(clock, &proxies).await;

        // Snapshot tier1 connections again before broadcasting.
        let tier1 = self.tier1.load();

        let my_proxies = match &vc.proxies {
            // In case of dynamic configuration, only the node itself can be its proxy,
            // so we look for a loop connection which would prove our node's address.
            config::ValidatorProxies::Dynamic(_) => match tier1.ready.get(&self.config.node_id()) {
                Some(conn) => {
                    log_assert!(PeerType::Outbound == conn.peer_type);
                    log_assert!(conn.peer_info.addr.is_some());
                    match conn.peer_info.addr {
                        Some(addr) => vec![PeerAddr { peer_id: self.config.node_id(), addr }],
                        None => vec![],
                    }
                }
                None => vec![],
            },
            // In case of static configuration, we look for connections to proxies matching the config.
            config::ValidatorProxies::Static(proxies) => {
                let mut connected_proxies = vec![];
                for proxy in proxies {
                    match tier1.ready.get(&proxy.peer_id) {
                        // Here we compare the address from the config with the
                        // address of the connection (which is the IP, to which the
                        // TCP socket is connected + port indicated by the peer).
                        // We will broadcast only those addresses which we confirmed are
                        // valid (i.e. we managed to connect to them).
                        //
                        // TODO(gprusak): It may happen that a single peer will be
                        // available under multiple IPs, in which case, we should
                        // prefer to connect to the IP from the config, however
                        // that would require having separate inbound and outbound
                        // pools, so that both endpoints can keep a connection
                        // to the IP that they prefer. This is a corner case which can happen
                        // only if 2 TIER1 validators are proxies for some other validator.
                        Some(conn) if conn.peer_info.addr == Some(proxy.addr) => {
                            connected_proxies.push(proxy.clone());
                        }
                        Some(conn) => {
                            tracing::info!(target:"network", "connected to {}, but got addr {:?}, while want {}",conn.peer_info.id,conn.peer_info.addr,proxy.addr)
                        }
                        _ => {}
                    }
                }
                connected_proxies
            }
        };
        tracing::info!(target:"network","connected to proxies {my_proxies:?}");
        let new_data = self.accounts_data.set_local(
            clock,
            LocalAccountData {
                signer: vc.signer.clone(),
                data: Arc::new(AccountData { peer_id: self.config.node_id(), proxies: my_proxies }),
            },
        );
        // Early exit in case this node is not a TIER1 node any more.
        let new_data = new_data?;
        // Advertise the new_data.
        self.tier2.broadcast_message(Arc::new(PeerMessage::SyncAccountsData(SyncAccountsData {
            incremental: true,
            requesting_full_sync: false,
            accounts_data: vec![new_data.clone()],
        })));
        Some(new_data)
    }

    /// Closes TIER1 connections from nodes which are not TIER1 any more.
    /// If this node is TIER1, it additionally connects to proxies of other TIER1 nodes.
    pub async fn tier1_connect(self: &Arc<Self>, clock: &time::Clock) {
        let tier1_cfg = match &self.config.tier1 {
            Some(it) => it,
            None => return,
        };
        if !tier1_cfg.enable_outbound {
            return;
        }
        let accounts_data = self.accounts_data.load();
        let validator_cfg = self.tier1_validator_config(&accounts_data);

        // Construct indices on accounts_data.
        let mut accounts_by_proxy = HashMap::<_, Vec<_>>::new();
        let mut proxies_by_account = HashMap::<_, Vec<_>>::new();
        for d in accounts_data.data.values() {
            proxies_by_account.entry(&d.account_key).or_default().extend(d.proxies.iter());
            for p in &d.proxies {
                accounts_by_proxy.entry(&p.peer_id).or_default().push(&d.account_key);
            }
        }

        // Browse the connections from newest to oldest.
        let tier1 = self.tier1.load();
        let mut ready: Vec<_> = tier1.ready.values().collect();
        ready.sort_unstable_by_key(|c| c.established_time);
        ready.reverse();

        // Select the oldest TIER1 connection for each account.
        let mut safe = HashMap::<&PublicKey, &PeerId>::new();

        match validator_cfg {
            // TIER1 nodes can establish outbound connections to other TIER1 nodes and TIER1 proxies.
            // TIER1 nodes can also accept inbound connections from TIER1 nodes.
            Some(_) => {
                for conn in &ready {
                    if conn.peer_type != PeerType::Outbound {
                        continue;
                    }
                    let peer_id = &conn.peer_info.id;
                    for key in accounts_by_proxy.get(peer_id).into_iter().flatten() {
                        safe.insert(key, peer_id);
                    }
                }
                // Direct TIER1 connections have priority over proxy connections.
                for key in &accounts_data.keys {
                    if let Some(conn) = tier1.ready_by_account_key.get(&key) {
                        safe.insert(key, &conn.peer_info.id);
                    }
                }
            }
            // All the other nodes should accept inbound connections from TIER1 nodes
            // (to act as a TIER1 proxy).
            None => {
                for key in &accounts_data.keys {
                    if let Some(conn) = tier1.ready_by_account_key.get(&key) {
                        if conn.peer_type == PeerType::Inbound {
                            safe.insert(key, &conn.peer_info.id);
                        }
                    }
                }
            }
        }

        // Construct a safe set of connections.
        let mut safe_set: HashSet<PeerId> = safe.values().map(|v| (*v).clone()).collect();
        // Add proxies of our node to the safe set.
        if let Some(vc) = validator_cfg {
            match &vc.proxies {
                config::ValidatorProxies::Dynamic(_) => {
                    safe_set.insert(self.config.node_id());
                }
                config::ValidatorProxies::Static(peer_addrs) => {
                    // TODO(gprusak): here we add peer_id to a safe set, even if
                    // the conn.peer_addr doesn't match the address from the validator config
                    // (so we cannot advertise it as our proxy). Consider making it more precise.
                    safe_set.extend(peer_addrs.iter().map(|pa| pa.peer_id.clone()));
                }
            }
        }
        // Close all other connections, as they are redundant or are no longer TIER1.
        for conn in tier1.ready.values() {
            if !safe_set.contains(&conn.peer_info.id) {
                conn.stop(None);
            }
        }
        if let Some(vc) = validator_cfg {
            // Try to establish new TIER1 connections to accounts in random order.
            let mut handles = vec![];
            let mut account_keys: Vec<_> = proxies_by_account.keys().copied().collect();
            account_keys.shuffle(&mut rand::thread_rng());
            for account_key in account_keys {
                // tier1_connect() is responsible for connecting to proxies
                // of this node. tier1_connect() connects only to proxies
                // of other TIER1 nodes.
                if account_key == &vc.signer.public_key() {
                    continue;
                }
                // Bound the number of connections established at a single call to
                // tier1_connect().
                if handles.len() as u64 >= tier1_cfg.new_connections_per_attempt {
                    break;
                }
                // If we are already connected to some proxy of account_key, then
                // don't establish another connection.
                if safe.contains_key(account_key) {
                    continue;
                }
                // Find addresses of proxies of account_key.
                let proxies: Vec<&PeerAddr> =
                    proxies_by_account.get(account_key).into_iter().flatten().map(|x| *x).collect();
                // Select a random proxy of the account_key and try to connect to it.
                let proxy = proxies.iter().choose(&mut rand::thread_rng());
                if let Some(proxy) = proxy {
                    let proxy = (*proxy).clone();
                    handles.push(async move {
                        let stream = tcp::Stream::connect(
                            &PeerInfo {
                                id: proxy.peer_id,
                                addr: Some(proxy.addr),
                                account_id: None,
                            },
                            tcp::Tier::T1,
                        )
                        .await?;
                        PeerActor::spawn_and_handshake(clock.clone(), stream, None, self.clone())
                            .await
                    });
                }
            }
            tracing::debug!(target:"network","{}: establishing {} new connections",self.config.node_id(),handles.len());
            for res in futures_util::future::join_all(handles).await {
                if let Err(err) = res {
                    tracing::info!(target:"network", ?err, "{}: failed to establish a TIER1 connection",self.config.node_id());
                }
            }
            tracing::debug!(target:"network","{}: establishing new connections DONE",self.config.node_id());
        }
    }

    /// Finds a TIER1 connection for the given SignedAccountData.
    /// It is expected to perform <10 lookups total on average,
    /// so the call latency should be negligible wrt sending a TCP packet.
    // TODO(gprusak): If not, consider precomputing the AccountKey -> Connection mapping.
    pub fn get_tier1_proxy(&self, data: &SignedAccountData) -> Option<Arc<connection::Connection>> {
        let tier1 = self.tier1.load();
        // Prefer direct connections.
        if let Some(conn) = tier1.ready_by_account_key.get(&data.account_key) {
            return Some(conn.clone());
        }
        // In case there is no direct connection and our node is a TIER1 validator, use a proxy.
        // TODO(gprusak): add a check that our node is actually a TIER1 validator.
        for proxy in &data.proxies {
            if let Some(conn) = tier1.ready.get(&proxy.peer_id) {
                return Some(conn.clone());
            }
        }
        None
    }
}
