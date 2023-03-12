
impl NetworkState {
    pub fn peer_store_view(&self) -> PeerStoreView {
        let mut peer_states_view = self
            .peer_store
            .load()
            .iter()
            .map(|(peer_id, known_peer_state)| KnownPeerStateView {
                peer_id: peer_id.clone(),
                status: format!("{:?}", known_peer_state.status),
                addr: format!("{:?}", known_peer_state.peer_info.addr),
                first_seen: known_peer_state.first_seen.unix_timestamp(),
                last_seen: known_peer_state.last_seen.unix_timestamp(),
                last_attempt: known_peer_state.last_outbound_attempt.clone().map(
                    |(attempt_time, attempt_result)| {
                        let foo = match attempt_result {
                            Ok(_) => String::from("Ok"),
                            Err(err) => format!("Error: {:?}", err.as_str()),
                        };
                        (attempt_time.unix_timestamp(), foo)
                    },
                ),
            })
            .collect::<Vec<_>>();

        peer_states_view.sort_by_key(|a| (
            -a.last_attempt.clone().map(|(attempt_time, _)| attempt_time).unwrap_or(0),
            -a.last_seen,
        ));
        PeerStoreView { peer_states: peer_states_view }
    }

    pub fn graph_view(&self) -> NetworkGraphView {
        NetworkGraphView {
            edges: self
                .graph
                .load()
                .edges
                .values()
                .map(|edge| {
                    let key = edge.key();
                    EdgeView { peer0: key.0.clone(), peer1: key.1.clone(), nonce: edge.nonce() }
                })
                .collect(),
        })
    }

    pub fn recent_outbound_connections_view(&self) -> RecentOutboundConnectionsView {
        RecentOutboundConnectionsView {
            recent_outbound_connections: this
                .connection_store
                .get_recent_outbound_connections()
                .iter()
                .map(|c| ConnectionInfoView {
                    peer_id: c.peer_info.id.clone(),
                    addr: format!("{:?}", c.peer_info.addr),
                    time_established: c.time_established.unix_timestamp(),
                    time_connected_until: c.time_connected_until.unix_timestamp(),
                })
                .collect(),
        }
    }

    pub fn info(&self) -> NetworkInfo {
        let tier1 = self.tier1.load();
        let tier2 = self.tier2.load();
        let graph = self.graph.load();
        let accounts_data = self.accounts_data.load();
        let now = ctx::time::now();
        let connected_peer = |cp: &Arc<connection::Connection>| ConnectedPeerInfo {
            full_peer_info: cp.full_peer_info(),
            received_bytes_per_sec: cp.stats.received_bytes_per_sec.load(Ordering::Relaxed),
            sent_bytes_per_sec: cp.stats.sent_bytes_per_sec.load(Ordering::Relaxed),
            last_time_peer_requested: cp.last_time_peer_requested.load().unwrap_or(now),
            last_time_received_message: cp.last_time_received_message.load(),
            connection_established_time: cp.established_time,
            peer_type: cp.peer_type,
            nonce: match graph.local_edges.get(&cp.peer_info.id) {
                Some(e) => e.nonce(),
                None => 0,
            },
        };
        NetworkInfo {
            connected_peers: tier2.ready.values().map(connected_peer).collect(),
            tier1_connections: tier1.ready.values().map(connected_peer).collect(),
            num_connected_peers: tier2.ready.len(),
            peer_max_count: self.config.max_num_peers,
            highest_height_peers: self.highest_height_peers(),
            sent_bytes_per_sec: tier2
                .ready
                .values()
                .map(|x| x.stats.sent_bytes_per_sec.lock().unwrap().rate_per_sec())
                .sum(),
            received_bytes_per_sec: tier2
                .ready
                .values()
                .map(|x| x.stats.received_bytes.lock().unwrap().rate_per_sec())
                .sum(),
            known_producers: self
                .graph
                .routing_table
                .get_announce_accounts()
                .into_iter()
                .map(|announce_account| KnownProducer {
                    account_id: announce_account.account_id,
                    peer_id: announce_account.peer_id.clone(),
                    // TODO: fill in the address.
                    addr: None,
                    next_hops: self.state.graph.routing_table.view_route(&announce_account.peer_id),
                })
                .collect(),
            tier1_accounts_keys: accounts_data.keys.iter().cloned().collect(),
            tier1_accounts_data: accounts_data.data.values().cloned().collect(),
        }
    }
}
