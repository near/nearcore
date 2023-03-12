impl NetworkState {
    /// Check if it is needed to create a new outbound connection.
    /// If the number of active connections is less than `ideal_connections_lo` or
    /// (the number of outgoing connections is less than `minimum_outbound_peers`
    ///     and the total connections is less than `max_num_peers`)
    fn is_outbound_bootstrap_needed(&self) -> bool {
        if self.config.outbound_disabled {
            return false;
        }
        let tier2 = self.tier2.load();
        let total_connections = tier2.outbound_handshakes.len();
        let potential_outbound_connections = tier2.outbound_handshake.values().filter(|peer| peer.peer_type == PeerType::Outbound).count();

        if total_connections < self.config.ideal_connections_lo as usize {
            return true;
        }
        if total_connections < self.config.max_num_peers as usize && potential_outbound_connections < self.config.minimum_outbound_peers as usize {
            return true;
        }
        return false;
    }

    /// Returns peers close to the highest height
    fn highest_height_peers(&self) -> Vec<HighestHeightPeerInfo> {
        let infos: Vec<HighestHeightPeerInfo> = self
            .tier2
            .load()
            .ready
            .values()
            .filter_map(|p| p.full_peer_info().into())
            .collect();

        // This finds max height among peers, and returns one peer close to such height.
        let max_height = match infos.iter().map(|i| i.highest_block_height).max() {
            Some(height) => height,
            None => return vec![],
        };
        // Find all peers whose height is within `highest_peer_horizon` from max height peer(s).
        infos
            .into_iter()
            .filter(|i| {
                i.highest_block_height.saturating_add(self.config.highest_peer_horizon)
                    >= max_height
            })
            .collect()
    }

    // Get peers that are potentially unreliable and we should avoid routing messages through them.
    // Currently we're picking the peers that are too much behind (in comparison to us).
    fn unreliable_peers(&self) -> HashSet<PeerId> {
        // If chain info is not set, that means we haven't received chain info message
        // from chain yet. Return empty set in that case. This should only last for a short period
        // of time.
        let Some(chain_info) = self.chain_info.load() else { return HashSet::new(); };
        let my_height = chain_info.block.header().height();
        // Find all peers whose height is below `highest_peer_horizon` from max height peer(s).
        // or the ones we don't have height information yet
        self.tier2
            .load()
            .ready
            .values()
            .filter(|p| {
                p.last_block
                    .load()
                    .as_ref()
                    .map(|x| x.height.saturating_add(UNRELIABLE_PEER_HORIZON) < my_height)
                    .unwrap_or(false)
            })
            .map(|p| p.peer_info.id.clone())
            .collect()
    }

    /// Check if the number of connections (excluding whitelisted ones) exceeds ideal_connections_hi.
    /// If so, constructs a safe set of peers and selects one random peer outside of that set
    /// and sends signal to stop connection to it gracefully.
    ///
    /// Safe set contruction process:
    /// 1. Add all whitelisted peers to the safe set.
    /// 2. If the number of outbound connections is less or equal than minimum_outbound_connections,
    ///    add all outbound connections to the safe set.
    /// 3. Find all peers who sent us a message within the last peer_recent_time_window,
    ///    and add them one by one to the safe_set (starting from earliest connection time)
    ///    until safe set has safe_set_size elements.
    fn maybe_stop_active_connection(&self) {
        let tier2 = self.tier2.load();
        let filter_peers = |predicate: &dyn Fn(&connection::Connection) -> bool| -> Vec<_> {
            tier2
                .ready
                .values()
                .filter(|peer| predicate(&*peer))
                .map(|peer| peer.peer_info.id.clone())
                .collect()
        };

        // Build safe set
        let mut safe_set = HashSet::new();

        // Add whitelisted nodes to the safe set.
        let whitelisted_peers = filter_peers(&|p| self.is_peer_whitelisted(&p.peer_info));
        safe_set.extend(whitelisted_peers);

        // If there is not enough non-whitelisted peers, return without disconnecting anyone.
        if tier2.ready.len() - safe_set.len() <= self.config.ideal_connections_hi as usize {
            return;
        }

        // If there is not enough outbound peers, add them to the safe set.
        let outbound_peers = filter_peers(&|p| p.peer_type == PeerType::Outbound);
        if outbound_peers.len() + tier2.outbound_handshakes.len()
            <= self.config.minimum_outbound_peers as usize
        {
            safe_set.extend(outbound_peers);
        }

        // If there is not enough archival peers, add them to the safe set.
        if self.config.archive {
            let archival_peers = filter_peers(&|p| p.archival);
            if archival_peers.len()
                <= self.config.archival_peer_connections_lower_bound as usize
            {
                safe_set.extend(archival_peers);
            }
        }

        // Find all recently active peers.
        let now = ctx::time::now();
        let mut active_peers: Vec<Arc<connection::Connection>> = tier2
            .ready
            .values()
            .filter(|p| {
                now - p.last_time_received_message.load()
                    < self.state.config.peer_recent_time_window
            })
            .cloned()
            .collect();

        // Sort by established time.
        active_peers.sort_by_key(|p| p.established_time);
        // Saturate safe set with recently active peers.
        let set_limit = self.state.config.safe_set_size as usize;
        for p in active_peers {
            if safe_set.len() >= set_limit {
                break;
            }
            safe_set.insert(p.peer_info.id.clone());
        }

        // Build valid candidate list to choose the peer to be removed. All peers outside the safe set.
        let candidates = tier2.ready.values().filter(|p| !safe_set.contains(&p.peer_info.id));
        if let Some(p) = candidates.choose(&mut rand::thread_rng()) {
            tracing::debug!(target: "network", id = ?p.peer_info.id,
                tier2_len = tier2.ready.len(),
                ideal_connections_hi = self.state.config.ideal_connections_hi,
                "Stop active connection"
            );
            p.terminate(None);
        }
    }

    /// Periodically monitor list of peers and:
    ///  - request new peers from connected peers,
    ///  - bootstrap outbound connections from known peers,
    ///  - unban peers that have been banned for awhile,
    ///  - remove expired peers,
    ///
    /// # Arguments:
    /// - `interval` - Time between consequent runs.
    /// - `default_interval` - we will set `interval` to this value once, after first successful connection
    /// - `max_interval` - maximum value of interval
    /// NOTE: in the current implementation `interval` increases by 1% every time, and it will
    ///       reach value of `max_internal` eventually.
    async fn monitor_peers_trigger() {
        while this.is_outbound_bootstrap_needed() && !ctx::is_canceled() {
            let tier2 = this.tier2.load();
            // With some odds - try picking one of the 'NotConnected' peers -- these are the ones that we were able to connect to in the past.
            let prefer_previously_connected_peer = thread_rng().gen_bool(PREFER_PREVIOUSLY_CONNECTED_PEER);
            let Some(peer_info) = this.peer_store.unconnected_peer(
                |peer_state| {
                    // Ignore connecting to ourself
                    this.config.node_id() == peer_state.peer_info.id
                    || this.config.node_addr.as_ref().map(|a|**a) == peer_state.peer_info.addr
                    // Or to peers we are currently trying to connect to
                    || tier2.handshakes.contains(ConnectionKey{peer_type: PeerType::Outbound, &peer_state.peer_info.id})
                },
                prefer_previously_connected_peer,
            ) else { break; }
            
            let result = async {
                let stream = tcp::Stream::connect(&peer_info, tcp::Tier::T2).await?;
                this.spawn_connection(stream).await?;
                anyhow::Ok(())
            }.await;
            if result.is_err() {
                tracing::info!(target:"network", ?result, "failed to connect to {peer_info}");
            }
            if state.peer_store.peer_connection_attempt(&peer_info.id, result).is_err() {
                tracing::error!(target: "network", ?peer_info, "Failed to store connection attempt.");
            }
        }
       
        // TODO: stuff below probably shouldn't a separate background task.

        // If there are too many active connections try to remove some connections
        this.maybe_stop_active_connection();

        // Find peers that are not reliable (too much behind) - and make sure that we're not routing messages through them.
        let unreliable_peers = this.unreliable_peers();
        metrics::PEER_UNRELIABLE.set(unreliable_peers.len() as i64);
        this.graph.set_unreliable_peers(unreliable_peers);
    }

    /// Re-establish each outbound connection in the connection store (single attempt)
    fn bootstrap_outbound_from_recent_connections(this: &scope::ServiceScope<Self>) {
        for conn_info in self.connection_store.get_recent_outbound_connections() {
            scope::spawn!(this, |this| async { this.reconnect(peer_info, 1).await });
        }
    }
}
