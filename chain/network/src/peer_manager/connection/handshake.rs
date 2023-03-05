type InboundHandshakePermit = tokio::sync::OwnedSemaphorePermit;

impl scope::ServiceTrait for Connection {
    type E = ClosingReason;

    async fn start(this: &scope::ServiceScope<Self>) -> Result<(),Self::E> {
        s.spawn(async { conn.stream.terminated().await });
        s.spawn(async { conn.sync_routing_table().await });
        
        // TIER1 is strictly reserved for BFT consensensus messages,
        // so all kinds of periodical syncs happen only on TIER2 connections.
        if this.tier==tcp::Tier::T2 {
            // Exchange peers periodically.
            scope::spawn!(this, |this| async {
                let mut interval = ctx::time::Interval::new(this.established,REQUEST_PEERS_INTERVAL);
                loop {
                    interval.tick().await?;
                    this.send_message(&PeerMessage::PeersRequest).await?;
                }
            });
            // Send latest block periodically.
            scope::spawn!(this, |this| async {
                let mut interval = ctx::time::Interval::new(this.established, SYNC_LATEST_BLOCK_INTERVAL);
                loop {
                    interval.tick().await?;
                    if let Some(chain_info) = state.chain_info.load().as_ref() {
                        this.send_message(&PeerMessage::Block(chain_info.block.clone())).await?;
                    }
                }
            });
            if conn.peer_type() == PeerType::Outbound {
                // Trigger a full accounts data sync periodically.
                // Note that AccountsData is used to establish TIER1 network,
                // it is broadcasted over TIER2 network. This is a bootstrapping
                // mechanism, because TIER2 is established before TIER1.
                //
                // TODO(gprusak): consider whether it wouldn't be more uniform to just
                // send full sync from both sides of the connection independently. Or
                // perhaps make the full sync request a separate message which doesn't
                // carry the accounts_data at all.
                scope::spawn!(this, |this| async {
                    let mut interval = ctx::time::Interval::new(this.established,ACCOUNTS_DATA_FULL_SYNC_INTERVAL);
                    loop {
                        interval.tick().await?;
                        conn.send_message(Arc::new(PeerMessage::SyncAccountsData(SyncAccountsData{
                            accounts_data: network_state.accounts_data.load().data.values().cloned().collect(),
                            incremental: false,
                            requesting_full_sync: true,
                        })));
                    }
                });
                // Refresh connection nonces but only if we're outbound. For inbound connection, the other party should
                // take care of nonce refresh.
                scope::spawn!(this, |this| async {
                    // How often should we refresh a nonce from a peer.
                    // It should be smaller than PRUNE_EDGES_AFTER.
                    let mut interval = ctx::time::Interval::new(this.established + PRUNE_EDGES_AFTER / 3, PRUNE_EDGES_AFTER / 3);
                    loop {
                        interval.tick().await?;
                        this.send_message(Arc::new(
                            PeerMessage::RequestUpdateNonce(PartialEdgeInfo::new(
                                &network_state.config.node_id(),
                                &conn.peer_info.id,
                                Edge::create_fresh_nonce(),
                                &network_state.config.node_key,
                            )
                        )));

                    }
                });
            }
        } 
    });
}

impl NetworkState {
    fn make_handshake(
        &self,
        tier: tcp::Tier,
        target_peer_id: PeerId,
        protocol_version: ProtocolVersion,
        partial_edge_info: PartialEdgeInfo,
    ) -> Handshake {
        let (height, tracked_shards) =
            if let Some(chain_info) = self.chain_info.load().as_ref() {
                (chain_info.block.header().height(), self.tracked_shards.clone())
            } else {
                (0, vec![])
            };
        Handshake {
            tier,
            protocol_version,
            oldest_supported_version: PEER_MIN_ALLOWED_PROTOCOL_VERSION,
            sender_peer_id: self.node_id(),
            target_peer_id: peer_id,
            sender_listen_port: self.config.node_addr.as_ref().map(|a| a.port()),
            sender_chain_info: PeerChainInfoV2 {
                genesis_id: network_state.genesis_id.clone(),
                // TODO: remove `height` from PeerChainInfo
                height,
                tracked_shards,
                archival: self.config.archive,
            },
            partial_edge_info: partial_edge_info,
            owned_account: self.config.validator.as_ref().map(|vc| {
                OwnedAccount {
                    account_key: vc.signer.public_key(),
                    peer_id: self.node_id(),
                    timestamp: ctx::time::now_utc(),
                }
                .sign(vc.signer.as_ref())
            }),
        }
    }

    fn verify_handshake_request(&self, h: &Handshake) -> Result<(),PeerMessage> {
        if PEER_MIN_ALLOWED_PROTOCOL_VERSION > h.protocol_version || h.protocol_version > PROTOCOL_VERSION {
            tracing::debug!(target: "network", version = h.protocol_version, "Received connection from node with unsupported PROTOCOL_VERSION.");
            return Some(PeerMessage::HandshakeFailure(
                self.my_node_info.clone(),
                HandshakeFailureReason::ProtocolVersionMismatch {
                    version: PROTOCOL_VERSION,
                    oldest_supported_version: PEER_MIN_ALLOWED_PROTOCOL_VERSION,
                },
            );
        }
        let genesis_id = self.genesis_id.clone();
        if h.sender_chain_info.genesis_id != genesis_id {
            tracing::debug!(target: "network", "Received connection from node with different genesis.");
            return Err(PeerMessage::HandshakeFailure(
                self.my_node_info.clone(),
                HandshakeFailureReason::GenesisMismatch(genesis_id),
            )));
        }
        if h.target_peer_id != self.config.node_id() {
            tracing::debug!(target: "network", "Received handshake from {:?} to {:?} but I am {:?}", h.sender_peer_id, h.target_peer_id, self.my_node_info.id);
            return Err(PeerMessage::HandshakeFailure(
                self.my_node_info.clone(),
                HandshakeFailureReason::InvalidTarget,
            ));
        }
    
        // Check that the received nonce is greater than the current nonce of this connection.
        // If not (and this is an inbound connection) propose a new nonce.
        if let Some(last_edge) = self.graph.load().local_edges.get(&h.sender_peer_id) {
            if last_edge.nonce() >= h.partial_edge_info.nonce {
                tracing::debug!(target: "network", "{:?}: Received too low nonce from peer {:?} sending evidence.", network_state.config.node_id(), self.peer_addr);
                return Err(PeerMessage::LastEdge(last_edge.clone()));
            }
        }
        Ok(())
    }

    fn verify_handshake_response(&self, req: &mut Handshake, msg: &PeerMessage) -> Result<Option<Handshake>>,HandshakeError> {
        match msg {
            PeerMessage::Handshake(h) => return Ok(h),
            PeerMessage::HandshakeFailure(peer_info, reason) => match reason {
                HandshakeFailureReason::GenesisMismatch(genesis) => return Err(HandshakeError::GenesisMismatch),
                HandshakeFailureReason::InvalidTarget => {
                    self.peer_store.add_direct_peer(peer_info);
                    return Err(HandshakeError::TargetMismatch);
                }
                HandshakeFailureReason::ProtocolVersionMismatch { version, oldest_supported_version } => {
                    // Retry the handshake with the common protocol version.
                    let common_version = std::cmp::min(version, PROTOCOL_VERSION);
                    if common_version < oldest_supported_version || common_version < PEER_MIN_ALLOWED_PROTOCOL_VERSION {
                        return Err(HandshakeError::ProtocolVersionMismatch);
                    }
                    req.protocol_version = common_version;
                }
            }
            // TODO(gprusak): LastEdge should rather be a variant of HandshakeFailure.
            // Clean this up (you don't have to modify the proto, just the translation layer).
            PeerMessage::LastEdge(edge) => {
                // Check that the edge provided:
                let ok =
                    // - is for the relevant pair of peers
                    edge.key()==&Edge::make_key(self.node_id(),spec.peer_id.clone()) &&
                    // - is not younger than what we proposed originally. This protects us from
                    //   a situation in which the peer presents us with a very outdated edge e,
                    //   and then we sign a new edge with nonce e.nonce+1 which is also outdated.
                    //   It may still happen that an edge with an old nonce gets signed, but only
                    //   if both nodes not know about the newer edge. We don't defend against that.
                    //   Also a malicious peer might send the LastEdge with the edge we just
                    //   signed (pretending that it is old) but we cannot detect that, because the
                    //   signatures are currently deterministic.
                    edge.nonce() >= spec.partial_edge_info.nonce &&
                    // - is a correctly signed edge
                    edge.verify();
                // Disconnect if neighbor sent an invalid edge.
                if !ok {
                    return Err(HandshakeError::InvalidEdge);
                }
                // Recreate the edge with a newer nonce.
                let nonce = std::cmp::max(edge.next(), Edge::create_fresh_nonce());
                req.partial_edge_info = self.propose_edge(&req.peer_id, Some(nonce));
            }
            msg => tracing::warn!(target:"network","unexpected message during handshake: {}",msg),
        }
    }

    fn verify() -> {
        let edge = merge_handshakes(req,resp)?;
        // TODO(gprusak): not enabling a port for listening is also a valid setup.
        // In that case peer_info.addr should be None (same as now), however
        // we still should do the check against the PeerStore::blacklist.
        // Currently PeerManager is rejecting connections with peer_info.addr == None
        // preemptively.
        let peer_info = PeerInfo {
            id: edge.other(this.node_id()),
            // TODO: req or resp?
            addr: req.sender_listen_port.map(|port| SocketAddr::new(send.peer_addr.ip(), port)),
            account_id: None,
        };
        
        // Check if this is a blacklisted peer.
        if peer_info.addr.as_ref().map_or(true, |addr| this.peer_store.is_blacklisted(addr)) {
            tracing::debug!(target: "network", peer_info = ?peer_info, "Dropping connection from blacklisted peer or unknown address");
            return Err(HandshakeError::Blacklisted);
        }

        if this.peer_store.is_banned(&peer_info.id) {
            tracing::debug!(target: "network", id = ?peer_info.id, "Dropping connection from banned peer");
            return Err(HandshakeError::Banned);
        }
        
        match conn.tier {
            tcp::Tier::T1 => {
                if conn.peer_type() == tcp::StreamType::Inbound {
                    if !this.config.tier1.as_ref().map_or(false, |c| c.enable_inbound) {
                        return Err(HandshakeError::Tier1InboundDisabled);
                    }
                    // Allow for inbound TIER1 connections only directly from a TIER1 peers.
                    let owned_account = conn.owned_account.as_ref().ok_or(HandshakeError::NotTier1Peer)?;
                    if !this.accounts_data.load().keys.contains(&owned_account.account_key) {
                        return Err(HandshakeError::NotTier1Peer);
                    }
                }
                let conn = this.new_service(conn);
            }
            tcp::Tier::T2 => {
                if conn.peer_type() == PeerType::Inbound {
                    if !this.is_inbound_allowed(&peer_info) {
                        // TODO(1896): Gracefully drop inbound connection for other peer.
                        let tier2 = this.tier2.load();
                        tracing::debug!(target: "network",
                            tier2 = tier2.ready.len(), outgoing_peers = tier2.outbound_handshakes.len(),
                            max_num_peers = self.config.max_num_peers,
                            "Dropping handshake (network at max capacity)."
                        );
                        return Err(HandshakeError::ConnectionLimitExceeded);
                    }
                }
                // First verify and broadcast the edge of the connection, so that in case
                // it is invalid, the connection is not added to the pool.
                // TODO(gprusak): consider actually banning the peer for consistency.
                this.add_edges(vec![edge]).await.map_err(|_: ReasonForBan| HandshakeError::InvalidEdge)?;
            }
        }
    }

    struct HandshakedStream {
        encoding: Encoding,
        handshake_sent: Handshake,
        handshake_received: Handshake,
        sender: stream::FrameSender,
        receiver: stream::FrameReceiver,
        permit: Permit,
    }

    /// Spawns a PeerActor on a separate actix arbiter.
    /// Returns the actor address and a HandshakeSignal: an asynchronous channel
    /// which will be closed as soon as the handshake is finished (successfully or not).
    /// You can asynchronously await the returned HandshakeSignal.
    pub async fn run_handshake(
        this: &Self,
        stream: tcp::Stream,
    ) -> Result<HandshakeScope,HandshakeError> { 
        let encoding = match stream.tier {
            tcp::Tier1::T1 => Encoding::Proto,
            tcp::Tier2::T2 => this.config.encoding,
        };
        let (send,recv) = stream::split(stream);
        match &send.stats.info.type_ {
            tcp::StreamType::Inbound => {
                let handshake_permit = this
                    .inbound_handshake_permits
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| HandshakeError::TooManyInbound)?;
                let req = async {
                    loop {
                        let req = match PeerMessage::deserialize(encoding,recv.recv()?) {
                            Ok(PeerMessage::Handshake(req)) => req,
                            Err(_) => continue,
                        };
                        if Err(resp) = this.verify_handshake_request(req) {
                            stream.send(&stream::Frame(PeerMessage::encode(encoding,resp))).await?;
                        } else {
                            return Ok(req);
                        }
                    }
                })?;
                let resp = this.make_handshake(
                    req.tier,
                    req.sender_peer_id.clone(),
                    req.protocol_version,
                    this.propose_edge(&req.sender_peer_id, Some(nonce)),
                );
                verify(req,resp)?;
                Self::register(this,encoding,req,resp,send,recv)
            }
            tcp::StreamType::Outbound{tier,peer_id} => {
                let permit = match tier {
                    tcp::Tier::T1 => this.tier1.start_outbound(peer_id.clone())?
                    tcp::Tier::T2 => {
                        // A loop connection is not allowed on TIER2
                        // (it is allowed on TIER1 to verify node's public IP).
                        // TODO(gprusak): try to make this more consistent.
                        if peer_id == &this.node_id() {
                            return Err(HandshakeError::OutboundNotAllowed(
                                connection::PoolError::UnexpectedLoopConnection,
                            ));
                        }
                        this.tier2.start_outbound(peer_id.clone())?
                    }
                };
                
                let mut req = this.make_handshake(
                    tier,
                    peer_id,
                    partial_edge_info: network_state.propose_edge(peer_id, None),
                    protocol_version: PROTOCOL_VERSION,
                );
                let resp = async {
                    loop {
                        send.send(&stream::Frame(req.serialize(encoding))).await?;
                        let resp = PeerMessage::deserialize(e,recv.recv().await?)?;
                        if let Some(resp) = this.verify_handshake_response(&mut req, resp)? {
                            return Ok(resp);
                        }
                    }
                }.await;
            }
        };
    }
}
