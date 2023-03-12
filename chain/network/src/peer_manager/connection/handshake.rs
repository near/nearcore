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

    /// Returns Ok(Some(resp)) if the peer accepted the handshake and resp is their response.
    /// Returns Ok(None) if handshake failed but it is retriable (h has been updated, so that it
    /// can be sent in the next attempt).
    /// Returns Err if handshake failed and is not retriable.
    fn verify_handshake_response(&self, h: &mut Handshake, resp: &PeerMessage) -> Result<Option<Handshake>>,HandshakeError> {
        match resp {
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
    }

    fn start_handshake(&self, tier: tcp::Tier, key: pool::HandshakeKey) -> Result<pool::Handshake<'a>, pool::Error> {
        match tier {
            tcp::Tier::T1 => self.tier1.start_handshake(key),
            tcp::Tier::T2 => {
                // A loop connection is not allowed on TIER2
                // (it is allowed on TIER1 to verify node's public IP).
                // TODO(gprusak): try to make this more consistent.
                if peer_id == &this.node_id() {
                    return Err(pool::Error::UnexpectedLoopConnection);
                }
                self.tier2.start_handshake(key)
            }
        }
    }

    /// Spawns a PeerActor on a separate actix arbiter.
    /// Returns the actor address and a HandshakeSignal: an asynchronous channel
    /// which will be closed as soon as the handshake is finished (successfully or not).
    /// You can asynchronously await the returned HandshakeSignal.
    pub async fn run_handshake(
        this: &Self,
        stream: tcp::Stream,
    ) -> Result<HandshakeStream,HandshakeError> { 
        let encoding = match stream.tier {
            tcp::Tier1::T1 => Encoding::Proto,
            tcp::Tier2::T2 => this.config.encoding,
        };
        let (send,recv) = stream::split(stream);
        match &send.stats.info.type_ {
            tcp::StreamType::Inbound => { 
                let handshake_received = async {
                    loop {
                        let h = match PeerMessage::deserialize(encoding,recv.recv()?) {
                            Ok(PeerMessage::Handshake(h)) => h,
                            Err(_) => continue,
                        };
                        if Err(resp) = this.verify_inbound_handshake(h) {
                            stream.send(&stream::Frame(PeerMessage::encode(encoding,resp))).await?;
                        } else {
                            return Ok(h);
                        }
                    }
                }.await?;
                let handshake_sent = this.make_handshake(
                    req.tier,
                    req.sender_peer_id.clone(),
                    req.protocol_version,
                    this.propose_edge(&req.sender_peer_id, Some(nonce)),
                );
                verify(req,resp)?;
                let handshake = this.start_handshake(tier,HandshakeKey{peer_type:PeerType::Inbound,peer_id: req.sender_peer_id.clone()})?;
                send.send(&stream::Frame(resp.serialize(encoding))).await?;
                Ok(HandshakeScope { encoding, handshake, req, resp, send, recv })
            }
            tcp::StreamType::Outbound{tier,peer_id} => {
                let key = HandshakeKey{peer_type:PeerType::Outbound, peer_id };
                let handshake = this.start_handshake(key)?; 
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
                }.await?;
                Ok(HandshakeStream { encoding, handshake, req, resp, send, recv })
            }
        };
    }
}
