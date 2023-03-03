type InboundHandshakePermit = tokio::sync::OwnedSemaphorePermit;

#[derive(Clone, Debug)]
struct HandshakeSpec {
    /// ID of the peer on the other side of the connection.
    peer_id: PeerId,
    tier: tcp::Tier,
    protocol_version: ProtocolVersion,
    partial_edge_info: PartialEdgeInfo,
}

impl HandshakeSpec {
    fn message(&self, network_state: &NetworkState) -> PeerMessage {
        let (height, tracked_shards) =
            if let Some(chain_info) = network_state.chain_info.load().as_ref() {
                (chain_info.block.header().height(), chain_info.tracked_shards.clone())
            } else {
                (0, vec![])
            };
        let handshake = Handshake {
            protocol_version: self.protocol_version,
            oldest_supported_version: PEER_MIN_ALLOWED_PROTOCOL_VERSION,
            sender_peer_id: network_state.config.node_id(),
            target_peer_id: self.peer_id,
            sender_listen_port: network_state.config.node_addr.as_ref().map(|a| a.port()),
            sender_chain_info: PeerChainInfoV2 {
                genesis_id: network_state.genesis_id.clone(),
                // TODO: remove `height` from PeerChainInfo
                height,
                tracked_shards,
                archival: network_state.config.archive,
            },
            partial_edge_info: spec.partial_edge_info,
            owned_account: network_state.config.validator.as_ref().map(|vc| {
                OwnedAccount {
                    account_key: vc.signer.public_key(),
                    peer_id: network_state.config.node_id(),
                    timestamp: ctx::time::now_utc(),
                }
                .sign(vc.signer.as_ref())
            }),
        };
        match spec.tier {
            tcp::Tier::T1 => PeerMessage::Tier1Handshake(handshake),
            tcp::Tier::T2 => PeerMessage::Tier2Handshake(handshake),
        }
    }
}

fn verify_handshake(my: &Handshake, their: &Handshake) -> Result<Connection,HandshakeError> {
    network_state.service.spawn(|s| {
        if resp.protocol_version != spec.protocol_version {
            anyhow::bail!("Protocol version mismatch");
        }
        if resp.sender_chain_info.genesis_id != network_state.genesis_id {
            anyhow::bail!("Genesis mismatch");
        }
        if resp.sender_peer_id != spec.peer_id {
            anyhow::bail!("PeerId mismatch");
        }
        // This can happen only in case of a malicious node.
        // Outbound peer requests a connection of a given TIER, the inbound peer can just
        // confirm the TIER or drop connection. TIER is not negotiable during handshake.
        if resp_tier != spec.tier {
            anyhow::bail!("Connection TIER mismatch");
        }

        // Verify if nonce is sane.
        verify_nonce(h.partial_edge_info.nonce)?;
        
        // Verify that handshake.owned_account is valid.
        if let Some(owned_account) = &h.owned_account {
            if Err(_) = owned_account.payload().verify(&owned_account.account_key) {
                return HandshakeError::OwnedAccountInvalidSignature;
            if owned_account.peer_id != handshake.sender_peer_id {
                return HandshakeError::OwnedAccountMismatch;
            }
            if (owned_account.timestamp - ctx::time::now_utc()).abs() >= MAX_CLOCK_SKEW {
                return HandshakeError::TooLargeClockSkew;
            }
        }

        // Merge partial edges.
        let edge = Edge::new(
            network_state.config.node_id(),
            h.sender_peer_id.clone(),
            h.partial_edge_info.nonce,
            partial_edge_info.signature.clone(),
            h.partial_edge_info.signature.clone(),
        );

        // TODO(gprusak): not enabling a port for listening is also a valid setup.
        // In that case peer_info.addr should be None (same as now), however
        // we still should do the check against the PeerStore::blacklist.
        // Currently PeerManager is rejecting connections with peer_info.addr == None
        // preemptively.
        let peer_info = PeerInfo {
            id: handshake.sender_peer_id.clone(),
            addr: handshake
                .sender_listen_port
                .map(|port| SocketAddr::new(self.peer_addr.ip(), port)),
            account_id: None,
        };

        let now = ctx::time::now();
        let conn = Arc::new(connection::Connection {
            tier,
            encoding,
            service,
            peer_info: peer_info.clone(),
            owned_account: handshake.owned_account.clone(),
            genesis_id: handshake.sender_chain_info.genesis_id.clone(),
            tracked_shards: handshake.sender_chain_info.tracked_shards.clone(),
            archival: handshake.sender_chain_info.archival,
            last_block: Default::default(),
            _peer_connections_metric: metrics::PEER_CONNECTIONS.new_point(&metrics::Connection {
                type_: self.peer_type,
                encoding,
            }),
            last_time_peer_requested: AtomicCell::new(None),
            last_time_received_message: AtomicCell::new(now),
            established_time: now,
            send_accounts_data_demux: demux::Demux::new(network_state.config.accounts_data_broadcast_rate_limit),
            tracker: self.tracker.clone(),
        });
        if Err(err) = network_state.register(edge,conn).await {
            tracing::info!(target: "network", "{:?}: Connection with {:?} rejected by PeerManager: {:?}", act.my_node_id(),conn.peer_info.id,err);
            return ClosingReason::RejectedByPeerManager(err);
        }
        s.spawn(async {
            
            network_state.unregister(conn).await;
        });
    });
}

async fn run() {
    scope::run!(|s| async {
        // Send back the handshake. 
        if conn.stream.type_ == tcp::StreamType::Inbound {
            conn.stream.send(my).await?;
        }

        // TIER1 is strictly reserved for BFT consensensus messages,
        // so all kinds of periodical syncs happen only on TIER2 connections.
        if conn.tier==tcp::Tier::T2 {
            // Exchange peers periodically.
            s.spawn(async {
                let mut interval = ctx::time::Interval::new(start_time,REQUEST_PEERS_INTERVAL);
                loop {
                    interval.tick().await?;
                    conn.send_message(&PeerMessage::PeersRequest).await?;
                }
            });
            // Send latest block periodically.
            s.spawn(async {
                let mut interval = ctx::time::Interval::new(ctx::time::now(), SYNC_LATEST_BLOCK_INTERVAL);
                loop {
                    interval.tick().await?;
                    if let Some(chain_info) = state.chain_info.load().as_ref() {
                        conn.send_message(&PeerMessage::Block(chain_info.block.clone())).await?;
                    }
                }
            });
            if conn.peer_type == PeerType::Outbound {
                // Trigger a full accounts data sync periodically.
                // Note that AccountsData is used to establish TIER1 network,
                // it is broadcasted over TIER2 network. This is a bootstrapping
                // mechanism, because TIER2 is established before TIER1.
                //
                // TODO(gprusak): consider whether it wouldn't be more uniform to just
                // send full sync from both sides of the connection independently. Or
                // perhaps make the full sync request a separate message which doesn't
                // carry the accounts_data at all.
                s.spawn(async {
                    let mut interval = ctx::time::Interval::new(start_time,ACCOUNTS_DATA_FULL_SYNC_INTERVAL);
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
                s.spawn(async {
                    // How often should we refresh a nonce from a peer.
                    // It should be smaller than PRUNE_EDGES_AFTER.
                    let mut interval = ctx::time::Interval::new(start_time + PRUNE_EDGES_AFTER / 3, PRUNE_EDGES_AFTER / 3);
                    loop {
                        interval.tick().await?;
                        conn.send_message(Arc::new(
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
        conn.sync_routing_table();
        let routed_message_cache = LruCache::new(ROUTED_MESSAGE_CACHE_SIZE);
        loop {
            let req = conn.stream.recv().await?;
            // TODO: message handling.
        }
    });
}

async fn inbound_handshake() -> Result<(InboundHandshakePermit,tcp::Tier,Handshake),HandshakeError> {
    let handshake_permit = network_state
            .inbound_handshake_permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| HandshakeError::TooManyInbound)?;
    loop {
        let msg = if let Ok(msg) = PeerMessage::deserialize(e,stream.recv()?) { msg } else { continue };
        let (tier,h) = match msg {
            PeerMessage::Tier1Handshake(h) => (tcp::Tier::T1,h),
            PeerMessage::Tier2Handshake(h) => (tcp::Tier::T2,h),
            _ => continue,
        };
        
        let resp = async {
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
            let genesis_id = self.network_state.genesis_id.clone();
            if h.sender_chain_info.genesis_id != genesis_id {
                tracing::debug!(target: "network", "Received connection from node with different genesis.");
                return Some(PeerMessage::HandshakeFailure(
                    self.my_node_info.clone(),
                    HandshakeFailureReason::GenesisMismatch(genesis_id),
                )));
            }
            if h.target_peer_id != network_state.config.node_id() {
                tracing::debug!(target: "network", "Received handshake from {:?} to {:?} but I am {:?}", h.sender_peer_id, h.target_peer_id, self.my_node_info.id);
                return Some(PeerMessage::HandshakeFailure(
                    self.my_node_info.clone(),
                    HandshakeFailureReason::InvalidTarget,
                ));
            }
        
            // Check that the received nonce is greater than the current nonce of this connection.
            // If not (and this is an inbound connection) propose a new nonce.
            if let Some(last_edge) = network_state.graph.load().local_edges.get(&h.sender_peer_id) {
                if last_edge.nonce() >= h.partial_edge_info.nonce {
                    tracing::debug!(target: "network", "{:?}: Received too low nonce from peer {:?} sending evidence.", network_state.config.node_id(), self.peer_addr);
                    return Some(PeerMessage::LastEdge(last_edge.clone()));
                }
            }
            None
        }.await;
        if Some(resp) = resp {
            stream.send(PeerMessage::encode(encoding,resp)).await?;
        } else {
            let spec = HandshakeSpec{
                peer_id: h.sender_peer_id.clone(),
                tier,
                protocol_version: h.protocol_version,
                partial_edge_info: network_state.propose_edge(&h.sender_peer_id, Some(nonce)),
            }
            let resp = spec.message(network_state);
            verify_handshake(resp, h)
        }
    }
}

async fn outbound_handshake() -> Result<(),HandshakeError> {
    tcp::StreamType::Outbound { tier, peer_id } => ConnectingStatus::Outbound {
        let permit = match tier {
            tcp::Tier::T1 => network_state
                .tier1
                .start_outbound(peer_id.clone())
                .map_err(HandshakeError::OutboundNotAllowed)?,
            tcp::Tier::T2 => {
                // A loop connection is not allowed on TIER2
                // (it is allowed on TIER1 to verify node's public IP).
                // TODO(gprusak): try to make this more consistent.
                if peer_id == &network_state.config.node_id() {
                    return Err(HandshakeError::OutboundNotAllowed(
                        connection::PoolError::UnexpectedLoopConnection,
                    ));
                }
                network_state
                    .tier2
                    .start_outbound(peer_id.clone())
                    .map_err(HandshakeError::OutboundNotAllowed)?;
            }
        };
        
        let spec = HandshakeSpec {
            tier,
            peer_id,
            partial_edge_info: network_state.propose_edge(peer_id, None),
            protocol_version: PROTOCOL_VERSION,
        };
        let (resp_tier,resp) = async {
            loop {
                stream.send(stream::Frame(spec.message(network_state).serialize(encoding)).await?;
                match PeerMessage::deserialize(e,stream.recv().await?)? {
                    PeerMessage::Tier1Handshake(h) => return Ok((tcp::Tier::T1,h)),
                    PeerMessage::Tier2Handshake(h) => return Ok((tcp::Tier::T2,h)), 
                    PeerMessage::HandshakeFailure(peer_info, reason) => match reason {
                        HandshakeFailureReason::GenesisMismatch(genesis) => {
                            anyhow::bail!("genesis mismatch: our = {:?}, their = {:?}", network_state.genesis_id, genesis);
                        }
                        HandshakeFailureReason::InvalidTarget => {
                            self.network_state.peer_store.add_direct_peer(peer_info);
                            return HandshakeError::TargetMismatch;
                        }
                        HandshakeFailureReason::ProtocolVersionMismatch { version, oldest_supported_version } => {
                            // Retry the handshake with the common protocol version.
                            let common_version = std::cmp::min(version, PROTOCOL_VERSION);
                            if common_version < oldest_supported_version || common_version < PEER_MIN_ALLOWED_PROTOCOL_VERSION {
                                anyhow::bail!("protocol version mismatch: our = {:?}, their = {:?}",
                                    (PROTOCOL_VERSION, PEER_MIN_ALLOWED_PROTOCOL_VERSION),
                                    (version, oldest_supported_version),
                                );
                            }
                            spec.protocol_version = common_version;
                        }
                    }
                    // TODO(gprusak): LastEdge should rather be a variant of HandshakeFailure.
                    // Clean this up (you don't have to modify the proto, just the translation layer).
                    PeerMessage::LastEdge(edge) => {
                        // Check that the edge provided:
                        let ok =
                            // - is for the relevant pair of peers
                            edge.key()==&Edge::make_key(network_state.node_id(),spec.peer_id.clone()) &&
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
                            anyhow::bail!("peer sent an invalid edge");
                        }
                        // Recreate the edge with a newer nonce.
                        let nonce = std::cmp::max(edge.next(), Edge::create_fresh_nonce());
                        spec.partial_edge_info = network_state.propose_edge(&spec.peer_id, Some(nonce));
                    }
                    msg => tracing::warn!(target:"network","unexpected message during handshake: {}",msg)
                }
            }
        }.await?; 
        verify_handshake(req,resp);
    }
}

impl Connection {
    /// Spawns a PeerActor on a separate actix arbiter.
    /// Returns the actor address and a HandshakeSignal: an asynchronous channel
    /// which will be closed as soon as the handshake is finished (successfully or not).
    /// You can asynchronously await the returned HandshakeSignal.
    pub(crate) async fn spawn(
        service: scope::Service,
        stream: tcp::Stream,
        network_state: Arc<NetworkState>,
    ) -> anyhow::Result<()> { 
        let stream = stream::FramedStream::new(service.new_service()?,stream)?;
        let encoding = match stream.tier {
            tcp::Tier1::T1 => Encoding::Proto,
            tcp::Tier2::T2 => network_state.config.encoding,
        };
        let conn = match &stream.type_ {
            tcp::StreamType::Inbound => inbound_handshake(service,stream,network_state).await?,
            tcp::StreamType::Outbound{tier,peer_addr} => outbound_handshake(service,stream,network_state).await?, 
        };
    }
}
