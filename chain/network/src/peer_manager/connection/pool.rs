
#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
pub(crate) enum PoolError {
    #[error("already connected to this peer")]
    AlreadyConnected,
    #[error("already connected to peer {peer_id} with the same account key {account_key}")]
    AlreadyConnectedAccount { peer_id: PeerId, account_key: PublicKey },
    #[error("already started another outbound connection to this peer")]
    AlreadyStartedConnecting,
    #[error("loop connections are not allowed")]
    UnexpectedLoopConnection,
}

#[derive(Clone)]
pub(crate) struct PoolSnapshot {
    pub me: PeerId,
    /// Set of started outbound connections, which are not ready yet.
    /// We need to keep those to prevent a deadlock when 2 peers try
    /// to connect to each other at the same time.
    ///
    /// The procedure of establishing a connections should look as follows:
    /// 1. Peer A decides to connect to peer B.
    /// 2. Peer A gets an OutboundHandshakePermit by calling pool.start_outbound(B).
    /// 3. Peer A connects to peer B.
    /// 4. Peer B accepts the connection by calling pool.insert_ready(<connection to A>).
    /// 5. Peer B notifies A that it has accepted the connection.
    /// 6. Peer A accepts the connection by calling pool.insert_ready(<connection to B>).
    /// 7. Peer A drops the OutboundHandshakePermit.
    ///
    /// In case any of these steps fails the connection and the OutboundHandshakePermit
    /// should be dropped.
    ///
    /// Now imagine that A and B try to connect to each other at the same time:
    /// a. Peer A executes 1,2,3.
    /// b. Peer B executes 1,2,3.
    /// c. Both A and B try to execute 4 and exactly one of these calls will succeed: the tie
    ///    is broken by comparing PeerIds: connection from smaller to bigger takes priority.
    ///    WLOG let us assume that A < B.
    /// d. Peer A rejects connection from B, peer B accepts connection from A and notifies A.
    /// e. A continues with 6,7, B just drops the connection and the permit.
    ///
    /// Now imagine a different interleaving:
    /// a. Peer B executes 1,2,3 and A accepts the connection (i.e. 4)
    /// b. Peer A executes 1 and then attempts 2.
    /// In this scenario A will fail to obtain a permit, because it has already accepted a
    /// connection from B.
    pub handshakes: im::HashSet<HandshakeKey>,
    /// Connections which have completed the handshake and are ready
    /// for transmitting messages.
    pub ready: im::HashMap<PeerId, scope::Service<Connection>>,
    /// Index on `ready` by Connection.owned_account.account_key.
    /// We allow only 1 connection to a peer with the given account_key,
    /// as it is an invalid setup to have 2 nodes acting as the same validator.
    pub ready_by_account_key: im::HashMap<PublicKey, scope::Service<Connection>>,
    /// Inbound end of the loop connection. The outbound end is added to the `ready` set.
    pub ready_loop_inbound: Option<scope::Service<Connection>>,
}

pub(crate) struct Pool(ArcMutex<PoolSnapshot>);

pub(crate) struct Handshake<'a>(&'a Pool, HandshakeKey);

pub(crate) struct Ready<'a>(Handshake<'a>, Arc<Connection>);

impl<'a> Handshake<'a> {
    pub fn complete(self, conn: Arc<Connection>) -> Result<Ready<'a>,PoolError> {
        self.0.try_update(|mut pool| {
            // We support loopback connections for the purpose of 
            // validating our own external IP. This is the only case
            // in which we allow 2 connections in a pool to have the same
            // PeerId. The outbound connection is added to the
            // `ready` set, the inbound connection is put into dedicated `loop_inbound` field.
            if self.1.peer_type == PeerType::Inbound && pool.me == self.1.peer_id {
                if pool.ready_loop_inbound.swap(conn.clone()).is_some() {
                    return Err(PoolError::AlreadyConnected);
                }
                return Ok(((),pool));
            }

            if self.1.peer_type == PeerType::Inbound && pool.me < self.1.peer_id {
                if pool.handshakes.contains(HandshakeKey{peer_type: PeerType::Outbound, peer_id: self.1.peer_id}) {
                    return Err(PoolError::AlreadyStartedConnecting);
                } 
            }

            if pool.ready.insert(self.1.peer_id,conn).is_some() {
                return Err(PoolError::AlreadyConnected);
            }

            if let Some(owned_account) = &conn.owned_account {
                // Only 1 connection per account key is allowed.
                // Having 2 peers use the same account key is an invalid setup,
                // which violates the BFT consensus anyway.
                // TODO(gprusak): an incorrectly closed TCP connection may remain in ESTABLISHED
                // state up to minutes/hours afterwards. This may cause problems in
                // case a validator is restarting a node after crash and the new node has the same
                // peer_id/account_key/IP:port as the old node. What is the desired behavior is
                // such a case? Linux TCP implementation supports:
                // TCP_USER_TIMEOUT - timeout for ACKing the sent data
                // TCP_KEEPIDLE - idle connection time after which a KEEPALIVE is sent
                // TCP_KEEPINTVL - interval between subsequent KEEPALIVE probes
                // TCP_KEEPCNT - number of KEEPALIVE probes before closing the connection.
                // If it ever becomes a problem, we can eiter:
                // 1. replace TCP with sth else, like QUIC.
                // 2. use some lower level API than tokio::net to be able to set the linux flags.
                // 3. implement KEEPALIVE equivalent manually on top of TCP to resolve conflicts.
                // 4. allow overriding old connections by new connections, but that will require
                //    a deeper thought to make sure that the connections will be eventually stable
                //    and that incorrect setups will be detectable.
                if let Some(conn) = pool.ready_by_account_key.insert(owned_account.account_key.clone(), peer.clone()) {
                    // Unwrap is safe, because pool.ready_by_account_key is an index on connections
                    // with owned_account present.
                    let err = PoolError::AlreadyConnectedAccount{
                        peer_id: conn.peer_info.id.clone(),
                        account_key: conn.owned_account.as_ref().unwrap().account_key.clone(),
                    };
                    // We duplicate the error logging here, because returning an error
                    // from insert_ready is expected (pool may regularly reject connections),
                    // however conflicting connections with the same account key indicate an
                    // incorrect validator setup, so we log it here as a warn!, rather than just
                    // info!.
                    tracing::warn!(target:"network", "Pool::register({id}): {err}");
                    metrics::ALREADY_CONNECTED_ACCOUNT.inc();
                    return Err(err);
                }
            }
            Ok(((),pool))
        })?;
        Ok(Ready(self,conn))
    }
}

impl<'a> Drop for Handshake<'a> {
    fn drop(&mut self) {
        self.0.update(|mut pool| pool.handshake.remove(&self.1));
    }
}

impl<'a> fmt::Debug for Handshake<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.peer_id().fmt(f)
    }
}

impl<'a> Drop for Ready<'a> {
    fn drop(&mut self) {
        self.0.0.update(|mut pool| {
            if self.0.1.peer_type && self.0.1.peer_id==pool.me {
                pool.loop_inbound = None;
            } else {
                pool.ready.remove(&self.0.1.peer_id);
                if let Some(owned_account) = &conn.owned_account {
                    pool.ready_by_account_key.remove(&owned_account.account_key);
                }
            }
        });
    }
}

impl Pool {
    pub fn new(me: PeerId) -> Pool {
        Self(ArcMutex::new(PoolSnapshot {
            me,
            handshakes: im::HashMap::new(),
            ready: im::HashMap::new(),
            ready_by_account_key: im::HashMap::new(),
            ready_loop_inbound: None,
        }))
    }

    pub fn load(&self) -> Arc<PoolSnapshot> {
        self.0.load()
    }
}

impl Pool {
    /// Reserves an OutboundHandshakePermit for the given peer_id.
    /// It should be called before attempting to connect to this peer.
    /// The returned permit shouldn't be dropped until insert_ready for this
    /// outbound connection is called.
    ///
    /// This is required to resolve race conditions in case 2 nodes try to connect
    /// to each other at the same time.
    ///
    /// NOTE: Pool supports loop connections (i.e. connections in which both ends are the same
    /// node) for the purpose of verifying one's own public IP.
    pub fn start_handshake(&self, key: HandshakeKey) -> Result<Handshake<'_>, PoolError> {
        self.0.try_update(|mut pool| {
            if pool.handshakes.insert(key.clone()).is_some() {
                return Err(PoolError::AlreadyStartedConnecting);
            } 
            Ok(((), pool))
        })?;
        Ok(Handshake(self,key))
    }

    /// Send message to peer that belongs to our active set
    /// Return whether the message is sent or not.
    pub async fn send_message(&self, peer_id: PeerId, msg: &PeerMessage) -> Result<bool,stream::Error> {
        let this = self.load();
        let Some(conn) = pool.ready.get(&peer_id) else { return Ok(false); };
        conn.send_message(msg).await?
        Ok(true)
    }

    /// Broadcast message to all ready peers.
    pub async fn broadcast_message(&self, msg: &PeerMessage) {
        metrics::BROADCAST_MESSAGES.with_label_values(&[msg.msg_variant()]).inc();
        scope::run!(|s| async {
            let this = self.load();
            for conn in this.ready.values() {
                if conn.peer_id != this.me {
                    s.spawn(async {
                        if let Err(err) = conn.send_message(msg).await {
                            tracing::warn!(target:"network", "send_message({conn:?}): {err}");
                        }
                        Ok(())
                    });
                }
            }
            Ok(())
        }).unwrap();
    }
}
