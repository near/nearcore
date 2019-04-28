use std::collections::hash_map::RandomState;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use actix::actors::resolver::{ConnectAddr, Resolver};
use actix::io::{FramedWrite, WriteHandler};
use actix::prelude::Stream;
use actix::{
    Actor, ActorContext, ActorFuture, Addr, Arbiter, AsyncContext, Context, ContextFutureSpawner,
    Handler, Message, Recipient, StreamHandler, System, SystemService, WrapFuture,
};
use chrono::{DateTime, Utc};
use log::{debug, error, info, warn};
use rand::{thread_rng, Rng};
use tokio::codec::FramedRead;
use tokio::io::AsyncRead;
use tokio::io::WriteHalf;
use tokio::net::{TcpListener, TcpStream};

use near_store::Store;
use primitives::crypto::signature::{PublicKey, SecretKey};
use primitives::hash::CryptoHash;

use crate::codec::Codec;
use crate::peer::Peer;
use crate::types::{
    Consolidate, InboundTcpConnect, KnownPeerState, KnownPeerStatus, NumActivePeers,
    OutboundTcpConnect, PeerId, PeerType, SendMessage, Unregister,
};
pub use crate::types::{NetworkConfig, PeerInfo};

mod codec;
mod peer;
mod protocol;
pub mod types;

pub mod test_utils;

pub enum NetworkRequests {
    Block { hash: CryptoHash, peer_info: PeerInfo },
}

impl Message for NetworkRequests {
    type Result = ();
}

pub struct PeerManagerActor {
    store: Arc<Store>,
    config: NetworkConfig,
    outgoing_peers: HashSet<PeerId>,
    active_peers: HashMap<PeerId, Recipient<SendMessage>>,
    peer_states: HashMap<PeerId, KnownPeerState>,
}

impl PeerManagerActor {
    pub fn new(store: Arc<Store>, config: NetworkConfig) -> Self {
        let mut peer_states = HashMap::default();
        for peer_info in config.boot_nodes.iter() {
            peer_states.insert(peer_info.id, KnownPeerState::new(peer_info.clone()));
        }
        debug!(target: "network", "Found known peers: {} (boot nodes={})", peer_states.len(), config.boot_nodes.len());
        PeerManagerActor {
            store,
            config,
            active_peers: HashMap::default(),
            outgoing_peers: HashSet::default(),
            peer_states,
        }
    }

    fn num_active_peers(&self) -> usize {
        self.active_peers.len()
    }

    fn register_peer(&mut self, peer_info: PeerInfo, addr: Recipient<SendMessage>) {
        if self.outgoing_peers.contains(&peer_info.id) {
            self.outgoing_peers.remove(&peer_info.id);
        }
        self.active_peers.insert(peer_info.id, addr);
        let entry = self.peer_states.entry(peer_info.id).or_insert(KnownPeerState::new(peer_info));
        entry.last_seen = Utc::now();
        entry.status = KnownPeerStatus::Connected;
    }

    fn unregister_peer(&mut self, peer_id: PeerId) {
        if let Some(peer_state) = self.peer_states.get_mut(&peer_id) {
            self.active_peers.remove(&peer_id);
            peer_state.last_seen = Utc::now();
            peer_state.status = KnownPeerStatus::NotConnected;
        } else {
            error!(target: "network", "Unregistering unknown peer: {}", peer_id);
        }
    }

    fn ban_peer(&mut self, peer_id: PeerId) {
        if let Some(peer_state) = self.peer_states.get_mut(&peer_id) {
            info!(target: "network", "Banning peer {:?}", peer_state.peer_info);
            peer_state.status = KnownPeerStatus::Banned;
        } else {
            error!(target: "network", "Trying to ban unknown peer: {}", peer_id);
        }
    }

    fn connect_peer(
        &mut self,
        recipient: Addr<Self>,
        stream: TcpStream,
        peer_type: PeerType,
        peer_info: Option<PeerInfo>,
    ) {
        let public_key = self.config.public_key;
        let server_addr = self.config.addr;
        let handshake_timeout = self.config.handshake_timeout;
        Peer::create(move |ctx| {
            let server_addr = server_addr.unwrap_or_else(|| stream.local_addr().unwrap());
            let remote_addr = stream.peer_addr().unwrap();
            let (read, write) = stream.split();

            Peer::add_stream(FramedRead::new(read, Codec::new()), ctx);
            Peer::new(
                // TODO: add node's account id if given.
                PeerInfo { id: public_key, addr: Some(server_addr), account_id: None },
                remote_addr,
                peer_info,
                peer_type,
                FramedWrite::new(write, Codec::new(), ctx),
                handshake_timeout,
                recipient,
            )
        });
    }

    fn is_outbound_bootstrap_needed(&self) -> bool {
        self.active_peers.len() < (self.config.peer_max_count as usize)
    }

    /// Get a random peer we are not connected to from the known list.
    fn sample_random_peer(&self) -> Option<PeerInfo> {
        let unconnected_peers: Vec<PeerInfo> = self
            .peer_states
            .values()
            .filter_map(|p| {
                if p.status == KnownPeerStatus::NotConnected || p.status == KnownPeerStatus::Unknown
                {
                    Some(p.peer_info.clone())
                } else {
                    None
                }
            })
            .collect();
        let index = thread_rng().gen_range(0, std::cmp::max(unconnected_peers.len(), 1));

        unconnected_peers
            .iter()
            .enumerate()
            .filter_map(|(i, v)| if i == index { Some(v.clone()) } else { None })
            .next()
    }

    /// Periodically bootstrap outbound connections from known peers.
    fn bootstrap_peers(&self, ctx: &mut Context<Self>) {
        if self.is_outbound_bootstrap_needed() {
            if let Some(peer_info) = self.sample_random_peer() {
                ctx.notify(OutboundTcpConnect { peer_info });
            }
        }

        // Reschedule the bootstrap peer task.
        ctx.run_later(self.config.bootstrap_peers_period, move |act, ctx| {
            act.bootstrap_peers(ctx);
        });
    }
}

impl Actor for PeerManagerActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // Start server if address provided.
        if let Some(server_addr) = self.config.addr {
            // TODO: for now crashes if server didn't start.
            let listener = TcpListener::bind(&server_addr).unwrap();
            info!(target: "network", "Server listening at {}", server_addr);
            ctx.add_message_stream(listener.incoming().map_err(|_| ()).map(InboundTcpConnect::new));
        }

        // Start outbound peer bootstrapping.
        self.bootstrap_peers(ctx);
    }
}

impl Handler<NetworkRequests> for PeerManagerActor {
    type Result = ();

    fn handle(&mut self, msg: NetworkRequests, ctx: &mut Context<Self>) -> Self::Result {
        match msg {
            _ => panic!("123"),
        }
    }
}

impl Handler<InboundTcpConnect> for PeerManagerActor {
    type Result = ();

    fn handle(&mut self, msg: InboundTcpConnect, ctx: &mut Self::Context) {
        self.connect_peer(ctx.address(), msg.stream, PeerType::Inbound, None);
    }
}

impl Handler<OutboundTcpConnect> for PeerManagerActor {
    type Result = ();

    fn handle(&mut self, msg: OutboundTcpConnect, ctx: &mut Self::Context) {
        if let Some(addr) = msg.peer_info.addr {
            Resolver::from_registry()
                .send(ConnectAddr(addr))
                .into_actor(self)
                .then(move |res, act, ctx| match res {
                    Ok(res) => match res {
                        Ok(stream) => {
                            debug!(target: "network", "Connected to {}", msg.peer_info);
                            act.outgoing_peers.insert(msg.peer_info.id);
                            act.connect_peer(
                                ctx.address(),
                                stream,
                                PeerType::Outbound,
                                Some(msg.peer_info),
                            );
                            actix::fut::ok(())
                        }
                        Err(err) => {
                            error!(target: "network", "Error connecting to {}: {}", addr, err);
                            actix::fut::err(())
                        }
                    },
                    Err(err) => {
                        error!(target: "network", "Error connecting to {}: {}", addr, err);
                        actix::fut::err(())
                    }
                })
                .wait(ctx);
        } else {
            warn!(target: "network", "Trying to connect to peer with no public address: {:?}", msg.peer_info);
        }
    }
}

impl Handler<Consolidate> for PeerManagerActor {
    type Result = bool;

    fn handle(&mut self, msg: Consolidate, ctx: &mut Self::Context) -> Self::Result {
        // We already connected to this peer.
        if self.active_peers.contains_key(&msg.peer_info.id) {
            return false;
        }
        // This is incoming connection but we have it in outgoing.
        // This only happens when both of us connect at the same time, break tie.
        if msg.peer_type == PeerType::Inbound && self.outgoing_peers.contains(&msg.peer_info.id) {
            // We pick connection that has lower id.
            if msg.peer_info.id > self.config.public_key {
                return false;
            }
        }
        // TODO: check that address works and add account id.
        self.register_peer(msg.peer_info, msg.actor);
        true
    }
}

impl Handler<Unregister> for PeerManagerActor {
    type Result = ();

    fn handle(&mut self, msg: Unregister, ctx: &mut Self::Context) {
        self.unregister_peer(msg.peer_id);
    }
}

impl Handler<NumActivePeers> for PeerManagerActor {
    type Result = usize;

    fn handle(&mut self, msg: NumActivePeers, _: &mut Self::Context) -> Self::Result {
        self.num_active_peers()
    }
}
