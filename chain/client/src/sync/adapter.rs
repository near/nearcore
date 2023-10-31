use super::sync_actor::SyncActor;
use actix::dev::ToEnvelope;
use actix::prelude::SendError;
use actix::{Actor, Message};
use actix_rt::Arbiter;
use core::fmt::Debug;
use near_async::messaging::Sender;
use near_network::types::{
    PeerManagerMessageRequest, StateSync as NetworkStateSync, StateSyncResponse,
};
use near_o11y::WithSpanContextExt;
use near_primitives::hash::CryptoHash;
use near_store::ShardUId;
use std::collections::HashMap;
use tracing::warn;

/// Information about the shard being synced
#[derive(Debug)]
pub struct SyncShardInfo {
    pub shard_uid: ShardUId,
    pub sync_hash: CryptoHash,
}

/// Messages between Client and Sync Actor
#[derive(Message, Debug)]
#[rtype(result = "()")]
pub enum SyncMessage {
    /// Notify an active actor to start syncing
    StartSync(SyncShardInfo),
    /// Notify the client that the work is done
    SyncDone(SyncShardInfo),
}

struct ActorHandler {
    /// Address of actor mailbox
    addr: actix::Addr<SyncActor>,
    /// Thread handler of actor
    arbiter: actix::Arbiter,
}

/// Manager for state sync threads.
/// Offers functions to interact with the sync actors, such as start, stop and send messages.
pub struct SyncAdapter {
    /// Address of the sync actors indexed by the SharUid
    actor_handler_map: HashMap<ShardUId, ActorHandler>,
    /// Message channel for client
    client_adapter: Sender<SyncMessage>,
    /// Message channel with network
    network_adapter: Sender<PeerManagerMessageRequest>,
}

impl SyncAdapter {
    pub fn new(
        client_adapter: Sender<SyncMessage>,
        network_adapter: Sender<PeerManagerMessageRequest>,
    ) -> Self {
        Self { actor_handler_map: [].into(), client_adapter, network_adapter }
    }

    /// Starts a new arbiter and runs the actor on it
    pub fn start(&mut self, shard_uid: ShardUId) {
        assert!(!self.actor_handler_map.contains_key(&shard_uid), "Actor already started.");
        let arbiter = Arbiter::new();
        let arbiter_handle = arbiter.handle();
        let client = self.client_adapter.clone();
        let network = self.network_adapter.clone();
        let addr = SyncActor::start_in_arbiter(&arbiter_handle, move |_ctx| {
            SyncActor::new(shard_uid, client, network)
        });
        self.actor_handler_map.insert(shard_uid, ActorHandler { addr, arbiter });
    }

    /// Stop the actor and remove it
    pub fn stop(&mut self, shard_uid: ShardUId) {
        self.actor_handler_map.remove(&shard_uid).expect("Actor not started.").arbiter.stop();
    }

    pub fn stop_all(&mut self) {
        self.actor_handler_map.drain().for_each(|(_shard_uid, handler)| {
            handler.arbiter.stop();
        });
    }

    /// Forward message to the right shard
    pub fn send<M>(&self, shard_uid: ShardUId, msg: M)
    where
        M: Message + Send + 'static,
        M::Result: Send,
        SyncActor: actix::Handler<M>,
        <SyncActor as Actor>::Context: ToEnvelope<SyncActor, M>,
    {
        let handler = self.actor_handler_map.get(&shard_uid);
        match handler {
            None => {
                warn!(target: "sync", ?shard_uid, "Tried sending message to non existing actor.")
            }
            Some(handler) => match handler.addr.try_send(msg) {
                Ok(_) => {}
                Err(SendError::Closed(_)) => {
                    warn!(target: "sync", ?shard_uid, "Error sending message, mailbox is closed.")
                }
                Err(SendError::Full(_)) => {
                    warn!(target: "sync", ?shard_uid, "Error sending message, mailbox is full.")
                }
            },
        }
    }
}

/// Interface for network
#[async_trait::async_trait]
impl NetworkStateSync for SyncAdapter {
    async fn send(&self, shard_uid: ShardUId, msg: StateSyncResponse) {
        self.send(shard_uid, msg.with_span_context());
    }
}
