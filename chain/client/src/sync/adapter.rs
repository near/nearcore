use super::sync_actor::SyncActor;
use actix::{Actor, Message};
use actix_rt::Arbiter;
use core::fmt::Debug;
use near_async::actix::AddrWithAutoSpanContextExt;
use near_async::messaging::{IntoSender, Sender};
use near_network::types::{PeerManagerMessageRequest, StateSyncResponse};
use near_primitives::hash::CryptoHash;
use near_store::ShardUId;
use std::collections::HashMap;
use std::ops::DerefMut;
use std::sync::{Arc, Mutex};
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

pub struct SyncActorHandler {
    pub client_sender: Sender<SyncMessage>,
    pub network_sender: Sender<StateSyncResponse>,
    pub shutdown: Mutex<Box<dyn FnMut() + Send>>,
}

impl Drop for SyncActorHandler {
    fn drop(&mut self) {
        std::mem::replace(self.shutdown.lock().unwrap().deref_mut(), Box::new(|| {}))();
    }
}

/// Manager for state sync threads.
/// Offers functions to interact with the sync actors, such as start, stop and send messages.
pub struct SyncAdapter {
    /// Address of the sync actors indexed by the SharUid
    actor_handler_map: HashMap<ShardUId, SyncActorHandler>,
    /// Message channel for client
    client_adapter: Sender<SyncMessage>,
    /// Message channel with network
    network_adapter: Sender<PeerManagerMessageRequest>,
    /// Function to make an actor
    actor_maker: Arc<
        dyn Fn(ShardUId, Sender<SyncMessage>, Sender<PeerManagerMessageRequest>) -> SyncActorHandler
            + Send
            + Sync,
    >,
}

impl SyncAdapter {
    pub fn new(
        client_adapter: Sender<SyncMessage>,
        network_adapter: Sender<PeerManagerMessageRequest>,
        actor_maker: Arc<
            dyn Fn(
                    ShardUId,
                    Sender<SyncMessage>,
                    Sender<PeerManagerMessageRequest>,
                ) -> SyncActorHandler
                + Send
                + Sync,
        >,
    ) -> Self {
        Self { actor_handler_map: [].into(), client_adapter, network_adapter, actor_maker }
    }

    pub fn actix_actor_maker() -> Arc<
        dyn Fn(ShardUId, Sender<SyncMessage>, Sender<PeerManagerMessageRequest>) -> SyncActorHandler
            + Send
            + Sync,
    > {
        Arc::new(|shard_uid, client_sender, network_sender| {
            let arbiter = Arbiter::new();
            let arbiter_handle = arbiter.handle();
            let sync_actor = SyncActor::start_in_arbiter(&arbiter_handle, move |_ctx| {
                SyncActor::new(shard_uid, client_sender, network_sender)
            });
            SyncActorHandler {
                client_sender: sync_actor.clone().with_auto_span_context().into_sender(),
                network_sender: sync_actor.with_auto_span_context().into_sender(),
                shutdown: Mutex::new(Box::new(move || {
                    arbiter.stop();
                })),
            }
        })
    }

    /// Starts a new arbiter and runs the actor on it
    pub fn start(&mut self, shard_uid: ShardUId) {
        assert!(!self.actor_handler_map.contains_key(&shard_uid), "Actor already started.");
        let client = self.client_adapter.clone();
        let network = self.network_adapter.clone();
        self.actor_handler_map.insert(shard_uid, (self.actor_maker)(shard_uid, client, network));
    }

    /// Stop the actor and remove it
    pub fn stop(&mut self, shard_uid: ShardUId) {
        self.actor_handler_map.remove(&shard_uid).expect("Actor not started.");
    }

    pub fn stop_all(&mut self) {
        self.actor_handler_map.clear();
    }

    /// Forward message to the right shard
    pub fn send_state_sync_response(&self, shard_uid: ShardUId, msg: StateSyncResponse) {
        let handler = self.actor_handler_map.get(&shard_uid);
        match handler {
            None => {
                warn!(target: "sync", ?shard_uid, "Tried sending message to non existing actor.")
            }
            Some(handler) => handler.network_sender.send(msg),
        }
    }

    pub fn send_sync_message(&self, shard_uid: ShardUId, msg: SyncMessage) {
        let handler = self.actor_handler_map.get(&shard_uid);
        match handler {
            None => {
                warn!(target: "sync", ?shard_uid, "Tried sending message to non existing actor.")
            }
            Some(handler) => handler.client_sender.send(msg),
        }
    }
}
