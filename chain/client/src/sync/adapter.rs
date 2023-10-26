use super::sync_actor::SyncActor;
use actix::{dev::ToEnvelope, Actor, Message};
use core::fmt::Debug;
use near_async::messaging::Sender;
use near_chain::chain::ApplyStatePartsResponse;
use near_network::types::{
    PeerManagerMessageRequest, StateSync as NetworkStateSync, StateSyncResponse,
};
use near_primitives::hash::CryptoHash;
use near_primitives::types::ShardId;
use std::collections::HashMap;
use tracing::warn;

/// Information about the shard being synced
#[derive(Debug)]
pub struct SyncShardInfo {
    pub shard_id: ShardId,
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
    actor_handler_map: HashMap<ShardId, ActorHandler>,
}

impl SyncAdapter {
    pub fn new() -> Self {
        Self { actor_handler_map: [].into() }
    }

    /// Starts a new arbiter and runs the actor on it
    pub fn start(
        &mut self,
        shard_id: ShardId,
        client: Sender<ApplyStatePartsResponse>,
        network: Sender<PeerManagerMessageRequest>,
    ) {
        assert!(!self.actor_handler_map.contains_key(&shard_id), "Actor already started.");
        let arbiter = actix::Arbiter::new();
        let arbiter_handle = arbiter.handle();
        let addr = SyncActor::start_in_arbiter(&arbiter_handle, move |_ctx| {
            SyncActor::new(shard_id, client, network)
        });
        self.actor_handler_map.insert(shard_id, ActorHandler { addr, arbiter });
    }

    pub fn contains(&self, shard_id: ShardId) -> bool {
        self.actor_handler_map.contains_key(&shard_id)
    }

    /// Stop the actor and remove it
    pub fn stop(&mut self, shard_id: ShardId) {
        self.actor_handler_map
            .get_mut(&shard_id)
            .map_or_else(|| panic!("Actor not started."), |v| v.arbiter.stop());
    }

    /// Forward message to the right shard
    pub fn send<M>(&self, shard_id: ShardId, msg: M)
    where
        M: Message + Send + 'static,
        M::Result: Send,

        SyncActor: actix::Handler<M>,
        <SyncActor as Actor>::Context: ToEnvelope<SyncActor, M>,
    {
        let handler = self.actor_handler_map.get(&shard_id);
        match handler {
            None => {
                warn!(target: "sync", ?shard_id, "Tried sending message to non existing actor.")
            }
            Some(handler) => {
                if let Err(err) = handler.addr.try_send(msg) {
                    warn!(target: "sync", ?shard_id, "Error sending message for shard {}: {:?}.", shard_id, err);
                };
            }
        }
    }
}

/// Interface for network
#[async_trait::async_trait]
impl NetworkStateSync for SyncAdapter {
    async fn send(&mut self, shard_uid: ShardId, msg: StateSyncResponse) {
        let _ = self.send(shard_uid, msg);
    }
}
