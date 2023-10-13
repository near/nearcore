use super::sync_actor::SyncActor;
use actix::{Actor, Message};
use near_async::messaging::Sender;
use near_network::types::PeerManagerMessageRequest;
use near_store::ShardUId;
use std::collections::HashMap;

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub enum SyncMessageForClient {
    SyncDone,
}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub enum SyncMessage {
    StartSync,
}

struct ActorHandler {
    addr: actix::Addr<SyncActor>,
    arbiter: actix::Arbiter,
}

pub struct SyncAdapter {
    /// Address of the sync actors indexed by the ShardId.
    actor_handler_map: HashMap<ShardUId, ActorHandler>,
    client_adapter: Sender<SyncMessage>,
    network_adapter: Sender<PeerManagerMessageRequest>,
}

impl SyncAdapter {
    pub fn new(
        client_adapter: Sender<SyncMessage>,
        network_adapter: Sender<PeerManagerMessageRequest>,
    ) -> Self {
        Self { actor_handler_map: [].into(), client_adapter, network_adapter }
    }

    pub fn start(mut self, shard_uid: ShardUId) {
        let arbiter = actix::Arbiter::new();
        let arbiter_handle = arbiter.handle();
        let addr = SyncActor::start_in_arbiter(&arbiter_handle, move |_ctx| {
            SyncActor::new(shard_uid, self.client_adapter, self.network_adapter)
        });
        self.actor_handler_map.insert(shard_uid, ActorHandler { addr, arbiter });
    }

    pub fn stop(mut self, shard_uid: ShardUId) {
        self.actor_handler_map.get_mut(&shard_uid).map(|v| v.arbiter.stop());
    }
}
