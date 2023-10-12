use super::sync_actor::SyncActor;
use actix::Actor;
use near_store::ShardUId;
use std::collections::HashMap;

struct ActorHandler {
    addr: actix::Addr<SyncActor>,
    arbiter: actix::Arbiter,
}

pub struct SyncAdapter {
    /// Address of the sync actors indexed by the ShardId.
    actor_handler_map: HashMap<ShardUId, ActorHandler>,
}

impl SyncAdapter {
    pub fn new() -> Self {
        Self { actor_handler_map: [].into() }
    }

    pub fn start(mut self, shard_uid: ShardUId) {
        let arbiter = actix::Arbiter::new();
        let arbiter_handle = arbiter.handle();
        let addr =
            SyncActor::start_in_arbiter(&arbiter_handle, move |_ctx| SyncActor::new(shard_uid));
        self.actor_handler_map.insert(shard_uid, ActorHandler { addr, arbiter });
    }

    pub fn stop(mut self, shard_uid: ShardUId) {
        self.actor_handler_map.get_mut(&shard_uid).map(|v| v.arbiter.stop());
    }
}
