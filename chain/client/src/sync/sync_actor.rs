use near_primitives::hash::CryptoHash;
use near_store::ShardUId;
use tracing::log::info;

pub struct SyncActor {
    shard_uid: ShardUId,
    sync_hash: CryptoHash,
}

impl SyncActor {
    pub fn new(shard_uid: ShardUId) -> Self {
        Self { shard_uid, sync_hash: CryptoHash::new() }
    }
}

impl actix::Actor for SyncActor {
    type Context = actix::Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        info!(target: "sync", "Sync actor started for shard_id: {}", self.shard_uid.shard_id);
    }

    fn stopping(&mut self, ctx: &mut Self::Context) -> actix::Running {
        actix::Running::Stop
    }

    fn stopped(&mut self, ctx: &mut Self::Context) {
        info!(target: "sync", "Sync actor stopped for shard_id: {}", self.shard_uid.shard_id);
    }
}
