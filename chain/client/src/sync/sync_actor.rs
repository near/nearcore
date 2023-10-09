use near_async::messaging::Sender;
use near_primitives::hash::CryptoHash;
use near_store::ShardUId;
use tracing::log::info;

use super::adapter::SyncMessageForClient;
use near_network::state_sync::SyncActorMessageForNetwork;
struct MessageSenders {
    client_adapter: Sender<SyncMessageForClient>,
    network_adapter: Sender<SyncActorMessageForNetwork>,
}

pub struct SyncActor {
    shard_uid: ShardUId,
    sync_hash: CryptoHash,
    senders: MessageSenders,
}

impl SyncActor {
    pub fn new(
        shard_uid: ShardUId,
        client_adapter: Sender<SyncMessageForClient>,
        network_adapter: Sender<SyncActorMessageForNetwork>,
    ) -> Self {
        Self {
            shard_uid,
            sync_hash: CryptoHash::new(),
            senders: MessageSenders { client_adapter, network_adapter },
        }
    }
}

impl actix::Actor for SyncActor {
    type Context = actix::Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        info!(target: "sync", "Sync actor started for shard_id: {}", self.shard_uid.shard_id);
    }

    fn stopped(&mut self, ctx: &mut Self::Context) {
        info!(target: "sync", "Sync actor stopped for shard_id: {}", self.shard_uid.shard_id);
    }
}
