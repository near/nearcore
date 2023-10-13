use near_async::messaging::Sender;
use near_network::types::PeerManagerMessageRequest;
use near_primitives::hash::CryptoHash;
use near_store::ShardUId;
use tracing::log::info;

use super::adapter::SyncMessage;
struct MessageSenders {
    client_adapter: Sender<SyncMessage>,
    network_adapter: Sender<PeerManagerMessageRequest>,
}

pub struct SyncActor {
    shard_uid: ShardUId,
    sync_hash: CryptoHash,
    senders: MessageSenders,
}

impl SyncActor {
    pub fn new(
        shard_uid: ShardUId,
        client_adapter: Sender<SyncMessage>,
        network_adapter: Sender<PeerManagerMessageRequest>,
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
