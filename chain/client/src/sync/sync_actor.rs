use super::adapter::{SyncMessage as ClientSyncMessage, SyncShardInfo};
use near_async::messaging::Sender;
use near_network::types::{PeerManagerMessageRequest, StateSyncResponse};
use near_o11y::{handler_debug_span, WithSpanContext};
use near_performance_metrics_macros::perf;
use near_primitives::hash::CryptoHash;
use near_store::ShardUId;
use tracing::{debug, info, warn};

/// Message channels
#[allow(dead_code)]
struct MessageSenders {
    /// Used to send messages to client
    client_adapter: Sender<ClientSyncMessage>,
    /// Used to send messages to peer manager
    network_adapter: Sender<PeerManagerMessageRequest>,
}

/// Actor that runs state sync for a shard
#[allow(dead_code)]
pub struct SyncActor {
    /// Shard being synced
    shard_uid: ShardUId,
    /// Hash of the state that is downloaded
    sync_hash: CryptoHash,
    /// Channels used to communicate with other actors
    senders: MessageSenders,
}

impl SyncActor {
    pub fn new(
        shard_uid: ShardUId,
        client_adapter: Sender<ClientSyncMessage>,
        network_adapter: Sender<PeerManagerMessageRequest>,
    ) -> Self {
        Self {
            shard_uid,
            sync_hash: CryptoHash::new(),
            senders: MessageSenders { client_adapter, network_adapter },
        }
    }

    pub fn handle_client_sync_message(&mut self, msg: ClientSyncMessage) {
        match msg {
            ClientSyncMessage::StartSync(SyncShardInfo { sync_hash, shard_uid }) => {
                assert_eq!(shard_uid, self.shard_uid, "Message is not for this shard SyncActor");
                // Start syncing the shard.
                if self.sync_hash == sync_hash {
                    debug!(target: "sync", shard_id = ?self.shard_uid.shard_id, "Sync already running.");
                    return;
                }
                info!(target: "sync", shard_id = ?self.shard_uid.shard_id, "Starting sync on shard");
                // TODO: Add logic to commence state sync.
                self.sync_hash = sync_hash;
            }
            ClientSyncMessage::SyncDone(_) => {
                warn!(target: "sync", "Unsupported message received by SyncActor: SyncDone.");
            }
        }
    }

    pub fn handle_network_sync_message(&mut self, msg: StateSyncResponse) {
        match msg {
            StateSyncResponse::HeaderResponse => {
                debug!(target: "sync", shard_id = ?self.shard_uid.shard_id, "Got header response");
            }
            StateSyncResponse::PartResponse => {
                warn!(target: "sync", "Unsupported message received by SyncActor: SyncDone.");
            }
        }
    }
}

/// Control the flow of the state sync actor
impl actix::Actor for SyncActor {
    type Context = actix::Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        info!(target: "sync", shard_id = ?self.shard_uid.shard_id, "Sync actor started.");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!(target: "sync", shard_id = ?self.shard_uid.shard_id, "Sync actor stopped.");
    }
}

/// Process messages from client
impl actix::Handler<WithSpanContext<ClientSyncMessage>> for SyncActor {
    type Result = ();
    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<ClientSyncMessage>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "sync", msg);
        self.handle_client_sync_message(msg);
    }
}

/// Process messages from network
impl actix::Handler<WithSpanContext<StateSyncResponse>> for SyncActor {
    type Result = ();
    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<StateSyncResponse>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "sync", msg);
        self.handle_network_sync_message(msg);
    }
}
