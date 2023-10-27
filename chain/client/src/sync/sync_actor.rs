use super::adapter::{SyncMessage as ClientSyncMessage, SyncShardInfo};
use near_async::messaging::Sender;
use near_chain::chain::{ApplyStatePartsRequest, ApplyStatePartsResponse};
use near_network::types::{PeerManagerMessageRequest, StateSyncResponse};
use near_o11y::{handler_debug_span, OpenTelemetrySpanExt, WithSpanContext};
use near_performance_metrics_macros::perf;
use near_primitives::hash::CryptoHash;
use near_primitives::state_part::PartId;
use near_primitives::state_sync::StatePartKey;
use near_primitives::types::ShardId;
use near_store::DBCol;
use tracing::{debug, info, warn};

/// Message channels
#[allow(dead_code)]
struct MessageSenders {
    /// Used to send messages to client
    client_adapter: Sender<ApplyStatePartsResponse>,
    /// Used to send messages to peer manager
    network_adapter: Sender<PeerManagerMessageRequest>,
}

/// Actor that runs state sync for a shard
#[allow(dead_code)]
pub struct SyncActor {
    /// Shard being synced
    shard_id: ShardId,
    /// Hash of the state that is downloaded
    sync_hash: CryptoHash,
    /// Channels used to communicate with other actors
    senders: MessageSenders,
}

impl SyncActor {
    pub fn new(
        shard_id: ShardId,
        client_adapter: Sender<ApplyStatePartsResponse>,
        network_adapter: Sender<PeerManagerMessageRequest>,
    ) -> Self {
        Self {
            shard_id,
            sync_hash: CryptoHash::new(),
            senders: MessageSenders { client_adapter, network_adapter },
        }
    }

    fn handle_client_sync_message(&mut self, msg: ClientSyncMessage) {
        match msg {
            ClientSyncMessage::StartSync(SyncShardInfo { sync_hash, shard_id }) => {
                assert_eq!(shard_id, self.shard_id, "Message is not for this shard SyncActor");
                // Start syncing the shard.
                info!(target: "sync", shard_id = ?self.shard_id, "Startgin sync on shard");
                // TODO: Add logic to commence state sync.
                self.sync_hash = sync_hash;
            }
            ClientSyncMessage::SyncDone(_) => {
                warn!(target: "sync", "Unsupported message received by SyncActor: SyncDone.");
            }
        }
    }

    fn handle_network_sync_message(&mut self, msg: StateSyncResponse) {
        match msg {
            StateSyncResponse::HeaderResponse => {
                debug!(target: "sync", shard_id = ?self.shard_id, "Got header response");
            }
            StateSyncResponse::PartResponse => {
                warn!(target: "sync", "Unsupported message received by SyncActor: SyncDone.");
            }
        }
    }

    /// Clears flat storage before applying state parts.
    /// Returns whether the flat storage state was cleared.
    fn clear_flat_state(
        &mut self,
        msg: &ApplyStatePartsRequest,
    ) -> Result<bool, near_chain_primitives::error::Error> {
        let _span = tracing::debug_span!(target: "client", "clear_flat_state").entered();
        Ok(msg
            .runtime_adapter
            .get_flat_storage_manager()
            .remove_flat_storage_for_shard(msg.shard_uid)?)
    }

    fn apply_parts(
        &mut self,
        msg: &ApplyStatePartsRequest,
    ) -> Result<(), near_chain_primitives::error::Error> {
        let _span = tracing::debug_span!(target: "client", "apply_parts").entered();
        let store = msg.runtime_adapter.store();

        let shard_id = msg.shard_uid.shard_id as ShardId;
        for part_id in 0..msg.num_parts {
            let key = borsh::to_vec(&StatePartKey(msg.sync_hash, shard_id, part_id))?;
            let part = store.get(DBCol::StateParts, &key)?.unwrap();

            msg.runtime_adapter.apply_state_part(
                shard_id,
                &msg.state_root,
                PartId::new(part_id, msg.num_parts),
                &part,
                &msg.epoch_id,
            )?;
        }

        Ok(())
    }
}

/// Control the flow of the state sync actor
impl actix::Actor for SyncActor {
    type Context = actix::Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        info!(target: "sync", shard_id = ?self.shard_id, "Sync actor started.");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!(target: "sync", shard_id = ?self.shard_id, "Sync actor stopped.");
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

impl actix::Handler<WithSpanContext<ApplyStatePartsRequest>> for SyncActor {
    type Result = ();
    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<ApplyStatePartsRequest>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "sync", msg);
        let shard_id = msg.shard_uid.shard_id as ShardId;
        match self.clear_flat_state(&msg) {
            Err(err) => {
                self.senders.client_adapter.send(ApplyStatePartsResponse {
                    apply_result: Err(err),
                    shard_id,
                    sync_hash: msg.sync_hash,
                });
                return;
            }
            Ok(false) => {
                // Can't panic here, because that breaks many KvRuntime tests.
                // TODO: figure out how to fix things so that we can panic here
                tracing::error!(target: "client", shard_uid = ?msg.shard_uid, "Failed to delete Flat State, but proceeding with applying state parts.");
            }
            Ok(true) => {
                tracing::debug!(target: "client", shard_uid = ?msg.shard_uid, "Deleted all Flat State");
            }
        }
        let result = self.apply_parts(&msg);
        self.senders.client_adapter.send(ApplyStatePartsResponse {
            apply_result: result,
            shard_id,
            sync_hash: msg.sync_hash,
        });
    }
}
