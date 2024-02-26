use near_async::futures::{FutureSpawner, FutureSpawnerExt};
use near_async::messaging::{CanSend, Sender};
use near_async::{MultiSend, MultiSendMessage, MultiSenderFrom};
use near_chain::chain::{
    do_apply_chunks, ApplyStatePartsRequest, ApplyStatePartsResponse, BlockCatchUpRequest,
    BlockCatchUpResponse,
};
use near_chain::resharding::{ReshardingRequest, ReshardingResponse};
use near_chain::Chain;
use near_primitives::state_part::PartId;
use near_primitives::state_sync::StatePartKey;
use near_primitives::types::ShardId;
use near_store::DBCol;
use std::time::Duration;

#[derive(Clone, MultiSend, MultiSenderFrom, MultiSendMessage)]
#[multi_send_message_derive(Debug)]
pub struct ClientSenderForSyncJobs {
    apply_state_parts_response: Sender<ApplyStatePartsResponse>,
    block_catch_up_response: Sender<BlockCatchUpResponse>,
    resharding_response: Sender<ReshardingResponse>,
}

#[derive(Clone, MultiSend, MultiSenderFrom, MultiSendMessage)]
#[multi_send_message_derive(Debug)]
pub struct SyncJobsSenderForSyncJobs {
    resharding_request: Sender<ReshardingRequest>,
}

pub struct SyncJobsActions {
    client_sender: ClientSenderForSyncJobs,
    sync_jobs_sender: SyncJobsSenderForSyncJobs,
}

impl SyncJobsActions {
    pub fn new(
        client_sender: ClientSenderForSyncJobs,
        sync_jobs_sender: SyncJobsSenderForSyncJobs,
    ) -> Self {
        Self { client_sender, sync_jobs_sender }
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

    /// Clears flat storage before applying state parts.
    /// Returns whether the flat storage state was cleared.
    fn clear_flat_state(
        &mut self,
        msg: &ApplyStatePartsRequest,
    ) -> Result<bool, near_chain_primitives::error::Error> {
        let _span = tracing::debug_span!(target: "client", "clear_flat_state").entered();
        let mut store_update = msg.runtime_adapter.store().store_update();
        let success = msg
            .runtime_adapter
            .get_flat_storage_manager()
            .remove_flat_storage_for_shard(msg.shard_uid, &mut store_update)?;
        store_update.commit()?;
        Ok(success)
    }

    pub fn handle_apply_state_parts_request(&mut self, msg: ApplyStatePartsRequest) {
        let shard_id = msg.shard_uid.shard_id as ShardId;
        match self.clear_flat_state(&msg) {
            Err(err) => {
                self.client_sender.send(ApplyStatePartsResponse {
                    apply_result: Err(err),
                    shard_id,
                    sync_hash: msg.sync_hash,
                });
                return;
            }
            Ok(false) => {
                // Can't panic here, because that breaks many KvRuntime tests.
                tracing::error!(target: "client", shard_uid = ?msg.shard_uid, "Failed to delete Flat State, but proceeding with applying state parts.");
            }
            Ok(true) => {
                tracing::debug!(target: "client", shard_uid = ?msg.shard_uid, "Deleted all Flat State");
            }
        }

        let result = self.apply_parts(&msg);
        self.client_sender.send(ApplyStatePartsResponse {
            apply_result: result,
            shard_id,
            sync_hash: msg.sync_hash,
        });
    }

    pub fn handle_block_catch_up_request(&mut self, msg: BlockCatchUpRequest) {
        tracing::debug!(target: "client", ?msg);
        let results = do_apply_chunks(msg.block_hash, msg.block_height, msg.work);

        self.client_sender.send(BlockCatchUpResponse {
            sync_hash: msg.sync_hash,
            block_hash: msg.block_hash,
            results,
        });
    }

    pub fn handle_resharding_request(
        &mut self,
        mut resharding_request: ReshardingRequest,
        future_spawner: &dyn FutureSpawner,
    ) {
        let config = resharding_request.config.get();

        // Wait for the initial delay. It should only be used in tests.
        let initial_delay = config.initial_delay;
        if resharding_request.curr_poll_time == Duration::ZERO && initial_delay > Duration::ZERO {
            tracing::debug!(target: "resharding", ?resharding_request, ?initial_delay, "Waiting for the initial delay");
            resharding_request.curr_poll_time += initial_delay;
            future_spawner.spawn("resharding initial delay", {
                let sender = self.sync_jobs_sender.clone();
                async move {
                    tokio::time::sleep(initial_delay).await;
                    sender.send(resharding_request);
                }
            });
            return;
        }

        if Chain::retry_build_state_for_split_shards(&resharding_request) {
            // Actix implementation let's us send message to ourselves with a delay.
            // In case snapshots are not ready yet, we will retry resharding later.
            let retry_delay = config.retry_delay;
            tracing::debug!(target: "resharding", ?resharding_request, ?retry_delay, "Snapshot missing, retrying resharding later");
            resharding_request.curr_poll_time += retry_delay;
            future_spawner.spawn("resharding retry", {
                let sender = self.sync_jobs_sender.clone();
                async move {
                    tokio::time::sleep(retry_delay).await;
                    sender.send(resharding_request);
                }
            });
            return;
        }

        tracing::debug!(target: "resharding", ?resharding_request, "Starting resharding");
        let response = Chain::build_state_for_split_shards(resharding_request);
        self.client_sender.send(response);
    }
}
