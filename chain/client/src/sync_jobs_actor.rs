use crate::ClientActor;
use actix::AsyncContext;
use std::time::Duration;

use near_chain::chain::{
    do_apply_chunks, ApplyStatePartsRequest, ApplyStatePartsResponse, BlockCatchUpRequest,
    BlockCatchUpResponse,
};
use near_chain::resharding::ReshardingRequest;
use near_chain::Chain;
use near_o11y::{handler_debug_span, OpenTelemetrySpanExt, WithSpanContext, WithSpanContextExt};
use near_performance_metrics_macros::perf;
use near_primitives::state_part::PartId;
use near_primitives::state_sync::StatePartKey;
use near_primitives::types::ShardId;
use near_store::DBCol;

pub(crate) struct SyncJobsActor {
    pub(crate) client_addr: actix::Addr<ClientActor>,
}

pub(crate) fn create_sync_job_scheduler<M>(address: actix::Addr<SyncJobsActor>) -> Box<dyn Fn(M)>
where
    M: actix::Message + Send + 'static,
    M::Result: Send,
    SyncJobsActor: actix::Handler<WithSpanContext<M>>,
{
    Box::new(move |msg: M| {
        if let Err(err) = address.try_send(msg.with_span_context()) {
            match err {
                actix::dev::SendError::Full(request) => {
                    address.do_send(request);
                }
                actix::dev::SendError::Closed(_) => {
                    tracing::error!("Can't send message to SyncJobsActor, mailbox is closed");
                }
            }
        }
    })
}

impl SyncJobsActor {
    pub(crate) const MAILBOX_CAPACITY: usize = 100;

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
}

impl actix::Actor for SyncJobsActor {
    type Context = actix::Context<Self>;
}

impl actix::Handler<WithSpanContext<ApplyStatePartsRequest>> for SyncJobsActor {
    type Result = ();

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<ApplyStatePartsRequest>,
        _: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        let shard_id = msg.shard_uid.shard_id as ShardId;
        match self.clear_flat_state(&msg) {
            Err(err) => {
                self.client_addr.do_send(
                    ApplyStatePartsResponse {
                        apply_result: Err(err),
                        shard_id,
                        sync_hash: msg.sync_hash,
                    }
                    .with_span_context(),
                );
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
        self.client_addr.do_send(
            ApplyStatePartsResponse { apply_result: result, shard_id, sync_hash: msg.sync_hash }
                .with_span_context(),
        );
    }
}

impl actix::Handler<WithSpanContext<BlockCatchUpRequest>> for SyncJobsActor {
    type Result = ();

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<BlockCatchUpRequest>,
        _: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        tracing::debug!(target: "client", ?msg);
        let results = do_apply_chunks(msg.block_hash, msg.block_height, msg.work);

        self.client_addr.do_send(
            BlockCatchUpResponse { sync_hash: msg.sync_hash, block_hash: msg.block_hash, results }
                .with_span_context(),
        );
    }
}

impl actix::Handler<WithSpanContext<ReshardingRequest>> for SyncJobsActor {
    type Result = ();

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<ReshardingRequest>,
        context: &mut Self::Context,
    ) -> Self::Result {
        let (_span, mut resharding_request) = handler_debug_span!(target: "resharding", msg);
        let config = resharding_request.config.get();

        // Wait for the initial delay. It should only be used in tests.
        let initial_delay = config.initial_delay;
        if resharding_request.curr_poll_time == Duration::ZERO && initial_delay > Duration::ZERO {
            tracing::debug!(target: "resharding", ?resharding_request, ?initial_delay, "Waiting for the initial delay");
            resharding_request.curr_poll_time += initial_delay;
            context.notify_later(resharding_request.with_span_context(), initial_delay);
            return;
        }

        if Chain::retry_build_state_for_split_shards(&resharding_request) {
            // Actix implementation let's us send message to ourselves with a delay.
            // In case snapshots are not ready yet, we will retry resharding later.
            let retry_delay = config.retry_delay;
            tracing::debug!(target: "resharding", ?resharding_request, ?retry_delay, "Snapshot missing, retrying resharding later");
            resharding_request.curr_poll_time += retry_delay;
            context.notify_later(resharding_request.with_span_context(), retry_delay);
            return;
        }

        tracing::debug!(target: "resharding", ?resharding_request, "Starting resharding");
        let response = Chain::build_state_for_split_shards(resharding_request);
        self.client_addr.do_send(response.with_span_context());
    }
}
