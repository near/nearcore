use actix::Actor;
use near_async::actix_wrapper::ActixWrapper;
use near_async::messaging::{self, CanSend, Handler, Sender};
use near_async::{MultiSend, MultiSenderFrom};
use near_chain::chain::{
    do_apply_chunks, ApplyStatePartsRequest, ApplyStatePartsResponse, BlockCatchUpRequest,
    BlockCatchUpResponse, LoadMemtrieRequest, LoadMemtrieResponse,
};
use near_performance_metrics_macros::perf;
use near_primitives::state_part::PartId;
use near_primitives::state_sync::StatePartKey;
use near_primitives::types::ShardId;
use near_store::adapter::StoreUpdateAdapter;
use near_store::DBCol;

// Set the mailbox capacity for the SyncJobsActor from default 16 to 100.
const MAILBOX_CAPACITY: usize = 100;

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct ClientSenderForSyncJobs {
    apply_state_parts_response: Sender<ApplyStatePartsResponse>,
    block_catch_up_response: Sender<BlockCatchUpResponse>,
    load_memtrie_response: Sender<LoadMemtrieResponse>,
}

pub struct SyncJobsActor {
    client_sender: ClientSenderForSyncJobs,
}

impl messaging::Actor for SyncJobsActor {}

impl Handler<LoadMemtrieRequest> for SyncJobsActor {
    #[perf]
    fn handle(&mut self, msg: LoadMemtrieRequest) {
        self.handle_load_memtrie_request(msg);
    }
}

impl Handler<ApplyStatePartsRequest> for SyncJobsActor {
    #[perf]
    fn handle(&mut self, msg: ApplyStatePartsRequest) {
        self.handle_apply_state_parts_request(msg);
    }
}

impl Handler<BlockCatchUpRequest> for SyncJobsActor {
    #[perf]
    fn handle(&mut self, msg: BlockCatchUpRequest) {
        self.handle_block_catch_up_request(msg);
    }
}

impl SyncJobsActor {
    pub fn new(client_sender: ClientSenderForSyncJobs) -> Self {
        Self { client_sender }
    }

    pub fn spawn_actix_actor(self) -> (actix::Addr<ActixWrapper<Self>>, actix::ArbiterHandle) {
        let actix_wrapper = ActixWrapper::new(self);
        let arbiter = actix::Arbiter::new().handle();
        let addr = ActixWrapper::<Self>::start_in_arbiter(&arbiter, |ctx| {
            ctx.set_mailbox_capacity(MAILBOX_CAPACITY);
            actix_wrapper
        });
        (addr, arbiter)
    }

    fn apply_parts(
        &mut self,
        msg: &ApplyStatePartsRequest,
    ) -> Result<(), near_chain_primitives::error::Error> {
        let _span: tracing::span::EnteredSpan =
            tracing::debug_span!(target: "sync", "apply_parts").entered();
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
        let _span = tracing::debug_span!(target: "sync", "clear_flat_state").entered();
        let mut store_update = msg.runtime_adapter.store().store_update().flat_store_update();
        let success = msg
            .runtime_adapter
            .get_flat_storage_manager()
            .remove_flat_storage_for_shard(msg.shard_uid, &mut store_update)?;
        store_update.commit()?;
        Ok(success)
    }

    /// This call is synchronous and handled in `sync_jobs_actor`.
    pub fn handle_load_memtrie_request(&mut self, msg: LoadMemtrieRequest) {
        let result = msg
            .runtime_adapter
            .get_tries()
            .load_mem_trie_on_catchup(&msg.shard_uid, &msg.prev_state_root)
            .map_err(|error| error.into());
        self.client_sender.send(LoadMemtrieResponse {
            load_result: result,
            shard_uid: msg.shard_uid,
            sync_hash: msg.sync_hash,
        });
    }

    pub fn handle_apply_state_parts_request(&mut self, msg: ApplyStatePartsRequest) {
        // Unload mem-trie (in case it is still loaded) before we apply state parts.
        msg.runtime_adapter.get_tries().unload_mem_trie(&msg.shard_uid);

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
                tracing::error!(target: "sync", shard_uid = ?msg.shard_uid, "Failed to delete Flat State, but proceeding with applying state parts.");
            }
            Ok(true) => {
                tracing::debug!(target: "sync", shard_uid = ?msg.shard_uid, "Deleted all Flat State");
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
        tracing::debug!(target: "sync", ?msg);
        let results = do_apply_chunks(msg.block_hash, msg.block_height, msg.work);

        self.client_sender.send(BlockCatchUpResponse {
            sync_hash: msg.sync_hash,
            block_hash: msg.block_hash,
            results,
        });
    }
}
