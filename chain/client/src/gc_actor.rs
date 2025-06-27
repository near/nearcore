use crate::metrics;
use near_async::futures::{DelayedActionRunner, DelayedActionRunnerExt};
use near_async::messaging::Actor;
#[cfg(feature = "test_features")]
use near_async::messaging::Handler;
use near_chain::ChainGenesis;
use near_chain::{ChainStore, ChainStoreAccess, types::RuntimeAdapter};
use near_chain_configs::GCConfig;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_store::Store;
use near_store::db::metadata::DbKind;
use std::sync::Arc;

/// An actor for garbage collection that runs in its own thread
/// The actor runs periodically, as determined by `gc_step_period`,
/// to garbage collect blockchain data
pub struct GCActor {
    store: ChainStore,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    gc_config: GCConfig,
    is_archive: bool,
    /// In some tests we may want to temporarily disable GC
    no_gc: bool,
}

impl GCActor {
    pub fn new(
        store: Store,
        genesis: &ChainGenesis,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
        gc_config: GCConfig,
        is_archive: bool,
    ) -> Self {
        GCActor {
            store: ChainStore::new(store, true, genesis.transaction_validity_period),
            runtime_adapter,
            gc_config,
            epoch_manager,
            shard_tracker,
            is_archive,
            no_gc: false,
        }
    }

    fn clear_data(&mut self) -> Result<(), near_chain::Error> {
        // A RPC node should do regular garbage collection.
        if !self.is_archive {
            return self.store.clear_data(
                &self.gc_config,
                self.runtime_adapter.clone(),
                self.epoch_manager.clone(),
                &self.shard_tracker,
            );
        }
        // The ReshardingV3 mapping for archival nodes (#12578) was built under the assumption that
        // once we start tracking a shard, we continue tracking all of its descendant shards.
        // If this ever changes and this assertion needs to be removed, please make sure to
        // properly handle the State Mapping.
        debug_assert!(self.shard_tracker.is_valid_for_archival());

        // An archival node with split storage should perform garbage collection
        // on the hot storage. In order to determine if split storage is enabled
        // *and* that the migration to split storage is finished we can check
        // the store kind. It's only set to hot after the migration is finished.
        let store = self.store.store();
        let kind = store.get_db_kind()?;
        if kind == Some(DbKind::Hot) {
            return self.store.clear_data(
                &self.gc_config,
                self.runtime_adapter.clone(),
                self.epoch_manager.clone(),
                &self.shard_tracker,
            );
        }

        // An archival node with legacy storage or in the midst of migration to split
        // storage should do the legacy clear_archive_data.
        self.store.clear_archive_data(self.gc_config.gc_blocks_limit, self.runtime_adapter.clone())
    }

    fn gc(&mut self) {
        if self.no_gc {
            tracing::warn!(target: "garbage collection", "GC is disabled");
            return;
        }
        if self.store.head().is_err() {
            tracing::warn!(target: "garbage collection", "State not initialized yet. Head doesn't exist.");
            return;
        }

        let timer = metrics::GC_TIME.start_timer();
        if let Err(e) = self.clear_data() {
            tracing::error!(target: "garbage collection", "Error in gc: {}", e);
            debug_assert!(false, "Error in GCActor");
        }
        timer.observe_duration();
    }

    fn gc_loop(&mut self, ctx: &mut dyn DelayedActionRunner<Self>) {
        self.gc();
        ctx.run_later("garbage collection", self.gc_config.gc_step_period, move |act, ctx| {
            act.gc_loop(ctx);
        });
    }
}

impl Actor for GCActor {
    fn start_actor(&mut self, ctx: &mut dyn DelayedActionRunner<Self>) {
        self.gc_loop(ctx);
    }
}

#[cfg(feature = "test_features")]
#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub enum NetworkAdversarialMessage {
    StopGC,
    ResumeGC,
}

#[cfg(feature = "test_features")]
impl Handler<NetworkAdversarialMessage> for GCActor {
    fn handle(&mut self, msg: NetworkAdversarialMessage) {
        match msg {
            NetworkAdversarialMessage::StopGC => {
                self.no_gc = true;
            }
            NetworkAdversarialMessage::ResumeGC => {
                self.no_gc = false;
            }
        }
    }
}
