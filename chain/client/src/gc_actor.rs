use crate::metrics;
use near_async::futures::{DelayedActionRunner, DelayedActionRunnerExt};
use near_async::messaging::Actor;
#[cfg(feature = "test_features")]
use near_async::messaging::Handler;
use near_chain::{types::RuntimeAdapter, ChainStore, ChainStoreAccess};
use near_chain_configs::GCConfig;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::types::BlockHeight;
use near_store::{metadata::DbKind, Store};
use std::sync::Arc;
use tracing::warn;

/// An actor for garbage collection that runs in its own thread
/// The actor runs periodically, as determined by `gc_step_period`,
/// to garbage collect blockchain data
pub struct GCActor {
    store: ChainStore,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    gc_config: GCConfig,
    is_archive: bool,
    has_cold_store: bool,
    /// In some tests we may want to temporarily disable GC
    no_gc: bool,
}

impl GCActor {
    pub fn new(
        store: Store,
        genesis_height: BlockHeight,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        gc_config: GCConfig,
        is_archive: bool,
        has_cold_store: bool,
    ) -> Self {
        GCActor {
            store: ChainStore::new(store, genesis_height, true),
            runtime_adapter,
            gc_config,
            epoch_manager,
            is_archive,
            has_cold_store,
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
            );
        }

        // If the node was started from genesis with no existing database and
        // archive=true in the config, but no cold store was configured, then
        // the node will only have a hot store, and will not create a cold
        // store until that is configured explicitly. In this case we don't want
        // to garbage collect anything
        if !self.has_cold_store {
            return Ok(());
        }

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
            );
        }

        // An archival node with legacy storage or in the midst of migration to split
        // storage should do the legacy clear_archive_data.
        self.store.clear_archive_data(self.gc_config.gc_blocks_limit, self.runtime_adapter.clone())
    }

    fn gc(&mut self, ctx: &mut dyn DelayedActionRunner<Self>) {
        if !self.no_gc {
            let timer = metrics::GC_TIME.start_timer();
            if let Err(e) = self.clear_data() {
                warn!(target: "garbage collection", "Error in gc: {}", e);
            }
            timer.observe_duration();
        }

        ctx.run_later("garbage collection", self.gc_config.gc_step_period, move |act, ctx| {
            act.gc(ctx);
        });
    }
}

impl Actor for GCActor {
    fn start_actor(&mut self, ctx: &mut dyn DelayedActionRunner<Self>) {
        self.gc(ctx);
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
