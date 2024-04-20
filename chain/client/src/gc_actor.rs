use crate::metrics;
#[cfg(feature = "test_features")]
use actix::Handler;
use actix::{Actor, Addr, Arbiter, ArbiterHandle, AsyncContext, Context};
use near_chain::{types::RuntimeAdapter, ChainStore, ChainStoreAccess};
use near_chain_configs::{ClientConfig, GCConfig};
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
    ) -> Self {
        GCActor {
            store: ChainStore::new(store, genesis_height, true),
            runtime_adapter,
            gc_config,
            epoch_manager,
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
            );
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

    fn gc(&mut self, ctx: &mut Context<GCActor>) {
        if !self.no_gc {
            let timer = metrics::GC_TIME.start_timer();
            if let Err(e) = self.clear_data() {
                warn!(target: "garbage collection", "Error in gc: {}", e);
            }
            timer.observe_duration();
        }

        ctx.run_later(self.gc_config.gc_step_period, move |act, ctx| {
            act.gc(ctx);
        });
    }
}

impl Actor for GCActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
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
    type Result = ();

    fn handle(&mut self, msg: NetworkAdversarialMessage, _ctx: &mut Self::Context) -> Self::Result {
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

pub fn start_gc_actor(
    store: Store,
    genesis_height: BlockHeight,
    client_config: ClientConfig,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
) -> (Addr<GCActor>, ArbiterHandle) {
    let gc_arbiter = Arbiter::new().handle();
    let gc_addr = GCActor::start_in_arbiter(&gc_arbiter, move |_ctx| {
        GCActor::new(
            store,
            genesis_height,
            runtime_adapter,
            epoch_manager,
            client_config.gc,
            client_config.archive,
        )
    });
    (gc_addr, gc_arbiter)
}
