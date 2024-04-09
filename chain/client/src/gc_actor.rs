use crate::metrics;
use actix::{Actor, Addr, Arbiter, ArbiterHandle, AsyncContext, Context};
use near_chain::{types::RuntimeAdapter, ChainStore, ChainStoreAccess};
use near_chain_configs::{ClientConfig, GCConfig};
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::types::BlockHeight;
use near_store::{metadata::DbKind, Store};
use std::sync::Arc;
use tracing::warn;

pub struct GCActor {
    store: ChainStore,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    gc_config: GCConfig,
    is_archive: bool,
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
        }
    }

    fn gc(&mut self, ctx: &mut Context<GCActor>) {
        let timer = metrics::GC_TIME.start_timer();
        if !self.is_archive {
            if let Err(e) = self.store.clear_data(
                &self.gc_config,
                self.runtime_adapter.clone(),
                self.epoch_manager.clone(),
            ) {
                warn!(target: "client", "Error in gc: {}", e);
            }
        } else {
            let kind = self.store.store().get_db_kind();
            match kind {
                Ok(Some(DbKind::Hot)) => {
                    if let Err(e) = self.store.clear_data(
                        &self.gc_config,
                        self.runtime_adapter.clone(),
                        self.epoch_manager.clone(),
                    ) {
                        warn!(target: "client", "Error in gc: {}", e);
                    }
                }
                Err(e) => {
                    warn!(target: "client", "Error in gc: {}", e);
                }
                _ => {}
            }
        }

        timer.observe_duration();
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
