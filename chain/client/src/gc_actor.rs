use crate::metrics;
use actix::{Actor, Addr, Arbiter, AsyncContext, Context};
use log::warn;
use near_chain::{ChainStore, RuntimeAdapter};
use near_chain_configs::ClientConfig;
use near_primitives::types::{BlockHeight, NumBlocks};
use near_store::Store;
use std::sync::Arc;
use std::time::Duration;

pub struct GCActor {
    store: ChainStore,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    /// How frequently we run the actor
    gc_step_period: Duration,
    /// Number of blocks we gc at each step,
    gc_step_limit: NumBlocks,
}

impl GCActor {
    pub fn new(
        store: Arc<Store>,
        genesis_height: BlockHeight,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        gc_step_period: Duration,
        gc_step_limit: NumBlocks,
    ) -> Self {
        GCActor {
            store: ChainStore::new(store, genesis_height),
            runtime_adapter,
            gc_step_period,
            gc_step_limit,
        }
    }

    fn gc(&mut self, ctx: &mut Context<GCActor>) {
        let timer = near_metrics::start_timer(&metrics::GC_TIME);
        if let Err(e) = self.store.clear_data(self.runtime_adapter.as_ref(), self.gc_step_limit) {
            warn!(target: "client", "Error in gc: {}", e);
        }
        near_metrics::stop_timer(timer);
        ctx.run_later(self.gc_step_period, move |act, ctx| {
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
    store: Arc<Store>,
    genesis_height: BlockHeight,
    client_config: ClientConfig,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
) -> (Addr<GCActor>, Arbiter) {
    let gc_arbiter = Arbiter::new();
    let gc_addr = GCActor::start_in_arbiter(&gc_arbiter, move |_ctx| {
        GCActor::new(
            store,
            genesis_height,
            runtime_adapter,
            client_config.gc_step_period,
            client_config.gc_blocks_limit,
        )
    });
    (gc_addr, gc_arbiter)
}
