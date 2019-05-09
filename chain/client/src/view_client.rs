//! Readonly view of the chain and state of the database.
//! Useful for querying from RPC.

use std::sync::Arc;

use actix::{Actor, Context, Handler};
use chrono::{DateTime, Utc};

use near_chain::{Chain, RuntimeAdapter, Block};
use near_primitives::rpc::ABCIQueryResponse;
use near_store::Store;

use crate::{Error, Query, GetBlock};

/// View client provides currently committed (to the storage) view of the current chain and state.
pub struct ViewClientActor {
    chain: Chain,
    runtime_adapter: Arc<RuntimeAdapter>,
}

impl ViewClientActor {
    pub fn new(
        store: Arc<Store>,
        genesis_time: DateTime<Utc>,
        runtime_adapter: Arc<RuntimeAdapter>,
    ) -> Result<Self, Error> {
        let chain = Chain::new(store, runtime_adapter.clone(), genesis_time)?;
        Ok(ViewClientActor { chain, runtime_adapter })
    }
}

impl Actor for ViewClientActor {
    type Context = Context<Self>;
}

/// Handles runtime query.
impl Handler<Query> for ViewClientActor {
    type Result = Result<ABCIQueryResponse, String>;

    fn handle(&mut self, msg: Query, _: &mut Context<Self>) -> Self::Result {
        let head = self.chain.head().map_err(|err| err.to_string())?;
        let state_root =
            self.chain.get_post_state_root(&head.last_block_hash).map_err(|err| err.to_string())?;
        self.runtime_adapter.query(*state_root, head.height, &msg.path, &msg.data)
    }
}

/// Handles retrieving block from the chain.
impl Handler<GetBlock> for ViewClientActor {
    type Result = Option<Block>;

    fn handle(&mut self, msg: GetBlock, _: &mut Context<Self>) -> Self::Result {
        match msg {
            GetBlock::Best => match self.chain.head() {
                Ok(head) => self.chain.get_block(&head.last_block_hash).map(Clone::clone).ok(),
                _ => None,
            },
            GetBlock::Height(height) => {
                self.chain.get_block_by_height(height).map(Clone::clone).ok()
            }
            GetBlock::Hash(hash) => self.chain.get_block(&hash).map(Clone::clone).ok(),
        }
    }
}
