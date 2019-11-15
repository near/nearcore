//! Readonly view of the chain and state of the database.
//! Useful for querying from RPC.

use std::sync::Arc;

use actix::{Actor, Context, Handler};

use near_chain::{Chain, ChainGenesis, RuntimeAdapter};
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, StateRoot};
use near_primitives::views::{
    BlockView, ChunkView, EpochValidatorInfo, FinalExecutionOutcomeView, QueryResponse,
};
use near_store::Store;

use crate::types::{Error, GetBlock, Query, TxStatus};
use crate::{GetChunk, GetValidatorInfo};

/// View client provides currently committed (to the storage) view of the current chain and state.
pub struct ViewClientActor {
    chain: Chain,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
}

impl ViewClientActor {
    pub fn new(
        store: Arc<Store>,
        chain_genesis: &ChainGenesis,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
    ) -> Result<Self, Error> {
        // TODO: should we create shared ChainStore that is passed to both Client and ViewClient?
        let chain = Chain::new(store, runtime_adapter.clone(), chain_genesis)?;
        Ok(ViewClientActor { chain, runtime_adapter })
    }
}

impl Actor for ViewClientActor {
    type Context = Context<Self>;
}

/// Handles runtime query.
impl Handler<Query> for ViewClientActor {
    type Result = Result<QueryResponse, String>;

    fn handle(&mut self, msg: Query, _: &mut Context<Self>) -> Self::Result {
        let header = self.chain.head_header().map_err(|err| err.to_string())?.clone();
        let path_parts: Vec<&str> = msg.path.split('/').collect();
        if path_parts.is_empty() {
            return Err("At least one query parameter is required".to_string());
        }
        let state_root = {
            if path_parts[0] == "validators" && path_parts.len() == 1 {
                // for querying validators we don't need state root
                StateRoot { hash: CryptoHash::default(), num_parts: 0 }
            } else {
                let account_id = AccountId::from(path_parts[1]);
                let shard_id = self.runtime_adapter.account_id_to_shard_id(&account_id);
                self.chain
                    .get_chunk_extra(&header.hash, shard_id)
                    .map_err(|_e| "Failed to fetch the chunk while executing request")?
                    .state_root
                    .clone()
            }
        };

        self.runtime_adapter
            .query(
                &state_root,
                header.inner.height,
                header.inner.timestamp,
                &header.hash,
                path_parts,
                &msg.data,
            )
            .map_err(|err| err.to_string())
    }
}

/// Handles retrieving block from the chain.
impl Handler<GetBlock> for ViewClientActor {
    type Result = Result<BlockView, String>;

    fn handle(&mut self, msg: GetBlock, _: &mut Context<Self>) -> Self::Result {
        match msg {
            GetBlock::Best => match self.chain.head() {
                Ok(head) => self.chain.get_block(&head.last_block_hash).map(Clone::clone),
                Err(err) => Err(err),
            },
            GetBlock::Height(height) => self.chain.get_block_by_height(height).map(Clone::clone),
            GetBlock::Hash(hash) => self.chain.get_block(&hash).map(Clone::clone),
        }
        .map(|block| block.into())
        .map_err(|err| err.to_string())
    }
}

impl Handler<GetChunk> for ViewClientActor {
    type Result = Result<ChunkView, String>;

    fn handle(&mut self, msg: GetChunk, _: &mut Self::Context) -> Self::Result {
        match msg {
            GetChunk::ChunkHash(chunk_hash) => self.chain.get_chunk(&chunk_hash).map(Clone::clone),
            GetChunk::BlockHash(block_hash, shard_id) => {
                self.chain.get_block(&block_hash).map(Clone::clone).and_then(|block| {
                    self.chain
                        .get_chunk(&block.chunks[shard_id as usize].chunk_hash())
                        .map(Clone::clone)
                })
            }
            GetChunk::BlockHeight(block_height, shard_id) => {
                self.chain.get_block_by_height(block_height).map(Clone::clone).and_then(|block| {
                    self.chain
                        .get_chunk(&block.chunks[shard_id as usize].chunk_hash())
                        .map(Clone::clone)
                })
            }
        }
        .map(|chunk| chunk.into())
        .map_err(|err| err.to_string())
    }
}

impl Handler<TxStatus> for ViewClientActor {
    type Result = Result<FinalExecutionOutcomeView, String>;

    fn handle(&mut self, msg: TxStatus, _: &mut Context<Self>) -> Self::Result {
        self.chain.get_final_transaction_result(&msg.tx_hash)
    }
}

impl Handler<GetValidatorInfo> for ViewClientActor {
    type Result = Result<EpochValidatorInfo, String>;

    fn handle(&mut self, msg: GetValidatorInfo, _: &mut Context<Self>) -> Self::Result {
        self.runtime_adapter.get_validator_info(&msg.last_block_hash).map_err(|e| e.to_string())
    }
}
