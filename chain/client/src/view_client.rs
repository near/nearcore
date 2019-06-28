//! Readonly view of the chain and state of the database.
//! Useful for querying from RPC.

use std::sync::Arc;

use actix::{Actor, Context, Handler};
use chrono::{DateTime, Utc};

use near_chain::{Block, Chain, ErrorKind, RuntimeAdapter};
use near_primitives::hash::CryptoHash;
use near_primitives::rpc::QueryResponse;
use near_primitives::transaction::{
    FinalTransactionResult, FinalTransactionStatus, TransactionLogs, TransactionResult,
    TransactionStatus,
};
use near_primitives::types::AccountId;
use near_store::Store;

use crate::types::{Error, GetBlock, Query, TxStatus};
use crate::TxDetails;

/// View client provides currently committed (to the storage) view of the current chain and state.
pub struct ViewClientActor {
    chain: Chain,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
}

impl ViewClientActor {
    pub fn new(
        store: Arc<Store>,
        genesis_time: DateTime<Utc>,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
    ) -> Result<Self, Error> {
        // TODO: should we create shared ChainStore that is passed to both Client and ViewClient?
        let chain = Chain::new(store, runtime_adapter.clone(), genesis_time)?;
        Ok(ViewClientActor { chain, runtime_adapter })
    }

    pub fn get_transaction_result(
        &mut self,
        hash: &CryptoHash,
    ) -> Result<TransactionResult, String> {
        match self.chain.get_transaction_result(hash) {
            Ok(result) => Ok(result.clone()),
            Err(err) => match err.kind() {
                ErrorKind::DBNotFoundErr(_) => Ok(TransactionResult::default()),
                _ => Err(err.to_string()),
            },
        }
    }

    fn collect_transaction_final_result(
        &mut self,
        transaction_result: &TransactionResult,
        logs: &mut Vec<TransactionLogs>,
    ) -> Result<FinalTransactionStatus, String> {
        match transaction_result.status {
            TransactionStatus::Unknown => Ok(FinalTransactionStatus::Unknown),
            TransactionStatus::Failed => Ok(FinalTransactionStatus::Failed),
            TransactionStatus::Completed => {
                for r in transaction_result.receipts.iter() {
                    let receipt_result = self.get_transaction_result(&r)?;
                    logs.push(TransactionLogs {
                        hash: *r,
                        lines: receipt_result.logs.clone(),
                        receipts: receipt_result.receipts.clone(),
                        result: receipt_result.result.clone(),
                    });
                    match self.collect_transaction_final_result(&receipt_result, logs)? {
                        FinalTransactionStatus::Failed => {
                            return Ok(FinalTransactionStatus::Failed)
                        }
                        FinalTransactionStatus::Completed => {}
                        _ => return Ok(FinalTransactionStatus::Started),
                    };
                }
                Ok(FinalTransactionStatus::Completed)
            }
        }
    }
}

impl Actor for ViewClientActor {
    type Context = Context<Self>;
}

/// Handles runtime query.
impl Handler<Query> for ViewClientActor {
    type Result = Result<QueryResponse, String>;

    fn handle(&mut self, msg: Query, _: &mut Context<Self>) -> Self::Result {
        let head = self.chain.head().map_err(|err| err.to_string())?;
        let path_parts: Vec<&str> = msg.path.split('/').collect();
        let account_id = AccountId::from(path_parts[1]);
        let shard_id = self.runtime_adapter.account_id_to_shard_id(&account_id);
        let head_block = self
            .chain
            .get_block(&head.last_block_hash)
            .map_err(|_e| "Failed to fetch head block while executing request")?;
        let chunk_hash = head_block.chunks[shard_id as usize].chunk_hash().clone();
        let state_root = self
            .chain
            .get_post_state_root(&chunk_hash)
            .map_err(|_e| "Failed to fetch the chunk while executing request")?;

        self.runtime_adapter
            .query(*state_root, head.height, path_parts, &msg.data)
            .map_err(|err| err.to_string())
    }
}

/// Handles retrieving block from the chain.
impl Handler<GetBlock> for ViewClientActor {
    type Result = Result<Block, String>;

    fn handle(&mut self, msg: GetBlock, _: &mut Context<Self>) -> Self::Result {
        match msg {
            GetBlock::Best => match self.chain.head() {
                Ok(head) => self.chain.get_block(&head.last_block_hash).map(Clone::clone),
                Err(err) => Err(err),
            },
            GetBlock::Height(height) => self.chain.get_block_by_height(height).map(Clone::clone),
            GetBlock::Hash(hash) => self.chain.get_block(&hash).map(Clone::clone),
        }
        .map_err(|err| err.to_string())
    }
}

impl Handler<TxStatus> for ViewClientActor {
    type Result = Result<FinalTransactionResult, String>;

    fn handle(&mut self, msg: TxStatus, _: &mut Context<Self>) -> Self::Result {
        let transaction_result = self.get_transaction_result(&msg.tx_hash)?;
        let mut result = FinalTransactionResult {
            status: FinalTransactionStatus::Unknown,
            logs: vec![TransactionLogs {
                hash: msg.tx_hash,
                lines: transaction_result.logs.clone(),
                receipts: transaction_result.receipts.clone(),
                result: transaction_result.result.clone(),
            }],
        };
        result.status =
            self.collect_transaction_final_result(&transaction_result, &mut result.logs)?;
        Ok(result)
    }
}

impl Handler<TxDetails> for ViewClientActor {
    type Result = Result<TransactionResult, String>;

    fn handle(&mut self, msg: TxDetails, _: &mut Context<Self>) -> Self::Result {
        self.get_transaction_result(&msg.tx_hash)
    }
}
