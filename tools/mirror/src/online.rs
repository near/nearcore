use crate::{ChainError, SourceBlock, SourceChunk};
use anyhow::Context;
use async_trait::async_trait;
use near_async::ActorSystem;
use near_async::messaging::CanSendAsync;
use near_async::multithread::MultithreadRuntimeHandle;
use near_chain_configs::GenesisValidationMode;
use near_client::ViewClientActor;
use near_client_primitives::types::{
    GetBlock, GetBlockError, GetChunkError, GetExecutionOutcome, GetReceipt, GetShardChunk, Query,
};
use near_crypto::PublicKey;
use near_primitives::action::{
    Action, AddKeyAction, CreateAccountAction, DeleteAccountAction, DeleteKeyAction,
    FunctionCallAction, StakeAction, TransferAction,
};
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{
    ActionReceipt, ActionReceiptV2, DataReceipt, GlobalContractDistributionReceipt, Receipt,
    ReceiptEnum, ReceiptV1,
};
use near_primitives::sharding::ChunkHash;
use near_primitives::types::{
    AccountId, BlockHeight, BlockId, BlockReference, Finality, TransactionOrReceiptId,
};
use near_primitives::views::{
    AccessKeyPermissionView, ExecutionOutcomeWithIdView, QueryRequest, QueryResponseKind,
    ReceiptEnumView, ReceiptView,
};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

/// Attempts to convert a ReceiptView to a Receipt.
/// This is fundamentally limited because ActionView (within ReceiptView) doesn't contain 
/// the actual contract code for DeployContract actions, only the hash. This function will 
/// fail for receipts containing DeployContract, DeployGlobalContract, or 
/// DeployGlobalContractByAccountId actions.
fn try_receipt_view_to_receipt(receipt_view: ReceiptView) -> Result<Receipt, String> {
    Ok(Receipt::V1(ReceiptV1 {
        predecessor_id: receipt_view.predecessor_id,
        receiver_id: receipt_view.receiver_id,
        receipt_id: receipt_view.receipt_id,
        receipt: match receipt_view.receipt {
            ReceiptEnumView::Action {
                signer_id,
                signer_public_key,
                gas_price,
                output_data_receivers,
                input_data_ids,
                actions,
                is_promise_yield,
                refund_to,
            } => {
                let output_data_receivers: Vec<_> = output_data_receivers
                    .into_iter()
                    .map(|data_receiver_view| near_primitives::receipt::DataReceiver {
                        data_id: data_receiver_view.data_id,
                        receiver_id: data_receiver_view.receiver_id,
                    })
                    .collect();
                let input_data_ids: Vec<CryptoHash> =
                    input_data_ids.into_iter().map(Into::into).collect();

                // Convert actions, but fail if we encounter DeployContract
                let mut converted_actions = Vec::with_capacity(actions.len());
                for action in actions {
                    let converted = match action {
                        near_primitives::views::ActionView::CreateAccount => {
                            Action::CreateAccount(CreateAccountAction {})
                        }
                        near_primitives::views::ActionView::DeployContract { .. }
                        | near_primitives::views::ActionView::DeployGlobalContract { .. }
                        | near_primitives::views::ActionView::DeployGlobalContractByAccountId {
                            ..
                        } => {
                            return Err(format!(
                                "Cannot convert ReceiptView to Receipt for DeployContract actions. \
                                 The ActionView only contains the code hash, not the actual contract code."
                            ));
                        }
                        near_primitives::views::ActionView::FunctionCall {
                            method_name,
                            args,
                            gas,
                            deposit,
                        } => Action::FunctionCall(Box::new(FunctionCallAction {
                            method_name,
                            args: args.into(),
                            gas,
                            deposit,
                        })),
                        near_primitives::views::ActionView::Transfer { deposit } => {
                            Action::Transfer(TransferAction { deposit })
                        }
                        near_primitives::views::ActionView::Stake { stake, public_key } => {
                            Action::Stake(Box::new(StakeAction { stake, public_key }))
                        }
                        near_primitives::views::ActionView::AddKey {
                            public_key,
                            access_key,
                        } => Action::AddKey(Box::new(AddKeyAction {
                            public_key,
                            access_key: access_key.into(),
                        })),
                        near_primitives::views::ActionView::DeleteKey { public_key } => {
                            Action::DeleteKey(Box::new(DeleteKeyAction { public_key }))
                        }
                        near_primitives::views::ActionView::DeleteAccount { beneficiary_id } => {
                            Action::DeleteAccount(DeleteAccountAction { beneficiary_id })
                        }
                        action => {
                            return Err(format!(
                                "Cannot convert ReceiptView action {:?} to Action",
                                action
                            ));
                        }
                    };
                    converted_actions.push(converted);
                }

                if refund_to.is_some() {
                    let action_receipt = ActionReceiptV2 {
                        signer_id,
                        signer_public_key,
                        gas_price,
                        output_data_receivers,
                        input_data_ids,
                        actions: converted_actions,
                        refund_to,
                    };
                    if is_promise_yield {
                        ReceiptEnum::PromiseYieldV2(action_receipt)
                    } else {
                        ReceiptEnum::ActionV2(action_receipt)
                    }
                } else {
                    let action_receipt = ActionReceipt {
                        signer_id,
                        signer_public_key,
                        gas_price,
                        output_data_receivers,
                        input_data_ids,
                        actions: converted_actions,
                    };
                    if is_promise_yield {
                        ReceiptEnum::PromiseYield(action_receipt)
                    } else {
                        ReceiptEnum::Action(action_receipt)
                    }
                }
            }
            ReceiptEnumView::Data { data_id, data, is_promise_resume } => {
                let data_receipt = DataReceipt { data_id, data };

                if is_promise_resume {
                    ReceiptEnum::PromiseResume(data_receipt)
                } else {
                    ReceiptEnum::Data(data_receipt)
                }
            }
            ReceiptEnumView::GlobalContractDistribution {
                id,
                target_shard,
                already_delivered_shards,
                code,
            } => ReceiptEnum::GlobalContractDistribution(
                GlobalContractDistributionReceipt::new(
                    id,
                    target_shard,
                    already_delivered_shards,
                    code.into(),
                ),
            ),
        },
        priority: receipt_view.priority,
    }))
}

pub(crate) struct ChainAccess {
    view_client: MultithreadRuntimeHandle<ViewClientActor>,
}

impl ChainAccess {
    pub(crate) async fn new<P: AsRef<Path>>(home: P) -> anyhow::Result<Self> {
        let config =
            nearcore::config::load_config(home.as_ref(), GenesisValidationMode::UnsafeFast)
                .with_context(|| format!("Error loading config from {:?}", home.as_ref()))?;

        let node = nearcore::start_with_config(home.as_ref(), config, ActorSystem::new())
            .await
            .context("failed to start NEAR node")?;
        Ok(Self { view_client: node.view_client })
    }
}

#[async_trait(?Send)]
impl crate::ChainAccess for ChainAccess {
    async fn init(
        &self,
        last_height: BlockHeight,
        num_initial_blocks: usize,
    ) -> anyhow::Result<Vec<BlockHeight>> {
        // first wait until HEAD moves. We don't really need it to be fully synced.
        let mut first_height = None;
        loop {
            match self.head_height().await {
                Ok(head) => match first_height {
                    Some(h) => {
                        if h != head {
                            break;
                        }
                    }
                    None => {
                        first_height = Some(head);
                    }
                },
                Err(ChainError::Unknown) => {}
                Err(ChainError::Other(e)) => return Err(e),
            };

            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        let mut block_heights = Vec::with_capacity(num_initial_blocks);
        let mut height = last_height;

        loop {
            // note that here we are using the fact that get_next_block_height() for this struct
            // allows passing a height that doesn't exist in the chain. This is not true for the offline
            // version
            match self.get_next_block_height(height).await {
                Ok(h) => {
                    block_heights.push(h);
                    height = h;
                    if block_heights.len() >= num_initial_blocks {
                        return Ok(block_heights);
                    }
                }
                Err(ChainError::Unknown) => {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(ChainError::Other(e)) => {
                    return Err(e)
                        .with_context(|| format!("failed fetching next block after #{}", height));
                }
            }
        }
    }

    async fn block_height_to_hash(&self, height: BlockHeight) -> Result<CryptoHash, ChainError> {
        Ok(self
            .view_client
            .send_async(GetBlock(BlockReference::BlockId(BlockId::Height(height))))
            .await
            .unwrap()?
            .header
            .hash)
    }

    async fn head_height(&self) -> Result<BlockHeight, ChainError> {
        Ok(self
            .view_client
            .send_async(GetBlock(BlockReference::Finality(Finality::Final)))
            .await
            .unwrap()?
            .header
            .height)
    }

    async fn get_txs(&self, height: BlockHeight) -> Result<SourceBlock, ChainError> {
        let block = self
            .view_client
            .send_async(GetBlock(BlockReference::BlockId(BlockId::Height(height))))
            .await
            .unwrap()?;
        let mut chunks = Vec::new();
        for c in block.chunks {
            let chunk = match self
                .view_client
                .send_async(GetShardChunk::ChunkHash(ChunkHash(c.chunk_hash)))
                .await
                .unwrap()
            {
                Ok(c) => c,
                Err(e) => match e {
                    GetChunkError::UnknownChunk { .. } => {
                        tracing::error!(
                            %c.shard_id,
                            ?c.chunk_hash,
                            %height,
                            "can't fetch source chain shard chunk at height, are we tracking all shards?"
                        );
                        continue;
                    }
                    _ => return Err(e.into()),
                },
            };
            if chunk.height_included() == height {
                chunks.push(SourceChunk {
                    shard_id: chunk.shard_id(),
                    transactions: chunk.to_transactions().to_vec(),
                    receipts: chunk.prev_outgoing_receipts().to_vec(),
                })
            }
        }

        Ok(SourceBlock { hash: block.header.hash, chunks })
    }

    async fn get_next_block_height(
        &self,
        mut height: BlockHeight,
    ) -> Result<BlockHeight, ChainError> {
        let head = self.head_height().await?;

        if height >= head {
            // let's only return finalized heights
            Err(ChainError::Unknown)
        } else if height + 1 == head {
            Ok(head)
        } else {
            loop {
                height += 1;
                if height >= head {
                    break Err(ChainError::Unknown);
                }
                match self
                    .view_client
                    .send_async(GetBlock(BlockReference::BlockId(BlockId::Height(height))))
                    .await
                    .unwrap()
                {
                    Ok(b) => break Ok(b.header.height),
                    Err(GetBlockError::UnknownBlock { .. }) => {}
                    Err(e) => break Err(ChainError::other(e)),
                }
            }
        }
    }

    async fn get_outcome(
        &self,
        id: TransactionOrReceiptId,
    ) -> Result<ExecutionOutcomeWithIdView, ChainError> {
        Ok(self.view_client.send_async(GetExecutionOutcome { id }).await.unwrap()?.outcome_proof)
    }

    async fn get_receipt(&self, id: &CryptoHash) -> Result<Arc<Receipt>, ChainError> {
        let receipt_view = self
            .view_client
            .send_async(GetReceipt { receipt_id: *id })
            .await
            .unwrap()?
            .ok_or(ChainError::Unknown)?;

        try_receipt_view_to_receipt(receipt_view)
            .map(Arc::new)
            .map_err(|e| ChainError::Other(anyhow::anyhow!(e)))
    }

    async fn get_full_access_keys(
        &self,
        account_id: &AccountId,
        block_hash: &CryptoHash,
    ) -> Result<Vec<PublicKey>, ChainError> {
        let mut ret = Vec::new();
        match self
            .view_client
            .send_async(Query {
                block_reference: BlockReference::BlockId(BlockId::Hash(*block_hash)),
                request: QueryRequest::ViewAccessKeyList { account_id: account_id.clone() },
            })
            .await
            .unwrap()?
            .kind
        {
            QueryResponseKind::AccessKeyList(l) => {
                for k in l.keys {
                    if k.access_key.permission == AccessKeyPermissionView::FullAccess {
                        ret.push(k.public_key);
                    }
                }
            }
            _ => unreachable!(),
        };
        Ok(ret)
    }
}
