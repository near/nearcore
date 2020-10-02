//! Streamer watches the network and collects all the blocks and related chunks
//! into one struct and pushes in in to the given queue
use std::collections::HashMap;
use std::convert::TryFrom;
use std::time::Duration;

use actix::{Addr, MailboxError};
use futures::stream::StreamExt;
use rocksdb::DB;
use tokio::sync::mpsc;
use tokio::time;
use tracing::{debug, info};

use near_client;
pub use near_primitives::hash::CryptoHash;
pub use near_primitives::{types, views};
use node_runtime::config::tx_cost;

use crate::IndexerConfig;

const INTERVAL: Duration = Duration::from_millis(500);
const INDEXER: &str = "indexer";

pub type ExecutionOutcomesWithReceipts = HashMap<CryptoHash, IndexerExecutionOutcomeWithReceipt>;

/// Error occurs in case of failed data fetch
#[derive(Debug)]
pub enum FailedToFetchData {
    MailboxError(MailboxError),
    String(String),
}

impl From<MailboxError> for FailedToFetchData {
    fn from(actix_error: MailboxError) -> Self {
        FailedToFetchData::MailboxError(actix_error)
    }
}

/// Resulting struct represents block with chunks
#[derive(Debug)]
pub struct StreamerMessage {
    pub block: views::BlockView,
    pub chunks: Vec<IndexerChunkView>,
    pub receipt_execution_outcomes: ExecutionOutcomesWithReceipts,
    pub state_changes: views::StateChangesKindsView,
    /// Transaction where signer is receiver produces so called "local receipt"
    /// these receipts will never get to chunks' `receipts` field. Anyway they can
    /// contain actions that is necessary to handle in Indexers
    pub local_receipts: Vec<views::ReceiptView>,
}

#[derive(Debug)]
pub struct IndexerChunkView {
    pub author: types::AccountId,
    pub header: views::ChunkHeaderView,
    pub transactions: Vec<IndexerTransactionWithOutcome>,
    pub receipts: Vec<views::ReceiptView>,
}

#[derive(Clone, Debug)]
pub struct IndexerTransactionWithOutcome {
    pub transaction: views::SignedTransactionView,
    pub outcome: IndexerExecutionOutcomeWithReceipt,
}

#[derive(Clone, Debug)]
pub struct IndexerExecutionOutcomeWithReceipt {
    pub execution_outcome: views::ExecutionOutcomeWithIdView,
    pub receipt: Option<views::ReceiptView>,
}

async fn fetch_status(
    client: &Addr<near_client::ClientActor>,
) -> Result<near_primitives::views::StatusResponse, FailedToFetchData> {
    client
        .send(near_client::Status { is_health_check: false })
        .await?
        .map_err(|err| FailedToFetchData::String(err))
}

/// Fetches the status to retrieve `latest_block_height` to determine if we need to fetch
/// entire block or we already fetched this block.
async fn fetch_latest_block(
    client: &Addr<near_client::ViewClientActor>,
) -> Result<views::BlockView, FailedToFetchData> {
    client
        .send(near_client::GetBlock(types::BlockReference::Finality(types::Finality::Final)))
        .await?
        .map_err(|err| FailedToFetchData::String(err))
}

/// Fetches specific block by it's height
async fn fetch_block_by_height(
    client: &Addr<near_client::ViewClientActor>,
    height: u64,
) -> Result<views::BlockView, FailedToFetchData> {
    client
        .send(near_client::GetBlock(near_primitives::types::BlockReference::BlockId(
            near_primitives::types::BlockId::Height(height),
        )))
        .await?
        .map_err(|err| FailedToFetchData::String(err))
}

/// Fetches specific block by it's hash
async fn fetch_block_by_hash(
    client: &Addr<near_client::ViewClientActor>,
    hash: CryptoHash,
) -> Result<views::BlockView, FailedToFetchData> {
    client
        .send(near_client::GetBlock(near_primitives::types::BlockId::Hash(hash).into()))
        .await?
        .map_err(|err| FailedToFetchData::String(err))
}

async fn convert_transactions_sir_into_local_receipts(
    client: &Addr<near_client::ViewClientActor>,
    near_config: &neard::NearConfig,
    txs: Vec<&IndexerTransactionWithOutcome>,
    block: &views::BlockView,
) -> Result<Vec<views::ReceiptView>, FailedToFetchData> {
    let prev_block = fetch_block_by_hash(&client, block.header.prev_hash.clone()).await?;
    let prev_block_gas_price = prev_block.header.gas_price;

    let local_receipts: Vec<views::ReceiptView> = txs
        .into_iter()
        .map(|tx| {
            let cost = tx_cost(
                &near_config.genesis.config.runtime_config.transaction_costs,
                &near_primitives::transaction::Transaction {
                    signer_id: tx.transaction.signer_id.clone(),
                    public_key: tx.transaction.public_key.clone(),
                    nonce: tx.transaction.nonce,
                    receiver_id: tx.transaction.receiver_id.clone(),
                    block_hash: block.header.hash,
                    actions: tx
                        .transaction
                        .actions
                        .clone()
                        .into_iter()
                        .map(|action| {
                            near_primitives::transaction::Action::try_from(action).unwrap()
                        })
                        .collect(),
                },
                prev_block_gas_price,
                true,
                near_config.genesis.config.protocol_version,
            );
            views::ReceiptView {
                predecessor_id: tx.transaction.signer_id.clone(),
                receiver_id: tx.transaction.receiver_id.clone(),
                receipt_id: tx
                    .outcome
                    .execution_outcome
                    .outcome
                    .receipt_ids
                    .first()
                    .expect("The transaction ExecutionOutcome should have one receipt id in vec")
                    .clone(),
                receipt: views::ReceiptEnumView::Action {
                    signer_id: tx.transaction.signer_id.clone(),
                    signer_public_key: tx.transaction.public_key.clone(),
                    gas_price: cost
                        .expect("TransactionCost returned IntegerOverflowError")
                        .receipt_gas_price,
                    output_data_receivers: vec![],
                    input_data_ids: vec![],
                    actions: tx.transaction.actions.clone(),
                },
            }
        })
        .collect();

    Ok(local_receipts)
}

/// This function supposed to return the entire `StreamerMessage`.
/// It fetches the block and all related parts (chunks, outcomes, state changes etc.)
/// and returns everything together in one struct
async fn build_streamer_message(
    client: &Addr<near_client::ViewClientActor>,
    block: views::BlockView,
    near_config: &neard::NearConfig,
) -> Result<StreamerMessage, FailedToFetchData> {
    let chunks_to_fetch = block
        .chunks
        .iter()
        .filter_map(|c| {
            if c.height_included == block.header.height {
                Some(c.chunk_hash)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    let chunks = fetch_chunks(&client, chunks_to_fetch).await?;

    let mut local_receipts: Vec<views::ReceiptView> = vec![];
    let mut outcomes = fetch_outcomes(&client, block.header.hash).await?;
    let mut indexer_chunks: Vec<IndexerChunkView> = vec![];

    for chunk in chunks {
        let views::ChunkView { transactions, author, header, receipts } = chunk;

        let indexer_transactions = transactions
        .into_iter()
        .map(|transaction| {
            IndexerTransactionWithOutcome {
            outcome: outcomes.remove(&transaction.hash).expect("The transaction execution outcome should always present in the same block as the transaction itself"),
            transaction,
        }
        })
        .collect::<Vec<IndexerTransactionWithOutcome>>();

        local_receipts.extend(
            convert_transactions_sir_into_local_receipts(
                &client,
                near_config,
                indexer_transactions
                    .iter()
                    .filter(|tx| tx.transaction.signer_id == tx.transaction.receiver_id)
                    .collect::<Vec<&IndexerTransactionWithOutcome>>(),
                &block,
            )
            .await?,
        );

        indexer_chunks.push(IndexerChunkView {
            author,
            header,
            transactions: indexer_transactions,
            receipts,
        });
    }

    let state_changes = fetch_state_changes(&client, block.header.hash).await?;

    Ok(StreamerMessage {
        block,
        chunks: indexer_chunks,
        receipt_execution_outcomes: outcomes,
        state_changes,
        local_receipts,
    })
}

async fn fetch_state_changes(
    client: &Addr<near_client::ViewClientActor>,
    block_hash: CryptoHash,
) -> Result<views::StateChangesKindsView, FailedToFetchData> {
    client
        .send(near_client::GetStateChangesInBlock { block_hash })
        .await?
        .map_err(|err| FailedToFetchData::String(err))
}

/// Fetches single chunk (as `near_primitives::views::ChunkView`) by provided `near_client::GetChunk` enum
async fn fetch_single_chunk(
    client: &Addr<near_client::ViewClientActor>,
    get_chunk: near_client::GetChunk,
) -> Result<views::ChunkView, FailedToFetchData> {
    client.send(get_chunk).await?.map_err(|err| FailedToFetchData::String(err))
}

/// Fetch all ExecutionOutcomeWithId for current block
/// Returns a HashMap where the key is Receipt id or Transaction hash and the value is ExecutionOutcome wth id and proof
async fn fetch_outcomes(
    client: &Addr<near_client::ViewClientActor>,
    block_hash: CryptoHash,
) -> Result<ExecutionOutcomesWithReceipts, FailedToFetchData> {
    let outcomes = client
        .send(near_client::GetExecutionOutcomesForBlock { block_hash })
        .await?
        .map_err(|err| FailedToFetchData::String(err))?;

    let mut outcomes_with_receipts = ExecutionOutcomesWithReceipts::new();
    for outcome in outcomes {
        let receipt = match fetch_receipt_by_id(&client, outcome.id).await {
            Ok(res) => res,
            Err(_) => None,
        };
        outcomes_with_receipts.insert(
            outcome.id,
            IndexerExecutionOutcomeWithReceipt { execution_outcome: outcome, receipt },
        );
    }

    Ok(outcomes_with_receipts)
}

async fn fetch_receipt_by_id(
    client: &Addr<near_client::ViewClientActor>,
    receipt_id: CryptoHash,
) -> Result<Option<views::ReceiptView>, FailedToFetchData> {
    client
        .send(near_client::GetReceipt { receipt_id })
        .await?
        .map_err(|err| FailedToFetchData::String(err))
}

/// Fetches all the chunks by their hashes.
/// Includes transactions and receipts in custom struct (to provide more info).
/// Returns Chunks as a `Vec`
async fn fetch_chunks(
    client: &Addr<near_client::ViewClientActor>,
    chunk_hashes: Vec<CryptoHash>,
) -> Result<Vec<views::ChunkView>, FailedToFetchData> {
    let mut chunks: futures::stream::FuturesUnordered<_> = chunk_hashes
        .into_iter()
        .map(|chunk_hash| {
            fetch_single_chunk(&client, near_client::GetChunk::ChunkHash(chunk_hash.into()))
        })
        .collect();
    let mut response = Vec::<views::ChunkView>::with_capacity(chunks.len());

    while let Some(chunk) = chunks.next().await {
        response.push(chunk?);
    }

    Ok(response)
}

/// Function that starts Streamer's busy loop. Every half a seconds it fetches the status
/// compares to already fetched block height and in case it differs fetches new block of given height.
///
/// We have to pass `client: Addr<near_client::ClientActor>` and `view_client: Addr<near_client::ViewClientActor>`.
pub(crate) async fn start(
    view_client: Addr<near_client::ViewClientActor>,
    client: Addr<near_client::ClientActor>,
    near_config: neard::NearConfig,
    indexer_config: IndexerConfig,
    mut blocks_sink: mpsc::Sender<StreamerMessage>,
) {
    info!(target: INDEXER, "Starting Streamer...");
    let mut indexer_db_path = neard::get_store_path(&indexer_config.home_dir);
    indexer_db_path.push_str("/indexer");

    // TODO: implement proper error handling
    let db = DB::open_default(indexer_db_path).unwrap();
    let mut last_synced_block_height: Option<types::BlockHeight> = None;

    'main: loop {
        time::delay_for(INTERVAL).await;
        let status = fetch_status(&client).await;
        if let Ok(status) = status {
            if status.sync_info.syncing {
                continue;
            }
        }

        let block = if let Ok(block) = fetch_latest_block(&view_client).await {
            block
        } else {
            continue;
        };

        let latest_block_height = block.header.height;
        let start_syncing_block_height = if let Some(last_synced_block_height) =
            last_synced_block_height
        {
            last_synced_block_height + 1
        } else {
            match indexer_config.sync_mode {
                crate::SyncModeEnum::FromInterruption => {
                    match db.get(b"last_synced_block_height").unwrap() {
                        Some(value) => String::from_utf8(value).unwrap().parse::<u64>().unwrap(),
                        None => latest_block_height,
                    }
                }
                crate::SyncModeEnum::LatestSynced => latest_block_height,
                crate::SyncModeEnum::BlockHeight(height) => height,
            }
        };

        debug!(
            target: INDEXER,
            "Streaming is about to start from block #{} and the latest block is #{}",
            start_syncing_block_height,
            latest_block_height
        );
        for block_height in start_syncing_block_height..=latest_block_height {
            if let Ok(block) = fetch_block_by_height(&view_client, block_height).await {
                let response = build_streamer_message(&view_client, block, &near_config).await;

                match response {
                    Ok(streamer_message) => {
                        debug!(target: INDEXER, "{:#?}", &streamer_message);
                        if let Err(_) = blocks_sink.send(streamer_message).await {
                            info!(
                                        target: INDEXER,
                                        "Unable to send StreamerMessage to listener, listener doesn't listen. terminating..."
                                    );
                            break 'main;
                        }
                    }
                    Err(err) => {
                        debug!(
                            target: INDEXER,
                            "Missing data, skipping block #{}...", block_height
                        );
                        debug!(target: INDEXER, "{:#?}", err);
                    }
                }
            }
            db.put(b"last_synced_block_height", &block_height.to_string()).unwrap();
            last_synced_block_height = Some(block_height);
        }
    }
}
