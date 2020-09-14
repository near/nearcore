//! Streamer watches the network and collects all the blocks and related chunks
//! into one struct and pushes in in to the given queue
use std::collections::HashMap;
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

use crate::IndexerConfig;

const INTERVAL: Duration = Duration::from_millis(500);
const INDEXER: &str = "indexer";

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
    pub outcomes: HashMap<CryptoHash, views::ExecutionOutcomeWithIdView>,
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
    pub outcome: views::ExecutionOutcomeWithIdView,
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

/// This function supposed to return the entire `StreamerMessage`.
/// It fetches the block and all related parts (chunks, outcomes, state changes etc.)
/// and returns everything together in one struct
async fn build_streamer_message(
    client: &Addr<near_client::ViewClientActor>,
    block: views::BlockView,
) -> Result<StreamerMessage, FailedToFetchData> {
    let chunks = fetch_chunks(&client, &block.chunks).await?;

    let mut local_receipts: Vec<views::ReceiptView> = vec![];
    let mut outcomes = fetch_outcomes(&client, block.header.hash).await?;
    let mut indexer_chunks: Vec<IndexerChunkView> = vec![];

    for chunk in &chunks {
        let indexer_transactions = chunk
            .transactions
            .iter()
            .map(|transaction| {
                if transaction.signer_id == transaction.receiver_id {
                    // This transaction generates local receipt (sir)
                    local_receipts.push(views::ReceiptView {
                        predecessor_id: transaction.signer_id.clone(),
                        receiver_id: transaction.receiver_id.clone(),
                        receipt_id: outcomes
                            .get(&transaction.hash)
                            .unwrap()
                            .outcome
                            .receipt_ids
                            .first()
                            .unwrap()
                            .clone(),
                        receipt: views::ReceiptEnumView::Action {
                            signer_id: transaction.signer_id.clone(),
                            signer_public_key: transaction.public_key.clone(),
                            gas_price: block.header.gas_price, // TODO: fill it
                            output_data_receivers: vec![],
                            input_data_ids: vec![],
                            actions: transaction.actions.clone(),
                        },
                    })
                }
                IndexerTransactionWithOutcome {
                    transaction: transaction.clone(),
                    outcome: outcomes.remove_entry(&transaction.hash).unwrap().1,
                }
            })
            .collect::<Vec<IndexerTransactionWithOutcome>>();
        indexer_chunks.push(IndexerChunkView {
            author: chunk.author.clone(),
            header: chunk.header.clone(),
            transactions: indexer_transactions,
            receipts: chunk.receipts.clone(),
        });
    }

    let state_changes = fetch_state_changes(&client, block.header.hash).await?;

    Ok(StreamerMessage { block, chunks: indexer_chunks, outcomes, state_changes, local_receipts })
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

/// Fetch ExecutionOutcomeWithId for receipts and transactions from previous block
/// Returns fetched Outcomes and Vec of failed to fetch outcome which should be retried to fetch
/// in the next block
async fn fetch_outcomes(
    client: &Addr<near_client::ViewClientActor>,
    block_hash: CryptoHash,
) -> Result<HashMap<CryptoHash, views::ExecutionOutcomeWithIdView>, FailedToFetchData> {
    let outcomes = client
        .send(near_client::GetExecutionOutcomesForBlock { block_hash })
        .await?
        .map_err(|err| FailedToFetchData::String(err))?;

    Ok(outcomes
        .into_iter()
        .map(|outcome| (outcome.id, outcome))
        .collect::<HashMap<CryptoHash, views::ExecutionOutcomeWithIdView>>())
}

/// Fetches all the chunks by their hashes.
/// Includes transactions and receipts in custom struct (to provide more info).
/// Returns Chunks as a `Vec`
async fn fetch_chunks(
    client: &Addr<near_client::ViewClientActor>,
    chunks: &[views::ChunkHeaderView],
) -> Result<Vec<views::ChunkView>, FailedToFetchData> {
    let mut chunks: futures::stream::FuturesUnordered<_> = chunks
        .iter()
        .map(|chunk| {
            fetch_single_chunk(&client, near_client::GetChunk::ChunkHash(chunk.chunk_hash.into()))
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
                let response = build_streamer_message(&view_client, block).await;

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
