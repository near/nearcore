//! Streamer watches the network and collects all the blocks and related chunks
//! into one struct and pushes in in to the given queue
use std::time::Duration;

use actix::{Addr, MailboxError};
use futures::stream::StreamExt;
use near_primitives::borsh;
use near_primitives::borsh::de::BorshDeserialize;
use near_primitives::borsh::ser::BorshSerialize;
use rocksdb::{WriteBatch, DB};
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
    pub chunks: Vec<views::ChunkView>,
    pub outcomes: Vec<Outcome>,
    pub state_changes: views::StateChangesKindsView,
    pub local_receipts: Vec<views::ReceiptView>,
}

#[derive(Clone, Debug)]
pub enum Outcome {
    Receipt(views::ExecutionOutcomeWithIdView),
    Transaction(views::ExecutionOutcomeWithIdView),
}

#[derive(borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Debug)]
pub struct IndexerReceiptMetaData {
    pub parent_receipt_id: CryptoHash,
    pub transaction_hash: CryptoHash,
    pub receipt_id: CryptoHash,
    pub receiver_id: types::AccountId,
}

#[derive(borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Debug)]
pub enum TransactionOrReceiptId {
    Transaction { transaction_hash: CryptoHash, sender_id: types::AccountId },
    Receipt { receipt_id: CryptoHash, receiver_id: types::AccountId },
}

impl TransactionOrReceiptId {
    fn id(&self) -> &CryptoHash {
        match self {
            TransactionOrReceiptId::Transaction { transaction_hash, sender_id: _ } => {
                transaction_hash
            }
            TransactionOrReceiptId::Receipt { receipt_id, receiver_id: _ } => receipt_id,
        }
    }
}

impl From<types::TransactionOrReceiptId> for TransactionOrReceiptId {
    fn from(near_enum: types::TransactionOrReceiptId) -> Self {
        match near_enum {
            types::TransactionOrReceiptId::Receipt { receipt_id, receiver_id } => {
                Self::Receipt { receipt_id, receiver_id }
            }
            types::TransactionOrReceiptId::Transaction { transaction_hash, sender_id } => {
                Self::Transaction { transaction_hash, sender_id }
            }
        }
    }
}

impl From<TransactionOrReceiptId> for types::TransactionOrReceiptId {
    fn from(indexer_enum: TransactionOrReceiptId) -> Self {
        match indexer_enum {
            TransactionOrReceiptId::Receipt { receipt_id, receiver_id } => {
                Self::Receipt { receipt_id, receiver_id }
            }
            TransactionOrReceiptId::Transaction { transaction_hash, sender_id } => {
                Self::Transaction { transaction_hash, sender_id }
            }
        }
    }
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
    db: &DB,
) -> Result<StreamerMessage, FailedToFetchData> {
    let chunks = fetch_chunks(&client, &block.chunks).await?;

    let mut outcomes_to_fetch: Vec<TransactionOrReceiptId> = vec![];
    let mut local_receipts: Vec<views::ReceiptView> = vec![];
    let mut outcomes: Vec<Outcome> = vec![];

    for chunk in &chunks {
        for transaction in &chunk.transactions {
            match client
                .send(near_client::GetExecutionOutcome {
                    id: types::TransactionOrReceiptId::Transaction {
                        transaction_hash: transaction.hash.clone(),
                        sender_id: transaction.signer_id.to_string(),
                    },
                })
                .await
                .map_err(|e| eprintln!("{:?}", e))
            {
                Ok(Ok(outcome)) => {
                    if transaction.signer_id == transaction.receiver_id {
                        // This transaction generates local receipt (sir)
                        local_receipts.push(views::ReceiptView {
                            predecessor_id: transaction.signer_id.clone(),
                            receiver_id: transaction.receiver_id.clone(),
                            receipt_id: outcome
                                .outcome_proof
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
                    outcomes.push(Outcome::Transaction(outcome.outcome_proof));
                }
                _ => {}
            }
        }

        outcomes_to_fetch.extend(chunk.receipts.iter().map(|receipt| {
            TransactionOrReceiptId::Receipt {
                receipt_id: receipt.receipt_id,
                receiver_id: receipt.receiver_id.to_string(),
            }
        }));
    }

    // Add local receipts to the start of the outcomes to fetch
    outcomes_to_fetch.extend(&mut local_receipts.iter().map(|receipt| {
        TransactionOrReceiptId::Receipt {
            receipt_id: receipt.receipt_id,
            receiver_id: receipt.receiver_id.to_string(),
        }
    }));

    put_outcomes_to_fetch(outcomes_to_fetch, &db).await;

    outcomes.extend(fetch_outcomes(&client, &db).await);

    let state_changes = fetch_state_changes(&client, block.header.hash).await?;

    Ok(StreamerMessage { block, chunks, outcomes, state_changes, local_receipts })
}

async fn put_outcomes_to_fetch(transaction_or_receipt_ids: Vec<TransactionOrReceiptId>, db: &DB) {
    let mut batch = WriteBatch::default();
    for transaction_or_receipt_id in transaction_or_receipt_ids {
        // Generate key with prefix "outcome_to_get_" and CryptoHash tail of particular Transaction or Receipt
        let key = format!("outcome_to_get_{}", transaction_or_receipt_id.id().to_string());
        batch.put(key.into_bytes(), transaction_or_receipt_id.try_to_vec().unwrap())
    }
    db.write(batch);
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
async fn fetch_outcomes(client: &Addr<near_client::ViewClientActor>, db: &DB) -> Vec<Outcome> {
    let mut outcomes: Vec<Outcome> = vec![];
    for (db_key, v) in db.prefix_iterator("outcome_to_get_") {
        let transaction_or_receipt_id = TransactionOrReceiptId::try_from_slice(&v).unwrap();
        match client
            .send(near_client::GetExecutionOutcome { id: transaction_or_receipt_id.clone().into() })
            .await
            .map_err(|err| FailedToFetchData::MailboxError(err))
        {
            Ok(Ok(outcome)) => {
                match &transaction_or_receipt_id {
                    TransactionOrReceiptId::Transaction { .. } => {
                        outcomes.push(Outcome::Transaction(outcome.outcome_proof));
                    }
                    TransactionOrReceiptId::Receipt { .. } => {
                        outcomes.push(Outcome::Receipt(outcome.outcome_proof));
                    }
                };
                db.delete(db_key);
            }
            _ => {}
        }
    }
    outcomes
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
    // let mut outcomes_to_get = Vec::<types::TransactionOrReceiptId>::new();
    let mut last_synced_block_height: Option<types::BlockHeight> = None;

    'main: loop {
        time::delay_for(INTERVAL).await;
        let status = fetch_status(&client).await;
        // if let Ok(status) = status {
        //     if status.sync_info.syncing {
        //         continue;
        //     }
        // }

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
                let response = build_streamer_message(&view_client, block, &db).await;

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
