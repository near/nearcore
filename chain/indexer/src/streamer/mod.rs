use std::time::Duration;

use actix::Addr;
use async_recursion::async_recursion;
use near_indexer_primitives::types::ProtocolVersion;
use node_runtime::config::RuntimeConfig;
use rocksdb::DB;
use tokio::sync::mpsc;
use tokio::time;
use tracing::{debug, info};

use near_indexer_primitives::{
    IndexerChunkView, IndexerExecutionOutcomeWithOptionalReceipt,
    IndexerExecutionOutcomeWithReceipt, IndexerShard, IndexerTransactionWithOutcome,
    StreamerMessage,
};
use near_primitives::hash::CryptoHash;
use near_primitives::views;

use crate::{AwaitForNodeSyncedEnum, IndexerConfig};

use self::errors::FailedToFetchData;
use self::fetchers::{
    fetch_block, fetch_block_by_height, fetch_block_chunks, fetch_latest_block, fetch_outcomes,
    fetch_state_changes, fetch_status,
};
use self::utils::convert_transactions_sir_into_local_receipts;
use crate::streamer::fetchers::fetch_protocol_config;
use crate::INDEXER;

mod errors;
mod fetchers;
mod metrics;
mod utils;

const INTERVAL: Duration = Duration::from_millis(500);

/// Blocks #47317863 and #47317864 with restored receipts.
const PROBLEMATIC_BLOCKS: [CryptoHash; 2] = [
    CryptoHash(
        *b"\xcd\xde\x9a\x3f\x5d\xdf\xb4\x2c\xb9\x9b\xf4\x8c\x04\x95\x6f\x5b\
           \xa0\xb7\x29\xe2\xa5\x04\xf8\xbd\x9c\x86\x92\xd6\x16\x8c\xcf\x14",
    ),
    CryptoHash(
        *b"\x12\xa9\x5a\x1a\x3d\x14\xa7\x36\xb3\xce\xe6\xea\x07\x20\x8e\x75\
           \x4e\xb5\xc2\xd7\xf9\x11\xca\x29\x09\xe0\xb8\x85\xb5\x2b\x95\x6a",
    ),
];

/// Tests whether raw hashes in [`PROBLEMATIC_BLOCKS`] match expected
/// user-readable hashes.  Ideally we would compute the hashes at compile time
/// but there’s no const function for base58→bytes conversion so instead we’re
/// hard-coding the raw base in [`PROBLEMATIC_BLOCKS`] and have this test to
/// confirm the raw values are correct.
#[test]
fn test_problematic_blocks_hash() {
    let got: Vec<String> =
        PROBLEMATIC_BLOCKS.iter().map(std::string::ToString::to_string).collect();
    assert_eq!(
        vec![
            "ErdT2vLmiMjkRoSUfgowFYXvhGaLJZUWrgimHRkousrK",
            "2Fr7dVAZGoPYgpwj6dfASSde6Za34GNUJb4CkZ8NSQqw"
        ],
        got
    );
}

/// This function supposed to return the entire `StreamerMessage`.
/// It fetches the block and all related parts (chunks, outcomes, state changes etc.)
/// and returns everything together in one struct
#[async_recursion]
async fn build_streamer_message(
    client: &Addr<near_client::ViewClientActor>,
    block: views::BlockView,
) -> Result<StreamerMessage, FailedToFetchData> {
    let _timer = metrics::BUILD_STREAMER_MESSAGE_TIME.start_timer();
    let chunks = fetch_block_chunks(&client, &block).await?;

    let protocol_config_view = fetch_protocol_config(&client, block.header.hash).await?;
    let num_shards = protocol_config_view.num_block_producer_seats_per_shard.len()
        as near_primitives::types::NumShards;

    let runtime_config_store =
        near_primitives::runtime::config_store::RuntimeConfigStore::new(None);
    let runtime_config = runtime_config_store.get_config(protocol_config_view.protocol_version);

    let mut shards_outcomes = fetch_outcomes(&client, block.header.hash).await?;
    let mut state_changes = fetch_state_changes(
        &client,
        block.header.hash,
        near_primitives::types::EpochId(block.header.epoch_id),
    )
    .await?;
    let mut indexer_shards = (0..num_shards)
        .map(|shard_id| IndexerShard {
            shard_id,
            chunk: None,
            receipt_execution_outcomes: vec![],
            state_changes: state_changes.remove(&shard_id).unwrap_or_default(),
        })
        .collect::<Vec<_>>();

    for chunk in chunks {
        let views::ChunkView { transactions, author, header, receipts: chunk_non_local_receipts } =
            chunk;

        let shard_id = header.shard_id as usize;

        let mut outcomes = shards_outcomes
            .remove(&header.shard_id)
            .expect("Execution outcomes for given shard should be present");

        // Take execution outcomes for receipts from the vec and keep only the ones for transactions
        let mut receipt_outcomes = outcomes.split_off(transactions.len());

        let indexer_transactions = transactions
            .into_iter()
            .zip(outcomes.into_iter())
            .map(|(transaction, outcome)| {
                assert_eq!(
                    outcome.execution_outcome.id, transaction.hash,
                    "This ExecutionOutcome must have the same id as Transaction hash"
                );
                IndexerTransactionWithOutcome { outcome, transaction }
            })
            .collect::<Vec<IndexerTransactionWithOutcome>>();

        let chunk_local_receipts = convert_transactions_sir_into_local_receipts(
            &client,
            &runtime_config,
            protocol_config_view.protocol_version,
            indexer_transactions
                .iter()
                .filter(|tx| tx.transaction.signer_id == tx.transaction.receiver_id)
                .collect::<Vec<&IndexerTransactionWithOutcome>>(),
            &block,
        )
        .await?;

        // Add local receipts to corresponding outcomes
        for receipt in &chunk_local_receipts {
            if let Some(outcome) = receipt_outcomes
                .iter_mut()
                .find(|outcome| outcome.execution_outcome.id == receipt.receipt_id)
            {
                debug_assert!(outcome.receipt.is_none());
                outcome.receipt = Some(receipt.clone());
            }
        }

        let mut chunk_receipts = chunk_local_receipts;

        let mut receipt_execution_outcomes: Vec<IndexerExecutionOutcomeWithReceipt> = vec![];
        for outcome in receipt_outcomes {
            let IndexerExecutionOutcomeWithOptionalReceipt { execution_outcome, receipt } = outcome;
            let receipt = if let Some(receipt) = receipt {
                receipt
            } else {
                // Receipt might be missing only in case of delayed local receipt
                // that appeared in some of the previous blocks
                // we will be iterating over previous blocks until we found the receipt
                let mut prev_block_tried = 0u16;
                let mut prev_block_hash = block.header.prev_hash;
                'find_local_receipt: loop {
                    if prev_block_tried > 1000 {
                        panic!("Failed to find local receipt in 1000 prev blocks");
                    }
                    let prev_block = match fetch_block(&client, prev_block_hash).await {
                        Ok(block) => block,
                        Err(err) => panic!("Unable to get previous block: {:?}", err),
                    };

                    prev_block_hash = prev_block.header.prev_hash;

                    if let Some(receipt) = find_local_receipt_by_id_in_block(
                        &client,
                        &runtime_config,
                        protocol_config_view.protocol_version,
                        prev_block,
                        execution_outcome.id,
                    )
                    .await?
                    {
                        break 'find_local_receipt receipt;
                    }

                    prev_block_tried += 1;
                }
            };
            receipt_execution_outcomes
                .push(IndexerExecutionOutcomeWithReceipt { execution_outcome, receipt: receipt });
        }

        // Blocks #47317863 and #47317864
        // (ErdT2vLmiMjkRoSUfgowFYXvhGaLJZUWrgimHRkousrK, 2Fr7dVAZGoPYgpwj6dfASSde6Za34GNUJb4CkZ8NSQqw)
        // are the first blocks of an upgraded protocol version on mainnet.
        // In this block ExecutionOutcomes for restored Receipts appear.
        // However the Receipts are not included in any Chunk. Indexer Framework needs to include them,
        // so it was decided to artificially include the Receipts into the Chunk of the Block where
        // ExecutionOutcomes appear.
        // ref: https://github.com/near/nearcore/pull/4248
        if PROBLEMATIC_BLOCKS.contains(&block.header.hash)
            && &protocol_config_view.chain_id == "mainnet"
        {
            let mut restored_receipts: Vec<views::ReceiptView> = vec![];
            let receipt_ids_included: std::collections::HashSet<CryptoHash> =
                chunk_non_local_receipts.iter().map(|receipt| receipt.receipt_id).collect();
            for outcome in &receipt_execution_outcomes {
                if receipt_ids_included.get(&outcome.receipt.receipt_id).is_none() {
                    restored_receipts.push(outcome.receipt.clone());
                }
            }

            chunk_receipts.extend(restored_receipts);
        }

        chunk_receipts.extend(chunk_non_local_receipts);

        indexer_shards[shard_id].receipt_execution_outcomes = receipt_execution_outcomes;
        // Put the chunk into corresponding indexer shard
        indexer_shards[shard_id].chunk = Some(IndexerChunkView {
            author,
            header,
            transactions: indexer_transactions,
            receipts: chunk_receipts,
        });
    }

    // Ideally we expect `shards_outcomes` to be empty by this time, but if something went wrong with
    // chunks and we end up with non-empty `shards_outcomes` we want to be sure we put them into IndexerShard
    // That might happen before the fix https://github.com/near/nearcore/pull/4228
    for (shard_id, outcomes) in shards_outcomes {
        indexer_shards[shard_id as usize].receipt_execution_outcomes.extend(
            outcomes.into_iter().map(|outcome| IndexerExecutionOutcomeWithReceipt {
                execution_outcome: outcome.execution_outcome,
                receipt: outcome.receipt.expect("`receipt` must be present at this moment"),
            }),
        )
    }

    Ok(StreamerMessage { block, shards: indexer_shards })
}

/// Function that tries to find specific local receipt by it's ID and returns it
/// otherwise returns None
async fn find_local_receipt_by_id_in_block(
    client: &Addr<near_client::ViewClientActor>,
    runtime_config: &RuntimeConfig,
    protocol_version: ProtocolVersion,
    block: views::BlockView,
    receipt_id: near_primitives::hash::CryptoHash,
) -> Result<Option<views::ReceiptView>, FailedToFetchData> {
    let chunks = fetch_block_chunks(&client, &block).await?;

    let mut shards_outcomes = fetch_outcomes(&client, block.header.hash).await?;

    for chunk in chunks {
        let views::ChunkView { header, transactions, .. } = chunk;

        let outcomes = shards_outcomes
            .remove(&header.shard_id)
            .expect("Execution outcomes for given shard should be present");

        if let Some((transaction, outcome)) =
            transactions.into_iter().zip(outcomes.into_iter()).find(|(_, outcome)| {
                outcome
                    .execution_outcome
                    .outcome
                    .receipt_ids
                    .first()
                    .expect("The transaction ExecutionOutcome should have one receipt id in vec")
                    == &receipt_id
            })
        {
            let indexer_transaction = IndexerTransactionWithOutcome { transaction, outcome };
            let local_receipts = convert_transactions_sir_into_local_receipts(
                &client,
                &runtime_config,
                protocol_version,
                vec![&indexer_transaction],
                &block,
            )
            .await?;

            return Ok(local_receipts.into_iter().next());
        }
    }
    Ok(None)
}

/// Function that starts Streamer's busy loop. Every half a seconds it fetches the status
/// compares to already fetched block height and in case it differs fetches new block of given height.
///
/// We have to pass `client: Addr<near_client::ClientActor>` and `view_client: Addr<near_client::ViewClientActor>`.
pub(crate) async fn start(
    view_client: Addr<near_client::ViewClientActor>,
    client: Addr<near_client::ClientActor>,
    indexer_config: IndexerConfig,
    store_config: near_store::StoreConfig,
    archive: bool,
    blocks_sink: mpsc::Sender<StreamerMessage>,
) {
    info!(target: INDEXER, "Starting Streamer...");
    let indexer_db_path =
        near_store::NodeStorage::opener(&indexer_config.home_dir, archive, &store_config, None)
            .path()
            .join("indexer");

    // TODO: implement proper error handling
    let db = DB::open_default(indexer_db_path).unwrap();
    let mut last_synced_block_height: Option<near_primitives::types::BlockHeight> = None;

    'main: loop {
        time::sleep(INTERVAL).await;
        match indexer_config.await_for_node_synced {
            AwaitForNodeSyncedEnum::WaitForFullSync => {
                let status = fetch_status(&client).await;
                if let Ok(status) = status {
                    if status.sync_info.syncing {
                        continue;
                    }
                }
            }
            AwaitForNodeSyncedEnum::StreamWhileSyncing => {}
        };

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
        metrics::START_BLOCK_HEIGHT.set(start_syncing_block_height as i64);
        metrics::LATEST_BLOCK_HEIGHT.set(latest_block_height as i64);
        for block_height in start_syncing_block_height..=latest_block_height {
            metrics::CURRENT_BLOCK_HEIGHT.set(block_height as i64);
            if let Ok(block) = fetch_block_by_height(&view_client, block_height).await {
                let response = build_streamer_message(&view_client, block).await;

                match response {
                    Ok(streamer_message) => {
                        debug!(target: INDEXER, "{:#?}", &streamer_message);
                        if blocks_sink.send(streamer_message).await.is_err() {
                            info!(
                                target: INDEXER,
                                "Unable to send StreamerMessage to listener, listener doesn't listen. terminating..."
                            );
                            break 'main;
                        } else {
                            metrics::NUM_STREAMER_MESSAGES_SENT.inc();
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
