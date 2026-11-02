use crate::INDEXER;
use crate::{AwaitForNodeSyncedEnum, IndexerConfig};
pub use fetchers::IndexerClientFetcher;
use near_async::time::{Clock, Duration};
pub use near_client::indexer::IndexerViewClientFetcher;
use near_client::indexer::{self, FailedToFetchData};
use near_epoch_manager::shard_tracker::ShardTracker;
use near_indexer_primitives::StreamerMessage;
use near_primitives::types::BlockHeight;
use near_primitives::views::BlockView;
use rocksdb::DB;
use tokio::sync::mpsc;

mod fetchers;
mod metrics;

const INTERVAL: Duration = Duration::milliseconds(250);

/// How many consecutive times we retry building a streamer message for the same
/// height before terminating. In `WaitForFullSync` mode, failures while the node
/// is still syncing are expected (e.g. epoch data not yet available after a
/// restart, see #15867) and do not count against this budget. In
/// `StreamWhileSyncing` mode the node is ~always syncing, so every failure is
/// counted - otherwise the budget would never be enforced.
const MAX_BUILD_STREAMER_MESSAGE_ATTEMPTS: u32 = 10;
const LAST_SYNCED_BLOCK_HEIGHT_KEY: &[u8] = b"last_synced_block_height";

/// Fetches a block's chunks, outcomes, receipts and state changes into a streamer message.
pub async fn build_streamer_message(
    client: &IndexerViewClientFetcher,
    block: BlockView,
    shard_tracker: &ShardTracker,
) -> Result<StreamerMessage, FailedToFetchData> {
    let _timer = metrics::BUILD_STREAMER_MESSAGE_TIME.start_timer();
    indexer::build_streamer_message(client, block, shard_tracker).await
}

/// Whether the node reports it is fully synced and in a steady state. A failed
/// status fetch is treated as "not ready" so we don't prematurely give up while
/// the node is not in a steady state.
async fn node_is_ready(client: &IndexerClientFetcher) -> bool {
    match client.fetch_status().await {
        Ok(status) => !status.sync_info.syncing,
        Err(err) => {
            tracing::warn!(target: INDEXER, ?err, "failed to fetch node status, assuming the node is not ready");
            false
        }
    }
}

/// Function that starts Streamer's busy loop. Every half a seconds it fetches the status
/// compares to already fetched block height and in case it differs fetches new block of given height.
pub async fn start(
    view_client: IndexerViewClientFetcher,
    client: IndexerClientFetcher,
    shard_tracker: ShardTracker,
    indexer_config: IndexerConfig,
    store_config: near_store::StoreConfig,
    blocks_sink: mpsc::Sender<StreamerMessage>,
    clock: Clock,
) {
    tracing::info!(target: INDEXER, "starting streamer");
    let indexer_db_path =
        near_store::NodeStorage::opener(&indexer_config.home_dir, &store_config, None, None)
            .path()
            .join("indexer");

    let db = match DB::open_default(indexer_db_path) {
        Ok(db) => db,
        Err(err) => panic!("Unable to open indexer db: {:?}", err),
    };

    let mut last_synced_block_height: Option<BlockHeight> = None;
    // Consecutive failed attempts to build a streamer message; reset on success.
    // In `WaitForFullSync` mode it is also reset while the node is syncing (see
    // `MAX_BUILD_STREAMER_MESSAGE_ATTEMPTS`).
    let mut build_streamer_message_attempts: u32 = 0;

    'main: loop {
        clock.sleep(INTERVAL).await;
        match indexer_config.await_for_node_synced {
            AwaitForNodeSyncedEnum::WaitForFullSync => {
                let status = client.fetch_status().await;
                let Ok(status) = status else {
                    tracing::error!(target: INDEXER, ?status, "failed to fetch node status, retrying");
                    continue;
                };
                if status.sync_info.syncing {
                    tracing::debug!(target: INDEXER, ?status, "the node is syncing, waiting");
                    continue;
                }
            }
            AwaitForNodeSyncedEnum::StreamWhileSyncing => {}
        };

        tracing::debug!(target: INDEXER, "starting streaming the next block range");
        let block = view_client.fetch_latest_block(indexer_config.finality.clone()).await;
        let Ok(block) = block else {
            tracing::error!(target: INDEXER, ?block, "failed to fetch latest block, retrying");
            continue;
        };

        let latest_block_height = block.header.height;
        let start_syncing_block_height = get_start_syncing_block_height(
            &db,
            &indexer_config,
            last_synced_block_height,
            latest_block_height,
        );

        tracing::debug!(
            target: INDEXER,
            %start_syncing_block_height,
            %latest_block_height,
            "streaming is about to start",
        );
        metrics::START_BLOCK_HEIGHT.set(start_syncing_block_height as i64);
        metrics::LATEST_BLOCK_HEIGHT.set(latest_block_height as i64);
        for block_height in start_syncing_block_height..=latest_block_height {
            metrics::CURRENT_BLOCK_HEIGHT.set(block_height as i64);

            let block = match view_client.fetch_block_by_height(block_height).await {
                Ok(Some(block)) => block,
                Ok(None) => {
                    tracing::debug!(target: INDEXER, ?block_height, "skip height - missing block");
                    continue;
                }
                Err(err) => {
                    tracing::error!(target: INDEXER, ?block_height, ?err, "skip height - failed to fetch block");
                    continue;
                }
            };

            let streamer_message =
                Box::pin(build_streamer_message(&view_client, block, &shard_tracker)).await;
            let streamer_message = match streamer_message {
                Ok(streamer_message) => {
                    build_streamer_message_attempts = 0;
                    streamer_message
                }
                Err(err) => {
                    // When waiting for full sync, a build failure while the node
                    // is not yet ready is expected (e.g. epoch data not available
                    // after a restart, see #15867): retry the same height forever
                    // without counting it against the budget. When streaming while
                    // syncing the node is ~always "syncing", so that gate would
                    // make the budget unreachable; there we count every failure so
                    // a genuinely stuck height eventually surfaces.
                    let transient_while_syncing = matches!(
                        indexer_config.await_for_node_synced,
                        AwaitForNodeSyncedEnum::WaitForFullSync
                    ) && !node_is_ready(&client).await;
                    if transient_while_syncing {
                        build_streamer_message_attempts = 0;
                        tracing::warn!(target: INDEXER, ?block_height, ?err, "failed to build streamer message while the node is syncing, retrying the same height");
                    } else {
                        build_streamer_message_attempts += 1;
                        if build_streamer_message_attempts >= MAX_BUILD_STREAMER_MESSAGE_ATTEMPTS {
                            // Failed to build the block
                            if indexer_config.skip_broken_blocks {
                                build_streamer_message_attempts = 0;
                                tracing::error!(target: INDEXER, ?block_height, ?err, "skip height - failed to build streamer message");
                                // Record the skipped height as synced so the next outer iteration resumes right after it
                                record_synced_block_height(
                                    &db,
                                    &mut last_synced_block_height,
                                    block_height,
                                );
                                // break the inner loop, the next outer iteration resumes from `last_synced_block_height + 1`.
                                break;
                            }
                            panic!(
                                "failed to build streamer message at height {block_height} after {MAX_BUILD_STREAMER_MESSAGE_ATTEMPTS} attempts: {err:?}"
                            )
                        }
                    }

                    // Retry the same height on the next outer iteration instead of
                    // advancing `last_synced_block_height`.
                    tracing::error!(target: INDEXER, ?block_height, ?err, attempts = build_streamer_message_attempts, "failed to build streamer message, retrying the same height");
                    break;
                }
            };

            tracing::debug!(target: INDEXER, ?block_height, "sending streamer message to the listener");
            let send_result = blocks_sink.send(streamer_message).await;
            if send_result.is_err() {
                tracing::error!(
                    target: INDEXER,
                    ?block_height,
                    ?send_result,
                    "unable to send streamer message to listener, listener doesn't listen, terminating",
                );
                break 'main;
            };

            metrics::NUM_STREAMER_MESSAGES_SENT.inc();
            record_synced_block_height(&db, &mut last_synced_block_height, block_height);
        }
    }
}

/// Persists the synced block height to the indexer db and updates the in-memory copy.
fn record_synced_block_height(
    db: &rocksdb::DB,
    last_synced_block_height: &mut Option<BlockHeight>,
    block_height: BlockHeight,
) {
    db.put(LAST_SYNCED_BLOCK_HEIGHT_KEY, &block_height.to_string()).unwrap();
    *last_synced_block_height = Some(block_height);
}

fn get_start_syncing_block_height(
    db: &rocksdb::DB,
    indexer_config: &IndexerConfig,
    last_synced_block_height: Option<u64>,
    latest_block_height: u64,
) -> u64 {
    // If last synced is set, start from the next height
    if let Some(last_synced_block_height) = last_synced_block_height {
        return last_synced_block_height + 1;
    }

    // Otherwise determine the start height based on the sync mode
    match indexer_config.sync_mode {
        crate::SyncModeEnum::FromInterruption => {
            match db.get(LAST_SYNCED_BLOCK_HEIGHT_KEY).unwrap() {
                Some(value) => String::from_utf8(value).unwrap().parse::<u64>().unwrap(),
                None => latest_block_height,
            }
        }
        crate::SyncModeEnum::LatestSynced => latest_block_height,
        crate::SyncModeEnum::BlockHeight(height) => height,
    }
}
