use super::StateSyncDownloadSource;
use super::task_tracker::TaskHandle;
use super::util::{get_state_header_if_exists_in_storage, query_epoch_id_and_height_for_block};
use crate::sync::external::{ExternalConnection, StateFileType, external_storage_location};
use crate::sync::state::util::increment_download_count;
use borsh::BorshDeserialize;
use futures::FutureExt;
use futures::future::BoxFuture;
use near_async::time::{Clock, Duration};
use near_primitives::hash::CryptoHash;
use near_primitives::state_sync::ShardStateSyncResponseHeader;
use near_primitives::types::ShardId;
use near_store::Store;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

/// Logic for downloading state sync headers and parts from an external source.
pub(super) struct StateSyncDownloadSourceExternal {
    pub clock: Clock,
    pub store: Store,
    pub chain_id: String,
    pub conn: ExternalConnection,
    pub timeout: Duration,
    pub backoff: Duration,
}

impl StateSyncDownloadSourceExternal {
    async fn get_file_with_timeout(
        clock: Clock,
        timeout: Duration,
        backoff: Duration,
        cancellation: CancellationToken,
        conn: ExternalConnection,
        shard_id: ShardId,
        location: String,
        file_type: StateFileType,
    ) -> Result<Vec<u8>, near_chain::Error> {
        let fut = conn.get_file(shard_id, &location, &file_type);
        let deadline = clock.now() + timeout;
        let typ = match &file_type {
            StateFileType::StateHeader => "header",
            StateFileType::StatePart { .. } => "part",
        };
        tokio::select! {
            _ = clock.sleep_until(deadline) => {
                increment_download_count(shard_id, typ, "external", "timeout");
                Err(near_chain::Error::Other("Timeout".to_owned()))
            }
            _ = cancellation.cancelled() => {
                increment_download_count(shard_id, typ, "external", "cancelled");
                Err(near_chain::Error::Other("Cancelled".to_owned()))
            }
            result = fut => {
                match result {
                    Err(err) => {
                        // A download error typically indicates that the file is not available yet. At the
                        // start of the epoch it takes a while for dumpers to populate the external storage
                        // with state files. This backoff period prevents spamming requests during that time.
                        let deadline = clock.now() + backoff;
                        tokio::select! {
                            _ = clock.sleep_until(deadline) => {}
                            _ = cancellation.cancelled() => {}
                        }
                        increment_download_count(shard_id, typ, "external", "download_error");
                        Err(near_chain::Error::Other(format!("Failed to download: {}", err)))
                    }
                    Ok(res) => Ok(res)
                }
            }
        }
    }
}

impl StateSyncDownloadSource for StateSyncDownloadSourceExternal {
    fn download_shard_header(
        &self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
        handle: Arc<TaskHandle>,
        cancel: CancellationToken,
    ) -> BoxFuture<Result<ShardStateSyncResponseHeader, near_chain::Error>> {
        let clock = self.clock.clone();
        let timeout = self.timeout;
        let backoff = self.backoff;
        let chain_id = self.chain_id.clone();
        let conn = self.conn.clone();
        let store = self.store.clone();
        async move {
            handle.set_status("Preparing download");
            let (epoch_id, epoch_height) = query_epoch_id_and_height_for_block(&store, sync_hash)?;
            let location = external_storage_location(
                &chain_id,
                &epoch_id,
                epoch_height,
                shard_id,
                &StateFileType::StateHeader,
            );
            handle.set_status(&format!("Downloading file {}", location));
            let data = Self::get_file_with_timeout(
                clock,
                timeout,
                backoff,
                cancel,
                conn,
                shard_id,
                location,
                StateFileType::StateHeader,
            )
            .await?;
            let header = ShardStateSyncResponseHeader::try_from_slice(&data).map_err(|e| {
                increment_download_count(shard_id, "header", "external", "parse_error");
                near_chain::Error::Other(format!("Failed to parse header: {}", e))
            })?;

            increment_download_count(shard_id, "header", "external", "success");
            Ok(header)
        }
        .instrument(tracing::debug_span!("StateSyncDownloadSourceExternal::download_shard_header"))
        .boxed()
    }

    fn download_shard_part(
        &self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
        part_id: u64,
        handle: Arc<TaskHandle>,
        cancel: CancellationToken,
    ) -> BoxFuture<Result<Vec<u8>, near_chain::Error>> {
        let clock = self.clock.clone();
        let timeout = self.timeout;
        let backoff = self.backoff;
        let chain_id = self.chain_id.clone();
        let conn = self.conn.clone();
        let store = self.store.clone();
        async move {
            handle.set_status("Preparing download");
            let (epoch_id, epoch_height) = query_epoch_id_and_height_for_block(&store, sync_hash)?;
            let num_parts = get_state_header_if_exists_in_storage(&store, sync_hash, shard_id)?
                .ok_or_else(|| {
                    near_chain::Error::DBNotFoundErr(format!("No shard state header {}", sync_hash))
                })?
                .num_state_parts();
            let location = external_storage_location(
                &chain_id,
                &epoch_id,
                epoch_height,
                shard_id,
                &StateFileType::StatePart { part_id, num_parts },
            );
            handle.set_status("Downloading file");
            let data = Self::get_file_with_timeout(
                clock,
                timeout,
                backoff,
                cancel,
                conn,
                shard_id,
                location,
                StateFileType::StatePart { part_id, num_parts },
            )
            .await?;
            increment_download_count(shard_id, "part", "external", "success");
            Ok(data)
        }
        .instrument(tracing::debug_span!("StateSyncDownloadSourceExternal::download_shard_part"))
        .boxed()
    }
}
