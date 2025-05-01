use super::StateSyncDownloadSource;
use super::chain_requests::StateHeaderValidationRequest;
use super::task_tracker::TaskTracker;
use super::util::get_state_header_if_exists_in_storage;
use futures::FutureExt;
use futures::future::BoxFuture;
use near_async::messaging::AsyncSender;
use near_async::time::{Clock, Duration};
use near_chain::types::RuntimeAdapter;
use near_primitives::hash::CryptoHash;
use near_primitives::state_part::PartId;
use near_primitives::state_sync::{ShardStateSyncResponseHeader, StatePartKey};
use near_primitives::types::ShardId;
use near_store::{DBCol, Store};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

/// The downloader works on top of a StateSyncDownloadSource, by adding:
///  - caching of the header / part in rocksdb.
///  - validation of the header / part before persisting into rocksdb.
///  - retrying, if the download fails, or validation fails.
///
/// As a result, the user of this API only needs to request the header or ensure the
/// part exists on disk, and the downloader will take care of the rest.
pub(super) struct StateSyncDownloader {
    pub clock: Clock,
    pub store: Store,
    pub preferred_source: Arc<dyn StateSyncDownloadSource>,
    pub fallback_source: Option<Arc<dyn StateSyncDownloadSource>>,
    pub num_attempts_before_fallback: usize,
    pub header_validation_sender:
        AsyncSender<StateHeaderValidationRequest, Result<(), near_chain::Error>>,
    pub runtime: Arc<dyn RuntimeAdapter>,
    pub retry_backoff: Duration,
    pub task_tracker: TaskTracker,
}

impl StateSyncDownloader {
    /// Obtains the shard header. If the header exists on disk, returns that; otherwise
    /// downloads the header, validates it, retrying if needed.
    ///
    /// This method will only return an error if the download cannot be completed even
    /// with retries, or if the download is cancelled.
    pub fn ensure_shard_header(
        &self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
        cancel: CancellationToken,
    ) -> BoxFuture<Result<ShardStateSyncResponseHeader, near_chain::Error>> {
        let store = self.store.clone();
        let validation_sender = self.header_validation_sender.clone();
        let preferred_source = self.preferred_source.clone();
        let fallback_source = self.fallback_source.clone();
        let num_attempts_before_fallback = self.num_attempts_before_fallback;
        let task_tracker = self.task_tracker.clone();
        let clock = self.clock.clone();
        let retry_backoff = self.retry_backoff;
        async move {
            let handle = task_tracker.get_handle(&format!("shard {} header", shard_id)).await;
            handle.set_status("Reading existing header");
            let existing_header =
                get_state_header_if_exists_in_storage(&store, sync_hash, shard_id)?;
            if let Some(header) = existing_header {
                return Ok(header);
            }

            let i = AtomicUsize::new(0); // for easier Rust async capture
            let attempt = || {
                async {
                    // We cannot assume that either source is infallible. We interleave attempts
                    // to the available sources until one of them gives us the state successfully.
                    let source = if fallback_source.is_some()
                        && i.load(Ordering::Relaxed) >= num_attempts_before_fallback
                    {
                        i.store(0, Ordering::Relaxed);
                        fallback_source.as_ref().unwrap().as_ref()
                    } else {
                        i.fetch_add(1, Ordering::Relaxed);
                        preferred_source.as_ref()
                    };

                    let header = source
                        .download_shard_header(shard_id, sync_hash, handle.clone(), cancel.clone())
                        .await?;
                    // We cannot validate the header with just a Store. We need the Chain, so we queue it up
                    // so the chain can pick it up later, and we await until the chain gives us a response.
                    handle.set_status("Waiting for validation");
                    validation_sender
                        .send_async(StateHeaderValidationRequest {
                            shard_id,
                            sync_hash,
                            header: header.clone(),
                        })
                        .await
                        .map_err(|_| {
                            near_chain::Error::Other(
                                "Validation request could not be handled".to_owned(),
                            )
                        })??;
                    Ok::<ShardStateSyncResponseHeader, near_chain::Error>(header)
                }
            };

            loop {
                match attempt().await {
                    Ok(header) => return Ok(header),
                    Err(err) => {
                        handle.set_status(&format!(
                            "Error: {}, will retry in {}",
                            err, retry_backoff
                        ));
                        let deadline = clock.now() + retry_backoff;
                        tokio::select! {
                            _ = cancel.cancelled() => {
                                return Err(near_chain::Error::Other("Cancelled".to_owned()));
                            }
                            _ = clock.sleep_until(deadline) => {}
                        }
                    }
                }
            }
        }
        .instrument(tracing::debug_span!("StateSyncDownloader::download_shard_header"))
        .boxed()
    }

    /// Attempts once to ensure that the shard part is downloaded and validated.
    /// If the part exists on disk, just returns. Otherwise, makes one attempt
    /// to download the part and validate it.
    ///
    /// This method will return an error if the download fails or is cancelled.
    pub fn ensure_shard_part_downloaded_single_attempt(
        &self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
        state_root: CryptoHash,
        num_state_parts: u64,
        part_id: u64,
        num_prior_attempts: usize,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, Result<(), near_chain::Error>> {
        let store = self.store.clone();
        let runtime_adapter = self.runtime.clone();
        let preferred_source = self.preferred_source.clone();
        let fallback_source = self.fallback_source.clone();
        let num_attempts_before_fallback = self.num_attempts_before_fallback;
        let clock = self.clock.clone();
        let task_tracker = self.task_tracker.clone();
        let retry_backoff = self.retry_backoff;
        async move {
            if cancel.is_cancelled() {
                return Err(near_chain::Error::Other("Cancelled".to_owned()));
            }
            let handle =
                task_tracker.get_handle(&format!("shard {} part {}", shard_id, part_id)).await;
            handle.set_status("Reading existing part");
            if does_state_part_exist_on_disk(&store, sync_hash, shard_id, part_id)? {
                return Ok(());
            }

            let attempt = || async {
                let source = if fallback_source.is_some()
                    && num_prior_attempts >= num_attempts_before_fallback
                {
                    fallback_source.as_ref().unwrap().as_ref()
                } else {
                    preferred_source.as_ref()
                };

                let part = source
                    .download_shard_part(
                        shard_id,
                        sync_hash,
                        part_id,
                        handle.clone(),
                        cancel.clone(),
                    )
                    .await?;
                if runtime_adapter.validate_state_part(
                    &state_root,
                    PartId { idx: part_id, total: num_state_parts },
                    &part,
                ) {
                    let mut store_update = store.store_update();
                    let key = borsh::to_vec(&StatePartKey(sync_hash, shard_id, part_id)).unwrap();
                    store_update.set(DBCol::StateParts, &key, &part);
                    store_update.commit().map_err(|e| {
                        near_chain::Error::Other(format!("Failed to store part: {}", e))
                    })?;
                } else {
                    return Err(near_chain::Error::Other("Part data failed validation".to_owned()));
                }
                Ok(())
            };

            let res = attempt().await;
            if let Err(ref err) = res {
                handle.set_status(&format!("Error: {}, will retry in {}", err, retry_backoff));
                let deadline = clock.now() + retry_backoff;
                tokio::select! {
                    _ = cancel.cancelled() => {}
                    _ = clock.sleep_until(deadline) => {}
                }
            }
            res
        }
        .instrument(tracing::debug_span!(
            "StateSyncDownloader::ensure_shard_part_downloaded_single_attempt"
        ))
        .boxed()
    }
}

fn does_state_part_exist_on_disk(
    store: &Store,
    sync_hash: CryptoHash,
    shard_id: ShardId,
    part_id: u64,
) -> Result<bool, near_chain::Error> {
    Ok(store.exists(
        DBCol::StateParts,
        &borsh::to_vec(&StatePartKey(sync_hash, shard_id, part_id)).unwrap(),
    )?)
}
