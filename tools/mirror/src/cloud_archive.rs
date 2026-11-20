//! The `--source-cloud-archive` source: a cloud archive reader store the mirror opens once
//! and shares between its own reads and a background copy of the recent reader loop, which
//! keeps pulling batches out of the bucket while the mirror runs.

use crate::offline::{ChainAccess, Follow};
use anyhow::Context;
use near_async::time::{Clock, Duration};
use near_chain_configs::GenesisValidationMode;
use near_client::archive::cloud_archival_utils::CloudArchivalReaderError;
use near_client::archive::cloud_historical_reader::bootstrap_range;
use near_client::archive::cloud_recent_reader::{CloudArchivalRecentReader, PullLimit};
use near_epoch_manager::EpochManager;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::types::BlockHeight;
use near_store::Store;
use near_store::adapter::StoreAdapter;
use near_store::archive::cloud_storage::CloudStorage;
use near_store::genesis::initialize_genesis_state;
use nearcore::{NightshadeRuntime, NightshadeRuntimeExt};
use std::path::Path;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Batches the reader pulls between two progress lines.
const LOG_EVERY_BATCHES: u64 = 16;

/// How far, in blocks, the reader store runs ahead of the heights the mirror asks for.
/// The bootstrap copies this many blocks, and the reader then pulls only up to this far
/// above the mirror. Without the bound the reader copies everything up to the bucket
/// head, which for a fork days behind mainnet is millions of blocks. 3200 blocks are
/// 100 batches: about 70 s to bootstrap, about 30 minutes of mirror traffic.
const SOURCE_READ_AHEAD: BlockHeight = 3200;

/// How many times in a row a bucket read may fail while the source store opens before
/// `open` gives up. The reads are retried a polling interval apart.
const OPEN_MAX_ATTEMPTS: u32 = 5;

/// What `mirror run --source-cloud-archive` reads the source chain through.
pub(crate) struct CloudArchiveSource {
    pub(crate) chain_access: ChainAccess,
    pub(crate) follower: ReaderFollower,
}

impl CloudArchiveSource {
    /// Opens the reader home at `home`, bootstraps its store out of the bucket when it
    /// holds no reader head yet, and starts following the bucket. `start_height` is the
    /// first height the mirror wants; the bootstrap opens there, so the bucket has to
    /// hold it and a block below it. The reader stays at most `SOURCE_READ_AHEAD` blocks
    /// above the heights the mirror asks for. A store whose reader head is more than that
    /// below `start_height` is refused: following it would copy everything in between.
    pub(crate) async fn open(
        home: &Path,
        start_height: BlockHeight,
        skip_state: bool,
        shutdown: CancellationToken,
    ) -> anyhow::Result<Self> {
        let config = nearcore::config::load_config(home, GenesisValidationMode::UnsafeFast)
            .with_context(|| format!("error loading config from {:?}", home))?;
        let polling_interval = config
            .config
            .cloud_archival
            .as_ref()
            .and_then(|cloud_archival| cloud_archival.reader.as_ref())
            .context("cloud_archival.reader is not configured in the source home's config.json")?
            .polling_interval;
        // `open_storage` hands the config's cloud storage context to the opener, which
        // builds the bucket client next to the store.
        let node_storage =
            nearcore::open_storage(home, &config).context("failed opening the source store")?;
        let store = node_storage.get_hot_store();
        let cloud_storage = node_storage
            .get_cloud_storage()
            .context(
                "the source store opened without cloud storage; is cloud_archival.location set?",
            )?
            .clone();
        // Idempotent, and nothing else writes them into a store the bootstrap builds.
        initialize_genesis_state(store.clone(), &config.genesis, Some(home));
        let epoch_manager =
            EpochManager::new_arc_handle(store.clone(), &config.genesis.config, Some(home));
        let shard_tracker = ShardTracker::new(
            config.client_config.tracked_shards_config.clone(),
            epoch_manager.clone(),
            config.validator_signer.clone(),
        );
        let runtime =
            NightshadeRuntime::from_config(home, store.clone(), &config, epoch_manager.clone())
                .context("could not create the transaction runtime")?;

        let pull_limit = PullLimit::new(start_height + SOURCE_READ_AHEAD);
        match store.cloud_archival_store().reader_head() {
            Some(reader_head) if reader_head.height + SOURCE_READ_AHEAD < start_height => {
                anyhow::bail!(
                    "the source store's reader head #{} is far below the mirror's start #{}; \
                     remove {:?} so the store bootstraps at the start",
                    reader_head.height,
                    start_height,
                    home.join(
                        config.config.store.path.as_deref().unwrap_or_else(|| Path::new("data"))
                    ),
                )
            }
            // A bootstrap that stopped part way left a reader head too, and the reader
            // loop takes the archive on from there.
            Some(reader_head) => tracing::info!(
                target: "mirror",
                reader_head = reader_head.height,
                start_height,
                "the source store has a reader head; following the cloud archive from there"
            ),
            None => {
                bootstrap(
                    &store,
                    &cloud_storage,
                    &shard_tracker,
                    start_height,
                    skip_state,
                    polling_interval,
                    &pull_limit,
                    &shutdown,
                )
                .await?
            }
        }

        let reader = CloudArchivalRecentReader::new(
            Clock::real(),
            store.clone(),
            cloud_storage.clone(),
            shard_tracker.epoch_manager().clone(),
            shard_tracker,
            polling_interval,
        )
        .with_skip_state(skip_state)
        .with_pull_limit(pull_limit.clone());
        let follower = ReaderFollower::spawn(
            reader,
            store.clone(),
            cloud_storage,
            polling_interval,
            shutdown.clone(),
        );
        let chain_access = ChainAccess::from_parts(
            store,
            &config,
            epoch_manager,
            runtime,
            Some(Follow { shutdown, pull_limit, read_ahead: SOURCE_READ_AHEAD }),
            skip_state,
        );
        Ok(Self { chain_access, follower })
    }
}

/// Fills a fresh reader store out of the bucket, from `start_height` up
/// `SOURCE_READ_AHEAD` blocks, or to the bucket's block head if that is lower. Raises
/// `pull_limit` to the same height, so the reader goes on from there. The bootstrap
/// anchors on the block below `start_height`, so the bucket has to hold one.
async fn bootstrap(
    store: &Store,
    cloud_storage: &CloudStorage,
    shard_tracker: &ShardTracker,
    start_height: BlockHeight,
    skip_state: bool,
    polling_interval: Duration,
    pull_limit: &PullLimit,
    shutdown: &CancellationToken,
) -> anyhow::Result<()> {
    let cloud_head =
        wait_for_cloud_head(cloud_storage, start_height, polling_interval, shutdown).await?;
    let end_height = cloud_head.min(start_height + SOURCE_READ_AHEAD);
    pull_limit.raise_to(end_height);
    tracing::info!(
        target: "mirror",
        start_height,
        end_height,
        cloud_head,
        skip_state,
        "bootstrapping the source store from the cloud archive"
    );
    // The writer moves the block head before it writes the window's shard batches, so the
    // last windows can be missing a shard batch for a while, and a bucket read can fail.
    // The bootstrap saves the reader head after every window, so once it has one the
    // reader loop takes the rest on, waiting for each shard. Before that it is retried.
    let mut attempt = 1;
    loop {
        let result = bootstrap_range(
            store,
            cloud_storage,
            shard_tracker.epoch_manager().as_ref(),
            shard_tracker,
            start_height,
            end_height,
            skip_state,
        )
        .await;
        let Err(error) = result else { break };
        if let Some(reader_head) = store.cloud_archival_store().reader_head() {
            tracing::warn!(
                target: "mirror",
                reader_head = reader_head.height,
                error = format!("{:#}", error),
                "the bootstrap stopped part way; the reader loop goes on from its head"
            );
            return Ok(());
        }
        if attempt >= OPEN_MAX_ATTEMPTS {
            return Err(error).with_context(|| {
                format!(
                    "bootstrapping the source store from the cloud archive failed; \
                     the archive must cover the start height #{} and hold a block below it",
                    start_height
                )
            });
        }
        tracing::warn!(
            target: "mirror",
            attempt,
            error = format!("{:#}", error),
            "bootstrapping the source store failed; retrying"
        );
        attempt += 1;
        sleep_or_shutdown(polling_interval, shutdown).await?;
    }
    let reader_head = store
        .cloud_archival_store()
        .reader_head()
        .context("the bootstrap left no reader head in the source store")?;
    tracing::info!(
        target: "mirror",
        start_height,
        reader_head = reader_head.height,
        "bootstrapped the source store from the cloud archive"
    );
    Ok(())
}

/// The bucket's block head once it is at or above `height`. A fork taken near the chain's
/// head can run ahead of the writer, so a bucket that is not there yet is polled.
async fn wait_for_cloud_head(
    cloud_storage: &CloudStorage,
    height: BlockHeight,
    polling_interval: Duration,
    shutdown: &CancellationToken,
) -> anyhow::Result<BlockHeight> {
    let mut failed_reads = 0;
    loop {
        match cloud_storage.get_cloud_block_head().await {
            Ok(cloud_head) => {
                failed_reads = 0;
                if let Some(cloud_head) = cloud_head.filter(|head| *head >= height) {
                    return Ok(cloud_head);
                }
                tracing::info!(
                    target: "mirror",
                    ?cloud_head,
                    height,
                    "the bucket's block head is below the height the source store opens at; waiting"
                );
            }
            Err(error) => {
                failed_reads += 1;
                if failed_reads >= OPEN_MAX_ATTEMPTS {
                    return Err(error).context("could not read the bucket's block head");
                }
                tracing::warn!(
                    target: "mirror",
                    failed_reads,
                    %error,
                    "could not read the bucket's block head; retrying"
                );
            }
        }
        sleep_or_shutdown(polling_interval, shutdown).await?;
    }
}

/// Sleeps for `duration`, or fails once shutdown is requested.
async fn sleep_or_shutdown(duration: Duration, shutdown: &CancellationToken) -> anyhow::Result<()> {
    let clock = Clock::real();
    tokio::select! {
        _ = shutdown.cancelled() => {
            anyhow::bail!("shutdown requested while opening the source store")
        }
        _ = clock.sleep(duration) => Ok(()),
    }
}

/// The recent reader loop running next to the mirror on the shared store, with a monitor
/// task that reports its progress and stops it on shutdown.
pub(crate) struct ReaderFollower {
    reader: CloudArchivalRecentReader,
    loop_task: JoinHandle<Result<(), CloudArchivalReaderError>>,
    monitor_task: JoinHandle<()>,
}

impl ReaderFollower {
    fn spawn(
        reader: CloudArchivalRecentReader,
        store: Store,
        cloud_storage: Arc<CloudStorage>,
        polling_interval: Duration,
        shutdown: CancellationToken,
    ) -> Self {
        let loop_task = tokio::spawn({
            let reader = reader.clone();
            let shutdown = shutdown.clone();
            async move {
                let result = reader.cloud_archival_loop().await;
                if let Err(error) = &result {
                    // The loop only fails in its startup checks; the mirror would
                    // otherwise run to the store's head and wait there for good.
                    tracing::error!(
                        target: "mirror",
                        %error,
                        "the cloud archive reader loop failed; stopping the mirror"
                    );
                    shutdown.cancel();
                }
                result
            }
        });
        let monitor_task = tokio::spawn(monitor(store, cloud_storage, polling_interval, shutdown));
        Self { reader, loop_task, monitor_task }
    }

    /// Stops the reader loop after the batch in flight and reports how it ended. The
    /// monitor is aborted and awaited, so its `Store` clone is gone before the caller's.
    pub(crate) async fn stop(self) -> anyhow::Result<()> {
        self.reader.stop();
        self.monitor_task.abort();
        let _ = self.monitor_task.await;
        match self.loop_task.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(error).context("the cloud archive reader loop failed"),
            Err(join_error) => Err(join_error).context("the cloud archive reader loop panicked"),
        }
    }
}

/// Keeps the head gauges current on every poll (the bucket head is one small read) and
/// logs the reader's progress every `LOG_EVERY_BATCHES` batches, until shutdown. The
/// reader loop is left to `ReaderFollower::stop`, which runs once the mirror has
/// stopped: its interrupt handle asserts against a second stop.
async fn monitor(
    store: Store,
    cloud_storage: Arc<CloudStorage>,
    polling_interval: Duration,
    shutdown: CancellationToken,
) {
    let log_every = LOG_EVERY_BATCHES * cloud_storage.batch_size() as u64;
    let mut last_logged: Option<BlockHeight> = None;
    let clock = Clock::real();
    loop {
        let cloud_head = match cloud_storage.get_cloud_block_head().await {
            Ok(cloud_head) => cloud_head,
            Err(error) => {
                tracing::warn!(target: "mirror", %error, "could not read the bucket's block head");
                None
            }
        };
        if let Some(cloud_head) = cloud_head {
            crate::metrics::SOURCE_BUCKET_HEAD.set(cloud_head as i64);
        }
        if let Some(reader_head) = store.cloud_archival_store().reader_head() {
            crate::metrics::SOURCE_READER_HEAD.set(reader_head.height as i64);
            if last_logged.is_none_or(|height| reader_head.height >= height + log_every) {
                tracing::info!(
                    target: "mirror",
                    reader_head = reader_head.height,
                    ?cloud_head,
                    "cloud archive reader progress"
                );
                last_logged = Some(reader_head.height);
            }
        }
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = clock.sleep(polling_interval) => {}
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use near_chain::test_utils::setup;
    use near_store::archive::cloud_storage::bucket_config::BucketConfig;
    use near_store::archive::cloud_storage::config::create_test_cloud_storage;

    /// A bucket whose head is above the start but which holds no block below it: the
    /// bootstrap fails, and does not move the start.
    #[tokio::test]
    async fn test_bootstrap_fails_when_the_bucket_does_not_cover_the_start() {
        let tmp = tempfile::tempdir().unwrap();
        let bucket = create_test_cloud_storage(
            tmp.path().to_path_buf(),
            "test".to_string(),
            BucketConfig::canonical(),
        );
        bucket.update_cloud_block_head(2000).await.unwrap();
        let (chain, epoch_manager, _, _) = setup(Clock::real());
        let store = chain.chain_store().store();

        let error = bootstrap(
            &store,
            &bucket,
            &ShardTracker::new_empty(epoch_manager),
            1000,
            true,
            Duration::milliseconds(1),
            &PullLimit::new(1000),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert!(format!("{error:#}").contains("must cover the start height #1000"), "{error:#}");
        assert!(store.cloud_archival_store().reader_head().is_none());
    }
}
