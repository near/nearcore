//! The `--source-cloud-archive` source: a cloud archive reader store the mirror opens once
//! and shares between its own reads and a background task that keeps extending it out of
//! the bucket with `bootstrap_range` while the mirror runs. The store holds no state.

use crate::metrics::{SOURCE_BUCKET_HEAD, SOURCE_READER_HEAD};
use crate::offline::{ChainAccess, Follow};
use anyhow::Context;
use near_async::time::{Clock, Duration};
use near_chain_configs::GenesisValidationMode;
use near_client::archive::cloud_historical_reader::bootstrap_range;
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
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// How far, in blocks, the source store runs ahead of the heights the mirror asks for.
/// The first fill copies this many blocks, and an extend runs only while the store is less
/// than this far above the mirror, copying at most this many. Without the bound the store
/// would copy everything up to the bucket head, which for a fork days behind mainnet is
/// millions of blocks. 3200 blocks are 100 batches: about 70 s to copy, about 30 minutes
/// of mirror traffic.
const SOURCE_READ_AHEAD: BlockHeight = 3200;

/// How many extends in a row may fail while the source store opens before `open` gives
/// up. They are retried a polling interval apart.
const OPEN_MAX_ATTEMPTS: u32 = 5;

/// How many extends in a row may fail, once the mirror runs, before each failure is logged
/// as a warning. At the bucket's head the writer moves the block head before it uploads
/// the window's shard batches, so an extend there can fail a time or two before it goes
/// through.
const QUIET_FAILURES: u32 = 3;

/// What `mirror run --source-cloud-archive` reads the source chain through.
pub(crate) struct CloudArchiveSource {
    pub(crate) chain_access: ChainAccess,
    pub(crate) extender: StoreExtender,
}

impl CloudArchiveSource {
    /// Opens the reader home at `home`, fills its store out of the bucket from
    /// `start_height` when it holds no reader head yet, and starts extending it. The bucket
    /// has to hold `start_height` and a block below it. An extend runs only while the store
    /// is less than `SOURCE_READ_AHEAD` blocks above the heights the mirror asks for. A
    /// store whose reader head is more than that below `start_height` is refused: extending
    /// it would copy everything in between.
    pub(crate) async fn open(
        home: &Path,
        start_height: BlockHeight,
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

        let source = SourceStore {
            store: store.clone(),
            cloud_storage,
            shard_tracker,
            first_height: start_height,
            asked: Arc::new(AtomicU64::new(start_height)),
        };
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
            // A fill that stopped part way left a reader head too, and the extends go on
            // from there.
            Some(reader_head) => tracing::info!(
                target: "mirror",
                reader_head = reader_head.height,
                start_height,
                "the source store has a reader head; extending it from there"
            ),
            None => source.fill(polling_interval, &shutdown).await?,
        }

        let asked = source.asked.clone();
        let extender = StoreExtender::spawn(source, polling_interval, shutdown.child_token());
        let chain_access = ChainAccess::from_parts(
            store,
            &config,
            epoch_manager,
            runtime,
            Some(Follow { shutdown, asked }),
        );
        Ok(Self { chain_access, extender })
    }
}

/// The source store and what extends it out of the bucket.
struct SourceStore {
    store: Store,
    cloud_storage: Arc<CloudStorage>,
    shard_tracker: ShardTracker,
    /// Where the store opens when it has no reader head yet.
    first_height: BlockHeight,
    /// The highest height the mirror has asked about.
    asked: Arc<AtomicU64>,
}

impl SourceStore {
    fn reader_head(&self) -> Option<BlockHeight> {
        self.store.cloud_archival_store().reader_head().map(|reader_head| reader_head.height)
    }

    /// Copies the next range out of the bucket: from above the reader head, or from
    /// `first_height` when there is none, up `SOURCE_READ_AHEAD` blocks, or to the bucket's
    /// block head if that is lower. Does nothing when the store already reaches
    /// `SOURCE_READ_AHEAD` above the heights the mirror asked for, or the bucket holds
    /// nothing new.
    ///
    /// `bootstrap_range` anchors the range on the block below it, which is the reader
    /// head's, and saves the reader head after every batch, so a failed extend is taken on
    /// from there by the next one. It runs without state: with state, every call would
    /// rebuild each shard from a snapshot.
    async fn extend(&self) -> anyhow::Result<()> {
        let start = self.reader_head().map_or(self.first_height, |height| height + 1);
        let cloud_head = self
            .cloud_storage
            .get_cloud_block_head()
            .await
            .context("could not read the bucket's block head")?;
        if let Some(cloud_head) = cloud_head {
            SOURCE_BUCKET_HEAD.set(cloud_head as i64);
        }
        if start > self.asked.load(Ordering::Relaxed) + SOURCE_READ_AHEAD {
            return Ok(());
        }
        let Some(cloud_head) = cloud_head.filter(|cloud_head| *cloud_head >= start) else {
            return Ok(());
        };
        let end = cloud_head.min(start + SOURCE_READ_AHEAD);
        // TODO(cloud_archival): switch to a proper extend once the bootstrap code has one. It
        // would skip fetching the anchor batch and the epoch data again on every call, and stop
        // at the shard heads, so it never asks for shard data that is not uploaded yet.
        bootstrap_range(
            &self.store,
            &self.cloud_storage,
            self.shard_tracker.epoch_manager().as_ref(),
            &self.shard_tracker,
            start,
            end,
            true,
        )
        .await
    }

    /// Extends an empty store until it has a reader head, which the mirror reads up to.
    /// Waits while the bucket's block head is below `first_height`, which a fork taken near
    /// the chain's head can be. Fails after `OPEN_MAX_ATTEMPTS` failed extends in a row.
    async fn fill(
        &self,
        polling_interval: Duration,
        shutdown: &CancellationToken,
    ) -> anyhow::Result<()> {
        tracing::info!(
            target: "mirror",
            start_height = self.first_height,
            "bootstrapping the source store from the cloud archive"
        );
        let mut failures = 0;
        loop {
            let result = self.extend().await;
            // The writer moves the block head before it writes the window's shard batches,
            // so the last windows can be missing a shard batch for a while. Once a batch
            // is in, the extends after `open` take the rest on.
            if let Some(reader_head) = self.reader_head() {
                if let Err(error) = result {
                    tracing::warn!(
                        target: "mirror",
                        reader_head,
                        error = format!("{:#}", error),
                        "the bootstrap stopped part way; extending goes on from its head"
                    );
                }
                tracing::info!(
                    target: "mirror",
                    start_height = self.first_height,
                    reader_head,
                    "bootstrapped the source store from the cloud archive"
                );
                return Ok(());
            }
            match result {
                Ok(()) => {
                    // A successful read of the bucket's block head ends a run of failures.
                    failures = 0;
                    tracing::info!(
                        target: "mirror",
                        start_height = self.first_height,
                        "the bucket's block head is below the height the source store opens at; waiting"
                    );
                }
                Err(error) => {
                    failures += 1;
                    if failures >= OPEN_MAX_ATTEMPTS {
                        return Err(error).with_context(|| {
                            format!(
                                "bootstrapping the source store from the cloud archive failed; \
                                 the archive must cover the start height #{} and hold a block below it",
                                self.first_height
                            )
                        });
                    }
                    tracing::warn!(
                        target: "mirror",
                        attempt = failures,
                        error = format!("{:#}", error),
                        "bootstrapping the source store failed; retrying"
                    );
                }
            }
            sleep_or_shutdown(polling_interval, shutdown).await?;
        }
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

/// The task that extends the source store once per polling interval while the mirror runs,
/// and keeps the head gauges current. A failed extend is retried on the next tick.
pub(crate) struct StoreExtender {
    stop: CancellationToken,
    task: JoinHandle<()>,
}

impl StoreExtender {
    fn spawn(source: SourceStore, polling_interval: Duration, stop: CancellationToken) -> Self {
        let task = tokio::spawn({
            let stop = stop.clone();
            async move {
                let clock = Clock::real();
                let mut failures = 0;
                loop {
                    tokio::select! {
                        _ = stop.cancelled() => return,
                        result = source.extend() => match result {
                            Ok(()) => failures = 0,
                            Err(error) => {
                                failures += 1;
                                let error = format!("{:#}", error);
                                if failures > QUIET_FAILURES {
                                    tracing::warn!(target: "mirror", failures, error, "extending the source store failed; retrying");
                                } else {
                                    tracing::debug!(target: "mirror", failures, error, "extending the source store failed; retrying");
                                }
                            }
                        },
                    }
                    if let Some(reader_head) = source.reader_head() {
                        SOURCE_READER_HEAD.set(reader_head as i64);
                    }
                    tokio::select! {
                        _ = stop.cancelled() => return,
                        _ = clock.sleep(polling_interval) => {}
                    }
                }
            }
        });
        Self { stop, task }
    }

    /// Stops extending, dropping an extend in flight, and waits for the task, so its
    /// `Store` clone is gone before the caller's. A dropped extend leaves the reader head
    /// at its last whole batch.
    pub(crate) async fn stop(self) -> anyhow::Result<()> {
        self.stop.cancel();
        self.task.await.context("the source store extender panicked")
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use near_chain::test_utils::setup;
    use near_store::archive::cloud_storage::bucket_config::BucketConfig;
    use near_store::archive::cloud_storage::config::create_test_cloud_storage;

    /// A bucket whose head is above the start but which holds no block below it: the
    /// fill fails, and does not move the start.
    #[tokio::test]
    async fn test_fill_fails_when_the_bucket_does_not_cover_the_start() {
        let tmp = tempfile::tempdir().unwrap();
        let bucket = create_test_cloud_storage(
            tmp.path().to_path_buf(),
            "test".to_string(),
            BucketConfig::canonical(),
        );
        bucket.update_cloud_block_head(2000).await.unwrap();
        let (chain, epoch_manager, _, _) = setup(Clock::real());
        let store = chain.chain_store().store();
        let source = SourceStore {
            store: store.clone(),
            cloud_storage: bucket,
            shard_tracker: ShardTracker::new_empty(epoch_manager),
            first_height: 1000,
            asked: Arc::new(AtomicU64::new(1000)),
        };

        let error =
            source.fill(Duration::milliseconds(1), &CancellationToken::new()).await.unwrap_err();
        assert!(format!("{error:#}").contains("must cover the start height #1000"), "{error:#}");
        assert!(store.cloud_archival_store().reader_head().is_none());
    }
}
