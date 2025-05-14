use crate::metrics;

use anyhow::Context;
use borsh::BorshSerialize;
use futures::future::select_all;
use futures::{FutureExt, StreamExt};
use near_async::futures::{FutureSpawner, respawn_for_parallelism};
use near_async::time::{Clock, Duration, Interval};
use near_chain::types::RuntimeAdapter;
use near_chain::{Chain, ChainGenesis, DoomslugThresholdMode};
use near_chain_configs::{ClientConfig, ExternalStorageLocation, MutableValidatorSigner};
use near_client::sync::external::{
    ExternalConnection, external_storage_location_directory, get_part_id_from_filename,
    is_part_filename,
};
use near_client::sync::external::{
    StateFileType, create_bucket_read_write, external_storage_location,
};
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::block::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::state_part::PartId;
use near_primitives::state_sync::StateSyncDumpProgress;
use near_primitives::types::{EpochHeight, EpochId, ShardId, StateRoot};
use parking_lot::{Condvar, Mutex, RwLock};
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::collections::{HashMap, HashSet};
use std::i64;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use tokio::sync::Semaphore;
use tokio::sync::oneshot;

/// Time limit per state dump iteration.
/// A node must check external storage for parts to dump again once time is up.
pub const STATE_DUMP_ITERATION_TIME_LIMIT_SECS: u64 = 300;

// TODO: could refactor this further and just have one "Dumper" struct here
pub struct StateSyncDumper {
    pub clock: Clock,
    pub client_config: ClientConfig,
    pub chain_genesis: ChainGenesis,
    pub epoch_manager: Arc<dyn EpochManagerAdapter>,
    pub shard_tracker: ShardTracker,
    pub runtime: Arc<dyn RuntimeAdapter>,
    /// Contains validator key for this node. This field is mutable and optional. Use with caution!
    /// Lock the value of mutable validator signer for the duration of a request to ensure consistency.
    /// Please note that the locked value should not be stored anywhere or passed through the thread boundary.
    pub validator: MutableValidatorSigner,
    pub future_spawner: Arc<dyn FutureSpawner>,
    pub handle: Option<Arc<StateSyncDumpHandle>>,
}

impl StateSyncDumper {
    /// Starts a thread that periodically checks whether any new parts need to be uploaded, and then spawns
    /// futures to generate and upload them
    pub fn start(&mut self) -> anyhow::Result<()> {
        assert!(self.handle.is_none(), "StateSyncDumper already started");

        let dump_config = if let Some(dump_config) = self.client_config.state_sync.dump.clone() {
            dump_config
        } else {
            // Dump is not configured, and therefore not enabled.
            tracing::debug!(target: "state_sync_dump", "Not spawning the state sync dump loop");
            return Ok(());
        };
        tracing::info!(target: "state_sync_dump", "Spawning the state sync dump loop");

        let external = match dump_config.location {
            ExternalStorageLocation::S3 { bucket, region } => ExternalConnection::S3 {
                bucket: Arc::new(create_bucket_read_write(&bucket, &region, std::time::Duration::from_secs(30), dump_config.credentials_file).expect(
                    "Failed to authenticate connection to S3. Please either provide AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in the environment, or create a credentials file and link it in config.json as 's3_credentials_file'."))
            },
            ExternalStorageLocation::Filesystem { root_dir } => ExternalConnection::Filesystem { root_dir },
            ExternalStorageLocation::GCS { bucket } => {
                if let Some(credentials_file) = dump_config.credentials_file {
                    if let Ok(var) = std::env::var("SERVICE_ACCOUNT") {
                        tracing::warn!(target: "state_sync_dump", "Environment variable 'SERVICE_ACCOUNT' is set to {var}, but 'credentials_file' in config.json overrides it to '{credentials_file:?}'");
                        println!("Environment variable 'SERVICE_ACCOUNT' is set to {var}, but 'credentials_file' in config.json overrides it to '{credentials_file:?}'");
                    }
                    // SAFE: no threads *yet*.
                    unsafe { std::env::set_var("SERVICE_ACCOUNT", &credentials_file) };
                    tracing::info!(target: "state_sync_dump", "Set the environment variable 'SERVICE_ACCOUNT' to '{credentials_file:?}'");
                }
                ExternalConnection::GCS {
                    gcs_client: Arc::new(object_store::gcp::GoogleCloudStorageBuilder::new().with_bucket_name(&bucket).build().unwrap()),
                    reqwest_client: Arc::new(reqwest::Client::default()),
                    bucket,
                }
            }
        };

        let chain_id = self.client_config.chain_id.clone();
        let handle = Arc::new(StateSyncDumpHandle::new());

        let chain = Chain::new_for_view_client(
            self.clock.clone(),
            self.epoch_manager.clone(),
            self.shard_tracker.clone(),
            self.runtime.clone(),
            &self.chain_genesis,
            DoomslugThresholdMode::TwoThirds,
            false,
        )
        .unwrap();
        if let Some(shards) = dump_config.restart_dump_for_shards.as_ref() {
            for shard_id in shards {
                chain.chain_store().set_state_sync_dump_progress(*shard_id, None).unwrap();
                tracing::debug!(target: "state_sync_dump", ?shard_id, "Dropped existing progress");
            }
        }
        self.future_spawner.spawn_boxed(
            "state_sync_dump",
            do_state_sync_dump(
                self.clock.clone(),
                chain,
                self.epoch_manager.clone(),
                self.shard_tracker.clone(),
                self.runtime.clone(),
                chain_id,
                external,
                dump_config.iteration_delay.unwrap_or(Duration::seconds(10)),
                self.validator.clone(),
                handle.clone(),
                self.future_spawner.clone(),
            )
            .boxed(),
        );

        self.handle = Some(handle);
        Ok(())
    }

    pub fn stop(&mut self) {
        self.handle.take();
    }

    // Tell the dumper to stop and wait until it's finished
    pub fn stop_and_await(&mut self) {
        let Some(handle) = self.handle.take() else {
            return;
        };
        handle.stop_and_await();
    }
}

/// Cancels the dumper when dropped and allows waiting for the dumper task to finish
pub struct StateSyncDumpHandle {
    keep_running: AtomicBool,
    task_running: Mutex<bool>,
    await_task: Condvar,
}

impl Drop for StateSyncDumpHandle {
    fn drop(&mut self) {
        self.stop()
    }
}

impl StateSyncDumpHandle {
    fn new() -> Self {
        Self {
            keep_running: AtomicBool::new(true),
            task_running: Mutex::new(true),
            await_task: Condvar::new(),
        }
    }

    // Tell the dumper to stop
    fn stop(&self) {
        tracing::warn!(target: "state_sync_dump", "Stopping state dumper");
        self.keep_running.store(false, Ordering::Relaxed);
    }

    // Tell the dumper to stop and wait until it's finished
    fn stop_and_await(&self) {
        self.stop();
        let mut running = self.task_running.lock();
        while *running {
            self.await_task.wait(&mut running);
        }
    }

    // Called by the dumper when it's finished, and wakes up any threads waiting on it
    fn task_finished(&self) {
        let mut running = self.task_running.lock();
        *running = false;
        self.await_task.notify_all();
    }
}

pub fn extract_part_id_from_part_file_name(file_name: &String) -> u64 {
    assert!(is_part_filename(file_name));
    return get_part_id_from_filename(file_name).unwrap();
}

async fn get_missing_part_ids_for_epoch(
    shard_id: ShardId,
    chain_id: &str,
    epoch_id: &EpochId,
    epoch_height: u64,
    total_parts: u64,
    external: &ExternalConnection,
) -> Result<HashSet<u64>, anyhow::Error> {
    if total_parts == 0 {
        return Ok(HashSet::new());
    }
    let directory_path = external_storage_location_directory(
        chain_id,
        epoch_id,
        epoch_height,
        shard_id,
        &StateFileType::StatePart { part_id: 0, num_parts: 0 },
    );
    let file_names = external.list_objects(shard_id, &directory_path).await?;
    if !file_names.is_empty() {
        let existing_nums: HashSet<_> = file_names
            .iter()
            .map(|file_name| extract_part_id_from_part_file_name(file_name))
            .collect();
        let missing_nums: HashSet<_> =
            (0..total_parts).filter(|i| !existing_nums.contains(i)).collect();
        let num_missing = missing_nums.len();
        tracing::debug!(target: "state_sync_dump", ?num_missing, ?directory_path, "Some parts have already been dumped.");
        Ok(missing_nums)
    } else {
        tracing::debug!(target: "state_sync_dump", ?total_parts, ?directory_path, "No part has been dumped.");
        let missing_nums = (0..total_parts).collect();
        Ok(missing_nums)
    }
}

// State associated with dumping a shard's state
struct ShardDump {
    state_root: StateRoot,
    // None if it's already been dumped
    header_to_dump: Option<Vec<u8>>,
    num_parts: u64,
    parts_dumped: Arc<AtomicI64>,
    // This is the set of parts who have an associated file stored in the ExternalConnection,
    // meaning they've already been dumped. We periodically check this (since other processes/machines
    // might have uploaded parts that we didn't) and avoid duplicating work for those parts that have already been updated.
    parts_missing: Arc<RwLock<HashSet<u64>>>,
    // This will give Ok(()) when they're all done, or Err() when one gives an error
    // For now the tasks never fail, since we just retry all errors like the old implementation did,
    // but we probably want to make a change to distinguish which errors are actually retryable
    // (e.g. the state snapshot isn't ready yet)
    upload_parts: oneshot::Receiver<anyhow::Result<()>>,
}

// State associated with dumping an epoch's state
struct DumpState {
    epoch_id: EpochId,
    epoch_height: EpochHeight,
    sync_prev_prev_hash: CryptoHash,
    // Contains state for each shard we need to dump. We remove shard IDs from
    // this map as we finish them.
    dump_state: HashMap<ShardId, ShardDump>,
    canceled: Arc<AtomicBool>,
}

impl DumpState {
    /// For each shard, checks the filenames that exist in `external` and sets the corresponding `parts_missing` fields
    /// to contain the parts that haven't yet been uploaded, so that we only try to generate those.
    async fn set_missing_parts(&self, external: &ExternalConnection, chain_id: &str) {
        for (shard_id, s) in &self.dump_state {
            match get_missing_part_ids_for_epoch(
                *shard_id,
                chain_id,
                &self.epoch_id,
                self.epoch_height,
                s.num_parts,
                external,
            )
            .await
            {
                Ok(missing) => {
                    *s.parts_missing.write() = missing;
                }
                Err(error) => {
                    tracing::error!(target: "state_sync_dump", ?error, ?shard_id, "Failed to list stored state parts.");
                }
            }
        }
    }

    /// Waits until all part upload tasks are done for some shard.
    async fn await_parts_upload(&mut self) -> (ShardId, anyhow::Result<()>) {
        let ((shard_id, result), _, _still_going) =
            futures::future::select_all(self.dump_state.iter_mut().map(|(shard_id, s)| {
                async {
                    let r = (&mut s.upload_parts).await.unwrap();
                    (*shard_id, r)
                }
                .boxed()
            }))
            .await;

        drop(_still_going);

        self.dump_state.remove(&shard_id);
        (shard_id, result)
    }

    /// Sets the `canceled` variable to true and waits for all tasks to exit
    async fn cancel(&mut self) {
        self.canceled.store(true, Ordering::Relaxed);
        for (_shard_id, d) in &self.dump_state {
            // Set it to -1 to tell the existing tasks not to set the metrics anymore
            d.parts_dumped.store(-1, Ordering::SeqCst);
        }
        while !self.dump_state.is_empty() {
            let (shard_id, result) = self.await_parts_upload().await;
            if let Err(error) = result {
                tracing::error!(target: "state_sync_dump", epoch_id = ?&self.epoch_id, %shard_id, ?error, "Shard dump failed after cancellation");
            }
        }
    }
}

// Represents the state of the current epoch's state part dump
enum CurrentDump {
    None,
    InProgress(DumpState),
    Done(EpochId),
}

// Helper type used as an intermediate return value where the caller will want the sender only
// if there's something to do
enum NewDump {
    Dump(DumpState, HashMap<ShardId, oneshot::Sender<anyhow::Result<()>>>),
    NoTrackedShards,
}

/// State associated with dumps for all shards responsible for checking when we need to dump for a new epoch
/// The basic flow is as follows:
///
/// At startup or when we enter a new epoch, we initialize the `current_dump` field to represent the current epoch's state dump.
/// Then for each shard that we track and want to dump state for, we'll have one `ShardDump` struct representing it stored in the
/// `DumpState` struct that holds the global state. First we upload headers if they're not already present in the external storage, and
/// then we start the part uploading by calling `start_upload_parts()`. This initializes one `PartUploader` struct for each shard_id and part_id,
/// and spawns a PartUploader::upload_state_part() future for each, that will be responsible for generating and uploading that part if it's not
/// already uploaded. When all the parts for a shard have been uploaded, we'll be notified by the `upload_parts` field of the associated
/// `ShardDump` struct, which we check in `check_parts_upload()`.
///
/// Separately, every so often we check whether there's a new epoch to dump state for (in `check_head()`) and whether other processes
/// have uploaded some state parts that we can therefore skip (in `check_stored_parts()`).
struct StateDumper {
    clock: Clock,
    chain_id: String,
    validator: MutableValidatorSigner,
    shard_tracker: ShardTracker,
    chain: Chain,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    runtime: Arc<dyn RuntimeAdapter>,
    // State associated with dumping the current epoch
    current_dump: CurrentDump,
    external: ExternalConnection,
    future_spawner: Arc<dyn FutureSpawner>,
    // Used to limit how many tasks can be doing the computation-heavy state part generation at a time
    obtain_parts: Arc<Semaphore>,
}

// Stores needed data for use in part upload futures
struct PartUploader {
    clock: Clock,
    external: ExternalConnection,
    runtime: Arc<dyn RuntimeAdapter>,
    chain_id: String,
    epoch_id: EpochId,
    epoch_height: EpochHeight,
    sync_prev_prev_hash: CryptoHash,
    shard_id: ShardId,
    state_root: StateRoot,
    num_parts: u64,
    // Used for setting the num_parts_dumped gauge metric (which is an i64)
    // When part upload tasks are cancelled on a new epoch, this is set to -1 so tasks
    // know not to touch that metric anymore.
    parts_dumped: Arc<AtomicI64>,
    parts_missing: Arc<RwLock<HashSet<u64>>>,
    obtain_parts: Arc<Semaphore>,
    canceled: Arc<AtomicBool>,
}

impl PartUploader {
    /// Increment the STATE_SYNC_DUMP_NUM_PARTS_DUMPED metric
    fn inc_parts_dumped(&self) {
        match self.parts_dumped.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |prev| {
            if prev >= 0 { Some(prev + 1) } else { None }
        }) {
            Ok(prev_parts_dumped) => {
                metrics::STATE_SYNC_DUMP_NUM_PARTS_DUMPED
                    .with_label_values(&[&self.shard_id.to_string()])
                    .set(prev_parts_dumped + 1);
            }
            Err(_) => {}
        };
    }

    /// Attempt to generate the state part for `self.epoch_id`, `self.shard_id` and `part_idx`, and upload it to
    /// the external storage. The state part generation is limited by the number of permits allocated to the `obtain_parts`
    /// Semaphore. For now, this always returns OK(()) (loops forever retrying in case of errors), but this should be changed
    /// to return Err() if the error is not going to be retryable.
    async fn upload_state_part(self: Arc<Self>, part_idx: u64) -> anyhow::Result<()> {
        if !self.parts_missing.read().contains(&part_idx) {
            self.inc_parts_dumped();
            return Ok(());
        }
        let part_id = PartId::new(part_idx, self.num_parts);

        let state_part = loop {
            if self.canceled.load(Ordering::Relaxed) {
                return Ok(());
            }
            let _timer = metrics::STATE_SYNC_DUMP_ITERATION_ELAPSED
                .with_label_values(&[&self.shard_id.to_string()])
                .start_timer();
            let state_part = {
                let _permit = self.obtain_parts.acquire().await.unwrap();
                self.runtime.obtain_state_part(
                    self.shard_id,
                    &self.sync_prev_prev_hash,
                    &self.state_root,
                    part_id,
                )
            };
            match state_part {
                Ok(state_part) => {
                    break state_part;
                }
                Err(error) => {
                    // TODO: return non retryable errors.
                    tracing::warn!(
                        target: "state_sync_dump",
                        shard_id = %self.shard_id, epoch_height=%self.epoch_height, epoch_id=?&self.epoch_id, ?part_id, ?error,
                        "Failed to obtain state part. Retrying in 200 millis."
                    );
                    self.clock.sleep(Duration::milliseconds(200)).await;
                    continue;
                }
            }
        };

        let file_type = StateFileType::StatePart { part_id: part_idx, num_parts: self.num_parts };
        let location = external_storage_location(
            &self.chain_id,
            &self.epoch_id,
            self.epoch_height,
            self.shard_id,
            &file_type,
        );
        loop {
            if self.canceled.load(Ordering::Relaxed) {
                return Ok(());
            }
            match self
                .external
                .put_file(file_type.clone(), &state_part, self.shard_id, &location)
                .await
            {
                Ok(()) => {
                    self.inc_parts_dumped();
                    metrics::STATE_SYNC_DUMP_SIZE_TOTAL
                        .with_label_values(&[
                            &self.epoch_height.to_string(),
                            &self.shard_id.to_string(),
                        ])
                        .inc_by(state_part.len() as u64);
                    tracing::debug!(target: "state_sync_dump", shard_id = %self.shard_id, epoch_height=%self.epoch_height, epoch_id=?&self.epoch_id, ?part_id, "Uploaded state part.");
                    return Ok(());
                }
                Err(error) => {
                    tracing::warn!(
                        target: "state_sync_dump", shard_id = %self.shard_id, epoch_height=%self.epoch_height, epoch_id=?&self.epoch_id, ?part_id, ?error,
                        "Failed to upload state part. Retrying in 200 millis."
                    );
                    self.clock.sleep(Duration::milliseconds(200)).await;
                    continue;
                }
            }
        }
    }

    /// Enumerate all state parts in the shard and spawn a future for each that will obtain and upload it,
    /// then send the result on `sender` when it's done
    async fn dump_shard_state(
        self: Arc<Self>,
        sender: oneshot::Sender<anyhow::Result<()>>,
        future_spawner: Arc<dyn FutureSpawner>,
    ) {
        let mut parts = (0..self.num_parts).collect::<Vec<_>>();
        // We randomize so different nodes uploading parts don't try to upload in the same order
        parts.shuffle(&mut thread_rng());

        let mut tasks = tokio_stream::iter(parts)
            .map(|part_id| {
                let me = self.clone();
                let task = me.upload_state_part(part_id);
                respawn_for_parallelism(&*future_spawner, "upload part", task)
            })
            .buffer_unordered(5);

        while let Some(result) = tasks.next().await {
            if result.is_err() {
                let _ = sender.send(result);
                // Any remaining upload_state_part() tasks will exit when they read the `canceled` variable,
                // and we'll drop anything still left to be started in `tasks`.
                // However if upload_state_part() doesn't return because it's looping retrying an error, we won't finish
                // dumping this shard's state, and the task will stay around until the `canceled` variable is set when
                // the next epoch starts.
                return;
            }
        }
        let _ = sender.send(Ok(()));
    }
}

// Stores needed data for use in header upload futures
struct HeaderUploader {
    clock: Clock,
    external: ExternalConnection,
    chain_id: String,
    epoch_id: EpochId,
    epoch_height: EpochHeight,
}

impl HeaderUploader {
    /// For each shard we're dumping state for, check whether the state sync header is already
    /// stored in the external storage, and set `header_to_dump` to `None` if so, so we don't waste
    /// time uploading it again.
    async fn check_stored_headers(self: Arc<Self>, dump: &mut DumpState) {
        let shards = dump.dump_state.iter().map(|(shard_id, _)| *shard_id).collect::<Vec<_>>();
        tokio_stream::iter(shards)
            .filter_map(|shard_id| {
                self.clone()
                    .header_stored(shard_id)
                    .map(move |stored| stored.then_some(futures::future::ready(shard_id)))
            })
            .buffer_unordered(10)
            .for_each(|shard_id| {
                tracing::info!(
                    target: "state_sync_dump", %shard_id, epoch_height = %dump.epoch_height,
                    "Header already saved to external storage."
                );
                let s = dump.dump_state.get_mut(&shard_id).unwrap();
                s.header_to_dump = None;
                futures::future::ready(())
            })
            .await;
    }

    /// Upload all state sync headers from the given `DumpState`.
    async fn upload_headers(self: Arc<Self>, dump: &mut DumpState) {
        let headers = dump
            .dump_state
            .iter_mut()
            .map(|(shard_id, shard_dump)| (*shard_id, shard_dump.header_to_dump.take()))
            .collect::<Vec<_>>();

        tokio_stream::iter(headers)
            .map(|(shard_id, header)| self.clone().upload_header(shard_id, header))
            .buffer_unordered(10)
            .collect::<()>()
            .await;
    }

    /// Attempt to generate the state header for `self.epoch_id` and `self.shard_id`, and upload it to
    /// the external storage. For now, this always returns OK(()) (loops forever retrying in case of errors),
    /// but this should be changed to return Err() if the error is not going to be retryable.
    async fn upload_header(self: Arc<Self>, shard_id: ShardId, header: Option<Vec<u8>>) {
        let Some(header) = header else {
            return;
        };
        let file_type = StateFileType::StateHeader;
        let location = external_storage_location(
            &self.chain_id,
            &self.epoch_id,
            self.epoch_height,
            shard_id,
            &file_type,
        );
        loop {
            match self.external.put_file(file_type.clone(), &header, shard_id, &location).await {
                Ok(_) => {
                    tracing::info!(
                        target: "state_sync_dump", %shard_id, epoch_height = %self.epoch_height,
                        "Header saved to external storage."
                    );
                    return;
                }
                Err(err) => {
                    tracing::warn!(
                        target: "state_sync_dump", %shard_id, epoch_height = %self.epoch_height, ?err,
                        "Failed to put header into external storage. Will retry next iteration."
                    );
                    self.clock.sleep(Duration::seconds(5)).await;
                    continue;
                }
            };
        }
    }

    /// Returns whether the state sync header for `self.epoch_id` and `self.shard_id` is already uploaded to the
    /// external storage
    async fn header_stored(self: Arc<Self>, shard_id: ShardId) -> bool {
        match self
            .external
            .is_state_sync_header_stored_for_epoch(
                shard_id,
                &self.chain_id,
                &self.epoch_id,
                self.epoch_height,
            )
            .await
        {
            Ok(stored) => stored,
            Err(err) => {
                tracing::error!(target: "state_sync_dump", ?err, ?shard_id, "Failed to determine header presence in external storage.");
                false
            }
        }
    }
}

impl StateDumper {
    fn new(
        clock: Clock,
        chain_id: String,
        validator: MutableValidatorSigner,
        shard_tracker: ShardTracker,
        chain: Chain,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        runtime: Arc<dyn RuntimeAdapter>,
        external: ExternalConnection,
        future_spawner: Arc<dyn FutureSpawner>,
    ) -> Self {
        Self {
            clock,
            chain_id,
            validator,
            shard_tracker,
            chain,
            epoch_manager,
            runtime,
            current_dump: CurrentDump::None,
            external,
            future_spawner,
            obtain_parts: Arc::new(Semaphore::new(4)),
        }
    }

    fn get_block_header(&self, hash: &CryptoHash) -> anyhow::Result<BlockHeader> {
        self.chain.get_block_header(hash).with_context(|| format!("Failed getting header {}", hash))
    }

    /// Reads the DB entries starting with `STATE_SYNC_DUMP_KEY`, and checks which ShardIds and EpochIds are indicated as
    /// already having been fully dumped. For each shard ID whose state for `epoch_id` has already been dumped, we remove it
    /// from `dump` and `senders` so that we don't start the state dump logic for it.
    fn check_old_progress(
        &self,
        epoch_id: &EpochId,
        dump: &mut DumpState,
        senders: &mut HashMap<ShardId, oneshot::Sender<anyhow::Result<()>>>,
    ) -> anyhow::Result<()> {
        for res in self.chain.chain_store().iter_state_sync_dump_progress() {
            let (shard_id, (dumped_epoch_id, done)) =
                res.context("failed iterating over stored dump progress")?;
            if &dumped_epoch_id != epoch_id {
                self.chain
                    .chain_store()
                    .set_state_sync_dump_progress(shard_id, None)
                    .context("failed setting state dump progress")?;
            } else if done {
                dump.dump_state.remove(&shard_id);
                senders.remove(&shard_id);
            }
        }
        Ok(())
    }

    /// Returns the `sync_hash` header corresponding to the latest final block if it's already known.
    fn latest_sync_header(&self) -> anyhow::Result<Option<BlockHeader>> {
        let head = self.chain.head().context("Failed getting chain head")?;
        let header = self.get_block_header(&head.last_block_hash)?;
        let final_hash = header.last_final_block();
        if final_hash == &CryptoHash::default() {
            return Ok(None);
        }
        let Some(sync_hash) = self
            .chain
            .get_sync_hash(final_hash)
            .with_context(|| format!("Failed getting sync hash for {}", &final_hash))?
        else {
            return Ok(None);
        };
        self.get_block_header(&sync_hash).map(Some)
    }

    /// Generates the state sync header for the shard and initializes the `ShardDump` struct which
    /// will be used to keep track of what's been dumped so far for this shard.
    fn get_shard_dump(
        &self,
        shard_id: ShardId,
        sync_hash: &CryptoHash,
    ) -> anyhow::Result<(ShardDump, oneshot::Sender<anyhow::Result<()>>)> {
        let state_header = self
            .chain
            .state_sync_adapter
            .get_state_response_header(shard_id, *sync_hash)
            .with_context(|| {
                format!("Failed getting state response header for {} {}", shard_id, sync_hash)
            })?;
        let state_root = state_header.chunk_prev_state_root();
        let num_parts = state_header.num_state_parts();
        metrics::STATE_SYNC_DUMP_NUM_PARTS_TOTAL
            .with_label_values(&[&shard_id.to_string()])
            .set(num_parts.try_into().unwrap_or(i64::MAX));

        let mut header_bytes: Vec<u8> = Vec::new();
        state_header.serialize(&mut header_bytes)?;
        let (sender, receiver) = oneshot::channel();
        Ok((
            ShardDump {
                state_root,
                header_to_dump: Some(header_bytes),
                num_parts,
                parts_dumped: Arc::new(AtomicI64::new(0)),
                parts_missing: Arc::new(RwLock::new((0..num_parts).collect())),
                upload_parts: receiver,
            },
            sender,
        ))
    }

    /// Initializes a `NewDump` struct, which is a helper return value that either returns `NoTrackedShards`
    /// if we're not tracking anything, or a `DumpState` struct, which holds one `ShardDump` initialized by `get_shard_dump()`
    /// for each shard that we track. This, and the associated oneshot::Senders will then hold all the state related to the
    /// progress of dumping the current epoch's state. This is to be called at startup and also upon each new epoch.
    fn get_dump_state(&self, sync_header: &BlockHeader) -> anyhow::Result<NewDump> {
        let epoch_info = self
            .epoch_manager
            .get_epoch_info(sync_header.epoch_id())
            .with_context(|| format!("Failed getting epoch info {:?}", sync_header.epoch_id()))?;
        let sync_prev_header = self.get_block_header(sync_header.prev_hash())?;
        let sync_prev_prev_hash = *sync_prev_header.prev_hash();
        let shard_ids = self
            .epoch_manager
            .shard_ids(sync_header.epoch_id())
            .with_context(|| format!("Failed getting shard IDs {:?}", sync_header.epoch_id()))?;

        let v = self.validator.get();
        let account_id = v.as_ref().map(|v| v.validator_id());
        let mut dump_state = HashMap::new();
        let mut senders = HashMap::new();
        for shard_id in shard_ids {
            if !self.shard_tracker.cares_about_shard(
                account_id,
                sync_header.prev_hash(),
                shard_id,
                true,
            ) {
                tracing::debug!(
                    target: "state_sync_dump", epoch_height = %epoch_info.epoch_height(), epoch_id = ?sync_header.epoch_id(), %shard_id,
                    "Not dumping state for non-tracked shard."
                );
                continue;
            }
            metrics::STATE_SYNC_DUMP_EPOCH_HEIGHT
                .with_label_values(&[&shard_id.to_string()])
                .set(epoch_info.epoch_height().try_into().unwrap_or(i64::MAX));

            let (shard_dump, sender) = self.get_shard_dump(shard_id, sync_header.hash())?;
            dump_state.insert(shard_id, shard_dump);
            senders.insert(shard_id, sender);
        }
        assert_eq!(
            dump_state.keys().collect::<HashSet<_>>(),
            senders.keys().collect::<HashSet<_>>()
        );
        if dump_state.is_empty() {
            tracing::warn!(
                target: "state_sync_dump", epoch_height = %epoch_info.epoch_height(), epoch_id = ?sync_header.epoch_id(),
                "Not doing anything for the current epoch. No shards tracked."
            );
            return Ok(NewDump::NoTrackedShards);
        }
        Ok(NewDump::Dump(
            DumpState {
                epoch_id: *sync_header.epoch_id(),
                epoch_height: epoch_info.epoch_height(),
                sync_prev_prev_hash,
                dump_state,
                canceled: Arc::new(AtomicBool::new(false)),
            },
            senders,
        ))
    }

    fn header_uploader(&self, dump: &DumpState) -> Arc<HeaderUploader> {
        Arc::new(HeaderUploader {
            clock: self.clock.clone(),
            external: self.external.clone(),
            chain_id: self.chain_id.clone(),
            epoch_id: dump.epoch_id,
            epoch_height: dump.epoch_height,
        })
    }

    /// Start uploading state parts. For each shard we're dumping state for and each state part in that shard, this
    /// starts one PartUploader::upload_state_part() future. It also starts one future that will examine the results
    /// of those futures as they finish, and that will send on `senders` either the first error that occurs or Ok(())
    /// when all parts have been uploaded for the shard.
    fn start_upload_parts(
        &self,
        mut senders: HashMap<ShardId, oneshot::Sender<anyhow::Result<()>>>,
        dump: &DumpState,
    ) {
        debug_assert_eq!(
            senders.keys().collect::<HashSet<_>>(),
            dump.dump_state.keys().collect::<HashSet<_>>()
        );

        // cspell:words uploaders
        let mut uploaders = dump
            .dump_state
            .iter()
            .filter_map(|(shard_id, shard_dump)| {
                metrics::STATE_SYNC_DUMP_NUM_PARTS_DUMPED
                    .with_label_values(&[&shard_id.to_string()])
                    .set(0);

                let sender = senders.remove(shard_id).unwrap();
                if shard_dump.num_parts == 0 {
                    let _ = sender.send(Ok(()));
                    return None;
                }
                let uploader = Arc::new(PartUploader {
                    clock: self.clock.clone(),
                    external: self.external.clone(),
                    runtime: self.runtime.clone(),
                    chain_id: self.chain_id.clone(),
                    epoch_id: dump.epoch_id,
                    epoch_height: dump.epoch_height,
                    sync_prev_prev_hash: dump.sync_prev_prev_hash,
                    shard_id: *shard_id,
                    state_root: shard_dump.state_root,
                    num_parts: shard_dump.num_parts,
                    parts_dumped: shard_dump.parts_dumped.clone(),
                    parts_missing: shard_dump.parts_missing.clone(),
                    obtain_parts: self.obtain_parts.clone(),
                    canceled: dump.canceled.clone(),
                });
                let dump_shard = uploader.dump_shard_state(sender, self.future_spawner.clone());
                Some(dump_shard.boxed())
            })
            .collect::<Vec<_>>();

        let fut = async move {
            while !uploaders.is_empty() {
                let (_output, _idx, remaining) = select_all(uploaders).await;
                uploaders = remaining;
            }
        };
        self.future_spawner.spawn_boxed("upload_parts", fut.boxed());
    }

    /// Sets the in-memory and on-disk state to reflect that we're currently dumping state for a new epoch,
    /// with the info and progress represented in `dump`.
    fn new_dump(&mut self, dump: DumpState, sync_hash: CryptoHash) -> anyhow::Result<()> {
        for (shard_id, _) in &dump.dump_state {
            self.chain
                .chain_store()
                .set_state_sync_dump_progress(
                    *shard_id,
                    Some(StateSyncDumpProgress::InProgress {
                        epoch_id: dump.epoch_id,
                        epoch_height: dump.epoch_height,
                        sync_hash,
                    }),
                )
                .context("failed setting state dump progress")?;
        }
        self.current_dump = CurrentDump::InProgress(dump);
        Ok(())
    }

    // Checks the current epoch and initializes the types associated with dumping its state
    // if it hasn't already been dumped.
    async fn init(&mut self, iteration_delay: Duration) -> anyhow::Result<()> {
        loop {
            let Some(sync_header) = self.latest_sync_header()? else {
                self.clock.sleep(iteration_delay).await;
                continue;
            };
            match self.get_dump_state(&sync_header)? {
                NewDump::Dump(mut dump, mut senders) => {
                    self.check_old_progress(sync_header.epoch_id(), &mut dump, &mut senders)?;
                    if dump.dump_state.is_empty() {
                        self.current_dump = CurrentDump::Done(*sync_header.epoch_id());
                        return Ok(());
                    }

                    self.header_uploader(&dump).check_stored_headers(&mut dump).await;
                    self.header_uploader(&dump).upload_headers(&mut dump).await;

                    dump.set_missing_parts(&self.external, &self.chain_id).await;
                    self.start_upload_parts(senders, &dump);
                    self.new_dump(dump, *sync_header.hash())?;
                }
                NewDump::NoTrackedShards => {
                    self.current_dump = CurrentDump::Done(*sync_header.epoch_id());
                }
            }
            return Ok(());
        }
    }

    // Returns when the part upload tasks are finished
    async fn check_parts_upload(&mut self) -> anyhow::Result<()> {
        let CurrentDump::InProgress(dump) = &mut self.current_dump else {
            return std::future::pending().await;
        };
        let (shard_id, result) = dump.await_parts_upload().await;

        match result {
            Ok(()) => {
                tracing::info!(target: "state_sync_dump", epoch_id = ?&dump.epoch_id, %shard_id, "Shard dump finished");
            }
            Err(error) => {
                tracing::error!(target: "state_sync_dump", epoch_id = ?&dump.epoch_id, %shard_id, ?error, "Shard dump failed");
            }
        }

        self.chain
            .chain_store()
            .set_state_sync_dump_progress(
                shard_id,
                Some(StateSyncDumpProgress::AllDumped {
                    epoch_id: dump.epoch_id,
                    epoch_height: dump.epoch_height,
                }),
            )
            .context("failed setting state dump progress")?;

        if dump.dump_state.is_empty() {
            self.current_dump = CurrentDump::Done(dump.epoch_id);
        }
        Ok(())
    }

    // Checks which parts have already been uploaded possibly by other nodes
    fn check_stored_parts(&self) -> impl Future<Output = ()> + Send {
        let CurrentDump::InProgress(dump) = &self.current_dump else {
            return futures::future::Either::Left(futures::future::ready(()));
        };
        let external = self.external.clone();
        let chain_id = self.chain_id.clone();
        futures::future::Either::Right(async move {
            dump.set_missing_parts(&external, &chain_id).await;
        })
    }

    /// Check whether there's a new epoch to dump state for. In that case, we start dumping
    /// state for the new epoch whether or not we've finished with the old one, since other nodes
    /// will be interested in the latest state.
    async fn check_head(&mut self) -> anyhow::Result<()> {
        let Some(sync_header) = self.latest_sync_header()? else {
            return Ok(());
        };
        match &mut self.current_dump {
            CurrentDump::InProgress(dump) => {
                if &dump.epoch_id == sync_header.epoch_id() {
                    return Ok(());
                }
                tracing::warn!(
                    target: "state_sync_dump", "Canceling existing dump of state for epoch {} upon new epoch {}",
                    &dump.epoch_id.0, &sync_header.epoch_id().0,
                );
                dump.cancel().await;
            }
            CurrentDump::Done(epoch_id) => {
                if epoch_id == sync_header.epoch_id() {
                    return Ok(());
                }
            }
            CurrentDump::None => {}
        };
        match self.get_dump_state(&sync_header)? {
            NewDump::Dump(mut dump, sender) => {
                self.header_uploader(&dump).upload_headers(&mut dump).await;
                self.start_upload_parts(sender, &dump);
                self.new_dump(dump, *sync_header.hash())?;
            }
            NewDump::NoTrackedShards => {
                self.current_dump = CurrentDump::Done(*sync_header.epoch_id());
            }
        };
        Ok(())
    }
}

const CHECK_STORED_PARTS_INTERVAL: Duration = Duration::seconds(20);

/// Main entry point into the state dumper. Initializes the state dumper and starts a loop that periodically
/// checks whether there's a new epoch to dump state for.
async fn state_sync_dump(
    clock: Clock,
    chain: Chain,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    runtime: Arc<dyn RuntimeAdapter>,
    chain_id: String,
    external: ExternalConnection,
    iteration_delay: Duration,
    validator: MutableValidatorSigner,
    keep_running: &AtomicBool,
    future_spawner: Arc<dyn FutureSpawner>,
) -> anyhow::Result<()> {
    tracing::info!(target: "state_sync_dump", "Running StateSyncDump loop");

    let mut dumper = StateDumper::new(
        clock.clone(),
        chain_id,
        validator,
        shard_tracker,
        chain,
        epoch_manager,
        runtime,
        external,
        future_spawner,
    );
    dumper.init(iteration_delay).await?;

    let now = clock.now();
    // This is set to zero in some tests where the block production delay is very small (10 millis).
    // In that case we'll actually just wait for 1 millisecond. The previous behavior was to call
    // clock.sleep(ZERO), but setting it to 1 is probably fine, and works with the Instant below.
    let min_iteration_delay = Duration::milliseconds(1);
    let mut check_head =
        Interval::new(now + iteration_delay, iteration_delay.max(min_iteration_delay));
    let mut check_stored_parts =
        Interval::new(now + CHECK_STORED_PARTS_INTERVAL, CHECK_STORED_PARTS_INTERVAL);

    while keep_running.load(Ordering::Relaxed) {
        tokio::select! {
            _ = check_head.tick(&clock) => {
                dumper.check_head().await?;
            }
            _ = check_stored_parts.tick(&clock) => {
                dumper.check_stored_parts().await;
            }
            result = dumper.check_parts_upload() => {
                result?;
            }
        }
    }

    if let CurrentDump::InProgress(mut dump) = dumper.current_dump {
        tracing::debug!(target: "state_sync_dump", "Awaiting upload task cancellation");
        dump.cancel().await;
    }
    tracing::debug!(target: "state_sync_dump", "Stopped state dump thread");
    Ok(())
}

async fn do_state_sync_dump(
    clock: Clock,
    chain: Chain,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    runtime: Arc<dyn RuntimeAdapter>,
    chain_id: String,
    external: ExternalConnection,
    iteration_delay: Duration,
    validator: MutableValidatorSigner,
    handle: Arc<StateSyncDumpHandle>,
    future_spawner: Arc<dyn FutureSpawner>,
) {
    if let Err(error) = state_sync_dump(
        clock,
        chain,
        epoch_manager,
        shard_tracker,
        runtime,
        chain_id,
        external,
        iteration_delay,
        validator,
        &handle.keep_running,
        future_spawner,
    )
    .await
    {
        tracing::error!(target: "state_sync_dump", ?error, "State dumper failed");
    }
    handle.task_finished();
}
