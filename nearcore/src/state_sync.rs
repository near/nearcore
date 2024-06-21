use crate::metrics;

use actix_rt::Arbiter;
use borsh::BorshSerialize;
use futures::future::BoxFuture;
use futures::FutureExt;
use near_async::time::{Clock, Duration, Instant};
use near_chain::types::RuntimeAdapter;
use near_chain::{Chain, ChainGenesis, ChainStoreAccess, DoomslugThresholdMode, Error};
use near_chain_configs::{ClientConfig, ExternalStorageLocation, MutableConfigValue};
use near_client::sync::external::{
    create_bucket_readwrite, external_storage_location, StateFileType,
};
use near_client::sync::external::{
    external_storage_location_directory, get_part_id_from_filename, is_part_filename,
    ExternalConnection,
};
use near_client::sync::state::{StateSync, STATE_DUMP_ITERATION_TIME_LIMIT_SECS};
use near_epoch_manager::shard_tracker::ShardTracker;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::hash::CryptoHash;
use near_primitives::state_part::PartId;
use near_primitives::state_sync::{StatePartKey, StateSyncDumpProgress};
use near_primitives::types::{AccountId, EpochHeight, EpochId, ShardId, StateRoot};
use near_primitives::validator_signer::ValidatorSigner;
use near_store::DBCol;
use rand::{thread_rng, Rng};
use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

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
    pub validator: MutableConfigValue<Option<Arc<ValidatorSigner>>>,
    pub dump_future_runner: Box<dyn Fn(BoxFuture<'static, ()>) -> Box<dyn FnOnce()>>,
    pub handle: Option<StateSyncDumpHandle>,
}

impl StateSyncDumper {
    /// Starts one a thread per tracked shard.
    /// Each started thread will be dumping state parts of a single epoch to external storage.
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
                bucket: Arc::new(create_bucket_readwrite(&bucket, &region, std::time::Duration::from_secs(30), dump_config.credentials_file).expect(
                    "Failed to authenticate connection to S3. Please either provide AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in the environment, or create a credentials file and link it in config.json as 's3_credentials_file'."))
            },
            ExternalStorageLocation::Filesystem { root_dir } => ExternalConnection::Filesystem { root_dir },
            ExternalStorageLocation::GCS { bucket } => {
                if let Some(credentials_file) = dump_config.credentials_file {
                    if let Ok(var) = std::env::var("SERVICE_ACCOUNT") {
                        tracing::warn!(target: "state_sync_dump", "Environment variable 'SERVICE_ACCOUNT' is set to {var}, but 'credentials_file' in config.json overrides it to '{credentials_file:?}'");
                        println!("Environment variable 'SERVICE_ACCOUNT' is set to {var}, but 'credentials_file' in config.json overrides it to '{credentials_file:?}'");
                    }
                    std::env::set_var("SERVICE_ACCOUNT", &credentials_file);
                    tracing::info!(target: "state_sync_dump", "Set the environment variable 'SERVICE_ACCOUNT' to '{credentials_file:?}'");
                }
                ExternalConnection::GCS {
                    gcs_client: Arc::new(cloud_storage::Client::default()),
                    reqwest_client: Arc::new(reqwest::Client::default()),
                    bucket,
                }
            },
        };

        // Determine how many threads to start.
        // TODO(resharding): Handle the case of changing the shard layout.
        let shard_ids = {
            // Sadly, `Chain` is not `Send` and each thread needs to create its own `Chain` instance.
            let chain = Chain::new_for_view_client(
                self.clock.clone(),
                self.epoch_manager.clone(),
                self.shard_tracker.clone(),
                self.runtime.clone(),
                &self.chain_genesis,
                DoomslugThresholdMode::TwoThirds,
                false,
            )?;
            let epoch_id = chain.head()?.epoch_id;
            self.epoch_manager.shard_ids(&epoch_id)
        }?;

        let chain_id = self.client_config.chain_id.clone();
        let keep_running = Arc::new(AtomicBool::new(true));
        // Start a thread for each shard.
        let handles = shard_ids
            .into_iter()
            .map(|shard_id| {
                let runtime = self.runtime.clone();
                let chain_genesis = self.chain_genesis.clone();
                let chain = Chain::new_for_view_client(
                    self.clock.clone(),
                    self.epoch_manager.clone(),
                    self.shard_tracker.clone(),
                    runtime.clone(),
                    &chain_genesis,
                    DoomslugThresholdMode::TwoThirds,
                    false,
                )
                .unwrap();
                (self.dump_future_runner)(
                    state_sync_dump(
                        self.clock.clone(),
                        shard_id as ShardId,
                        chain,
                        self.epoch_manager.clone(),
                        self.shard_tracker.clone(),
                        runtime.clone(),
                        chain_id.clone(),
                        dump_config.restart_dump_for_shards.clone().unwrap_or_default(),
                        external.clone(),
                        dump_config.iteration_delay.unwrap_or(Duration::seconds(10)),
                        self.validator.clone(),
                        keep_running.clone(),
                    )
                    .boxed(),
                )
            })
            .collect();

        self.handle = Some(StateSyncDumpHandle { handles, keep_running });
        Ok(())
    }

    pub fn arbiter_dump_future_runner() -> Box<dyn Fn(BoxFuture<'static, ()>) -> Box<dyn FnOnce()>>
    {
        Box::new(|future| {
            let arbiter = Arbiter::new();
            assert!(arbiter.spawn(future));
            Box::new(move || {
                arbiter.stop();
            })
        })
    }

    pub fn stop(&mut self) {
        self.handle.take();
    }
}

/// Holds arbiter handles controlling the lifetime of the spawned threads.
pub struct StateSyncDumpHandle {
    pub handles: Vec<Box<dyn FnOnce()>>,
    keep_running: Arc<AtomicBool>,
}

impl Drop for StateSyncDumpHandle {
    fn drop(&mut self) {
        self.stop()
    }
}

impl StateSyncDumpHandle {
    pub fn stop(&mut self) {
        self.keep_running.store(false, std::sync::atomic::Ordering::Relaxed);
        self.handles.drain(..).for_each(|dropper| {
            dropper();
        });
    }
}

/// Fetches the state sync header from DB and serializes it.
fn get_serialized_header(
    shard_id: ShardId,
    sync_hash: CryptoHash,
    chain: &Chain,
) -> anyhow::Result<Vec<u8>> {
    let header = chain.get_state_response_header(shard_id, sync_hash)?;
    let mut buffer: Vec<u8> = Vec::new();
    header.serialize(&mut buffer)?;
    Ok(buffer)
}

pub fn extract_part_id_from_part_file_name(file_name: &String) -> u64 {
    assert!(is_part_filename(file_name));
    return get_part_id_from_filename(file_name).unwrap();
}

async fn get_missing_part_ids_for_epoch(
    shard_id: ShardId,
    chain_id: &String,
    epoch_id: &EpochId,
    epoch_height: u64,
    total_parts: u64,
    external: &ExternalConnection,
) -> Result<Vec<u64>, anyhow::Error> {
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
        let missing_nums: Vec<u64> =
            (0..total_parts).filter(|i| !existing_nums.contains(i)).collect();
        let num_missing = missing_nums.len();
        tracing::debug!(target: "state_sync_dump", ?num_missing, ?directory_path, "Some parts have already been dumped.");
        Ok(missing_nums)
    } else {
        tracing::debug!(target: "state_sync_dump", ?total_parts, ?directory_path, "No part has been dumped.");
        let missing_nums = (0..total_parts).collect::<Vec<_>>();
        Ok(missing_nums)
    }
}

fn select_random_part_id_with_index(parts_to_be_dumped: &Vec<u64>) -> (u64, usize) {
    let mut rng = thread_rng();
    let selected_idx = rng.gen_range(0..parts_to_be_dumped.len());
    let selected_element = parts_to_be_dumped[selected_idx];
    tracing::debug!(target: "state_sync_dump", ?selected_element, "selected parts to dump: ");
    (selected_element, selected_idx)
}

enum StateDumpAction {
    Wait,
    Dump { epoch_id: EpochId, epoch_height: EpochHeight, sync_hash: CryptoHash },
}

fn get_current_state(
    chain: &Chain,
    shard_id: &ShardId,
    shard_tracker: &ShardTracker,
    account_id: &Option<AccountId>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
) -> Result<StateDumpAction, Error> {
    let was_last_epoch_done = match chain.chain_store().get_state_sync_dump_progress(*shard_id) {
        Ok(StateSyncDumpProgress::AllDumped { epoch_id, .. }) => Some(epoch_id),
        Ok(StateSyncDumpProgress::Skipped { epoch_id, .. }) => Some(epoch_id),
        _ => None,
    };

    let latest_epoch_info = get_latest_epoch(shard_id, &chain, epoch_manager.clone());
    let LatestEpochInfo {
        prev_epoch_id,
        epoch_id: new_epoch_id,
        epoch_height: new_epoch_height,
        sync_hash: new_sync_hash,
    } = latest_epoch_info.map_err(|err| {
        tracing::error!(target: "state_sync_dump", shard_id, ?err, "Failed to get the latest epoch");
        err
    })?;

    if Some(&new_epoch_id) == was_last_epoch_done.as_ref() {
        tracing::debug!(target: "state_sync_dump", shard_id, ?was_last_epoch_done, ?new_epoch_id, new_epoch_height, ?new_sync_hash, "latest epoch is done. No new epoch to dump. Idle");
        Ok(StateDumpAction::Wait)
    } else if epoch_manager.get_shard_layout(&prev_epoch_id)
        != epoch_manager.get_shard_layout(&new_epoch_id)
    {
        tracing::debug!(target: "state_sync_dump", shard_id, ?was_last_epoch_done, ?new_epoch_id, new_epoch_height, ?new_sync_hash, "Shard layout change detected, will skip dumping for this epoch. Idle");
        chain.chain_store().set_state_sync_dump_progress(
            *shard_id,
            Some(StateSyncDumpProgress::Skipped {
                epoch_id: new_epoch_id,
                epoch_height: new_epoch_height,
            }),
        )?;
        Ok(StateDumpAction::Wait)
    } else if cares_about_shard(chain, shard_id, &new_sync_hash, &shard_tracker, &account_id)? {
        Ok(StateDumpAction::Dump {
            epoch_id: new_epoch_id,
            epoch_height: new_epoch_height,
            sync_hash: new_sync_hash,
        })
    } else {
        tracing::debug!(target: "state_sync_dump", shard_id, ?new_epoch_id, new_epoch_height, ?new_sync_hash, "Doesn't care about the shard in the current epoch. Idle");
        Ok(StateDumpAction::Wait)
    }
}

/// Uploads header to external storage.
/// Returns true if it was successful.
async fn upload_state_header(
    chain_id: &String,
    epoch_id: &EpochId,
    epoch_height: u64,
    shard_id: ShardId,
    state_sync_header: anyhow::Result<Vec<u8>>,
    external: &ExternalConnection,
) -> bool {
    match state_sync_header {
        Err(err) => {
            tracing::error!(target: "state_sync_dump", ?err, ?shard_id, "Failed to serialize header.");
            false
        }
        Ok(header) => {
            let file_type = StateFileType::StateHeader;
            let location =
                external_storage_location(&chain_id, &epoch_id, epoch_height, shard_id, &file_type);
            match external.put_file(file_type, &header, shard_id, &location).await {
                Err(err) => {
                    tracing::warn!(target: "state_sync_dump", shard_id, epoch_height, ?err, "Failed to put header into external storage. Will retry next iteration.");
                    false
                }
                Ok(_) => {
                    tracing::trace!(target: "state_sync_dump", shard_id, epoch_height, "Header saved to external storage.");
                    true
                }
            }
        }
    }
}

const FAILURES_ALLOWED_PER_ITERATION: u32 = 10;

async fn state_sync_dump(
    clock: Clock,
    shard_id: ShardId,
    chain: Chain,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    runtime: Arc<dyn RuntimeAdapter>,
    chain_id: String,
    restart_dump_for_shards: Vec<ShardId>,
    external: ExternalConnection,
    iteration_delay: Duration,
    validator: MutableConfigValue<Option<Arc<ValidatorSigner>>>,
    keep_running: Arc<AtomicBool>,
) {
    tracing::info!(target: "state_sync_dump", shard_id, "Running StateSyncDump loop");

    if restart_dump_for_shards.contains(&shard_id) {
        tracing::debug!(target: "state_sync_dump", shard_id, "Dropped existing progress");
        chain.chain_store().set_state_sync_dump_progress(shard_id, None).unwrap();
    }

    // Stop if the node is stopped.
    // Note that without this check the state dumping thread is unstoppable, i.e. non-interruptable.
    while keep_running.load(std::sync::atomic::Ordering::Relaxed) {
        tracing::debug!(target: "state_sync_dump", shard_id, "Running StateSyncDump loop iteration");
        let account_id = validator.get().map(|v| v.validator_id().clone());
        let current_state = get_current_state(
            &chain,
            &shard_id,
            &shard_tracker,
            &account_id,
            epoch_manager.clone(),
        );
        let next_state = match current_state {
            Err(err) => {
                tracing::error!(target: "state_sync_dump", ?err, ?shard_id, "Failed to get the current state");
                None
            }
            Ok(StateDumpAction::Wait) => None,
            Ok(StateDumpAction::Dump { epoch_id, epoch_height, sync_hash }) => {
                let in_progress_data = get_in_progress_data(shard_id, sync_hash, &chain);
                match in_progress_data {
                    Err(err) => {
                        tracing::error!(target: "state_sync_dump", ?err, ? shard_id, "Failed to get in progress data");
                        None
                    }
                    Ok((state_root, num_parts, sync_prev_prev_hash)) => {
                        // Upload header
                        let header_in_external_storage = match external
                            .is_state_sync_header_stored_for_epoch(
                                shard_id,
                                &chain_id,
                                &epoch_id,
                                epoch_height,
                            )
                            .await
                        {
                            Err(err) => {
                                tracing::error!(target: "state_sync_dump", ?err, ?shard_id, "Failed to determine header presence in external storage.");
                                false
                            }
                            // Header is already stored
                            Ok(true) => true,
                            // Header is missing
                            Ok(false) => {
                                upload_state_header(
                                    &chain_id,
                                    &epoch_id,
                                    epoch_height,
                                    shard_id,
                                    get_serialized_header(shard_id, sync_hash, &chain),
                                    &external,
                                )
                                .await
                            }
                        };

                        let header_upload_status = if header_in_external_storage {
                            None
                        } else {
                            Some(StateSyncDumpProgress::InProgress {
                                epoch_id: epoch_id.clone(),
                                epoch_height,
                                sync_hash,
                            })
                        };

                        // Upload parts
                        let parts_upload_status = match get_missing_part_ids_for_epoch(
                            shard_id,
                            &chain_id,
                            &epoch_id,
                            epoch_height,
                            num_parts,
                            &external,
                        )
                        .await
                        {
                            Err(err) => {
                                tracing::error!(target: "state_sync_dump", ?err, ?shard_id, "Failed to determine missing parts");
                                None
                            }
                            Ok(missing_parts) if missing_parts.is_empty() => {
                                update_dumped_size_and_cnt_metrics(
                                    &shard_id,
                                    epoch_height,
                                    None,
                                    num_parts,
                                    num_parts,
                                );
                                Some(StateSyncDumpProgress::AllDumped { epoch_id, epoch_height })
                            }
                            Ok(missing_parts) => {
                                let mut parts_to_dump = missing_parts.clone();
                                let timer = Instant::now();
                                let mut dumped_any_state_part = false;
                                let mut failures_cnt = 0;
                                // Stop if the node is stopped.
                                // Note that without this check the state dumping thread is unstoppable, i.e. non-interruptable.
                                while keep_running.load(std::sync::atomic::Ordering::Relaxed)
                                    && timer.elapsed().as_secs()
                                        <= STATE_DUMP_ITERATION_TIME_LIMIT_SECS
                                    && !parts_to_dump.is_empty()
                                    && failures_cnt < FAILURES_ALLOWED_PER_ITERATION
                                {
                                    let _timer = metrics::STATE_SYNC_DUMP_ITERATION_ELAPSED
                                        .with_label_values(&[&shard_id.to_string()])
                                        .start_timer();

                                    let (part_id, selected_idx) =
                                        select_random_part_id_with_index(&parts_to_dump);

                                    let state_part = obtain_and_store_state_part(
                                        runtime.as_ref(),
                                        shard_id,
                                        sync_hash,
                                        &sync_prev_prev_hash,
                                        &state_root,
                                        part_id,
                                        num_parts,
                                        &chain,
                                    );
                                    let state_part = match state_part {
                                        Ok(state_part) => state_part,
                                        Err(err) => {
                                            tracing::warn!(target: "state_sync_dump", shard_id, epoch_height, part_id, ?err, "Failed to obtain and store part. Will skip this part.");
                                            failures_cnt += 1;
                                            continue;
                                        }
                                    };

                                    let file_type = StateFileType::StatePart { part_id, num_parts };
                                    let location = external_storage_location(
                                        &chain_id,
                                        &epoch_id,
                                        epoch_height,
                                        shard_id,
                                        &file_type,
                                    );
                                    if let Err(err) = external
                                        .put_file(file_type, &state_part, shard_id, &location)
                                        .await
                                    {
                                        // no need to break if there's an error, we should keep dumping other parts.
                                        // reason is we are dumping random selected parts, so it's fine if we are not able to finish all of them
                                        tracing::warn!(target: "state_sync_dump", shard_id, epoch_height, part_id, ?err, "Failed to put a store part into external storage. Will skip this part.");
                                        failures_cnt += 1;
                                        continue;
                                    }

                                    // Remove the dumped part from parts_to_dump so that we draw without replacement.
                                    parts_to_dump.swap_remove(selected_idx);
                                    update_dumped_size_and_cnt_metrics(
                                        &shard_id,
                                        epoch_height,
                                        Some(state_part.len()),
                                        num_parts.checked_sub(parts_to_dump.len() as u64).unwrap(),
                                        num_parts,
                                    );
                                    dumped_any_state_part = true;
                                }
                                if parts_to_dump.is_empty() {
                                    Some(StateSyncDumpProgress::AllDumped {
                                        epoch_id,
                                        epoch_height,
                                    })
                                } else if dumped_any_state_part {
                                    Some(StateSyncDumpProgress::InProgress {
                                        epoch_id,
                                        epoch_height,
                                        sync_hash,
                                    })
                                } else {
                                    // No progress made. Wait before retrying.
                                    None
                                }
                            }
                        };
                        match (&parts_upload_status, &header_upload_status) {
                            (
                                Some(StateSyncDumpProgress::AllDumped { .. }),
                                Some(StateSyncDumpProgress::InProgress { .. }),
                            ) => header_upload_status,
                            _ => parts_upload_status,
                        }
                    }
                }
            }
        };

        // Record the next state of the state machine.
        let has_progress = match next_state {
            Some(next_state) => {
                tracing::debug!(target: "state_sync_dump", shard_id, ?next_state);
                match chain.chain_store().set_state_sync_dump_progress(shard_id, Some(next_state)) {
                    Ok(_) => true,
                    Err(err) => {
                        // This will be retried.
                        tracing::debug!(target: "state_sync_dump", shard_id, ?err, "Failed to set progress");
                        false
                    }
                }
            }
            None => {
                // Nothing to do, will check again later.
                tracing::debug!(target: "state_sync_dump", shard_id, "Idle");
                false
            }
        };

        if !has_progress {
            // Avoid a busy-loop when there is nothing to do.
            clock.sleep(iteration_delay).await;
        }
    }
    tracing::debug!(target: "state_sync_dump", shard_id, "Stopped state dump thread");
}

// Extracts extra data needed for obtaining state parts.
fn get_in_progress_data(
    shard_id: ShardId,
    sync_hash: CryptoHash,
    chain: &Chain,
) -> Result<(StateRoot, u64, CryptoHash), Error> {
    let state_header = chain.get_state_response_header(shard_id, sync_hash)?;
    let state_root = state_header.chunk_prev_state_root();
    let num_parts = state_header.num_state_parts();

    let sync_block_header = chain.get_block_header(&sync_hash)?;
    let sync_prev_block_header = chain.get_previous_header(&sync_block_header)?;
    let sync_prev_prev_hash = sync_prev_block_header.prev_hash();
    Ok((state_root, num_parts, *sync_prev_prev_hash))
}

fn update_dumped_size_and_cnt_metrics(
    shard_id: &ShardId,
    epoch_height: EpochHeight,
    part_len: Option<usize>,
    parts_dumped: u64,
    num_parts: u64,
) {
    if let Some(part_len) = part_len {
        metrics::STATE_SYNC_DUMP_SIZE_TOTAL
            .with_label_values(&[&epoch_height.to_string(), &shard_id.to_string()])
            .inc_by(part_len as u64);
    }

    metrics::STATE_SYNC_DUMP_EPOCH_HEIGHT
        .with_label_values(&[&shard_id.to_string()])
        .set(epoch_height as i64);

    metrics::STATE_SYNC_DUMP_NUM_PARTS_DUMPED
        .with_label_values(&[&shard_id.to_string()])
        .set(parts_dumped as i64);

    metrics::STATE_SYNC_DUMP_NUM_PARTS_TOTAL
        .with_label_values(&[&shard_id.to_string()])
        .set(num_parts as i64);
}

/// Obtains and then saves the part data.
fn obtain_and_store_state_part(
    runtime: &dyn RuntimeAdapter,
    shard_id: ShardId,
    sync_hash: CryptoHash,
    sync_prev_prev_hash: &CryptoHash,
    state_root: &StateRoot,
    part_id: u64,
    num_parts: u64,
    chain: &Chain,
) -> Result<Vec<u8>, Error> {
    let state_part = runtime.obtain_state_part(
        shard_id,
        sync_prev_prev_hash,
        state_root,
        PartId::new(part_id, num_parts),
    )?;

    let key = borsh::to_vec(&StatePartKey(sync_hash, shard_id, part_id))?;
    let mut store_update = chain.chain_store().store().store_update();
    store_update.set(DBCol::StateParts, &key, &state_part);
    store_update.commit()?;
    Ok(state_part)
}

fn cares_about_shard(
    chain: &Chain,
    shard_id: &ShardId,
    sync_hash: &CryptoHash,
    shard_tracker: &ShardTracker,
    account_id: &Option<AccountId>,
) -> Result<bool, Error> {
    let sync_header = chain.get_block_header(&sync_hash)?;
    let sync_prev_hash = sync_header.prev_hash();
    Ok(shard_tracker.care_about_shard(account_id.as_ref(), sync_prev_hash, *shard_id, true))
}

struct LatestEpochInfo {
    prev_epoch_id: EpochId,
    epoch_id: EpochId,
    epoch_height: EpochHeight,
    sync_hash: CryptoHash,
}

/// return epoch_id and sync_hash of the latest complete epoch available locally.
fn get_latest_epoch(
    shard_id: &ShardId,
    chain: &Chain,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
) -> Result<LatestEpochInfo, Error> {
    let head = chain.head()?;
    tracing::debug!(target: "state_sync_dump", shard_id, "Check if a new complete epoch is available");
    let hash = head.last_block_hash;
    let header = chain.get_block_header(&hash)?;
    let final_hash = header.last_final_block();
    let sync_hash = StateSync::get_epoch_start_sync_hash(chain, final_hash)?;
    let final_block_header = chain.get_block_header(&final_hash)?;
    let epoch_id = final_block_header.epoch_id().clone();
    let epoch_info = epoch_manager.get_epoch_info(&epoch_id)?;
    let prev_epoch_id = epoch_manager.get_prev_epoch_id_from_prev_block(&head.prev_block_hash)?;
    let epoch_height = epoch_info.epoch_height();
    tracing::debug!(target: "state_sync_dump", ?final_hash, ?sync_hash, ?epoch_id, epoch_height, "get_latest_epoch");

    Ok(LatestEpochInfo { prev_epoch_id, epoch_id, epoch_height, sync_hash })
}
