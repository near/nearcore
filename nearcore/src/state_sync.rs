use crate::metrics;
use borsh::BorshSerialize;
use near_chain::types::RuntimeAdapter;
use near_chain::{Chain, ChainGenesis, ChainStoreAccess, DoomslugThresholdMode, Error};
use near_chain_configs::{ClientConfig, ExternalStorageLocation};
use near_client::sync::state::{
    external_storage_location, external_storage_location_directory, get_part_id_from_filename,
    is_part_filename, ExternalConnection, StateSync, STATE_DUMP_ITERATION_TIME_LIMIT_SECS,
};
use near_epoch_manager::shard_tracker::ShardTracker;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::hash::CryptoHash;
use near_primitives::state_part::PartId;
use near_primitives::syncing::{get_num_state_parts, StatePartKey, StateSyncDumpProgress};
use near_primitives::types::{AccountId, EpochHeight, EpochId, ShardId, StateRoot};
use near_store::DBCol;
use rand::{thread_rng, Rng};
use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Starts one a thread per tracked shard.
/// Each started thread will be dumping state parts of a single epoch to external storage.
pub fn spawn_state_sync_dump(
    client_config: &ClientConfig,
    chain_genesis: ChainGenesis,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    runtime: Arc<dyn RuntimeAdapter>,
    account_id: Option<AccountId>,
) -> anyhow::Result<Option<StateSyncDumpHandle>> {
    let dump_config = if let Some(dump_config) = client_config.state_sync.dump.clone() {
        dump_config
    } else {
        // Dump is not configured, and therefore not enabled.
        tracing::debug!(target: "state_sync_dump", "Not spawning the state sync dump loop");
        return Ok(None);
    };
    tracing::info!(target: "state_sync_dump", "Spawning the state sync dump loop");

    let external = match dump_config.location {
        ExternalStorageLocation::S3 { bucket, region } => {
            // Credentials to establish a connection are taken from environment variables:
            // * `AWS_ACCESS_KEY_ID`
            // * `AWS_SECRET_ACCESS_KEY`
            let creds = match s3::creds::Credentials::default() {
                Ok(creds) => creds,
                Err(err) => {
                    tracing::error!(target: "state_sync_dump", "Failed to create a connection to S3. Did you provide environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY?");
                    return Err(err.into());
                }
            };
            let bucket = s3::Bucket::new(&bucket, region.parse::<s3::Region>()?, creds)?;
            ExternalConnection::S3 { bucket: Arc::new(bucket) }
        }
        ExternalStorageLocation::Filesystem { root_dir } => {
            ExternalConnection::Filesystem { root_dir }
        }
    };

    // Determine how many threads to start.
    // TODO: Handle the case of changing the shard layout.
    let num_shards = {
        // Sadly, `Chain` is not `Send` and each thread needs to create its own `Chain` instance.
        let chain = Chain::new_for_view_client(
            epoch_manager.clone(),
            shard_tracker.clone(),
            runtime.clone(),
            &chain_genesis,
            DoomslugThresholdMode::TwoThirds,
            false,
        )?;
        let epoch_id = chain.head()?.epoch_id;
        epoch_manager.num_shards(&epoch_id)
    }?;

    let chain_id = client_config.chain_id.clone();
    let keep_running = Arc::new(AtomicBool::new(true));
    // Start a thread for each shard.
    let handles = (0..num_shards as usize)
        .map(|shard_id| {
            let runtime = runtime.clone();
            let chain_genesis = chain_genesis.clone();
            let chain = Chain::new_for_view_client(
                epoch_manager.clone(),
                shard_tracker.clone(),
                runtime.clone(),
                &chain_genesis,
                DoomslugThresholdMode::TwoThirds,
                false,
            )
            .unwrap();
            let arbiter_handle = actix_rt::Arbiter::new().handle();
            assert!(arbiter_handle.spawn(state_sync_dump(
                shard_id as ShardId,
                chain,
                epoch_manager.clone(),
                shard_tracker.clone(),
                runtime.clone(),
                chain_id.clone(),
                dump_config.restart_dump_for_shards.clone().unwrap_or_default(),
                external.clone(),
                dump_config.iteration_delay.unwrap_or(Duration::from_secs(10)),
                account_id.clone(),
                keep_running.clone(),
            )));
            arbiter_handle
        })
        .collect();

    Ok(Some(StateSyncDumpHandle { handles, keep_running }))
}

/// Holds arbiter handles controlling the lifetime of the spawned threads.
pub struct StateSyncDumpHandle {
    pub handles: Vec<actix_rt::ArbiterHandle>,
    keep_running: Arc<AtomicBool>,
}

impl Drop for StateSyncDumpHandle {
    fn drop(&mut self) {
        self.stop()
    }
}

impl StateSyncDumpHandle {
    pub fn stop(&self) {
        self.keep_running.store(false, std::sync::atomic::Ordering::Relaxed);
        self.handles.iter().for_each(|handle| {
            handle.stop();
        });
    }
}

fn extract_part_id_from_part_file_name(file_name: &String) -> u64 {
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
    let directory_path =
        external_storage_location_directory(chain_id, epoch_id, epoch_height, shard_id);
    let file_names = external.list_state_parts(shard_id, &directory_path).await?;
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

async fn state_sync_dump(
    shard_id: ShardId,
    chain: Chain,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    runtime: Arc<dyn RuntimeAdapter>,
    chain_id: String,
    restart_dump_for_shards: Vec<ShardId>,
    external: ExternalConnection,
    iteration_delay: Duration,
    account_id: Option<AccountId>,
    keep_running: Arc<AtomicBool>,
) {
    tracing::info!(target: "state_sync_dump", shard_id, "Running StateSyncDump loop");

    if restart_dump_for_shards.contains(&shard_id) {
        tracing::debug!(target: "state_sync_dump", shard_id, "Dropped existing progress");
        chain.store().set_state_sync_dump_progress(shard_id, None).unwrap();
    }

    // Stop if the node is stopped.
    // Note that without this check the state dumping thread is unstoppable, i.e. non-interruptable.
    while keep_running.load(std::sync::atomic::Ordering::Relaxed) {
        tracing::debug!(target: "state_sync_dump", shard_id, "Running StateSyncDump loop iteration");
        let latest_epoch_info = get_latest_epoch(shard_id, &chain);
        let progress = chain.store().get_state_sync_dump_progress(shard_id);
        let current_state = match (progress, latest_epoch_info) {
            (_, Err(err)) => {
                tracing::debug!(target: "state_sync_dump", shard_id, ?err, "check_latest_epoch failed. Will retry.");
                Err(err)
            }
            (last_progress, Ok((new_epoch_id, new_sync_hash))) => match last_progress {
                Err(Error::DBNotFoundErr(_)) | Ok(None) => {
                    tracing::debug!(target: "state_sync_dump", shard_id, ?new_epoch_id, "Starting the first epoch to be dumped.");
                    start_dumping(
                        new_epoch_id,
                        new_sync_hash,
                        shard_id,
                        &chain,
                        epoch_manager.as_ref(),
                        &shard_tracker,
                        &account_id,
                    )
                }
                Ok(Some(StateSyncDumpProgress::InProgress {
                    epoch_id: last_epoch_id,
                    epoch_height: last_epoch_height,
                    sync_hash: last_sync_hash,
                })) => {
                    if new_epoch_id == last_epoch_id {
                        tracing::debug!(target: "state_sync_dump", shard_id, ?last_epoch_id, last_epoch_height, "Still dumping for the same epoch.");
                        Ok(Some(StateSyncDumpProgress::InProgress {
                            epoch_id: last_epoch_id,
                            epoch_height: last_epoch_height,
                            sync_hash: last_sync_hash,
                        }))
                    } else {
                        tracing::debug!(target: "state_sync_dump", shard_id, ?new_epoch_id, ?last_epoch_id, last_epoch_height, "Last epoch has not finished, but new epoch is available. Start dump for new epoch.");
                        start_dumping(
                            new_epoch_id,
                            new_sync_hash,
                            shard_id,
                            &chain,
                            epoch_manager.as_ref(),
                            &shard_tracker,
                            &account_id,
                        )
                    }
                }
                Ok(Some(StateSyncDumpProgress::AllDumped {
                    epoch_id: last_epoch_id,
                    epoch_height: last_epoch_height,
                })) => {
                    if new_epoch_id == last_epoch_id {
                        tracing::debug!(target: "state_sync_dump", shard_id, ?last_epoch_id, last_epoch_height, "latest epoch is all dumped. No new epoch to dump. Idle");
                        Ok(None)
                    } else {
                        tracing::debug!(target: "state_sync_dump", shard_id, ?last_epoch_id, last_epoch_height, ?new_epoch_id,  "last epoch is all dumped. Starting dump for a new epoch.");
                        start_dumping(
                            new_epoch_id,
                            new_sync_hash,
                            shard_id,
                            &chain,
                            epoch_manager.as_ref(),
                            &shard_tracker,
                            &account_id,
                        )
                    }
                }
                _ => Ok(None),
            },
        };

        let next_state: Result<Option<StateSyncDumpProgress>, Error> = match current_state {
            Err(err) => Err(err),
            Ok(Some(StateSyncDumpProgress::InProgress { epoch_id, epoch_height, sync_hash })) => {
                let in_progress_data = get_in_progress_data(shard_id, sync_hash, &chain);
                match in_progress_data {
                    Err(error) => Err(error),
                    Ok((state_root, num_parts, sync_prev_prev_hash)) => {
                        let missing_parts = get_missing_part_ids_for_epoch(
                            shard_id,
                            &chain_id,
                            &epoch_id,
                            epoch_height,
                            num_parts,
                            &external,
                        )
                        .await;

                        match missing_parts {
                            Err(err) => {
                                tracing::debug!(target: "state_sync_dump", shard_id, ?err, "get_missing_state_parts_for_epoch error");
                                Err(Error::Other(
                                    "get_missing_state_parts_for_epoch failed".to_string(),
                                ))
                            }
                            Ok(parts_not_dumped) if parts_not_dumped.is_empty() => {
                                Ok(Some(StateSyncDumpProgress::AllDumped {
                                    epoch_id,
                                    epoch_height,
                                }))
                            }
                            Ok(parts_not_dumped) => {
                                let mut parts_to_dump = parts_not_dumped.clone();
                                let timer = Instant::now();
                                // Stop if the node is stopped.
                                // Note that without this check the state dumping thread is unstoppable, i.e. non-interruptable.
                                while keep_running.load(std::sync::atomic::Ordering::Relaxed)
                                    && timer.elapsed().as_secs()
                                        <= STATE_DUMP_ITERATION_TIME_LIMIT_SECS
                                    && !parts_to_dump.is_empty()
                                {
                                    let _timer = metrics::STATE_SYNC_DUMP_ITERATION_ELAPSED
                                        .with_label_values(&[&shard_id.to_string()])
                                        .start_timer();

                                    let (part_id, selected_idx) =
                                        select_random_part_id_with_index(&parts_to_dump);

                                    let state_part = match obtain_and_store_state_part(
                                        runtime.as_ref(),
                                        shard_id,
                                        sync_hash,
                                        &sync_prev_prev_hash,
                                        &state_root,
                                        part_id,
                                        num_parts,
                                        &chain,
                                    ) {
                                        Ok(state_part) => state_part,
                                        Err(err) => {
                                            tracing::warn!(target: "state_sync_dump", shard_id, epoch_height, part_id, ?err, "Failed to obtain and store part. Will skip this part.");
                                            break;
                                        }
                                    };
                                    let location = external_storage_location(
                                        &chain_id,
                                        &epoch_id,
                                        epoch_height,
                                        shard_id,
                                        part_id,
                                        num_parts,
                                    );
                                    if let Err(_) = external
                                        .put_state_part(&state_part, shard_id, &location)
                                        .await
                                    {
                                        // no need to break if there's an error, we should keep dumping other parts.
                                        // reason is we are dumping random selected parts, so it's fine if we are not able to finish all of them
                                        continue;
                                    }

                                    // remove the dumped part from parts_to_dump so that we draw without replacement
                                    parts_to_dump.swap_remove(selected_idx);
                                    update_dumped_size_and_cnt_metrics(
                                        &shard_id,
                                        epoch_height,
                                        state_part.len(),
                                    );
                                }
                                if parts_to_dump.is_empty() {
                                    Ok(Some(StateSyncDumpProgress::AllDumped {
                                        epoch_id,
                                        epoch_height,
                                    }))
                                } else {
                                    Ok(Some(StateSyncDumpProgress::InProgress {
                                        epoch_id,
                                        epoch_height,
                                        sync_hash,
                                    }))
                                }
                            }
                        }
                    }
                }
            }
            _ => Ok(None),
        };

        // Record the next state of the state machine.
        let has_progress = match next_state {
            Ok(Some(next_state)) => {
                tracing::debug!(target: "state_sync_dump", shard_id, ?next_state);
                match chain.store().set_state_sync_dump_progress(shard_id, Some(next_state)) {
                    Ok(_) => true,
                    Err(err) => {
                        // This will be retried.
                        tracing::debug!(target: "state_sync_dump", shard_id, ?err, "Failed to set progress");
                        false
                    }
                }
            }
            Ok(None) => {
                // Will retry.
                tracing::debug!(target: "state_sync_dump", shard_id, "Idle");
                false
            }
            Err(err) => {
                // Will retry.
                tracing::debug!(target: "state_sync_dump", shard_id, ?err, "Failed to determine what to do");
                false
            }
        };

        if !has_progress {
            // Avoid a busy-loop when there is nothing to do.
            actix_rt::time::sleep(tokio::time::Duration::from(iteration_delay)).await;
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
    let num_parts = get_num_state_parts(state_header.state_root_node().memory_usage);

    let sync_block_header = chain.get_block_header(&sync_hash)?;
    let sync_prev_block_header = chain.get_previous_header(&sync_block_header)?;
    let sync_prev_prev_hash = sync_prev_block_header.prev_hash();
    Ok((state_root, num_parts, *sync_prev_prev_hash))
}

fn update_dumped_size_and_cnt_metrics(
    shard_id: &ShardId,
    epoch_height: EpochHeight,
    part_len: usize,
) {
    metrics::STATE_SYNC_DUMP_SIZE_TOTAL
        .with_label_values(&[&epoch_height.to_string(), &shard_id.to_string()])
        .inc_by(part_len as u64);

    metrics::STATE_SYNC_DUMP_NUM_PARTS_DUMPED.with_label_values(&[&shard_id.to_string()]).inc();
}

fn set_metrics(
    shard_id: &ShardId,
    parts_dumped: Option<u64>,
    num_parts: Option<u64>,
    epoch_height: Option<EpochHeight>,
) {
    if let Some(parts_dumped) = parts_dumped {
        metrics::STATE_SYNC_DUMP_NUM_PARTS_DUMPED
            .with_label_values(&[&shard_id.to_string()])
            .set(parts_dumped as i64);
    }
    if let Some(num_parts) = num_parts {
        metrics::STATE_SYNC_DUMP_NUM_PARTS_TOTAL
            .with_label_values(&[&shard_id.to_string()])
            .set(num_parts as i64);
    }
    if let Some(epoch_height) = epoch_height {
        assert!(
            epoch_height < 10000,
            "Impossible: {:?} {:?} {:?} {:?}",
            shard_id,
            parts_dumped,
            num_parts,
            epoch_height
        );
        metrics::STATE_SYNC_DUMP_EPOCH_HEIGHT
            .with_label_values(&[&shard_id.to_string()])
            .set(epoch_height as i64);
    }
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

    let key = StatePartKey(sync_hash, shard_id, part_id).try_to_vec()?;
    let mut store_update = chain.store().store().store_update();
    store_update.set(DBCol::StateParts, &key, &state_part);
    store_update.commit()?;
    Ok(state_part)
}

/// Gets basic information about the epoch to be dumped.
fn start_dumping(
    epoch_id: EpochId,
    sync_hash: CryptoHash,
    shard_id: ShardId,
    chain: &Chain,
    epoch_manager: &dyn EpochManagerAdapter,
    shard_tracker: &ShardTracker,
    account_id: &Option<AccountId>,
) -> Result<Option<StateSyncDumpProgress>, Error> {
    let epoch_info = epoch_manager.get_epoch_info(&epoch_id)?;
    let epoch_height = epoch_info.epoch_height();

    let sync_header = chain.get_block_header(&sync_hash)?;
    let sync_prev_hash = sync_header.prev_hash();
    let sync_prev_header = chain.get_block_header(&sync_prev_hash)?;
    // Need to check if the completed epoch had a shard this account cares about.
    // sync_hash is the first block of the next epoch.
    // `cares_about_shard()` accepts `parent_hash`, therefore we need prev-prev-hash,
    // and its next-hash will be prev-hash. That is the last block of the completed epoch,
    // which is what we wanted.
    let sync_prev_prev_hash = sync_prev_header.prev_hash();

    let state_header = chain.get_state_response_header(shard_id, sync_hash)?;
    let num_parts = get_num_state_parts(state_header.state_root_node().memory_usage);
    if shard_tracker.care_about_shard(account_id.as_ref(), sync_prev_prev_hash, shard_id, true) {
        tracing::info!(target: "state_sync_dump", shard_id, ?epoch_id, epoch_height, %sync_prev_hash, %sync_hash, "Dumping state of Epoch");
        // Note that first the state of the state machines gets changes to
        // `InProgress` and it starts dumping state after a short interval.
        set_metrics(&shard_id, Some(0), Some(num_parts), Some(epoch_height));
        Ok(Some(StateSyncDumpProgress::InProgress { epoch_id, epoch_height, sync_hash }))
    } else {
        tracing::info!(target: "state_sync_dump", shard_id, ?epoch_id, epoch_height, %sync_hash, "Shard is not tracked, skip the epoch");
        Ok(Some(StateSyncDumpProgress::AllDumped { epoch_id, epoch_height }))
    }
}

/// return epoch_id and sync_hash of the latest complete epoch available locally.
fn get_latest_epoch(shard_id: ShardId, chain: &Chain) -> Result<(EpochId, CryptoHash), Error> {
    let head = chain.head()?;
    tracing::debug!(target: "state_sync_dump", shard_id, "Check if a new complete epoch is available");
    let hash = head.last_block_hash;
    let header = chain.get_block_header(&hash)?;
    let final_hash = header.last_final_block();
    let sync_hash = StateSync::get_epoch_start_sync_hash(chain, final_hash)?;

    Ok((head.epoch_id, sync_hash))
}
