use crate::metrics;
use borsh::BorshSerialize;
use near_chain::types::RuntimeAdapter;
use near_chain::{Chain, ChainGenesis, ChainStoreAccess, DoomslugThresholdMode, Error};
use near_chain_configs::{ClientConfig, ExternalStorageLocation};
use near_client::sync::external::{create_bucket_readwrite, external_storage_location};
use near_client::sync::external::{
    external_storage_location_directory, get_part_id_from_filename, is_part_filename,
    ExternalConnection,
};
use near_client::sync::state::{StateSync, STATE_DUMP_ITERATION_TIME_LIMIT_SECS};
use near_epoch_manager::shard_tracker::ShardTracker;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::hash::CryptoHash;
use near_primitives::state_part::PartId;
use near_primitives::syncing::{get_num_state_parts, StatePartKey, StateSyncDumpProgress};
use near_primitives::types::{AccountId, EpochHeight, EpochId, ShardId, StateRoot};
use near_store::DBCol;
use rand::{thread_rng, Rng};
use std::collections::HashSet;
use std::path::PathBuf;
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
    credentials_file: Option<PathBuf>,
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
        ExternalStorageLocation::S3 { bucket, region } => ExternalConnection::S3{
            bucket: Arc::new(create_bucket_readwrite(&bucket, &region, Duration::from_secs(30), credentials_file).expect(
                "Failed to authenticate connection to S3. Please either provide AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in the environment, or create a credentials file and link it in config.json as 's3_credentials_file'."))
        },
        ExternalStorageLocation::Filesystem { root_dir } => ExternalConnection::Filesystem { root_dir },
        ExternalStorageLocation::GCS { bucket } => ExternalConnection::GCS {
            gcs_client: Arc::new(cloud_storage::Client::default()),
            reqwest_client: Arc::new(reqwest::Client::default()),
            bucket },
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

fn get_current_state(
    chain: &Chain,
    shard_id: &ShardId,
    shard_tracker: &ShardTracker,
    account_id: &Option<AccountId>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
) -> Result<Option<(EpochId, EpochHeight, CryptoHash)>, Error> {
    let was_last_epoch_dumped = match chain.store().get_state_sync_dump_progress(*shard_id) {
        Ok(StateSyncDumpProgress::AllDumped { epoch_id, .. }) => Some(epoch_id),
        _ => None,
    };

    match get_latest_epoch(shard_id, &chain, epoch_manager) {
        Err(err) => {
            tracing::debug!(target: "state_sync_dump", shard_id, ?err, "check_latest_epoch failed. Will retry.");
            Err(err)
        }
        Ok((new_epoch_id, new_epoch_height, new_sync_hash)) => {
            if Some(&new_epoch_id) == was_last_epoch_dumped.as_ref() {
                tracing::debug!(target: "state_sync_dump", shard_id, ?was_last_epoch_dumped, ?new_epoch_id, new_epoch_height, ?new_sync_hash, "latest epoch is all dumped. No new epoch to dump. Idle");
                Ok(None)
            } else if cares_about_shard(
                chain,
                shard_id,
                &new_sync_hash,
                &shard_tracker,
                &account_id,
            )? {
                Ok(Some((new_epoch_id, new_epoch_height, new_sync_hash)))
            } else {
                tracing::debug!(target: "state_sync_dump", shard_id, ?new_epoch_id, new_epoch_height, ?new_sync_hash, "Doesn't care about the shard in the current epoch. Idle");
                Ok(None)
            }
        }
    }
}

const FAILURES_ALLOWED_PER_ITERATION: u32 = 10;

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
            Ok(None) => None,
            Ok(Some((epoch_id, epoch_height, sync_hash))) => {
                let in_progress_data = get_in_progress_data(shard_id, sync_hash, &chain);
                match in_progress_data {
                    Err(err) => {
                        tracing::error!(target: "state_sync_dump", ?err, ? shard_id, "Failed to get in progress data");
                        None
                    }
                    Ok((state_root, num_parts, sync_prev_prev_hash)) => {
                        match get_missing_part_ids_for_epoch(
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

                                    let location = external_storage_location(
                                        &chain_id,
                                        &epoch_id,
                                        epoch_height,
                                        shard_id,
                                        part_id,
                                        num_parts,
                                    );
                                    if let Err(err) = external
                                        .put_state_part(&state_part, shard_id, &location)
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
                        }
                    }
                }
            }
        };

        // Record the next state of the state machine.
        let has_progress = match next_state {
            Some(next_state) => {
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
            None => {
                // Nothing to do, will check again later.
                tracing::debug!(target: "state_sync_dump", shard_id, "Idle");
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

    let key = StatePartKey(sync_hash, shard_id, part_id).try_to_vec()?;
    let mut store_update = chain.store().store().store_update();
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

/// return epoch_id and sync_hash of the latest complete epoch available locally.
fn get_latest_epoch(
    shard_id: &ShardId,
    chain: &Chain,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
) -> Result<(EpochId, EpochHeight, CryptoHash), Error> {
    let head = chain.head()?;
    tracing::debug!(target: "state_sync_dump", shard_id, "Check if a new complete epoch is available");
    let hash = head.last_block_hash;
    let header = chain.get_block_header(&hash)?;
    let final_hash = header.last_final_block();
    let sync_hash = StateSync::get_epoch_start_sync_hash(chain, final_hash)?;
    let final_block_header = chain.get_block_header(&final_hash)?;
    let epoch_id = final_block_header.epoch_id().clone();
    let epoch_info = epoch_manager.get_epoch_info(&epoch_id)?;
    let epoch_height = epoch_info.epoch_height();
    tracing::debug!(target: "state_sync_dump", ?final_hash, ?sync_hash, ?epoch_id, epoch_height, "get_latest_epoch");

    Ok((epoch_id, epoch_height, sync_hash))
}
