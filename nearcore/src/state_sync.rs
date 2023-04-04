use crate::{metrics, NearConfig, NightshadeRuntime};
use borsh::BorshSerialize;
use near_chain::types::RuntimeAdapter;
use near_chain::{Chain, ChainGenesis, ChainStoreAccess, DoomslugThresholdMode, Error};
use near_chain_configs::ClientConfig;
use near_client::sync::state::StateSync;
use near_crypto::PublicKey;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::hash::CryptoHash;
use near_primitives::state_part::PartId;
use near_primitives::syncing::{get_num_state_parts, StatePartKey, StateSyncDumpProgress};
use near_primitives::types::{EpochHeight, EpochId, ShardId, StateRoot};
use near_store::DBCol;
use std::sync::Arc;

/// Starts one a thread per tracked shard.
/// Each started thread will be dumping state parts of a single epoch to external storage.
pub fn spawn_state_sync_dump(
    config: &NearConfig,
    chain_genesis: ChainGenesis,
    runtime: Arc<NightshadeRuntime>,
    node_key: &PublicKey,
) -> anyhow::Result<Option<StateSyncDumpHandle>> {
    if !config.client_config.state_sync_dump_enabled {
        return Ok(None);
    }
    if config.client_config.state_sync_s3_bucket.is_empty()
        || config.client_config.state_sync_s3_region.is_empty()
    {
        panic!("Enabled dumps of state to external storage. Please specify state_sync.s3_bucket and state_sync.s3_region");
    }
    tracing::info!(target: "state_sync_dump", "Spawning the state sync dump loop");

    // Create a connection to S3.
    let s3_bucket = config.client_config.state_sync_s3_bucket.clone();
    let s3_region = config.client_config.state_sync_s3_region.clone();

    // Credentials to establish a connection are taken from environment variables: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.
    let bucket = s3::Bucket::new(
        &s3_bucket,
        s3_region
            .parse::<s3::Region>()
            .map_err(|err| <std::str::Utf8Error as Into<anyhow::Error>>::into(err))?,
        s3::creds::Credentials::default().map_err(|err| {
            tracing::error!(target: "state_sync_dump", "Failed to create a connection to S3. Did you provide environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY?");
            <s3::creds::error::CredentialsError as Into<anyhow::Error>>::into(err)
        })?,
    ).map_err(|err| <s3::error::S3Error as Into<anyhow::Error>>::into(err))?;

    // Determine how many threads to start.
    // TODO: Handle the case of changing the shard layout.
    let num_shards = {
        // Sadly, `Chain` is not `Send` and each thread needs to create its own `Chain` instance.
        let chain = Chain::new_for_view_client(
            runtime.clone(),
            &chain_genesis,
            DoomslugThresholdMode::TwoThirds,
            false,
        )?;
        let epoch_id = chain.head()?.epoch_id;
        runtime.num_shards(&epoch_id)
    }?;

    // Start a thread for each shard.
    let handles = (0..num_shards as usize)
        .map(|shard_id| {
            let client_config = config.client_config.clone();
            let runtime = runtime.clone();
            let chain_genesis = chain_genesis.clone();
            let chain = Chain::new_for_view_client(
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
                runtime,
                client_config,
                bucket.clone(),
                node_key.clone(),
            )));
            arbiter_handle
        })
        .collect();

    Ok(Some(StateSyncDumpHandle { handles }))
}

/// Holds arbiter handles controlling the lifetime of the spawned threads.
pub struct StateSyncDumpHandle {
    pub handles: Vec<actix_rt::ArbiterHandle>,
}

impl Drop for StateSyncDumpHandle {
    fn drop(&mut self) {
        self.stop()
    }
}

impl StateSyncDumpHandle {
    pub fn stop(&self) {
        let _: Vec<bool> = self.handles.iter().map(|handle| handle.stop()).collect();
    }
}

/// A thread loop that dumps state of the latest available epoch to S3.
/// Operates as a state machine. Persists its state in the Misc column.
/// Shards store the state-machine state separately.
async fn state_sync_dump(
    shard_id: ShardId,
    chain: Chain,
    runtime: Arc<NightshadeRuntime>,
    config: ClientConfig,
    bucket: s3::Bucket,
    _node_key: PublicKey,
) {
    tracing::info!(target: "state_sync_dump", shard_id, "Running StateSyncDump loop");
    let mut interval = actix_rt::time::interval(std::time::Duration::from_secs(10));

    if config.state_sync_restart_dump_for_shards.contains(&shard_id) {
        tracing::debug!(target: "state_sync_dump", shard_id, "Dropped existing progress");
        chain.store().set_state_sync_dump_progress(shard_id, None).unwrap();
    }

    loop {
        // Avoid a busy-loop when there is nothing to do.
        interval.tick().await;

        let progress = chain.store().get_state_sync_dump_progress(shard_id);
        tracing::debug!(target: "state_sync_dump", shard_id, ?progress, "Running StateSyncDump loop iteration");
        // The `match` returns the next state of the state machine.
        let next_state = match progress {
            Ok(Some(StateSyncDumpProgress::AllDumped { epoch_id, epoch_height, num_parts })) => {
                // The latest epoch was dumped. Check if a newer epoch is available.
                check_new_epoch(
                    Some(epoch_id),
                    Some(epoch_height),
                    num_parts,
                    shard_id,
                    &chain,
                    &runtime,
                )
            }
            Err(Error::DBNotFoundErr(_)) | Ok(None) => {
                // First invocation of this state-machine. See if at least one epoch is available for dumping.
                check_new_epoch(None, None, None, shard_id, &chain, &runtime)
            }
            Err(err) => {
                // Something went wrong, let's retry.
                tracing::warn!(target: "state_sync_dump", shard_id, ?err, "Failed to read the progress, will now delete and retry");
                if let Err(err) = chain.store().set_state_sync_dump_progress(shard_id, None) {
                    tracing::warn!(target: "state_sync_dump", shard_id, ?err, "and failed to delete the progress. Will later retry.");
                }
                Ok(None)
            }
            Ok(Some(StateSyncDumpProgress::InProgress {
                epoch_id,
                epoch_height,
                sync_hash,
                parts_dumped,
            })) => {
                let state_header = chain.get_state_response_header(shard_id, sync_hash);
                match state_header {
                    Ok(state_header) => {
                        let state_root = state_header.chunk_prev_state_root();
                        let num_parts =
                            get_num_state_parts(state_header.state_root_node().memory_usage);

                        let mut res = None;
                        // The actual dumping of state to S3.
                        tracing::info!(target: "state_sync_dump", shard_id, ?epoch_id, epoch_height, %sync_hash, parts_dumped, "Creating parts and dumping them");
                        for part_id in parts_dumped..num_parts {
                            // Dump parts sequentially synchronously.
                            // TODO: How to make it possible to dump state more effectively using multiple nodes?
                            let _timer = metrics::STATE_SYNC_DUMP_ITERATION_ELAPSED
                                .with_label_values(&[&shard_id.to_string()])
                                .start_timer();

                            let state_part = match obtain_and_store_state_part(
                                &runtime,
                                &shard_id,
                                &sync_hash,
                                &state_root,
                                part_id,
                                num_parts,
                                &chain,
                            ) {
                                Ok(state_part) => state_part,
                                Err(err) => {
                                    res = Some(err);
                                    break;
                                }
                            };
                            let location = s3_location(
                                &config.chain_id,
                                epoch_height,
                                shard_id,
                                part_id,
                                num_parts,
                            );
                            if let Err(err) =
                                put_state_part(&location, &state_part, &shard_id, &bucket).await
                            {
                                res = Some(err);
                                break;
                            }
                            update_progress(
                                &shard_id,
                                &epoch_id,
                                epoch_height,
                                &sync_hash,
                                part_id,
                                num_parts,
                                state_part.len(),
                                &chain,
                            );
                        }
                        if let Some(err) = res {
                            Err(err)
                        } else {
                            Ok(Some(StateSyncDumpProgress::AllDumped {
                                epoch_id,
                                epoch_height,
                                num_parts: Some(num_parts),
                            }))
                        }
                    }
                    Err(err) => Err(err),
                }
            }
        };

        // Record the next state of the state machine.
        match next_state {
            Ok(Some(next_state)) => {
                tracing::debug!(target: "state_sync_dump", shard_id, ?next_state);
                match chain.store().set_state_sync_dump_progress(shard_id, Some(next_state)) {
                    Ok(_) => {}
                    Err(err) => {
                        // This will be retried.
                        tracing::debug!(target: "state_sync_dump", shard_id, ?err, "Failed to set progress");
                    }
                }
            }
            Ok(None) => {
                // Do nothing.
                tracing::debug!(target: "state_sync_dump", shard_id, "Idle");
            }
            Err(err) => {
                // Will retry.
                tracing::debug!(target: "state_sync_dump", shard_id, ?err, "Failed to determine what to do");
            }
        }
    }
}

async fn put_state_part(
    location: &str,
    state_part: &[u8],
    shard_id: &ShardId,
    bucket: &s3::Bucket,
) -> Result<s3::request_trait::ResponseData, Error> {
    let _timer = metrics::STATE_SYNC_DUMP_PUT_OBJECT_ELAPSED
        .with_label_values(&[&shard_id.to_string()])
        .start_timer();
    let put = bucket
        .put_object(&location, &state_part)
        .await
        .map_err(|err| Error::Other(err.to_string()));
    tracing::debug!(target: "state_sync_dump", shard_id, part_length = state_part.len(), ?location, "Wrote a state part to S3");
    put
}

fn update_progress(
    shard_id: &ShardId,
    epoch_id: &EpochId,
    epoch_height: EpochHeight,
    sync_hash: &CryptoHash,
    part_id: u64,
    num_parts: u64,
    part_len: usize,
    chain: &Chain,
) {
    // Record that a part was obtained and dumped.
    metrics::STATE_SYNC_DUMP_SIZE_TOTAL
        .with_label_values(&[&shard_id.to_string()])
        .inc_by(part_len as u64);
    let next_progress = StateSyncDumpProgress::InProgress {
        epoch_id: epoch_id.clone(),
        epoch_height,
        sync_hash: *sync_hash,
        parts_dumped: part_id + 1,
    };
    match chain.store().set_state_sync_dump_progress(*shard_id, Some(next_progress.clone())) {
        Ok(_) => {
            tracing::debug!(target: "state_sync_dump", shard_id, ?next_progress, "Updated dump progress");
        }
        Err(err) => {
            tracing::debug!(target: "state_sync_dump", shard_id, ?err, "Failed to update dump progress, continue");
        }
    }
    set_metrics(shard_id, Some(part_id + 1), Some(num_parts), Some(epoch_height));
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
    runtime: &Arc<NightshadeRuntime>,
    shard_id: &ShardId,
    sync_hash: &CryptoHash,
    state_root: &StateRoot,
    part_id: u64,
    num_parts: u64,
    chain: &Chain,
) -> Result<Vec<u8>, Error> {
    let state_part = runtime.obtain_state_part(
        *shard_id,
        &sync_hash,
        &state_root,
        PartId::new(part_id, num_parts),
    )?;

    let key = StatePartKey(*sync_hash, *shard_id, part_id).try_to_vec()?;
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
    runtime: &Arc<NightshadeRuntime>,
) -> Result<Option<StateSyncDumpProgress>, Error> {
    let epoch_info = runtime.get_epoch_info(&epoch_id)?;
    let epoch_height = epoch_info.epoch_height();
    let sync_prev_header = chain.get_block_header(&sync_hash)?;
    let sync_prev_hash = sync_prev_header.hash();

    let state_header = chain.get_state_response_header(shard_id, sync_hash)?;
    let num_parts = get_num_state_parts(state_header.state_root_node().memory_usage);
    if runtime.cares_about_shard(None, sync_prev_hash, shard_id, false) {
        tracing::debug!(target: "state_sync_dump", shard_id, ?epoch_id, %sync_prev_hash, %sync_hash, "Initialize dumping state of Epoch");
        // Note that first the state of the state machines gets changes to
        // `InProgress` and it starts dumping state after a short interval.
        set_metrics(&shard_id, Some(0), Some(num_parts), Some(epoch_height));
        Ok(Some(StateSyncDumpProgress::InProgress {
            epoch_id,
            epoch_height,
            sync_hash,
            parts_dumped: 0,
        }))
    } else {
        tracing::debug!(target: "state_sync_dump", shard_id, ?epoch_id, %sync_prev_hash, %sync_hash, "Shard is not tracked, skip the epoch");
        Ok(Some(StateSyncDumpProgress::AllDumped { epoch_id, epoch_height, num_parts: Some(0) }))
    }
}

/// Checks what is the latest complete epoch.
/// `epoch_id` represents the last fully dumped epoch.
fn check_new_epoch(
    epoch_id: Option<EpochId>,
    epoch_height: Option<EpochHeight>,
    num_parts: Option<u64>,
    shard_id: ShardId,
    chain: &Chain,
    runtime: &Arc<NightshadeRuntime>,
) -> Result<Option<StateSyncDumpProgress>, Error> {
    let head = chain.head()?;
    if Some(&head.epoch_id) == epoch_id.as_ref() {
        set_metrics(&shard_id, num_parts, num_parts, epoch_height);
        Ok(None)
    } else {
        // Check if the final block is now in the next epoch.
        tracing::info!(target: "state_sync_dump", shard_id, ?epoch_id, "Check if a new complete epoch is available");
        let hash = head.last_block_hash;
        let header = chain.get_block_header(&hash)?;
        let final_hash = header.last_final_block();
        let sync_hash = StateSync::get_epoch_start_sync_hash(&chain, &final_hash)?;
        let header = chain.get_block_header(&sync_hash)?;
        if Some(header.epoch_id()) == epoch_id.as_ref() {
            // Still in the latest dumped epoch. Do nothing.
            Ok(None)
        } else {
            start_dumping(head.epoch_id, sync_hash, shard_id, &chain, runtime)
        }
    }
}

fn s3_location(
    chain_id: &str,
    epoch_height: u64,
    shard_id: u64,
    part_id: u64,
    num_parts: u64,
) -> String {
    format!(
        "chain_id={}/epoch_height={}/shard_id={}/state_part_{:06}_of_{:06}",
        chain_id, epoch_height, shard_id, part_id, num_parts
    )
}
