use crate::{metrics, NearConfig, NightshadeRuntime};
use near_chain::types::RuntimeAdapter;
use near_chain::{Chain, ChainGenesis, DoomslugThresholdMode, Error};
use near_chain_configs::ClientConfig;
use near_client::sync::state::StateSync;
use near_crypto::PublicKey;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::hash::CryptoHash;
use near_primitives::state_part::PartId;
use near_primitives::syncing::{get_num_state_parts, StateSyncDumpProgress};
use near_primitives::types::{EpochId, ShardId};
use std::sync::Arc;

pub fn spawn_state_sync_dump(
    config: &NearConfig,
    chain_genesis: &ChainGenesis,
    runtime: Arc<NightshadeRuntime>,
    node_key: &PublicKey,
) -> anyhow::Result<Option<StateSyncDumpHandle>> {
    if config.client_config.state_sync_s3_bucket.is_none()
        || config.client_config.state_sync_s3_region.is_none()
    {
        return Ok(None);
    }
    tracing::info!(target: "state_sync_dump", "Spawning the state sync dump loop");

    // Create a connection to S3.
    let s3_bucket = config.client_config.state_sync_s3_bucket.as_ref().unwrap();
    let s3_region = config.client_config.state_sync_s3_region.as_ref().unwrap();
    let bucket = s3::Bucket::new(
        &s3_bucket,
        s3_region
            .parse::<s3::Region>()
            .map_err(|err| <std::str::Utf8Error as Into<anyhow::Error>>::into(err))?,
        s3::creds::Credentials::default().map_err(|err| {
            <s3::creds::error::CredentialsError as Into<anyhow::Error>>::into(err)
        })?,
    )
    .map_err(|err| <s3::error::S3Error as Into<anyhow::Error>>::into(err))?;

    // Determine how many threads to start.
    // Doesn't handle the case of changing the shard layout.
    let num_shards = {
        // Sadly, `Chain` is not `Send` and each thread needs to create its own `Chain` instance.
        let chain = Chain::new_for_view_client(
            runtime.clone(),
            chain_genesis,
            DoomslugThresholdMode::TwoThirds,
            config.client_config.save_trie_changes,
        )?;
        let epoch_id = chain.head()?.epoch_id;
        runtime.num_shards(&epoch_id)
    }?;

    // Start a thread for each shard.
    let handles = (0..num_shards as usize)
        .map(|shard_id| {
            let client_config = config.client_config.clone();
            let runtime = runtime.clone();
            let save_trie_changes = client_config.save_trie_changes;
            let chain_genesis = chain_genesis.clone();
            let chain = Chain::new_for_view_client(
                runtime.clone(),
                &chain_genesis,
                DoomslugThresholdMode::TwoThirds,
                save_trie_changes,
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
    node_key: PublicKey,
) {
    tracing::info!(target: "state_sync_dump", shard_id, "Running StateSyncDump loop");
    let mut interval = actix_rt::time::interval(std::time::Duration::from_secs(10));

    loop {
        // Avoid a busy-loop when there is nothing to do.
        interval.tick().await;

        let progress = chain.store().get_state_sync_dump_progress(shard_id);
        tracing::debug!(target: "state_sync_dump", shard_id, ?progress, "Running StateSyncDump loop iteration");
        // The `match` returns the next state of the state machine.
        let next_state = match progress {
            Ok(Some(StateSyncDumpProgress::AllDumped { epoch_id, num_parts })) => {
                // The latest epoch was dumped. Check if a newer epoch is available.
                check_new_epoch(Some(epoch_id), num_parts, shard_id, &chain, &runtime, &config)
            }
            Err(Error::DBNotFoundErr(_)) | Ok(None) => {
                // First invocation of this state-machine. See if at least one epoch is available for dumping.
                check_new_epoch(None, None, shard_id, &chain, &runtime, &config)
            }
            Err(err) => {
                // Something went wrong, let's retry.
                tracing::debug!(target: "state_sync_dump", shard_id, ?err, "Failed to read the progress, delete and retry");
                if let Err(err) = chain.store().set_state_sync_dump_progress(shard_id, None) {
                    tracing::debug!(target: "state_sync_dump", shard_id, ?err, "And failed to delete it too :(");
                }
                Ok(None)
            }
            Ok(Some(StateSyncDumpProgress::InProgress {
                epoch_id,
                epoch_height,
                sync_hash,
                state_root,
                parts_dumped,
                num_parts,
            })) => {
                // TODO: Metric for num_parts per shard
                // TODO: Metric for num_dumped per shard

                // The actual dumping of state to S3.
                tracing::info!(target: "state_sync_dump", shard_id, ?epoch_id, epoch_height, ?sync_hash, ?state_root, parts_dumped, num_parts, "Creating parts and dumping them");
                let mut res = None;
                for part_id in parts_dumped..num_parts {
                    // Dump parts sequentially synchronously.
                    // TODO: How to make it possible to dump state more effectively using multiple nodes?
                    let _timer = metrics::STATE_SYNC_DUMP_ITERATION_ELAPSED
                        .with_label_values(&[&shard_id.to_string()])
                        .start_timer();
                    let state_part = {
                        let _timer = metrics::STATE_SYNC_DUMP_OBTAIN_PART_ELAPSED
                            .with_label_values(&[&shard_id.to_string()])
                            .start_timer();
                        runtime.obtain_state_part(
                            shard_id,
                            &sync_hash,
                            &state_root,
                            PartId::new(part_id, num_parts),
                        )
                    };
                    let state_part = match state_part {
                        Ok(state_part) => state_part,
                        Err(err) => {
                            res = Some(err);
                            break;
                        }
                    };
                    let location = s3_location(&config.chain_id, epoch_height, shard_id, part_id);

                    {
                        let _timer = metrics::STATE_SYNC_DUMP_PUT_OBJECT_ELAPSED
                            .with_label_values(&[&shard_id.to_string()])
                            .start_timer();
                        let put = bucket
                            .put_object(&location, &state_part)
                            .await
                            .map_err(|err| Error::Other(err.to_string()));
                        if let Err(err) = put {
                            res = Some(err);
                            break;
                        }

                        // Optional, we probably don't need this.
                        let put = bucket
                            .put_object_tagging(
                                &location,
                                &[
                                    ("chain_id", &config.chain_id),
                                    ("epoch_id", &format!("{:?}", epoch_id.0)),
                                    ("epoch_height", &epoch_height.to_string()),
                                    ("state_root", &format!("{:?}", state_root)),
                                    ("sync_hash", &format!("{:?}", sync_hash)),
                                    ("node_key", &format!("{:?}", node_key)),
                                    ("num_parts", &format!("{}", num_parts)),
                                ],
                            )
                            .await
                            .map_err(|err| Error::Other(err.to_string()));
                        if let Err(err) = put {
                            res = Some(err);
                            break;
                        }
                    }

                    // Record that a part was obtained and dumped.
                    tracing::debug!(target: "state_sync_dump", shard_id, ?epoch_id, epoch_height, ?sync_hash, ?state_root, part_id, part_length = state_part.len(), ?location, "Wrote a state part to S3");
                    let next_progress = StateSyncDumpProgress::InProgress {
                        epoch_id: epoch_id.clone(),
                        epoch_height,
                        sync_hash,
                        state_root,
                        parts_dumped: part_id + 1,
                        num_parts,
                    };
                    match chain
                        .store()
                        .set_state_sync_dump_progress(shard_id, Some(next_progress.clone()))
                    {
                        Ok(_) => {
                            tracing::debug!(target: "state_sync_dump", shard_id, ?next_progress, "Updated dump progress");
                        }
                        Err(err) => {
                            tracing::debug!(target: "state_sync_dump", shard_id, ?err, "Failed to update dump progress, continue");
                        }
                    }
                    metrics::STATE_SYNC_DUMP_NUM_PARTS_DUMPED
                        .with_label_values(&[&shard_id.to_string()])
                        .set(part_id as i64 + 1);
                    metrics::STATE_SYNC_DUMP_NUM_PARTS_TOTAL
                        .with_label_values(&[&shard_id.to_string()])
                        .set(num_parts as i64);
                }
                if let Some(err) = res {
                    Err(err)
                } else {
                    Ok(Some(StateSyncDumpProgress::AllDumped {
                        epoch_id,
                        num_parts: Some(num_parts),
                    }))
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
    let num_shards = runtime.num_shards(&epoch_id)?;
    let sync_hash_block = chain.get_block(&sync_hash)?;
    if runtime.cares_about_shard(None, sync_hash_block.header().prev_hash(), shard_id, false) {
        assert_eq!(num_shards, sync_hash_block.chunks().len() as u64);
        let state_root = sync_hash_block.chunks()[shard_id as usize].prev_state_root();
        let state_root_node = runtime.get_state_root_node(shard_id, &sync_hash, &state_root)?;
        let num_parts = get_num_state_parts(state_root_node.memory_usage);
        tracing::debug!(target: "state_sync_dump", shard_id, ?epoch_id, ?sync_hash, ?state_root, num_parts, "Initialize dumping state of Epoch");
        // Note that first the state of the state machines gets changes to
        // `InProgress` and it starts dumping state after a short interval.
        metrics::STATE_SYNC_DUMP_NUM_PARTS_DUMPED
            .with_label_values(&[&shard_id.to_string()])
            .set(0);
        metrics::STATE_SYNC_DUMP_NUM_PARTS_TOTAL
            .with_label_values(&[&shard_id.to_string()])
            .set(num_parts as i64);
        Ok(Some(StateSyncDumpProgress::InProgress {
            epoch_id,
            epoch_height,
            sync_hash,
            state_root,
            parts_dumped: 0,
            num_parts,
        }))
    } else {
        tracing::debug!(target: "state_sync_dump", shard_id, ?epoch_id, ?sync_hash, "Shard is not tracked, skip the epoch");
        Ok(Some(StateSyncDumpProgress::AllDumped { epoch_id, num_parts: Some(0) }))
    }
}

/// Checks what is the latest complete epoch.
/// `epoch_id` represents the last fully dumped epoch.
fn check_new_epoch(
    epoch_id: Option<EpochId>,
    num_parts: Option<u64>,
    shard_id: ShardId,
    chain: &Chain,
    runtime: &Arc<NightshadeRuntime>,
    config: &ClientConfig,
) -> Result<Option<StateSyncDumpProgress>, Error> {
    let head = chain.head()?;
    if Some(&head.epoch_id) == epoch_id.as_ref() {
        if let Some(num_parts) = num_parts {
            metrics::STATE_SYNC_DUMP_NUM_PARTS_DUMPED
                .with_label_values(&[&shard_id.to_string()])
                .set(num_parts as i64);
            metrics::STATE_SYNC_DUMP_NUM_PARTS_TOTAL
                .with_label_values(&[&shard_id.to_string()])
                .set(num_parts as i64);
        }
        Ok(None)
    } else {
        tracing::info!(target: "state_sync_dump", shard_id, ?epoch_id, "Check if a new complete epoch is available");
        let mut sync_hash = head.prev_block_hash;
        // Step back a few blocks to avoid dealing with forks.
        for _ in 0..config.state_fetch_horizon {
            sync_hash = *chain.get_block_header(&sync_hash)?.prev_hash();
        }
        let sync_hash = StateSync::get_epoch_start_sync_hash(&chain, &sync_hash)?;
        let header = chain.get_block_header(&sync_hash)?;
        if Some(header.epoch_id()) == epoch_id.as_ref() {
            // Still in the latest dumped epoch. Do nothing.
            Ok(None)
        } else {
            start_dumping(head.epoch_id.clone(), sync_hash, shard_id, &chain, runtime)
        }
    }
}

fn s3_location(chain_id: &str, epoch_height: u64, shard_id: u64, part_id: u64) -> String {
    format!(
        "chain_id={}/epoch_height={}/shard_id={}/state_part_{:06}",
        chain_id, epoch_height, shard_id, part_id
    )
}
