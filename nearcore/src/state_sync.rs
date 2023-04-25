use crate::metrics;
use borsh::BorshSerialize;
use near_chain::{
    Chain, ChainGenesis, ChainStoreAccess, DoomslugThresholdMode, Error,
    RuntimeWithEpochManagerAdapter,
};
use near_chain_configs::{ClientConfig, ExternalStorageLocation};
use near_client::sync::state::{external_storage_location, ExternalConnection, StateSync};
use near_primitives::hash::CryptoHash;
use near_primitives::state_part::PartId;
use near_primitives::syncing::{get_num_state_parts, StatePartKey, StateSyncDumpProgress};
use near_primitives::types::{AccountId, EpochHeight, EpochId, ShardId, StateRoot};
use near_store::DBCol;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

/// Starts one a thread per tracked shard.
/// Each started thread will be dumping state parts of a single epoch to external storage.
pub fn spawn_state_sync_dump(
    client_config: &ClientConfig,
    chain_genesis: ChainGenesis,
    runtime: Arc<dyn RuntimeWithEpochManagerAdapter>,
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
            runtime.clone(),
            &chain_genesis,
            DoomslugThresholdMode::TwoThirds,
            false,
        )?;
        let epoch_id = chain.head()?.epoch_id;
        runtime.num_shards(&epoch_id)
    }?;

    let chain_id = client_config.chain_id.clone();
    let keep_running = Arc::new(AtomicBool::new(true));
    // Start a thread for each shard.
    let handles = (0..num_shards as usize)
        .map(|shard_id| {
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

/// A thread loop that dumps state of the latest available epoch to S3.
/// Operates as a state machine. Persists its state in the Misc column.
/// Shards store the state-machine state separately.
async fn state_sync_dump(
    shard_id: ShardId,
    chain: Chain,
    runtime: Arc<dyn RuntimeWithEpochManagerAdapter>,
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

    while keep_running.load(std::sync::atomic::Ordering::Relaxed) {
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
                    &account_id,
                )
            }
            Err(Error::DBNotFoundErr(_)) | Ok(None) => {
                // First invocation of this state-machine. See if at least one epoch is available for dumping.
                check_new_epoch(None, None, None, shard_id, &chain, &runtime, &account_id)
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
                let in_progress_data = get_in_progress_data(shard_id, sync_hash, &chain);
                let mut res = None;
                match in_progress_data {
                    Ok((state_root, num_parts, sync_prev_hash)) => {
                        // The actual dumping of state to S3.
                        tracing::info!(target: "state_sync_dump", shard_id, ?epoch_id, epoch_height, %sync_hash, ?state_root, parts_dumped, "Creating parts and dumping them");
                        for part_id in parts_dumped..num_parts {
                            // Dump parts sequentially synchronously.
                            // TODO: How to make it possible to dump state more effectively using multiple nodes?
                            let _timer = metrics::STATE_SYNC_DUMP_ITERATION_ELAPSED
                                .with_label_values(&[&shard_id.to_string()])
                                .start_timer();

                            let state_part = match obtain_and_store_state_part(
                                &runtime,
                                shard_id,
                                sync_hash,
                                &sync_prev_hash,
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
                            let location = external_storage_location(
                                &chain_id,
                                epoch_height,
                                shard_id,
                                part_id,
                                num_parts,
                            );
                            if let Err(err) =
                                external.put_state_part(&state_part, shard_id, &location).await
                            {
                                res = Some(Error::Other(err.to_string()));
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

                            // Stop if the node is stopped.
                            // Note that without this check the state dumping thread is unstoppable, i.e. non-interruptable.
                            if !keep_running.load(std::sync::atomic::Ordering::Relaxed) {
                                res = Some(Error::Other("Stopped".to_owned()));
                                break;
                            }
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
                // Do nothing.
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

    let sync_block = chain.get_block(&sync_hash)?;
    let sync_prev_hash = sync_block.header().prev_hash();
    Ok((state_root, num_parts, *sync_prev_hash))
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
        .with_label_values(&[&epoch_height.to_string(), &shard_id.to_string()])
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
    runtime: &Arc<dyn RuntimeWithEpochManagerAdapter>,
    shard_id: ShardId,
    sync_hash: CryptoHash,
    sync_prev_hash: &CryptoHash,
    state_root: &StateRoot,
    part_id: u64,
    num_parts: u64,
    chain: &Chain,
) -> Result<Vec<u8>, Error> {
    let state_part = runtime.obtain_state_part(
        shard_id,
        sync_prev_hash,
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
    runtime: &Arc<dyn RuntimeWithEpochManagerAdapter>,
    account_id: &Option<AccountId>,
) -> Result<Option<StateSyncDumpProgress>, Error> {
    let epoch_info = runtime.get_epoch_info(&epoch_id)?;
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
    if runtime.cares_about_shard(account_id.as_ref(), sync_prev_prev_hash, shard_id, true) {
        tracing::info!(target: "state_sync_dump", shard_id, ?epoch_id, %sync_prev_hash, %sync_hash, "Initialize dumping state of Epoch");
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
        tracing::info!(target: "state_sync_dump", shard_id, ?epoch_id, %sync_hash, "Shard is not tracked, skip the epoch");
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
    runtime: &Arc<dyn RuntimeWithEpochManagerAdapter>,
    account_id: &Option<AccountId>,
) -> Result<Option<StateSyncDumpProgress>, Error> {
    let head = chain.head()?;
    if Some(&head.epoch_id) == epoch_id.as_ref() {
        set_metrics(&shard_id, num_parts, num_parts, epoch_height);
        Ok(None)
    } else {
        // Check if the final block is now in the next epoch.
        tracing::debug!(target: "state_sync_dump", shard_id, ?epoch_id, "Check if a new complete epoch is available");
        let hash = head.last_block_hash;
        let header = chain.get_block_header(&hash)?;
        let final_hash = header.last_final_block();
        let sync_hash = StateSync::get_epoch_start_sync_hash(chain, final_hash)?;
        let header = chain.get_block_header(&sync_hash)?;
        if Some(header.epoch_id()) == epoch_id.as_ref() {
            // Still in the latest dumped epoch. Do nothing.
            Ok(None)
        } else {
            start_dumping(head.epoch_id, sync_hash, shard_id, chain, runtime, account_id)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::state_sync::spawn_state_sync_dump;
    use near_chain::{ChainGenesis, Provenance};
    use near_chain_configs::{DumpConfig, ExternalStorageLocation};
    use near_client::sync::state::external_storage_location;
    use near_client::test_utils::TestEnv;
    use near_network::test_utils::wait_or_timeout;
    use near_o11y::testonly::init_test_logger;
    use near_primitives::types::BlockHeight;
    use std::ops::ControlFlow;
    use std::time::Duration;

    #[test]
    /// Produce several blocks, wait for the state dump thread to notice and
    /// write files to a temp dir.
    fn test_state_dump() {
        init_test_logger();

        let mut chain_genesis = ChainGenesis::test();
        chain_genesis.epoch_length = 5;
        let mut env = TestEnv::builder(chain_genesis.clone()).build();
        let chain = &env.clients[0].chain;
        let runtime = chain.runtime_adapter();
        let mut config = env.clients[0].config.clone();
        let root_dir = tempfile::Builder::new().prefix("state_dump").tempdir().unwrap();
        config.state_sync.dump = Some(DumpConfig {
            location: ExternalStorageLocation::Filesystem {
                root_dir: root_dir.path().to_path_buf(),
            },
            restart_dump_for_shards: None,
            iteration_delay: Some(Duration::from_millis(250)),
        });

        const MAX_HEIGHT: BlockHeight = 15;

        near_actix_test_utils::run_actix(async move {
            let _state_sync_dump_handle = spawn_state_sync_dump(
                &config,
                chain_genesis,
                runtime.clone(),
                Some("test0".parse().unwrap()),
            )
            .unwrap();
            let mut last_block_hash = None;
            for i in 1..=MAX_HEIGHT {
                let block = env.clients[0].produce_block(i as u64).unwrap().unwrap();
                last_block_hash = Some(*block.hash());
                env.process_block(0, block, Provenance::PRODUCED);
            }
            let epoch_id = runtime.get_epoch_id(last_block_hash.as_ref().unwrap()).unwrap();
            let epoch_info = runtime.get_epoch_info(&epoch_id).unwrap();
            let epoch_height = epoch_info.epoch_height();

            wait_or_timeout(100, 10000, || async {
                let mut all_parts_present = true;

                let num_shards = runtime.num_shards(&epoch_id).unwrap();
                assert_ne!(num_shards, 0);

                for shard_id in 0..num_shards {
                    let num_parts = 3;
                    for part_id in 0..num_parts {
                        let path = root_dir.path().join(external_storage_location(
                            "unittest",
                            epoch_height,
                            shard_id,
                            part_id,
                            num_parts,
                        ));
                        if std::fs::read(&path).is_err() {
                            println!("Missing {:?}", path);
                            all_parts_present = false;
                        }
                    }
                }
                if all_parts_present {
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                }
            })
            .await
            .unwrap();
            actix_rt::System::current().stop();
        });
    }
}
