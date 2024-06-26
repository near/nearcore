use actix_web::{web, App, HttpServer};
use anyhow::anyhow;
use borsh::BorshDeserialize;
use near_client::sync::external::{
    create_bucket_readonly, external_storage_location, external_storage_location_directory,
    get_num_parts_from_filename, ExternalConnection, StateFileType,
};
use near_jsonrpc::client::{new_client, JsonRpcClient};
use near_primitives::hash::CryptoHash;
use near_primitives::state_part::PartId;
use near_primitives::state_sync::ShardStateSyncResponseHeader;
use near_primitives::types::{
    BlockId, BlockReference, EpochId, EpochReference, Finality, ShardId, StateRoot,
};
use near_primitives::views::BlockView;
use near_store::Trie;
use nearcore::state_sync::extract_part_id_from_part_file_name;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant};
use tokio::time::timeout;

// This isn't set to a huge number because if we keep retrying for too long, we will miss the next epoch's check
const MAX_RETRIES: u32 = 5;

#[derive(clap::Parser)]
pub struct StatePartsDumpCheckCommand {
    #[clap(long)]
    chain_id: String,
    // the root dir to use when retrieving state parts from local storage
    #[clap(long, value_parser)]
    root_dir: Option<PathBuf>,
    // the s3 bucket to use when retrieving state parts from S3
    #[clap(long)]
    s3_bucket: Option<String>,
    // the s3 region to use when retrieving state parts from S3
    #[clap(long)]
    s3_region: Option<String>,
    // the gcs bucket to use when retrieving state parts from GCP
    #[clap(long)]
    gcs_bucket: Option<String>,
    // this can be either loop-check or single-check
    #[clap(subcommand)]
    subcmd: StatePartsDumpCheckSubCommand,
}

#[derive(clap::Subcommand)]
pub enum StatePartsDumpCheckSubCommand {
    /// Download and validate the state parts given the epoch_id, epoch_height, state_root and shard_id
    SingleCheck(SingleCheckCommand),
    /// Runs an infinite loop to download and validate state parts of all shards for each epoch when it becomes available
    LoopCheck(LoopCheckCommand),
}

#[derive(clap::Parser)]
pub struct LoopCheckCommand {
    /// Listen address for prometheus metrics.
    #[clap(long, default_value = "0.0.0.0:4040")]
    prometheus_addr: String,
    // address of RPC server to retrieve latest block, epoch information
    #[clap(long)]
    rpc_server_addr: Option<String>,
    // Loop interval in seconds
    #[clap(long, default_value = "60")]
    interval: u64,
}

#[derive(clap::Parser)]
pub struct SingleCheckCommand {
    #[clap(long)]
    epoch_id: EpochId,
    #[clap(long)]
    epoch_height: u64,
    #[clap(long)]
    state_root: StateRoot,
    #[clap(long)]
    shard_id: ShardId,
}

impl StatePartsDumpCheckCommand {
    pub fn run(&self) -> anyhow::Result<()> {
        self.subcmd.run(
            self.chain_id.clone(),
            self.root_dir.clone(),
            self.s3_bucket.clone(),
            self.s3_region.clone(),
            self.gcs_bucket.clone(),
        )
    }
}

impl StatePartsDumpCheckSubCommand {
    pub fn run(
        &self,
        chain_id: String,
        root_dir: Option<PathBuf>,
        s3_bucket: Option<String>,
        s3_region: Option<String>,
        gcs_bucket: Option<String>,
    ) -> anyhow::Result<()> {
        match self {
            StatePartsDumpCheckSubCommand::SingleCheck(cmd) => {
                cmd.run(chain_id, root_dir, s3_bucket, s3_region, gcs_bucket)
            }
            StatePartsDumpCheckSubCommand::LoopCheck(cmd) => {
                cmd.run(chain_id, root_dir, s3_bucket, s3_region, gcs_bucket)
            }
        }
    }
}

impl SingleCheckCommand {
    fn run(
        &self,
        chain_id: String,
        root_dir: Option<PathBuf>,
        s3_bucket: Option<String>,
        s3_region: Option<String>,
        gcs_bucket: Option<String>,
    ) -> anyhow::Result<()> {
        let sys = actix::System::new();
        sys.block_on(async move {
            let _check_result = run_single_check_with_3_retries(
                None,
                chain_id,
                self.epoch_id,
                self.epoch_height,
                self.shard_id,
                self.state_root,
                root_dir,
                s3_bucket,
                s3_region,
                gcs_bucket,
            )
            .await;
        });

        Ok(())
    }
}

impl LoopCheckCommand {
    // Connect to an RPC server to request latest epoch information.
    // Whenever an epoch is complete, use the location specified by root_dir/s3_bucket&s3_location/gcs_bucket to download parts and validate them.
    // Metrics will be emitted for epoch_height and dumped/valid/invalid/total state parts of the shard.
    fn run(
        &self,
        chain_id: String,
        root_dir: Option<PathBuf>,
        s3_bucket: Option<String>,
        s3_region: Option<String>,
        gcs_bucket: Option<String>,
    ) -> anyhow::Result<()> {
        let rpc_server_addr = match &self.rpc_server_addr {
            None => {
                if chain_id == "mainnet" || chain_id == "testnet" {
                    format!("http://rpc.{}.near.org", chain_id)
                } else {
                    return Err(anyhow!("rpc_server_addr needs to be supplied if chain_id is not mainnet or testnet"));
                }
            }
            Some(addr) => addr.to_string(),
        };

        let rpc_client = new_client(&rpc_server_addr);

        tracing::info!("started running LoopCheckCommand");

        let result = run_loop_all_shards(
            chain_id,
            root_dir,
            s3_bucket,
            s3_region,
            gcs_bucket,
            &rpc_client,
            &self.prometheus_addr,
            self.interval,
        );

        tracing::info!("run_loop_all_shards finished");

        match result {
            Ok(_) => {
                tracing::info!("run_loop_all_shards returned OK()");
            }
            Err(err) => {
                tracing::info!("run_loop_all_shards errored out with {}", err);
            }
        }

        Ok(())
    }
}

#[derive(Clone)]
// whether the check for the specified epoch's state part dump is finished or waiting
enum StatePartsDumpCheckStatus {
    // download and validation for dumped parts of the epoch is done
    Done { epoch_height: u64 },
    // not all required parts and headers are dumped for the epoch
    Waiting { epoch_height: u64, parts_done: bool, headers_done: bool },
}

#[derive(Clone)]
struct DumpCheckIterInfo {
    epoch_id: EpochId,
    epoch_height: u64,
    state_roots: Vec<CryptoHash>,
}

fn create_external_connection(
    root_dir: Option<PathBuf>,
    bucket: Option<String>,
    region: Option<String>,
    gcs_bucket: Option<String>,
) -> ExternalConnection {
    if let Some(root_dir) = root_dir {
        ExternalConnection::Filesystem { root_dir }
    } else if let (Some(bucket), Some(region)) = (bucket, region) {
        let bucket = create_bucket_readonly(&bucket, &region, Duration::from_secs(5))
            .expect("Failed to create an S3 bucket");
        ExternalConnection::S3 { bucket: Arc::new(bucket) }
    } else if let Some(bucket) = gcs_bucket {
        ExternalConnection::GCS {
            gcs_client: Arc::new(cloud_storage::Client::default()),
            reqwest_client: Arc::new(reqwest::Client::default()),
            bucket,
        }
    } else {
        panic!(
            "Please provide --root-dir, or both of --s3-bucket and --s3-region, or --gcs-bucket"
        );
    }
}

fn validate_state_part(state_root: &StateRoot, part_id: PartId, part: &[u8]) -> bool {
    match BorshDeserialize::try_from_slice(part) {
        Ok(trie_nodes) => {
            match Trie::validate_state_part(state_root, part_id, trie_nodes) {
                Ok(_) => true,
                // Storage error should not happen
                Err(err) => {
                    tracing::error!(target: "state-parts", ?err, "State part storage error");
                    false
                }
            }
        }
        // Deserialization error means we've got the data from malicious peer
        Err(err) => {
            tracing::error!(target: "state-parts", ?err, "State part deserialization error");
            false
        }
    }
}

fn validate_state_header(header: &[u8]) -> bool {
    match ShardStateSyncResponseHeader::try_from_slice(&header) {
        Ok(_) => {
            // Nodes will be able to download and deserialize the header.
            true
        }
        // Deserialization error means we've got invalid data stored in the external folder.
        Err(err) => {
            tracing::error!(target: "state-parts", ?err, "Header deserialization error");
            false
        }
    }
}

// run an infinite loop to download and validate state parts for all shards whenever new epoch becomes available
fn run_loop_all_shards(
    chain_id: String,
    root_dir: Option<PathBuf>,
    s3_bucket: Option<String>,
    s3_region: Option<String>,
    gcs_bucket: Option<String>,
    rpc_client: &JsonRpcClient,
    prometheus_addr: &str,
    loop_interval: u64,
) -> anyhow::Result<()> {
    let mut last_check_status =
        HashMap::<ShardId, anyhow::Result<StatePartsDumpCheckStatus>>::new();

    let mut is_prometheus_server_up: bool = false;

    let sys = actix::System::new();
    loop {
        tracing::info!("running the loop inside run_loop_all_shards");
        let dump_check_iter_info_res =
            sys.block_on(async move { get_processing_epoch_information(&rpc_client).await });
        if let Err(err) = dump_check_iter_info_res {
            tracing::info!(
                "get_processing_epoch_information errs out with {}. sleeping for {loop_interval}s.",
                err
            );
            sleep(Duration::from_secs(loop_interval));
            continue;
        }
        let dump_check_iter_info = dump_check_iter_info_res?;
        let num_shards = dump_check_iter_info.state_roots.len();
        for shard_id in 0..num_shards as u64 {
            tracing::info!(shard_id, "started check");
            let dump_check_iter_info = dump_check_iter_info.clone();
            let status = last_check_status.get(&shard_id).unwrap_or(&Ok(
                StatePartsDumpCheckStatus::Waiting {
                    epoch_height: 0,
                    parts_done: false,
                    headers_done: false,
                },
            ));
            match status {
                Ok(StatePartsDumpCheckStatus::Done { epoch_height }) => {
                    tracing::info!(epoch_height, "last one was done.");
                    if *epoch_height >= dump_check_iter_info.epoch_height {
                        tracing::info!(
                            "current height was already checked. sleeping for {loop_interval}s."
                        );
                        sleep(Duration::from_secs(loop_interval));
                        continue;
                    }

                    tracing::info!("current height was not already checked, will start checking.");
                    if dump_check_iter_info.epoch_height > *epoch_height + 1 {
                        tracing::info!("there is a skip between last done epoch at epoch height: {epoch_height}, and latest available epoch at {}", dump_check_iter_info.epoch_height);
                        crate::metrics::STATE_SYNC_DUMP_CHECK_HAS_SKIPPED_EPOCH
                            .with_label_values(&[&shard_id.to_string(), &chain_id.to_string()])
                            .set(1);
                    } else {
                        crate::metrics::STATE_SYNC_DUMP_CHECK_HAS_SKIPPED_EPOCH
                            .with_label_values(&[&shard_id.to_string(), &chain_id.to_string()])
                            .set(0);
                    }
                    reset_num_parts_metrics(&chain_id, shard_id);
                }
                Ok(StatePartsDumpCheckStatus::Waiting {
                    epoch_height,
                    parts_done,
                    headers_done,
                }) => {
                    tracing::info!(epoch_height, "last one was waiting.");
                    if dump_check_iter_info.epoch_height > *epoch_height {
                        tracing::info!("last one was never finished. There is a skip between last waiting epoch at epoch height {epoch_height}, and latest available epoch at {}", dump_check_iter_info.epoch_height);
                        crate::metrics::STATE_SYNC_DUMP_CHECK_HAS_SKIPPED_EPOCH
                            .with_label_values(&[&shard_id.to_string(), &chain_id.to_string()])
                            .set(1);
                        reset_num_parts_metrics(&chain_id, shard_id);
                    } else {
                        // this check would be working on the same epoch as last check, so we don't reset the num parts metrics to 0 repeatedlyd
                        tracing::info!(
                            ?epoch_height,
                            ?parts_done,
                            ?headers_done,
                            "Still working on the same epoch as last check."
                        );
                    }
                }
                Err(_) => {
                    tracing::info!("last one errored out, will start check from the latest epoch");
                    reset_num_parts_metrics(&chain_id, shard_id);
                }
            }

            let chain_id = chain_id.clone();
            let root_dir = root_dir.clone();
            let s3_bucket = s3_bucket.clone();
            let s3_region = s3_region.clone();
            let gcs_bucket = gcs_bucket.clone();
            let old_status = status.as_ref().ok().cloned();
            let new_status = sys.block_on(async move {
                if !is_prometheus_server_up {
                    let server = HttpServer::new(move || {
                        App::new().service(
                            web::resource("/metrics")
                                .route(web::get().to(near_jsonrpc::prometheus_handler)),
                        )
                    })
                    .bind(prometheus_addr)?
                    .workers(1)
                    .shutdown_timeout(3)
                    .disable_signals()
                    .run();
                    tokio::spawn(server);
                }

                run_single_check_with_3_retries(
                    old_status,
                    chain_id,
                    dump_check_iter_info.epoch_id,
                    dump_check_iter_info.epoch_height,
                    shard_id as u64,
                    dump_check_iter_info.state_roots[shard_id as usize],
                    root_dir,
                    s3_bucket,
                    s3_region,
                    gcs_bucket,
                )
                .await
            });
            last_check_status.insert(shard_id, new_status);
            is_prometheus_server_up = true;
        }
    }
}

fn reset_num_parts_metrics(chain_id: &str, shard_id: ShardId) -> () {
    tracing::info!(shard_id, "Resetting num of parts metrics to 0.");
    crate::metrics::STATE_SYNC_DUMP_CHECK_NUM_PARTS_VALID
        .with_label_values(&[&shard_id.to_string(), chain_id])
        .set(0);
    crate::metrics::STATE_SYNC_DUMP_CHECK_NUM_PARTS_INVALID
        .with_label_values(&[&shard_id.to_string(), chain_id])
        .set(0);
    crate::metrics::STATE_SYNC_DUMP_CHECK_NUM_HEADERS_VALID
        .with_label_values(&[&shard_id.to_string(), chain_id])
        .set(0);
    crate::metrics::STATE_SYNC_DUMP_CHECK_NUM_HEADERS_INVALID
        .with_label_values(&[&shard_id.to_string(), chain_id])
        .set(0);
}

async fn run_single_check_with_3_retries(
    status: Option<StatePartsDumpCheckStatus>,
    chain_id: String,
    epoch_id: EpochId,
    epoch_height: u64,
    shard_id: ShardId,
    state_root: StateRoot,
    root_dir: Option<PathBuf>,
    s3_bucket: Option<String>,
    s3_region: Option<String>,
    gcs_bucket: Option<String>,
) -> anyhow::Result<StatePartsDumpCheckStatus> {
    let mut retries = 0;
    let mut res;
    loop {
        let chain_id = chain_id.clone();
        let root_dir = root_dir.clone();
        let s3_bucket = s3_bucket.clone();
        let s3_region = s3_region.clone();
        let gcs_bucket = gcs_bucket.clone();
        res = run_single_check(
            status.clone(),
            chain_id,
            epoch_id,
            epoch_height,
            shard_id,
            state_root,
            root_dir,
            s3_bucket,
            s3_region,
            gcs_bucket,
        )
        .await;
        match res {
            Ok(_) => {
                tracing::info!(shard_id, epoch_height, "run_single_check returned OK.",);
                break;
            }
            Err(_) if retries < MAX_RETRIES => {
                tracing::info!(shard_id, epoch_height, "run_single_check failure. Will retry.",);
                retries += 1;
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
            Err(_) => {
                tracing::info!(
                    shard_id,
                    epoch_height,
                    "run_single_check failure. No more retries."
                );
                break;
            }
        }
    }
    res
}

// download and validate state parts for a single epoch and shard
async fn check_parts(
    chain_id: &String,
    epoch_id: &EpochId,
    epoch_height: u64,
    shard_id: ShardId,
    state_root: StateRoot,
    external: &ExternalConnection,
) -> anyhow::Result<bool> {
    let directory_path = external_storage_location_directory(
        &chain_id,
        &epoch_id,
        epoch_height,
        shard_id,
        &StateFileType::StatePart { part_id: 0, num_parts: 0 },
    );
    tracing::info!(directory_path, "the storage location for the state parts being checked:");
    let part_file_names = external.list_objects(shard_id, &directory_path).await?;
    if part_file_names.is_empty() {
        return Ok(false);
    }
    let part_file_ids: HashSet<_> = part_file_names
        .iter()
        .map(|file_name| extract_part_id_from_part_file_name(file_name))
        .collect();
    let num_parts = part_file_ids.len() as u64;
    let total_required_parts = part_file_names
        .iter()
        .map(|file_name| get_num_parts_from_filename(file_name).unwrap())
        .min()
        .unwrap() as u64;

    tracing::info!(
        epoch_height,
        %state_root,
        total_required_parts,
        num_parts
    );

    crate::metrics::STATE_SYNC_DUMP_CHECK_NUM_PARTS_TOTAL
        .with_label_values(&[&shard_id.to_string(), &chain_id.to_string()])
        .set(total_required_parts as i64);
    crate::metrics::STATE_SYNC_DUMP_CHECK_NUM_PARTS_DUMPED
        .with_label_values(&[&shard_id.to_string(), &chain_id.to_string()])
        .set(num_parts as i64);

    if num_parts < total_required_parts {
        tracing::info!(
            epoch_height,
            shard_id,
            total_required_parts,
            num_parts,
            "Waiting for all parts to be dumped."
        );
        return Ok(false);
    } else if num_parts > total_required_parts {
        tracing::info!(
            epoch_height,
            shard_id,
            total_required_parts,
            num_parts,
            "There are more dumped parts than total required, something is seriously wrong."
        );
        return Ok(true);
    }

    tracing::info!(
        shard_id,
        epoch_height,
        num_parts,
        "Spawning threads to download and validate state parts."
    );

    let start = Instant::now();
    let mut handles = vec![];
    for part_id in 0..num_parts {
        let chain_id = chain_id.clone();
        let external = external.clone();
        let epoch_id = *epoch_id;
        let handle = tokio::spawn(async move {
            process_part_with_3_retries(
                part_id,
                chain_id,
                epoch_id,
                epoch_height,
                shard_id,
                state_root,
                num_parts,
                external,
            )
            .await
        });
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.await?;
    }

    let duration = start.elapsed();
    tracing::info!("Time elapsed in downloading and validating the parts is: {:?}", duration);
    Ok(true)
}

// download and validate state headers for a single epoch and shard
async fn check_headers(
    chain_id: &String,
    epoch_id: &EpochId,
    epoch_height: u64,
    shard_id: ShardId,
    external: &ExternalConnection,
) -> anyhow::Result<bool> {
    let directory_path = external_storage_location_directory(
        &chain_id,
        &epoch_id,
        epoch_height,
        shard_id,
        &StateFileType::StateHeader,
    );
    tracing::info!(directory_path, "the storage location for the state header being checked:");
    if !external
        .is_state_sync_header_stored_for_epoch(shard_id, chain_id, epoch_id, epoch_height)
        .await?
    {
        tracing::info!(epoch_height, shard_id, "Waiting for header to be dumped.");
        return Ok(false);
    }

    crate::metrics::STATE_SYNC_DUMP_CHECK_NUM_HEADERS_DUMPED
        .with_label_values(&[&shard_id.to_string(), &chain_id.to_string()])
        .set(1 as i64);

    tracing::info!(shard_id, epoch_height, "Download and validate state header.");

    let start = Instant::now();
    let chain_id = chain_id.clone();
    let external = external.clone();

    process_header_with_3_retries(chain_id, *epoch_id, epoch_height, shard_id, external).await?;

    let duration = start.elapsed();
    tracing::info!("Time elapsed in downloading and validating the header is: {:?}", duration);
    Ok(true)
}

async fn run_single_check(
    status: Option<StatePartsDumpCheckStatus>,
    chain_id: String,
    epoch_id: EpochId,
    current_epoch_height: u64,
    shard_id: ShardId,
    state_root: StateRoot,
    root_dir: Option<PathBuf>,
    s3_bucket: Option<String>,
    s3_region: Option<String>,
    gcs_bucket: Option<String>,
) -> anyhow::Result<StatePartsDumpCheckStatus> {
    tracing::info!(
        current_epoch_height,
        %state_root,
        "run_single_check for"
    );
    crate::metrics::STATE_SYNC_DUMP_CHECK_EPOCH_HEIGHT
        .with_label_values(&[&shard_id.to_string(), &chain_id.to_string()])
        .set(current_epoch_height as i64);

    crate::metrics::STATE_SYNC_DUMP_CHECK_PROCESS_IS_UP
        .with_label_values(&[&shard_id.to_string(), &chain_id.to_string()])
        .set(1);

    let external = create_external_connection(
        root_dir.clone(),
        s3_bucket.clone(),
        s3_region.clone(),
        gcs_bucket.clone(),
    );

    let (mut parts_done, mut headers_done) = match status {
        Some(StatePartsDumpCheckStatus::Done { epoch_height }) => {
            if epoch_height == current_epoch_height {
                (true, true)
            } else {
                (false, false)
            }
        }
        Some(StatePartsDumpCheckStatus::Waiting { parts_done, headers_done, epoch_height }) => {
            if epoch_height == current_epoch_height {
                (parts_done, headers_done)
            } else {
                (false, false)
            }
        }
        None => (false, false),
    };

    parts_done = parts_done
        || check_parts(&chain_id, &epoch_id, current_epoch_height, shard_id, state_root, &external)
            .await
            .unwrap_or(false);
    headers_done = headers_done
        || check_headers(&chain_id, &epoch_id, current_epoch_height, shard_id, &external)
            .await
            .unwrap_or(false);
    if !parts_done || !headers_done {
        Ok(StatePartsDumpCheckStatus::Waiting {
            epoch_height: current_epoch_height,
            parts_done,
            headers_done,
        })
    } else {
        Ok(StatePartsDumpCheckStatus::Done { epoch_height: current_epoch_height })
    }
}

async fn process_part_with_3_retries(
    part_id: u64,
    chain_id: String,
    epoch_id: EpochId,
    epoch_height: u64,
    shard_id: ShardId,
    state_root: StateRoot,
    num_parts: u64,
    external: ExternalConnection,
) -> anyhow::Result<()> {
    let mut retries = 0;
    let mut res;
    loop {
        let chain_id = chain_id.clone();
        let external = external.clone();
        // timeout is needed to deal with edge cases where process_part awaits forever, i.e. the get_file().await somehow waits forever
        // this is set to a long duration because the timer for each task, i.e. process_part, starts when the task is started, i.e. tokio::spawn is called,
        // and counts actual time instead of CPU time.
        // The bottom line is this timeout * MAX_RETRIES > time it takes for all parts to be processed, which is typically < 30 mins on monitoring nodes
        let timeout_duration = tokio::time::Duration::from_secs(600);
        res = timeout(
            timeout_duration,
            process_part(
                part_id,
                chain_id,
                epoch_id,
                epoch_height,
                shard_id,
                state_root,
                num_parts,
                external,
            ),
        )
        .await;
        match res {
            Ok(Ok(_)) => {
                tracing::info!(shard_id, epoch_height, part_id, "process_part success.",);
                break;
            }
            _ if retries < MAX_RETRIES => {
                tracing::info!(shard_id, epoch_height, part_id, "process_part failed. Will retry.",);
                retries += 1;
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            _ => {
                tracing::info!(
                    shard_id,
                    epoch_height,
                    part_id,
                    "process_part failed. No more retries.",
                );
                break;
            }
        }
    }
    res?
}

async fn process_header_with_3_retries(
    chain_id: String,
    epoch_id: EpochId,
    epoch_height: u64,
    shard_id: ShardId,
    external: ExternalConnection,
) -> anyhow::Result<()> {
    let mut retries = 0;
    let mut res: Result<Result<(), anyhow::Error>, tokio::time::error::Elapsed>;
    loop {
        let chain_id = chain_id.clone();
        let external = external.clone();
        let timeout_duration = tokio::time::Duration::from_secs(60);
        res = timeout(
            timeout_duration,
            process_header(chain_id, epoch_id, epoch_height, shard_id, external),
        )
        .await;
        match res {
            Ok(Ok(_)) => {
                tracing::info!(shard_id, epoch_height, "process_header success.",);
                break;
            }
            _ if retries < MAX_RETRIES => {
                tracing::info!(shard_id, epoch_height, ?res, "process_header failed. Will retry.",);
                retries += 1;
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            _ => {
                tracing::info!(
                    shard_id,
                    epoch_height,
                    ?res,
                    "process_header failed. No more retries.",
                );
                break;
            }
        }
    }
    res?
}

async fn process_part(
    part_id: u64,
    chain_id: String,
    epoch_id: EpochId,
    epoch_height: u64,
    shard_id: ShardId,
    state_root: StateRoot,
    num_parts: u64,
    external: ExternalConnection,
) -> anyhow::Result<()> {
    tracing::info!(part_id, "process_part started.");
    let file_type = StateFileType::StatePart { part_id, num_parts };
    let location =
        external_storage_location(&chain_id, &epoch_id, epoch_height, shard_id, &file_type);
    let part = external.get_file(shard_id, &location, &file_type).await?;
    let is_part_valid = validate_state_part(&state_root, PartId::new(part_id, num_parts), &part);
    if is_part_valid {
        crate::metrics::STATE_SYNC_DUMP_CHECK_NUM_PARTS_VALID
            .with_label_values(&[&shard_id.to_string(), &chain_id.to_string()])
            .inc();
        tracing::info!("part {part_id} is valid.");
    } else {
        crate::metrics::STATE_SYNC_DUMP_CHECK_NUM_PARTS_INVALID
            .with_label_values(&[&shard_id.to_string(), &chain_id.to_string()])
            .inc();
        tracing::info!("part {part_id} is invalid.");
    }
    Ok(())
}

async fn process_header(
    chain_id: String,
    epoch_id: EpochId,
    epoch_height: u64,
    shard_id: ShardId,
    external: ExternalConnection,
) -> anyhow::Result<()> {
    tracing::info!("process_header started.");
    let file_type = StateFileType::StateHeader;
    let location =
        external_storage_location(&chain_id, &epoch_id, epoch_height, shard_id, &file_type);
    let header = external.get_file(shard_id, &location, &file_type).await?;

    if validate_state_header(&header) {
        crate::metrics::STATE_SYNC_DUMP_CHECK_NUM_HEADERS_VALID
            .with_label_values(&[&shard_id.to_string(), &chain_id.to_string()])
            .inc();
        tracing::info!("header {shard_id} is valid.");
    } else {
        crate::metrics::STATE_SYNC_DUMP_CHECK_NUM_HEADERS_INVALID
            .with_label_values(&[&shard_id.to_string(), &chain_id.to_string()])
            .inc();
        tracing::info!("header {shard_id} is invalid.");
    }
    Ok(())
}

// get epoch information of the latest epoch that's complete
async fn get_processing_epoch_information(
    rpc_client: &JsonRpcClient,
) -> anyhow::Result<DumpCheckIterInfo> {
    let latest_block_response = rpc_client
        .block(BlockReference::Finality(Finality::Final))
        .await
        .or_else(|err| Err(anyhow!("get final block failed {err}")))?;
    let latest_epoch_id = latest_block_response.header.epoch_id;
    let latest_epoch_response = rpc_client
        .validators(Some(EpochReference::EpochId(EpochId(latest_epoch_id))))
        .await
        .or_else(|err| Err(anyhow!("validators_by_epoch_id for latest_epoch_id failed: {err}")))?;
    let latest_epoch_height = latest_epoch_response.epoch_height;
    let prev_epoch_last_block_response =
        get_previous_epoch_last_block_response(rpc_client, latest_epoch_id).await?;
    let mut chunks = prev_epoch_last_block_response.chunks;
    chunks.sort_by(|c1, c2| c1.shard_id.cmp(&c2.shard_id));
    let prev_epoch_state_roots: Vec<CryptoHash> =
        chunks.iter().map(|chunk| chunk.prev_state_root).collect();

    Ok(DumpCheckIterInfo {
        epoch_id: EpochId(latest_epoch_id),
        epoch_height: latest_epoch_height,
        state_roots: prev_epoch_state_roots,
    })
}

async fn get_previous_epoch_last_block_response(
    rpc_client: &JsonRpcClient,
    current_epoch_id: CryptoHash,
) -> anyhow::Result<BlockView> {
    let current_epoch_response = rpc_client
        .validators(Some(EpochReference::EpochId(EpochId(current_epoch_id))))
        .await
        .or_else(|_| Err(anyhow!("validators_by_epoch_id for current_epoch_id failed")))?;
    let current_epoch_first_block_height = current_epoch_response.epoch_start_height;
    let current_epoch_first_block_response = rpc_client
        .block_by_id(BlockId::Height(current_epoch_first_block_height))
        .await
        .or_else(|_| Err(anyhow!("block_by_id failed for current_epoch_first_block_height")))?;
    let prev_epoch_last_block_hash = current_epoch_first_block_response.header.prev_hash;
    let prev_epoch_last_block_response = rpc_client
        .block_by_id(BlockId::Hash(prev_epoch_last_block_hash))
        .await
        .or_else(|_| Err(anyhow!("block_by_id failed for prev_epoch_last_block_hash")))?;
    Ok(prev_epoch_last_block_response)
}
