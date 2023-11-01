use actix_web::{web, App, HttpServer};
use anyhow::anyhow;
use borsh::BorshDeserialize;
use near_client::sync::external::{create_bucket_readonly, ExternalConnection};
use near_client::sync::external::{
    external_storage_location, external_storage_location_directory, get_num_parts_from_filename,
};
use near_jsonrpc::client::{new_client, JsonRpcClient};
use near_primitives::hash::CryptoHash;
use near_primitives::state_part::PartId;
use near_primitives::types::{BlockId, BlockReference, EpochId, Finality, ShardId, StateRoot};
use near_primitives::views::BlockView;
use near_store::Trie;
use nearcore::state_sync::extract_part_id_from_part_file_name;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant};

const MAX_RETRIES: u32 = 3;

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
    /// Runs an infinite loop to download and validate state parts of all 4 shards for each epoch when it becomes available
    LoopCheck(LoopCheckCommand),
}

#[derive(clap::Parser)]
pub struct LoopCheckCommand {
    /// Listen address for prometheus metrics.
    #[clap(long, default_value = "0.0.0.0:9090")]
    prometheus_addr: String,
    // address of RPC server to retrieve latest block, epoch information
    #[clap(long)]
    rpc_server_addr: Option<String>,
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
                chain_id,
                self.epoch_id.clone(),
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
    // not all required parts are dumped for the epoch
    WaitingForParts { epoch_height: u64 },
}

#[derive(Clone)]
struct DumpCheckIterInfo {
    prev_epoch_id: EpochId,
    prev_epoch_height: u64,
    prev_epoch_state_roots: Vec<CryptoHash>,
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

// run an infinite loop to download and validate state parts for all shards whenever new epoch becomes available
fn run_loop_all_shards(
    chain_id: String,
    root_dir: Option<PathBuf>,
    s3_bucket: Option<String>,
    s3_region: Option<String>,
    gcs_bucket: Option<String>,
    rpc_client: &JsonRpcClient,
    prometheus_addr: &str,
) -> anyhow::Result<()> {
    let mut last_check_status_vec = vec![];
    for _ in 0..4 {
        last_check_status_vec
            .push(Ok(StatePartsDumpCheckStatus::WaitingForParts { epoch_height: 0 }));
    }

    let mut is_prometheus_server_up: bool = false;

    let sys = actix::System::new();
    loop {
        tracing::info!("running the loop inside run_loop_all_shards");
        let dump_check_iter_info_res =
            sys.block_on(async move { get_processing_epoch_information(&rpc_client).await });
        if let Err(err) = dump_check_iter_info_res {
            tracing::info!(
                "get_processing_epoch_information errs out with {}. sleeping for 60s.",
                err
            );
            sleep(Duration::from_secs(60));
            continue;
        }
        let dump_check_iter_info = dump_check_iter_info_res?;

        for shard_id in 0..4 as usize {
            tracing::info!("started check for shard_id {}", shard_id);
            let dump_check_iter_info = dump_check_iter_info.clone();
            match last_check_status_vec[shard_id] {
                Ok(StatePartsDumpCheckStatus::Done { epoch_height }) => {
                    tracing::info!("last one was done, epoch_height: {}", epoch_height);
                    if epoch_height >= dump_check_iter_info.prev_epoch_height {
                        tracing::info!("current height was already checked. sleeping for 60s.");
                        sleep(Duration::from_secs(60));
                        continue;
                    }

                    tracing::info!("current height was not already checked, will start checking.");
                    if dump_check_iter_info.prev_epoch_height > epoch_height + 1 {
                        tracing::info!("there is a skip between last done epoch at epoch_height: {}, and latest available epoch at {}", epoch_height, dump_check_iter_info.prev_epoch_height);
                        crate::metrics::STATE_SYNC_DUMP_CHECK_HAS_SKIPPED_EPOCH
                            .with_label_values(&[&shard_id.to_string(), &chain_id.to_string()])
                            .set(1);
                    } else {
                        crate::metrics::STATE_SYNC_DUMP_CHECK_HAS_SKIPPED_EPOCH
                            .with_label_values(&[&shard_id.to_string(), &chain_id.to_string()])
                            .set(0);
                    }
                }
                Ok(StatePartsDumpCheckStatus::WaitingForParts { epoch_height }) => {
                    tracing::info!("last one was waiting, epoch_height: {}", epoch_height);
                    if dump_check_iter_info.prev_epoch_height > epoch_height {
                        tracing::info!("last one was never finished. There is a skip between last waiting epoch at epoch_height: {}, and latest available epoch at {}", epoch_height, dump_check_iter_info.prev_epoch_height);
                        crate::metrics::STATE_SYNC_DUMP_CHECK_HAS_SKIPPED_EPOCH
                            .with_label_values(&[&shard_id.to_string(), &chain_id.to_string()])
                            .set(1);
                    } else {
                        tracing::info!("last one was waiting. Latest epoch is the same as last one waiting. Will recheck the same epoch at epoch_height: {}", epoch_height);
                        crate::metrics::STATE_SYNC_DUMP_CHECK_HAS_SKIPPED_EPOCH
                            .with_label_values(&[&shard_id.to_string(), &chain_id.to_string()])
                            .set(0);
                    }
                }
                Err(_) => {
                    tracing::info!("last one errored out, will start check from the latest epoch");
                }
            }

            let chain_id = chain_id.clone();
            let root_dir = root_dir.clone();
            let s3_bucket = s3_bucket.clone();
            let s3_region = s3_region.clone();
            let gcs_bucket = gcs_bucket.clone();
            last_check_status_vec[shard_id] = sys.block_on(async move {
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
                    chain_id,
                    dump_check_iter_info.prev_epoch_id,
                    dump_check_iter_info.prev_epoch_height,
                    shard_id as u64,
                    dump_check_iter_info.prev_epoch_state_roots[shard_id],
                    root_dir,
                    s3_bucket,
                    s3_region,
                    gcs_bucket,
                )
                .await
            });
            is_prometheus_server_up = true;
        }
    }
}

async fn run_single_check_with_3_retries(
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
        let epoch_id = epoch_id.clone();
        res = run_single_check(
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
                tracing::info!(
                    "run_single_check returned OK for shard_id: {}, epoch_height: {}",
                    shard_id,
                    epoch_height
                );
                break;
            }
            Err(_) if retries < MAX_RETRIES => {
                tracing::info!(
                    "run_single_check failure for shard_id: {}, epoch_height: {}. Retrying...",
                    shard_id,
                    epoch_height
                );
                retries += 1;
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
            Err(_) => {
                tracing::info!(
                    "run_single_check failure for shard_id: {}, epoch_height: {}. No more retries",
                    shard_id,
                    epoch_height
                );
                break;
            }
        }
    }
    res
}

// download and validate state parts for a single epoch and shard
async fn run_single_check(
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
    tracing::info!(
        "run_single_check for epoch_height: {}, state_root: {}",
        epoch_height,
        state_root
    );
    crate::metrics::STATE_SYNC_DUMP_CHECK_EPOCH_HEIGHT
        .with_label_values(&[&shard_id.to_string(), &chain_id.to_string()])
        .set(epoch_height as i64);

    crate::metrics::STATE_SYNC_DUMP_CHECK_PROCESS_IS_UP
        .with_label_values(&[&shard_id.to_string(), &chain_id.to_string()])
        .set(1);

    crate::metrics::STATE_SYNC_DUMP_CHECK_NUM_PARTS_TOTAL
        .with_label_values(&[&shard_id.to_string(), &chain_id.to_string()])
        .set(0);
    crate::metrics::STATE_SYNC_DUMP_CHECK_NUM_PARTS_DUMPED
        .with_label_values(&[&shard_id.to_string(), &chain_id.to_string()])
        .set(0);
    crate::metrics::STATE_SYNC_DUMP_CHECK_NUM_PARTS_VALID
        .with_label_values(&[&shard_id.to_string(), &chain_id.to_string()])
        .set(0);
    crate::metrics::STATE_SYNC_DUMP_CHECK_NUM_PARTS_INVALID
        .with_label_values(&[&shard_id.to_string(), &chain_id.to_string()])
        .set(0);

    let external = create_external_connection(
        root_dir.clone(),
        s3_bucket.clone(),
        s3_region.clone(),
        gcs_bucket.clone(),
    );

    let directory_path =
        external_storage_location_directory(&chain_id, &epoch_id, epoch_height, shard_id);
    let part_file_names = external.list_state_parts(shard_id, &directory_path).await?;
    if part_file_names.is_empty() {
        return Ok(StatePartsDumpCheckStatus::WaitingForParts { epoch_height: epoch_height });
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
        "epoch_height: {}, state_root: {}, total state parts required: {},  num parts: {}",
        epoch_height,
        state_root,
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
        tracing::info!("total state parts required: {} < number of parts already dumped: {}, waiting for all parts to be dumped", total_required_parts, num_parts);
        return Ok(StatePartsDumpCheckStatus::WaitingForParts { epoch_height: epoch_height });
    } else if num_parts > total_required_parts {
        tracing::info!("total state parts required: {} > number of parts already dumped: {}, there are more dumped parts than total required, something is seriously wrong", total_required_parts, num_parts);
        return Ok(StatePartsDumpCheckStatus::Done { epoch_height: epoch_height });
    }

    tracing::info!(
        "Spawning threads to download and validate state parts for shard_id: {}, epoch_height: {}, num_parts: {}",
        shard_id, epoch_height, num_parts
    );

    let start = Instant::now();
    let mut handles = vec![];
    for part_id in 0..num_parts {
        let chain_id = chain_id.clone();
        let epoch_id = epoch_id.clone();
        let external = external.clone();
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
        let _ = handle.await.unwrap();
    }

    let duration = start.elapsed();
    tracing::info!("Time elapsed in downloading and validating the parts is: {:?}", duration);
    Ok(StatePartsDumpCheckStatus::Done { epoch_height: epoch_height })
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
        let epoch_id = epoch_id.clone();
        let external = external.clone();
        res = process_part(
            part_id,
            chain_id,
            epoch_id,
            epoch_height,
            shard_id,
            state_root,
            num_parts,
            external,
        )
        .await;
        match res {
            Ok(_) => {
                tracing::info!(
                    "process_part success for shard_id: {}, epoch_height: {}, part_id: {}",
                    shard_id,
                    epoch_height,
                    part_id
                );
                break;
            }
            Err(_) if retries < MAX_RETRIES => {
                tracing::info!(
                            "process_part failure for shard_id: {}, epoch_height: {}, part_id: {}. Retrying...",
                            shard_id, epoch_height, part_id
                        );
                retries += 1;
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            Err(_) => {
                tracing::info!(
                            "process_part failure for shard_id: {}, epoch_height: {}, part_id: {}. No more retries",
                            shard_id, epoch_height, part_id
                        );
                break;
            }
        }
    }
    res
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
    tracing::info!("process_part for {} started", part_id);
    let location =
        external_storage_location(&chain_id, &epoch_id, epoch_height, shard_id, part_id, num_parts);
    let part = external.get_part(shard_id, &location).await?;
    let is_part_valid = validate_state_part(&state_root, PartId::new(part_id, num_parts), &part);
    if is_part_valid {
        crate::metrics::STATE_SYNC_DUMP_CHECK_NUM_PARTS_VALID
            .with_label_values(&[&shard_id.to_string(), &chain_id.to_string()])
            .inc();
        tracing::info!("part {} is valid", part_id);
    } else {
        crate::metrics::STATE_SYNC_DUMP_CHECK_NUM_PARTS_INVALID
            .with_label_values(&[&shard_id.to_string(), &chain_id.to_string()])
            .inc();
        tracing::info!("part {} is invalid", part_id);
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
        .or_else(|_| Err(anyhow!("get final block failed")))?;
    let latest_epoch_id = latest_block_response.header.epoch_id;
    let prev_epoch_last_block_response =
        get_previous_epoch_last_block_response(rpc_client, latest_epoch_id).await?;
    let prev_epoch_id = prev_epoch_last_block_response.header.epoch_id;
    let prev_epoch_response = rpc_client
        .validators_by_epoch_id(EpochId(prev_epoch_id))
        .await
        .or_else(|_| Err(anyhow!("validators_by_epoch_id for prev_epoch_id failed")))?;
    let prev_epoch_height = prev_epoch_response.epoch_height;
    let prev_prev_epoch_last_block_response =
        get_previous_epoch_last_block_response(rpc_client, prev_epoch_id).await?;
    let shard_ids: Vec<usize> = (0..4).collect();
    // state roots ordered by shard_id
    let prev_epoch_state_roots: Vec<CryptoHash> = shard_ids
        .iter()
        .map(|&shard_id| prev_prev_epoch_last_block_response.chunks[shard_id].prev_state_root)
        .collect();

    Ok(DumpCheckIterInfo {
        prev_epoch_id: EpochId(prev_epoch_id),
        prev_epoch_height: prev_epoch_height,
        prev_epoch_state_roots: prev_epoch_state_roots,
    })
}

async fn get_previous_epoch_last_block_response(
    rpc_client: &JsonRpcClient,
    current_epoch_id: CryptoHash,
) -> anyhow::Result<BlockView> {
    let current_epoch_response = rpc_client
        .validators_by_epoch_id(EpochId(current_epoch_id))
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
