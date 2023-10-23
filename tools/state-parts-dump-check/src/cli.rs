use near_client::sync::external::{
    external_storage_location, external_storage_location_directory, get_num_parts_from_filename,
};
extern crate rayon;
use actix_web::{web, App, HttpServer};
use anyhow::anyhow;
use borsh::BorshDeserialize;
use near_client::sync::external::{create_bucket_readonly, ExternalConnection};
use near_primitives::hash::CryptoHash;
use near_primitives::state_part::PartId;
use near_primitives::types::{EpochId, ShardId, StateRoot};
use near_store::Trie;
use nearcore::state_sync::extract_part_id_from_part_file_name;
use std::collections::HashSet;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant};

#[derive(clap::Parser)]
pub struct StatePartsDumpCheckCommand {
    #[clap(long)]
    chain_id: String,
    #[clap(long, value_parser)]
    root_dir: Option<PathBuf>,
    #[clap(long)]
    s3_bucket: Option<String>,
    #[clap(long)]
    s3_region: Option<String>,
    #[clap(long)]
    gcs_bucket: Option<String>,
    #[clap(subcommand)]
    subcmd: StatePartsDumpCheckSubCommand,
}

#[derive(clap::Subcommand)]
pub enum StatePartsDumpCheckSubCommand {
    SingleCheck(SingleCheckCommand),
    LoopCheck(LoopCheckCommand),
}

#[derive(clap::Parser)]
pub struct LoopCheckCommand {
    /// Listen address for prometheus metrics.
    #[clap(long, default_value = "0.0.0.0:9090")]
    prometheus_addr: String,
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

#[derive(Clone)]
pub enum StatePartsDumpCheckStatus {
    Done { epoch_height: u64 },
    WaitingForParts { epoch_height: u64 },
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
    pub fn run(
        &self,
        chain_id: String,
        root_dir: Option<PathBuf>,
        s3_bucket: Option<String>,
        s3_region: Option<String>,
        gcs_bucket: Option<String>,
    ) -> anyhow::Result<()> {
        let sys = actix::System::new();
        sys.block_on(async move {
            let _check_result = run_single_check(
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
    fn run(
        &self,
        chain_id: String,
        root_dir: Option<PathBuf>,
        s3_bucket: Option<String>,
        s3_region: Option<String>,
        gcs_bucket: Option<String>,
    ) -> anyhow::Result<()> {
        let rpc_client = crate::rpc_requests::RpcClient::new(&chain_id);

        tracing::info!("started running LoopCheckCommand");

        let result = run_loop_single_shard(
            chain_id,
            root_dir,
            s3_bucket,
            s3_region,
            gcs_bucket,
            rpc_client,
            &self.prometheus_addr,
        );

        tracing::info!("run_loop_single_shard finished");

        match result {
            Ok(_) => {
                tracing::info!("run_loop_single_shard returned OK()");
            }
            Err(err) => {
                tracing::info!("run_loop_single_shard errored out with {}", err);
            }
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct DumpCheckIterInfo {
    pub latest_block_hash: String,
    pub latest_epoch_id: String,
    pub prev_epoch_id: String,
    pub prev_epoch_height: u64,
}

pub fn create_external_connection(
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

fn run_loop_single_shard(
    chain_id: String,
    root_dir: Option<PathBuf>,
    s3_bucket: Option<String>,
    s3_region: Option<String>,
    gcs_bucket: Option<String>,
    rpc_client: crate::rpc_requests::RpcClient,
    prometheus_addr: &str,
) -> anyhow::Result<()> {
    let mut last_check_status_vec = vec![];
    for _ in 0..4 {
        last_check_status_vec
            .push(Ok(StatePartsDumpCheckStatus::WaitingForParts { epoch_height: 0 }));
    }
    let mut last_check_iter_info: DumpCheckIterInfo;

    let mut is_prometheus_server_up: bool = false;

    let sys = actix::System::new();
    loop {
        tracing::info!("running the loop inside run_loop_single_shard");
        let dump_check_iter_info_res = get_processing_epoch_information(&rpc_client);
        if let Err(_) = dump_check_iter_info_res {
            tracing::info!("sleeping for 60s");
            sleep(Duration::from_secs(60));
            continue;
        }
        let dump_check_iter_info = dump_check_iter_info_res.unwrap();

        for shard_id in 0..4 as u64 {
            tracing::info!("started check for shard_id {}", shard_id);
            let dump_check_iter_info = dump_check_iter_info.clone();
            match last_check_status_vec[shard_id as usize] {
                Ok(StatePartsDumpCheckStatus::Done { epoch_height })
                | Ok(StatePartsDumpCheckStatus::WaitingForParts { epoch_height }) => {
                    tracing::info!("last one was done or waiting");
                    if epoch_height >= dump_check_iter_info.prev_epoch_height {
                        tracing::info!("current height was already checked");
                        tracing::info!("sleeping for 60s");
                        sleep(Duration::from_secs(60));
                        continue;
                    }

                    tracing::info!("current height was not already checked");
                    if dump_check_iter_info.prev_epoch_height > epoch_height + 1 {
                        crate::metrics::STATE_SYNC_DUMP_CHECK_HAS_SKIPPED_EPOCH
                            .with_label_values(&[&shard_id.to_string(), &chain_id.to_string()])
                            .set(1);
                    } else {
                        crate::metrics::STATE_SYNC_DUMP_CHECK_HAS_SKIPPED_EPOCH
                            .with_label_values(&[&shard_id.to_string(), &chain_id.to_string()])
                            .set(0);
                    }
                }
                Err(_) => (),
            }

            let state_root_str = rpc_client
                .get_prev_epoch_state_root(&dump_check_iter_info.prev_epoch_id, shard_id)
                .or_else(|_| Err(anyhow!("get_prev_epoch_state_root failed")))?;

            let state_root: StateRoot = CryptoHash::from_str(&state_root_str)
                .or_else(|_| Err(anyhow!("convert str to StateRoot failed")))?;
            let prev_epoch_id = EpochId(
                CryptoHash::from_str(&dump_check_iter_info.prev_epoch_id)
                    .or_else(|_| Err(anyhow!("convert str to EpochId failed")))?,
            );

            last_check_iter_info = dump_check_iter_info;
            let chain_id = chain_id.clone();
            let root_dir = root_dir.clone();
            let s3_bucket = s3_bucket.clone();
            let s3_region = s3_region.clone();
            let gcs_bucket = gcs_bucket.clone();
            last_check_status_vec[shard_id as usize] = sys.block_on(async move {
                if !is_prometheus_server_up {
                    let server = HttpServer::new(move || {
                        App::new().service(
                            web::resource("/metrics")
                                .route(web::get().to(near_jsonrpc::prometheus_handler)),
                        )
                    })
                    .bind(prometheus_addr)
                    .unwrap()
                    .workers(1)
                    .shutdown_timeout(3)
                    .disable_signals()
                    .run();
                    tokio::spawn(server);
                }

                run_single_check(
                    chain_id,
                    prev_epoch_id,
                    last_check_iter_info.prev_epoch_height,
                    shard_id,
                    state_root,
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
    tracing::info!("epoch_height: {}, state_root: {}", epoch_height, state_root);
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
    // TODO: return error instead of assert!()
    assert!(!part_file_names.is_empty(), "Number of state parts is 0");
    let part_file_ids: HashSet<_> = part_file_names
        .iter()
        .map(|file_name| extract_part_id_from_part_file_name(file_name))
        .collect();
    let num_parts = part_file_ids.len() as u64;
    let total_required_parts = part_file_names
        .iter()
        .map(|file_name| get_num_parts_from_filename(file_name).unwrap())
        .min()
        .unwrap();

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
        //target: "store", %shard_id, %epoch_height, %num_parts,
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
            const MAX_RETRIES: u32 = 3;
            let mut retries = 0;
            loop {
                let chain_id = chain_id.clone();
                let epoch_id = epoch_id.clone();
                let external = external.clone();
                match process_part(
                    part_id,
                    chain_id,
                    epoch_id,
                    epoch_height,
                    shard_id,
                    state_root,
                    num_parts,
                    external,
                ).await {
                    Ok(_) => {
                        tracing::info!(
                            //target: "store", %shard_id, %epoch_height, %num_parts,
                            "Success for shard_id: {}, epoch_height: {}, part_id: {}",
                            shard_id, epoch_height, part_id
                        );
                        break;
                    },
                    Err(e) if retries < MAX_RETRIES => {
                        tracing::info!(
                            //target: "store", %shard_id, %epoch_height, %num_parts,
                            "Failure for shard_id: {}, epoch_height: {}, part_id: {}, with error: {}. Retrying...",
                            shard_id, epoch_height, part_id, e
                        );
                        retries += 1;
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    },
                    Err(e) => {
                        tracing::info!(
                            //target: "store", %shard_id, %epoch_height, %num_parts,
                            "Failure for shard_id: {}, epoch_height: {}, part_id: {}, with error: {}. No more retries",
                            shard_id, epoch_height, part_id, e
                        );
                        break;
                    },
                }
            }
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

fn get_processing_epoch_information(
    rpc_client: &crate::rpc_requests::RpcClient,
) -> anyhow::Result<DumpCheckIterInfo> {
    let latest_block_hash = rpc_client
        .get_final_block_hash()
        .or_else(|_| Err(anyhow!("get_final_block_hash failed")))?;
    let latest_epoch_id = rpc_client
        .get_epoch_id(&latest_block_hash)
        .or_else(|_| Err(anyhow!("get_epoch_id failed")))?;

    let prev_epoch_id_str = rpc_client
        .get_prev_epoch_id(&latest_epoch_id)
        .or_else(|_| Err(anyhow!("get_prev_epoch_id failed")))?;
    let prev_epoch_height = rpc_client
        .get_epoch_height(&latest_epoch_id)
        .or_else(|_| Err(anyhow!("get_epoch_height failed")))?
        - 1;

    Ok(DumpCheckIterInfo {
        latest_block_hash: latest_block_hash,
        latest_epoch_id: latest_epoch_id,
        prev_epoch_id: prev_epoch_id_str,
        prev_epoch_height: prev_epoch_height,
    })
}
