use near_client::sync::external::{
    external_storage_location, external_storage_location_directory, get_num_parts_from_filename,
};
extern crate rayon;
use near_primitives::hash::CryptoHash;
use near_primitives::state_part::PartId;
use near_primitives::types::{EpochId, ShardId, StateRoot};
use rayon::prelude::*;

use anyhow::anyhow;
use borsh::BorshDeserialize;
use near_client::sync::external::{create_bucket_readonly, ExternalConnection};
use near_store::Trie;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use nearcore::state_sync::extract_part_id_from_part_file_name;
use std::collections::HashSet;

#[derive(clap::Parser)]
pub struct StatePartsDumpCheckCommand {
    #[clap(long)]
    chain_id: String,
    #[clap(long)]
    shard_id: ShardId,
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
pub struct LoopCheckCommand {}

#[derive(clap::Parser)]
pub struct SingleCheckCommand {
    #[clap(long)]
    epoch_id: EpochId,
    #[clap(long)]
    epoch_height: u64,
    #[clap(long)]
    state_root: StateRoot,
}

#[derive(Clone)]
pub enum StatePartsDumpCheckStatus {
    Done { epoch_height: u64 },
    WaitingForParts { epoch_height: u64 },
}

impl StatePartsDumpCheckCommand {
    pub fn run(
        &self
    ) -> anyhow::Result<StatePartsDumpCheckStatus> {
        self.subcmd.run(self.chain_id.clone(), self.shard_id, self.root_dir.clone(), self.s3_bucket.clone(), self.s3_region.clone(), self.gcs_bucket.clone())
    }
}

impl StatePartsDumpCheckSubCommand {
    pub fn run(
        &self,
        chain_id: String,
        shard_id: ShardId,
        root_dir: Option<PathBuf>,
        s3_bucket: Option<String>,
        s3_region: Option<String>,
        gcs_bucket: Option<String>,
    ) -> anyhow::Result<StatePartsDumpCheckStatus> {
        match self {
            StatePartsDumpCheckSubCommand::SingleCheck(cmd) => cmd.run(
                chain_id,
                shard_id,
                root_dir,
                s3_bucket,
                s3_region,
                gcs_bucket
            ),
            StatePartsDumpCheckSubCommand::LoopCheck(cmd) => cmd.run(
                chain_id,
                shard_id,
                root_dir,
                s3_bucket,
                s3_region,
                gcs_bucket
            )
        }   
    }
}

impl SingleCheckCommand {
    pub fn run(
        &self,
        chain_id: String,
        shard_id: ShardId,
        root_dir: Option<PathBuf>,
        s3_bucket: Option<String>,
        s3_region: Option<String>,
        gcs_bucket: Option<String>,
    ) -> anyhow::Result<StatePartsDumpCheckStatus> {
        let sys = actix::System::new();
        sys.block_on(async move {
            let check_result = run_single_check(
                chain_id,
                self.epoch_id.clone(),
                self.epoch_height,
                shard_id,
                self.state_root,
                root_dir,
                s3_bucket,
                s3_region,
                gcs_bucket,
            )
            .await?;
            Ok(check_result)
        })
    }
}

impl LoopCheckCommand {
    pub fn run(
        &self,
        chain_id: String,
        shard_id: ShardId,
        root_dir: Option<PathBuf>,
        s3_bucket: Option<String>,
        s3_region: Option<String>,
        gcs_bucket: Option<String>,
    ) -> anyhow::Result<StatePartsDumpCheckStatus> {
        let rpc_client = crate::rpc_requests::RpcClient::new("mainnet");

        let mut last_check_status =
            Ok(StatePartsDumpCheckStatus::WaitingForParts { epoch_height: 0 });

        let mut last_check_iter_info;

        loop {
            let dump_check_iter_info_res = get_processing_epoch_information(&rpc_client);
            if let Err(_) = dump_check_iter_info_res {
                // TODO: sleep 5 mins
                continue;
            }
            let dump_check_iter_info = dump_check_iter_info_res.unwrap();

            match last_check_status {
                Ok(StatePartsDumpCheckStatus::Done{epoch_height}) | Ok(StatePartsDumpCheckStatus::WaitingForParts{epoch_height}) => {
                    if epoch_height >= dump_check_iter_info.prev_epoch_height {
                        // TODO: sleep 5 mins
                        continue;
                    }
    
                    if dump_check_iter_info.prev_epoch_height > epoch_height + 1 {
                        crate::metrics::STATE_SYNC_DUMP_CHECK_HAS_SKIPPED_EPOCH
                            .with_label_values(&[&shard_id.to_string()])
                            .set(1);  
                    } else {
                        crate::metrics::STATE_SYNC_DUMP_CHECK_HAS_SKIPPED_EPOCH
                            .with_label_values(&[&shard_id.to_string()])
                            .set(0); 
                    }
                }
                Err(_) => ()
            }

            let state_root_str = rpc_client
                .get_prev_epoch_state_root(&dump_check_iter_info.prev_epoch_id, shard_id)
                .or_else(|_| Err(anyhow!("get_prev_epoch_state_root failed")))?;

            let state_root: StateRoot = CryptoHash::from_str(&state_root_str).or_else(|_| Err(anyhow!("convert str to StateRoot failed")))?;
            let prev_epoch_id = EpochId(CryptoHash::from_str(&dump_check_iter_info.prev_epoch_id).or_else(|_| Err(anyhow!("convert str to EpochId failed")))?);

            last_check_iter_info = dump_check_iter_info;
            let sys = actix::System::new();
            let chain_id = chain_id.clone();
            let root_dir = root_dir.clone();
            let s3_bucket = s3_bucket.clone();
            let s3_region = s3_region.clone();
            let gcs_bucket = gcs_bucket.clone();
            last_check_status = sys.block_on(async move {
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
        }
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
    print!("epoch_height: {}, state_root: {}", epoch_height, state_root);
    crate::metrics::STATE_SYNC_DUMP_CHECK_EPOCH_HEIGHT
        .with_label_values(&[&shard_id.to_string()])
        .set(epoch_height as i64);

    crate::metrics::STATE_SYNC_DUMP_CHECK_PROCESS_IS_UP.set(1);

    crate::metrics::STATE_SYNC_DUMP_CHECK_NUM_PARTS_TOTAL
        .with_label_values(&[&shard_id.to_string()])
        .set(0);
    crate::metrics::STATE_SYNC_DUMP_CHECK_NUM_PARTS_DUMPED
        .with_label_values(&[&shard_id.to_string()])
        .set(0);
    crate::metrics::STATE_SYNC_DUMP_CHECK_NUM_PARTS_VALID
        .with_label_values(&[&shard_id.to_string()])
        .set(0);
    crate::metrics::STATE_SYNC_DUMP_CHECK_NUM_PARTS_INVALID
        .with_label_values(&[&shard_id.to_string()])
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
        .min().unwrap();

    crate::metrics::STATE_SYNC_DUMP_CHECK_NUM_PARTS_TOTAL
        .with_label_values(&[&shard_id.to_string()])
        .set(total_required_parts as i64);
    crate::metrics::STATE_SYNC_DUMP_CHECK_NUM_PARTS_DUMPED
        .with_label_values(&[&shard_id.to_string()])
        .set(num_parts as i64);

    if num_parts < total_required_parts {
        println!("total state parts required: {} < number of parts already dumped: {}, waiting for all parts to be dumped", total_required_parts, num_parts);
        return Ok(StatePartsDumpCheckStatus::WaitingForParts { epoch_height: epoch_height });
    } else if num_parts > total_required_parts {
        println!("total state parts required: {} > number of parts already dumped: {}, there are more dumped parts than total required, something is seriously wrong", total_required_parts, num_parts);
        return Ok(StatePartsDumpCheckStatus::Done { epoch_height: epoch_height });
    }

    // TODO: run in parallel with rayon
    for part_id in 0..total_required_parts {
        if part_id == 0 {
            crate::metrics::STATE_SYNC_DUMP_CHECK_NUM_PARTS_VALID
                .with_label_values(&[&shard_id.to_string()])
                .set(0);
            crate::metrics::STATE_SYNC_DUMP_CHECK_NUM_PARTS_INVALID
                .with_label_values(&[&shard_id.to_string()])
                .set(0);
        }
        let location = external_storage_location(
            &chain_id,
            &epoch_id,
            epoch_height,
            shard_id,
            part_id,
            num_parts,
        );
        let part = external.get_part(shard_id, &location).await?;
        let is_part_valid =
            validate_state_part(&state_root, PartId::new(part_id, num_parts), &part);
        if is_part_valid {
            crate::metrics::STATE_SYNC_DUMP_CHECK_NUM_PARTS_VALID
                .with_label_values(&[&shard_id.to_string()])
                .inc();
            println!("part {} is valid", part_id);
        } else {
            crate::metrics::STATE_SYNC_DUMP_CHECK_NUM_PARTS_INVALID
                .with_label_values(&[&shard_id.to_string()])
                .inc();
            println!("part {} is invalid", part_id);
        }
    }
    Ok(StatePartsDumpCheckStatus::Done { epoch_height: epoch_height })
}

fn get_processing_epoch_information(
    rpc_client: &crate::rpc_requests::RpcClient,
) -> anyhow::Result<DumpCheckIterInfo> {
    let latest_block_hash =
        rpc_client.get_final_block_hash().or_else(|_| Err(anyhow!("get_final_block_hash failed")))?;
    let latest_epoch_id =
        rpc_client.get_epoch_id(&latest_block_hash).or_else(|_| Err(anyhow!("get_epoch_id failed")))?;

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
