use near_client::sync::external::{
    external_storage_location, external_storage_location_directory, get_num_parts_from_filename
};
extern crate rayon;
use rayon::prelude::*;
use near_primitives::state_part::PartId;
use near_primitives::types::{EpochId, ShardId, StateRoot};

use borsh::BorshDeserialize;
use near_client::sync::external::{create_bucket_readonly, ExternalConnection};
use near_store::Trie;
use std::time::Duration;
use std::path::PathBuf;
use std::sync::Arc;
use anyhow::anyhow;

pub struct StatePartsDumpCheck {
    chain_id: String,
    epoch_id: EpochId,
    epoch_height: u64,
    shard_id: ShardId,
    state_root: StateRoot,
    root_dir: Option<PathBuf>,
    s3_bucket: Option<String>,
    s3_region: Option<String>,
    gcs_bucket: Option<String>,
}

pub enum StatePartsDumpCheckStatus {
    Done {
        epoch_height: u64
    },
    WaitingForParts{
        epoch_height: u64
    },
}

impl StatePartsDumpCheck {
    pub fn new(
        chain_id: String,
        epoch_id: EpochId,
        epoch_height: u64,
        shard_id: ShardId,
        state_root: StateRoot,
        root_dir: Option<PathBuf>,
        s3_bucket: Option<String>,
        s3_region: Option<String>,
        gcs_bucket: Option<String>,
    ) -> Self {
        Self {
            chain_id,
            epoch_id,
            epoch_height,
            shard_id,
            state_root,
            root_dir,
            s3_bucket,
            s3_region, 
            gcs_bucket
        }
    }
    
    pub async fn run(&self) -> anyhow::Result<StatePartsDumpCheckStatus> {
        crate::metrics::STATE_SYNC_DUMP_CHECK_EPOCH_HEIGHT.with_label_values(&[&self.shard_id.to_string()]).set(self.epoch_height as i64);

        let external = create_external_connection(
            self.root_dir.clone(),
            self.s3_bucket.clone(),
            self.s3_region.clone(),
            self.gcs_bucket.clone(),
        );

        let directory_path = external_storage_location_directory(&self.chain_id, &self.epoch_id, self.epoch_height, self.shard_id);
        let part_file_names = external.list_state_parts(self.shard_id, &directory_path).await?;
        // TODO: return error instead of assert!()
        assert!(!part_file_names.is_empty(), "Number of state parts is 0");
        let num_parts = part_file_names.len() as u64;
        let total_required_parts = get_num_parts_from_filename(&part_file_names[0]).ok_or(anyhow!("get_num_parts_from_filename returned None"))?;
        
        crate::metrics::STATE_SYNC_DUMP_CHECK_NUM_PARTS_TOTAL.with_label_values(&[&self.shard_id.to_string()]).set(total_required_parts as i64);
        crate::metrics::STATE_SYNC_DUMP_CHECK_NUM_PARTS_DUMPED.with_label_values(&[&self.shard_id.to_string()]).set(num_parts as i64);

        if num_parts < total_required_parts {
            println!("total state parts required: {} < number of parts already dumped: {}, waiting for all parts to be dumped", total_required_parts, num_parts);
            return Ok(StatePartsDumpCheckStatus::WaitingForParts { epoch_height: self.epoch_height })
        } else if num_parts > total_required_parts{
            println!("total state parts required: {} > number of parts already dumped: {}, there are more dumped parts than total required, something is seriously wrong", total_required_parts, num_parts);
            return Ok(StatePartsDumpCheckStatus::Done { epoch_height: self.epoch_height })
        }

        // TODO: run in parallel with rayon
        for part_id in 0..total_required_parts {
            if part_id == 0 {
                crate::metrics::STATE_SYNC_DUMP_CHECK_NUM_PARTS_VALID.with_label_values(&[&self.shard_id.to_string()]).set(0);
                crate::metrics::STATE_SYNC_DUMP_CHECK_NUM_PARTS_INVALID.with_label_values(&[&self.shard_id.to_string()]).set(0);
            }
            let location = external_storage_location(
                &self.chain_id,
                &self.epoch_id,
                self.epoch_height,
                self.shard_id,
                part_id,
                num_parts,
            );
            let part = external.get_part(self.shard_id, &location).await?;
            let is_part_valid = validate_state_part(
                &self.state_root,
                PartId::new(part_id, num_parts),
                &part
            );
            if is_part_valid {
                crate::metrics::STATE_SYNC_DUMP_CHECK_NUM_PARTS_VALID.with_label_values(&[&self.shard_id.to_string()]).inc();
                println!("part {} is valid", part_id);
            } else {
                crate::metrics::STATE_SYNC_DUMP_CHECK_NUM_PARTS_INVALID.with_label_values(&[&self.shard_id.to_string()]).inc();
                println!("part {} is invalid", part_id);
            }
        };
        Ok(StatePartsDumpCheckStatus::Done { epoch_height: self.epoch_height })
    }
}


pub fn create_external_connection(
    root_dir: Option<PathBuf>,
    bucket: Option<String>,
    region: Option<String>,
    gcs_bucket: Option<String>
) -> ExternalConnection {
    if let Some(root_dir) = root_dir {
        ExternalConnection::Filesystem { root_dir }
    } else if let (Some(bucket), Some(region)) = (bucket, region) {
        let bucket = create_bucket_readonly(&bucket, &region, Duration::from_secs(5)).expect("Failed to create an S3 bucket");
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

