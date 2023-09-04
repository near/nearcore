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

// instead of listing all subdirectories inside gcp/s3 bucket, the script can query near rpc endpoint for latest epoch_height
// if the latest epoch_height > the last time we polled for epoch_height, and epoch_dump_check_progress = Done, then it means
// the check for last epoch was done, and we should check for last epoch_height + 1.
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
    pub async fn run(&self) -> anyhow::Result<()> {
        // TODO: emit metric for epoch_height

        let external = create_external_connection(
            self.root_dir.clone(),
            self.s3_bucket.clone(),
            self.s3_region.clone(),
            self.gcs_bucket.clone(),
        );

        let directory_path = external_storage_location_directory(&self.chain_id, &self.epoch_id, self.epoch_height, self.shard_id);
        let part_file_names = external.list_state_parts(self.shard_id, &directory_path).await.unwrap();
        assert!(!part_file_names.is_empty(), "Number of state parts is 0");
        let num_parts = part_file_names.len() as u64;
        let total_required_parts = get_num_parts_from_filename(&part_file_names[0]).unwrap();
        if num_parts < total_required_parts {
            println!("total state parts required: {} < number of parts already dumped: {}, waiting for all parts to be dumped", total_required_parts, num_parts);
            return Ok(())
        } else if num_parts > total_required_parts{
            println!("total state parts required: {} > number of parts already dumped: {}, something is seriously wrong", total_required_parts, num_parts);
            // TODO: emit metric indicating that dumped parts > total required parts
            return Ok(())
        }
        
        // only download and validate when all parts are dumped
        // TODO: run in parallel with rayon
        for part_id in 0..total_required_parts {
            if part_id == 0 {
                // TODO: reset the metrics of valid parts and invalid parts count to zero
            }
            let location = external_storage_location(
                &self.chain_id,
                &self.epoch_id,
                self.epoch_height,
                self.shard_id,
                part_id,
                num_parts,
            );
            let part = external.get_part(self.shard_id, &location).await.unwrap();
            let is_part_valid = validate_state_part(
                &self.state_root,
                PartId::new(part_id, num_parts),
                &part
            );
            if is_part_valid {
                // TODO: emit metric for valid parts
                println!("part {} is valid", part_id);
            } else {
                // TODO: emit metric for non-valid parts
                println!("part {} is invalid", part_id);
            }
        };
        Ok(())
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

