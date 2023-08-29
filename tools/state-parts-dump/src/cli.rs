use near_primitives::types::ShardId;
use std::str::FromStr;
use near_client::sync::external::{
    external_storage_location, external_storage_location_directory, get_num_parts_from_filename
};
use near_network::types::PeerInfo;

#[derive(clap::Parser)]
pub struct StatePartsDumpCheck {
    chain_id: String,
    epoch_id: EpochId,
    epoch_height: u64,
    shard_id: ShardID,
    chain_id: String,
    root_dir: Option<PathBuf>,
    s3_bucket: Option<String>,
    s3_region: Option<String>,
    gcs_bucket: Option<String>,
    credentials_file: Option<PathBuf>,
    // TODO: what if peer changed? Or can we have a stable RPC node to use?
    peer: String,
    recv_timeout_seconds: Option<u32>,
}

// instead of listing all subdirectories inside gcp/s3 bucket, the script can instead memorize the last epoch that finished
// and see if last_epoch_height+1 exists. This means we need to give the program an epoch_height to start with
impl StatePartsDumpCheck {
    pub fn run(&self) -> anyhow::Result<()> {
        // first get head_height, first block hash of the epoch and genesis_hash from rpc endpoints
        // then use these as input, to connect with a node on near network and ask that node for state header
        // then download all the parts, then validate them
        // every time a part is validated, emit metric

        let rpc_client = crate::RpcClient(self.chain_id);
        let head_height = rpc_client.get_head_height();
        let genesis_hash = rpc_client.get_genesis_hash();
        let sync_hash = rpc_client.get_epoch_start_block_hash(self.epoch_id);

        let peer = match PeerInfo::from_str(&self.peer) {
            Ok(p) => p,
            Err(e) => anyhow::bail!("Could not parse --peer {}: {:?}", &self.peer, e),
        };
        if peer.addr.is_none() {
            anyhow::bail!("--peer should be in the form [public key]@[socket addr]");
        }

        let state_root = state_header_from_node(
            sync_hash,
            shard_id,
            chain_id: self.chain_id,
            genesis_hash,
            head_height,
            None,
            peer.id.clone(),
            peer.addr.unwrap(),
            self.recv_timeout_seconds.unwrap_or(5),
        ).await.unwrap();

        // TODO: emit metric for epoch_height

        let external = crate::create_external_connection(
            self.root_dir,
            self.s3_bucket,
            self.s3_region,
            self.gcs_bucket,
            None,
            Mode::Readonly,
        );

        let directory_path = external_storage_location_directory(chain_id, &epoch_id, epoch_height, shard_id);
        let part_file_names = external.list_state_parts(shard_id, &directory_path).await.unwrap();
        assert!(!part_file_names.is_empty(), "Number of state parts is 0");
        let num_parts = part_file_names.len() as u64;
        let total_required_parts = get_num_parts_from_filename(&part_file_names[0]);
        if num_parts < total_required_parts {
            println!("total state parts required: {} < number of parts already dumped: {}, waiting for all parts to be dumped", total_required_parts, num_parts);
            return Ok(())
        } else if num_parts > total_required_parts{
            println!("total state parts required: {} > number of parts already dumped: {}, something is seriously wrong", total_required_parts, num_parts);
            // emit metric indicating that dumped parts > total required parts
            return Ok(())
        }
        
        // only download and validate when all parts are dumped
        for part_id in [0..total_required_parts] {
            if part_id == 0 {
                // TODO: reset the valid parts and invalid parts count to zero
            }
            let location = external_storage_location(
                chain_id,
                &self.epoch_id,
                self.epoch_height,
                shard_id,
                part_id,
                num_parts,
            );
            let part = external.get_part(shard_id, &location).await.unwrap();
            let is_part_valid = crate::validate_state_part(
                &state_root,
                PartId::new(part_id, num_parts),
                &part
            );
            if is_part_valid {
                // TODO: emit metric for valid parts
            } else {
                // TODO: emit metric for non-valid parts
            }
        }
    }
}

