use crate::metrics;
use near_chain_configs::ExternalStorageLocation;
use near_external_storage::{ExternalConnection, S3AccessConfig};
use near_primitives::types::{EpochId, ShardId};
use std::path::PathBuf;
use std::time::Instant;

#[derive(Debug, Clone)]
pub enum StateFileType {
    StatePart { part_id: u64, num_parts: u64 },
    StateHeader,
}

impl ToString for StateFileType {
    fn to_string(&self) -> String {
        match self {
            StateFileType::StatePart { .. } => StateFileType::part_str(),
            StateFileType::StateHeader => StateFileType::header_str(),
        }
    }
}

impl StateFileType {
    pub fn part_str() -> String {
        String::from("part")
    }

    pub fn header_str() -> String {
        String::from("header")
    }

    pub fn filename(&self) -> String {
        match self {
            StateFileType::StatePart { part_id, num_parts } => {
                format!("state_part_{:06}_of_{:06}", part_id, num_parts)
            }
            StateFileType::StateHeader => "header".to_string(),
        }
    }
}

/// Wrapper for a connection to the external storage, used by state sync.
#[derive(Clone)]
pub struct StateSyncConnection {
    connection: ExternalConnection,
    storage_name: String,
}

impl StateSyncConnection {
    pub fn new(
        location: &ExternalStorageLocation,
        credentials_file: Option<PathBuf>,
        s3_access_config: S3AccessConfig,
    ) -> Self {
        let connection =
            ExternalConnection::new(location, credentials_file, Some(s3_access_config));
        let storage_name = location.name().to_string();
        Self { connection, storage_name }
    }

    pub async fn get_file(
        &self,
        shard_id: ShardId,
        location: &str,
        file_type: &StateFileType,
    ) -> Result<Vec<u8>, anyhow::Error> {
        let _timer = metrics::STATE_SYNC_EXTERNAL_PARTS_REQUEST_DELAY
            .with_label_values(&[&shard_id.to_string(), &file_type.to_string()])
            .start_timer();
        let result = self.connection.get(location).await;
        match &result {
            Ok(bytes) => {
                tracing::debug!(target: "sync", %shard_id, location, num_bytes = bytes.len(), storage = self.storage_name, "request finished");
                metrics::STATE_SYNC_EXTERNAL_PARTS_SIZE_DOWNLOADED
                    .with_label_values(&[&shard_id.to_string(), &file_type.to_string()])
                    .inc_by(bytes.len() as u64);
            }
            Err(error) => {
                tracing::debug!(target: "sync", %shard_id, location, ?error, storage = self.storage_name, "request failed");
            }
        }
        result
    }

    /// Uploads the given state part or header to external storage.
    /// Wrapper for adding is_ok to the metric labels.
    pub async fn put_file(
        &self,
        file_type: StateFileType,
        data: &[u8],
        shard_id: ShardId,
        location: &str,
    ) -> Result<(), anyhow::Error> {
        let instant = Instant::now();
        let res = self.connection.put(location, data).await;
        let is_ok = match &res {
            Ok(()) => {
                tracing::debug!(target: "state_sync_dump", %shard_id, part_length = data.len(), ?location, ?file_type, storage = self.storage_name, "wrote a state part");
                "ok"
            }
            Err(error) => {
                tracing::error!(target: "state_sync_dump", %shard_id, part_length = data.len(), ?location, ?file_type, storage = self.storage_name, ?error, "failed to write a state part");
                "error"
            }
        };
        let elapsed = instant.elapsed();
        metrics::STATE_SYNC_DUMP_PUT_OBJECT_ELAPSED
            .with_label_values(&[&shard_id.to_string(), is_ok, &file_type.to_string()])
            .observe(elapsed.as_secs_f64());
        res
    }

    /// When using GCS external connection, this function requires credentials.
    /// Thus, this function shouldn't be used for sync node that is expected to operate anonymously.
    /// Only dump nodes should use this function.
    pub async fn list_objects(
        &self,
        shard_id: ShardId,
        directory_path: &str,
    ) -> Result<Vec<String>, anyhow::Error> {
        let _timer = metrics::STATE_SYNC_DUMP_LIST_OBJECT_ELAPSED
            .with_label_values(&[&shard_id.to_string()])
            .start_timer();
        tracing::debug!(target: "state_sync_dump", %shard_id, directory_path, "list state parts");
        self.connection.list(directory_path).await
    }

    /// Check if the state sync header exists in the external storage.
    pub async fn is_state_sync_header_stored_for_epoch(
        &self,
        shard_id: ShardId,
        chain_id: &String,
        epoch_id: &EpochId,
        epoch_height: u64,
    ) -> Result<bool, anyhow::Error> {
        let file_type = StateFileType::StateHeader;
        let directory_path = external_storage_location_directory(
            chain_id,
            epoch_id,
            epoch_height,
            shard_id,
            &file_type,
        );
        let file_names = self.list_objects(shard_id, &directory_path).await?;
        let header_exits = file_names.contains(&file_type.filename());
        tracing::debug!(
            target: "state_sync_dump",
            ?directory_path,
            "{}",
            match header_exits {
                true => "Header has already been dumped.",
                false => "header has not been dumped",
            }
        );
        Ok(header_exits)
    }
}

/// Construct the state file location on the external storage.
pub fn external_storage_location(
    chain_id: &str,
    epoch_id: &EpochId,
    epoch_height: u64,
    shard_id: ShardId,
    file_type: &StateFileType,
) -> String {
    format!(
        "{}/{}",
        location_prefix(chain_id, epoch_height, epoch_id, shard_id, file_type),
        file_type.filename()
    )
}

pub fn external_storage_location_directory(
    chain_id: &str,
    epoch_id: &EpochId,
    epoch_height: u64,
    shard_id: ShardId,
    obj_type: &StateFileType,
) -> String {
    location_prefix(chain_id, epoch_height, epoch_id, shard_id, obj_type)
}

pub fn location_prefix(
    chain_id: &str,
    epoch_height: u64,
    epoch_id: &EpochId,
    shard_id: ShardId,
    obj_type: &StateFileType,
) -> String {
    match obj_type {
        StateFileType::StatePart { .. } => format!(
            "chain_id={}/epoch_height={}/epoch_id={}/shard_id={}",
            chain_id, epoch_height, epoch_id.0, shard_id
        ),
        StateFileType::StateHeader => format!(
            "chain_id={}/epoch_height={}/epoch_id={}/headers/shard_id={}",
            chain_id, epoch_height, epoch_id.0, shard_id
        ),
    }
}

pub fn part_filename(part_id: u64, num_parts: u64) -> String {
    format!("state_part_{:06}_of_{:06}", part_id, num_parts)
}

pub fn match_filename(s: &str) -> Option<regex::Captures> {
    let re = regex::Regex::new(r"^state_part_(\d{6})_of_(\d{6})$").unwrap();
    re.captures(s)
}

pub fn is_part_filename(s: &str) -> bool {
    match_filename(s).is_some()
}

pub fn get_num_parts_from_filename(s: &str) -> Option<u64> {
    if let Some(captures) = match_filename(s) {
        if let Some(num_parts) = captures.get(2) {
            if let Ok(num_parts) = num_parts.as_str().parse::<u64>() {
                return Some(num_parts);
            }
        }
    }
    None
}

pub fn get_part_id_from_filename(s: &str) -> Option<u64> {
    if let Some(captures) = match_filename(s) {
        if let Some(part_id) = captures.get(1) {
            if let Ok(part_id) = part_id.as_str().parse::<u64>() {
                return Some(part_id);
            }
        }
    }
    None
}

#[cfg(test)]
mod test {
    use crate::sync::external::{
        ExternalConnection, StateFileType, StateSyncConnection, get_num_parts_from_filename,
        get_part_id_from_filename, is_part_filename,
    };
    use near_chain_configs::ExternalStorageLocation;
    use near_o11y::testonly::init_test_logger;
    use near_primitives::types::ShardId;
    use rand::distributions::{Alphanumeric, DistString};

    fn random_string(rand_len: usize) -> String {
        Alphanumeric.sample_string(&mut rand::thread_rng(), rand_len)
    }

    #[test]
    fn test_match_filename() {
        let filename = StateFileType::StatePart { part_id: 5, num_parts: 15 }.filename();
        assert!(is_part_filename(&filename));
        assert!(!is_part_filename("123123"));

        assert_eq!(get_num_parts_from_filename(&filename), Some(15));
        assert_eq!(get_num_parts_from_filename("123123"), None);

        assert_eq!(get_part_id_from_filename(&filename), Some(5));
        assert_eq!(get_part_id_from_filename("123123"), None);
    }

    /// This test should be ignored by default, as it requires gcloud credentials to run.
    /// Specify the path to service account json  in `SERVICE_ACCOUNT` variable to run the test.
    #[test]
    #[cfg_attr(not(feature = "gcs_credentials"), ignore)]
    fn test_gcs_upload_list_download() {
        init_test_logger();
        let rt = tokio::runtime::Runtime::new().unwrap();

        // Generate random filename.
        let filename = random_string(8);
        tracing::debug!(?filename, "filename");

        // Define bucket.
        let location = ExternalStorageLocation::GCS { bucket: "state-parts".into() };
        let connection = ExternalConnection::new(&location, None, None);
        let connection = StateSyncConnection { connection, storage_name: "GCS".into() };

        // Generate random data.
        let data = random_string(1000);
        tracing::debug!(?data, "data as string");
        let data: Vec<u8> = data.into();
        tracing::debug!(?data, "data");

        // Directory resembles real use case.
        let dir = "test_folder/chain_id=test/epoch_height=1/epoch_id=test/shard_id=0".to_string();
        let full_filename = format!("{}/{}", dir, filename);
        let file_type = StateFileType::StatePart { part_id: 0, num_parts: 1 };

        // Before uploading we shouldn't see filename in the list of files.
        let files =
            rt.block_on(async { connection.list_objects(ShardId::new(0), &dir).await.unwrap() });
        tracing::debug!(?files, "files before upload");
        assert_eq!(files.into_iter().filter(|x| *x == filename).count(), 0);

        // Uploading the file.
        rt.block_on(async {
            connection
                .put_file(file_type.clone(), &data, ShardId::new(0), &full_filename)
                .await
                .unwrap()
        });

        // After uploading we should see filename in the list of files.
        let files =
            rt.block_on(async { connection.list_objects(ShardId::new(0), &dir).await.unwrap() });
        tracing::debug!(?files, "files after upload");
        assert_eq!(files.into_iter().filter(|x| *x == filename).count(), 1);

        // And the data should match generates data.
        let download_data = rt.block_on(async {
            connection.get_file(ShardId::new(0), &full_filename, &file_type).await.unwrap()
        });
        assert_eq!(download_data, data);

        // Also try to download some data at nonexistent location and expect to fail.
        let filename = random_string(8);
        let full_filename = format!("{}/{}", dir, filename);

        let download_data = rt.block_on(async {
            connection.get_file(ShardId::new(0), &full_filename, &file_type).await
        });
        assert!(download_data.is_err(), "{:?}", download_data);
    }
}
