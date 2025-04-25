use crate::metrics;
use anyhow::Context;
use futures::TryStreamExt;
use near_primitives::types::{EpochId, ShardId};
use object_store::{ObjectStore as _, PutPayload};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
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

/// Connection to the external storage.
#[derive(Clone)]
pub enum ExternalConnection {
    S3 {
        bucket: Arc<s3::Bucket>,
    },
    Filesystem {
        root_dir: PathBuf,
    },
    GCS {
        // Used for uploading and listing state parts.
        // Requires valid credentials to be specified through env variable.
        gcs_client: Arc<object_store::gcp::GoogleCloudStorage>,
        // Used for anonymously downloading state parts.
        reqwest_client: Arc<reqwest::Client>,
        bucket: String,
    },
}

const GCS_ENCODE_SET: &percent_encoding::AsciiSet =
    &percent_encoding::NON_ALPHANUMERIC.remove(b'-').remove(b'.').remove(b'_');

impl ExternalConnection {
    pub async fn get_file(
        &self,
        shard_id: ShardId,
        location: &str,
        file_type: &StateFileType,
    ) -> Result<Vec<u8>, anyhow::Error> {
        let _timer = metrics::STATE_SYNC_EXTERNAL_PARTS_REQUEST_DELAY
            .with_label_values(&[&shard_id.to_string(), &file_type.to_string()])
            .start_timer();
        match self {
            ExternalConnection::S3 { bucket } => {
                let response = bucket.get_object(location).await?;
                tracing::debug!(target: "sync", %shard_id, location, response_code = response.status_code(), num_bytes = response.bytes().len(), "S3 request finished");
                if response.status_code() == 200 {
                    Ok(response.bytes().to_vec())
                } else {
                    Err(anyhow::anyhow!("Bad response status code: {}", response.status_code()))
                }
            }
            ExternalConnection::Filesystem { root_dir } => {
                let path = root_dir.join(location);
                tracing::debug!(target: "sync", %shard_id, ?path, "Reading a file");
                let data = std::fs::read(&path)?;
                Ok(data)
            }
            ExternalConnection::GCS { reqwest_client, bucket, .. } => {
                // Download should be handled anonymously, therefore we are not using cloud-storage crate.
                let url = format!(
                    "https://storage.googleapis.com/storage/v1/b/{}/o/{}?alt=media",
                    percent_encoding::percent_encode(bucket.as_bytes(), GCS_ENCODE_SET),
                    percent_encoding::percent_encode(location.as_bytes(), GCS_ENCODE_SET),
                );
                let response = reqwest_client.get(&url).send().await?.error_for_status();

                match response {
                    Err(e) => {
                        tracing::debug!(target: "sync", %shard_id, location, error = ?e, "GCS state_part request failed");
                        Err(e.into())
                    }
                    Ok(r) => {
                        let bytes = r.bytes().await?.to_vec();
                        tracing::debug!(target: "sync", %shard_id, location, num_bytes = bytes.len(), "GCS state_part request finished");
                        metrics::STATE_SYNC_EXTERNAL_PARTS_SIZE_DOWNLOADED
                            .with_label_values(&[&shard_id.to_string(), &file_type.to_string()])
                            .inc_by(bytes.len() as u64);
                        Ok(bytes)
                    }
                }
            }
        }
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
        let res = self.put_file_impl(&file_type, data, shard_id, location).await;
        let is_ok = if res.is_ok() { "ok" } else { "error" };
        let elapsed = instant.elapsed();
        metrics::STATE_SYNC_DUMP_PUT_OBJECT_ELAPSED
            .with_label_values(&[&shard_id.to_string(), is_ok, &file_type.to_string()])
            .observe(elapsed.as_secs_f64());
        res
    }

    /// Actual implementation.
    async fn put_file_impl(
        &self,
        file_type: &StateFileType,
        data: &[u8],
        shard_id: ShardId,
        location: &str,
    ) -> Result<(), anyhow::Error> {
        match self {
            ExternalConnection::S3 { bucket } => {
                bucket.put_object(&location, data).await?;
                tracing::debug!(target: "state_sync_dump", ?shard_id, part_length = data.len(), ?location, ?file_type, "Wrote a state part to S3");
                Ok(())
            }
            ExternalConnection::Filesystem { root_dir } => {
                let path = root_dir.join(location);
                if let Some(parent_dir) = path.parent() {
                    std::fs::create_dir_all(parent_dir)?;
                }
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&path)?;
                file.write_all(data)?;
                tracing::debug!(target: "state_sync_dump", ?shard_id, part_length = data.len(), ?location, ?file_type, "Wrote a state part to a file");
                Ok(())
            }
            ExternalConnection::GCS { gcs_client, .. } => {
                let path = object_store::path::Path::parse(location)
                    .with_context(|| format!("{location} isn't a valid path for GCP"))?;
                gcs_client.put(&path, PutPayload::from_bytes(data.to_vec().into())).await?;
                tracing::debug!(target: "state_sync_dump", ?shard_id, part_length = data.len(), ?location, ?file_type, "Wrote a state part to GCS");
                Ok(())
            }
        }
    }

    fn extract_file_name_from_full_path(full_path: String) -> String {
        return Self::extract_file_name_from_path_buf(PathBuf::from(full_path));
    }

    fn extract_file_name_from_path_buf(path_buf: PathBuf) -> String {
        return path_buf.file_name().unwrap().to_str().unwrap().to_string();
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
        match self {
            ExternalConnection::S3 { bucket } => {
                let prefix = format!("{}/", directory_path);
                let list_results = bucket.list(prefix.clone(), Some("/".to_string())).await?;
                tracing::debug!(target: "state_sync_dump", ?shard_id, ?directory_path, "List state parts in s3");
                let mut file_names = vec![];
                for res in list_results {
                    for obj in res.contents {
                        file_names.push(Self::extract_file_name_from_full_path(obj.key))
                    }
                }
                Ok(file_names)
            }
            ExternalConnection::Filesystem { root_dir } => {
                let path = root_dir.join(directory_path);
                tracing::debug!(target: "state_sync_dump", ?shard_id, ?path, "List state parts in local directory");
                std::fs::create_dir_all(&path)?;
                let mut file_names = vec![];
                let files = std::fs::read_dir(&path)?;
                for file in files {
                    let file_name = Self::extract_file_name_from_path_buf(file?.path());
                    file_names.push(file_name);
                }
                Ok(file_names)
            }
            ExternalConnection::GCS { gcs_client, .. } => {
                let prefix = format!("{}/", directory_path);
                tracing::debug!(target: "state_sync_dump", ?shard_id, ?directory_path, "List state parts in GCS");
                Ok(gcs_client
                    .list(Some(
                        &object_store::path::Path::parse(&prefix)
                            .with_context(|| format!("can't parse {prefix} as path"))?,
                    ))
                    .try_collect::<Vec<_>>()
                    .await?
                    .into_iter()
                    .map(|object| object.location.filename().unwrap().into())
                    .collect())
            }
        }
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
                false => "Header has not been dumped.",
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

pub fn create_bucket_readonly(
    bucket: &str,
    region: &str,
    timeout: Duration,
) -> Result<s3::Bucket, anyhow::Error> {
    let creds = s3::creds::Credentials::anonymous()?;
    create_bucket(bucket, region, timeout, creds)
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct S3CredentialsConfig {
    access_key: String,
    secret_key: String,
}

pub fn create_bucket_read_write(
    bucket: &str,
    region: &str,
    timeout: Duration,
    credentials_file: Option<PathBuf>,
) -> Result<s3::Bucket, anyhow::Error> {
    let creds = match credentials_file {
        Some(credentials_file) => {
            let mut file = std::fs::File::open(credentials_file)?;
            let mut json_config_str = String::new();
            file.read_to_string(&mut json_config_str)?;
            let credentials_config: S3CredentialsConfig = serde_json::from_str(&json_config_str)?;
            s3::creds::Credentials::new(
                Some(&credentials_config.access_key),
                Some(&credentials_config.secret_key),
                None,
                None,
                None,
            )
        }
        None => s3::creds::Credentials::default(),
    }?;
    create_bucket(bucket, region, timeout, creds)
}

fn create_bucket(
    bucket: &str,
    region: &str,
    timeout: Duration,
    creds: s3::creds::Credentials,
) -> Result<s3::Bucket, anyhow::Error> {
    let mut bucket = s3::Bucket::new(bucket, region.parse::<s3::Region>()?, creds)?;
    // Ensure requests finish in finite amount of time.
    bucket.set_request_timeout(Some(timeout));
    Ok(bucket)
}

#[cfg(test)]
mod test {
    use crate::sync::external::{
        ExternalConnection, StateFileType, get_num_parts_from_filename, get_part_id_from_filename,
        is_part_filename,
    };
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
        tracing::debug!("Filename: {:?}", filename);

        // Define bucket.
        let bucket = String::from("state-parts");
        let connection = ExternalConnection::GCS {
            gcs_client: std::sync::Arc::new(
                object_store::gcp::GoogleCloudStorageBuilder::new()
                    .with_bucket_name(&bucket)
                    .build()
                    .unwrap(),
            ),
            reqwest_client: std::sync::Arc::new(reqwest::Client::default()),
            bucket,
        };

        // Generate random data.
        let data = random_string(1000);
        tracing::debug!("Data as string: {:?}", data);
        let data: Vec<u8> = data.into();
        tracing::debug!("Data: {:?}", data);

        // Directory resembles real use case.
        let dir = "test_folder/chain_id=test/epoch_height=1/epoch_id=test/shard_id=0".to_string();
        let full_filename = format!("{}/{}", dir, filename);
        let file_type = StateFileType::StatePart { part_id: 0, num_parts: 1 };

        // Before uploading we shouldn't see filename in the list of files.
        let files =
            rt.block_on(async { connection.list_objects(ShardId::new(0), &dir).await.unwrap() });
        tracing::debug!("Files before upload: {:?}", files);
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
        tracing::debug!("Files after upload: {:?}", files);
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
