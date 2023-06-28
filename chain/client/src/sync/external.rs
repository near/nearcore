use crate::metrics;
use near_primitives::types::{EpochId, ShardId};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

/// Connection to the external storage.
#[derive(Clone)]
pub enum ExternalConnection {
    S3 { bucket: Arc<s3::Bucket> },
    Filesystem { root_dir: PathBuf },
}

impl ExternalConnection {
    pub async fn get_part(
        &self,
        shard_id: ShardId,
        location: &str,
    ) -> Result<Vec<u8>, anyhow::Error> {
        let _timer = metrics::STATE_SYNC_EXTERNAL_PARTS_REQUEST_DELAY
            .with_label_values(&[&shard_id.to_string()])
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
        }
    }

    /// Uploads the given state part to external storage.
    // Wrapper for adding is_ok to the metric labels.
    pub async fn put_state_part(
        &self,
        state_part: &[u8],
        shard_id: ShardId,
        location: &str,
    ) -> Result<(), anyhow::Error> {
        let instant = Instant::now();
        let res = self.put_state_part_impl(state_part, shard_id, location).await;
        let is_ok = if res.is_ok() { "ok" } else { "error" };
        let elapsed = instant.elapsed();
        metrics::STATE_SYNC_DUMP_PUT_OBJECT_ELAPSED
            .with_label_values(&[&shard_id.to_string(), is_ok])
            .observe(elapsed.as_secs_f64());
        res
    }

    // Actual implementation.
    async fn put_state_part_impl(
        &self,
        state_part: &[u8],
        shard_id: ShardId,
        location: &str,
    ) -> Result<(), anyhow::Error> {
        match self {
            ExternalConnection::S3 { bucket } => {
                bucket.put_object(&location, state_part).await?;
                tracing::debug!(target: "state_sync_dump", shard_id, part_length = state_part.len(), ?location, "Wrote a state part to S3");
                Ok(())
            }
            ExternalConnection::Filesystem { root_dir } => {
                let path = root_dir.join(location);
                if let Some(parent_dir) = path.parent() {
                    std::fs::create_dir_all(parent_dir)?;
                }
                let mut file = std::fs::OpenOptions::new().write(true).create(true).open(&path)?;
                file.write_all(state_part)?;
                tracing::debug!(target: "state_sync_dump", shard_id, part_length = state_part.len(), ?location, "Wrote a state part to a file");
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

    pub async fn list_state_parts(
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
                tracing::debug!(target: "state_sync_dump", shard_id, ?directory_path, "List state parts in s3");
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
                tracing::debug!(target: "state_sync_dump", shard_id, ?path, "List state parts in local directory");
                std::fs::create_dir_all(&path)?;
                let mut file_names = vec![];
                let files = std::fs::read_dir(&path)?;
                for file in files {
                    let file_name = Self::extract_file_name_from_path_buf(file?.path());
                    file_names.push(file_name);
                }
                Ok(file_names)
            }
        }
    }
}

/// Construct a location on the external storage.
pub fn external_storage_location(
    chain_id: &str,
    epoch_id: &EpochId,
    epoch_height: u64,
    shard_id: u64,
    part_id: u64,
    num_parts: u64,
) -> String {
    format!(
        "{}/{}",
        location_prefix(chain_id, epoch_height, epoch_id, shard_id),
        part_filename(part_id, num_parts)
    )
}

pub fn external_storage_location_directory(
    chain_id: &str,
    epoch_id: &EpochId,
    epoch_height: u64,
    shard_id: u64,
) -> String {
    location_prefix(chain_id, epoch_height, epoch_id, shard_id)
}

pub fn location_prefix(
    chain_id: &str,
    epoch_height: u64,
    epoch_id: &EpochId,
    shard_id: u64,
) -> String {
    format!(
        "chain_id={}/epoch_height={}/epoch_id={}/shard_id={}",
        chain_id, epoch_height, epoch_id.0, shard_id
    )
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

pub fn create_bucket_readwrite(
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
        get_num_parts_from_filename, get_part_id_from_filename, is_part_filename, part_filename,
    };

    #[test]
    fn test_match_filename() {
        let filename = part_filename(5, 15);
        assert!(is_part_filename(&filename));
        assert!(!is_part_filename("123123"));

        assert_eq!(get_num_parts_from_filename(&filename), Some(15));
        assert_eq!(get_num_parts_from_filename("123123"), None);

        assert_eq!(get_part_id_from_filename(&filename), Some(5));
        assert_eq!(get_part_id_from_filename("123123"), None);
    }
}
