use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use futures::TryStreamExt;
use near_chain_configs::ExternalStorageLocation;
use object_store::{ObjectStore, PutPayload};
use std::time::Duration;

/// Live connection/handle to an external storage backend.
#[derive(Clone)]
pub enum ExternalConnection {
    /// Authenticated S3 client (read-only or read/write).
    S3 { bucket: Arc<s3::Bucket> },
    /// Local filesystem root directory.
    Filesystem { root_dir: PathBuf },
    /// GCS client (upload/list via SDK, anonymous downloads via HTTP).
    GCS {
        // May be used for uploading and listing state parts.
        // Requires valid credentials to be specified through env variable.
        gcs_client: Arc<object_store::gcp::GoogleCloudStorage>,
        // May be used for anonymously downloading state parts.
        reqwest_client: Arc<reqwest::Client>,
        bucket: String,
    },
}

/// URL encoding rules for GCS object names.
const GCS_ENCODE_SET: &percent_encoding::AsciiSet =
    &percent_encoding::NON_ALPHANUMERIC.remove(b'-').remove(b'.').remove(b'_');

/// Behavior/configuration for S3 connections.
pub struct S3AccessConfig {
    pub is_readonly: bool,
    pub timeout: Duration,
}

impl ExternalConnection {
    /// Create a connection for the given storage location.
    /// For S3, `s3_access_config` is required; `credentials_file` is used only for RW.
    /// For GCS, `credentials_file` (if provided) overrides SERVICE_ACCOUNT.
    pub fn new(
        location: &ExternalStorageLocation,
        credentials_file: Option<PathBuf>,
        s3_access_config: Option<S3AccessConfig>,
    ) -> Self {
        match location {
            ExternalStorageLocation::S3 { bucket, region, .. } => {
                let S3AccessConfig { is_readonly, timeout } = s3_access_config
                    .expect("S3 access config not provided with S3 external storage location");
                let bucket = if is_readonly {
                    create_s3_bucket_readonly(&bucket, &region, timeout)
                } else {
                    create_s3_bucket_read_write(&bucket, &region, timeout, credentials_file)
                };
                if let Err(err) = bucket {
                    if is_readonly {
                        panic!("Failed to create an S3 bucket: {err}");
                    } else {
                        panic!(
                            "Failed to authenticate connection to S3. Please either provide AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in the environment, or create a credentials file and link it in config.json as 's3_credentials_file'. Error: {err}"
                        );
                    }
                }
                ExternalConnection::S3 { bucket: Arc::new(bucket.unwrap()) }
            }
            ExternalStorageLocation::Filesystem { root_dir } => {
                ExternalConnection::Filesystem { root_dir: root_dir.clone() }
            }
            ExternalStorageLocation::GCS { bucket, .. } => {
                if let Some(credentials_file) = credentials_file {
                    if let Ok(var) = std::env::var("SERVICE_ACCOUNT") {
                        tracing::warn!(target: "external", "Environment variable 'SERVICE_ACCOUNT' is set to {var}, but 'credentials_file' in config.json overrides it to '{credentials_file:?}'");
                        println!(
                            "Environment variable 'SERVICE_ACCOUNT' is set to {var}, but 'credentials_file' in config.json overrides it to '{credentials_file:?}'"
                        );
                    }
                    // SAFE: no threads *yet*.
                    unsafe { std::env::set_var("SERVICE_ACCOUNT", &credentials_file) };
                    tracing::info!(target: "external", "Set the environment variable 'SERVICE_ACCOUNT' to '{credentials_file:?}'");
                }
                ExternalConnection::GCS {
                    gcs_client: Arc::new(
                        object_store::gcp::GoogleCloudStorageBuilder::from_env()
                            .with_bucket_name(bucket)
                            .build()
                            .unwrap(),
                    ),
                    reqwest_client: Arc::new(reqwest::Client::default()),
                    bucket: bucket.clone(),
                }
            }
        }
    }

    /// Download an object at `path` as bytes.
    pub async fn get(&self, path: &str) -> Result<Vec<u8>, anyhow::Error> {
        match self {
            ExternalConnection::S3 { bucket } => {
                tracing::debug!(target: "external", path, "Reading from S3");
                let response = bucket.get_object(path).await?;
                if response.status_code() == 200 {
                    Ok(response.bytes().to_vec())
                } else {
                    Err(anyhow::anyhow!("Bad response status code: {}", response.status_code()))
                }
            }
            ExternalConnection::Filesystem { root_dir } => {
                let path = root_dir.join(path);
                tracing::debug!(target: "external", ?path, "Reading a file");
                let data = std::fs::read(&path)?;
                Ok(data)
            }
            ExternalConnection::GCS { reqwest_client, bucket, .. } => {
                // Download should be handled anonymously, therefore we are not using cloud-storage crate.
                // TODO(cloud_archival) Consider the case of cloud archival
                let url = format!(
                    "https://storage.googleapis.com/storage/v1/b/{}/o/{}?alt=media",
                    percent_encoding::percent_encode(bucket.as_bytes(), GCS_ENCODE_SET),
                    percent_encoding::percent_encode(path.as_bytes(), GCS_ENCODE_SET),
                );
                tracing::debug!(target: "external", url, "Reading from GCS");
                let response = reqwest_client.get(&url).send().await?.error_for_status();
                match response {
                    Err(e) => Err(e.into()),
                    Ok(r) => {
                        let bytes = r.bytes().await?.to_vec();
                        Ok(bytes)
                    }
                }
            }
        }
    }

    /// Upload/overwrite an object at `path` with `value`.
    pub async fn put(&self, path: &str, value: &[u8]) -> Result<(), anyhow::Error> {
        match self {
            ExternalConnection::S3 { bucket } => {
                tracing::debug!(target: "external", path, "Writing to S3");
                bucket.put_object(path, value).await?;
                Ok(())
            }
            ExternalConnection::Filesystem { root_dir } => {
                let path = root_dir.join(path);
                tracing::debug!(target: "external", ?path, "Writing to a file");
                if let Some(parent_dir) = path.parent() {
                    std::fs::create_dir_all(parent_dir)?;
                }
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&path)?;
                file.write_all(value)?;
                Ok(())
            }
            ExternalConnection::GCS { gcs_client, .. } => {
                let path = object_store::path::Path::parse(path)
                    .with_context(|| format!("{path} isn't a valid path for GCP"))?;
                tracing::debug!(target: "external", ?path, "Writing to GCS");
                gcs_client.put(&path, PutPayload::from_bytes(value.to_vec().into())).await?;
                Ok(())
            }
        }
    }

    /// List object names under the given directory.
    ///
    /// Non-recursive for Filesystem and S3.
    /// Recursive for GCS (lists all objects within the given directory).
    pub async fn list(&self, directory_path: &str) -> Result<Vec<String>, anyhow::Error> {
        match self {
            ExternalConnection::S3 { bucket } => {
                let prefix = format!("{}/", directory_path);
                let list_results = bucket.list(prefix.clone(), Some("/".to_string())).await?;
                tracing::debug!(target: "external", directory_path, "List directory in s3");
                let mut file_names = vec![];
                for res in list_results {
                    for obj in res.contents {
                        file_names.push(extract_file_name_from_full_path(obj.key))
                    }
                }
                Ok(file_names)
            }
            ExternalConnection::Filesystem { root_dir } => {
                let path = root_dir.join(directory_path);
                tracing::debug!(target: "external", ?path, "List files in local directory");
                std::fs::create_dir_all(&path)?;
                let mut file_names = vec![];
                let files = std::fs::read_dir(&path)?;
                for file in files {
                    let file_name = extract_file_name_from_path_buf(file?.path());
                    file_names.push(file_name);
                }
                Ok(file_names)
            }
            ExternalConnection::GCS { gcs_client, .. } => {
                let prefix = format!("{}/", directory_path);
                tracing::debug!(target: "external", directory_path, "List directory in GCS");
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
}

/// Extract file name from a full (string) path.
fn extract_file_name_from_full_path(full_path: String) -> String {
    return extract_file_name_from_path_buf(PathBuf::from(full_path));
}

/// Extract file name from a PathBuf.
fn extract_file_name_from_path_buf(path_buf: PathBuf) -> String {
    return path_buf.file_name().unwrap().to_str().unwrap().to_string();
}

/// Create an anonymous, read-only S3 bucket handle.
pub fn create_s3_bucket_readonly(
    bucket: &str,
    region: &str,
    timeout: Duration,
) -> Result<s3::Bucket, anyhow::Error> {
    let creds = s3::creds::Credentials::anonymous()?;
    create_s3_bucket(bucket, region, timeout, creds)
}

/// Credentials for S3 read/write access (from JSON file).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct S3CredentialsConfig {
    access_key: String,
    secret_key: String,
}

/// Create a read/write S3 bucket handle, optionally using a JSON credentials file.
pub fn create_s3_bucket_read_write(
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
    create_s3_bucket(bucket, region, timeout, creds)
}

/// Build an S3 bucket client and set request timeout.
fn create_s3_bucket(
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
