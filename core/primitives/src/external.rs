use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use object_store::{ObjectStore, PutPayload};
use std::time::Duration;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum ExternalStorageLocation {
    S3 {
        /// Location of state dumps on S3.
        bucket: String,
        /// Data may only be available in certain locations.
        region: String,
    },
    Filesystem {
        root_dir: PathBuf,
    },
    GCS {
        bucket: String,
    },
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
    pub fn storage_name(&self) -> &str {
        match self {
            Self::S3 { .. } => "S3",
            Self::Filesystem { .. } => "Filesystem",
            Self::GCS { .. } => "GCS",
        }
    }

    pub fn new(
        location: &ExternalStorageLocation,
        external_timeout: Duration,
        has_write_access: bool,
        credentials_file: Option<PathBuf>,
    ) -> Self {
        match location {
            ExternalStorageLocation::S3 { bucket, region, .. } => {
                let bucket = if has_write_access {
                    create_bucket_read_write(
                        &bucket,
                        &region,
                        external_timeout,
                        credentials_file,
                    )
                } else {
                    create_bucket_readonly(
                        &bucket,
                        &region,
                        external_timeout.max(Duration::ZERO),//.unsigned_abs(),
                    )
                };
                if let Err(err) = bucket {
                    if has_write_access {
                        panic!("Failed to authenticate connection to S3. Please either provide AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in the environment, or create a credentials file and link it in config.json as 's3_credentials_file'. Error: {err}");
                    } else {
                        panic!("Failed to create an S3 bucket: {err}");
                    }
                }
                ExternalConnection::S3 { bucket: Arc::new(bucket.unwrap()) }
            }
            ExternalStorageLocation::Filesystem { root_dir } => {
                ExternalConnection::Filesystem { root_dir: root_dir.clone() }
            }
            ExternalStorageLocation::GCS { bucket, .. } => {
                if has_write_access {
                    if let Some(credentials_file) = credentials_file {
                        if let Ok(var) = std::env::var("SERVICE_ACCOUNT") {
                            tracing::warn!(target: "external", "Environment variable 'SERVICE_ACCOUNT' is set to {var}, but 'credentials_file' in config.json overrides it to '{credentials_file:?}'");
                            println!("Environment variable 'SERVICE_ACCOUNT' is set to {var}, but 'credentials_file' in config.json overrides it to '{credentials_file:?}'");
                        }
                        // SAFE: no threads *yet*.
                        unsafe { std::env::set_var("SERVICE_ACCOUNT", &credentials_file) };
                        tracing::info!(target: "external", "Set the environment variable 'SERVICE_ACCOUNT' to '{credentials_file:?}'");
                    }
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

    pub async fn put(&self, path: &str, value: &[u8]) -> Result<(), anyhow::Error> {
        match self {
            ExternalConnection::S3 { bucket } => {
                bucket.put_object(path, value).await?;
                Ok(())
            }
            ExternalConnection::Filesystem { root_dir } => {
                let path = root_dir.join(path);
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
                gcs_client.put(&path, PutPayload::from_bytes(value.to_vec().into())).await?;
                Ok(())
            }
        }
    }
}

pub fn create_bucket_readonly(
    bucket: &str,
    region: &str,
    timeout: Duration,
) -> Result<s3::Bucket, anyhow::Error> {
    let creds = s3::creds::Credentials::anonymous()?;
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
