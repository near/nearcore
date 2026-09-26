use anyhow::Context;
use futures::TryStreamExt;
use near_chain_configs::ExternalStorageLocation;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path as ObjectStorePath;
use object_store::{ClientOptions, ObjectStore, ObjectStoreExt, PutPayload};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

/// Live connection/handle to an external storage backend.
// TODO(cloud_archival) Structure it better, e.g. make it a trait. May simplify when we
// use object_store for each backend.
#[derive(Clone)]
pub enum ExternalConnection {
    /// Authenticated S3 client (read-only or read/write).
    S3 { s3_client: Arc<AmazonS3> },
    /// Local filesystem root directory.
    Filesystem { root_dir: PathBuf },
    /// GCS client (SDK for signed calls, plain HTTP for anonymous downloads).
    GCS {
        // Uploads, listing, and signed downloads. Requires valid credentials
        // to be specified through env variable.
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
    /// Human-readable backend name for logging.
    pub fn name(&self) -> &'static str {
        match self {
            Self::S3 { .. } => "S3",
            Self::Filesystem { .. } => "Filesystem",
            Self::GCS { .. } => "GCS",
        }
    }

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
                let s3_client = if is_readonly {
                    create_s3_client_readonly(bucket, region, timeout)
                } else {
                    create_s3_client_read_write(bucket, region, timeout, credentials_file)
                };
                if let Err(err) = s3_client {
                    if is_readonly {
                        panic!("failed to create an S3 client: {err}");
                    } else {
                        panic!(
                            "failed to authenticate connection to S3. Please either provide AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in the environment, or create a credentials file and link it in config.json as 's3_credentials_file'. Error: {err}"
                        );
                    }
                }
                ExternalConnection::S3 { s3_client: Arc::new(s3_client.unwrap()) }
            }
            ExternalStorageLocation::Filesystem { root_dir } => {
                ExternalConnection::Filesystem { root_dir: root_dir.clone() }
            }
            ExternalStorageLocation::GCS { bucket, .. } => {
                if let Some(credentials_file) = credentials_file {
                    if let Ok(var) = std::env::var("SERVICE_ACCOUNT") {
                        tracing::warn!(target: "external", %var, ?credentials_file, "environment variable `SERVICE_ACCOUNT` is set, but `credentials_file` in config.json overrides it");
                        println!(
                            "Environment variable 'SERVICE_ACCOUNT' is set to {var}, but 'credentials_file' in config.json overrides it to '{credentials_file:?}'"
                        );
                    }
                    // SAFE: no threads *yet*.
                    unsafe { std::env::set_var("SERVICE_ACCOUNT", &credentials_file) };
                    tracing::info!(target: "external", ?credentials_file, "set the environment variable `SERVICE_ACCOUNT`");
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

    /// Download an object at `path` as bytes. GCS reads go out with no credentials,
    /// so a GCS bucket must grant anonymous read for this to succeed.
    pub async fn get(&self, path: &str) -> Result<Vec<u8>, anyhow::Error> {
        match self {
            ExternalConnection::S3 { s3_client } => {
                tracing::debug!(target: "external", path, "reading from S3");
                let obj_path = ObjectStorePath::parse(path)
                    .with_context(|| format!("{path} isn't a valid S3 path"))?;
                let result = s3_client.get(&obj_path).await?;
                Ok(result.bytes().await?.to_vec())
            }
            ExternalConnection::Filesystem { root_dir } => {
                let path = root_dir.join(path);
                tracing::debug!(target: "external", ?path, "reading a file");
                let data = std::fs::read(&path)?;
                Ok(data)
            }
            ExternalConnection::GCS { reqwest_client, bucket, .. } => {
                // A bare HTTP client, so the request goes out with no credentials.
                let url = format!(
                    "https://storage.googleapis.com/storage/v1/b/{}/o/{}?alt=media",
                    percent_encoding::percent_encode(bucket.as_bytes(), GCS_ENCODE_SET),
                    percent_encoding::percent_encode(path.as_bytes(), GCS_ENCODE_SET),
                );
                tracing::debug!(target: "external", url, "reading from GCS");
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

    /// Download an object at `path` as bytes, signing GCS reads with the connection's
    /// credentials. A bucket with `public_access_prevention` enforced requires them.
    ///
    /// S3 and filesystem reads already carry whatever access the connection was built with,
    /// so they take the same path as [`Self::get`].
    pub async fn get_authenticated(&self, path: &str) -> Result<Vec<u8>, anyhow::Error> {
        let ExternalConnection::GCS { gcs_client, .. } = self else {
            return self.get(path).await;
        };
        let parsed_path = ObjectStorePath::parse(path)
            .with_context(|| format!("{path} isn't a valid path for GCP"))?;
        tracing::debug!(target: "external", ?parsed_path, "reading from GCS with credentials");
        let response = gcs_client.get(&parsed_path).await?;
        let bytes = response.bytes().await?.to_vec();
        Ok(bytes)
    }

    /// Upload/overwrite an object at `path` with `value`.
    pub async fn put(&self, path: &str, value: &[u8]) -> Result<(), anyhow::Error> {
        match self {
            ExternalConnection::S3 { s3_client } => {
                tracing::debug!(target: "external", path, "writing to S3");
                let obj_path = ObjectStorePath::parse(path)
                    .with_context(|| format!("{path} isn't a valid S3 path"))?;
                s3_client.put(&obj_path, PutPayload::from_bytes(value.to_vec().into())).await?;
                Ok(())
            }
            ExternalConnection::Filesystem { root_dir } => {
                let path = root_dir.join(path);
                tracing::debug!(target: "external", ?path, "writing to a file");
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
                let path = ObjectStorePath::parse(path)
                    .with_context(|| format!("{path} isn't a valid path for GCP"))?;
                tracing::debug!(target: "external", ?path, "writing to GCS");
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
            ExternalConnection::S3 { s3_client } => {
                let prefix = format!("{}/", directory_path);
                tracing::debug!(target: "external", directory_path, "list directory in S3");
                let obj_prefix = ObjectStorePath::parse(&prefix)
                    .with_context(|| format!("can't parse {prefix} as path"))?;
                let result = s3_client.list_with_delimiter(Some(&obj_prefix)).await?;
                Ok(result
                    .objects
                    .into_iter()
                    .filter_map(|obj| obj.location.filename().map(|f| f.to_string()))
                    .collect())
            }
            ExternalConnection::Filesystem { root_dir } => {
                let path = root_dir.join(directory_path);
                tracing::debug!(target: "external", ?path, "list files in local directory");
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
                tracing::debug!(target: "external", directory_path, "list directory in GCS");
                Ok(gcs_client
                    .list(Some(
                        &ObjectStorePath::parse(&prefix)
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

/// Extract file name from a PathBuf.
fn extract_file_name_from_path_buf(path_buf: PathBuf) -> String {
    return path_buf.file_name().unwrap().to_str().unwrap().to_string();
}

/// Signing region for a custom endpoint, where the `region` config value holds the endpoint.
const CUSTOM_ENDPOINT_SIGNING_REGION: &str = "us-east-1";

/// Region to sign S3 requests with, and the endpoint to send them to if it is not AWS.
#[derive(Debug, PartialEq)]
struct S3RegionAndEndpoint {
    region: String,
    endpoint: Option<String>,
}

/// Parses the `region` config value the way the `rust-s3` crate did: names of non-AWS
/// providers and values with a scheme or a dot are endpoints, anything else is an AWS region.
fn parse_s3_region_and_endpoint(region: &str) -> S3RegionAndEndpoint {
    let provider_region_and_endpoint = match region {
        "nyc3" | "ams3" | "sgp1" | "fra1" => {
            Some((region, format!("https://{region}.digitaloceanspaces.com")))
        }
        "yandex" | "ru-central1" => {
            Some(("ru-central1", "https://storage.yandexcloud.net".to_string()))
        }
        _ => region.strip_prefix("wa-").map(|wasabi_region| {
            (wasabi_region, format!("https://s3.{wasabi_region}.wasabisys.com"))
        }),
    };
    if let Some((provider_region, endpoint)) = provider_region_and_endpoint {
        return S3RegionAndEndpoint {
            region: provider_region.to_string(),
            endpoint: Some(endpoint),
        };
    }
    let custom_endpoint = if region.contains("://") {
        region.to_string()
    } else if region.contains('.') {
        format!("https://{region}")
    } else {
        return S3RegionAndEndpoint { region: region.to_string(), endpoint: None };
    };
    S3RegionAndEndpoint {
        region: CUSTOM_ENDPOINT_SIGNING_REGION.to_string(),
        endpoint: Some(custom_endpoint),
    }
}

/// Sets bucket, region, endpoint and request timeout on an S3 client builder.
fn apply_s3_location_and_timeout(
    builder: AmazonS3Builder,
    bucket: &str,
    region: &str,
    timeout: Duration,
) -> AmazonS3Builder {
    let S3RegionAndEndpoint { region, endpoint } = parse_s3_region_and_endpoint(region);
    let allow_http = endpoint.as_deref().is_some_and(|endpoint| endpoint.starts_with("http://"));
    let builder = builder.with_bucket_name(bucket).with_region(region).with_client_options(
        ClientOptions::new().with_timeout(timeout).with_allow_http(allow_http),
    );
    match endpoint {
        Some(endpoint) => builder.with_endpoint(endpoint),
        None => builder,
    }
}

/// Create an anonymous, read-only S3 client.
fn create_s3_client_readonly(
    bucket: &str,
    region: &str,
    timeout: Duration,
) -> Result<AmazonS3, anyhow::Error> {
    apply_s3_location_and_timeout(AmazonS3Builder::new(), bucket, region, timeout)
        .with_skip_signature(true)
        .build()
        .map_err(Into::into)
}

/// Credentials for S3 read/write access (from JSON file).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct S3CredentialsConfig {
    access_key: String,
    secret_key: String,
}

/// Create a read/write S3 client, optionally using a JSON credentials file.
fn create_s3_client_read_write(
    bucket: &str,
    region: &str,
    timeout: Duration,
    credentials_file: Option<PathBuf>,
) -> Result<AmazonS3, anyhow::Error> {
    let s3 = match credentials_file {
        Some(credentials_file) => {
            let mut file = std::fs::File::open(credentials_file)?;
            let mut json_config_str = String::new();
            file.read_to_string(&mut json_config_str)?;
            let credentials_config: S3CredentialsConfig = serde_json::from_str(&json_config_str)?;
            apply_s3_location_and_timeout(AmazonS3Builder::new(), bucket, region, timeout)
                .with_access_key_id(&credentials_config.access_key)
                .with_secret_access_key(&credentials_config.secret_key)
                .build()?
        }
        None => apply_s3_location_and_timeout(AmazonS3Builder::from_env(), bucket, region, timeout)
            .build()?,
    };
    Ok(s3)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn aws_region(region: &str) -> S3RegionAndEndpoint {
        S3RegionAndEndpoint { region: region.to_string(), endpoint: None }
    }

    fn region_with_endpoint(region: &str, endpoint: &str) -> S3RegionAndEndpoint {
        S3RegionAndEndpoint { region: region.to_string(), endpoint: Some(endpoint.to_string()) }
    }

    #[test]
    fn parse_s3_region_keeps_aws_region_names() {
        assert_eq!(parse_s3_region_and_endpoint("us-east-1"), aws_region("us-east-1"));
        assert_eq!(parse_s3_region_and_endpoint("ap-south-2"), aws_region("ap-south-2"));
    }

    #[test]
    fn parse_s3_region_maps_provider_names_to_endpoints() {
        let digitalocean = region_with_endpoint("nyc3", "https://nyc3.digitaloceanspaces.com");
        let yandex = region_with_endpoint("ru-central1", "https://storage.yandexcloud.net");
        let wasabi = region_with_endpoint("eu-central-2", "https://s3.eu-central-2.wasabisys.com");
        assert_eq!(parse_s3_region_and_endpoint("nyc3"), digitalocean);
        assert_eq!(parse_s3_region_and_endpoint("yandex"), yandex);
        assert_eq!(parse_s3_region_and_endpoint("ru-central1"), yandex);
        assert_eq!(parse_s3_region_and_endpoint("wa-eu-central-2"), wasabi);
    }

    #[test]
    fn parse_s3_region_uses_custom_endpoint_with_default_signing_region() {
        let url_endpoint =
            region_with_endpoint(CUSTOM_ENDPOINT_SIGNING_REGION, "http://localhost:9000");
        let host_endpoint =
            region_with_endpoint(CUSTOM_ENDPOINT_SIGNING_REGION, "https://s3.example.com");
        assert_eq!(parse_s3_region_and_endpoint("http://localhost:9000"), url_endpoint);
        assert_eq!(parse_s3_region_and_endpoint("s3.example.com"), host_endpoint);
    }
}
