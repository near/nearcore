use anyhow::Result;
use std::path::PathBuf;
//use std::sync::Arc;

use near_chain_configs::CloudStorageConfig;

use crate::archive::cloud_storage::CloudStorage;

/// Opener for the external archival storage, which results in an `CloudStorage` instance.
#[allow(unused)]
pub struct CloudStorageOpener {
    /// NEAR home directory (eg. '/home/ubuntu/.near')
    home_dir: PathBuf,
    /// Configuration for the external storage.
    config: CloudStorageConfig,
}

impl CloudStorageOpener {
    pub fn new(home_dir: PathBuf, config: CloudStorageConfig) -> Self {
        Self { home_dir, config }
    }

    pub fn open(&self) -> Result<CloudStorage> {
        // let connection = match self.config.storage {
        //     ExternalStorageLocation::S3 { bucket, region } => ExternalConnection::S3 {
        //         bucket: Arc::new(create_bucket_read_write(&bucket, &region, std::time::Duration::from_secs(30), dump_config.credentials_file).expect(
        //             "Failed to authenticate connection to S3. Please either provide AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in the environment, or create a credentials file and link it in config.json as 's3_credentials_file'."))
        //     },
        //     ExternalStorageLocation::Filesystem { root_dir } => ExternalConnection::Filesystem { root_dir },
        //     ExternalStorageLocation::GCS { bucket } => {
        //         if let Some(credentials_file) = dump_config.credentials_file {
        //             if let Ok(var) = std::env::var("SERVICE_ACCOUNT") {
        //                 tracing::warn!(target: "state_sync_dump", "Environment variable 'SERVICE_ACCOUNT' is set to {var}, but 'credentials_file' in config.json overrides it to '{credentials_file:?}'");
        //                 println!("Environment variable 'SERVICE_ACCOUNT' is set to {var}, but 'credentials_file' in config.json overrides it to '{credentials_file:?}'");
        //             }
        //             // SAFE: no threads *yet*.
        //             unsafe { std::env::set_var("SERVICE_ACCOUNT", &credentials_file) };
        //             tracing::info!(target: "state_sync_dump", "Set the environment variable 'SERVICE_ACCOUNT' to '{credentials_file:?}'");
        //         }
        //         ExternalConnection::GCS {
        //             gcs_client: Arc::new(object_store::gcp::GoogleCloudStorageBuilder::from_env().with_bucket_name(&bucket).build().unwrap()),
        //             reqwest_client: Arc::new(reqwest::Client::default()),
        //             bucket,
        //         }
        //     }
        // };
        // let cloud_storage = CloudStorage { connection };
        // Ok(cloud_storage)
        unimplemented!("TODO(cloud_archival)")
    }
}
