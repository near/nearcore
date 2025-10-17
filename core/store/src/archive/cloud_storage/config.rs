use std::path::PathBuf;
use std::sync::Arc;

use near_chain_configs::{
    CloudArchivalWriterConfig, ExternalStorageLocation, default_archival_writer_polling_interval,
};

use crate::StoreConfig;
use crate::archive::cloud_storage::CloudStorage;
use crate::archive::cloud_storage::opener::CloudStorageOpener;

/// Configures the external storage used by the archival node.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct CloudStorageConfig {
    /// The storage location of the archival data.
    pub location: ExternalStorageLocation,
    /// Location of a json file with credentials allowing access to the bucket.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credentials_file: Option<PathBuf>,
}

/// Configuration for a cloud-based archival node.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct CloudArchivalConfig {
    /// Configures the external storage used by the archival node.
    pub cloud_storage: CloudStorageConfig,
    pub prefetch_db: Option<StoreConfig>,
}

pub fn test_cloud_archival_configs(
    root_dir: impl Into<PathBuf> + Clone,
) -> (CloudArchivalConfig, CloudArchivalWriterConfig) {
    let storage_dir = root_dir.into().join("cloud_archival");
    let cloud_storage = CloudStorageConfig {
        location: ExternalStorageLocation::Filesystem { root_dir: storage_dir },
        credentials_file: None,
    };
    let config =
        CloudArchivalConfig { cloud_storage, prefetch_db: Some(StoreConfig::test_config()) };
    let writer_config = CloudArchivalWriterConfig {
        archive_block_data: true,
        polling_interval: default_archival_writer_polling_interval(),
    };
    (config, writer_config)
}

pub fn create_test_cloud_storage(root_dir: PathBuf) -> Arc<CloudStorage> {
    let (config, _) = test_cloud_archival_configs(root_dir.clone());
    CloudStorageOpener::new(&root_dir, config).open().unwrap()
}
