use std::path::PathBuf;
use std::sync::Arc;

use near_chain_configs::{DumpConfig, ExternalStorageLocation};

use crate::archive::cloud_storage::CloudStorage;
use crate::archive::cloud_storage::opener::CloudStorageOpener;

/// Configuration for a cloud-based archival node.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct CloudArchivalConfig {
    /// The storage location of the archival data.
    pub location: ExternalStorageLocation,
    /// Location of a json file with credentials allowing access to the bucket.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credentials_file: Option<PathBuf>,
}

/// Default dumper config used by cloud archive writers to upload state snapshots.
impl CloudArchivalConfig {
    pub fn into_default_dump_config(self) -> DumpConfig {
        DumpConfig {
            location: self.location,
            restart_dump_for_shards: None,
            iteration_delay: None,
            credentials_file: self.credentials_file,
        }
    }
}

/// Configures the external storage used by the archival node.
#[derive(Clone)]
pub struct CloudStorageContext {
    pub cloud_archive: CloudArchivalConfig,
    pub chain_id: String,
}

/// Creates a test cloud archival configuration using a local filesystem path.
pub fn test_cloud_archival_config(root_dir: impl Into<PathBuf>) -> CloudArchivalConfig {
    let storage_dir = root_dir.into().join("cloud_archival");
    let location = ExternalStorageLocation::Filesystem { root_dir: storage_dir };
    CloudArchivalConfig { location, credentials_file: None }
}

/// Creates a test cloud storage context.
pub fn test_cloud_storage_context(
    root_dir: impl Into<PathBuf>,
    chain_id: String,
) -> CloudStorageContext {
    CloudStorageContext { cloud_archive: test_cloud_archival_config(root_dir), chain_id }
}

/// Initializes a test cloud storage instance based on the test configuration.
pub fn create_test_cloud_storage(root_dir: PathBuf, chain_id: String) -> Arc<CloudStorage> {
    let context = test_cloud_storage_context(root_dir, chain_id);
    CloudStorageOpener::new(context).open().unwrap()
}
