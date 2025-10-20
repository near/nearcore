use std::path::PathBuf;
use std::sync::Arc;

use near_chain_configs::ExternalStorageLocation;

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

/// Creates a test cloud archival configuration using a local filesystem path.
pub fn test_cloud_archival_config(root_dir: impl Into<PathBuf> + Clone) -> CloudStorageConfig {
    let storage_dir = root_dir.into().join("cloud_archival");
    let location = ExternalStorageLocation::Filesystem { root_dir: storage_dir };
    CloudStorageConfig { location, credentials_file: None }
}

/// Initializes a test cloud storage instance based on the test configuration.
pub fn create_test_cloud_storage(root_dir: PathBuf) -> Arc<CloudStorage> {
    let config = test_cloud_archival_config(root_dir);
    CloudStorageOpener::new(config).open()
}
