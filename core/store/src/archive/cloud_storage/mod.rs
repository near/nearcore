use std::path::PathBuf;

use near_external_storage::ExternalConnection;
use near_primitives::types::BlockHeight;

pub(crate) mod block_data;
pub mod opener;
pub mod retrieve;
pub mod update;

/// Represents the external storage for archival data.
pub struct CloudStorage {
    external: ExternalConnection,
}

/// Identifiers of files stored in cloud archival storage.
/// Each variant maps to a specific logical file within the archive.
#[derive(Clone, Debug)]
pub enum CloudStorageFileID {
    /// The metadata file storing the current archival head.
    Head,
    /// A block file for a given block height.
    Block(BlockHeight),
}

impl CloudStorageFileID {
    /// Returns the path components representing this file within the storage hierarchy.
    fn path_parts(&self) -> Vec<String> {
        match self {
            CloudStorageFileID::Head => vec!["metadata".to_string(), "head".to_string()],
            CloudStorageFileID::Block(height) => vec![height.to_string(), "block".to_string()],
        }
    }

    /// Returns the full relative path (as a UTF-8 string) for this file.
    fn path(&self) -> String {
        let path = PathBuf::from_iter(self.path_parts());
        path.to_str().expect("non UTF-8 string").to_string()
    }

    /// Returns the directory path and file name separately.
    fn dir_and_file_name(&self) -> (String, String) {
        let mut path_parts = self.path_parts();
        let file_name = path_parts.pop().expect("File path must be non-empty");
        let dir = PathBuf::from_iter(path_parts);
        let dir_str = dir.to_str().expect("non UTF-8 string").to_string();
        (dir_str, file_name)
    }
}
