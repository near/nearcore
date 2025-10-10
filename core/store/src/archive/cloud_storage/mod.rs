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

#[derive(Clone, Debug)]
pub enum CloudStorageFileID {
    Head,
    Block(BlockHeight),
}

impl CloudStorageFileID {
    fn path_parts(&self) -> Vec<String> {
        match self {
            CloudStorageFileID::Head => vec!["metadata".to_string(), "head".to_string()],
            CloudStorageFileID::Block(height) => vec![height.to_string(), "block".to_string()],
        }
    }

    fn path(&self) -> String {
        let path = PathBuf::from_iter(self.path_parts());
        path.to_str().expect("non UTF-8 string").to_string()
    }

    fn containing_dir_and_name(&self) -> (String, String) {
        let mut path_parts = self.path_parts();
        let name = path_parts.pop().expect("File path must be non-empty");
        let path = PathBuf::from_iter(path_parts);
        let containing_dir = path.to_str().expect("non UTF-8 string").to_string();
        (containing_dir, name)
    }
}
