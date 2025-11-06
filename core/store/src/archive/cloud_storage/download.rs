use near_primitives::types::BlockHeight;

use borsh::BorshDeserialize;

use crate::archive::cloud_storage::CloudStorage;
use crate::archive::cloud_storage::block_data::BlockData;
use crate::archive::cloud_storage::file_id::CloudStorageFileID;

/// Errors surfaced while retrieving data from the cloud archive.
#[derive(thiserror::Error, Debug)]
pub enum CloudRetrievalError {
    #[error("Failed to retrieve {file_id:?} from the cloud archive: {error}")]
    GetError { file_id: CloudStorageFileID, error: anyhow::Error },
    #[error("Failed to deserialize {file_id:?} from the cloud archive: {error}")]
    DeserializeError { file_id: CloudStorageFileID, error: borsh::io::Error },
    #[error("Failed to list directory in the cloud archive: {dir}; error: {error}")]
    ListError { dir: String, error: anyhow::Error },
}

impl CloudStorage {
    /// Returns the cloud head from external storage, if present.
    pub async fn retrieve_cloud_head_if_exists(
        &self,
    ) -> Result<Option<BlockHeight>, CloudRetrievalError> {
        if !self.exists(&CloudStorageFileID::Head).await? {
            return Ok(None);
        }
        let cloud_head = self.retrieve_cloud_head().await?;
        Ok(Some(cloud_head))
    }

    /// Returns the cloud head from external storage.
    pub async fn retrieve_cloud_head(&self) -> Result<BlockHeight, CloudRetrievalError> {
        self.retrieve(&CloudStorageFileID::Head).await
    }

    pub async fn retrieve_block_data(
        &self,
        block_height: BlockHeight,
    ) -> Result<BlockData, CloudRetrievalError> {
        let file_id = CloudStorageFileID::Block(block_height);
        self.retrieve(&file_id).await
    }

    /// Downloads and deserializes a file from the cloud archive.
    async fn retrieve<T: BorshDeserialize>(
        &self,
        file_id: &CloudStorageFileID,
    ) -> Result<T, CloudRetrievalError> {
        let bytes = self.download(file_id).await?;
        T::try_from_slice(&bytes).map_err(|error| CloudRetrievalError::DeserializeError {
            file_id: file_id.clone(),
            error,
        })
    }

    /// Downloads the raw bytes for a given file in the cloud archive.
    async fn download(&self, file_id: &CloudStorageFileID) -> Result<Vec<u8>, CloudRetrievalError> {
        let path = file_id.path();
        self.external
            .get(&path)
            .await
            .map_err(|error| CloudRetrievalError::GetError { file_id: file_id.clone(), error })
    }

    /// Checks if a given file exists in the cloud archive.
    ///
    /// Note: Internally this may trigger a recursive directory listing — avoid calling it
    /// on directories containing many files.
    async fn exists(&self, file_id: &CloudStorageFileID) -> Result<bool, CloudRetrievalError> {
        let (dir, name) = file_id.dir_and_file_name();
        let files = self
            .external
            .list(&dir)
            .await
            .map_err(|error| CloudRetrievalError::ListError { dir, error })?;
        Ok(files.contains(&name))
    }
}
