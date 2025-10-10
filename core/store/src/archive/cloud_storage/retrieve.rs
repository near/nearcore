use near_primitives::types::BlockHeight;

use borsh::BorshDeserialize;

use crate::archive::cloud_storage::{CloudStorage, CloudStorageFileID};

/// Error surfaced while archiving data or performing sanity checks.
#[derive(thiserror::Error, Debug)]
pub enum CloudRetrievalError {
    #[error("Error when performing get from cloud archive: {file_id:?}. Error: {error}")]
    GetError { file_id: CloudStorageFileID, error: anyhow::Error },
    #[error("Error when deserializing value from cloud archive: {file_id:?}. Error: {error}")]
    DeserializeError { file_id: CloudStorageFileID, error: borsh::io::Error },
    #[error("Error when listing directory in cloud archive: {dir}. Error: {error}")]
    ListError { dir: String, error: anyhow::Error },
}

impl CloudStorage {
    /// Returns the cloud head from external storage.
    pub async fn get_cloud_head(&self) -> Result<BlockHeight, CloudRetrievalError> {
        let file_id = CloudStorageFileID::Head;
        let value = self.get(file_id.clone()).await?;
        let head = BlockHeight::try_from_slice(&value)
            .map_err(|error| CloudRetrievalError::DeserializeError { file_id, error })?;
        Ok(head)
    }

    /// Returns the cloud head from external storage, if any.
    pub async fn get_cloud_head_if_exists(
        &self,
    ) -> Result<Option<BlockHeight>, CloudRetrievalError> {
        if !self.exists(&CloudStorageFileID::Head).await? {
            return Ok(None);
        }
        let cloud_head = self.get_cloud_head().await?;
        Ok(Some(cloud_head))
    }

    async fn get(&self, file_id: CloudStorageFileID) -> Result<Vec<u8>, CloudRetrievalError> {
        let path = file_id.path();
        self.external
            .get(&path)
            .await
            .map_err(|error| CloudRetrievalError::GetError { file_id, error })
    }

    /// Note: under the hood, it lists the containing directory recursively, use with caution!
    async fn exists(&self, file_id: &CloudStorageFileID) -> Result<bool, CloudRetrievalError> {
        let (dir, name) = file_id.containing_dir_and_name();
        let files = self
            .external
            .list(&dir)
            .await
            .map_err(|error| CloudRetrievalError::ListError { dir, error })?;
        Ok(files.contains(&name))
    }
}
