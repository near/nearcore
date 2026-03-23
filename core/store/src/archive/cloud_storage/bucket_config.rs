use borsh::{BorshDeserialize, BorshSerialize};

/// Versioned archive-wide configuration stored in the bucket.
/// The first writer creates it; subsequent writers validate their local
/// settings match. This ensures all writers use identical parameters.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, PartialEq, Eq)]
pub enum BucketConfig {
    V1(BucketConfigV1),
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, PartialEq, Eq)]
pub struct BucketConfigV1 {
    /// Zstd compression level for data blobs.
    pub compression_level: i32,
}

impl BucketConfig {
    /// The configuration that this binary expects to use.
    pub fn local() -> Self {
        // TODO(cloud_archival): Benchmark compression levels before releasing.
        Self::V1(BucketConfigV1 { compression_level: 3 })
    }

    pub fn compression_level(&self) -> i32 {
        match self {
            Self::V1(v1) => v1.compression_level,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::cloud_storage::file_id::CloudStorageFileID;
    use crate::archive::cloud_storage::tests::test_cloud_storage;

    #[tokio::test]
    async fn ensure_bucket_config_creates_config_on_empty_bucket() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let cloud_storage = test_cloud_storage(&tmp_dir);

        cloud_storage.ensure_bucket_config().await.unwrap();

        let config_path = tmp_dir.path().join(cloud_storage.file_path(&CloudStorageFileID::Config));
        assert!(config_path.exists(), "config file should be created");
        let bytes = std::fs::read(&config_path).unwrap();
        let config: BucketConfig = borsh::from_slice(&bytes).unwrap();
        assert_eq!(config, BucketConfig::local());
    }

    #[tokio::test]
    async fn ensure_bucket_config_accepts_matching_config() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let cloud_storage = test_cloud_storage(&tmp_dir);

        // First call creates the config.
        cloud_storage.ensure_bucket_config().await.unwrap();
        // Second call validates it matches.
        cloud_storage.ensure_bucket_config().await.unwrap();
    }

    #[tokio::test]
    async fn ensure_bucket_config_rejects_mismatched_config() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let cloud_storage = test_cloud_storage(&tmp_dir);

        // Write a config with a different compression level.
        let wrong_config = BucketConfig::V1(BucketConfigV1 { compression_level: 9 });
        let file_id = CloudStorageFileID::Config;
        let blob = borsh::to_vec(&wrong_config).unwrap();
        cloud_storage.upload(file_id, blob).await.unwrap();

        let result = cloud_storage.ensure_bucket_config().await;
        assert!(result.is_err(), "should reject mismatched config");
        let err = result.unwrap_err().to_string();
        assert!(err.contains("mismatch"), "error should mention mismatch: {err}");
    }
}
