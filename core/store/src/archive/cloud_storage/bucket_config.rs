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
    compression_level: i32,
    /// Number of consecutive block heights bundled into a single batch blob.
    batch_size: u32,
}

impl BucketConfigV1 {
    fn new(compression_level: i32, batch_size: u32) -> Self {
        assert!(batch_size > 0, "batch_size must be > 0");
        Self { compression_level, batch_size }
    }
}

impl BucketConfig {
    /// The canonical configuration that every writer is expected to use.
    /// The bucket stores one copy of this; mismatches across writers are rejected.
    pub fn canonical() -> Self {
        // TODO(cloud_archival): Benchmark compression levels before releasing.
        // TODO(cloud_archival): Bump batch_size once writer batches multiple
        // heights per upload.
        Self::V1(BucketConfigV1::new(3, 1))
    }

    /// Test-only constructor that overrides `batch_size`. All other fields match `canonical()`.
    #[cfg(feature = "test_features")]
    pub fn with_batch_size_for_test(batch_size: u32) -> Self {
        let canonical = Self::canonical();
        Self::V1(BucketConfigV1::new(canonical.compression_level(), batch_size))
    }

    pub fn compression_level(&self) -> i32 {
        match self {
            Self::V1(v1) => v1.compression_level,
        }
    }

    pub fn batch_size(&self) -> u32 {
        match self {
            Self::V1(v1) => v1.batch_size,
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
        assert_eq!(config, BucketConfig::canonical());
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

    #[cfg(feature = "test_features")]
    #[test]
    fn with_batch_size_for_test_preserves_compression_level() {
        let config = BucketConfig::with_batch_size_for_test(16);
        assert_eq!(config.compression_level(), BucketConfig::canonical().compression_level());
        assert_eq!(config.batch_size(), 16);
    }

    #[cfg(feature = "test_features")]
    #[test]
    #[should_panic(expected = "batch_size must be > 0")]
    fn with_batch_size_for_test_panics_on_zero() {
        let _ = BucketConfig::with_batch_size_for_test(0);
    }

    #[tokio::test]
    async fn ensure_bucket_config_rejects_mismatched_config() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let cloud_storage = test_cloud_storage(&tmp_dir);

        // Write a config with a different compression level.
        let wrong_config = BucketConfig::V1(BucketConfigV1::new(9, 4));
        let file_id = CloudStorageFileID::Config;
        let blob = borsh::to_vec(&wrong_config).unwrap();
        cloud_storage.upload(file_id, blob).await.unwrap();

        let result = cloud_storage.ensure_bucket_config().await;
        assert!(result.is_err(), "should reject mismatched config");
        let err = result.unwrap_err().to_string();
        assert!(err.contains("mismatch"), "error should mention mismatch: {err}");
    }
}
