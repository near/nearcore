use std::sync::Arc;

use std::io;

use super::ArchivalStorage;

pub(crate) struct GoogleCloudStorage {
    gcs_client: Arc<cloud_storage::Client>,
    bucket: String,
    runtime: tokio::runtime::Runtime,
}

impl GoogleCloudStorage {
    pub(crate) fn open(bucket: &str) -> Self {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        Self {
            gcs_client: Arc::new(cloud_storage::Client::default()),
            bucket: bucket.to_string(),
            runtime,
        }
    }
}

impl ArchivalStorage for GoogleCloudStorage {
    fn put(&self, path: &std::path::Path, value: &[u8]) -> io::Result<()> {
        let _ = self.runtime.block_on(async {
            let filename = path.to_str().unwrap();
            tracing::debug!(target: "cold_store", data_len = value.len(), ?filename, "Put to GCS");
            self.gcs_client
                    .object()
                    .create(&self.bucket, value.to_vec(), filename, "application/octet-stream")
                    .await
        }).map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
        Ok(())
    }

    fn get(&self, path: &std::path::Path) -> io::Result<Option<Vec<u8>>> {
        let value = self.runtime.block_on(async {
            let filename = path.to_str().unwrap();
            tracing::debug!(target: "cold_store", ?filename, "Get from GCS");
            self.gcs_client.object().download(&self.bucket, filename).await.ok()
        });
        Ok(value)
    }
}
