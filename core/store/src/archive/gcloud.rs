use std::sync::Arc;

use std::io;

use crate::DBCol;

use super::ArchivalStorage;

pub(crate) struct GoogleCloudArchiver {
    gcs_client: Arc<cloud_storage::Client>,
    bucket: String,
}

impl GoogleCloudArchiver {
    pub(crate) fn open(bucket: &str) -> Self {
        Self { gcs_client: Arc::new(cloud_storage::Client::default()), bucket: bucket.to_string() }
    }
}

impl ArchivalStorage for GoogleCloudArchiver {
    fn put(&self, col: DBCol, key: &[u8], value: &[u8]) -> io::Result<()> {
        let async_runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
        let _ = async_runtime.block_on(async {
            let location = "fake";
            tracing::debug!(target: "archiver", key_len=key.len(), data_len = value.len(), ?location, "Writing to GCS");
            self.gcs_client
                    .object()
                    .create(&self.bucket, value.to_vec(), location, "application/octet-stream")
                    .await
        }).map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
        Ok(())
    }

    fn get(&self, _col: DBCol, _key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        unimplemented!()
    }

    fn delete(&self, _col: DBCol, _key: &[u8]) -> io::Result<()> {
        unimplemented!()
    }
}
