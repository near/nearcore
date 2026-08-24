use crate::archive::cloud_archival_utils::CloudArchivalReaderError;

/// Reads recent chain data out of cloud storage into a local store.
#[derive(Clone)]
pub struct CloudArchivalRecentReader {}

impl CloudArchivalRecentReader {
    pub fn new() -> Self {
        Self {}
    }

    // TODO(cloud_archival): follow the bucket, which is what makes this async.
    #[allow(clippy::unused_async)]
    pub async fn cloud_archival_loop(self) -> Result<(), CloudArchivalReaderError> {
        Ok(())
    }
}
