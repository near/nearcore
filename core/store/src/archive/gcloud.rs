use std::sync::Arc;

use std::io;

use crate::DBCol;

use super::ArchivalStorage;

pub(crate) struct GoogleCloudArchiver {
    _gcs_client: Arc<cloud_storage::Client>,
    _bucket: String,
}

impl GoogleCloudArchiver {
    pub(crate) fn open(bucket: &str) -> Self {
        Self {
            _gcs_client: Arc::new(cloud_storage::Client::default()),
            _bucket: bucket.to_string(),
        }
    }
}

impl ArchivalStorage for GoogleCloudArchiver {
    fn put(&self, _col: DBCol, _key: &[u8], _value: &[u8]) -> io::Result<()> {
        unimplemented!()
    }

    fn get(&self, _col: DBCol, _key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        unimplemented!()
    }

    fn delete(&self, _col: DBCol, _key: &[u8]) -> io::Result<()> {
        unimplemented!()
    }
}
