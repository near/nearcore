use std::io::{Error, ErrorKind, Result};

use near_primitives::hash::CryptoHash;

use crate::DBCol;
use crate::adapter::StoreAdapter;
use crate::archive::cloud_storage::CloudStorage;
use crate::archive::cloud_storage::block_data::BlockData;
use crate::archive::cloud_storage::download::CloudRetrievalError;
use crate::db::DBSlice;

impl From<CloudRetrievalError> for std::io::Error {
    fn from(error: CloudRetrievalError) -> Self {
        Error::other(error)
    }
}

impl CloudStorage {
    pub fn get(&self, col: DBCol, key: &[u8]) -> Result<Option<DBSlice<'_>>> {
        let bytes = match col {
            DBCol::Block => {
                let block_data = self.get_block_data(key)?;
                borsh::to_vec(block_data.get_block())?
            }
            _ => {
                if cfg!(debug_assertions) {
                    todo!("implement");
                }
                return Ok(None);
            }
        };
        Ok(Some(DBSlice::from_vec(bytes)))
    }

    fn get_block_data(&self, block_hash: &[u8]) -> Result<BlockData> {
        let block_hash = CryptoHash::try_from(block_hash)
            .map_err(|error| Error::new(ErrorKind::InvalidInput, error))?;
        let block_height = self
            .hot_store
            .chain_store()
            .get_block_height(&block_hash)
            .map_err(|error| Error::new(ErrorKind::Other, error))?;
        let block_data = block_on_future(self.retrieve_block_data(block_height))?;
        Ok(block_data)
    }
}

// TODO(cloud_archival) Attention! This is a temporary solution for development.
// Make sure the final version won't negatively impact / crash the application.
fn block_on_future<F: Future>(fut: F) -> F::Output {
    futures::executor::block_on(fut)
}
