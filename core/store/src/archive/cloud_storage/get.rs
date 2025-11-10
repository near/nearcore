use std::io::{Error, ErrorKind, Result};
use std::sync::Arc;

use near_primitives::hash::CryptoHash;

use crate::adapter::StoreAdapter;
use crate::archive::cloud_storage::CloudStorage;
use crate::archive::cloud_storage::block_data::BlockData;
use crate::db::{DBSlice, Database};
use crate::{DBCol, Store};

impl CloudStorage {
    /// Synchronously retrieves data from cloud storage, blocking on asynchronous operations internally.
    /// Returns `None` for non-cold columns.
    // TODO(cloud_archival) handle remaining cold columns
    pub fn get(
        &self,
        hot_db: &Arc<dyn Database>,
        col: DBCol,
        key: &[u8],
    ) -> Result<Option<DBSlice<'_>>> {
        let bytes = match col {
            DBCol::Block => {
                let block_data = self.get_block_data(hot_db, key)?;
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

    fn get_block_data(&self, hot_db: &Arc<dyn Database>, block_hash: &[u8]) -> Result<BlockData> {
        let block_hash = CryptoHash::try_from(block_hash)
            .map_err(|error| Error::new(ErrorKind::InvalidInput, error))?;
        let hot_store = Store::new(hot_db.clone());
        let block_height =
            hot_store.chain_store().get_block_height(&block_hash).map_err(Error::other)?;
        let block_data =
            block_on_future(self.retrieve_block_data(block_height)).map_err(Error::other)?;
        Ok(block_data)
    }
}

// TODO(cloud_archival): This is a temporary solution for development.
// Ensure the final implementation does not negatively impact or crash the application.
fn block_on_future<F: Future>(fut: F) -> F::Output {
    futures::executor::block_on(fut)
}
