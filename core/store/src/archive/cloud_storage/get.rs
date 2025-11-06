use std::io::{Error, ErrorKind, Result};

use near_primitives::hash::CryptoHash;
use near_primitives::types::BlockHeight;

use crate::adapter::StoreAdapter;
use crate::archive::cloud_storage::download::CloudRetrievalError;
use crate::archive::cloud_storage::CloudStorage;
use crate::db::DBSlice;
use crate::DBCol;

impl From<CloudRetrievalError> for Error {
    fn from(error: CloudRetrievalError) -> Self {
        Error::other(error)
    }
}

impl CloudStorage {
    pub fn get(&self, col: DBCol, key: &[u8]) -> Result<Option<DBSlice<'_>>> {
        let bytes = futures::executor::block_on(self.get_impl(col, key))?;
        Ok(bytes.map(DBSlice::from_vec))
    }

    async fn get_impl(&self, col: DBCol, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match col {
            DBCol::Block => {
                let block_height = self.block_height_from_hash_bytes(key)?;
                let block_data = self.retrieve_block_data(block_height).await?;
                let bytes = borsh::to_vec(block_data.get_block())?;
                Ok(Some(bytes))
            }
            _ => {
                todo!("implement")
            }
        }
    }

    fn block_height_from_hash_bytes(&self, block_hash_bytes: &[u8]) -> Result<BlockHeight> {
        let block_hash = CryptoHash::try_from(block_hash_bytes).map_err(
            |error| Error::new(ErrorKind::InvalidInput, error)
        )?;
        let block_height = self.hot_store.chain_store().get_block_height(&block_hash).map_err(
            |error| Error::new(ErrorKind::Other, error)
        )?;
        Ok(block_height)
    }
}
