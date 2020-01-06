use crate::runtime_state::state::ValueRef;
use near_primitives::errors::StorageError;

pub type StateItem = Result<(Vec<u8>, Vec<u8>), StorageError>;

pub trait StateIterator: Iterator<Item = StateItem> {
    /// Position the iterator on the first element with key => `key`.
    fn seek(&mut self, key: &[u8]) -> Result<(), StorageError>;
}
