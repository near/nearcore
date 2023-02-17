use near_primitives::state::ValueRef;

pub struct FlatStorageChunkView {
}

impl FlatStorageChunkView {
    pub fn get_ref(&self, _key: &[u8]) -> Result<Option<ValueRef>, crate::StorageError> {
        todo!();
    }
}