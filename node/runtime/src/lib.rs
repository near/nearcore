extern crate primitives;

pub struct Runtime {}

impl Runtime {
    pub fn verify_epoch_header(&self, header: &primitives::types::EpochBlockHeader) -> bool {
        true
    }
}
