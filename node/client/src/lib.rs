extern crate runtime;
extern crate primitives;
extern crate storage;

use runtime::Runtime;
use primitives::{types::EpochBlockHeader, hash::HashValue};
use storage::Storage;

pub struct Client {
    runtime: Runtime,
    storage: Storage,
    last_header: HashValue,
}

impl Client {
    pub fn new(genesis: EpochBlockHeader,
               runtime: Runtime,
               storage: Storage) -> Self {
        let last_header = storage.put(genesis);
        Client {
            runtime,
            storage,
            last_header,
        }
    }

    pub fn add_header(&self, header: EpochBlockHeader) {
        if self.runtime.verify_epoch_header(&header) {
            self.storage.put(&header);
        }
    }
}
