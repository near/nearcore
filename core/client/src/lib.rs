extern crate runtime;
extern crate primitives;
extern crate storage;


pub struct Client {
    runtime: runtime::Runtime,
    storage: storage::Storage,
    last_header: primitives::hash::HashValue,
}

impl Client {
    pub fn new(genesis: primitives::types::EpochBlockHeader, runtime: runtime::Runtime, storage: storage::Storage) -> Self {
        let last_header = storage.put(genesis);
        Client {
            runtime: runtime,
            storage: storage,
            last_header: last_header,
        }
    }

    pub fn add_header(&self, header: primitives::types::EpochBlockHeader) {
        if self.runtime.verify_epoch_header(&header) {
            self.storage.put(&header);
        }
    }
}