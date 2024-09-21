pub mod flat_store;

use std::io;

use crate::{Store, StoreUpdate};

pub trait StoreAdapter {
    fn store(&self) -> Store;

    fn flat_store(&self) -> flat_store::FlatStoreAdapter {
        flat_store::FlatStoreAdapter::new(self.store())
    }
}

pub trait StoreUpdateAdapter: Sized {
    fn store_update(self) -> StoreUpdate;

    fn commit(self) -> io::Result<()> {
        self.store_update().commit()
    }

    fn flat_store_update(self) -> flat_store::FlatStoreUpdateAdapter {
        flat_store::FlatStoreUpdateAdapter::new(self.store_update())
    }
}
