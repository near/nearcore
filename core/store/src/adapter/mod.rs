pub mod flat_store;

use crate::{Store, StoreUpdate};

pub trait StoreAdapter {
    fn store(&self) -> Store;

    fn store_update(&self) -> StoreUpdate {
        self.store().store_update()
    }

    fn flat_store(&self) -> flat_store::FlatStoreAdapter {
        flat_store::FlatStoreAdapter::new(self.store())
    }
}

pub trait StoreUpdateAdapter: Sized {
    fn store_update(&mut self) -> &mut StoreUpdate;

    fn flat_store_update(&mut self) -> flat_store::FlatStoreUpdateAdapter {
        flat_store::FlatStoreUpdateAdapter::new(self.store_update())
    }
}
