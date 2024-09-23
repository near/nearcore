pub mod flat_store;

use std::ops::{Deref, DerefMut};

use crate::{Store, StoreUpdate};

enum StoreUpdateHolder<'a> {
    Reference(&'a mut StoreUpdate),
    Owned(StoreUpdate),
}

impl Deref for StoreUpdateHolder<'_> {
    type Target = StoreUpdate;

    fn deref(&self) -> &Self::Target {
        match self {
            StoreUpdateHolder::Reference(store_update) => store_update,
            StoreUpdateHolder::Owned(store_update) => store_update,
        }
    }
}

impl DerefMut for StoreUpdateHolder<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            StoreUpdateHolder::Reference(store_update) => store_update,
            StoreUpdateHolder::Owned(store_update) => store_update,
        }
    }
}

impl Into<StoreUpdate> for StoreUpdateHolder<'static> {
    fn into(self) -> StoreUpdate {
        match self {
            StoreUpdateHolder::Reference(_) => panic!("converting borrowed store update"),
            StoreUpdateHolder::Owned(store_update) => store_update,
        }
    }
}

pub trait StoreAdapter {
    fn store(&self) -> Store;

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
