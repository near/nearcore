pub mod flat_store;

use std::ops::{Deref, DerefMut};

use crate::{Store, StoreUpdate};

/// Internal enum that can store either an owned StoreUpdate to a reference to StoreUpdate.
/// 
/// While dealing with store update, the typical pattern is to do something like:
/// ```rust, ignore
/// let store_update: StoreUpdate = store.store_update();
/// 
/// store_update.set_foo("bar");
/// some_large_update_function(&mut store_update);
/// 
/// store_update.commit()?;
/// ```
/// Now with StoreAdapters, store could be of any of the type of the adapters, example `FlatStoreAdapter`.
/// In that case, we expect the above pattern to look similar, however we would expect calls to
/// `flat_store.store_update()` to return type `FlatStoreUpdateAdapter` instead of `StoreUpdate`.
/// 
/// At the same time we would like to allow conversion of `StoreUpdate` to `FlatStoreUpdateAdapter`.
/// 
/// ```rust, ignore
/// fn update_flat_store(flat_store_update: &mut FlatStoreUpdateAdapter) {
///     ...
/// }
/// 
/// // Pattern 1: reference to store_update
/// let store_update: StoreUpdate = store.store_update();
/// update_flat_store(&mut store_update.flat_store_update());
/// store_update.commit()?;
/// 
/// // Pattern 2: owned store_update
/// let flat_store: FlatStoreAdapter = store.flat_store();
/// let flat_store_update: FlatStoreUpdateAdapter<'static> = flat_store.store_update();
/// update_flat_store(&mut flat_store_update);
/// flat_store_update.commit()?;
/// ```
/// 
/// To make both these patterns possible, where in pattern 1, flat_store_update holds a reference to store_update
/// and in pattern 2, flat_store_update owns the instance of store_update, we use this enum.
/// 
/// Note that owned versions of flat_store_update have a static lifetime as compared to borrowed versions.
enum StoreUpdateHolder<'a> {
    Reference(&'a mut StoreUpdate),
    Owned(StoreUpdate),
}

// Seamless conversion from &store_update_holder to &store_update.
impl Deref for StoreUpdateHolder<'_> {
    type Target = StoreUpdate;

    fn deref(&self) -> &Self::Target {
        match self {
            StoreUpdateHolder::Reference(store_update) => store_update,
            StoreUpdateHolder::Owned(store_update) => store_update,
        }
    }
}

// Seamless conversion from &mut store_update_holder to &mut store_update.
impl DerefMut for StoreUpdateHolder<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            StoreUpdateHolder::Reference(store_update) => store_update,
            StoreUpdateHolder::Owned(store_update) => store_update,
        }
    }
}

// Static instances of StoreUpdateHolder always hold an owned StoreUpdate instance.
// In such case it should be possible to convert it to StoreUpdate.
impl Into<StoreUpdate> for StoreUpdateHolder<'static> {
    fn into(self) -> StoreUpdate {
        match self {
            StoreUpdateHolder::Reference(_) => panic!("converting borrowed store update"),
            StoreUpdateHolder::Owned(store_update) => store_update,
        }
    }
}

/// Simple adapter wrapper on top of Store to provide a more ergonomic interface for different store types.
/// We provide simple inter-convertibility between different store types like FlatStoreAdapter and TrieStoreAdapter.
pub trait StoreAdapter {
    fn store(&self) -> Store;

    fn flat_store(&self) -> flat_store::FlatStoreAdapter {
        flat_store::FlatStoreAdapter::new(self.store())
    }
}

/// Simple adapter wrapper on top of StoreUpdate to provide a more ergonomic interface for
/// different store update types.
/// We provide simple inter-convertibility between different store update types like FlatStoreUpdateAdapter
/// and TrieStoreUpdateAdapter, however these are conversions by reference only.
/// The underlying StoreUpdate instance remains the same.
pub trait StoreUpdateAdapter: Sized {
    fn store_update(&mut self) -> &mut StoreUpdate;

    fn flat_store_update(&mut self) -> flat_store::FlatStoreUpdateAdapter {
        flat_store::FlatStoreUpdateAdapter::new(self.store_update())
    }
}
