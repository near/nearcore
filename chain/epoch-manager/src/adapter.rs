use crate::{EpochManager, EpochManagerHandle};
use std::sync::{RwLockReadGuard, RwLockWriteGuard};

/// A trait that abstracts the interface of the EpochManager.
///
/// It is intended to be an intermediate state in a refactor: we want to remove
/// epoch manager stuff from RuntimeAdapter's interface, and, as a first step,
/// we move it to a new trait. The end goal is for the code to use the concrete
/// epoch manager type directly. Though, we might want to still keep this trait
/// in, to allow for easy overriding of epoch manager in tests.
pub trait EpochManagerAdapter: Send + Sync {}

/// A technical plumbing trait to conveniently implement [`EpochManagerAdapter`]
/// for `NightshadeRuntime` without too much copy-paste.
///
/// Once we remove `RuntimeAdapter: EpochManagerAdapter` bound, we could get rid
/// of this trait and instead add inherent methods directly to
/// `EpochManagerHandle`.
pub trait HasEpochMangerHandle {
    fn write(&self) -> RwLockWriteGuard<EpochManager>;
    fn read(&self) -> RwLockReadGuard<EpochManager>;
}

impl HasEpochMangerHandle for EpochManagerHandle {
    fn write(&self) -> RwLockWriteGuard<EpochManager> {
        self.write()
    }
    fn read(&self) -> RwLockReadGuard<EpochManager> {
        self.read()
    }
}

impl<T: HasEpochMangerHandle + Send + Sync> EpochManagerAdapter for T {}
