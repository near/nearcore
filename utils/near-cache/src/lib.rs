#[cfg(not(windows))]
mod file;
mod sync;
mod weighted;

#[cfg(not(windows))]
pub use crate::file::LruFileCache;
pub use crate::sync::SyncLruCache;
pub use crate::weighted::LruWeightedCache;

use std::sync::Arc;

/// Sink for fire-and-forget background maintenance jobs.
pub type BackgroundJobSpawner = Arc<dyn Fn(Box<dyn FnOnce() + Send>) + Send + Sync>;

/// A [`BackgroundJobSpawner`] that sends every job to the void.
pub fn noop_background_spawner() -> BackgroundJobSpawner {
    Arc::new(|_job| {})
}
