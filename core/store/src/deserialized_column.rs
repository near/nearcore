use crate::DBCol;
use parking_lot::Mutex;
use std::any::Any;
use std::num::NonZeroUsize;
use std::sync::Arc;

pub(super) struct ColumnCache {
    pub(super) values: lru::LruCache<Vec<u8>, Arc<dyn Any + Send + Sync>>,
    /// A counter indicating the number of ongoing write transaction flushes.
    ///
    /// This cache and transactional write operation in the underlying database can be
    /// difficult to synchronize, so instead any time there is an in-progress write operation
    /// that affects this column, we will simply disable the use of this column's cache
    /// altogether and have the read operations go to the underlying database unconditionally.
    ///
    /// This solves a race condition where we'd potentially serve inconsistent data from the
    /// cache between the moment when the transaction is written out and the modified keys are
    /// erased from the relevant columns in the cache.
    ///
    /// So for example, if we clear the cache contents before sending the transaction to the
    /// database, without any locking the following sequence of events is possible:
    ///
    /// 1. Erase all affected keys in the cache;
    /// 2. Code reads some of the affected keys and remembers it in the cache;
    /// 3. DB write transaction is applied;
    /// 4. From this point on cache still serves data it recalls from step 2.
    ///
    /// See the state machine constants for more information on how this is implemented.
    ///
    /// Instead, any time this counter is non-zero, the cache will stop saving values read out
    /// of database into itself and return them directly to the caller. The write operation
    /// itself will take care of discarding dirty data.
    pub(super) active_flushes: u64,
}

impl ColumnCache {
    fn disabled() -> Option<Mutex<Self>> {
        Self::new(0)
    }

    fn new(capacity: usize) -> Option<Mutex<Self>> {
        let capacity = NonZeroUsize::new(capacity);
        let values = capacity.map(|cap| lru::LruCache::new(cap))?;
        Some(Mutex::new(Self { values, active_flushes: 0 }))
    }
}

pub struct Cache {
    column_map: enum_map::EnumMap<DBCol, Option<Mutex<ColumnCache>>>,
}

impl Cache {
    pub(crate) fn enabled() -> Self {
        Self {
            column_map: enum_map::enum_map! {
                _ => ColumnCache::disabled(),
            },
        }
    }

    pub fn disabled() -> Arc<Self> {
        static ONCE: std::sync::OnceLock<Arc<Cache>> = std::sync::OnceLock::new();
        Arc::clone(ONCE.get_or_init(|| {
            Arc::new(Self {
                column_map: enum_map::enum_map! {
                    _ => ColumnCache::disabled(),
                },
            })
        }))
    }

    pub(super) fn work_with(&self, col: DBCol) -> Option<&Mutex<ColumnCache>> {
        self.column_map[col].as_ref()
    }
}
