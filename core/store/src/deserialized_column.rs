use crate::DBCol;
use quick_cache::sync::Cache as QuickCache;
use std::any::Any;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

pub(super) struct PutTicket<'a>(&'a ColumnCache);

impl<'a> Drop for PutTicket<'a> {
    fn drop(&mut self) {
        self.0.state.fetch_sub(ColumnCache::ONE_PUT_TICKET, Ordering::Release);
    }
}

pub(super) struct ColumnCache {
    pub(super) values: QuickCache<Vec<u8>, Arc<dyn Any + Send + Sync>>,
    /// A bitmask used for synchronization of the flush operations in this cache.
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
    /// This is solved with this state field which is built out out of the following bits:
    ///
    /// * 32 bits to indicate the number of ongoing insertions;
    /// * 32 high bits to indicate the number of ongoing flushes.
    ///
    /// During a get operation a value can only be inserted in the cache if all high 32 bits are
    /// zero. This means that no write operation is currently occurring and there is no risk for
    /// non-coherent access. If any of the bits are non-zero, the value is not placed in the cache
    /// and returned to the caller immediately.
    ///
    /// During a database write some of the high 32 bits will be set. At the same time the code
    /// will wait for the low 32-bits to clear to 0 to indicate it can now make progress with
    /// the eviction.
    pub(super) state: AtomicU64,
}

impl ColumnCache {
    pub(super) const ONE_WRITE_TICKET: u64 = 1 << 32;
    pub(super) const WRITE_TICKET_MASK: u64 = 0xFFFF_FFFF_0000_0000;
    pub(super) const ONE_PUT_TICKET: u64 = 1;

    pub(super) fn try_acquire_put_ticket(&self) -> Result<PutTicket, ()> {
        let result = self.state.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            if (current & Self::WRITE_TICKET_MASK) == 0 {
                Some(current + Self::ONE_PUT_TICKET)
            } else {
                None
            }
        });
        match result {
            Ok(_) => Ok(PutTicket(self)),
            Err(_) => Err(()),
        }
    }

    pub(super) fn acquire_write_ticket(&self) {
        self.state.fetch_add(Self::ONE_WRITE_TICKET, Ordering::Relaxed);
    }

    pub(super) fn release_write_ticket(&self) {
        self.state.fetch_sub(Self::ONE_WRITE_TICKET, Ordering::Release);
    }

    pub(super) fn wait_write_tickets_only(&self) {
        // This is an optimistic guess as to what the value of the state might
        // be if there is no contention whatsoever.
        let mut current_state = Self::ONE_WRITE_TICKET;
        loop {
            // This compare exchange serves two purposes -- first, it makes sure there
            // are no PUT tickets active by comparing against 0x????_????_0000_0000
            // (loop continues on while there is any, waiting for the put ticket to
            // be released.)
            let new = current_state & Self::WRITE_TICKET_MASK;
            let Err(new_current_state) = self.state.compare_exchange(
                current_state,
                new,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) else {
                return;
            };
            current_state = new_current_state;
        }
    }

    fn disabled() -> Option<Self> {
        Self::new(0)
    }

    fn new(capacity: usize) -> Option<Self> {
        let capacity = NonZeroUsize::new(capacity)?;
        let values = QuickCache::new(capacity.get());
        Some(Self { values, state: AtomicU64::new(0) })
    }
}

pub struct Cache {
    column_map: enum_map::EnumMap<DBCol, Option<ColumnCache>>,
}

impl Cache {
    pub(crate) fn enabled() -> Self {
        Self {
            column_map: enum_map::enum_map! {
                | DBCol::BlockHeader
                | DBCol::BlockHeight
                | DBCol::BlockMisc => ColumnCache::new(512),
                // Block cache is mostly beneficial for the RPC node which needs to check the block
                // for each transaction it receives.
                //
                // The cache isn't particularly large – it is expected that the block referenced in
                // the transaction isn't going to be very old most of the time.
                | DBCol::Block => ColumnCache::new(32),
                | DBCol::ChunkExtra => ColumnCache::new(1024),
                | DBCol::PartialChunks => ColumnCache::new(512),
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

    pub(super) fn work_with(&self, col: DBCol) -> Option<&ColumnCache> {
        self.column_map[col].as_ref()
    }
}
