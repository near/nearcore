//! Prefetcher for workload within a block.
//!
//! Prefetching data from the DB ahead of time, in background threads, can
//! reduce I/O overhead to almost zero. To enable that, the prefetcher gets to
//! see all receipts to be applied in a block, before the actions start
//! executing. Based on a receipt's metadata, it may be possible to predict what
//! data it will be accessing.
//!
//! There are two types of prefetching, predictive and non-predictive.
//! Predictive strategies rely on heuristics and are not guaranteed to always
//! fetch useful data. Non-predictive strategies on the contrary only fetch data
//! that is guaranteed to be needed.
//!
//! Predictive prefetching should be used with caution in blockchain context.
//! For one, it does not improve the worst-case. Furthermore, an adversary user
//! can look at the prefetcher's heuristics and force it to mispredict. This
//! means a client is doing (useless) work that is not covered by gas fees and
//! may negatively affect the client's performance.
//!
//! Non-predictive prefetching avoids the problems listed above. But bad
//! implementations could still negatively impact performance in corner cases.
//! For example, if a prefetcher looks too far ahead, it may end up evicting
//! data from LRU caches that is still required. A prefetcher could also use too
//! much of the available I/O bandwidth and therefore slow down the main
//! thread's I/O requests.
//!
//! The design goal is therefore to avoid the prefetcher from affecting the
//! performance of the main thread as much as possible. This is partially
//! accomplished by treating the main thread's caches as read-only. All
//! prefetched data is inserted in a separate prefetcher landing area. The main
//! thread can check that space and move it to its own LRU caches if it find the
//! data there. But the prefetcher never directly accesses the main thread's
//! caches.
//!
//! Another important measure is to limit the prefetcher in how far ahead it
//! should prefetch, how much bandwidth it consumes, and how much memory it
//! uses. We achieve this with a prefetch scheduler and checks for memory limits
//! right before each DB request.

use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{Receipt, ReceiptEnum};
use near_primitives::transaction::SignedTransaction;
use near_primitives::trie_key::TrieKey;
use near_store::{IoRequestQueue, IoThreadCmd, Trie, TrieCachingStorage, TriePrefetchingStorage};
use std::rc::Rc;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::Arc;
use tracing::debug;

/// How many threads will be prefetching data, without the scheduler thread.
/// Because the storage driver is blocking, there is only one request per thread
/// at the time.
const NUM_IO_THREADS: usize = 8;
/// A chunk must be fully processed in one second. If we send N prefetch requests
/// in a chunk, that means we spend at least N IOPS on prefetching. More than
/// that, even, because each prefetch request usually has a series of DB
/// requests.
/// A local NVME SSD should be able ro reach 100k IOPS. Assuming an average trie
/// depth of 10, this leads to 10k requests.
const MAX_PREFETCH_REQUESTS_PER_CHUNK: usize = 10_000;
/// How many requests to the prefetching scheduler can be queued up. Note that
/// because all requests are sent at the start of chunk processing, it means that
/// the queue will fill up quickly. Therefore setting it at the same value as
/// MAX_PREFETCH_REQUESTS_PER_CHUNK for now. Set it to lower values to limit
/// memory usage.
const SCHEDULER_MAX_BACK_PRESSURE: usize = 10_000;

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

pub(crate) struct TriePrefetcher {
    trie: Rc<Trie>,
    scheduler_tx: SyncSender<PrefetchSchedulerCmd>,
    requests_limit: usize,
}
struct PrefetchScheduler {
    rx: Receiver<PrefetchSchedulerCmd>,
    request_queue: IoRequestQueue,
}

enum PrefetchSchedulerCmd {
    PrefetchTrieNode(TrieKey),
    EndOfInput,
}

impl PrefetchScheduler {
    fn new(
        rx: Receiver<PrefetchSchedulerCmd>,
        root: CryptoHash,
        prefetcher_storage: TriePrefetchingStorage,
        kill_switch: Arc<AtomicBool>,
    ) -> Self {
        let request_queue = Arc::default();
        for _ in 0..NUM_IO_THREADS {
            let _handle = TrieCachingStorage::start_io_thread(
                root.clone(),
                Box::new(prefetcher_storage.clone()),
                Arc::clone(&request_queue),
                kill_switch.clone(),
            );
        }

        Self { rx, request_queue }
    }

    // Starts a scheduler thread and returns a Sender that accepts prefetch requests.
    fn start(
        root: CryptoHash,
        prefetcher_storage: TriePrefetchingStorage,
        kill_switch: Arc<AtomicBool>,
    ) -> (std::thread::JoinHandle<()>, SyncSender<PrefetchSchedulerCmd>) {
        let (tx, rx) =
            std::sync::mpsc::sync_channel::<PrefetchSchedulerCmd>(SCHEDULER_MAX_BACK_PRESSURE);
        let _handle = std::thread::spawn(move || {
            let scheduler = Self::new(rx, root, prefetcher_storage, kill_switch);

            while let Ok(req) = scheduler.rx.recv() {
                match req {
                    PrefetchSchedulerCmd::PrefetchTrieNode(trie_key) => scheduler
                        .request_queue
                        .lock()
                        .expect(POISONED_LOCK_ERR)
                        .push_back(IoThreadCmd::PrefetchTrieNode(trie_key)),
                    PrefetchSchedulerCmd::EndOfInput => {
                        break;
                    }
                }
            }

            for _ in 0..NUM_IO_THREADS {
                scheduler
                    .request_queue
                    .lock()
                    .expect(POISONED_LOCK_ERR)
                    .push_back(IoThreadCmd::StopSelf);
            }
        });
        (_handle, tx)
    }
}

impl TriePrefetcher {
    pub(crate) fn new(trie: Rc<Trie>) -> Option<Self> {
        if let Some(caching_storage) = trie.storage.as_caching_storage() {
            let root = trie.get_root().clone();
            let prefetcher_storage = caching_storage.prefetcher_storage();
            let kill_switch = caching_storage.io_kill_switch().clone();

            let (_handle, scheduler_tx) =
                PrefetchScheduler::start(root, prefetcher_storage, kill_switch);
            Some(Self { trie, scheduler_tx, requests_limit: MAX_PREFETCH_REQUESTS_PER_CHUNK })
        } else {
            None
        }
    }

    pub(crate) fn input_receipts(&mut self, receipts: &[Receipt]) {
        for receipt in receipts.iter() {
            if let ReceiptEnum::Action(_action_receipt) = &receipt.receipt {
                let account_id = receipt.receiver_id.clone();
                let trie_key = TrieKey::Account { account_id };
                self.prefetch(PrefetchSchedulerCmd::PrefetchTrieNode(trie_key));
            }
            if self.requests_limit == 0 {
                return;
            }
        }
    }

    pub(crate) fn input_transactions(&mut self, transactions: &[SignedTransaction]) {
        for t in transactions {
            let account_id = t.transaction.signer_id.clone();
            let trie_key = TrieKey::Account { account_id };
            self.prefetch(PrefetchSchedulerCmd::PrefetchTrieNode(trie_key));

            let trie_key = TrieKey::AccessKey {
                account_id: t.transaction.signer_id.clone(),
                public_key: t.transaction.public_key.clone(),
            };
            self.prefetch(PrefetchSchedulerCmd::PrefetchTrieNode(trie_key));
            if self.requests_limit == 0 {
                return;
            }
        }
    }

    /// Tell the prefetcher that no more input is coming, which allows stopping
    /// I/O threads once they are done.
    pub(crate) fn end_input(&self) {
        self.scheduler_tx.send(PrefetchSchedulerCmd::EndOfInput).unwrap();
    }

    pub(crate) fn stop_prefetching(&self) {
        if let Some(caching_storage) = self.trie.storage.as_caching_storage() {
            caching_storage.stop_prefetcher();
        }
    }

    fn prefetch(&mut self, cmd: PrefetchSchedulerCmd) {
        let res = self.scheduler_tx.try_send(cmd);
        if let Err(_err) = res {
            debug!(target: "prefetcher", "I/O scheduler input queue full, dropping prefetch request");
        } else {
            self.requests_limit = self.requests_limit.saturating_sub(1);
        }
    }
}
