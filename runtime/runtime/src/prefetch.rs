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
use near_store::{IoThreadCmd, Trie, TrieCachingStorage, TriePrefetchingStorage};
use std::rc::Rc;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;

pub(crate) struct TriePrefetcher {
    trie: Rc<Trie>,
    scheduler_tx: Sender<PrefetchSchedulerCmd>,
}
struct PrefetchScheduler {
    rx: Receiver<PrefetchSchedulerCmd>,
    io_threads: Vec<Sender<IoThreadCmd>>,
}

enum PrefetchSchedulerCmd {
    PrefetchTrieNode(TrieKey),
    EndOfInput,
}

impl TriePrefetcher {
    pub(crate) fn new(trie: Rc<Trie>) -> Option<Self> {
        if let Some(caching_storage) = trie.storage.as_caching_storage() {
            let root = trie.get_root().clone();
            let prefetcher_storage = caching_storage.prefetcher_storage();
            let kill_switch = caching_storage.io_kill_switch().clone();

            let (_handle, scheduler_tx) =
                PrefetchScheduler::start(root, prefetcher_storage, kill_switch);
            Some(Self { trie, scheduler_tx })
        } else {
            None
        }
    }

    pub(crate) fn input_receipts(&self, receipts: &[Receipt]) {
        for receipt in receipts.iter() {
            if let ReceiptEnum::Action(_action_receipt) = &receipt.receipt {
                self.prefetch_account(receipt.receiver_id.clone());
            }
        }
    }

    pub(crate) fn input_transactions(&self, transactions: &[SignedTransaction]) {
        for t in transactions {
            self.prefetch_account(t.transaction.signer_id.clone());
            let trie_key = TrieKey::AccessKey {
                account_id: t.transaction.signer_id.clone(),
                public_key: t.transaction.public_key.clone(),
            };
            let cmd = PrefetchSchedulerCmd::PrefetchTrieNode(trie_key);
            self.scheduler_tx.send(cmd).expect("TODO");
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

    fn prefetch_account(&self, account_id: near_primitives::types::AccountId) {
        let trie_key = TrieKey::Account { account_id };
        let cmd = PrefetchSchedulerCmd::PrefetchTrieNode(trie_key);
        self.scheduler_tx.send(cmd).expect("TODO");
    }
}

impl PrefetchScheduler {
    fn new(
        rx: Receiver<PrefetchSchedulerCmd>,
        root: CryptoHash,
        prefetcher_storage: TriePrefetchingStorage,
        kill_switch: Arc<AtomicBool>,
        num_io_threads: usize,
    ) -> Self {
        let io_threads = (0..num_io_threads)
            .map(|_| {
                let (_handle, tx) = TrieCachingStorage::start_io_thread(
                    root.clone(),
                    Box::new(prefetcher_storage.clone()),
                    kill_switch.clone(),
                );
                tx
            })
            .collect::<Vec<_>>();

        Self { rx, io_threads }
    }

    // Starts a scheduler thread and returns a Sender to it.
    fn start(
        root: CryptoHash,
        prefetcher_storage: TriePrefetchingStorage,
        kill_switch: Arc<AtomicBool>,
    ) -> (std::thread::JoinHandle<()>, Sender<PrefetchSchedulerCmd>) {
        let (tx, rx) = std::sync::mpsc::channel::<PrefetchSchedulerCmd>();
        let _handle = std::thread::spawn(move || {
            let num_io_threads = 8;
            let scheduler = Self::new(rx, root, prefetcher_storage, kill_switch, num_io_threads);
            let mut txs = scheduler.io_threads.iter().cycle();
            while let Ok(req) = scheduler.rx.recv() {
                match req {
                    PrefetchSchedulerCmd::PrefetchTrieNode(trie_key) => {
                        txs.next()
                            .unwrap()
                            .send(IoThreadCmd::PrefetchTrieNode(trie_key))
                            .expect("TODO");
                    }
                    PrefetchSchedulerCmd::EndOfInput => {
                        break;
                    }
                }
            }
            for tx in scheduler.io_threads {
                tx.send(IoThreadCmd::StopSelf).expect("TODO");
            }
        });
        (_handle, tx)
    }
}
