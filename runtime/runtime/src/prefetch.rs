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
//! The design goal is therefore that the prefetcher affects the performance of
//! the main thread as little as possible. This is partially accomplished by
//! treating the main thread's caches as read-only. All prefetched data is
//! inserted in a separate prefetcher landing area. The main thread can check
//! that space and move it to its own LRU caches if it finds the data there.
//! But the prefetcher never directly modifies the main thread's caches.
//! Reading from the LRU cache only makes some values stay longer in the cache.
//! Presumably this is only positive, as the prefetcher knows the value will be
//! used again.
//!
//! Another important measure is to limit the prefetcher in how far ahead it
//! should prefetch, how much bandwidth it consumes, and how much memory it
//! uses. We achieve this with a bounded queue for incoming requests, limiting
//! the number of IO threads, and memory checks before staring new DB requests
//! in the prefetcher. Implementation details for most limits are in
//! `core/store/src/trie/prefetching_trie_storage.rs`

use near_primitives::receipt::{Receipt, ReceiptEnum};
use near_primitives::transaction::SignedTransaction;
use near_primitives::trie_key::TrieKey;
use near_store::{PrefetchApi, Trie};
use std::rc::Rc;
use tracing::debug;

/// How many threads will be prefetching data, without the scheduler thread.
/// Because the storage driver is blocking, there is only one request per thread
/// at a time.
const NUM_IO_THREADS: usize = 8;

/// Transaction runtime view of the prefetching subsystem.
pub(crate) struct TriePrefetcher {
    prefetch_api: PrefetchApi,
    #[cfg(test)]
    handles: Vec<std::thread::JoinHandle<()>>,
}

impl TriePrefetcher {
    pub(crate) fn new(trie: Rc<Trie>) -> Option<Self> {
        if let Some(caching_storage) = trie.storage.as_caching_storage() {
            let trie_root = trie.get_root().clone();
            let parent = caching_storage.clone();
            let prefetch_api = PrefetchApi::new(&parent);
            #[cfg(test)]
            let mut handles = vec![];
            for _ in 0..NUM_IO_THREADS {
                let _handle = prefetch_api.start_io_thread(parent, trie_root);
                #[cfg(test)]
                handles.push(_handle);
            }
            Some(Self {
                prefetch_api,
                #[cfg(test)]
                handles,
            })
        } else {
            None
        }
    }

    /// Start prefetching data for processing the receipts.
    ///
    /// Returns an error if the prefetching queue is full.
    pub(crate) fn input_receipts(&mut self, receipts: &[Receipt]) -> Result<(), ()> {
        for receipt in receipts.iter() {
            if let ReceiptEnum::Action(_action_receipt) = &receipt.receipt {
                let account_id = receipt.receiver_id.clone();
                let trie_key = TrieKey::Account { account_id };
                self.prefetch_trie_key(trie_key)?;
            }
        }
        Ok(())
    }

    /// Start prefetching data for processing the transactions.
    ///
    /// Returns an error if the prefetching queue is full.
    pub(crate) fn input_transactions(
        &mut self,
        transactions: &[SignedTransaction],
    ) -> Result<(), ()> {
        for t in transactions {
            let account_id = t.transaction.signer_id.clone();
            let trie_key = TrieKey::Account { account_id };
            self.prefetch_trie_key(trie_key)?;

            let trie_key = TrieKey::AccessKey {
                account_id: t.transaction.signer_id.clone(),
                public_key: t.transaction.public_key.clone(),
            };
            self.prefetch_trie_key(trie_key)?;
        }
        Ok(())
    }

    /// Removes all queue up prefetch requests.
    pub(crate) fn clear(&self) {
        self.prefetch_api.clear();
    }

    /// Stops IO threads after they finish their current task.
    pub(crate) fn stop_prefetching(&self) {
        self.prefetch_api.stop();
    }

    fn prefetch_trie_key(&self, trie_key: TrieKey) -> Result<(), ()> {
        let queue_full = self.prefetch_api.prefetch_trie_key(trie_key).is_err();
        if queue_full {
            debug!(target: "prefetcher", "I/O scheduler input queue full, dropping prefetch request");
            Err(())
        } else {
            Ok(())
        }
    }
}

impl Drop for TriePrefetcher {
    fn drop(&mut self) {
        self.prefetch_api.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::TriePrefetcher;
    use near_primitives::{trie_key::TrieKey, types::AccountId};
    use near_store::{
        test_utils::{create_tries, test_populate_trie},
        ShardUId, Trie,
    };
    use std::{rc::Rc, str::FromStr, time::Duration};

    #[test]
    fn test_basic_prefetch_account() {
        let accounts = ["alice.near"];
        // One account <=> a root value and a value.
        let expected_prefetched = 2;
        check_prefetch_account(&accounts, &accounts, expected_prefetched);
    }

    #[test]
    fn test_prefetch_multiple_accounts() {
        let accounts = [
            "000.alice.near",
            "111.alice.near",
            "222.alice.near",
            "333.alice.near",
            "000.bob.near",
            "111.bob.near",
            "222.bob.near",
            "333.bob.near",
        ];
        // root is an extension with the prefix for accounts
        // that extension leads to a branch with four extensions ("000.","111.","222.","333.")
        // each extension leads to a branch with two leafs ("alice.near", "bob.near")
        //
        //                           root
        //                           extension
        //                           |
        //                           |
        //                           branch
        //     "0"-------------"1"-------------"2"-------------"3"
        //      |               |               |               |
        //      |               |               |               |
        //      extension       extension       extension       extension
        //      "00."           "11."           "22."           "33."
        //      |               |               |               |
        //      |               |               |               |
        //      branch          branch          branch          branch
        //  "a"--------"b"  "a"--------"b"  "a"--------"b"  "a"--------"b"
        //   |          |    |          |   |           |    |          |
        //   |          |    |          |   |           |    |          |
        //   |          |    |          |   |           |    |          |
        // "lice.near"  |  "lice.near"  |  "lice.near"  | "lice.near"   |
        //              |               |               |               |
        //          "ob.near"       "ob.near"       "ob.near"       "ob.near"
        //
        //
        // Note: drawing does not show values. Also, upper nibble is always equal
        // on branches, so we can just assume bytes instead of nibbles.

        // prefetching a single node results in 2 extensions + 2 branches + 1 leaf + 1 value
        let prefetch_accounts = &accounts[..1];
        let expected_prefetched = 6;
        check_prefetch_account(&accounts, prefetch_accounts, expected_prefetched);

        // prefetching two distant nodes results in 3 extensions + 3 branches + 2 leafs + 2 values
        let prefetch_accounts = &accounts[..2];
        let expected_prefetched = 10;
        check_prefetch_account(&accounts, prefetch_accounts, expected_prefetched);

        // prefetching two neighboring nodes results in 2 extensions + 2 branches + 2 leafs + 2 values
        let prefetch_accounts = &["000.alice.near", "000.bob.near"];
        let expected_prefetched = 8;
        check_prefetch_account(&accounts, prefetch_accounts, expected_prefetched);
    }

    #[test]
    fn test_prefetch_non_existing_account() {
        let existing_accounts = ["alice.near", "bob.near"];
        let non_existing_account = ["charlotta.near"];
        // Most importantly, it should not crash.
        // Secondly, it should prefetch the root extension + the first branch.
        let expected_prefetched = 2;
        check_prefetch_account(&existing_accounts, &non_existing_account, expected_prefetched);
    }

    #[track_caller]
    fn check_prefetch_account(input: &[&str], prefetch: &[&str], expected_prefetched: usize) {
        let input_keys = accounts_to_trie_keys(input);
        let prefetch_keys = accounts_to_trie_keys(prefetch);

        let tries = create_tries();
        let mut kvs = vec![];

        // insert different values for each account to ensure they have unique hashes
        for (i, trie_key) in input_keys.iter().enumerate() {
            let storage_key = trie_key.to_vec();
            kvs.push((storage_key.clone(), Some(i.to_string().as_bytes().to_vec())));
        }
        let root = test_populate_trie(&tries, &Trie::EMPTY_ROOT, ShardUId::single_shard(), kvs);

        let trie = Rc::new(tries.get_trie_for_shard(ShardUId::single_shard(), root.clone()));
        trie.storage.as_caching_storage().unwrap().clear_cache();

        let mut prefetcher = TriePrefetcher::new(trie.clone()).unwrap();
        let p = &prefetcher.prefetch_api;

        assert_eq!(p.num_prefetched_and_staged(), 0);

        for trie_key in &prefetch_keys {
            _ = prefetcher.prefetch_trie_key(trie_key.clone());
        }
        std::thread::yield_now();

        // don't use infinite loop to avoid test from ever hanging
        for _ in 0..1000 {
            if !p.work_queued() {
                break;
            }
            std::thread::sleep(Duration::from_micros(100));
        }

        // Request can still be pending. Stop threads and wait for them to finish.
        p.stop();
        for h in prefetcher.handles.drain(..) {
            h.join().unwrap();
        }

        assert_eq!(
            p.num_prefetched_and_staged(),
            expected_prefetched,
            "unexpected number of prefetched values"
        );

        // Read all prefetched values to ensure everything gets removed from the staging area.
        for trie_key in &prefetch_keys {
            let storage_key = trie_key.to_vec();
            let _value = trie.get(&storage_key).unwrap();
        }
        assert_eq!(
            p.num_prefetched_and_staged(),
            0,
            "prefetching staging area not clear after reading all values from main thread"
        );
    }

    #[track_caller]
    fn accounts_to_trie_keys(input: &[&str]) -> Vec<TrieKey> {
        input
            .iter()
            .map(|s| {
                let account_id = AccountId::from_str(s).expect("invalid input account id");
                TrieKey::Account { account_id }
            })
            .collect()
    }
}
