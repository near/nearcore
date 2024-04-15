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

use near_o11y::metrics::prometheus;
use near_o11y::metrics::prometheus::core::GenericCounter;
use near_primitives::receipt::{Receipt, ReceiptEnum};
use near_primitives::transaction::{Action, SignedTransaction};
use near_primitives::trie_key::TrieKey;
use near_primitives::types::AccountId;
use near_primitives::types::StateRoot;
use near_store::{PrefetchApi, PrefetchError, Trie};
use sha2::Digest;
use std::str::FromStr;
use tracing::{debug, warn};

use crate::metrics;
/// Transaction runtime view of the prefetching subsystem.
pub(crate) struct TriePrefetcher {
    prefetch_api: PrefetchApi,
    trie_root: StateRoot,
    prefetch_enqueued: GenericCounter<prometheus::core::AtomicU64>,
    prefetch_queue_full: GenericCounter<prometheus::core::AtomicU64>,
}

impl TriePrefetcher {
    pub(crate) fn new_if_enabled(trie: &Trie) -> Option<Self> {
        let Some(caching_storage) = trie.internal_get_storage_as_caching_storage() else {
            return None;
        };
        let Some(prefetch_api) = caching_storage.prefetch_api().clone() else {
            return None;
        };
        let trie_root = *trie.get_root();
        let shard_uid = prefetch_api.shard_uid;
        let metrics_labels: [&str; 1] = [&shard_uid.shard_id.to_string()];
        Some(Self {
            prefetch_api,
            trie_root,
            prefetch_enqueued: metrics::PREFETCH_ENQUEUED.with_label_values(&metrics_labels),
            prefetch_queue_full: metrics::PREFETCH_QUEUE_FULL.with_label_values(&metrics_labels),
        })
    }

    /// Starts prefetching data for processing the receipts.
    ///
    /// Returns an error if prefetching for any receipt fails.
    /// The function is not idempotent; in case of failure, prefetching
    /// for some receipts may have been initiated.
    pub(crate) fn prefetch_receipts_data(
        &mut self,
        receipts: &[Receipt],
    ) -> Result<(), PrefetchError> {
        for receipt in receipts.iter() {
            let is_refund = receipt.predecessor_id.is_system();
            let action_receipt = match &receipt.receipt {
                ReceiptEnum::Action(action_receipt) | ReceiptEnum::PromiseYield(action_receipt) => {
                    action_receipt
                }
                ReceiptEnum::Data(_) | ReceiptEnum::PromiseResume(_) => {
                    continue;
                }
            };
            let account_id = receipt.receiver_id.clone();

            // general-purpose account prefetching
            if self.prefetch_api.enable_receipt_prefetching {
                let trie_key = TrieKey::Account { account_id: account_id.clone() };
                self.prefetch_trie_key(trie_key)?;
                if is_refund {
                    let trie_key = TrieKey::AccessKey {
                        account_id: account_id.clone(),
                        public_key: action_receipt.signer_public_key.clone(),
                    };
                    self.prefetch_trie_key(trie_key)?;
                }
                for action in &action_receipt.actions {
                    match action {
                        Action::Delegate(delegate_action) => {
                            let trie_key = TrieKey::AccessKey {
                                account_id: delegate_action.delegate_action.sender_id.clone(),
                                public_key: delegate_action.delegate_action.public_key.clone(),
                            };
                            self.prefetch_trie_key(trie_key)?;
                        }
                        Action::AddKey(add_key_action) => {
                            let trie_key = TrieKey::AccessKey {
                                account_id: account_id.clone(),
                                public_key: add_key_action.public_key.clone(),
                            };
                            self.prefetch_trie_key(trie_key)?;
                        }
                        Action::DeleteKey(delete_key_action) => {
                            let trie_key = TrieKey::AccessKey {
                                account_id: account_id.clone(),
                                public_key: delete_key_action.public_key.clone(),
                            };
                            self.prefetch_trie_key(trie_key)?;
                        }
                        _ => {}
                    }
                }
            }

            for action in &action_receipt.actions {
                let Action::FunctionCall(fn_call) = action else {
                    continue;
                };
                if self.prefetch_api.sweat_prefetch_receivers.contains(&account_id)
                    && self.prefetch_api.sweat_prefetch_senders.contains(&receipt.predecessor_id)
                {
                    if fn_call.method_name == "record_batch" {
                        self.prefetch_sweat_record_batch(account_id.clone(), &fn_call.args)?;
                    }
                }

                if self.prefetch_api.claim_sweat_prefetch_config.iter().any(|cfg| {
                    cfg.sender == receipt.predecessor_id.as_str()
                        && cfg.receiver == account_id.as_str()
                        && cfg.method_name == fn_call.method_name
                }) {
                    self.prefetch_claim_sweat(account_id.clone(), &fn_call.args)?;
                }

                if self.prefetch_api.kaiching_prefetch_config.iter().any(|cfg| {
                    cfg.sender == receipt.predecessor_id.as_str()
                        && cfg.receiver == account_id.as_str()
                        && cfg.method_name == fn_call.method_name
                }) {
                    self.prefetch_kaiching(account_id.clone(), &fn_call.args)?;
                }
            }
        }
        Ok(())
    }

    /// Starts prefetching data for processing the transactions.
    ///
    /// Returns an error if prefetching for any transaction fails.
    /// The function is not idempotent; in case of failure, prefetching
    /// for some transactions may have been initiated.
    pub(crate) fn prefetch_transactions_data(
        &mut self,
        transactions: &[SignedTransaction],
    ) -> Result<(), PrefetchError> {
        if self.prefetch_api.enable_receipt_prefetching {
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
        }
        Ok(())
    }

    /// Removes all queued up prefetch requests and staged data.
    ///
    /// Note that IO threads currently prefetching a trie key might insert
    /// additional trie nodes afterwards. Therefore the staging area is not
    /// reliably empty after resetting.
    /// This is okay-ish. Resetting between processed chunks does not have to
    /// be perfect, as long as we remove each unclaimed value eventually. Doing
    /// it one chunk later is also okay.
    ///
    /// TODO: In the presence of forks, multiple chunks of a shard can be processed
    /// at the same time. They share a prefetcher, so they will clean each others
    /// data. Handling this is a bit more involved. Failing to do so makes prefetching
    /// less effective in those cases but crucially nothing breaks.
    ///
    /// Returns the number of prefetch requests that have been removed from the prefetch queue.
    /// If this number is large, the prefetches aren't actually getting executed before cancelling.
    pub(crate) fn clear(&self) -> usize {
        let ret = self.prefetch_api.clear_queue();
        self.prefetch_api.clear_data();
        ret
    }

    fn prefetch_trie_key(&self, trie_key: TrieKey) -> Result<(), PrefetchError> {
        let res = self.prefetch_api.prefetch_trie_key(self.trie_root, trie_key);
        match res {
            Err(PrefetchError::QueueFull) => {
                self.prefetch_queue_full.inc();
                debug!(target: "prefetcher", "I/O scheduler input queue is full, dropping prefetch request");
            }
            Err(PrefetchError::QueueDisconnected) => {
                // This shouldn't have happened, hence logging warning here
                warn!(target: "prefetcher", "I/O scheduler input queue is disconnected, dropping prefetch request");
            }
            Ok(()) => self.prefetch_enqueued.inc(),
        };
        res
    }

    /// Prefetcher tuned for SWEAT contract calls of method steps_batch.
    ///
    /// Remove after #10965 reaches mainnet.
    fn prefetch_sweat_record_batch(
        &self,
        account_id: AccountId,
        arg: &[u8],
    ) -> Result<(), PrefetchError> {
        let Ok(json) = serde_json::de::from_slice::<serde_json::Value>(arg) else {
            return Ok(());
        };
        let Some(list) = &json.get("steps_batch") else {
            return Ok(());
        };
        let Some(list) = list.as_array() else {
            return Ok(());
        };

        for tuple in list.iter() {
            let Some(tuple) = tuple.as_array() else {
                continue;
            };
            let Some(user_account) = tuple.first().and_then(|a| a.as_str()) else {
                continue;
            };
            let hashed_account = sha2::Sha256::digest(user_account.as_bytes()).into_iter();
            // This is a "t" string used as the unique prefix of underlying
            // data structure terminated by a null value.
            let mut key = vec![0x74, 0x00];
            key.extend(hashed_account);
            let trie_key =
                TrieKey::ContractData { account_id: account_id.clone(), key: key.to_vec() };
            near_o11y::io_trace!(count: "prefetch");
            self.prefetch_trie_key(trie_key)?;
        }
        Ok(())
    }

    /// Prefetcher tuned for claim.sweat contract calls.
    ///
    /// Remove after #10965 reaches mainnet.
    fn prefetch_claim_sweat(&self, account_id: AccountId, arg: &[u8]) -> Result<(), PrefetchError> {
        let Ok(json) = serde_json::de::from_slice::<serde_json::Value>(arg) else {
            return Ok(());
        };
        let Some(list) = json.get("amounts") else {
            return Ok(());
        };
        let Some(list) = list.as_array() else {
            return Ok(());
        };
        for tuple in list.iter() {
            let Some(tuple) = tuple.as_array() else {
                continue;
            };
            let Some(user_account) = tuple.first().and_then(|a| a.as_str()) else {
                continue;
            };
            // Unique prefix of underlying data structure.
            let mut key = vec![0, 64, 0, 0, 0];
            key.extend(user_account.as_bytes());
            let trie_key = TrieKey::ContractData { account_id: account_id.clone(), key };
            near_o11y::io_trace!(count: "prefetch");
            self.prefetch_trie_key(trie_key)?;
        }
        Ok(())
    }

    /// Prefetcher tuned for kaiching contract calls.
    ///
    /// Remove after #10965 reaches mainnet.
    fn prefetch_kaiching(&self, account_id: AccountId, arg: &[u8]) -> Result<(), PrefetchError> {
        let Ok(json) = serde_json::de::from_slice::<serde_json::Value>(&arg) else {
            return Ok(());
        };
        let Some(msg) = json.get("msg") else {
            return Ok(());
        };
        let Some(json) = msg
            .as_str()
            .and_then(|s| serde_json::de::from_slice::<serde_json::Value>(s.as_bytes()).ok())
        else {
            return Ok(());
        };
        let Some(list) = json.get("rewards") else {
            return Ok(());
        };
        let Some(list) = list.as_array() else {
            return Ok(());
        };

        for tuple in list.iter() {
            let Some(tuple) = tuple.as_array() else {
                continue;
            };
            // Unique prefix of underlying data structure.
            let mut user_account_key = vec![1, 109];
            let user_account_serialize_result = tuple
                .get(0)
                .and_then(|a| a.as_str())
                .and_then(|a| AccountId::from_str(a).ok())
                .and_then(|a| borsh::BorshSerialize::serialize(&a, &mut user_account_key).ok());
            if user_account_serialize_result.is_none() {
                continue;
            }
            let reward_id = tuple.get(2).and_then(|a| a.as_str());
            let Some(reward_id) = reward_id else {
                continue;
            };
            let user_account_key_hash = sha2::Sha256::digest(&user_account_key);
            let trie_key = TrieKey::ContractData {
                account_id: account_id.clone(),
                key: user_account_key_hash.to_vec(),
            };
            near_o11y::io_trace!(count: "prefetch");
            self.prefetch_trie_key(trie_key)?;

            // Unique prefix of underlying data structure.
            let mut reward_key = vec![0, 24, 0, 0, 0];
            reward_key.extend(reward_id.as_bytes());
            let trie_key =
                TrieKey::ContractData { account_id: account_id.clone(), key: reward_key };
            near_o11y::io_trace!(count: "prefetch");
            self.prefetch_trie_key(trie_key)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::TriePrefetcher;
    use near_primitives::{trie_key::TrieKey, types::AccountId};
    use near_store::test_utils::{create_test_store, test_populate_trie};
    use near_store::{ShardTries, ShardUId, StateSnapshotConfig, Trie, TrieConfig};
    use std::str::FromStr;
    use std::time::{Duration, Instant};

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

        let shard_uids = vec![ShardUId::single_shard()];
        let trie_config = TrieConfig { enable_receipt_prefetching: true, ..TrieConfig::default() };
        let store = create_test_store();
        let flat_storage_manager = near_store::flat::FlatStorageManager::new(store.clone());
        let tries = ShardTries::new(
            store,
            trie_config,
            &shard_uids,
            flat_storage_manager,
            StateSnapshotConfig::default(),
        );

        let mut kvs = vec![];

        // insert different values for each account to ensure they have unique hashes
        for (i, trie_key) in input_keys.iter().enumerate() {
            let storage_key = trie_key.to_vec();
            kvs.push((storage_key.clone(), Some(i.to_string().as_bytes().to_vec())));
        }
        let root = test_populate_trie(&tries, &Trie::EMPTY_ROOT, ShardUId::single_shard(), kvs);
        let trie = tries.get_trie_for_shard(ShardUId::single_shard(), root);
        trie.internal_get_storage_as_caching_storage().unwrap().clear_cache();

        let prefetcher =
            TriePrefetcher::new_if_enabled(&trie).expect("caching storage should have prefetcher");
        let prefetch_api = &prefetcher.prefetch_api;

        assert_eq!(prefetch_api.num_prefetched_and_staged(), 0);

        for trie_key in &prefetch_keys {
            _ = prefetcher.prefetch_trie_key(trie_key.clone());
        }
        std::thread::yield_now();

        let wait_work_queue_empty_start = Instant::now();
        while prefetch_api.work_queued() {
            std::thread::yield_now();
            // Use timeout to avoid hanging the test
            assert!(
                wait_work_queue_empty_start.elapsed() < Duration::from_millis(100),
                "timeout while waiting for prefetch queue to become empty"
            );
        }

        // The queue is empty now but there can still be requests in progress.
        // Looking at `Pending` slots in the prefetching area is not sufficient
        // here because threads can be in `Trie::lookup` between nodes, at which
        // point no slot is reserved for them but they are still doing work.
        //
        // Solution: `drop(tries)`, which also drops prefetchers. In particular,
        // we want to drop the `PrefetchingThreadsHandle` stored in tries.
        // `drop(tries)` causes all background threads to stop after they finish
        // the current work. It will even join them and wait until all threads
        // are done.
        //
        // Because threads are joined, there is also a possibility this will
        // hang forever. To avoid that, we drop in a separate thread.
        let dropped = std::sync::atomic::AtomicBool::new(false);
        std::thread::scope(|s| {
            s.spawn(|| {
                drop(tries);
                dropped.store(true, std::sync::atomic::Ordering::Release);
            });
            let spawned = Instant::now();
            while !dropped.load(std::sync::atomic::Ordering::Acquire) {
                std::thread::yield_now();
                // 100ms should be enough to finish pending requests. If not,
                // we should check how the prefetcher affects performance.
                assert!(
                    spawned.elapsed() < Duration::from_millis(100),
                    "timeout while waiting for background threads to terminate"
                );
            }
        });

        assert_eq!(
            prefetch_api.num_prefetched_and_staged(),
            expected_prefetched,
            "unexpected number of prefetched values"
        );

        // Read all prefetched values to ensure everything gets removed from the staging area.
        for trie_key in &prefetch_keys {
            let storage_key = trie_key.to_vec();
            let _value = trie.get(&storage_key).unwrap();
        }
        assert_eq!(
            prefetch_api.num_prefetched_and_staged(),
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
