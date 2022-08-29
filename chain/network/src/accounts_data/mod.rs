//! Cache of AccountData. It keeps AccountData for important accounts for the current epoch.
//! The set of important accounts for the given epoch is expected to never change (should be
//! deterministic). Note that "important accounts for the current epoch" is not limited to
//! "validators of the current epoch", but rather may include for example "validators of the next
//! epoch" so that AccountData of future validators is broadcasted in advance.
//!
//! Assumptions:
//! - verifying signatures is expensive, we need a dedicated threadpool for handling that.
//!   TODO(gprusak): it would be nice to have a benchmark for that
//! - a bad peer may attack by sending a lot of invalid signatures
//! - we can afford verifying each valid signature of the current epoch once.
//! - we can afford verifying a few invalid signatures per SyncAccountsData message.
//!
//! Strategy:
//! - handling of SyncAccountsData should be throttled by PeerActor/PeerManagerActor.
//! - synchronously select interesting AccountData (i.e. those with never timestamp than any
//!   previously seen for the given (account_id,epoch_id) pair.
//! - asynchronously verify signatures, until an invalid signature is encountered.
//! - if any signature is invalid, drop validation of the remaining signature and ban the peer
//! - all valid signatures verified, so far should be inserted, since otherwise we are open to the
//!   following attack:
//!     - a bad peer may spam us with <N valid AccountData> + <1 invalid AccountData>
//!     - we would validate everything every time, realizing that the last one is invalid, then
//!       discarding the progress
//!     - banning a peer wouldn't help since peers are anonymous, so a single attacker can act as a
//!       lot of peers
use crate::concurrency::arc_mutex::ArcMutex;
use crate::network_protocol;
use crate::network_protocol::SignedAccountData;
use crate::types::AccountKeys;
use near_crypto::PublicKey;
use near_o11y::log_assert;
use near_primitives::network::PeerId;
use near_primitives::types::{AccountId, EpochId};
use rayon::iter::{ParallelBridge, ParallelIterator};
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[cfg(test)]
mod tests;

struct MustCompleteGuard;

impl Drop for MustCompleteGuard {
    fn drop(&mut self) {
        log_assert!(false, "dropped a non-abortable future before completion");
    }
}

/// must_complete wraps a future, so that it logs an error if it is dropped before completion.
/// Possibility of future abort at every await makes the control flow unnecessarily complicated.
/// In fact, only few basic futures (like io primitives) actually need to be abortable, so
/// that they can be put together into a tokio::select block. All the higher level logic
/// would greatly benefit (in terms of readability and bug-resistance) from being non-abortable.
/// Rust doesn't support linear types as of now, so best we can do is a runtime check.
/// TODO(gprusak): we would like to make the futures non-abortable, however with the current
/// semantics of actix, which drops all the futures when stopped this is not feasible.
/// Reconsider how to introduce must_complete to our codebase.
#[allow(dead_code)]
fn must_complete<Fut: Future>(fut: Fut) -> impl Future<Output = Fut::Output> {
    let guard = MustCompleteGuard;
    async move {
        let res = fut.await;
        let _ = std::mem::ManuallyDrop::new(guard);
        res
    }
}

/// spawns a closure on a global rayon threadpool and awaits its completion.
/// Returns the closure result.
/// WARNING: panicking within a rayon task seems to be causing a double panic,
/// and hence the panic message is not visible when running "cargo test".
async fn rayon_spawn<T: 'static + Send>(f: impl 'static + Send + FnOnce() -> T) -> T {
    let (send, recv) = tokio::sync::oneshot::channel();
    rayon::spawn(move || {
        if send.send(f()).is_err() {
            tracing::warn!("rayon_spawn has been aborted");
        }
    });
    recv.await.unwrap()
}

/// Applies f to the iterated elements and collects the results, until the first None is returned.
/// Returns the results collected so far and a bool (false iff any None was returned).
fn try_map<I: ParallelIterator, T: Send>(
    iter: I,
    f: impl Sync + Send + Fn(I::Item) -> Option<T>,
) -> (Vec<T>, bool) {
    let ok = AtomicBool::new(true);
    let res = iter
        .filter_map(|v| {
            if !ok.load(Ordering::Acquire) {
                return None;
            }
            let res = f(v);
            if res.is_none() {
                ok.store(false, Ordering::Release);
            }
            res
        })
        .collect();
    (res, ok.load(Ordering::Acquire))
}

#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub(crate) enum Error {
    #[error("found an invalid signature")]
    InvalidSignature,
    #[error("found too large payload")]
    DataTooLarge,
    #[error("found multiple entries for the same (epoch_id,account_id)")]
    SingleAccountMultipleData,
}

#[derive(Clone)]
pub struct CacheSnapshot {
    pub keys: Arc<AccountKeys>,
    /// Current state of knowledge about an account.
    /// key is the public key of the account in the given epoch.
    /// It will be used to verify new incoming versions of SignedAccountData
    /// for this account.
    pub data: im::HashMap<(EpochId, AccountId), Arc<SignedAccountData>>,
    /// Indices on data.
    pub by_account: im::HashMap<AccountId, im::HashMap<EpochId, Arc<SignedAccountData>>>,
}

impl CacheSnapshot {
    /// Finds all `epoch_id` for which the given account key is registered.
    /// Complexity: O(keys.len()).
    pub fn epochs(&self, account_id: &AccountId, key: &PublicKey) -> Vec<EpochId> {
        self.keys
            .iter()
            .filter_map(move |((e, a), k)| if a == account_id && k == key { Some(e) } else { None })
            .cloned()
            .collect()
    }

    /// Checks if the key set contains the given account key.
    /// Complexity: O(keys.len()).
    pub fn contains_account_key(&self, account_id: &AccountId, key: &PublicKey) -> bool {
        self.epochs(account_id, key).len() > 0
    }

    /// Finds if peer_id is currently a TIER1 peer, according to the collected account data.
    /// Complexity: O(data.len()).
    pub fn is_tier1_peer(&self, peer_id: &PeerId) -> bool {
        self.data.values().filter(|d| d.peer_id.as_ref() == Some(peer_id)).count() > 0
    }

    fn is_new(&self, d: &SignedAccountData) -> bool {
        let id = (d.epoch_id.clone(), d.account_id.clone());
        self.keys.contains_key(&id)
            && match self.data.get(&id) {
                Some(old) if old.timestamp >= d.timestamp => false,
                _ => true,
            }
    }

    fn try_insert(&mut self, d: Arc<SignedAccountData>) -> Option<Arc<SignedAccountData>> {
        if !self.is_new(&d) {
            return None;
        }
        self.data.insert((d.epoch_id.clone(), d.account_id.clone()), d.clone());
        self.by_account
            .entry(d.account_id.clone())
            .or_default()
            .insert(d.epoch_id.clone(), d.clone());
        Some(d)
    }
}

pub(crate) struct Cache(ArcMutex<CacheSnapshot>);

impl Cache {
    pub fn new() -> Self {
        Self(ArcMutex::new(CacheSnapshot {
            keys: Arc::new(AccountKeys::default()),
            data: im::HashMap::new(),
            by_account: im::HashMap::new(),
        }))
    }

    /// Updates the set of important accounts and their public keys.
    /// The AccountData which is no longer important is dropped.
    /// Returns true iff the set of accounts actually changed.
    pub fn set_keys(&self, keys: Arc<AccountKeys>) -> bool {
        self.0.update(|inner| {
            // Skip further processing if the key set didn't change.
            // NOTE: if T implements Eq, then Arc<T> short circuits equality for x == x.
            if keys == inner.keys {
                return false;
            }
            std::mem::take(&mut inner.by_account);
            for (k, v) in std::mem::take(&mut inner.data) {
                if keys.contains_key(&k) {
                    inner.data.insert(k.clone(), v.clone());
                    inner.by_account.entry(k.1).or_default().insert(k.0, v);
                }
            }
            inner.keys = keys;
            true
        })
    }

    /// Selects new data and verifies the signatures.
    /// Returns the verified new data and an optional error.
    /// Note that even if error has been returned the partially validated output is returned
    /// anyway.
    fn verify(
        &self,
        data: Vec<Arc<SignedAccountData>>,
    ) -> (Vec<Arc<SignedAccountData>>, Option<Error>) {
        // Filter out non-interesting data, so that we never check signatures for valid non-interesting data.
        // Bad peers may force us to check signatures for fake data anyway, but we will ban them after first invalid signature.
        // It locks epochs for reading for a short period.
        let mut data_and_keys = HashMap::new();
        let inner = self.0.load();
        for d in data {
            // There is a limit on the amount of RAM occupied by per-account datasets.
            // Broadcasting larger datasets is considered malicious behavior.
            if d.payload().len() > network_protocol::MAX_ACCOUNT_DATA_SIZE_BYTES {
                return (vec![], Some(Error::DataTooLarge));
            }
            let id = (d.epoch_id.clone(), d.account_id.clone());
            // We want the communication needed for broadcasting per-account data to be minimal.
            // Therefore broadcasting multiple datasets per account is considered malicious
            // behavior, since all but one are obviously outdated.
            if data_and_keys.contains_key(&id) {
                return (vec![], Some(Error::SingleAccountMultipleData));
            }
            // It is fine to broadcast data we already know about.
            if !inner.is_new(&d) {
                continue;
            }
            // It is fine to broadcast account data that we don't care about.
            let key = match inner.keys.get(&id) {
                Some(key) => key.clone(),
                None => continue,
            };
            data_and_keys.insert(id, (d, key));
        }

        // Verify the signatures in parallel.
        // Verification will stop at the first encountered error.
        let (data, ok) = try_map(data_and_keys.into_values().par_bridge(), |(d, key)| {
            if d.payload().verify(&key).is_ok() {
                return Some(d);
            }
            return None;
        });
        if !ok {
            return (data, Some(Error::InvalidSignature));
        }
        (data, None)
    }

    /// Verifies the signatures and inserts verified data to the cache.
    /// Returns the data inserted and optionally a verification error.
    /// WriteLock is acquired only for the final update (after verification).
    pub async fn insert(
        self: &Arc<Self>,
        data: Vec<Arc<SignedAccountData>>,
    ) -> (Vec<Arc<SignedAccountData>>, Option<Error>) {
        let this = self.clone();
        // Execute verification on the rayon threadpool.
        let (data, err) = rayon_spawn(move || this.verify(data)).await;
        // Insert the successfully verified data, even if an error has been encountered.
        let inserted =
            self.0.update(|inner| data.into_iter().filter_map(|d| inner.try_insert(d)).collect());
        // Return the inserted data.
        (inserted, err)
    }

    /// Loads the current cache snapshot.
    pub fn load(&self) -> Arc<CacheSnapshot> {
        self.0.load()
    }
}
