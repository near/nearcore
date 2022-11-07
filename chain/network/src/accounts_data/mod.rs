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
use crate::concurrency;
use crate::concurrency::arc_mutex::ArcMutex;
use crate::network_protocol;
use crate::network_protocol::SignedAccountData;
use crate::types::AccountKeys;
use near_crypto::PublicKey;
//use near_primitives::network::PeerId;
//use near_primitives::types::{AccountId, EpochId};
use rayon::iter::ParallelBridge;
use std::collections::HashMap;
use std::sync::Arc;

#[cfg(test)]
mod tests;

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
    pub keys_by_id: Arc<AccountKeys>,
    pub keys: im::HashSet<PublicKey>,
    /// Current state of knowledge about an account.
    pub data: im::HashMap<PublicKey, Arc<SignedAccountData>>,
}

impl CacheSnapshot {
    fn is_new(&self, d: &SignedAccountData) -> bool {
        self.keys.contains(&d.account_key)
            && match self.data.get(&d.account_key) {
                Some(old) if old.timestamp >= d.timestamp => false,
                _ => true,
            }
    }

    fn try_insert(&mut self, d: Arc<SignedAccountData>) -> Option<Arc<SignedAccountData>> {
        if !self.is_new(&d) {
            return None;
        }
        self.data.insert(d.account_key.clone(), d.clone());
        Some(d)
    }
}

pub(crate) struct Cache(ArcMutex<CacheSnapshot>);

impl Cache {
    pub fn new() -> Self {
        Self(ArcMutex::new(CacheSnapshot {
            keys_by_id: Arc::new(AccountKeys::default()),
            keys: im::HashSet::new(),
            data: im::HashMap::new(),
        }))
    }

    /// Updates the set of important accounts and their public keys.
    /// The AccountData which is no longer important is dropped.
    /// Returns true iff the set of accounts actually changed.
    pub fn set_keys(&self, keys_by_id: Arc<AccountKeys>) -> bool {
        self.0.update(|inner| {
            // Skip further processing if the key set didn't change.
            // NOTE: if T implements Eq, then Arc<T> short circuits equality for x == x.
            if keys_by_id == inner.keys_by_id {
                return false;
            }
            inner.keys_by_id = keys_by_id;
            inner.keys = keys_by_id.values().cloned().collect();
            for (k, v) in std::mem::take(&mut inner.data) {
                if inner.keys.contains(&k) {
                    inner.data.insert(k.clone(), v.clone());
                }
            }
            true
        })
    }

    /// Selects new data and verifies the signatures.
    /// Returns the verified new data and an optional error.
    /// Note that even if error has been returned the partially validated output is returned
    /// anyway.
    async fn verify(
        &self,
        data: Vec<Arc<SignedAccountData>>,
    ) -> (Vec<Arc<SignedAccountData>>, Option<Error>) {
        // Filter out non-interesting data, so that we never check signatures for valid non-interesting data.
        // Bad peers may force us to check signatures for fake data anyway, but we will ban them after first invalid signature.
        // It locks epochs for reading for a short period.
        let mut new_data = HashMap::new();
        let inner = self.0.load();
        for d in data {
            // There is a limit on the amount of RAM occupied by per-account datasets.
            // Broadcasting larger datasets is considered malicious behavior.
            if d.payload().len() > network_protocol::MAX_ACCOUNT_DATA_SIZE_BYTES {
                return (vec![], Some(Error::DataTooLarge));
            }
            // We want the communication needed for broadcasting per-account data to be minimal.
            // Therefore broadcasting multiple datasets per account is considered malicious
            // behavior, since all but one are obviously outdated.
            if new_data.contains_key(&d.account_key) {
                return (vec![], Some(Error::SingleAccountMultipleData));
            }
            // It is fine to broadcast data we already know about.
            // It is fine to broadcast account data that we don't care about.
            if inner.is_new(&d) {
                new_data.insert(d.account_key.clone(),d);
            }
        }

        // Verify the signatures in parallel.
        // Verification will stop at the first encountered error.
        let (data, ok) = concurrency::rayon::run(
            move || concurrency::rayon::try_map(
                new_data.into_values().par_bridge(),
                |d| match d.payload().verify(&d.account_key) {
                    Ok(()) => Some(d),
                    Err(()) => None,
                }
            )
        ).await;
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
        let (data, err) = this.verify(data).await;
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
