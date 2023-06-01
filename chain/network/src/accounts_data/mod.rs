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
//! - synchronously select interesting AccountData (i.e. those with newer version than any
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
use crate::network_protocol::{AccountData, SignedAccountData, VersionedAccountData};
use crate::types::AccountKeys;
use near_async::time;
use near_crypto::PublicKey;
use near_primitives::validator_signer::ValidatorSigner;
use rayon::iter::ParallelBridge;
use std::collections::HashMap;
use std::sync::Arc;

#[cfg(test)]
mod tests;

#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub(crate) enum AccountDataError {
    #[error("found an invalid signature")]
    InvalidSignature,
    #[error("found too large payload")]
    DataTooLarge,
    #[error("found multiple entries for the same (epoch_id,account_id)")]
    SingleAccountMultipleData,
}

/// Most up-to-date AccountData of this node and a signer
/// to sign it with when there is a need to override some
/// already signed data received from the network. See `AccountDataCache::set_local`
/// for more details.
#[derive(Clone)]
pub struct LocalAccountData {
    pub signer: Arc<dyn ValidatorSigner>,
    pub data: Arc<AccountData>,
}

/// See module-level documentation.
#[derive(Clone)]
pub struct AccountDataCacheSnapshot {
    /// Map from account ID to account key.
    /// Used only for selecting target when routing a message to a TIER1 peer.
    /// TODO(gprusak): In fact, since the account key assigned to a given account ID can change
    /// between epochs, Client should rather send messages to node with a specific account key,
    /// rather than with a specific account ID.
    pub keys_by_id: Arc<AccountKeys>,
    /// Set of account keys allowed on TIER1 network.
    pub keys: im::HashSet<PublicKey>,
    /// Current state of knowledge about an account.
    /// `data.keys()` is a subset of `keys` at all times,
    /// as cache is collecting data only about the accounts from `keys`,
    /// and data about the particular account might be not known at the given moment.
    pub data: im::HashMap<PublicKey, Arc<SignedAccountData>>,

    pub local: Option<LocalAccountData>,
}

impl AccountDataCacheSnapshot {
    /// Checks if `(d.version,d.timestamp)` is newer (greater) than
    /// `(old.version,old.timestamp)`, where `old` is the AccountData for
    /// `d.account_key` already stored in the AccountDataCache.
    ///
    /// It returns `false` in case `d.account_key` is not in `d.keys`,
    /// because it means that `AccountDataCache` is not interested in these data at all.
    ///
    /// Note that when the node is restarted, it forgets
    /// which version it has signed last, so it will again start from version
    /// 0, until it learns from the network about data it already signed in the
    /// previous execution. It means that a node may sign 2 data with the exact same
    /// version. To avoid an inconsistent state of the network (i.e. situation in which
    /// some nodes will learn about one data with the given version, some about the other)
    /// we introduce a tie breaker in a form of UTC timestamp of signing (note that here
    /// we do not rely on clock monotonicity, which is not guaranteed for UTC clock,
    /// just assume that timestamp collision is unlikely).
    ///
    /// The alternatives to using a timestamp would be:
    /// * adding a random_minor_version to AccountData, specifically to avoid collisions
    ///   (so we would be comparing `(version,random_minor_version)` instead)
    /// * using some crypto hash function `h` and compare `(version,h(data))`. Assuming that `h`
    ///   behaves like a random oracle, the semantics will be equivaluent to
    ///   `random_minor_version`, except that if a node signs exactly the same data and in the
    ///   previous run, then there will be a collision. But in such a case it doesn't matter
    ///   since the data is the same.
    /// * storing `version` of the last signed AccountData in persistent storage,
    ///   so that the node literally never signs AccountsData with colling versions.
    ///   This assumption is fragile as long as validators migrate their nodes without copying over
    ///   the whole storage.
    fn is_new(&self, d: &SignedAccountData) -> bool {
        self.keys.contains(&d.account_key)
            && match self.data.get(&d.account_key) {
                Some(old) if (old.version, old.timestamp) >= (d.version, d.timestamp) => false,
                _ => true,
            }
    }

    /// Inserts d into self.data, if
    /// * `d.account_data` is in self.keys AND
    /// * `d.version > self.data[d.account_data].version`.
    /// If d would override local for this node, an AccountData based on `self.local` is signed
    /// and inserted instead to rollback the overriding change (it can happen in case the node has
    /// been restarted and we observe the old value emitted by the previous run).
    /// It returns the newly inserted value (or None if nothing changed).
    /// The returned value should be broadcasted to the network.
    fn try_insert(
        &mut self,
        clock: &time::Clock,
        d: Arc<SignedAccountData>,
    ) -> Option<Arc<SignedAccountData>> {
        if !self.is_new(&d) {
            return None;
        }
        let d = match &self.local {
            Some(local) if d.account_key == local.signer.public_key() => Arc::new(
                VersionedAccountData {
                    data: local.data.as_ref().clone(),
                    account_key: local.signer.public_key(),
                    version: d.version + 1,
                    timestamp: clock.now_utc(),
                }
                .sign(local.signer.as_ref())
                .unwrap(),
            ),
            _ => d,
        };
        self.data.insert(d.account_key.clone(), d.clone());
        Some(d)
    }

    /// Set the information about this node's account (i.e. AccountData for this node).
    /// It should be called whenever AccountData for the current node changes. This function
    /// is expected to be called periodically, even if AccountData doesn't change.
    ///
    /// If `self.signer` is in `self.keys` then it means that this node is a TIER1 node and
    /// the new AccountData should be broadcasted immediately - set_local() will return a value
    /// to be broadcasted then. Even if the AccountData didn't change since the last call to
    /// set_local(), a value to be broadcasted will be returned (just with newer timestamp).
    /// It is important to tell the network that "yes, I'm still alive, my AccountData didn't
    /// change" (eventually we might want to use the timestamp set expiration date on AccountData).
    ///
    /// If `self.signer` is not in `self.keys` (this is not a TIER1 node), set_local() returns
    /// None.
    fn set_local(
        &mut self,
        clock: &time::Clock,
        local: LocalAccountData,
    ) -> Option<Arc<SignedAccountData>> {
        let account_key = local.signer.public_key();
        let result = match self.keys.contains(&account_key) {
            false => None,
            true => {
                let d = Arc::new(
                    VersionedAccountData {
                        data: local.data.as_ref().clone(),
                        account_key: account_key.clone(),
                        version: self.data.get(&account_key).map_or(0, |d| d.version) + 1,
                        timestamp: clock.now_utc(),
                    }
                    .sign(local.signer.as_ref())
                    .unwrap(),
                );
                self.data.insert(account_key, d.clone());
                Some(d)
            }
        };
        self.local = Some(local);
        result
    }
}

pub(crate) struct AccountDataCache(ArcMutex<AccountDataCacheSnapshot>);

impl AccountDataCache {
    pub fn new() -> Self {
        Self(ArcMutex::new(AccountDataCacheSnapshot {
            keys_by_id: Arc::new(AccountKeys::default()),
            keys: im::HashSet::new(),
            data: im::HashMap::new(),
            local: None,
        }))
    }

    /// Updates the set of important accounts and their public keys.
    /// The AccountData which is no longer important is dropped.
    /// Returns true iff the set of accounts actually changed.
    /// TODO(gprusak): note that local data won't be generated, even if it could be
    ///   (i.e. in case self.local.signer was not present in the old key set, but is in the new)
    ///   so a call to set_local afterwards is required to do that. For now it is fine because
    ///   the AccountDataCache owner is expected to call set_local periodically anyway.
    pub fn set_keys(&self, keys_by_id: Arc<AccountKeys>) -> bool {
        self.0
            .try_update(|mut inner| {
                // Skip further processing if the key set didn't change.
                // NOTE: if T implements Eq, then Arc<T> short circuits equality for x == x.
                if keys_by_id == inner.keys_by_id {
                    return Err(());
                }
                inner.keys_by_id = keys_by_id;
                inner.keys = inner.keys_by_id.values().flatten().cloned().collect();
                inner.data.retain(|k, _| inner.keys.contains(k));
                Ok(((), inner))
            })
            .is_ok()
    }

    /// Selects new data and verifies the signatures.
    /// Returns the verified new data and an optional error.
    /// Note that even if error has been returned the partially validated output is returned
    /// anyway.
    async fn verify(
        &self,
        data: Vec<Arc<SignedAccountData>>,
    ) -> (Vec<Arc<SignedAccountData>>, Option<AccountDataError>) {
        // Filter out non-interesting data, so that we never check signatures for valid non-interesting data.
        // Bad peers may force us to check signatures for fake data anyway, but we will ban them after first invalid signature.
        let mut new_data = HashMap::new();
        let inner = self.0.load();
        for d in data {
            // There is a limit on the amount of RAM occupied by per-account datasets.
            // Broadcasting larger datasets is considered malicious behavior.
            if d.payload().len() > network_protocol::MAX_ACCOUNT_DATA_SIZE_BYTES {
                return (vec![], Some(AccountDataError::DataTooLarge));
            }
            // We want the communication needed for broadcasting per-account data to be minimal.
            // Therefore broadcasting multiple datasets per account is considered malicious
            // behavior, since all but one are obviously outdated.
            if new_data.contains_key(&d.account_key) {
                return (vec![], Some(AccountDataError::SingleAccountMultipleData));
            }
            // It is fine to broadcast data we already know about.
            // It is fine to broadcast account data that we don't care about.
            if inner.is_new(&d) {
                new_data.insert(d.account_key.clone(), d);
            }
        }

        // Verify the signatures in parallel.
        // Verification will stop at the first encountered error.
        let (data, ok) = concurrency::rayon::run(move || {
            concurrency::rayon::try_map(new_data.into_values().par_bridge(), |d| {
                match d.payload().verify(&d.account_key) {
                    Ok(()) => Some(d),
                    Err(()) => None,
                }
            })
        })
        .await;
        if !ok {
            return (data, Some(AccountDataError::InvalidSignature));
        }
        (data, None)
    }

    pub fn set_local(
        self: &Arc<Self>,
        clock: &time::Clock,
        local: LocalAccountData,
    ) -> Option<Arc<SignedAccountData>> {
        self.0.update(|mut inner| {
            let data = inner.set_local(clock, local);
            (data, inner)
        })
    }

    /// Verifies the signatures and inserts verified data to the cache.
    /// Returns the data inserted and optionally a verification error.
    /// WriteLock is acquired only for the final update (after verification).
    pub async fn insert(
        self: &Arc<Self>,
        clock: &time::Clock,
        data: Vec<Arc<SignedAccountData>>,
    ) -> (Vec<Arc<SignedAccountData>>, Option<AccountDataError>) {
        let this = self.clone();
        // Execute verification on the rayon threadpool.
        let (data, err) = this.verify(data).await;
        // Insert the successfully verified data, even if an error has been encountered.
        let inserted = self.0.update(|mut inner| {
            let inserted = data.into_iter().filter_map(|d| inner.try_insert(clock, d)).collect();
            (inserted, inner)
        });
        // Return the inserted data.
        (inserted, err)
    }

    /// Loads the current cache snapshot.
    pub fn load(&self) -> Arc<AccountDataCacheSnapshot> {
        self.0.load()
    }
}
