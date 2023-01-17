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
use crate::network_protocol::{AccountData,VersionedAccountData, SignedAccountData};
use near_primitives::validator_signer::ValidatorSigner;
use crate::types::AccountKeys;
use near_crypto::PublicKey;
use rayon::iter::ParallelBridge;
use std::collections::HashMap;
use std::sync::Arc;
use crate::time;

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

pub struct LocalData {
    pub signer :Arc<dyn ValidatorSigner>,
    pub data :Arc<AccountData>,
}

#[derive(Clone)]
pub struct CacheSnapshot {
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
    
    /// AccountData of this (local) node.
    /// In case we receive data about this node which is different than local.data,
    /// We sign local.data with local.signer with a newer version than the received data,
    /// to override what we received. This may happen in case the node has been restarted
    /// with a new node key - network knows AccountData with the old node key and we need
    /// to immediately override it, so that the network can correctly route the messages.
    pub local: Option<LocalData>,
}

impl CacheSnapshot {
    fn is_new(&self, d: &SignedAccountData) -> bool {
        self.keys.contains(&d.account_key)
            && match self.data.get(&d.account_key) {
                Some(old) if old.version >= d.version => false,
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

    fn update_local(&mut self, clock: &time::Clock) -> Option<Arc<SignedAccountData>> {
        let local = match self.local {
            Some(it) => it,
            None => return None,
        };
        let got = self.data.get(&local.signer.public_key());
        if got == Some(&local.data) { 
            return None;
        }
        let want = VersionedAccountData {
            data: local.data.clone(),
            account_key: local.signer.public_key(),
            version: got.map_or(0,|d|d.version)+1,
            timestamp: clock.now(),
        }.sign(local.signer).unwrap();
        self.data.try_insert(want)
    }
}

pub(crate) struct Cache(ArcMutex<CacheSnapshot>);

impl Cache {
    pub fn new() -> Self {
        Self(ArcMutex::new(CacheSnapshot {
            keys_by_id: Arc::new(AccountKeys::default()),
            keys: im::HashSet::new(),
            data: im::HashMap::new(),
            local: None,
        }))
    }

    /// Updates the set of important accounts and their public keys.
    /// The AccountData which is no longer important is dropped.
    /// Returns true iff the set of accounts actually changed.
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
    ) -> (Vec<Arc<SignedAccountData>>, Option<Error>) {
        // Filter out non-interesting data, so that we never check signatures for valid non-interesting data.
        // Bad peers may force us to check signatures for fake data anyway, but we will ban them after first invalid signature.
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
            return (data, Some(Error::InvalidSignature));
        }
        (data, None)
    }

    pub fn set_local(
        self: &Arc<Self>,
        clock: &time::Clock,
        local: LocalData,
    ) -> Option<Arc<SignedAccountData>> {
        self.0.update(|mut inner| {
            inner.local = Some(local);
            inner.update_local()
        })
    }

    /// Verifies the signatures and inserts verified data to the cache.
    /// Returns the data inserted and optionally a verification error.
    /// WriteLock is acquired only for the final update (after verification).
    pub async fn insert(
        self: &Arc<Self>,
        clock: &time::Clock,
        data: Vec<Arc<SignedAccountData>>,
    ) -> (Vec<Arc<SignedAccountData>>, Option<Error>) {
        let this = self.clone();
        // Execute verification on the rayon threadpool.
        let (data, err) = this.verify(data).await;
        // Insert the successfully verified data, even if an error has been encountered.
        let inserted = self.0.update(|mut inner| {
            let mut inserted : Vec<_> = data.into_iter().filter_map(|d| inner.try_insert(d)).collect();
            inserted.extend(inner.update_local(clock));
            (inserted, inner)
        });
        // Return the inserted data.
        (inserted, err)
    }

    /// Loads the current cache snapshot.
    pub fn load(&self) -> Arc<CacheSnapshot> {
        self.0.load()
    }
}
