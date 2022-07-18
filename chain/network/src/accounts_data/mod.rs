//! Cache of Cache for a small number of epochs. (probably just current and next epoch).
//! Assumptions:
//! - verifying signatures is expensive, we need a dedicated threadpool for handling that.
//!   TODO(gprusak): it would be nice to have a benchmark for that
//! - a bad peer may attack by sending a lot of invalid signatures
//! - we can afford verifying all valid signatures (for the given epochs) that we have never seen before.
//! - we can afford verifying a few invalid signatures per PeerMessage.
//!
//! Strategy:
//! - handling of SyncAccountDataResponses should be throttled by PeerActor/PeerManagerActor.
//! - synchronously select interesting Cache (i.e. those with never timestamp than any
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
use crate::network_protocol;
use crate::network_protocol::SignedAccountData;
use near_crypto::PublicKey;
use near_network_primitives::time;
use near_network_primitives::types::NetworkEpochInfo;
use near_primitives::types::{AccountId, EpochId};
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[cfg(test)]
mod tests;

/// Current state of knowledge about an account.
/// key is the public key of the account in the given epoch.
/// It will be used to verify new incoming versions of SignedAccountData
/// for this account.
struct Account {
    key: PublicKey,
    data: Option<SignedAccountData>,
}

impl Account {
    fn is_new(&self, t: time::Utc) -> bool {
        match &self.data {
            Some(old) if old.timestamp >= t => false,
            _ => true,
        }
    }
}

struct Epoch(HashMap<AccountId, Account>);

impl Epoch {
    fn new(keys: &HashMap<AccountId, PublicKey>) -> Self {
        Self(
            keys.iter()
                .map(|(id, key)| (id.clone(), Account { key: key.clone(), data: None }))
                .collect(),
        )
    }
}

#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub(crate) enum Error {
    #[error("found an invalid signature")]
    InvalidSignature,
    #[error("found too large payload")]
    DataTooLarge,
    #[error("found multiple entries for the same (epoch_id,account_id)")]
    SingleAccountMultipleData,
    #[error("found account data not relevant for the given epoch")]
    InvalidAccount,
}

/// Current state of per-epoch knowledge about accounts.
/// It is a small map: it is expected to just contain current epoch and next epoch.
struct Epochs(HashMap<EpochId, Epoch>);

impl Epochs {
    fn try_insert(&mut self, d: SignedAccountData) -> Option<SignedAccountData> {
        let mut a = self.0.get_mut(&d.epoch_id)?.0.get_mut(&d.account_id)?;
        if !a.is_new(d.timestamp) {
            return None;
        }
        a.data = Some(d.clone());
        Some(d)
    }
}

// TODO(gprusak): replace Option with ManuallyDrop?
struct Runtime(Option<tokio::runtime::Runtime>);

impl std::ops::Deref for Runtime {
    type Target = tokio::runtime::Runtime;
    fn deref(&self) -> &Self::Target {
        self.0.as_ref().unwrap()
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        self.0.take().unwrap().shutdown_background()
    }
}

impl Runtime {
    fn new() -> Self {
        Self(Some(tokio::runtime::Runtime::new().unwrap()))
    }
}

pub(crate) struct Cache {
    epochs: RwLock<Epochs>,
    runtime: Runtime,
}

// TODO(gprusak): Cache will be used in the next PR.
#[allow(dead_code)]
impl Cache {
    pub fn new() -> Self {
        Self { epochs: RwLock::new(Epochs(HashMap::new())), runtime: Runtime::new() }
    }

    /// Sets new_epochs as active and copies over the keys
    /// for accounts which were not active before.
    /// The set of accounts per epoch is expected to be deterministic,
    /// So it is not possible to update the set of accounts for an already
    /// active epoch - such an update will be ignored silently.
    /// This code is more general than needed: active epochs are expected
    /// to be just {this_epoch,next_epoch} and set_epochs should be called
    /// every time chain advances to the next epoch. set_epoch is idempotent
    /// and is very cheap in case it is called with the same argument multiple
    /// times.
    /// TODO(gprusak): note that the current implementation just checks that
    /// epoch_id didn't change, then ignoring content.
    /// It would be more strict (while still being cheap) to
    /// accept NetworkEpochInfo with a cached hash, and compare hashes instead of epoch_id.
    pub fn set_epochs(&self, new_epochs: Vec<&NetworkEpochInfo>) -> bool {
        let mut epochs = self.epochs.write();
        epochs.0.retain(|id, _| new_epochs.iter().any(|e| &e.id == id));
        let mut has_new = false;
        for e in new_epochs {
            epochs.0.entry(e.id.clone()).or_insert_with(|| {
                has_new = true;
                Epoch::new(&e.priority_accounts)
            });
        }
        has_new
    }

    /// Selects new data and verifies the signatures.
    /// Returns the verified new data and an optional error.
    /// Note that even if error has been returned the partially validated output is returned
    /// anyway.
    fn verify(&self, data: Vec<SignedAccountData>) -> (Vec<SignedAccountData>, Option<Error>) {
        // Filter out non-interesting data, so that we never check signatures for valid non-interesting data.
        // Bad peers may force us to check signatures for fake data anyway, but we will ban them after first invalid signature.
        // It locks epochs for reading for a short period.
        let mut found = HashSet::new();
        let mut data_and_keys = vec![];
        let epochs = self.epochs.read();
        for d in data {
            // We want the communication needed for broadcasting per-account data to be minimal.
            // Therefore broadcasting multiple datasets per account is considered malicious
            // behavior, since all but one are obviously outdated.
            let data_id = (d.epoch_id.clone(), d.account_id.clone());
            if found.contains(&data_id) {
                return (vec![], Some(Error::SingleAccountMultipleData));
            }
            found.insert(data_id);
            // There is a limit on the amount of RAM occupied by per-account datasets.
            // Broadcasting larger datasets is considered malicious behavior.
            if d.payload().len() > network_protocol::MAX_ACCOUNT_DATA_SIZE_BYTES {
                return (vec![], Some(Error::DataTooLarge));
            }
            // It is fine to broadcast account data for epochs that we don't care about.
            let epoch = match epochs.0.get(&d.epoch_id) {
                Some(e) => e,
                None => continue,
            };
            // It is NOT fine to broadcast account data which is not relevant to the given epoch.
            let account = match epoch.0.get(&d.account_id) {
                Some(a) => a,
                None => return (vec![], Some(Error::InvalidAccount)),
            };
            // It is fine to broadcast data we already know about.
            if !account.is_new(d.timestamp) {
                continue;
            }
            data_and_keys.push((d, account.key.clone()));
        }
        drop(epochs);

        // We verify signatures synchronously for now.
        // To verify signatures in parallel, we should have a way to stop verification at the first invalid one.
        let mut data = vec![];
        for (d, key) in data_and_keys {
            if !d.payload().verify(&key) {
                return (data, Some(Error::InvalidSignature));
            }
            data.push(d);
        }
        return (data, None);
    }

    /// Verifies the signatures and inserts verified data to the cache.
    /// Returns the data inserted and optionally a verification error.
    /// WriteLock is acquired only for the final update (after verification).
    pub async fn insert(
        self: Arc<Self>,
        data: Vec<SignedAccountData>,
    ) -> (Vec<SignedAccountData>, Option<Error>) {
        let x = self.clone();
        // Execute insertion in a dedicated runtime.
        self.runtime
            .spawn(async move {
                let (data, err) = x.verify(data);
                // Insert the successfully verified data, even if an error has been encountered.
                let mut epochs = x.epochs.write();
                // Return the inserted data.
                (data.into_iter().filter_map(|d| epochs.try_insert(d)).collect(), err)
            })
            .await
            .unwrap()
    }

    /// Copies and returns all the AccountData in the cache.
    pub fn dump(&self) -> Vec<SignedAccountData> {
        self.epochs
            .read()
            .0
            .values()
            .map(|e| &e.0)
            .flatten()
            .filter_map(|(_, a)| a.data.clone())
            .collect()
    }
}
