//! Cache of SnapshotHostInfos.
//!
//! Each node in the network which is willing to generate and serve state snapshots
//! publishes a SnapshotHostInfo once per epoch. The info is flooded to all nodes
//! in the network and stored locally in this cache.

use crate::concurrency;
use crate::concurrency::arc_mutex::ArcMutex;
use crate::network_protocol::SnapshotHostInfo;
use near_primitives::network::PeerId;
use rayon::iter::ParallelBridge;
use std::collections::HashMap;
use std::sync::Arc;

#[cfg(test)]
mod tests;

#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub(crate) enum SnapshotHostInfoError {
    #[error("found an invalid signature")]
    InvalidSignature,
    #[error("found multiple entries for the same peer_id")]
    DuplicatePeerId,
}

/// TODO(saketh): Introduce a cache size limit
#[derive(Clone)]
struct Inner {
    /// The latest known SnapshotHostInfo for each node in the network
    hosts: im::HashMap<PeerId, Arc<SnapshotHostInfo>>,
}

impl Inner {
    fn is_new(&self, h: &SnapshotHostInfo) -> bool {
        match self.hosts.get(&h.peer_id) {
            Some(old) if old.epoch_height >= h.epoch_height => false,
            _ => true,
        }
    }

    /// Inserts d into self.data, if it's new.
    /// It returns the newly inserted value (or None if nothing changed).
    /// The returned value should be broadcasted to the network.
    fn try_insert(&mut self, d: Arc<SnapshotHostInfo>) -> Option<Arc<SnapshotHostInfo>> {
        if !self.is_new(&d) {
            return None;
        }
        self.hosts.insert(d.peer_id.clone(), d.clone());
        Some(d)
    }
}

pub(crate) struct SnapshotHostsCache(ArcMutex<Inner>);

impl SnapshotHostsCache {
    pub fn new() -> Self {
        Self(ArcMutex::new(Inner { hosts: im::HashMap::new() }))
    }

    /// Selects new data and verifies the signatures.
    /// Returns the verified new data and an optional error.
    /// Note that even if error has been returned the partially validated output is returned anyway.
    async fn verify(
        &self,
        data: Vec<Arc<SnapshotHostInfo>>,
    ) -> (Vec<Arc<SnapshotHostInfo>>, Option<SnapshotHostInfoError>) {
        // Filter out any data which is outdated or which we already have.
        let mut new_data = HashMap::new();
        let inner = self.0.load();
        for d in data {
            // Sharing multiple entries for the same peer is considered malicious,
            // since all but one are obviously outdated.
            if new_data.contains_key(&d.peer_id) {
                return (vec![], Some(SnapshotHostInfoError::DuplicatePeerId));
            }
            // It is fine to broadcast data we already know about.
            // It is fine to broadcast data which we know to be outdated.
            if inner.is_new(&d) {
                new_data.insert(d.peer_id.clone(), d);
            }
        }

        // Verify the signatures in parallel.
        // Verification will stop at the first encountered error.
        let (data, ok) = concurrency::rayon::run(move || {
            concurrency::rayon::try_map(new_data.into_values().par_bridge(), |d| match d.verify() {
                true => Some(d),
                false => None,
            })
        })
        .await;
        if !ok {
            return (data, Some(SnapshotHostInfoError::InvalidSignature));
        }
        (data, None)
    }

    /// Verifies the signatures and inserts verified data to the cache.
    /// Returns the data inserted and optionally a verification error.
    /// WriteLock is acquired only for the final update (after verification).
    pub async fn insert(
        self: &Arc<Self>,
        data: Vec<Arc<SnapshotHostInfo>>,
    ) -> (Vec<Arc<SnapshotHostInfo>>, Option<SnapshotHostInfoError>) {
        let this = self.clone();
        // Execute verification on the rayon threadpool.
        let (data, err) = this.verify(data).await;
        // Insert the successfully verified data, even if an error has been encountered.
        let inserted = self.0.update(|mut inner| {
            let inserted = data.into_iter().filter_map(|d| inner.try_insert(d)).collect();
            (inserted, inner)
        });
        // Return the inserted data.
        (inserted, err)
    }

    pub fn get_hosts(&self) -> Vec<Arc<SnapshotHostInfo>> {
        self.0.load().hosts.values().cloned().collect()
    }
}
