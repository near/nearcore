//! Cache of SnapshotHostInfos.
//!
//! Each node in the network which is willing to generate and serve state snapshots
//! publishes a SnapshotHostInfo once per epoch. The info is flooded to all nodes
//! in the network and stored locally in this cache.

use crate::concurrency;
use crate::network_protocol::SnapshotHostInfo;
use lru::LruCache;
use near_primitives::network::PeerId;
use parking_lot::Mutex;
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

#[derive(Clone)]
pub struct Config {
    /// The maximum number of SnapshotHostInfos to store locally.
    /// At present this constraint is enforced using a simple
    /// least-recently-used cache. In the future, we may wish to
    /// implement something more sophisticated.
    pub snapshot_hosts_cache_size: u32,
}

struct Inner {
    /// The latest known SnapshotHostInfo for each node in the network
    hosts: LruCache<PeerId, Arc<SnapshotHostInfo>>,
}

impl Inner {
    fn is_new(&self, h: &SnapshotHostInfo) -> bool {
        match self.hosts.peek(&h.peer_id) {
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
        self.hosts.push(d.peer_id.clone(), d.clone());
        Some(d)
    }
}

pub(crate) struct SnapshotHostsCache(Mutex<Inner>);

impl SnapshotHostsCache {
    pub fn new(config: Config) -> Self {
        let hosts = LruCache::new(config.snapshot_hosts_cache_size as usize);
        Self(Mutex::new(Inner { hosts }))
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
        {
            let inner = self.0.lock();
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
        self: &Self,
        data: Vec<Arc<SnapshotHostInfo>>,
    ) -> (Vec<Arc<SnapshotHostInfo>>, Option<SnapshotHostInfoError>) {
        // Execute verification on the rayon threadpool.
        let (data, err) = self.verify(data).await;
        // Insert the successfully verified data, even if an error has been encountered.
        let mut newly_inserted_data: Vec<Arc<SnapshotHostInfo>> = vec![];
        let mut inner = self.0.lock();
        for d in data {
            if let Some(inserted) = inner.try_insert(d) {
                newly_inserted_data.push(inserted);
            }
        }
        // Return the inserted data.
        (newly_inserted_data, err)
    }

    pub fn get_hosts(&self) -> Vec<Arc<SnapshotHostInfo>> {
        self.0.lock().hosts.iter().map(|(_, v)| v.clone()).collect()
    }
}
