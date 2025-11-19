//! Cache of SnapshotHostInfos.
//!
//! Each node in the network which is willing to generate and serve state snapshots
//! publishes a SnapshotHostInfo once per epoch. The info is flooded to all nodes
//! in the network and stored locally in this cache.

use crate::concurrency;
use crate::network_protocol::SnapshotHostInfo;
use crate::network_protocol::SnapshotHostInfoVerificationError;
use itertools::Itertools;
use lru::LruCache;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::types::EpochHeight;
use near_primitives::types::ShardId;
use parking_lot::Mutex;
use rand::prelude::IteratorRandom;
use rand::thread_rng;
use rayon::iter::ParallelBridge;
use sha2::{Digest, Sha256};
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;

#[cfg(test)]
mod tests;

/// The number of epochs for which we keep the historical host info.
/// For now, we only keep the most recent epoch.
const EPOCH_RETENTION_WINDOW: u64 = 1;

#[derive(thiserror::Error, Debug, PartialEq, Eq, Clone)]
pub(crate) enum SnapshotHostInfoError {
    #[error("found multiple entries for the same peer_id")]
    DuplicatePeerId,
    #[error(transparent)]
    VerificationError(#[from] SnapshotHostInfoVerificationError),
}

#[derive(Clone)]
pub struct Config {
    /// The maximum number of SnapshotHostInfos to store locally.
    /// At present this constraint is enforced using a simple
    /// least-recently-used cache. In the future, we may wish to
    /// implement something more sophisticated.
    pub snapshot_hosts_cache_size: u32,
    /// The number of hosts we'll add to structures related to state part peer
    /// selection each time we need to request parts from a new peer
    pub part_selection_cache_batch_size: u32,
}

/// When multiple hosts offer the same part, this hash is compared
/// to determine the order in which to query them. All nodes
/// use the same hashing scheme, resulting in a rough consensus on
/// which hosts serve requests for which parts.
pub(crate) fn priority_score(peer_id: &PeerId, shard_id: ShardId, part_id: u64) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(peer_id.public_key().key_data());
    h.update(shard_id.to_le_bytes());
    h.update(part_id.to_le_bytes());
    h.finalize().into()
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct StatePartHost {
    /// A peer host for some desired state part
    peer_id: PeerId,
    /// Priority score computed over the peer_id, shard_id, and part_id
    score: [u8; 32],
    /// The number of times we have already queried this host for this part
    /// TODO: consider storing this on disk, so we can remember who hasn't
    /// been able to provide us with the parts across restarts
    num_requests: usize,
}

impl Ord for StatePartHost {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // std::collections:BinaryHeap used in PeerPartSelector is a max-heap.
        // We prefer hosts with the least num_requests, after which we break
        // ties according to the priority score and the peer_id.
        self.num_requests
            .cmp(&other.num_requests)
            .reverse()
            .then_with(|| self.score.cmp(&other.score))
            .then_with(|| self.peer_id.cmp(&other.peer_id))
    }
}

impl PartialOrd for StatePartHost {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl StatePartHost {
    fn increment_num_requests(&mut self) {
        self.num_requests += 1;
    }
}

#[derive(Default)]
struct PartPeerSelector {
    /// Ordered collection of available hosts for some desired state part
    peers: BinaryHeap<StatePartHost>,
}

impl PartPeerSelector {
    fn next(&mut self) -> Option<PeerId> {
        match self.peers.pop() {
            Some(mut p) => {
                p.increment_num_requests();
                let peer_id = p.peer_id.clone();
                self.peers.push(p);
                Some(peer_id)
            }
            None => None,
        }
    }

    fn insert_peers<T: IntoIterator<Item = StatePartHost>>(&mut self, peers: T) {
        self.peers.extend(peers);
    }

    fn len(&self) -> usize {
        self.peers.len()
    }

    fn tried_everybody(&self) -> bool {
        self.peers.iter().all(|priority| priority.num_requests > 0)
    }

    fn peer_set(&self) -> HashSet<PeerId> {
        self.peers.iter().map(|p| p.peer_id.clone()).collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Epoch {
    /// The hash for the most recent active state sync
    sync_hash: CryptoHash,
    /// The current epoch height for which we discovered a new sync hash
    epoch_height: EpochHeight,
}

struct Inner {
    /// The latest known SnapshotHostInfo for each node in the network
    hosts: LruCache<PeerId, Arc<SnapshotHostInfo>>,
    /// The current epoch for which the chain head passed the sync hash.
    current_epoch: Option<Epoch>,
    /// Available hosts for the active state sync, by shard
    hosts_for_shard: HashMap<ShardId, HashSet<PeerId>>,
    /// Local data structures used to distribute state part requests among known hosts
    peer_selector: HashMap<(ShardId, u64), PartPeerSelector>,
    /// Batch size for populating the peer_selector from the hosts
    part_selection_cache_batch_size: usize,
}

impl Inner {
    fn is_new(&self, h: &SnapshotHostInfo) -> bool {
        if self.current_epoch.as_ref().is_some_and(|current| current.epoch_height > h.epoch_height)
        {
            return false;
        }
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
        self.insert(&d);
        Some(d)
    }

    /// Ingests a new SnapshotHostInfo into the cache
    /// assumes that the SnapshotHostInfo is valid and new
    fn insert(&mut self, d: &Arc<SnapshotHostInfo>) {
        // If it does not know the current epoch, no need to update the cache, it will be rebuilt later when it knows the current epoch.
        if self.current_epoch.as_ref().is_some_and(|current| current.epoch_height == d.epoch_height)
        {
            for shard_id in &d.shards {
                self.hosts_for_shard
                    .entry(*shard_id)
                    .or_insert(HashSet::default())
                    .insert(d.peer_id.clone());
            }
        }
        self.hosts.push(d.peer_id.clone(), d.clone());
    }

    /// Updates the current epoch and clears the internal state if the sync hash has changed.
    /// This function should be called based on the chain head height.
    fn update_current_epoch(&mut self, epoch_height: &EpochHeight, sync_hash: &CryptoHash) {
        let new_epoch = Epoch { sync_hash: *sync_hash, epoch_height: *epoch_height };
        if self.current_epoch.as_ref() == Some(&new_epoch) {
            return;
        }

        self.current_epoch = Some(new_epoch);
        self.hosts_for_shard.clear();
        self.peer_selector.clear();
        // Only keep info about most recent epochs defined by EPOCH_RETENTION_WINDOW
        let mut new_hosts = LruCache::new(NonZeroUsize::new(self.hosts.cap().get()).unwrap());
        // Build current epoch's cache by keeping only the hosts that are still valid.
        // Lock is taken so the loop will eventually terminate when all hosts are removed.
        loop {
            let Some((peer_id, info)) = self.hosts.pop_lru() else { break };
            if !(info.epoch_height + EPOCH_RETENTION_WINDOW >= *epoch_height
                && info.sync_hash == *sync_hash)
            {
                continue;
            }
            new_hosts.push(peer_id.clone(), info.clone());
            if info.sync_hash != *sync_hash {
                continue;
            }

            for shard_id in &info.shards {
                self.hosts_for_shard
                    .entry(*shard_id)
                    .or_insert(HashSet::default())
                    .insert(peer_id.clone());
            }
        }
        self.hosts = new_hosts;
    }

    /// Given a state header request produced by the local node,
    /// selects a host to which the request should be routed.
    pub fn select_host_for_header(
        &self,
        sync_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Option<PeerId> {
        if self.current_epoch.as_ref().is_none_or(|current| current.sync_hash != *sync_hash) {
            tracing::info!(target: "network", ?sync_hash, current_epoch = ?self.current_epoch, "snapshot hosts cache is not up to date, skipping peer selection");
            return None;
        }

        self.hosts_for_shard.get(&shard_id)?.iter().choose(&mut thread_rng()).cloned()
    }

    /// Given a state part request produced by the local node,
    /// selects a host to which the request should be routed.
    pub fn select_host_for_part(
        &mut self,
        sync_hash: &CryptoHash,
        shard_id: ShardId,
        part_id: u64,
    ) -> Option<PeerId> {
        if self.current_epoch.as_ref().is_none_or(|current| current.sync_hash != *sync_hash) {
            tracing::info!(target: "network", ?sync_hash, current_epoch = ?self.current_epoch, "snapshot hosts cache is not up to date, skipping peer selection");
            return None;
        }

        let selector =
            self.peer_selector.entry((shard_id, part_id)).or_insert(PartPeerSelector::default());

        // Insert more hosts into the selector if needed
        let available_hosts = self.hosts_for_shard.get(&shard_id)?;
        if selector.tried_everybody() && selector.len() < available_hosts.len() {
            let mut new_peers = BinaryHeap::new();
            let already_included = selector.peer_set();

            for peer_id in available_hosts {
                if already_included.contains(peer_id) {
                    continue;
                }

                let score = priority_score(peer_id, shard_id, part_id);

                // Wrap entries with `Reverse` so that we pop the *least* desirable options
                new_peers.push(std::cmp::Reverse(StatePartHost {
                    peer_id: peer_id.clone(),
                    score,
                    num_requests: 0,
                }));

                if new_peers.len() > self.part_selection_cache_batch_size {
                    new_peers.pop();
                }
            }

            selector.insert_peers(new_peers.drain().map(|e| e.0));
        }

        let res = selector.next();
        res
    }
}

pub(crate) struct SnapshotHostsCache(Mutex<Inner>);

impl SnapshotHostsCache {
    pub fn new(config: Config) -> Self {
        Self(Mutex::new(Inner {
            hosts: LruCache::new(
                NonZeroUsize::new(config.snapshot_hosts_cache_size as usize).unwrap(),
            ),
            current_epoch: None,
            hosts_for_shard: HashMap::new(),
            peer_selector: HashMap::new(),
            part_selection_cache_batch_size: config.part_selection_cache_batch_size as usize,
        }))
    }

    pub fn set_current_epoch_if_changed(&self, epoch_height: &EpochHeight, sync_hash: &CryptoHash) {
        self.0.lock().update_current_epoch(epoch_height, sync_hash);
    }

    /// Selects new data and verifies the signatures.
    /// Returns the verified new data and an optional error.
    /// Note that even if error has been returned the partially validated output is returned anyway.
    async fn verify(
        &self,
        data: Vec<Arc<SnapshotHostInfo>>,
    ) -> (Vec<Arc<SnapshotHostInfo>>, Option<SnapshotHostInfoError>) {
        // Filter out any data which is invalid, outdated or which we already have.
        if data.iter().map(|d| d.peer_id.clone()).collect::<HashSet<_>>().len() != data.len() {
            return (vec![], Some(SnapshotHostInfoError::DuplicatePeerId));
        }
        let new_data = {
            let inner = self.0.lock();
            data.into_iter().filter(|d| inner.is_new(d)).collect_vec()
        };
        // Verify the signatures in parallel.
        // Verification will stop at the first encountered error.
        let (data, verification_result) = concurrency::rayon::run(move || {
            concurrency::rayon::try_map_result(new_data.into_iter().par_bridge(), |d| {
                match d.verify() {
                    Ok(()) => Ok(d),
                    Err(err) => Err(err),
                }
            })
        })
        .await;
        match verification_result {
            Ok(()) => (data, None),
            Err(err) => (data, Some(SnapshotHostInfoError::VerificationError(err))),
        }
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
        if data.is_empty() {
            return (vec![], err);
        }
        // Insert the successfully verified data.
        let mut inner = self.0.lock();
        data.iter().for_each(|d| inner.insert(d));
        (data, err)
    }

    /// Skips signature verification. Used only for the local node's own information.
    pub fn insert_skip_verify(self: &Self, my_info: Arc<SnapshotHostInfo>) {
        let _ = self.0.lock().try_insert(my_info);
    }

    pub fn get_hosts(&self) -> Vec<Arc<SnapshotHostInfo>> {
        self.0.lock().hosts.iter().map(|(_, v)| v.clone()).collect()
    }

    pub(crate) fn get_host_info(&self, peer_id: &PeerId) -> Option<Arc<SnapshotHostInfo>> {
        self.0.lock().hosts.peek(peer_id).cloned()
    }

    /// Given a state header request, selects a peer host to which the request should be sent.
    pub fn select_host_for_header(
        &self,
        sync_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Option<PeerId> {
        self.0.lock().select_host_for_header(sync_hash, shard_id)
    }

    /// Given a state part request, selects a peer host to which the request should be sent.
    pub fn select_host_for_part(
        &self,
        sync_hash: &CryptoHash,
        shard_id: ShardId,
        part_id: u64,
    ) -> Option<PeerId> {
        self.0.lock().select_host_for_part(sync_hash, shard_id, part_id)
    }

    /// Triggered by state sync actor after processing a state part.
    pub fn part_received(&self, shard_id: ShardId, part_id: u64) {
        let mut inner = self.0.lock();
        inner.peer_selector.remove(&(shard_id, part_id));
    }

    #[cfg(test)]
    pub(crate) fn has_selector(&self, shard_id: ShardId, part_id: u64) -> bool {
        let inner = self.0.lock();
        inner.peer_selector.contains_key(&(shard_id, part_id))
    }
}
