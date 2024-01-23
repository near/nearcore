//! Cache of SnapshotHostInfos.
//!
//! Each node in the network which is willing to generate and serve state snapshots
//! publishes a SnapshotHostInfo once per epoch. The info is flooded to all nodes
//! in the network and stored locally in this cache.

use crate::concurrency;
use crate::network_protocol::SnapshotHostInfo;
use crate::network_protocol::SnapshotHostInfoVerificationError;
use lru::LruCache;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::state_part::PartId;
use near_primitives::types::ShardId;
use parking_lot::Mutex;
use rayon::iter::ParallelBridge;
use sha2::{Digest, Sha256};
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;

#[cfg(test)]
mod tests;

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

pub(crate) fn priority_score(peer_id: &PeerId, part_id: &PartId) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(peer_id.public_key().key_data());
    h.update(part_id.idx.to_le_bytes());
    h.finalize().into()
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PartPriority {
    peer_id: PeerId,
    score: [u8; 32],
    // TODO: consider storing this on disk, so we can remember who hasn't
    // been able to provide us with the parts across restarts
    times_returned: usize,
}

impl PartPriority {
    fn inc(&mut self) {
        self.times_returned += 1;
    }
}

impl PartialOrd for PartPriority {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PartPriority {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.times_returned
            .cmp(&other.times_returned)
            .reverse()
            .then_with(|| self.score.cmp(&other.score).reverse())
            .then_with(|| self.peer_id.cmp(&other.peer_id))
    }
}

impl From<ReversePartPriority> for PartPriority {
    fn from(ReversePartPriority { peer_id, score }: ReversePartPriority) -> Self {
        Self { peer_id, score, times_returned: 0 }
    }
}

// used in insert_part_hosts() to iterate through the list of unseen hosts
// and keep the top N hosts as we go through. We use this struct there instead
// of PartPriority because we need the comparator to be the opposite of what
// it is for that struct
#[derive(Clone, Debug, PartialEq, Eq)]
struct ReversePartPriority {
    peer_id: PeerId,
    score: [u8; 32],
}

impl PartialOrd for ReversePartPriority {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ReversePartPriority {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score.cmp(&other.score).then_with(|| self.peer_id.cmp(&other.peer_id))
    }
}

#[derive(Default)]
struct PartPeerSelector {
    peers: BinaryHeap<PartPriority>,
}

impl PartPeerSelector {
    fn next(&mut self) -> Option<PeerId> {
        match self.peers.pop() {
            Some(mut priority) => {
                priority.inc();
                let peer_id = priority.peer_id.clone();
                self.peers.push(priority);
                Some(peer_id)
            }
            None => None,
        }
    }

    fn insert_peers<T: IntoIterator<Item = PartPriority>>(&mut self, peers: T) {
        self.peers.extend(peers)
    }

    fn len(&self) -> usize {
        self.peers.len()
    }

    fn tried_everybody(&self) -> bool {
        self.peers.iter().all(|priority| priority.times_returned > 0)
    }
}

#[derive(Default)]
struct PeerSelector {
    selectors: HashMap<u64, PartPeerSelector>,
}

impl PeerSelector {
    fn next(&mut self, part_id: &PartId) -> Option<PeerId> {
        self.selectors.entry(part_id.idx).or_default().next()
    }

    fn len(&self, part_id: &PartId) -> usize {
        match self.selectors.get(&part_id.idx) {
            Some(s) => s.len(),
            None => 0,
        }
    }

    fn insert_peers<T: IntoIterator<Item = PartPriority>>(&mut self, part_id: &PartId, peers: T) {
        self.selectors.entry(part_id.idx).or_default().insert_peers(peers);
    }

    fn seen_peers(&self, part_id: &PartId) -> HashSet<PeerId> {
        match self.selectors.get(&part_id.idx) {
            Some(s) => {
                let mut ret = HashSet::new();
                for p in s.peers.iter() {
                    ret.insert(p.peer_id.clone());
                }
                ret
            }
            None => HashSet::new(),
        }
    }

    // have we already returned every peer we know about?
    fn tried_everybody(&self, part_id: &PartId) -> bool {
        match self.selectors.get(&part_id.idx) {
            Some(s) => s.tried_everybody(),
            None => true,
        }
    }

    fn clear(&mut self, part_id: &PartId) {
        self.selectors.remove(&part_id.idx);
    }
}

struct Inner {
    /// The latest known SnapshotHostInfo for each node in the network
    hosts: LruCache<PeerId, Arc<SnapshotHostInfo>>,
    state_part_selectors: HashMap<ShardId, PeerSelector>,
    part_selection_cache_batch_size: usize,
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

    // Try to insert up to max_entries_added more peers into the state part selector for this part ID
    // this will look for the best priority `max_entries_added` peers that we haven't yet added to the set
    // of peers to ask for this part, and will add them to the heap so that we can return one of those next
    // time select_host() is called
    fn insert_part_hosts(
        &mut self,
        sync_hash: &CryptoHash,
        shard_id: ShardId,
        part_id: &PartId,
        max_entries_added: usize,
    ) {
        let selector = self.state_part_selectors.get(&shard_id).unwrap();
        let seen_peers = selector.seen_peers(part_id);

        let mut new_peers = BinaryHeap::new();
        for (peer_id, info) in self.hosts.iter() {
            if seen_peers.contains(peer_id)
                || info.sync_hash != *sync_hash
                || !info.shards.contains(&shard_id)
            {
                continue;
            }
            let score = priority_score(peer_id, part_id);
            if new_peers.len() < max_entries_added {
                new_peers.push(ReversePartPriority { peer_id: peer_id.clone(), score });
            } else {
                if score < new_peers.peek().unwrap().score {
                    new_peers.pop();
                    new_peers.push(ReversePartPriority { peer_id: peer_id.clone(), score });
                }
            }
        }
        let selector = self.state_part_selectors.get_mut(&shard_id).unwrap();
        selector.insert_peers(part_id, new_peers.into_iter().map(Into::into));
    }
}

pub(crate) struct SnapshotHostsCache(Mutex<Inner>);

impl SnapshotHostsCache {
    pub fn new(config: Config) -> Self {
        debug_assert!(config.part_selection_cache_batch_size > 0);
        let hosts = LruCache::new(config.snapshot_hosts_cache_size as usize);
        let state_part_selectors = HashMap::new();
        Self(Mutex::new(Inner {
            hosts,
            state_part_selectors,
            part_selection_cache_batch_size: config.part_selection_cache_batch_size as usize,
        }))
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
        let (data, verification_result) = concurrency::rayon::run(move || {
            concurrency::rayon::try_map_result(new_data.into_values().par_bridge(), |d| {
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

    // Selecs a peer to send the request for this part ID to. Chooses based on a priority score
    // calculated as a hash of the Peer ID plus the part ID, and will return different hosts
    // on subsequent calls, eventually iterating over all valid SnapshotHostInfos we know about
    // TODO: get rid of the dead_code and hook this up to the decentralized state sync
    #[allow(dead_code)]
    pub fn select_host(
        &self,
        sync_hash: &CryptoHash,
        shard_id: ShardId,
        part_id: &PartId,
    ) -> Option<PeerId> {
        let mut inner = self.0.lock();
        let num_hosts = inner.hosts.len();
        let selector = inner.state_part_selectors.entry(shard_id).or_default();

        if selector.tried_everybody(part_id) && selector.len(part_id) < num_hosts {
            let max_entries_added = inner.part_selection_cache_batch_size;
            inner.insert_part_hosts(sync_hash, shard_id, part_id, max_entries_added);
        }
        let selector = inner.state_part_selectors.get_mut(&shard_id).unwrap();
        selector.next(part_id)
    }

    // Lets us know that we have already successfully retrieved this part, and we can free any data
    // associated with it that we were going to use to respond to future calls to select_host()
    // TODO: get rid of the dead_code and hook this up to the decentralized state sync
    #[allow(dead_code)]
    pub fn part_received(&self, _sync_hash: &CryptoHash, shard_id: ShardId, part_id: &PartId) {
        let mut inner = self.0.lock();
        let selector = inner.state_part_selectors.entry(shard_id).or_default();
        selector.clear(part_id);
    }

    // used for testing purposes only to check that we clear state after part_received() is called
    #[allow(dead_code)]
    pub(crate) fn part_peer_state_len(&self, shard_id: ShardId, part_id: &PartId) -> usize {
        let inner = self.0.lock();
        match inner.state_part_selectors.get(&shard_id) {
            Some(s) => s.len(part_id),
            None => 0,
        }
    }
}
