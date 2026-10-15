//! Cache of SnapshotHostInfos.
//!
//! Each node in the network which is willing to generate and serve state snapshots
//! publishes a SnapshotHostInfo once per epoch. The info is flooded to all nodes
//! in the network and stored locally in this cache.
//!
//! The cache has a fixed number of slots. A host keeps its slot for as long as it keeps
//! publishing. This node's own info and the infos of validators' nodes don't count
//! toward the limit and are always admitted.
//!
//! Infos are kept only for epochs close to that of the latest block header, which the client
//! reports once header sync has caught up. Until the first report arrives, infos for any
//! epoch are accepted.

use crate::concurrency;
use crate::network_protocol::SnapshotHostInfo;
use crate::network_protocol::SnapshotHostInfoVerificationError;
use itertools::Itertools;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::state_part::StatePartIndex;
use near_primitives::types::EpochHeight;
use near_primitives::types::ShardId;
use parking_lot::Mutex;
use rand::prelude::IteratorRandom;
use rand::thread_rng;
use rayon::iter::ParallelBridge;
use sha2::{Digest, Sha256};
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;

#[cfg(test)]
mod tests;

/// The number of older epochs to retain snapshot host infos for.
pub const STATE_SNAPSHOT_INFO_RETENTION_WINDOW: EpochHeight = 1;

/// The number of epochs past that of the latest block header for which snapshot host infos
/// are accepted. A host may reach an epoch boundary shortly before this node's latest header
/// does, so one epoch of slack is allowed.
pub const STATE_SNAPSHOT_INFO_MAX_EPOCHS_AHEAD: EpochHeight = 1;

#[derive(thiserror::Error, Debug, PartialEq, Eq, Clone)]
pub(crate) enum SnapshotHostInfoError {
    #[error("found multiple entries for the same peer_id")]
    DuplicatePeerId,
    #[error(transparent)]
    VerificationError(#[from] SnapshotHostInfoVerificationError),
}

#[derive(Clone)]
pub struct Config {
    /// The maximum number of SnapshotHostInfos to store locally, not counting validators.
    pub snapshot_hosts_cache_size: u32,
    /// The number of hosts we'll add to structures related to state part peer
    /// selection each time we need to request parts from a new peer
    pub part_selection_cache_batch_size: u32,
}

/// When multiple hosts offer the same part, this hash is compared
/// to determine the order in which to query them. All nodes
/// use the same hashing scheme, resulting in a rough consensus on
/// which hosts serve requests for which parts.
pub(crate) fn priority_score(
    peer_id: &PeerId,
    shard_id: ShardId,
    part_idx: StatePartIndex,
) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(peer_id.public_key().key_data());
    h.update(shard_id.to_le_bytes());
    h.update(part_idx.to_le_bytes());
    h.finalize().into()
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct StatePartHost {
    /// A peer host for some desired state part
    peer_id: PeerId,
    /// Priority score computed over the peer_id, shard_id, and part_idx
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

/// The kind of node a SnapshotHostInfo belongs to.
#[derive(Clone, Copy, Debug)]
enum HostKind {
    /// This node.
    Own,
    /// A validator's node.
    Validator,
    /// Any other node.
    Other,
}

impl HostKind {
    /// Whether the host takes a reserved slot, which doesn't count toward the limit.
    fn takes_reserved_slot(self) -> bool {
        match self {
            HostKind::Own | HostKind::Validator => true,
            HostKind::Other => false,
        }
    }
}

struct Inner {
    /// The latest known SnapshotHostInfo for each node in the network
    hosts: HashMap<PeerId, Arc<SnapshotHostInfo>>,
    /// Hosts that occupy unreserved slots.
    unreserved_hosts: HashSet<PeerId>,
    /// Hosts that occupy the reserved slots of validators.
    validator_hosts: HashSet<PeerId>,
    /// Number of unreserved slots.
    max_unreserved_hosts: usize,
    /// The current sync hash being actively synced by this node. Used to reset peer selectors when changed.
    /// Updated only by locally-produced sync requests.
    current_state_sync_hash: Option<CryptoHash>,
    /// Epoch of the latest block header, as last reported by the client. Snapshot infos for
    /// epochs outside the window around it are discarded. None until the first report.
    current_epoch_height: Option<EpochHeight>,
    /// Available hosts for the active state sync, by shard
    hosts_for_shard: HashMap<ShardId, HashSet<PeerId>>,
    /// Local data structures used to distribute state part requests among known hosts
    peer_selector: HashMap<(ShardId, StatePartIndex), PartPeerSelector>,
    /// Batch size for populating the peer_selector from the hosts
    part_selection_cache_batch_size: usize,
    /// Epoch retention window
    epoch_retention_window: EpochHeight,
}

impl Inner {
    /// Whether infos for `epoch_height` are kept: at most `epoch_retention_window` epochs older
    /// and STATE_SNAPSHOT_INFO_MAX_EPOCHS_AHEAD epochs newer than the latest block header.
    fn in_epoch_window(&self, epoch_height: EpochHeight) -> bool {
        let Some(current) = self.current_epoch_height else {
            return true;
        };
        current.saturating_sub(self.epoch_retention_window) <= epoch_height
            && epoch_height <= current.saturating_add(STATE_SNAPSHOT_INFO_MAX_EPOCHS_AHEAD)
    }

    fn is_new(&self, h: &SnapshotHostInfo) -> bool {
        if !self.in_epoch_window(h.epoch_height()) {
            return false;
        }
        match self.hosts.get(h.peer_id()) {
            Some(old) if old.freshness() >= h.freshness() => false,
            _ => true,
        }
    }

    fn free_slots(&self) -> usize {
        self.max_unreserved_hosts.saturating_sub(self.unreserved_hosts.len())
    }

    /// Whether an info from `peer_id` can be stored.
    fn has_slot(&self, peer_id: &PeerId, kind: HostKind) -> bool {
        kind.takes_reserved_slot()
            || self.unreserved_hosts.contains(peer_id)
            || self.free_slots() > 0
    }

    /// Inserts d into self.data, if it's new and there is a slot for it.
    /// It returns the newly inserted value (or None if nothing changed).
    /// The returned value should be broadcasted to the network.
    fn try_insert(
        &mut self,
        d: Arc<SnapshotHostInfo>,
        kind: HostKind,
    ) -> Option<Arc<SnapshotHostInfo>> {
        if !self.is_new(&d) || !self.has_slot(d.peer_id(), kind) {
            return None;
        }
        self.insert(&d, kind);
        Some(d)
    }

    /// Adds the peer to the shard-specific caches, but only if the info matches
    /// the sync hash we are currently syncing.
    fn add_to_shard_hosts(&mut self, info: &SnapshotHostInfo) {
        if self.current_state_sync_hash != Some(info.sync_hash()) {
            return;
        }
        for shard_id in info.shards() {
            self.hosts_for_shard.entry(*shard_id).or_default().insert(info.peer_id().clone());
        }
    }

    fn remove_from_shard_hosts(&mut self, peer_id: &PeerId) {
        for hosts in self.hosts_for_shard.values_mut() {
            hosts.remove(peer_id);
        }
    }

    fn remove(&mut self, peer_id: &PeerId) {
        self.hosts.remove(peer_id);
        self.unreserved_hosts.remove(peer_id);
        self.validator_hosts.remove(peer_id);
        self.remove_from_shard_hosts(peer_id);
    }

    /// Inserts a new SnapshotHostInfo into the cache unconditionally:
    /// assumes that the info is valid and new, and that it has a slot
    fn insert(&mut self, d: &Arc<SnapshotHostInfo>, kind: HostKind) {
        self.add_to_shard_hosts(d.as_ref());
        // A renewal can move a host to another kind of slot, e.g. out of an unreserved slot
        // once its node becomes a validator's.
        self.unreserved_hosts.remove(d.peer_id());
        self.validator_hosts.remove(d.peer_id());
        match kind {
            HostKind::Own => {}
            HostKind::Validator => {
                self.validator_hosts.insert(d.peer_id().clone());
            }
            HostKind::Other => {
                self.unreserved_hosts.insert(d.peer_id().clone());
            }
        }
        self.hosts.insert(d.peer_id().clone(), d.clone());
    }

    /// Removes the validators' hosts whose nodes are no longer in `validators`. AccountData
    /// names a single node per validator, so this keeps each validator to one reserved slot.
    fn remove_unlisted_validator_hosts(&mut self, validators: &HashSet<PeerId>) {
        let unlisted: Vec<_> = self
            .validator_hosts
            .iter()
            .filter(|peer_id| !validators.contains(*peer_id))
            .cloned()
            .collect();
        for peer_id in &unlisted {
            self.remove(peer_id);
        }
    }

    /// Updates the current state sync hash. This is called when a local state sync request is initiated.
    /// Resets peer selectors if the sync hash has changed.
    fn update_current_state_sync_hash(&mut self, sync_hash: &CryptoHash) {
        if self.current_state_sync_hash == Some(*sync_hash) {
            return;
        }

        self.current_state_sync_hash = Some(*sync_hash);
        // Reset peer selectors and shard-specific caches for the new sync hash
        self.hosts_for_shard.clear();
        self.peer_selector.clear();

        // Rebuild the shard-specific caches with hosts that match the new sync hash
        let known_hosts: Vec<_> = self.hosts.values().cloned().collect();
        for info in &known_hosts {
            self.add_to_shard_hosts(info.as_ref());
        }
    }

    /// Records the epoch of the latest block header and discards the snapshot infos that fall
    /// outside the new epoch window.
    fn update_current_epoch_height(&mut self, epoch_height: EpochHeight) {
        if self.current_epoch_height == Some(epoch_height) {
            return;
        }

        self.current_epoch_height = Some(epoch_height);

        // Remove snapshot infos that are now outside the window, freeing their slots
        let discarded: Vec<_> = self
            .hosts
            .iter()
            .filter(|(_, info)| !self.in_epoch_window(info.epoch_height()))
            .map(|(peer_id, _)| peer_id.clone())
            .collect();
        for peer_id in &discarded {
            self.remove(peer_id);
        }
    }

    /// Given a state header request produced by the local node,
    /// selects a host to which the request should be routed.
    pub fn select_host_for_header(
        &mut self,
        sync_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Option<PeerId> {
        self.update_current_state_sync_hash(sync_hash);
        self.hosts_for_shard.get(&shard_id)?.iter().choose(&mut thread_rng()).cloned()
    }

    /// Given a state part request produced by the local node,
    /// selects a host to which the request should be routed.
    pub fn select_host_for_part(
        &mut self,
        sync_hash: &CryptoHash,
        shard_id: ShardId,
        part_idx: StatePartIndex,
    ) -> Option<PeerId> {
        self.update_current_state_sync_hash(sync_hash);

        let selector =
            self.peer_selector.entry((shard_id, part_idx)).or_insert(PartPeerSelector::default());

        // Insert more hosts into the selector if needed
        let available_hosts = self.hosts_for_shard.get(&shard_id)?;
        if selector.tried_everybody() && selector.len() < available_hosts.len() {
            let mut new_peers = BinaryHeap::new();
            let already_included = selector.peer_set();

            for peer_id in available_hosts {
                if already_included.contains(peer_id) {
                    continue;
                }

                let score = priority_score(peer_id, shard_id, part_idx);

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
        Self::new_with_epoch_retention_window(config, STATE_SNAPSHOT_INFO_RETENTION_WINDOW)
    }

    pub fn new_with_epoch_retention_window(
        config: Config,
        epoch_retention_window: EpochHeight,
    ) -> Self {
        Self(Mutex::new(Inner {
            hosts: HashMap::new(),
            unreserved_hosts: HashSet::new(),
            validator_hosts: HashSet::new(),
            max_unreserved_hosts: config.snapshot_hosts_cache_size as usize,
            current_state_sync_hash: None,
            current_epoch_height: None,
            hosts_for_shard: HashMap::new(),
            peer_selector: HashMap::new(),
            part_selection_cache_batch_size: config.part_selection_cache_batch_size as usize,
            epoch_retention_window,
        }))
    }

    /// Records the epoch of the latest block header, as reported by the client once header sync
    /// has caught up. Snapshot infos more than the retention window older, or more than
    /// STATE_SNAPSHOT_INFO_MAX_EPOCHS_AHEAD newer, are discarded and no longer accepted.
    pub fn set_current_epoch_height(&self, epoch_height: EpochHeight) {
        self.0.lock().update_current_epoch_height(epoch_height);
    }

    /// Selects new data and verifies the signatures.
    /// Returns the verified new data and an optional error.
    /// Note that even if error has been returned the partially validated output is returned anyway.
    async fn verify(
        &self,
        data: Vec<Arc<SnapshotHostInfo>>,
        validators: &HashSet<PeerId>,
    ) -> (Vec<Arc<SnapshotHostInfo>>, Option<SnapshotHostInfoError>) {
        // Filter out any data which is invalid, outdated or which we already have.
        if data.iter().map(|d| d.peer_id().clone()).collect::<HashSet<_>>().len() != data.len() {
            return (vec![], Some(SnapshotHostInfoError::DuplicatePeerId));
        }
        let new_data = {
            let inner = self.0.lock();
            // Older nodes can't parse a message containing an info without the legacy
            // signature, so such infos are dropped instead of relayed to them. The next
            // release will accept and relay them while still sending the legacy signature,
            // and the release after that will stop sending it. They are dropped without an
            // error so that nodes on this release don't ban peers on the next release.
            // TODO(network-overhaul): Accept them in the next release.
            //
            // Once the free slots are spoken for, infos that would need one are dropped before
            // their signatures are checked: they could not be stored anyway, and this makes a
            // replayed message cost nothing to process again.
            let mut free_slots = inner.free_slots();
            data.into_iter()
                .filter(|d| {
                    !d.shards().is_empty() && d.legacy_signature().is_some() && inner.is_new(d)
                })
                .filter(|d| {
                    // Renewals and validators don't need a free slot.
                    if validators.contains(d.peer_id())
                        || inner.unreserved_hosts.contains(d.peer_id())
                    {
                        return true;
                    }
                    if free_slots == 0 {
                        return false;
                    }
                    free_slots -= 1;
                    true
                })
                .collect_vec()
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
    /// Infos from `validators` take reserved slots.
    /// Returns the data inserted, which is what should be broadcast, and optionally a
    /// verification error.
    /// WriteLock is acquired only for the final update (after verification).
    pub async fn insert(
        self: &Self,
        data: Vec<Arc<SnapshotHostInfo>>,
        validators: &HashSet<PeerId>,
    ) -> (Vec<Arc<SnapshotHostInfo>>, Option<SnapshotHostInfoError>) {
        // Execute verification on the rayon threadpool.
        let (data, err) = self.verify(data, validators).await;
        if data.is_empty() {
            return (vec![], err);
        }
        // Insert the successfully verified data. A concurrent insert may have stored a fresher
        // info for the same host or taken the free slots during verification, so both are
        // checked again. Unlisted validators' hosts are removed under the same lock, so that
        // concurrent inserts carrying different nodes for one validator can't each keep theirs.
        let mut inner = self.0.lock();
        inner.remove_unlisted_validator_hosts(validators);
        let data = data
            .into_iter()
            .filter_map(|d| {
                let kind = if validators.contains(d.peer_id()) {
                    HostKind::Validator
                } else {
                    HostKind::Other
                };
                inner.try_insert(d, kind)
            })
            .collect();
        (data, err)
    }

    /// Skips signature verification. Used only for the local node's own information.
    pub fn insert_skip_verify(self: &Self, my_info: Arc<SnapshotHostInfo>) {
        let _ = self.0.lock().try_insert(my_info, HostKind::Own);
    }

    pub fn get_hosts(&self) -> Vec<Arc<SnapshotHostInfo>> {
        self.0.lock().hosts.values().cloned().collect()
    }

    pub(crate) fn get_host_info(&self, peer_id: &PeerId) -> Option<Arc<SnapshotHostInfo>> {
        self.0.lock().hosts.get(peer_id).cloned()
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
        part_idx: StatePartIndex,
    ) -> Option<PeerId> {
        self.0.lock().select_host_for_part(sync_hash, shard_id, part_idx)
    }

    /// Triggered by state sync actor after processing a state part.
    pub fn part_received(&self, shard_id: ShardId, part_idx: StatePartIndex) {
        let mut inner = self.0.lock();
        inner.peer_selector.remove(&(shard_id, part_idx));
    }

    #[cfg(test)]
    pub(crate) fn has_selector(&self, shard_id: ShardId, part_idx: StatePartIndex) -> bool {
        let inner = self.0.lock();
        inner.peer_selector.contains_key(&(shard_id, part_idx))
    }

    #[cfg(test)]
    pub(crate) fn shard_host_peers(&self) -> HashSet<PeerId> {
        self.0.lock().hosts_for_shard.values().flatten().cloned().collect()
    }
}
