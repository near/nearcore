
use std::sync::RwLock;
use log::debug;

use near_network::PeerInfo;

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

/// Various status sync can be in, whether it's fast sync or archival.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SyncStatus {
    /// Initial State (we don't not yet what state we should be).
    Initial,
    /// Not syncing / Done syncing.
    NoSync,
    /// Not enought peers to do anything yet.
    AwaitingPeers,
    /// Downloading block headers for fast sync.
    HeaderSync,
    /// Downloading state for fasy sync.
    StateDownload,
    /// Validating the full state.
    StateValidation,
    /// Finalizing state sync.
    StateDone,
    /// Catch up on blocks.
    BodySync,
}

/// Current sync state.
pub struct SyncState {
    current: RwLock<SyncStatus>,
}

impl SyncState {
    /// Return a new SyncState initialized with Initial state.
    pub fn new() -> Self {
        SyncState { current: RwLock::new(SyncStatus::Initial) }
    }

    /// Whether the current state matches any active syncing operation.
    pub fn is_syncing(&self) -> bool {
        self.current.read().expect(POISONED_LOCK_ERR) != SyncStatus::NoSync
    }

    /// Current syncing status.
    pub fn status(&self) -> SyncStatus {
        self.current.read().expect(POISONED_LOCK_ERR)
    }

    /// Update the syncing status.
    pub fn update(&self, new_status: SyncStatus) {
        if self.status() == new_status {
            return;
        }

        let mut status = self.current.write().expect(POISONED_LOCK_ERR);
        debug!(target: "sync", "Sync state: {:?} -> {:?}", *status, new_status);
        *status = new_status;
    }
}

pub struct Syncer {
    sync_state: Arc<SyncState>,
}

impl Syncer {
    pub fn new(sync_state: Arc<SyncState>) -> Self {
        Self { sync_state }
    }

    pub fn peer_connected(&self, peer_info: PeerInfo) {
        // TODO: also check enough peers.
        if self.sync_state.status() == SyncStatus::AwaitingPeers {
            // TODO: figure out how much weight peers have.
        }
    }
}
