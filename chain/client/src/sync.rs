use near_chain::{ChainStoreAccess, Error};
use std::cmp::min;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::{ops::Add, time::Duration as TimeDuration};

use ansi_term::Color::{Purple, Yellow};
use chrono::{DateTime, Duration, Utc};
use futures::{future, FutureExt};
use log::{debug, error, info, warn};
use rand::seq::{IteratorRandom, SliceRandom};
use rand::{thread_rng, Rng};

use near_chain::{Chain, RuntimeAdapter};
use near_network::types::{AccountOrPeerIdOrHash, NetworkResponses, ReasonForBan};
use near_network::{FullPeerInfo, NetworkAdapter, NetworkRequests};
use near_primitives::block::Tip;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::syncing::get_num_state_parts;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{AccountId, BlockHeight, BlockHeightDelta, EpochId, ShardId};
use near_primitives::utils::to_timestamp;

use cached::{Cached, SizedCache};
use near_client_primitives::types::{
    DownloadStatus, ShardSyncDownload, ShardSyncStatus, SyncStatus,
};

/// Maximum number of block headers send over the network.
pub const MAX_BLOCK_HEADERS: u64 = 512;

/// Maximum number of block header hashes to send as part of a locator.
pub const MAX_BLOCK_HEADER_HASHES: usize = 20;

const BLOCK_REQUEST_TIMEOUT: i64 = 2;

/// Maximum number of state parts to request per peer on each round when node is trying to download the state.
pub const MAX_STATE_PART_REQUEST: u64 = 16;
/// Number of state parts already requested stored as pending.
/// This number should not exceed MAX_STATE_PART_REQUEST times (number of peers in the network).
pub const MAX_PENDING_PART: u64 = MAX_STATE_PART_REQUEST * 10000;

pub const NS_PER_SECOND: u128 = 1_000_000_000;

/// Get random peer from the hightest height peers.
pub fn highest_height_peer(highest_height_peers: &Vec<FullPeerInfo>) -> Option<FullPeerInfo> {
    if highest_height_peers.len() == 0 {
        return None;
    }

    match highest_height_peers.iter().choose(&mut thread_rng()) {
        None => highest_height_peers.choose(&mut thread_rng()).cloned(),
        Some(peer) => Some(peer.clone()),
    }
}

/// Helper to keep track of the Epoch Sync
// TODO #3488
#[allow(dead_code)]
pub struct EpochSync {
    network_adapter: Arc<dyn NetworkAdapter>,
    /// Datastructure to keep track of when the last request to each peer was made.
    /// Peers do not respond to Epoch Sync requests more frequently than once per a certain time
    /// interval, thus there's no point in requesting more frequently.
    peer_to_last_request_time: HashMap<PeerId, DateTime<Utc>>,
    /// Tracks all the peers who have reported that we are already up to date
    peers_reporting_up_to_date: HashSet<PeerId>,
    /// The last epoch we are synced to
    current_epoch_id: EpochId,
    /// The next epoch id we need to sync
    next_epoch_id: EpochId,
    /// The block producers set to validate the light client block view for the next epoch
    next_block_producers: Vec<ValidatorStake>,
    /// The last epoch id that we have requested
    requested_epoch_id: EpochId,
    /// When and to whom was the last request made
    last_request_time: DateTime<Utc>,
    last_request_peer_id: Option<PeerId>,

    /// How long to wait for a response before re-requesting the same light client block view
    request_timeout: Duration,
    /// How frequently to send request to the same peer
    peer_timeout: Duration,

    /// True, if all peers agreed that we're at the last Epoch.
    /// Only finalization is needed.
    have_all_epochs: bool,
    /// Whether the Epoch Sync was performed to completion previously.
    /// Current state machine allows for only one Epoch Sync.
    pub done: bool,

    pub sync_hash: CryptoHash,

    received_epoch: bool,

    is_just_started: bool,
}

impl EpochSync {
    pub fn new(
        network_adapter: Arc<dyn NetworkAdapter>,
        genesis_epoch_id: EpochId,
        genesis_next_epoch_id: EpochId,
        first_epoch_block_producers: Vec<ValidatorStake>,
        request_timeout: TimeDuration,
        peer_timeout: TimeDuration,
    ) -> Self {
        Self {
            network_adapter,
            peer_to_last_request_time: HashMap::new(),
            peers_reporting_up_to_date: HashSet::new(),
            current_epoch_id: genesis_epoch_id.clone(),
            next_epoch_id: genesis_next_epoch_id.clone(),
            next_block_producers: first_epoch_block_producers,
            requested_epoch_id: genesis_epoch_id,
            last_request_time: Utc::now(),
            last_request_peer_id: None,
            request_timeout: Duration::from_std(request_timeout).unwrap(),
            peer_timeout: Duration::from_std(peer_timeout).unwrap(),
            received_epoch: false,
            have_all_epochs: false,
            done: false,
            sync_hash: CryptoHash::default(),
            is_just_started: true,
        }
    }
}

/// Helper to keep track of sync headers.
/// Handles major re-orgs by finding closest header that matches and re-downloading headers from that point.
pub struct HeaderSync {
    network_adapter: Arc<dyn NetworkAdapter>,
    history_locator: Vec<(BlockHeight, CryptoHash)>,
    prev_header_sync: (DateTime<Utc>, BlockHeight, BlockHeight, BlockHeight),
    syncing_peer: Option<FullPeerInfo>,
    stalling_ts: Option<DateTime<Utc>>,

    initial_timeout: Duration,
    progress_timeout: Duration,
    stall_ban_timeout: Duration,
    expected_height_per_second: u64,
}

impl HeaderSync {
    pub fn new(
        network_adapter: Arc<dyn NetworkAdapter>,
        initial_timeout: TimeDuration,
        progress_timeout: TimeDuration,
        stall_ban_timeout: TimeDuration,
        expected_height_per_second: u64,
    ) -> Self {
        HeaderSync {
            network_adapter,
            history_locator: vec![],
            prev_header_sync: (Utc::now(), 0, 0, 0),
            syncing_peer: None,
            stalling_ts: None,
            initial_timeout: Duration::from_std(initial_timeout).unwrap(),
            progress_timeout: Duration::from_std(progress_timeout).unwrap(),
            stall_ban_timeout: Duration::from_std(stall_ban_timeout).unwrap(),
            expected_height_per_second,
        }
    }

    pub fn run(
        &mut self,
        sync_status: &mut SyncStatus,
        chain: &mut Chain,
        highest_height: BlockHeight,
        highest_height_peers: &Vec<FullPeerInfo>,
    ) -> Result<(), near_chain::Error> {
        let header_head = chain.header_head()?;
        if !self.header_sync_due(sync_status, &header_head, highest_height) {
            return Ok(());
        }

        let enable_header_sync = match sync_status {
            SyncStatus::HeaderSync { .. }
            | SyncStatus::BodySync { .. }
            | SyncStatus::StateSyncDone => true,
            SyncStatus::NoSync | SyncStatus::AwaitingPeers | SyncStatus::EpochSync { .. } => {
                debug!(target: "sync", "Sync: initial transition to Header sync. Header head {} at {}",
                    header_head.last_block_hash, header_head.height,
                );
                self.history_locator.retain(|&x| x.0 == 0);
                true
            }
            SyncStatus::StateSync { .. } => false,
        };

        if enable_header_sync {
            *sync_status =
                SyncStatus::HeaderSync { current_height: header_head.height, highest_height };
            self.syncing_peer = None;
            if let Some(peer) = highest_height_peer(&highest_height_peers) {
                if peer.chain_info.height > header_head.height {
                    self.syncing_peer = self.request_headers(chain, peer);
                }
            }
        }

        Ok(())
    }

    fn compute_expected_height(
        &self,
        old_height: BlockHeight,
        time_delta: Duration,
    ) -> BlockHeight {
        (old_height as u128
            + (time_delta.num_nanoseconds().unwrap() as u128
                * self.expected_height_per_second as u128
                / NS_PER_SECOND)) as u64
    }

    fn header_sync_due(
        &mut self,
        sync_status: &SyncStatus,
        header_head: &Tip,
        highest_height: BlockHeight,
    ) -> bool {
        let now = Utc::now();
        let (timeout, old_expected_height, prev_height, prev_highest_height) =
            self.prev_header_sync;

        // Received all necessary header, can request more.
        let all_headers_received =
            header_head.height >= min(prev_height + MAX_BLOCK_HEADERS - 4, prev_highest_height);

        // Did we receive as many headers as we expected from the peer? Request more or ban peer.
        let stalling = header_head.height <= old_expected_height && now > timeout;

        // Always enable header sync on initial state transition from NoSync / NoSyncFewBlocksBehind / AwaitingPeers.
        let force_sync = match sync_status {
            SyncStatus::NoSync | SyncStatus::AwaitingPeers => true,
            _ => false,
        };

        if force_sync || all_headers_received || stalling {
            self.prev_header_sync = (
                now + self.initial_timeout,
                self.compute_expected_height(header_head.height, self.initial_timeout),
                header_head.height,
                highest_height,
            );

            if stalling {
                if self.stalling_ts.is_none() {
                    self.stalling_ts = Some(now);
                }
            } else {
                self.stalling_ts = None;
            }

            if all_headers_received {
                self.stalling_ts = None;
            } else {
                if let Some(ref stalling_ts) = self.stalling_ts {
                    if let Some(ref peer) = self.syncing_peer {
                        match sync_status {
                            SyncStatus::HeaderSync { highest_height, .. } => {
                                if now > *stalling_ts + self.stall_ban_timeout
                                    && *highest_height == peer.chain_info.height
                                {
                                    warn!(target: "sync", "Sync: ban a fraudulent peer: {}, claimed height: {}",
                                        peer.peer_info, peer.chain_info.height);
                                    self.network_adapter.do_send(NetworkRequests::BanPeer {
                                        peer_id: peer.peer_info.id.clone(),
                                        ban_reason: ReasonForBan::HeightFraud,
                                    });
                                    // This peer is fraudulent, let's skip this beat and wait for
                                    // the next one when this peer is not in the list anymore.
                                    self.syncing_peer = None;
                                    return false;
                                }
                            }
                            _ => (),
                        }
                    }
                }
            }
            self.syncing_peer = None;
            true
        } else {
            // Resetting the timeout as long as we make progress.
            let ns_time_till_timeout =
                (to_timestamp(timeout).saturating_sub(to_timestamp(now))) as u128;
            let remaining_expected_height = (self.expected_height_per_second as u128
                * ns_time_till_timeout
                / NS_PER_SECOND) as u64;
            if header_head.height >= old_expected_height.saturating_sub(remaining_expected_height) {
                let new_expected_height =
                    self.compute_expected_height(header_head.height, self.progress_timeout);
                self.prev_header_sync = (
                    now + self.progress_timeout,
                    new_expected_height,
                    prev_height,
                    prev_highest_height,
                );
            }
            false
        }
    }

    /// Request headers from a given peer to advance the chain.
    fn request_headers(&mut self, chain: &mut Chain, peer: FullPeerInfo) -> Option<FullPeerInfo> {
        if let Ok(locator) = self.get_locator(chain) {
            debug!(target: "sync", "Sync: request headers: asking {} for headers, {:?}", peer.peer_info.id, locator);
            self.network_adapter.do_send(NetworkRequests::BlockHeadersRequest {
                hashes: locator,
                peer_id: peer.peer_info.id.clone(),
            });
            return Some(peer);
        }
        None
    }

    fn get_locator(&mut self, chain: &mut Chain) -> Result<Vec<CryptoHash>, near_chain::Error> {
        let tip = chain.header_head()?;
        let genesis_height = chain.genesis().height();
        let heights = get_locator_heights(tip.height - genesis_height)
            .into_iter()
            .map(|h| h + genesis_height)
            .collect::<Vec<_>>();

        // For each height we need, we either check if something is close enough from last locator, or go to the db.
        let mut locator: Vec<(u64, CryptoHash)> = vec![(tip.height, tip.last_block_hash)];
        for h in heights {
            if let Some(x) = close_enough(&self.history_locator, h) {
                locator.push(x);
            } else {
                // Walk backwards to find last known hash.
                let last_loc = locator.last().unwrap().clone();
                if let Ok(header) = chain.get_header_by_height(h) {
                    if header.height() != last_loc.0 {
                        locator.push((header.height(), *header.hash()));
                    }
                }
            }
        }
        locator.dedup_by(|a, b| a.0 == b.0);
        debug!(target: "sync", "Sync: locator: {:?}", locator);
        self.history_locator = locator.clone();
        Ok(locator.iter().map(|x| x.1).collect())
    }
}

/// Check if there is a close enough value to provided height in the locator.
fn close_enough(locator: &Vec<(u64, CryptoHash)>, height: u64) -> Option<(u64, CryptoHash)> {
    if locator.len() == 0 {
        return None;
    }
    // Check boundaries, if lower than the last.
    if locator.last().unwrap().0 >= height {
        return locator.last().map(|x| x.clone());
    }
    // Higher than first and first is within acceptable gap.
    if locator[0].0 < height && height.saturating_sub(127) < locator[0].0 {
        return Some(locator[0]);
    }
    for h in locator.windows(2) {
        if height <= h[0].0 && height > h[1].0 {
            if h[0].0 - height < height - h[1].0 {
                return Some(h[0].clone());
            } else {
                return Some(h[1].clone());
            }
        }
    }
    None
}

/// Given height stepping back to 0 in powers of 2 steps.
fn get_locator_heights(height: u64) -> Vec<u64> {
    let mut current = height;
    let mut heights = vec![];
    while current > 0 {
        heights.push(current);
        if heights.len() >= MAX_BLOCK_HEADER_HASHES as usize - 1 {
            break;
        }
        let next = 2u64.pow(heights.len() as u32);
        current = if current > next { current - next } else { 0 };
    }
    heights.push(0);
    heights
}

pub struct BlockSyncRequest {
    height: BlockHeight,
    hash: CryptoHash,
    when: DateTime<Utc>,
}

/// Helper to track block syncing.
pub struct BlockSync {
    network_adapter: Arc<dyn NetworkAdapter>,
    last_request: Option<BlockSyncRequest>,
    /// How far to fetch blocks vs fetch state.
    block_fetch_horizon: BlockHeightDelta,
    /// Whether to enforce block sync
    archive: bool,
}

impl BlockSync {
    pub fn new(
        network_adapter: Arc<dyn NetworkAdapter>,
        block_fetch_horizon: BlockHeightDelta,
        archive: bool,
    ) -> Self {
        BlockSync { network_adapter, last_request: None, block_fetch_horizon, archive }
    }

    /// Runs check if block sync is needed, if it's needed and it's too far - sync state is started instead (returning true).
    /// Otherwise requests recent blocks from peers.
    pub fn run(
        &mut self,
        sync_status: &mut SyncStatus,
        chain: &mut Chain,
        highest_height: BlockHeight,
        highest_height_peers: &[FullPeerInfo],
    ) -> Result<bool, near_chain::Error> {
        if self.block_sync_due(chain)? {
            if self.block_sync(chain, highest_height_peers)? {
                debug!(target: "sync", "Sync: transition to State Sync.");
                return Ok(true);
            }
        }

        let head = chain.head()?;
        *sync_status = SyncStatus::BodySync { current_height: head.height, highest_height };
        Ok(false)
    }

    /// Check if state download is required
    fn check_state_needed(&self, chain: &Chain) -> Result<bool, near_chain::Error> {
        let head = chain.head()?;
        let header_head = chain.header_head()?;

        // If latest block is up to date return early.
        // No state download is required, neither any blocks need to be fetched.
        if head.height >= header_head.height {
            return Ok(false);
        }

        // Don't run State Sync if header head is not more than one epoch ahead.
        if head.epoch_id != header_head.epoch_id && head.next_epoch_id != header_head.epoch_id {
            if head.height < header_head.height.saturating_sub(self.block_fetch_horizon)
                && !self.archive
            {
                // Epochs are different and we are too far from horizon, State Sync is needed
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Returns true if state download is required (last known block is too far).
    /// Otherwise request recent blocks from peers round robin.
    pub fn block_sync(
        &mut self,
        chain: &mut Chain,
        highest_height_peers: &[FullPeerInfo],
    ) -> Result<bool, near_chain::Error> {
        if self.check_state_needed(chain)? {
            return Ok(true);
        }

        let reference_hash = match &self.last_request {
            Some(request) if chain.is_chunk_orphan(&request.hash) => request.hash,
            _ => chain.head()?.last_block_hash,
        };

        let reference_hash = {
            // Find the most recent block we know on the canonical chain.
            // In practice the forks from the last final block are very short, so it is
            // acceptable to perform this on each request
            let header = chain.get_block_header(&reference_hash)?;
            let mut candidate = (header.height(), *header.hash(), *header.prev_hash());

            // First go back until we find the common block
            while match chain.get_header_by_height(candidate.0) {
                Ok(header) => header.hash() != &candidate.1,
                Err(e) => match e.kind() {
                    near_chain::ErrorKind::DBNotFoundErr(_) => true,
                    _ => return Err(e),
                },
            } {
                let prev_header = chain.get_block_header(&candidate.2)?;
                candidate = (prev_header.height(), *prev_header.hash(), *prev_header.prev_hash());
            }

            // Then go forward for as long as we known the next block
            let mut ret_hash = candidate.1;
            loop {
                match chain.mut_store().get_next_block_hash(&ret_hash) {
                    Ok(hash) => {
                        let hash = hash.clone();
                        if chain.block_exists(&hash)? {
                            ret_hash = hash;
                        } else {
                            break;
                        }
                    }
                    Err(e) => match e.kind() {
                        near_chain::ErrorKind::DBNotFoundErr(_) => break,
                        _ => return Err(e),
                    },
                }
            }

            ret_hash
        };

        let next_hash = match chain.mut_store().get_next_block_hash(&reference_hash) {
            Ok(hash) => *hash,
            Err(e) => match e.kind() {
                near_chain::ErrorKind::DBNotFoundErr(_) => {
                    return Ok(false);
                }
                _ => return Err(e),
            },
        };
        let next_height = chain.get_block_header(&next_hash)?.height();

        let request = BlockSyncRequest { height: next_height, hash: next_hash, when: Utc::now() };

        let head = chain.head()?;
        let header_head = chain.header_head()?;

        debug!(target: "sync", "Block sync: {}/{} requesting block {} from {} peers", head.height, header_head.height, next_hash, highest_height_peers.len());

        let gc_stop_height = chain.runtime_adapter.get_gc_stop_height(&header_head.last_block_hash);

        let request_from_archival = self.archive && request.height < gc_stop_height;
        let peer = if request_from_archival {
            let archival_peer_iter = highest_height_peers.iter().filter(|p| p.chain_info.archival);
            archival_peer_iter.choose(&mut rand::thread_rng())
        } else {
            let peer_iter = highest_height_peers.iter();
            peer_iter.choose(&mut rand::thread_rng())
        };

        if let Some(peer) = peer {
            self.network_adapter.do_send(NetworkRequests::BlockRequest {
                hash: request.hash,
                peer_id: peer.peer_info.id.clone(),
            });
        }

        self.last_request = Some(request);

        Ok(false)
    }

    /// Check if we should run block body sync and ask for more full blocks.
    fn block_sync_due(&mut self, chain: &Chain) -> Result<bool, near_chain::Error> {
        match &self.last_request {
            None => Ok(true),
            Some(request) => Ok(chain.head()?.height >= request.height
                || chain.is_chunk_orphan(&request.hash)
                || Utc::now() - request.when > Duration::seconds(BLOCK_REQUEST_TIMEOUT)),
        }
    }
}

pub enum StateSyncResult {
    /// No shard has changed its status
    Unchanged,
    /// At least one shard has changed its status
    /// Boolean parameter specifies whether the client needs to start fetching the block
    Changed(bool),
    /// The state for all shards was downloaded.
    Completed,
}

struct PendingRequestStatus {
    missing_parts: usize,
    wait_until: DateTime<Utc>,
}

impl PendingRequestStatus {
    fn new(timeout: Duration) -> Self {
        Self { missing_parts: 1, wait_until: Utc::now().add(timeout) }
    }
    fn expired(&self) -> bool {
        Utc::now() > self.wait_until
    }
}

/// Helper to track state sync.
pub struct StateSync {
    network_adapter: Arc<dyn NetworkAdapter>,

    state_sync_time: HashMap<ShardId, DateTime<Utc>>,
    last_time_block_requested: Option<DateTime<Utc>>,

    last_part_id_requested: HashMap<(AccountOrPeerIdOrHash, ShardId), PendingRequestStatus>,
    /// Map from which part we requested to whom.
    requested_target: SizedCache<(u64, CryptoHash), AccountOrPeerIdOrHash>,

    timeout: Duration,
}

impl StateSync {
    pub fn new(network_adapter: Arc<dyn NetworkAdapter>, timeout: TimeDuration) -> Self {
        StateSync {
            network_adapter,
            state_sync_time: Default::default(),
            last_time_block_requested: None,
            last_part_id_requested: Default::default(),
            requested_target: SizedCache::with_size(MAX_PENDING_PART as usize),
            timeout: Duration::from_std(timeout).unwrap(),
        }
    }

    pub fn sync_block_status(
        &mut self,
        prev_hash: &CryptoHash,
        chain: &mut Chain,
        now: DateTime<Utc>,
    ) -> Result<(bool, bool), near_chain::Error> {
        let (request_block, have_block) = if !chain.block_exists(prev_hash)? {
            match self.last_time_block_requested {
                None => (true, false),
                Some(last_time) => {
                    if now - last_time >= self.timeout {
                        error!(target: "sync", "State sync: block request for {} timed out in {} seconds", prev_hash, self.timeout.num_seconds());
                        (true, false)
                    } else {
                        (false, false)
                    }
                }
            }
        } else {
            self.last_time_block_requested = None;
            (false, true)
        };
        if request_block {
            self.last_time_block_requested = Some(now);
        };
        Ok((request_block, have_block))
    }

    pub fn sync_shards_status(
        &mut self,
        me: &Option<AccountId>,
        sync_hash: CryptoHash,
        new_shard_sync: &mut HashMap<u64, ShardSyncDownload>,
        chain: &mut Chain,
        runtime_adapter: &Arc<dyn RuntimeAdapter>,
        highest_height_peers: &Vec<FullPeerInfo>,
        tracking_shards: Vec<ShardId>,
        now: DateTime<Utc>,
    ) -> Result<(bool, bool), near_chain::Error> {
        let mut all_done = true;
        let mut update_sync_status = false;
        let init_sync_download = ShardSyncDownload {
            downloads: vec![
                DownloadStatus {
                    start_time: now,
                    prev_update_time: now,
                    run_me: Arc::new(AtomicBool::new(true)),
                    error: false,
                    done: false,
                    state_requests_count: 0,
                    last_target: None,
                };
                1
            ],
            status: ShardSyncStatus::StateDownloadHeader,
        };

        for shard_id in tracking_shards {
            let mut download_timeout = false;
            let mut need_shard = false;
            let shard_sync_download = new_shard_sync.entry(shard_id).or_insert_with(|| {
                need_shard = true;
                init_sync_download.clone()
            });
            let mut this_done = false;
            match shard_sync_download.status {
                ShardSyncStatus::StateDownloadHeader => {
                    if shard_sync_download.downloads[0].done {
                        let shard_state_header = chain.get_state_header(shard_id, sync_hash)?;
                        let state_num_parts =
                            get_num_state_parts(shard_state_header.state_root_node().memory_usage);
                        *shard_sync_download = ShardSyncDownload {
                            downloads: vec![
                                DownloadStatus {
                                    start_time: now,
                                    prev_update_time: now,
                                    run_me: Arc::new(AtomicBool::new(true)),
                                    error: false,
                                    done: false,
                                    state_requests_count: 0,
                                    last_target: None,
                                };
                                state_num_parts as usize
                            ],
                            status: ShardSyncStatus::StateDownloadParts,
                        };
                        need_shard = true;
                    } else {
                        let prev = shard_sync_download.downloads[0].prev_update_time;
                        let error = shard_sync_download.downloads[0].error;
                        download_timeout = now - prev > self.timeout;
                        if download_timeout || error {
                            shard_sync_download.downloads[0].run_me.store(true, Ordering::SeqCst);
                            shard_sync_download.downloads[0].error = false;
                            shard_sync_download.downloads[0].prev_update_time = now;
                        }
                        if shard_sync_download.downloads[0].run_me.load(Ordering::SeqCst) {
                            need_shard = true;
                        }
                    }
                }
                ShardSyncStatus::StateDownloadParts => {
                    let mut parts_done = true;
                    for part_download in shard_sync_download.downloads.iter_mut() {
                        if !part_download.done {
                            parts_done = false;
                            let prev = part_download.prev_update_time;
                            let error = part_download.error;
                            let part_timeout = now - prev > self.timeout;
                            if part_timeout || error {
                                download_timeout |= part_timeout;
                                part_download.run_me.store(true, Ordering::SeqCst);
                                part_download.error = false;
                                part_download.prev_update_time = now;
                            }
                            if part_download.run_me.load(Ordering::SeqCst) {
                                need_shard = true;
                            }
                        }
                    }
                    if parts_done {
                        update_sync_status = true;
                        *shard_sync_download = ShardSyncDownload {
                            downloads: vec![],
                            status: ShardSyncStatus::StateDownloadFinalize,
                        };
                    }
                }
                ShardSyncStatus::StateDownloadFinalize => {
                    let shard_state_header = chain.get_state_header(shard_id, sync_hash)?;
                    let state_num_parts =
                        get_num_state_parts(shard_state_header.state_root_node().memory_usage);
                    match chain.set_state_finalize(shard_id, sync_hash, state_num_parts) {
                        Ok(_) => {
                            update_sync_status = true;
                            *shard_sync_download = ShardSyncDownload {
                                downloads: vec![],
                                status: ShardSyncStatus::StateDownloadComplete,
                            }
                        }
                        Err(e) => {
                            // Cannot finalize the downloaded state.
                            // The reasonable behavior here is to start from the very beginning.
                            error!(target: "sync", "State sync finalizing error, shard = {}, hash = {}: {:?}", shard_id, sync_hash, e);
                            update_sync_status = true;
                            *shard_sync_download = init_sync_download.clone();
                            chain.clear_downloaded_parts(shard_id, sync_hash, state_num_parts)?;
                        }
                    }
                }
                ShardSyncStatus::StateDownloadComplete => {
                    this_done = true;
                    let shard_state_header = chain.get_state_header(shard_id, sync_hash)?;
                    let state_num_parts =
                        get_num_state_parts(shard_state_header.state_root_node().memory_usage);
                    chain.clear_downloaded_parts(shard_id, sync_hash, state_num_parts)?;
                }
            }
            all_done &= this_done;

            if download_timeout {
                warn!(target: "sync", "State sync didn't download the state for shard {} in {} seconds, sending StateRequest again", shard_id, self.timeout.num_seconds());
                info!(target: "sync", "State sync status: me {:?}, sync_hash {}, phase {}",
                      me,
                      sync_hash,
                      match shard_sync_download.status {
                          ShardSyncStatus::StateDownloadHeader => format!("{} requests sent {}, last target {:?}",
                                                                          Purple.bold().paint(format!("HEADER")),
                                                                          shard_sync_download.downloads[0].state_requests_count,
                                                                          shard_sync_download.downloads[0].last_target),
                          ShardSyncStatus::StateDownloadParts => { let mut text = "".to_string();
                              for (i, download) in shard_sync_download.downloads.iter().enumerate() {
                                  text.push_str(&format!("[{}: {}, {}, {:?}] ",
                                                         Yellow.bold().paint(i.to_string()),
                                                         download.done,
                                                         download.state_requests_count,
                                                         download.last_target));
                              }
                              format!("{} [{}: is_done, requests sent, last target] {}",
                                      Purple.bold().paint("PARTS"),
                                      Yellow.bold().paint("part_id"),
                                      text)
                          }
                          _ => unreachable!("timeout cannot happen when all state is downloaded"),
                      },
                );
            }

            // Execute syncing for shard `shard_id`
            if need_shard {
                update_sync_status = true;
                *shard_sync_download = self.request_shard(
                    me,
                    shard_id,
                    chain,
                    runtime_adapter,
                    sync_hash,
                    shard_sync_download.clone(),
                    highest_height_peers,
                )?;
            }
        }

        Ok((update_sync_status, all_done))
    }

    /// Find the hash of the first block on the same epoch (and chain) of block with hash `sync_hash`.
    pub fn get_epoch_start_sync_hash(
        chain: &mut Chain,
        sync_hash: &CryptoHash,
    ) -> Result<CryptoHash, near_chain::Error> {
        let mut header = chain.get_block_header(sync_hash)?;
        let mut epoch_id = header.epoch_id().clone();
        let mut hash = header.hash().clone();
        let mut prev_hash = header.prev_hash().clone();
        loop {
            if prev_hash == CryptoHash::default() {
                return Ok(hash);
            }
            header = chain.get_block_header(&prev_hash)?;
            if &epoch_id != header.epoch_id() {
                return Ok(hash);
            }
            epoch_id = header.epoch_id().clone();
            hash = header.hash().clone();
            prev_hash = header.prev_hash().clone();
        }
    }

    fn sent_request_part(
        &mut self,
        target: AccountOrPeerIdOrHash,
        part_id: u64,
        shard_id: ShardId,
        sync_hash: CryptoHash,
    ) {
        self.requested_target.cache_set((part_id, sync_hash), target.clone());

        let timeout = self.timeout;
        self.last_part_id_requested
            .entry((target, shard_id))
            .and_modify(|pending_request| {
                pending_request.missing_parts += 1;
            })
            .or_insert_with(|| PendingRequestStatus::new(timeout));
    }

    pub fn received_requested_part(
        &mut self,
        part_id: u64,
        shard_id: ShardId,
        sync_hash: CryptoHash,
    ) {
        let key = (part_id, sync_hash);
        if let Some(target) = self.requested_target.cache_get(&key) {
            if self.last_part_id_requested.get_mut(&(target.clone(), shard_id)).map_or(
                false,
                |request| {
                    request.missing_parts = request.missing_parts.saturating_sub(1);
                    request.missing_parts == 0
                },
            ) {
                self.last_part_id_requested.remove(&(target.clone(), shard_id));
            }
        }
    }

    /// Find possible targets to download state from.
    /// Candidates are validators at current epoch and peers at highest height.
    /// Only select candidates that we have no pending request currently ongoing.
    fn possible_targets(
        &mut self,
        me: &Option<AccountId>,
        shard_id: ShardId,
        chain: &mut Chain,
        runtime_adapter: &Arc<dyn RuntimeAdapter>,
        sync_hash: CryptoHash,
        highest_height_peers: &Vec<FullPeerInfo>,
    ) -> Result<Vec<AccountOrPeerIdOrHash>, Error> {
        // Remove candidates from pending list if request expired due to timeout
        self.last_part_id_requested.retain(|_, request| !request.expired());

        let prev_block_hash = chain.get_block_header(&sync_hash)?.prev_hash();
        let epoch_hash = runtime_adapter.get_epoch_id_from_prev_block(&prev_block_hash)?;

        Ok(runtime_adapter
            .get_epoch_block_producers_ordered(&epoch_hash, &sync_hash)?
            .iter()
            .filter_map(|(validator_stake, _slashed)| {
                let account_id = validator_stake.account_id();
                if runtime_adapter.cares_about_shard(
                    Some(account_id),
                    &prev_block_hash,
                    shard_id,
                    false,
                ) {
                    if me.as_ref().map(|me| me != account_id).unwrap_or(true) {
                        Some(AccountOrPeerIdOrHash::AccountId(account_id.clone()))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .chain(highest_height_peers.iter().filter_map(|peer| {
                if peer.chain_info.tracked_shards.contains(&shard_id) {
                    Some(AccountOrPeerIdOrHash::PeerId(peer.peer_info.id.clone()))
                } else {
                    None
                }
            }))
            .filter(|candidate| {
                !self.last_part_id_requested.contains_key(&(candidate.clone(), shard_id))
            })
            .collect::<Vec<_>>())
    }

    /// Returns new ShardSyncDownload if successful, otherwise returns given shard_sync_download
    pub fn request_shard(
        &mut self,
        me: &Option<AccountId>,
        shard_id: ShardId,
        chain: &mut Chain,
        runtime_adapter: &Arc<dyn RuntimeAdapter>,
        sync_hash: CryptoHash,
        shard_sync_download: ShardSyncDownload,
        highest_height_peers: &Vec<FullPeerInfo>,
    ) -> Result<ShardSyncDownload, near_chain::Error> {
        let possible_targets = self.possible_targets(
            me,
            shard_id,
            chain,
            runtime_adapter,
            sync_hash,
            highest_height_peers,
        )?;

        if possible_targets.is_empty() {
            return Ok(shard_sync_download);
        }

        // Downloading strategy starts here
        let mut new_shard_sync_download = shard_sync_download.clone();

        match shard_sync_download.status {
            ShardSyncStatus::StateDownloadHeader => {
                let target = possible_targets.choose(&mut thread_rng()).cloned().unwrap();
                assert!(new_shard_sync_download.downloads[0].run_me.load(Ordering::SeqCst));
                new_shard_sync_download.downloads[0].run_me.store(false, Ordering::SeqCst);
                new_shard_sync_download.downloads[0].state_requests_count += 1;
                new_shard_sync_download.downloads[0].last_target = Some(target.clone());
                let run_me = new_shard_sync_download.downloads[0].run_me.clone();
                near_performance_metrics::actix::spawn(
                    std::any::type_name::<Self>(),
                    file!(),
                    line!(),
                    self.network_adapter
                        .send(NetworkRequests::StateRequestHeader { shard_id, sync_hash, target })
                        .then(move |result| {
                            if let Ok(NetworkResponses::RouteNotFound) = result {
                                // Send a StateRequestHeader on the next iteration
                                run_me.store(true, Ordering::SeqCst);
                            }
                            future::ready(())
                        }),
                );
            }
            ShardSyncStatus::StateDownloadParts => {
                let possible_targets_sampler =
                    SamplerLimited::new(possible_targets, MAX_STATE_PART_REQUEST);

                // Iterate over all parts that needs to be requested (i.e. download.run_me is true).
                // Parts are ordered such that its index match its part_id.
                // Finally, for every part that needs to be requested it is selected one peer (target) randomly
                // to request the part from
                for ((part_id, download), target) in new_shard_sync_download
                    .downloads
                    .iter_mut()
                    .enumerate()
                    .filter(|(_, download)| download.run_me.load(Ordering::SeqCst))
                    .zip(possible_targets_sampler)
                {
                    self.sent_request_part(target.clone(), part_id as u64, shard_id, sync_hash);
                    download.run_me.store(false, Ordering::SeqCst);
                    download.state_requests_count += 1;
                    download.last_target = Some(target.clone());
                    let run_me = download.run_me.clone();

                    near_performance_metrics::actix::spawn(
                        std::any::type_name::<Self>(),
                        file!(),
                        line!(),
                        self.network_adapter
                            .send(NetworkRequests::StateRequestPart {
                                shard_id,
                                sync_hash,
                                part_id: part_id as u64,
                                target: target.clone(),
                            })
                            .then(move |result| {
                                if let Ok(NetworkResponses::RouteNotFound) = result {
                                    // Send a StateRequestPart on the next iteration
                                    run_me.store(true, Ordering::SeqCst);
                                }
                                future::ready(())
                            }),
                    );
                }
            }
            _ => {}
        }

        Ok(new_shard_sync_download)
    }

    pub fn run(
        &mut self,
        me: &Option<AccountId>,
        sync_hash: CryptoHash,
        new_shard_sync: &mut HashMap<u64, ShardSyncDownload>,
        chain: &mut Chain,
        runtime_adapter: &Arc<dyn RuntimeAdapter>,
        highest_height_peers: &Vec<FullPeerInfo>,
        tracking_shards: Vec<ShardId>,
    ) -> Result<StateSyncResult, near_chain::Error> {
        let prev_hash = chain.get_block_header(&sync_hash)?.prev_hash().clone();
        let now = Utc::now();

        let (request_block, have_block) = self.sync_block_status(&prev_hash, chain, now)?;

        if tracking_shards.is_empty() {
            // This case is possible if a validator cares about the same shards in the new epoch as
            //    in the previous (or about a subset of them), return success right away

            return if !have_block {
                Ok(StateSyncResult::Changed(request_block))
            } else {
                Ok(StateSyncResult::Completed)
            };
        }

        let (update_sync_status, all_done) = self.sync_shards_status(
            me,
            sync_hash,
            new_shard_sync,
            chain,
            runtime_adapter,
            highest_height_peers,
            tracking_shards,
            now,
        )?;

        if have_block && all_done {
            self.state_sync_time.clear();
            return Ok(StateSyncResult::Completed);
        }

        Ok(if update_sync_status || request_block {
            StateSyncResult::Changed(request_block)
        } else {
            StateSyncResult::Unchanged
        })
    }
}

/// Create an abstract collection of elements to be shuffled.
/// Each element will appear in the shuffled output exactly `limit` times.
/// Use it as an iterator to access the shuffled collection.
///
/// ```rust,ignore
/// let sampler = SamplerLimited::new(vec![1, 2, 3], 2);
///
/// let res = sampler.collect::<Vec<_>>();
///
/// assert!(res.len() == 6);
/// assert!(res.iter().filter(|v| v == 1).count() == 2);
/// assert!(res.iter().filter(|v| v == 2).count() == 2);
/// assert!(res.iter().filter(|v| v == 3).count() == 2);
/// ```
///
/// Out of the 90 possible values of `res` in the code above on of them is:
///
/// ```
/// vec![1, 2, 1, 3, 3, 2];
/// ```
struct SamplerLimited<T> {
    data: Vec<T>,
    limit: Vec<u64>,
}

impl<T> SamplerLimited<T> {
    fn new(data: Vec<T>, limit: u64) -> Self {
        if limit == 0 {
            Self { data: vec![], limit: vec![] }
        } else {
            let len = data.len();
            Self { data, limit: vec![limit; len] }
        }
    }
}

impl<T: Clone> Iterator for SamplerLimited<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.limit.is_empty() {
            None
        } else {
            let len = self.limit.len();
            let ix = thread_rng().gen_range(0, len);
            self.limit[ix] -= 1;

            if self.limit[ix] == 0 {
                if ix + 1 != len {
                    self.limit[ix] = self.limit[len - 1];
                    self.data.swap(ix, len - 1);
                }

                self.limit.pop();
                self.data.pop()
            } else {
                Some(self.data[ix].clone())
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use std::thread;

    use near_chain::test_utils::{setup, setup_with_validators};
    use near_chain::{ChainGenesis, Provenance};
    use near_crypto::{KeyType, PublicKey};
    use near_network::routing::EdgeInfo;
    use near_network::test_utils::MockNetworkAdapter;
    use near_network::types::PeerChainInfoV2;
    use near_network::PeerInfo;
    use near_primitives::block::{Approval, Block, GenesisId};
    use near_primitives::network::PeerId;

    use super::*;
    use crate::test_utils::TestEnv;
    use near_primitives::merkle::PartialMerkleTree;
    use near_primitives::types::EpochId;
    use near_primitives::validator_signer::InMemoryValidatorSigner;
    use near_primitives::version::PROTOCOL_VERSION;
    use num_rational::Ratio;
    use std::collections::HashSet;

    #[test]
    fn test_get_locator_heights() {
        assert_eq!(get_locator_heights(0), vec![0]);
        assert_eq!(get_locator_heights(1), vec![1, 0]);
        assert_eq!(get_locator_heights(2), vec![2, 0]);
        assert_eq!(get_locator_heights(3), vec![3, 1, 0]);
        assert_eq!(get_locator_heights(10), vec![10, 8, 4, 0]);
        assert_eq!(get_locator_heights(100), vec![100, 98, 94, 86, 70, 38, 0]);
        assert_eq!(
            get_locator_heights(1000),
            vec![1000, 998, 994, 986, 970, 938, 874, 746, 490, 0]
        );
        // Locator is still reasonable size even given large height.
        assert_eq!(
            get_locator_heights(10000),
            vec![10000, 9998, 9994, 9986, 9970, 9938, 9874, 9746, 9490, 8978, 7954, 5906, 1810, 0,]
        );
    }

    /// Starts two chains that fork of genesis and checks that they can sync heaaders to the longest.
    #[test]
    fn test_sync_headers_fork() {
        let mock_adapter = Arc::new(MockNetworkAdapter::default());
        let mut header_sync = HeaderSync::new(
            mock_adapter.clone(),
            TimeDuration::from_secs(10),
            TimeDuration::from_secs(2),
            TimeDuration::from_secs(120),
            1_000_000_000,
        );
        let (mut chain, _, signer) = setup();
        for _ in 0..3 {
            let prev = chain.get_block(&chain.head().unwrap().last_block_hash).unwrap();
            let block = Block::empty(prev, &*signer);
            chain
                .process_block(&None, block, Provenance::PRODUCED, |_| {}, |_| {}, |_| {})
                .unwrap();
        }
        let (mut chain2, _, signer2) = setup();
        for _ in 0..5 {
            let prev = chain2.get_block(&chain2.head().unwrap().last_block_hash).unwrap();
            let block = Block::empty(&prev, &*signer2);
            chain2
                .process_block(&None, block, Provenance::PRODUCED, |_| {}, |_| {}, |_| {})
                .unwrap();
        }
        let mut sync_status = SyncStatus::NoSync;
        let peer1 = FullPeerInfo {
            peer_info: PeerInfo::random(),
            chain_info: PeerChainInfoV2 {
                genesis_id: GenesisId {
                    chain_id: "unittest".to_string(),
                    hash: *chain.genesis().hash(),
                },
                height: chain2.head().unwrap().height,
                tracked_shards: vec![],
                archival: false,
            },
            edge_info: EdgeInfo::default(),
        };
        let head = chain.head().unwrap();
        assert!(header_sync
            .run(&mut sync_status, &mut chain, head.height, &vec![peer1.clone()])
            .is_ok());
        assert!(sync_status.is_syncing());
        // Check that it queried last block, and then stepped down to genesis block to find common block with the peer.
        assert_eq!(
            mock_adapter.pop().unwrap(),
            NetworkRequests::BlockHeadersRequest {
                hashes: [3, 1, 0]
                    .iter()
                    .map(|i| *chain.get_block_by_height(*i).unwrap().hash())
                    .collect(),
                peer_id: peer1.peer_info.id
            }
        );
    }

    /// Sets up `HeaderSync` with particular tolerance for slowness, and makes sure that a peer that
    /// sends headers below the threshold gets banned, and the peer that sends them faster doesn't get
    /// banned.
    /// Also makes sure that if `header_sync_due` is checked more frequently than the `progress_timeout`
    /// the peer doesn't get banned. (specifically, that the expected height downloaded gets properly
    /// adjusted for time passed)
    #[test]
    fn test_slow_header_sync() {
        let network_adapter = Arc::new(MockNetworkAdapter::default());
        let highest_height = 1000;

        // Setup header_sync with expectation of 25 headers/second
        let mut header_sync = HeaderSync::new(
            network_adapter.clone(),
            TimeDuration::from_secs(1),
            TimeDuration::from_secs(1),
            TimeDuration::from_secs(3),
            25,
        );

        let set_syncing_peer = |header_sync: &mut HeaderSync| {
            header_sync.syncing_peer = Some(FullPeerInfo {
                peer_info: PeerInfo {
                    id: PeerId::new(PublicKey::empty(KeyType::ED25519)),
                    addr: None,
                    account_id: None,
                },
                chain_info: Default::default(),
                edge_info: Default::default(),
            });
            header_sync.syncing_peer.as_mut().unwrap().chain_info.height = highest_height;
        };
        set_syncing_peer(&mut header_sync);

        let (mut chain, _, signers) = setup_with_validators(
            vec!["test0", "test1", "test2", "test3", "test4"]
                .iter()
                .map(|x| x.to_string())
                .collect(),
            1,
            1,
            1000,
            100,
        );
        let genesis = chain.get_block(&chain.genesis().hash().clone()).unwrap().clone();

        let mut last_block = &genesis;
        let mut all_blocks = vec![];
        let mut block_merkle_tree = PartialMerkleTree::default();
        for i in 0..61 {
            let current_height = 3 + i * 5;

            let approvals = [None, None, Some("test3"), Some("test4")]
                .iter()
                .map(|account_id| {
                    account_id.map(|account_id| {
                        let signer = InMemoryValidatorSigner::from_seed(
                            account_id,
                            KeyType::ED25519,
                            account_id,
                        );
                        Approval::new(
                            *last_block.hash(),
                            last_block.header().height(),
                            current_height,
                            &signer,
                        )
                        .signature
                    })
                })
                .collect();
            let (epoch_id, next_epoch_id) =
                if last_block.header().prev_hash() == &CryptoHash::default() {
                    (last_block.header().next_epoch_id().clone(), EpochId(*last_block.hash()))
                } else {
                    (
                        last_block.header().epoch_id().clone(),
                        last_block.header().next_epoch_id().clone(),
                    )
                };
            let block = Block::produce(
                PROTOCOL_VERSION,
                &last_block.header(),
                current_height,
                #[cfg(feature = "protocol_feature_block_header_v3")]
                (last_block.header().block_ordinal() + 1),
                last_block.chunks().iter().cloned().collect(),
                epoch_id,
                next_epoch_id,
                #[cfg(feature = "protocol_feature_block_header_v3")]
                None,
                approvals,
                Ratio::new(0, 1),
                0,
                100,
                Some(0),
                vec![],
                vec![],
                &*signers[3],
                last_block.header().next_bp_hash().clone(),
                block_merkle_tree.root(),
            );
            block_merkle_tree.insert(*block.hash());

            all_blocks.push(block);

            last_block = &all_blocks[all_blocks.len() - 1];
        }

        let mut last_added_block_ord = 0;
        // First send 30 heights every second for a while and make sure it doesn't get
        // banned
        for _iter in 0..12 {
            let block = &all_blocks[last_added_block_ord];
            let current_height = block.header().height();
            set_syncing_peer(&mut header_sync);
            header_sync.header_sync_due(
                &SyncStatus::HeaderSync { current_height, highest_height },
                &Tip::from_header(&block.header()),
                highest_height,
            );

            last_added_block_ord += 3;

            thread::sleep(TimeDuration::from_millis(500));
        }
        // 6 blocks / second is fast enough, we should not have banned the peer
        assert!(network_adapter.requests.read().unwrap().is_empty());

        // Now the same, but only 20 heights / sec
        for _iter in 0..12 {
            let block = &all_blocks[last_added_block_ord];
            let current_height = block.header().height();
            set_syncing_peer(&mut header_sync);
            header_sync.header_sync_due(
                &SyncStatus::HeaderSync { current_height, highest_height },
                &Tip::from_header(&block.header()),
                highest_height,
            );

            last_added_block_ord += 2;

            thread::sleep(TimeDuration::from_millis(500));
        }
        // This time the peer should be banned, because 4 blocks/s is not fast enough
        let ban_peer = network_adapter.requests.write().unwrap().pop_back().unwrap();
        if let NetworkRequests::BanPeer { .. } = ban_peer {
            /* expected */
        } else {
            assert!(false);
        }
    }

    /// Helper function for block sync tests
    fn collect_hashes_from_network_adapter(
        network_adapter: Arc<MockNetworkAdapter>,
    ) -> HashSet<CryptoHash> {
        let mut requested_block_hashes = HashSet::new();
        let mut network_request = network_adapter.requests.write().unwrap();
        while let Some(request) = network_request.pop_back() {
            match request {
                NetworkRequests::BlockRequest { hash, .. } => {
                    requested_block_hashes.insert(hash);
                }
                _ => panic!("unexpected network request {:?}", request),
            }
        }
        requested_block_hashes
    }

    fn create_peer_infos(num_peers: usize) -> Vec<FullPeerInfo> {
        (0..num_peers)
            .map(|_| FullPeerInfo {
                peer_info: PeerInfo {
                    id: PeerId::new(PublicKey::empty(KeyType::ED25519)),
                    addr: None,
                    account_id: None,
                },
                chain_info: Default::default(),
                edge_info: Default::default(),
            })
            .collect()
    }

    #[test]
    fn test_block_sync() {
        let network_adapter = Arc::new(MockNetworkAdapter::default());
        let block_fetch_horizon = 10;
        let mut block_sync = BlockSync::new(network_adapter.clone(), block_fetch_horizon, false);
        let mut chain_genesis = ChainGenesis::test();
        chain_genesis.epoch_length = 100;
        let mut env = TestEnv::new(chain_genesis, 2, 1);
        let mut blocks = vec![];
        for i in 1..21 {
            let block = env.clients[0].produce_block(i).unwrap().unwrap();
            blocks.push(block.clone());
            env.process_block(0, block, Provenance::PRODUCED);
        }
        let block_headers = blocks.iter().map(|b| b.header().clone()).collect::<Vec<_>>();
        let peer_infos = create_peer_infos(2);
        env.clients[1].chain.sync_block_headers(block_headers, |_| unreachable!()).unwrap();

        for block in blocks.iter().take(5) {
            let is_state_sync =
                block_sync.block_sync(&mut env.clients[1].chain, &peer_infos).unwrap();
            assert!(!is_state_sync);

            let requested_block_hashes =
                collect_hashes_from_network_adapter(network_adapter.clone());
            assert_eq!(
                requested_block_hashes,
                [block].iter().map(|x| *x.hash()).collect::<HashSet<_>>()
            );

            env.process_block(1, block.clone(), Provenance::NONE);
        }

        // Receive all blocks. Should not request more.
        for i in 5..21 {
            env.process_block(1, blocks[i - 1].clone(), Provenance::NONE);
        }
        block_sync.block_sync(&mut env.clients[1].chain, &peer_infos).unwrap();
        let requested_block_hashes = collect_hashes_from_network_adapter(network_adapter.clone());
        assert!(requested_block_hashes.is_empty());
    }

    #[test]
    fn test_block_sync_archival() {
        let network_adapter = Arc::new(MockNetworkAdapter::default());
        let block_fetch_horizon = 10;
        let mut block_sync = BlockSync::new(network_adapter.clone(), block_fetch_horizon, true);
        let mut chain_genesis = ChainGenesis::test();
        chain_genesis.epoch_length = 5;
        let mut env = TestEnv::new(chain_genesis, 2, 1);
        let mut blocks = vec![];
        for i in 1..31 {
            let block = env.clients[0].produce_block(i).unwrap().unwrap();
            blocks.push(block.clone());
            env.process_block(0, block, Provenance::PRODUCED);
        }
        let block_headers = blocks.iter().map(|b| b.header().clone()).collect::<Vec<_>>();
        let peer_infos = create_peer_infos(2);
        env.clients[1].chain.sync_block_headers(block_headers, |_| unreachable!()).unwrap();
        let is_state_sync = block_sync.block_sync(&mut env.clients[1].chain, &peer_infos).unwrap();
        assert!(!is_state_sync);
        let requested_block_hashes = collect_hashes_from_network_adapter(network_adapter.clone());
        // We don't have archival peers, and thus cannot request any blocks
        assert_eq!(requested_block_hashes, HashSet::new());

        let mut peer_infos = create_peer_infos(2);
        for peer in peer_infos.iter_mut() {
            peer.chain_info.archival = true;
        }
        let is_state_sync = block_sync.block_sync(&mut env.clients[1].chain, &peer_infos).unwrap();
        assert!(!is_state_sync);
        let requested_block_hashes = collect_hashes_from_network_adapter(network_adapter.clone());
        assert_eq!(
            requested_block_hashes,
            blocks.iter().take(1).map(|b| *b.hash()).collect::<HashSet<_>>()
        );
    }
}
