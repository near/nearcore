use std::cmp;
use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use log::{debug, error, info};
use rand::{thread_rng, Rng};

use near_chain::types::ShardStateSyncResponseHeader;
use near_chain::{Chain, RuntimeAdapter, Tip};
use near_chunks::NetworkAdapter;
use near_network::types::ReasonForBan;
use near_network::{FullPeerInfo, NetworkRequests};
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, BlockIndex, Range, ShardId};
use near_primitives::unwrap_or_return;

use crate::types::{DownloadStatus, ShardSyncDownload, ShardSyncStatus, SyncStatus};

/// Maximum number of block headers send over the network.
pub const MAX_BLOCK_HEADERS: u64 = 512;

const BLOCK_HEADER_PROGRESS_TIMEOUT: i64 = 2;

/// Maximum number of block header hashes to send as part of a locator.
pub const MAX_BLOCK_HEADER_HASHES: usize = 20;

/// Maximum number of blocks to request in one step.
const MAX_BLOCK_REQUEST: usize = 100;

/// Maximum number of blocks to ask from single peer.
const MAX_PEER_BLOCK_REQUEST: usize = 10;

const BLOCK_REQUEST_TIMEOUT: i64 = 6;
const BLOCK_SOME_RECEIVED_TIMEOUT: i64 = 1;
const BLOCK_REQUEST_BROADCAST_OFFSET: u64 = 2;

/// Sync state download timeout in seconds.
const STATE_SYNC_TIMEOUT: i64 = 10;

/// Get random peer from the most weighted peers.
pub fn most_weight_peer(most_weight_peers: &Vec<FullPeerInfo>) -> Option<FullPeerInfo> {
    if most_weight_peers.len() == 0 {
        return None;
    }
    let index = thread_rng().gen_range(0, most_weight_peers.len());
    Some(most_weight_peers[index].clone())
}

/// Helper to keep track of sync headers.
/// Handles major re-orgs by finding closest header that matches and re-downloading headers from that point.
pub struct HeaderSync {
    network_adapter: Arc<dyn NetworkAdapter>,
    history_locator: Vec<(BlockIndex, CryptoHash)>,
    prev_header_sync: (DateTime<Utc>, BlockIndex, BlockIndex),
    syncing_peer: Option<FullPeerInfo>,
    stalling_ts: Option<DateTime<Utc>>,
}

impl HeaderSync {
    pub fn new(network_adapter: Arc<dyn NetworkAdapter>) -> Self {
        HeaderSync {
            network_adapter,
            history_locator: vec![],
            prev_header_sync: (Utc::now(), 0, 0),
            syncing_peer: None,
            stalling_ts: None,
        }
    }

    pub fn run(
        &mut self,
        sync_status: &mut SyncStatus,
        chain: &mut Chain,
        highest_height: BlockIndex,
        most_weight_peers: &Vec<FullPeerInfo>,
    ) -> Result<(), near_chain::Error> {
        let header_head = chain.header_head()?;
        if !self.header_sync_due(sync_status, &header_head) {
            return Ok(());
        }

        let enable_header_sync = match sync_status {
            SyncStatus::HeaderSync { .. }
            | SyncStatus::BodySync { .. }
            | SyncStatus::StateSyncDone => true,
            SyncStatus::NoSync | SyncStatus::AwaitingPeers => {
                let sync_head = chain.sync_head()?;
                debug!(target: "sync", "Sync: initial transition to Header sync. Sync head: {} at {}, resetting to {} at {}",
                    sync_head.last_block_hash, sync_head.height,
                    header_head.last_block_hash, header_head.height,
                );
                // Reset sync_head to header_head on initial transition to HeaderSync.
                chain.reset_sync_head()?;
                self.history_locator.retain(|&x| x.0 == 0);
                true
            }
            SyncStatus::StateSync { .. } => false,
        };

        if enable_header_sync {
            *sync_status =
                SyncStatus::HeaderSync { current_height: header_head.height, highest_height };
            let header_head = chain.header_head()?;
            self.syncing_peer = None;
            if let Some(peer) = most_weight_peer(&most_weight_peers) {
                if peer.chain_info.total_weight > header_head.total_weight {
                    self.syncing_peer = self.request_headers(chain, peer);
                }
            }
        }
        Ok(())
    }

    fn header_sync_due(&mut self, sync_status: &SyncStatus, header_head: &Tip) -> bool {
        let now = Utc::now();
        let (timeout, latest_height, prev_height) = self.prev_header_sync;

        // Received all necessary header, can request more.
        let all_headers_received = header_head.height >= prev_height + MAX_BLOCK_HEADERS - 4;
        // No headers processed and it's past timeout, request more.

        let stalling = header_head.height <= latest_height && now > timeout;

        // Always enable header sync on initial state transition from NoSync / AwaitingPeers.
        let force_sync = match sync_status {
            SyncStatus::NoSync | SyncStatus::AwaitingPeers => true,
            _ => false,
        };

        if force_sync || all_headers_received || stalling {
            self.prev_header_sync =
                (now + Duration::seconds(10), header_head.height, header_head.height);

            if stalling {
                if self.stalling_ts.is_none() {
                    self.stalling_ts = Some(now);
                } else {
                    self.stalling_ts = None;
                }
            }

            if all_headers_received {
                self.stalling_ts = None;
            } else {
                if let Some(ref stalling_ts) = self.stalling_ts {
                    if let Some(ref peer) = self.syncing_peer {
                        match sync_status {
                            SyncStatus::HeaderSync { highest_height, .. } => {
                                if now > *stalling_ts + Duration::seconds(120)
                                    && *highest_height == peer.chain_info.height
                                {
                                    info!(target: "sync", "Sync: ban a fraudulent peer: {}, claimed height: {}, total weight: {}",
                                        peer.peer_info, peer.chain_info.height, peer.chain_info.total_weight);
                                    self.network_adapter.send(NetworkRequests::BanPeer {
                                        peer_id: peer.peer_info.id.clone(),
                                        ban_reason: ReasonForBan::HeightFraud,
                                    });
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
            if header_head.height > latest_height {
                self.prev_header_sync = (
                    now + Duration::seconds(BLOCK_HEADER_PROGRESS_TIMEOUT),
                    header_head.height,
                    prev_height,
                );
            }
            false
        }
    }

    /// Request headers from a given peer to advance the chain.
    fn request_headers(&mut self, chain: &mut Chain, peer: FullPeerInfo) -> Option<FullPeerInfo> {
        if let Ok(locator) = self.get_locator(chain) {
            debug!(target: "sync", "Sync: request headers: asking {} for headers, {:?}", peer.peer_info.id, locator);
            self.network_adapter.send(NetworkRequests::BlockHeadersRequest {
                hashes: locator,
                peer_id: peer.peer_info.id.clone(),
            });
            return Some(peer);
        }
        None
    }

    fn get_locator(&mut self, chain: &mut Chain) -> Result<Vec<CryptoHash>, near_chain::Error> {
        let tip = chain.sync_head()?;
        let heights = get_locator_heights(tip.height);

        // Clear history_locator in any case of header chain rollback.
        if self.history_locator.len() > 0
            && tip.last_block_hash != chain.header_head()?.last_block_hash
        {
            self.history_locator.retain(|&x| x.0 == 0);
        }

        // For each height we need, we either check if something is close enough from last locator, or go to the db.
        let mut locator: Vec<(u64, CryptoHash)> = vec![(tip.height, tip.last_block_hash)];
        for h in heights {
            if let Some(x) = close_enough(&self.history_locator, h) {
                locator.push(x);
            } else {
                // Walk backwards to find last known hash.
                let last_loc = locator.last().unwrap().clone();
                if let Ok(header) = chain.get_header_by_height(h) {
                    if header.inner.height != last_loc.0 {
                        locator.push((header.inner.height, header.hash()));
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

/// Helper to track block syncing.
pub struct BlockSync {
    network_adapter: Arc<dyn NetworkAdapter>,
    blocks_requested: BlockIndex,
    receive_timeout: DateTime<Utc>,
    prev_blocks_received: BlockIndex,
    /// How far to fetch blocks vs fetch state.
    block_fetch_horizon: BlockIndex,
}

impl BlockSync {
    pub fn new(network_adapter: Arc<dyn NetworkAdapter>, block_fetch_horizon: BlockIndex) -> Self {
        BlockSync {
            network_adapter,
            blocks_requested: 0,
            receive_timeout: Utc::now(),
            prev_blocks_received: 0,
            block_fetch_horizon,
        }
    }

    /// Runs check if block sync is needed, if it's needed and it's too far - sync state is started instead (returning true).
    /// Otherwise requests recent blocks from peers.
    pub fn run(
        &mut self,
        sync_status: &mut SyncStatus,
        chain: &mut Chain,
        highest_height: BlockIndex,
        most_weight_peers: &[FullPeerInfo],
    ) -> Result<bool, near_chain::Error> {
        if self.block_sync_due(chain)? {
            if self.block_sync(chain, most_weight_peers, self.block_fetch_horizon)? {
                return Ok(true);
            }

            let head = chain.head()?;
            *sync_status = SyncStatus::BodySync { current_height: head.height, highest_height };
        }
        Ok(false)
    }

    /// Returns true if state download is required (last known block is too far).
    /// Otherwise request recent blocks from peers round robin.
    pub fn block_sync(
        &mut self,
        chain: &mut Chain,
        most_weight_peers: &[FullPeerInfo],
        block_fetch_horizon: BlockIndex,
    ) -> Result<bool, near_chain::Error> {
        let (state_needed, mut hashes) = chain.check_state_needed(block_fetch_horizon)?;
        if state_needed {
            return Ok(true);
        }
        hashes.reverse();
        // Ask for `num_peers * MAX_PEER_BLOCK_REQUEST` blocks up to 100, throttle if there is too many orphans in the chain.
        let block_count = cmp::min(
            cmp::min(MAX_BLOCK_REQUEST, MAX_PEER_BLOCK_REQUEST * most_weight_peers.len()),
            near_chain::MAX_ORPHAN_SIZE.saturating_sub(chain.orphans_len()) + 1,
        );

        let hashes_to_request = hashes
            .iter()
            .filter(|x| !chain.get_block(x).is_ok() && !chain.is_orphan(x))
            .take(block_count)
            .collect::<Vec<_>>();
        if hashes_to_request.len() > 0 {
            let head = chain.head()?;
            let header_head = chain.header_head()?;

            debug!(target: "sync", "Block sync: {}/{} requesting blocks {:?} from {} peers", head.height, header_head.height, hashes_to_request, most_weight_peers.len());

            self.blocks_requested = 0;
            self.receive_timeout = Utc::now() + Duration::seconds(BLOCK_REQUEST_TIMEOUT);

            let mut peers_iter = most_weight_peers.iter().cycle();
            for hash in hashes_to_request.into_iter() {
                if let Some(peer) = peers_iter.next() {
                    self.network_adapter.send(NetworkRequests::BlockRequest {
                        hash: hash.clone(),
                        peer_id: peer.peer_info.id.clone(),
                    });
                    self.blocks_requested += 1;
                }
            }
        }
        Ok(false)
    }

    /// Check if we should run block body sync and ask for more full blocks.
    fn block_sync_due(&mut self, chain: &Chain) -> Result<bool, near_chain::Error> {
        let blocks_received = self.blocks_received(chain)?;

        // Some blocks have been requested.
        if self.blocks_requested > 0 {
            let timeout = Utc::now() > self.receive_timeout;
            if timeout && blocks_received <= self.prev_blocks_received {
                debug!(target: "sync", "Block sync: expecting {} more blocks and none received for a while", self.blocks_requested);
                return Ok(true);
            }
        }

        if blocks_received > self.prev_blocks_received {
            // Some blocks received, update for next check.
            self.receive_timeout = Utc::now() + Duration::seconds(BLOCK_SOME_RECEIVED_TIMEOUT);
            self.blocks_requested =
                self.blocks_requested.saturating_sub(blocks_received - self.prev_blocks_received);
            self.prev_blocks_received = blocks_received;
        }

        // Account for broadcast adding few blocks to orphans during.
        if self.blocks_requested < BLOCK_REQUEST_BROADCAST_OFFSET {
            // debug!(target: "sync", "Block sync: No pending block requests, requesting more.");
            return Ok(true);
        }

        Ok(false)
    }

    /// Total number of received blocks by the chain.
    fn blocks_received(&self, chain: &Chain) -> Result<u64, near_chain::Error> {
        Ok((chain.head()?).height
            + chain.orphans_len() as u64
            + chain.blocks_with_missing_chunks_len() as u64
            + chain.orphans_evicted_len() as u64)
    }
}

pub enum StateSyncResult {
    /// No shard has changed its status
    Unchanged,
    /// At least one shard has changed its status
    /// Boolean parameter specifies whether the client needs to start fetching the block
    Changed(bool),
    /// The state for all shards was downloaded
    Completed,
}

pub struct StateSyncStrategy {}

impl StateSyncStrategy {
    pub fn download_by_one(downloads: &Vec<DownloadStatus>) -> Vec<Vec<Range>> {
        let mut strategy = vec![];
        for (i, download) in downloads.iter().enumerate() {
            if download.run_me {
                strategy.push(vec![Range(i as u64, i as u64 + 1)]);
            }
        }
        strategy
    }

    pub fn download_sqrt(downloads: &Vec<DownloadStatus>) -> Vec<Vec<Range>> {
        let len = downloads.len();
        let run_count = downloads.iter().filter(|d| d.run_me).count();
        if run_count * 5 < len {
            // We downloaded more than 80% of the state.
            // Let's distribute all small pieces between all nodes.
            return StateSyncStrategy::download_by_one(downloads);
        }
        let mut strategy = vec![];
        let mut begin = 0;
        for (i, download) in downloads.iter().enumerate() {
            if download.run_me {
                if i - begin >= (len as f64).sqrt() as usize {
                    strategy.push(vec![Range(begin as u64, i as u64)]);
                    begin = i;
                }
            } else {
                if begin != i {
                    strategy.push(vec![Range(begin as u64, i as u64)]);
                }
                begin = i + 1;
            }
        }
        if begin != len {
            strategy.push(vec![Range(begin as u64, len as u64)]);
        }
        strategy
    }
}

/// Helper to track state sync.
pub struct StateSync {
    network_adapter: Arc<dyn NetworkAdapter>,

    state_sync_time: HashMap<ShardId, DateTime<Utc>>,
    last_time_block_requested: Option<DateTime<Utc>>,
}

impl StateSync {
    pub fn new(network_adapter: Arc<dyn NetworkAdapter>) -> Self {
        StateSync {
            network_adapter,
            state_sync_time: Default::default(),
            last_time_block_requested: None,
        }
    }

    pub fn sync_block_status(
        &mut self,
        sync_hash: CryptoHash,
        chain: &mut Chain,
        now: DateTime<Utc>,
    ) -> Result<(bool, bool), near_chain::Error> {
        let prev_hash = chain.get_block_header(&sync_hash)?.inner.prev_hash.clone();
        let (request_block, have_block) = if !chain.block_exists(&prev_hash)? {
            match self.last_time_block_requested {
                None => (true, false),
                Some(last_time) => {
                    if now - last_time >= Duration::seconds(STATE_SYNC_TIMEOUT) {
                        error!(target: "sync", "State sync: block request for {} timed out in {} seconds", prev_hash, STATE_SYNC_TIMEOUT);
                        (true, false)
                    } else {
                        (false, false)
                    }
                }
            }
        } else {
            (false, true)
        };
        if request_block {
            self.last_time_block_requested = Some(now);
        };
        Ok((request_block, have_block))
    }

    pub fn sync_shards_status(
        &mut self,
        sync_hash: CryptoHash,
        new_shard_sync: &mut HashMap<u64, ShardSyncDownload>,
        chain: &mut Chain,
        runtime_adapter: &Arc<dyn RuntimeAdapter>,
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
                    run_me: true,
                    error: false,
                    done: false,
                };
                1
            ],
            status: ShardSyncStatus::StateDownloadHeader,
        };

        for shard_id in tracking_shards {
            let mut download_timeout = false;
            let mut need_shard = false;
            let shard_sync_download = if new_shard_sync.contains_key(&shard_id) {
                &new_shard_sync[&shard_id]
            } else {
                need_shard = true;
                &init_sync_download
            };
            let mut new_sync_download = shard_sync_download.clone();
            let mut this_done = false;
            match shard_sync_download.status {
                ShardSyncStatus::StateDownloadHeader => {
                    if shard_sync_download.downloads[0].done {
                        let shard_state_header =
                            chain.get_received_state_header(shard_id, sync_hash)?;
                        let ShardStateSyncResponseHeader { chunk, .. } = shard_state_header;
                        let state_num_parts = chunk.header.inner.prev_state_root.num_parts;
                        new_sync_download = ShardSyncDownload {
                            downloads: vec![
                                DownloadStatus {
                                    start_time: now,
                                    prev_update_time: now,
                                    run_me: true,
                                    error: false,
                                    done: false,
                                };
                                state_num_parts as usize
                            ],
                            status: ShardSyncStatus::StateDownloadParts,
                        };
                        update_sync_status = true;
                        need_shard = true;
                    } else {
                        let prev = shard_sync_download.downloads[0].prev_update_time;
                        let error = shard_sync_download.downloads[0].error;
                        if now - prev > Duration::seconds(STATE_SYNC_TIMEOUT) || error {
                            download_timeout = true;
                            new_sync_download.downloads[0].run_me = true;
                            new_sync_download.downloads[0].error = false;
                            new_sync_download.downloads[0].prev_update_time = now;
                            update_sync_status = true;
                            need_shard = true;
                        }
                    }
                }
                ShardSyncStatus::StateDownloadParts => {
                    let mut parts_done = true;
                    for (i, part_download) in shard_sync_download.downloads.iter().enumerate() {
                        if !part_download.done {
                            parts_done = false;
                            let prev = shard_sync_download.downloads[i].prev_update_time;
                            let error = shard_sync_download.downloads[i].error;
                            if now - prev > Duration::seconds(STATE_SYNC_TIMEOUT) || error {
                                download_timeout = true;
                                new_sync_download.downloads[i].run_me = true;
                                new_sync_download.downloads[i].error = false;
                                new_sync_download.downloads[i].prev_update_time = now;
                                update_sync_status = true;
                                need_shard = true;
                            }
                        }
                    }
                    if parts_done {
                        update_sync_status = true;
                        new_sync_download = ShardSyncDownload {
                            downloads: vec![],
                            status: ShardSyncStatus::StateDownloadFinalize,
                        };
                    }
                }
                ShardSyncStatus::StateDownloadFinalize => {
                    match chain.set_state_finalize(shard_id, sync_hash) {
                        Ok(_) => {
                            update_sync_status = true;
                            new_sync_download = ShardSyncDownload {
                                downloads: vec![],
                                status: ShardSyncStatus::StateDownloadComplete,
                            }
                        }
                        _ => {
                            // Cannot finalize the downloaded state.
                            // The reasonable behavior here is to start from the very beginning.
                            update_sync_status = true;
                            new_sync_download = init_sync_download.clone();
                        }
                    }
                }
                ShardSyncStatus::StateDownloadComplete => {
                    this_done = true;
                }
            }
            all_done &= this_done;
            // Execute syncing for shard `shard_id`
            if need_shard {
                new_sync_download = self.request_shard(
                    shard_id,
                    chain,
                    runtime_adapter,
                    sync_hash,
                    new_sync_download,
                )?;
            }

            new_shard_sync.insert(shard_id, new_sync_download);

            if download_timeout {
                error!(target: "sync", "State sync: state download for shard {} timed out in {} seconds", shard_id, STATE_SYNC_TIMEOUT);
            }
        }

        Ok((update_sync_status, all_done))
    }

    /// Returns new ShardSyncDownload if successful, otherwise returns given shard_sync_download
    pub fn request_shard(
        &mut self,
        shard_id: ShardId,
        chain: &mut Chain,
        runtime_adapter: &Arc<dyn RuntimeAdapter>,
        hash: CryptoHash,
        shard_sync_download: ShardSyncDownload,
    ) -> Result<ShardSyncDownload, near_chain::Error> {
        let prev_block_hash =
            unwrap_or_return!(chain.get_block_header(&hash), Ok(shard_sync_download))
                .inner
                .prev_hash;
        let epoch_hash = unwrap_or_return!(
            runtime_adapter.get_epoch_id_from_prev_block(&prev_block_hash),
            Ok(shard_sync_download)
        );
        let shard_bps = unwrap_or_return!(
            runtime_adapter.get_epoch_block_producers(&epoch_hash, &hash),
            Ok(shard_sync_download)
        )
        .iter()
        .filter_map(|(account_id, _slashed)| {
            if runtime_adapter.cares_about_shard(
                Some(account_id),
                &prev_block_hash,
                shard_id,
                false,
            ) {
                Some(account_id.clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
        if shard_bps.len() == 0 {
            return Ok(shard_sync_download);
        }

        // Downloading strategy starts here
        let mut new_shard_sync_download = shard_sync_download.clone();
        match shard_sync_download.status {
            ShardSyncStatus::StateDownloadHeader => {
                self.network_adapter.send(NetworkRequests::StateRequest {
                    shard_id,
                    hash,
                    need_header: true,
                    parts_ranges: vec![],
                    account_id: shard_bps[thread_rng().gen_range(0, shard_bps.len())].clone(),
                });
                assert!(new_shard_sync_download.downloads[0].run_me);
                new_shard_sync_download.downloads[0].run_me = false;
            }
            ShardSyncStatus::StateDownloadParts => {
                let download_strategy =
                    StateSyncStrategy::download_sqrt(&shard_sync_download.downloads);
                self.apply_download_strategy(
                    shard_id,
                    hash,
                    &shard_bps,
                    download_strategy,
                    &shard_sync_download,
                    &mut new_shard_sync_download,
                )?;
            }
            _ => {}
        }
        Ok(new_shard_sync_download)
    }

    pub fn run(
        &mut self,
        sync_hash: CryptoHash,
        new_shard_sync: &mut HashMap<u64, ShardSyncDownload>,
        chain: &mut Chain,
        runtime_adapter: &Arc<dyn RuntimeAdapter>,
        tracking_shards: Vec<ShardId>,
    ) -> Result<StateSyncResult, near_chain::Error> {
        let now = Utc::now();
        let (request_block, have_block) = self.sync_block_status(sync_hash, chain, now)?;

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
            sync_hash,
            new_shard_sync,
            chain,
            runtime_adapter,
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

    pub fn apply_download_strategy(
        &mut self,
        shard_id: ShardId,
        hash: CryptoHash,
        shard_bps: &Vec<AccountId>,
        download_strategy: Vec<Vec<Range>>,
        shard_sync_download: &ShardSyncDownload,
        new_shard_sync_download: &mut ShardSyncDownload,
    ) -> Result<(), near_chain::Error> {
        let state_num_parts = shard_sync_download.downloads.len();
        assert_eq!(state_num_parts, new_shard_sync_download.downloads.len());
        for parts_ranges in download_strategy {
            for Range(from, to) in parts_ranges.iter() {
                for i in *from as usize..*to as usize {
                    assert!(new_shard_sync_download.downloads[i].run_me);
                    new_shard_sync_download.downloads[i].run_me = false;
                }
            }
            self.network_adapter.send(NetworkRequests::StateRequest {
                shard_id,
                hash,
                need_header: false,
                parts_ranges,
                account_id: shard_bps[thread_rng().gen_range(0, shard_bps.len())].clone(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use near_chain::test_utils::setup;
    use near_chain::Provenance;
    use near_network::types::PeerChainInfo;
    use near_network::PeerInfo;
    use near_primitives::block::{Block, GenesisId};

    use super::*;
    use crate::test_utils::MockNetworkAdapter;
    use near_network::routing::EdgeInfo;

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
        let mut header_sync = HeaderSync::new(mock_adapter.clone());
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
            chain_info: PeerChainInfo {
                genesis_id: GenesisId {
                    chain_id: "unittest".to_string(),
                    hash: chain.genesis().hash(),
                },
                height: chain2.head().unwrap().height,
                total_weight: chain2.head().unwrap().total_weight,
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
                    .map(|i| chain.get_block_by_height(*i).unwrap().hash())
                    .collect(),
                peer_id: peer1.peer_info.id
            }
        );
    }
}
