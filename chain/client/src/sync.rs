use std::cmp;

use actix::Recipient;
use chrono::{DateTime, Duration, Utc};
use log::{debug, error, info};
use rand::{thread_rng, Rng};

use near_chain::{Chain, Tip};
use near_network::types::ReasonForBan;
use near_network::{FullPeerInfo, NetworkRequests};
use near_primitives::hash::CryptoHash;
use near_primitives::types::BlockIndex;

use crate::types::SyncStatus;

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
    network_recipient: Recipient<NetworkRequests>,
    history_locator: Vec<(BlockIndex, CryptoHash)>,
    prev_header_sync: (DateTime<Utc>, BlockIndex, BlockIndex),
    syncing_peer: Option<FullPeerInfo>,
    stalling_ts: Option<DateTime<Utc>>,
}

impl HeaderSync {
    pub fn new(network_recipient: Recipient<NetworkRequests>) -> Self {
        HeaderSync {
            network_recipient,
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
            SyncStatus::HeaderSync { .. } | SyncStatus::BodySync { .. } | SyncStatus::StateDone => {
                true
            }
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
            _ => false,
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
                                    let _ =
                                        self.network_recipient.do_send(NetworkRequests::BanPeer {
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
    fn request_headers(&mut self, chain: &Chain, peer: FullPeerInfo) -> Option<FullPeerInfo> {
        if let Ok(locator) = self.get_locator(chain) {
            debug!(target: "sync", "Sync: request headers: asking {} for headers, {:?}", peer.peer_info.id, locator);
            // TODO: actix::spawn?
            let _ = self.network_recipient.do_send(NetworkRequests::BlockHeadersRequest {
                hashes: locator,
                peer_id: peer.peer_info.id.clone(),
            });
            return Some(peer);
        }
        None
    }

    fn get_locator(&mut self, chain: &Chain) -> Result<Vec<CryptoHash>, near_chain::Error> {
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
    network_recipient: Recipient<NetworkRequests>,
    blocks_requested: BlockIndex,
    receive_timeout: DateTime<Utc>,
    prev_blocks_recevied: BlockIndex,
}

impl BlockSync {
    pub fn new(network_recipient: Recipient<NetworkRequests>) -> Self {
        BlockSync {
            network_recipient,
            blocks_requested: 0,
            receive_timeout: Utc::now(),
            prev_blocks_recevied: 0,
        }
    }

    pub fn run(
        &mut self,
        sync_status: &mut SyncStatus,
        chain: &mut Chain,
        highest_height: BlockIndex,
        most_weight_peers: &[FullPeerInfo],
    ) -> Result<(), near_chain::Error> {
        if self.block_sync_due(chain)? {
            if self.block_sync(chain, most_weight_peers)? {
                return Ok(());
            }

            let head = chain.head()?;
            *sync_status = SyncStatus::BodySync { current_height: head.height, highest_height };
        }
        Ok(())
    }

    /// Returns true if state download is required (last known block is too far).
    /// Otherwise request recent blocks from peers round robin.
    pub fn block_sync(
        &mut self,
        chain: &mut Chain,
        most_weight_peers: &[FullPeerInfo],
    ) -> Result<bool, near_chain::Error> {
        let (state_needed, mut hashes) = chain.check_state_needed()?;
        if state_needed {
            return Ok(true);
        }
        hashes.reverse();
        // Ask for `num_peers * MAX_PEER_BLOCK_REQUEST` blocks up to 100, throttle if there is too many orphans in the chain.
        let block_count = cmp::min(
            cmp::min(MAX_BLOCK_REQUEST, MAX_PEER_BLOCK_REQUEST * most_weight_peers.len()),
            near_chain::MAX_ORPHAN_SIZE.saturating_sub(chain.orphans_len()) + 1,
        );

        let mut hashes_to_request = hashes
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
            for hash in hashes_to_request.drain(..) {
                if let Some(peer) = peers_iter.next() {
                    if self
                        .network_recipient
                        .do_send(NetworkRequests::BlockRequest {
                            hash: hash.clone(),
                            peer_id: peer.peer_info.id.clone(),
                        })
                        .is_ok()
                    {
                        self.blocks_requested += 1;
                    } else {
                        error!(target: "sync", "Failed to send message to network agent");
                    }
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
            if timeout && blocks_received <= self.prev_blocks_recevied {
                debug!(target: "sync", "Block sync: expecting {} more blocks and none received for a while", self.blocks_requested);
                return Ok(true);
            }
        }

        if blocks_received > self.prev_blocks_recevied {
            // Some blocks received, update for next check.
            self.receive_timeout = Utc::now() + Duration::seconds(BLOCK_SOME_RECEIVED_TIMEOUT);
            self.blocks_requested =
                self.blocks_requested.saturating_sub(blocks_received - self.prev_blocks_recevied);
            self.prev_blocks_recevied = blocks_received;
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
        Ok((chain.head()?).height + chain.orphans_len() as u64 + chain.orphans_evicted_len() as u64)
    }
}

#[cfg(test)]
mod test {
    use super::*;

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
}
