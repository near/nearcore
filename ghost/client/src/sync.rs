use chrono::{DateTime, Duration, Utc};
use log::debug;

use near_chain::{Chain, Tip};

use crate::types::SyncStatus;
use actix::Addr;
use near_network::{FullPeerInfo, NetworkRequests, PeerInfo};
use primitives::hash::CryptoHash;
use primitives::types::BlockIndex;
use std::cmp;

/// Maximum number of block headers send over the network.
const MAX_BLOCK_HEADERS: u64 = 512;

/// Maximum number of blocks to request in one step.
const MAX_BLOCK_REQUEST: usize = 100;

/// Maximum number of blocks to ask from single peer.
const MAX_PEER_BLOCK_REQUEST: usize = 10;

const BLOCK_REQUEST_TIMEOUT: i64 = 6;
const BLOCK_SOME_RECEIVED_TIMEOUT: i64 = 1;
const BLOCK_REQUEST_BROADCAST_OFFSET: u64 = 2;

/// Helper to keep track of sync headers.
pub struct HeaderSync {
    prev_header_sync: (DateTime<Utc>, BlockIndex, BlockIndex),
    stalling_ts: Option<DateTime<Utc>>,
}

impl HeaderSync {
    pub fn new() -> Self {
        HeaderSync { prev_header_sync: (Utc::now(), 0, 0), stalling_ts: None }
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
                    //                    if let Some(ref peer) = self.syncing_peer {
                    //                        match sync_status {
                    //                            SyncStatus::HeaderSync { highest_height, .. } => {
                    ////                                if now > *stalling_ts + Duration::seconds(120) && highest_height ==  {
                    ////
                    ////                                }
                    //                            },
                    //                            _ => (),
                    //                        }
                    //                    }
                }
            }
            true
        } else {
            // Resetting the timeout as long as we make progress.
            if header_head.height > latest_height {
                self.prev_header_sync =
                    (now + Duration::seconds(2), header_head.height, prev_height);
            }
            false
        }
    }

    pub fn run(
        &mut self,
        sync_status: &mut SyncStatus,
        chain: &Chain,
        highest_height: BlockIndex,
    ) -> Result<(), near_chain::Error> {
        let header_head = chain.store().header_head()?;
        if !self.header_sync_due(sync_status, &header_head) {
            return Ok(());
        }

        let enable_header_sync = match sync_status {
            SyncStatus::HeaderSync { .. } | SyncStatus::BodySync { .. } | SyncStatus::StateDone => {
                true
            }
            SyncStatus::NoSync | SyncStatus::AwaitingPeers => {
                debug!(target: "client", "Sync: initial transition to Header sync.");
                true
            }
            _ => false,
        };

        if enable_header_sync {
            *sync_status =
                SyncStatus::HeaderSync { current_height: header_head.height, highest_height };
        }
        Ok(())
    }

    /// Request headers from a given peer to advance the chain.
    fn request_headers(&mut self, peer: PeerInfo) {
        // let hashes = ;
        //        peer.do_send(NetworkRequests::BlockHeaders { hashes });
    }
}

/// Helper to track block syncing.
pub struct BlockSync {
    blocks_requested: BlockIndex,
    receive_timeout: DateTime<Utc>,
    prev_blocks_recevied: BlockIndex,
}

impl BlockSync {
    pub fn new() -> Self {
        BlockSync { blocks_requested: 0, receive_timeout: Utc::now(), prev_blocks_recevied: 0 }
    }

    pub fn run(
        &mut self,
        sync_status: &mut SyncStatus,
        chain: &Chain,
        head: &Tip,
        highest_height: BlockIndex,
        most_weight_peers: &[FullPeerInfo],
    ) -> Result<(), near_chain::Error> {
        if self.block_sync_due(chain)? {
            if self.block_sync(chain, most_weight_peers)? {
                return Ok(());
            }

            *sync_status = SyncStatus::BodySync { current_height: head.height, highest_height };
        }
        Ok(())
    }

    /// Returns true if state download is required (last known block is too far).
    pub fn block_sync(
        &mut self,
        chain: &Chain,
        most_weight_peers: &[FullPeerInfo],
    ) -> Result<bool, near_chain::Error> {
        let mut hashes: Option<Vec<CryptoHash>> = Some(vec![]);
        // TODO: check state download is needed.
        let state_needed = false;
        if state_needed {
            return Ok(true);
        }
        let mut hashes = hashes.ok_or::<near_chain::Error>(
            near_chain::ErrorKind::Other("Sync: hashes is None".to_string())
                .into(),
        )?;
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
            for hash in hashes_to_request.iter() {
                if let Some(peer) = peers_iter.next() {
                    // TODO: send request
                    //                    if let Err(e)
                    //                    else {
                    //                        self.blocks_requested += 1;
                    //                    }
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
            debug!(target: "sync", "Block sync: No pending block requests, requesting more.");
            return Ok(true);
        }

        Ok(false)
    }

    /// Total number of received blocks by the chain.
    fn blocks_received(&self, chain: &Chain) -> Result<u64, near_chain::Error> {
        Ok((chain.head()?).height + chain.orphans_len() as u64 + chain.orphans_evicted_len() as u64)
    }
}
