use near_async::messaging::CanSend;
use near_async::time::{Clock, Duration, Utc};
use near_chain::{Chain, ChainStoreAccess};
use near_network::config::MAX_BLOCK_HEADER_HASHES;
use near_network::types::{HighestHeightPeerInfo, NetworkRequests, PeerManagerAdapter};
use near_network::types::{PeerManagerMessageRequest, ReasonForBan};
use near_primitives::block::Tip;
use near_primitives::hash::CryptoHash;
use near_primitives::types::BlockHeight;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::cmp::min;

/// Maximum number of block headers send over the network.
pub const MAX_BLOCK_HEADERS: u64 = 512;

pub const NS_PER_SECOND: u128 = 1_000_000_000;

/// Progress of downloading the currently requested batch of headers.
struct BatchProgress {
    /// An intermediate timeout by which a certain number of headers is expected.
    timeout: Utc,
    /// Height expected at the moment of `timeout`.
    expected_height: BlockHeight,
    /// Header head height at the moment this batch was requested.
    header_head_height: BlockHeight,
    highest_height_of_peers: BlockHeight,
}

/// Helper to keep track of sync headers.
/// Handles major re-orgs by finding closest header that matches and re-downloading headers from that point.
pub struct HeaderSync {
    clock: Clock,

    network_adapter: PeerManagerAdapter,

    /// Progress of downloading the currently requested batch of headers.
    // TODO: Change type to Option<BatchProgress>.
    batch_progress: BatchProgress,

    /// Peer from which the next batch of headers was requested.
    syncing_peer: Option<HighestHeightPeerInfo>,

    /// When the stalling was first detected.
    stalling_ts: Option<Utc>,

    /// How much time to wait after initial header sync.
    initial_timeout: Duration,

    /// How much time to wait after some progress is made in header sync.
    progress_timeout: Duration,

    /// How much time to wait before banning a peer in header sync if sync is too slow.
    stall_ban_timeout: Duration,

    /// Expected increase of header head height per second during header sync
    expected_height_per_second: u64,

    /// Not for production use.
    /// Expected height when node will be automatically shut down, so header
    /// sync can be stopped.
    shutdown_height: near_chain_configs::MutableConfigValue<Option<BlockHeight>>,
}

impl HeaderSync {
    pub fn new(
        clock: Clock,
        network_adapter: PeerManagerAdapter,
        initial_timeout: Duration,
        progress_timeout: Duration,
        stall_ban_timeout: Duration,
        expected_height_per_second: u64,
        shutdown_height: near_chain_configs::MutableConfigValue<Option<BlockHeight>>,
    ) -> Self {
        HeaderSync {
            clock: clock.clone(),
            network_adapter,
            batch_progress: BatchProgress {
                timeout: clock.now_utc(),
                expected_height: 0,
                header_head_height: 0,
                highest_height_of_peers: 0,
            },
            syncing_peer: None,
            stalling_ts: None,
            initial_timeout,
            progress_timeout,
            stall_ban_timeout,
            expected_height_per_second,
            shutdown_height,
        }
    }

    /// Run one tick of header sync. Each tick checks two things in order:
    ///
    /// 1. In-flight request (i.e. `syncing_peer` is set): evaluate the
    ///    current batch — did we receive all ~512 headers (batch complete),
    ///    or has the peer failed to deliver in time (stalling)? If either,
    ///    clear the peer and fall through to step 2. If headers are arriving
    ///    ahead of schedule, extend the timeout and wait.
    ///
    /// 2. Next batch (i.e. `syncing_peer` is `None`): pick a random peer and send
    ///    a header request. This also handles the very first tick, where
    ///    `syncing_peer` starts as `None` from construction.
    ///
    /// When `ban_stalling_peers` is true, a peer that has been stalling
    /// longer than `stall_ban_timeout` is banned before being replaced.
    /// Pass false during block sync where banning could hurt block downloads.
    pub fn run(
        &mut self,
        chain: &Chain,
        highest_height: BlockHeight,
        highest_height_peers: &[HighestHeightPeerInfo],
        ban_stalling_peers: bool,
    ) -> Result<(), near_chain::Error> {
        let header_head = chain.header_head()?;
        let now = self.clock.now_utc();

        // If a request is in flight, check whether the batch is complete or stalling.
        if self.syncing_peer.is_some() {
            let BatchProgress {
                timeout,
                expected_height,
                header_head_height,
                highest_height_of_peers,
            } = self.batch_progress;

            let batch_complete = header_head.height
                >= min(header_head_height + MAX_BLOCK_HEADERS - 4, highest_height_of_peers);
            let stalling = header_head.height <= expected_height && now > timeout;

            if batch_complete {
                self.stalling_ts = None;
                self.syncing_peer = None;
            } else if stalling {
                if self.stalling_ts.is_none() {
                    self.stalling_ts = Some(now);
                }
                if ban_stalling_peers {
                    self.try_ban_stalling_peer(highest_height);
                }
                self.syncing_peer = None;
            } else if self.made_enough_progress(header_head.height, expected_height, now, timeout) {
                self.batch_progress = BatchProgress {
                    timeout: now + self.progress_timeout,
                    expected_height: self
                        .compute_expected_height(header_head.height, self.progress_timeout),
                    header_head_height,
                    highest_height_of_peers,
                };
            }
        }

        // If idle (no request in flight), pick a peer and send a request.
        if self.syncing_peer.is_none() {
            self.start_header_batch(chain, &header_head, highest_height, highest_height_peers)?;
        }

        Ok(())
    }

    /// Ban syncing_peer if stalling has exceeded stall_ban_timeout and the
    /// peer claims to be at the highest height.
    fn try_ban_stalling_peer(&mut self, highest_height: BlockHeight) {
        let Some(stalling_ts) = self.stalling_ts else { unreachable!("stalling_ts always set") };
        if self.clock.now_utc() - stalling_ts <= self.stall_ban_timeout {
            return;
        }
        let Some(peer) = &self.syncing_peer else { return };
        if highest_height != peer.highest_block_height {
            return;
        }
        tracing::warn!(target: "sync", peer_id = ?peer.peer_info.id, highest_height, "header sync stalling, banning peer");
        self.stalling_ts = None;
        // TODO: Consider not banning straightaway, but give a node a few attempts before banning it.
        // TODO: Prefer not to request the next batch of headers from the same peer.
        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::BanPeer {
                peer_id: peer.peer_info.id.clone(),
                ban_reason: ReasonForBan::ProvidedNotEnoughHeaders,
            },
        ));
    }

    /// Pick a random peer, send a header request, and start tracking the batch.
    fn start_header_batch(
        &mut self,
        chain: &Chain,
        header_head: &Tip,
        highest_height: BlockHeight,
        peers: &[HighestHeightPeerInfo],
    ) -> Result<(), near_chain::Error> {
        let Some(peer) = peers.choose(&mut thread_rng()).cloned() else { return Ok(()) };
        let shutdown_height = self.shutdown_height.get().unwrap_or(u64::MAX);
        if peer.highest_block_height.min(shutdown_height) <= header_head.height {
            return Ok(());
        }
        self.request_headers(chain, &peer)?;
        self.syncing_peer = Some(peer);
        self.stalling_ts = None;
        let now = self.clock.now_utc();
        self.batch_progress = BatchProgress {
            timeout: now + self.initial_timeout,
            expected_height: self.compute_expected_height(header_head.height, self.initial_timeout),
            header_head_height: header_head.height,
            highest_height_of_peers: highest_height,
        };
        Ok(())
    }

    /// Returns the height that we expect to reach starting from `old_height` after `time_delta`.
    fn compute_expected_height(
        &self,
        old_height: BlockHeight,
        time_delta: Duration,
    ) -> BlockHeight {
        (old_height as u128
            + (time_delta.whole_nanoseconds() as u128 * self.expected_height_per_second as u128
                / NS_PER_SECOND)) as u64
    }

    /// Checks whether the node made enough progress.
    /// Returns true iff it needs less time than (timeout-now) to get (expected_height - current_height) headers at the rate of `expected_height_per_second` headers per second.
    fn made_enough_progress(
        &self,
        current_height: BlockHeight,
        expected_height: BlockHeight,
        now: Utc,
        timeout: Utc,
    ) -> bool {
        if now <= timeout {
            self.compute_expected_height(current_height, timeout - now) >= expected_height
        } else {
            current_height >= expected_height
        }
    }

    /// Request headers from a given peer to advance the chain.
    fn request_headers(
        &self,
        chain: &Chain,
        peer: &HighestHeightPeerInfo,
    ) -> Result<(), near_chain::Error> {
        let locator = self.get_locator(chain)?;
        tracing::debug!(target: "sync", peer_id = %peer.peer_info.id, ?locator, "requesting headers");
        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::BlockHeadersRequest {
                hashes: locator,
                peer_id: peer.peer_info.id.clone(),
            },
        ));
        Ok(())
    }

    // The remote side will return MAX_BLOCK_HEADERS headers, starting from the first hash in
    // the returned "locator" list that is on their canonical chain.
    //
    // The locator allows us to start syncing from a reasonably recent common ancestor. Since
    // we don't know which fork the remote side is on, we include a few hashes. The first one
    // we include is the tip of our chain, and the next one is 2 blocks back (on the same chain,
    // by number of blocks (or in other words, by ordinals), not by height), then 4 blocks
    // back, then 8 blocks back, etc, until we reach the most recent final block. The reason
    // why we stop at the final block is because the consensus guarantees us that the final
    // blocks observed by all nodes are on the same fork.
    fn get_locator(&self, chain: &Chain) -> Result<Vec<CryptoHash>, near_chain::Error> {
        let store = chain.chain_store();
        let tip = store.header_head()?;
        // We could just get the ordinal from the header, but it's off by one: #8177.
        // Note: older block headers don't have ordinals, so for them we can't get the ordinal from header,
        // have to use get_block_merkle_tree.
        let tip_ordinal = store.get_block_merkle_tree(&tip.last_block_hash)?.size();
        let final_head = store.final_head()?;
        let final_head_ordinal = store.get_block_merkle_tree(&final_head.last_block_hash)?.size();
        let ordinals = get_locator_ordinals(final_head_ordinal, tip_ordinal);
        let mut locator: Vec<CryptoHash> = vec![];
        for ordinal in &ordinals {
            match store.get_block_hash_from_ordinal(*ordinal) {
                Ok(block_hash) => {
                    locator.push(block_hash);
                }
                Err(e) => {
                    // In the case of epoch sync, it is normal and expected that we will not have
                    // many headers before the tip, so that case is fine.
                    if *ordinal == tip_ordinal {
                        return Err(e);
                    }
                    tracing::debug!(target: "sync", ordinal, error = ?e, "failed to get block hash from ordinal, this is normal if we just finished epoch sync");
                }
            }
        }
        tracing::debug!(target: "sync", ?locator, ?ordinals);
        Ok(locator)
    }
}

/// Step back from highest to lowest ordinal, in powers of 2 steps, limited by MAX_BLOCK_HEADERS
/// heights per step, and limited by MAX_BLOCK_HEADER_HASHES steps in total.
fn get_locator_ordinals(lowest_ordinal: u64, highest_ordinal: u64) -> Vec<u64> {
    let mut current = highest_ordinal;
    let mut ordinals = vec![];
    let mut step = 2;
    while current > lowest_ordinal && ordinals.len() < MAX_BLOCK_HEADER_HASHES as usize - 1 {
        ordinals.push(current);
        if current <= lowest_ordinal + step {
            break;
        }
        current -= step;
        // Do not step back more than MAX_BLOCK_HEADERS, as the gap in between would not
        // allow us to sync to a more recent block.
        step = min(step * 2, MAX_BLOCK_HEADERS);
    }
    ordinals.push(lowest_ordinal);
    ordinals
}

#[cfg(test)]
mod test {
    use crate::sync::header::get_locator_ordinals;

    #[test]
    fn test_get_locator_ordinals() {
        assert_eq!(get_locator_ordinals(0, 0), vec![0]);
        assert_eq!(get_locator_ordinals(0, 1), vec![1, 0]);
        assert_eq!(get_locator_ordinals(0, 2), vec![2, 0]);
        assert_eq!(get_locator_ordinals(0, 3), vec![3, 1, 0]);
        assert_eq!(get_locator_ordinals(0, 10), vec![10, 8, 4, 0]);
        assert_eq!(get_locator_ordinals(0, 100), vec![100, 98, 94, 86, 70, 38, 0]);
        assert_eq!(
            get_locator_ordinals(0, 1000),
            vec![1000, 998, 994, 986, 970, 938, 874, 746, 490, 0]
        );
        // Locator is still reasonable size even given large height.
        assert_eq!(
            get_locator_ordinals(0, 10000),
            vec![
                10000, 9998, 9994, 9986, 9970, 9938, 9874, 9746, 9490, 8978, 8466, 7954, 7442,
                6930, 6418, 5906, 5394, 4882, 4370, 0
            ]
        );
        assert_eq!(get_locator_ordinals(100, 100), vec![100]);
        assert_eq!(get_locator_ordinals(100, 101), vec![101, 100]);
        assert_eq!(get_locator_ordinals(100, 102), vec![102, 100]);
        assert_eq!(get_locator_ordinals(100, 103), vec![103, 101, 100]);
        assert_eq!(get_locator_ordinals(100, 110), vec![110, 108, 104, 100]);
        assert_eq!(get_locator_ordinals(100, 200), vec![200, 198, 194, 186, 170, 138, 100]);
        assert_eq!(
            get_locator_ordinals(20000, 21000),
            vec![21000, 20998, 20994, 20986, 20970, 20938, 20874, 20746, 20490, 20000]
        );
        assert_eq!(
            get_locator_ordinals(20000, 30000),
            vec![
                30000, 29998, 29994, 29986, 29970, 29938, 29874, 29746, 29490, 28978, 28466, 27954,
                27442, 26930, 26418, 25906, 25394, 24882, 24370, 20000
            ]
        );
    }
}
