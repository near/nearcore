use near_network::types::PeerAdvertisedHead;
use near_primitives::types::BlockHeight;

/// What the client knows about the network for one sync step.
pub struct SyncPeers<'a> {
    /// Where we are syncing to: verified when our head is fresh, our own header
    /// head during block sync, an unverified claim otherwise. It picks the sync
    /// phase and the download targets, never anything that discards data.
    pub highest_height: BlockHeight,
    /// Highest height backed by a header with >2/3 stake approvals from an epoch
    /// we know. `None` when nothing is verifiable. Required by any decision that
    /// discards data.
    pub verified_highest_height: Option<BlockHeight>,
    /// Peers the network layer reports as close to the highest advertised head.
    pub highest_height_peers: &'a [PeerAdvertisedHead],
}
