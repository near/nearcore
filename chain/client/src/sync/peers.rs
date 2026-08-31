use near_network::types::PeerAdvertisedHead;
use near_primitives::types::BlockHeight;

/// What the client knows about the network for one sync step.
pub struct SyncPeers<'a> {
    /// Where we are syncing to: our own header head during block sync, an
    /// unverified peer claim otherwise. It picks the sync phase and the download
    /// targets.
    pub highest_height: BlockHeight,
    /// Peers the network layer reports as close to the highest advertised head.
    pub highest_height_peers: &'a [PeerAdvertisedHead],
}
