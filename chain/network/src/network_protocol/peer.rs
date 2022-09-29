/// `network_protocol.rs` contains types which are part of network protocol.
/// We need to maintain backward compatibility in network protocol.
/// All changes to this file should be reviewed.
///
/// TODO: - document all types in this file
use near_primitives::block::GenesisId;
use near_primitives::network::PeerId;
use near_primitives::types::{AccountId, BlockHeight, ShardId};
use std::fmt;
use std::net::{SocketAddr, ToSocketAddrs};
use std::str::FromStr;

/// Peer information.
#[derive(borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Debug, Eq, PartialEq)]
pub struct PeerInfo {
    pub id: PeerId,
    pub addr: Option<SocketAddr>,
    pub account_id: Option<AccountId>,
}

impl PeerInfo {
    /// Creates random peer info.
    pub fn new(id: PeerId, addr: SocketAddr) -> Self {
        PeerInfo { id, addr: Some(addr), account_id: None }
    }

    pub fn random() -> Self {
        PeerInfo { id: PeerId::random(), addr: None, account_id: None }
    }

    pub fn addr_port(&self) -> Option<u16> {
        self.addr.map(|addr| addr.port())
    }
}

// Note, `Display` automatically implements `ToString` which must be reciprocal to `FromStr`.
impl fmt::Display for PeerInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.id)?;
        if let Some(addr) = &self.addr {
            write!(f, "@{}", addr)?;
        }
        if let Some(account_id) = &self.account_id {
            write!(f, "@{}", account_id)?;
        }
        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ParsePeerInfoError {
    #[error("invalid format: {0}")]
    InvalidFormat(String),
    #[error("PeerId: {0}")]
    PeerId(#[source] near_crypto::ParseKeyError),
}

impl FromStr for PeerInfo {
    type Err = ParsePeerInfoError;
    /// Returns a PeerInfo from string
    ///
    /// Valid format examples:
    ///     ed25519:C6HLP37VJN1Wj2irxxZPsVsSya92Rnx12tqK3us5erKV
    ///     ed25519:C6HLP37VJN1Wj2irxxZPsVsSya92Rnx12tqK3us5erKV@127.0.0.1:24567
    ///     ed25519:C6HLP37VJN1Wj2irxxZPsVsSya92Rnx12tqK3us5erKV@test.near
    ///     ed25519:C6HLP37VJN1Wj2irxxZPsVsSya92Rnx12tqK3us5erKV@127.0.0.1:24567@test.near
    ///
    /// Hostname can be used instead of IP address, if node trusts DNS server it connects to, for example:
    ///     ed25519:C6HLP37VJN1Wj2irxxZPsVsSya92Rnx12tqK3us5erKV@localhost:24567@test.near
    ///     ed25519:C6HLP37VJN1Wj2irxxZPsVsSya92Rnx12tqK3us5erKV@my.own.node.test:24567@test.near
    ///
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let chunks: Vec<&str> = s.split('@').collect();
        let addr;
        let account_id;

        if chunks.len() == 1 {
            addr = None;
            account_id = None;
        } else if chunks.len() == 2 {
            if let Ok(mut x) = chunks[1].to_socket_addrs() {
                addr = x.next();
                account_id = None;
            } else {
                addr = None;
                account_id = Some(chunks[1].parse().unwrap());
            }
        } else if chunks.len() == 3 {
            if let Ok(mut x) = chunks[1].to_socket_addrs() {
                addr = x.next();
                account_id = Some(chunks[2].parse().unwrap());
            } else {
                return Err(Self::Err::InvalidFormat(s.to_string()));
            }
        } else {
            return Err(Self::Err::InvalidFormat(s.to_string()));
        }
        Ok(PeerInfo {
            id: PeerId::new(chunks[0].parse().map_err(Self::Err::PeerId)?),
            addr,
            account_id,
        })
    }
}

/// Peer chain information.
/// TODO: Remove in next version
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Debug, Eq, PartialEq, Default)]
/// Represents `peers` view about chain.
pub struct PeerChainInfo {
    /// Chain Id and hash of genesis block.
    pub genesis_id: GenesisId,
    /// Last known chain height of the peer.
    pub height: BlockHeight,
    /// Shards that the peer is tracking.
    pub tracked_shards: Vec<ShardId>,
}

/// Peer chain information.
#[derive(borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct PeerChainInfoV2 {
    /// Chain Id and hash of genesis block.
    pub genesis_id: GenesisId,
    /// Last known chain height of the peer.
    pub height: BlockHeight,
    /// Shards that the peer is tracking.
    pub tracked_shards: Vec<ShardId>,
    /// Denote if a node is running in archival mode or not.
    pub archival: bool,
}

impl From<PeerChainInfo> for PeerChainInfoV2 {
    fn from(peer_chain_info: PeerChainInfo) -> Self {
        Self {
            genesis_id: peer_chain_info.genesis_id,
            height: peer_chain_info.height,
            tracked_shards: peer_chain_info.tracked_shards,
            archival: false,
        }
    }
}

#[cfg(test)]
mod test {
    use std::net::IpAddr;
    use std::net::SocketAddr;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    /// TODO this test might require an improvement (probably by mocking the DNS resolution)
    fn test_from_str() {
        use crate::network_protocol::PeerInfo;
        use std::str::FromStr;

        let socket_v4 = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1337);
        let socket_v6 = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 1337);

        let mut peer_test = PeerInfo::from_str(
            "ed25519:C6HLP37VJN1Wj2irxxZPsVsSya92Rnx12tqK3us5erKV@localhost:1337@account.near",
        )
        .unwrap();
        assert!(peer_test.addr.unwrap() == socket_v4 || peer_test.addr.unwrap() == socket_v6);

        peer_test = PeerInfo::from_str(
            "ed25519:C6HLP37VJN1Wj2irxxZPsVsSya92Rnx12tqK3us5erKV@localhost:1337",
        )
        .unwrap();
        assert!(peer_test.addr.unwrap() == socket_v4 || peer_test.addr.unwrap() == socket_v6);

        peer_test = PeerInfo::from_str(
            "ed25519:C6HLP37VJN1Wj2irxxZPsVsSya92Rnx12tqK3us5erKV@127.0.0.1:1337@account.near",
        )
        .unwrap();
        assert!(peer_test.addr.unwrap() == socket_v4 || peer_test.addr.unwrap() == socket_v6);

        peer_test = PeerInfo::from_str(
            "ed25519:C6HLP37VJN1Wj2irxxZPsVsSya92Rnx12tqK3us5erKV@127.0.0.1:1337",
        )
        .unwrap();
        assert!(peer_test.addr.unwrap() == socket_v4 || peer_test.addr.unwrap() == socket_v6);
    }
}
