use std::borrow::Borrow;
use std::convert::{Into, TryFrom, TryInto};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::iter::FromIterator;
use std::net::SocketAddr;

use crate::hash::CryptoHash;
use crate::types::{AccountId, PeerId};
use crate::utils::to_string_value;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerAddr {
    pub id: PeerId,
    pub addr: SocketAddr,
}

impl PeerAddr {
    pub fn parse(addr_id: &str) -> Result<Self, Box<std::error::Error>> {
        let addr_id: Vec<_> = addr_id.split('/').collect();
        let (addr, id) = (addr_id[0], addr_id[1]);
        Ok(PeerAddr {
            id: id.to_string().try_into()?,
            addr: addr
                .parse::<SocketAddr>()
                .map_err(|e| format!("Error parsing address {:?}: {:?}", addr, e))?,
        })
    }
}

impl Display for PeerAddr {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}/{}", self.addr, self.id)
    }
}

impl TryFrom<PeerInfo> for PeerAddr {
    type Error = Box<std::error::Error>;

    fn try_from(peer_info: PeerInfo) -> Result<Self, Self::Error> {
        match peer_info.addr {
            Some(addr) => Ok(PeerAddr { id: peer_info.id, addr }),
            None => Err(format!("PeerInfo {:?} doesn't have an address", peer_info).into()),
        }
    }
}

/// Info about the peer. If peer is an authority then we also know its account id.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerInfo {
    pub id: PeerId,
    pub addr: Option<SocketAddr>,
    pub account_id: Option<AccountId>,
}

impl PeerInfo {
    pub fn addr_port(&self) -> Option<u16> {
        self.addr.map(|addr| addr.port())
    }
}

impl PartialEq for PeerInfo {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Hash for PeerInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl Eq for PeerInfo {}

impl fmt::Display for PeerInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(acc) = self.account_id.as_ref() {
            write!(f, "({}, {:?}, {})", self.id, self.addr, acc)
        } else {
            write!(f, "({}, {:?})", self.id, self.addr)
        }
    }
}

impl Borrow<PeerId> for PeerInfo {
    fn borrow(&self) -> &PeerId {
        &self.id
    }
}

impl From<PeerAddr> for PeerInfo {
    fn from(node_addr: PeerAddr) -> Self {
        PeerInfo { id: node_addr.id, addr: Some(node_addr.addr), account_id: None }
    }
}
