use near_protos::network as network_proto;
use protobuf::well_known_types::UInt32Value;
use protobuf::{RepeatedField, SingularPtrField};
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
