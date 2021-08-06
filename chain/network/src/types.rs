use actix::dev::MessageResponse;
use actix::{Addr, Message};

use crate::peer::Peer;
use crate::routing::{Edge, EdgeInfo};

pub use near_network_primitives::types::*;

/// Actor message to consolidate potential new peer.
/// Returns if connection should be kept or dropped.
pub struct Consolidate {
    pub actor: Addr<Peer>,
    pub peer_info: PeerInfo,
    pub peer_type: PeerType,
    pub chain_info: PeerChainInfoV2,
    // Edge information from this node.
    // If this is None it implies we are outbound connection, so we need to create our
    // EdgeInfo part and send it to the other peer.
    pub this_edge_info: Option<EdgeInfo>,
    // Edge information from other node.
    pub other_edge_info: EdgeInfo,
}

impl Message for Consolidate {
    type Result = ConsolidateResponse;
}

#[derive(MessageResponse, Debug)]
pub enum ConsolidateResponse {
    Accept(Option<EdgeInfo>),
    InvalidNonce(Box<Edge>),
    Reject,
}

#[cfg(test)]
mod tests {
    use super::*;

    // NOTE: this has it's counterpart in `near-network-primitives::types::tests`
    const ALLOWED_SIZE: usize = 1 << 20;
    const NOTIFY_SIZE: usize = 1024;

    macro_rules! assert_size {
        ($type:ident) => {
            let struct_size = std::mem::size_of::<$type>();
            if struct_size >= NOTIFY_SIZE {
                println!("The size of {} is {}", stringify!($type), struct_size);
            }
            assert!(struct_size <= ALLOWED_SIZE);
        };
    }

    #[test]
    fn test_enum_size() {
        assert_size!(ConsolidateResponse);
    }

    #[test]
    fn test_struct_size() {
        assert_size!(Consolidate);
    }
}
