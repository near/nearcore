use near_primitives::network::PeerId;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct StartRoutingTableSyncRequest {
    pub peer_id: PeerId,
}
