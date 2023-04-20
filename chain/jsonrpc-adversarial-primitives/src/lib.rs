use near_primitives::network::PeerId;

#[derive(serde::Deserialize)]
pub struct StartRoutingTableSyncRequest {
    pub peer_id: PeerId,
}
