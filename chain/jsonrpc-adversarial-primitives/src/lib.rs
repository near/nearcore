use near_primitives::network::PeerId;

#[derive(serde::Deserialize)]
pub(crate) struct StartRoutingTableSyncRequest {
    pub(crate) peer_id: PeerId,
}
