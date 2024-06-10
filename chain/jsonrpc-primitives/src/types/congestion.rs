use super::chunks::{ChunkReference, RpcChunkError};

// Reuse the same error as for chunk lookup since the congestion level call
// simply does a chunk lookup followed by a small and infallible computation.
pub type RpcCongestionLevelError = RpcChunkError;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct RpcCongestionLevelRequest {
    #[serde(flatten)]
    pub chunk_reference: ChunkReference,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct RpcCongestionLevelResponse {
    pub congestion_level: f64,
}
