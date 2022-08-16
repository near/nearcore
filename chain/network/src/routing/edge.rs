use near_network_primitives::time;
use near_network_primitives::types::{PartialEdgeInfo,Edge};
use near_primitives::network::{PeerId};
use crate::stats::metrics;

// Don't accept nonces (edges) that are more than this delta from current time.
// This value should be smaller than PRUNE_EDGES_AFTER (otherwise, the might accept the edge and garbage collect it seconds later).
pub(crate) const EDGE_NONCE_MAX_TIME_DELTA: time::Duration = time::Duration::minutes(20);

pub(crate) enum MergeEdgesError {
    NonceMismatch,
    InvalidSignature,
    InvalidNonce,
    NonceTooOld,
}

pub(crate) fn merge_edges(
    clock:&time::Clock,
    peer0:PeerId,
    peer1:PeerId,
    edge0:PartialEdgeInfo,
    edge1:PartialEdgeInfo,
) -> Result<Edge,MergeEdgesError> {
    if edge0.nonce!=edge1.nonce {
        return Err(MergeEdgesError::NonceMismatch);
    }
    let edge = Edge::new(
        peer0,
        peer1,
        edge0.nonce,
        edge0.signature,
        edge1.signature,
    );
    if !edge.verify() {
        return Err(MergeEdgesError::InvalidSignature);
    }
    if let Ok(maybe_nonce_timestamp) = Edge::nonce_to_utc(edge.nonce()) {
        if let Some(nonce_timestamp) = maybe_nonce_timestamp {
            if (clock.now_utc() - nonce_timestamp).abs() < EDGE_NONCE_MAX_TIME_DELTA {
                metrics::EDGE_NONCE.with_label_values(&["new_style"]).inc();
            } else {
                metrics::EDGE_NONCE.with_label_values(&["error_timestamp_too_distant"]).inc();
                return Err(MergeEdgesError::NonceTooOld);
            }
        } else {
            metrics::EDGE_NONCE.with_label_values(&["old_style"]).inc();
        }
    } else {
        return Err(MergeEdgesError::InvalidNonce);
    }
}
