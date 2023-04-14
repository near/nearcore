use crate::network_protocol::{Edge, InvalidNonceError};
use crate::stats::metrics;
use near_async::time;

// Don't accept nonces (edges) that are more than this delta from current time.
// This value should be smaller than PRUNE_EDGES_AFTER (otherwise, the might accept the edge and garbage collect it seconds later).
pub(crate) const EDGE_NONCE_MAX_TIME_DELTA: time::Duration = time::Duration::minutes(20);

#[derive(thiserror::Error, Debug)]
pub(crate) enum VerifyNonceError {
    #[error("{0}")]
    InvalidNonce(#[source] InvalidNonceError),
    #[error("nonce timestamp too distant in the future/past: got = {got}, now_timestamp = {now}, max_delta = {EDGE_NONCE_MAX_TIME_DELTA}")]
    NonceTimestampTooDistant { got: time::Utc, now: time::Utc },
    #[error("nonce cannot be 0")]
    ZeroNonce,
}

pub(crate) fn verify_nonce(clock: &time::Clock, nonce: u64) -> Result<(), VerifyNonceError> {
    if nonce == 0 {
        return Err(VerifyNonceError::ZeroNonce);
    }
    match Edge::nonce_to_utc(nonce) {
        Err(err) => Err(VerifyNonceError::InvalidNonce(err)),
        Ok(Some(nonce)) => {
            let now = clock.now_utc();
            if (now - nonce).abs() >= EDGE_NONCE_MAX_TIME_DELTA {
                metrics::EDGE_NONCE.with_label_values(&["error_timestamp_too_distant"]).inc();
                Err(VerifyNonceError::NonceTimestampTooDistant { got: nonce, now })
            } else {
                metrics::EDGE_NONCE.with_label_values(&["new_style"]).inc();
                Ok(())
            }
        }
        Ok(None) => {
            metrics::EDGE_NONCE.with_label_values(&["old_style"]).inc();
            Ok(())
        }
    }
}
