//! Extra definitions used for implementing batch signature verification with
//! additional non-malleable signature checks (`safe_verify_batch`).

use crate::errors::InternalError;

use curve25519_dalek::EdwardsPoint;
use curve25519_dalek::edwards::CompressedEdwardsY;
use ed25519_dalek::{PUBLIC_KEY_LENGTH, VerifyingKey};
use std::convert::TryFrom;

pub(crate) struct VerifyingKeyInternal {
    pub(crate) compressed: CompressedEdwardsY,
    pub(crate) point: EdwardsPoint,
}

impl VerifyingKeyInternal {
    /// Returns the compressed bytes of the verifying key.
    pub fn as_bytes(&self) -> &[u8; PUBLIC_KEY_LENGTH] {
        &(self.compressed).0
    }

    /// Checks if the verifying key is weak (i.e., has a small order).
    pub fn is_weak(&self) -> bool {
        self.point.is_small_order()
    }
}

impl TryFrom<&VerifyingKey> for VerifyingKeyInternal {
    type Error = InternalError;

    fn try_from(verifying_key: &VerifyingKey) -> Result<Self, Self::Error> {
        let compressed_bytes = verifying_key.as_bytes();
        let compressed_point = CompressedEdwardsY::from_slice(compressed_bytes).map_err(|_| {
            InternalError::BytesLength { name: "compressed_bytes", length: PUBLIC_KEY_LENGTH }
        })?;
        let point = verifying_key.to_edwards();
        Ok(Self { compressed: compressed_point, point })
    }
}

pub(crate) trait IsCanonicalY {
    fn is_canonical_y(&self) -> bool;
}

impl IsCanonicalY for CompressedEdwardsY {
    /// Checks the canonicity of a point on Curve25519.
    /// a canonic point has its y-coordinate reduced mod q (2^255 - 19)
    fn is_canonical_y(&self) -> bool {
        if self.0[0] < 237 {
            return true;
        };
        for i in 1..=30 {
            if self.0[i] != 255 {
                return true;
            }
        }
        (self.0[31] | 128) != 255
    }
}
