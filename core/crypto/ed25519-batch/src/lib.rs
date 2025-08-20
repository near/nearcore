//! Ed25519 batch signature verification
//! Adds additional malleability protections for batch signature verification.
//! See: https://eprint.iacr.org/2020/1244.pdf, https://csrc.nist.gov/presentations/2023/taming-the-many-eddsas

mod batch;
mod errors;
mod extras;
mod signature;

pub use batch::safe_verify_batch;
pub use errors::SignatureError;
