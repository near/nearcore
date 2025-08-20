//! Ed25519 batch signature verification
//! Adds additional malleability protections for batch signature verification.
//! See: https://eprint.iacr.org/2020/1244.pdf, https://csrc.nist.gov/presentations/2023/taming-the-many-eddsas

mod batch;
mod errors;
mod extras;
mod signature;

pub use batch::safe_verify_batch;
pub use errors::SignatureError;

pub mod test_utils {
    // Arbitrary message tests and benchmarks can use to sign.
    pub const MESSAGE_TO_SIGN: &[u8] =
        b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
}
