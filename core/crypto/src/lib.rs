#![deny(clippy::arithmetic_side_effects)]

pub use errors::{ParseKeyError, ParseKeyTypeError, ParseSignatureError};
pub use key_file::KeyFile;
pub use signature::{
    ED25519PublicKey, ED25519SecretKey, KeyType, ML_DSA_65_HASH_LENGTH,
    ML_DSA_65_PUBLIC_KEY_LENGTH, ML_DSA_65_SIGNATURE_LENGTH, MlDsa65PublicKey,
    MlDsa65PublicKeyHandle, MlDsa65Signature, PublicKey, PublicKeyHandle, Secp256K1PublicKey,
    Secp256K1Signature, SecretKey, Signature,
};
pub use signer::{EmptySigner, InMemorySigner, Signer};

#[macro_use]
mod hash;
#[macro_use]
mod traits;
#[macro_use]
mod util;

mod errors;
pub mod hash_domain;
pub mod key_conversion;
mod key_file;
mod signature;
mod signer;
mod test_utils;
pub mod vrf;
