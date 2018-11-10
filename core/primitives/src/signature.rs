extern crate exonum_sodiumoxide as sodiumoxide;

pub use signature::sodiumoxide::crypto::sign::ed25519::{Signature, SecretKey, PublicKey, Seed};


pub fn sign(data: &[u8], secret_key: &SecretKey) -> Signature {
    sodiumoxide::crypto::sign::ed25519::sign_detached(data, secret_key)
}

pub fn get_keypair_from_seed(seed: &Seed) -> (PublicKey, SecretKey) {
    sodiumoxide::crypto::sign::ed25519::keypair_from_seed(seed)
}

pub fn get_keypair() -> (PublicKey, SecretKey) {
    sodiumoxide::crypto::sign::ed25519::gen_keypair()
}
