extern crate exonum_sodiumoxide as sodiumoxide;

pub use signature::sodiumoxide::crypto::sign::ed25519::Seed;

pub type PublicKey = sodiumoxide::crypto::sign::ed25519::PublicKey;
pub type SecretKey = sodiumoxide::crypto::sign::ed25519::SecretKey;
pub type Signature = sodiumoxide::crypto::sign::ed25519::Signature;


pub fn sign(data: &[u8], secret_key: &SecretKey) -> Signature {
    sodiumoxide::crypto::sign::ed25519::sign_detached(data, secret_key)
}

pub fn get_keypair_from_seed(seed: &Seed) -> (PublicKey, SecretKey) {
    sodiumoxide::crypto::sign::ed25519::keypair_from_seed(seed)
}

pub fn get_keypair() -> (PublicKey, SecretKey) {
    sodiumoxide::crypto::sign::ed25519::gen_keypair()
}
