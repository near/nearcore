use crate::{signature, vrf, PublicKey};
use curve25519_dalek::edwards::{CompressedEdwardsY, EdwardsPoint};
use curve25519_dalek::ristretto::RistrettoPoint;
use curve25519_dalek::scalar::Scalar;
use std::mem::transmute;

pub fn is_valid_staking_key(public_key: &PublicKey) -> bool {
    // The valid staking key is ED25519, and can be converted to ristretto.
    match public_key {
        PublicKey::ED25519(key) => convert_public_key(key).is_some(),
        PublicKey::SECP256K1(_) => false,
    }
}

pub fn convert_public_key(key: &signature::ED25519PublicKey) -> Option<vrf::PublicKey> {
    let ep: EdwardsPoint = CompressedEdwardsY::from_slice(&key.0).decompress()?;
    // All properly generated public keys are torsion-free. RistrettoPoint type can handle some values that are not torsion-free, but not all.
    if !ep.is_torsion_free() {
        return None;
    }
    // Unfortunately, dalek library doesn't provide a better way to do this.
    let rp: RistrettoPoint = unsafe { transmute(ep) };
    Some(vrf::PublicKey(rp.compress().to_bytes(), rp))
}

pub fn convert_secret_key(key: &signature::ED25519SecretKey) -> vrf::SecretKey {
    let b = ed25519_dalek::ExpandedSecretKey::from(
        &ed25519_dalek::SecretKey::from_bytes(&key.0[..32]).unwrap(),
    )
    .to_bytes();
    vrf::SecretKey::from_scalar(Scalar::from_bytes_mod_order(b[0..32].try_into().unwrap()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_conversion() {
        for _ in 0..10 {
            let kk = signature::SecretKey::from_random(signature::KeyType::ED25519);
            let pk = match kk.public_key() {
                signature::PublicKey::ED25519(k) => k,
                _ => unreachable!(),
            };
            let sk = match kk {
                signature::SecretKey::ED25519(k) => k,
                _ => unreachable!(),
            };
            assert_eq!(
                convert_secret_key(&sk).public_key().clone(),
                convert_public_key(&pk).unwrap()
            );
        }
    }
}
