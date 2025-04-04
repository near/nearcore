use crate::{PublicKey, signature, vrf};
use curve25519_dalek::edwards::{CompressedEdwardsY, EdwardsPoint};
use curve25519_dalek::ristretto::RistrettoPoint;
use std::mem::transmute;

pub fn is_valid_staking_key(public_key: &PublicKey) -> bool {
    // The valid staking key is ED25519, and can be converted to ristretto.
    match public_key {
        PublicKey::ED25519(key) => convert_public_key(key).is_some(),
        PublicKey::SECP256K1(_) => false,
    }
}

pub fn convert_public_key(key: &signature::ED25519PublicKey) -> Option<vrf::PublicKey> {
    let ep: EdwardsPoint = CompressedEdwardsY::from_slice(&key.0).ok()?.decompress()?;
    // All properly generated public keys are torsion-free. RistrettoPoint type can handle some values that are not torsion-free, but not all.
    if !ep.is_torsion_free() {
        return None;
    }
    // Unfortunately, dalek library doesn't provide a better way to do this.
    let rp: RistrettoPoint = unsafe { transmute(ep) };
    Some(vrf::PublicKey(rp.compress().to_bytes(), rp))
}

pub fn convert_secret_key(key: &signature::ED25519SecretKey) -> vrf::SecretKey {
    let b = <&[u8; 32]>::try_from(&key.0[..32]).unwrap();
    let s = ed25519_dalek::hazmat::ExpandedSecretKey::from(b).scalar;
    vrf::SecretKey::from_scalar(s)
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "rand")]
    #[test]
    fn test_conversion() {
        use super::*;

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
