use hkdf::Hkdf;
use near_crypto::{ED25519PublicKey, ED25519SecretKey, PublicKey, Secp256K1PublicKey, SecretKey};
use sha2::Sha256;

fn ed25519_map_secret(
    buf: &mut [u8],
    public: &ED25519PublicKey,
    secret: Option<&[u8; crate::secret::SECRET_LEN]>,
) {
    match secret {
        Some(secret) => {
            let hk = Hkdf::<Sha256>::new(None, secret);
            hk.expand(&public.0, buf).unwrap();
        }
        None => {
            buf.copy_from_slice(&public.0);
        }
    };
}

fn map_ed25519(
    public: &ED25519PublicKey,
    secret: Option<&[u8; crate::secret::SECRET_LEN]>,
) -> ED25519SecretKey {
    let mut buf = [0; ed25519_dalek::KEYPAIR_LENGTH];

    ed25519_map_secret(&mut buf[..ed25519_dalek::SECRET_KEY_LENGTH], public, secret);

    let secret_key =
        ed25519_dalek::SecretKey::from_bytes(&buf[..ed25519_dalek::SECRET_KEY_LENGTH]).unwrap();
    let public_key = ed25519_dalek::PublicKey::from(&secret_key);

    buf[ed25519_dalek::SECRET_KEY_LENGTH..].copy_from_slice(public_key.as_bytes());
    ED25519SecretKey(buf)
}

fn secp256k1_from_slice(buf: &mut [u8], public: &Secp256K1PublicKey) -> secp256k1::key::SecretKey {
    match secp256k1::key::SecretKey::from_slice(&near_crypto::SECP256K1, buf) {
        Ok(s) => s,
        Err(_) => {
            tracing::warn!(target: "mirror", "Something super unlikely occurred! SECP256K1 key mapped from {:?} is too large. Flipping most significant bit.", public);
            // If we got an error, it means that either `buf` is all zeros, or that when interpreted as a 256-bit
            // int, it is larger than the order of the secp256k1 curve. Since the order of the curve starts with 0xFF,
            // in either case flipping the first bit should work, and we can unwrap() below.
            buf[0] ^= 0x80;
            secp256k1::key::SecretKey::from_slice(&near_crypto::SECP256K1, buf).unwrap()
        }
    }
}

fn map_secp256k1(
    public: &Secp256K1PublicKey,
    secret: Option<&[u8; crate::secret::SECRET_LEN]>,
) -> secp256k1::key::SecretKey {
    let mut buf = [0; secp256k1::constants::SECRET_KEY_SIZE];

    match secret {
        Some(secret) => {
            let hk = Hkdf::<Sha256>::new(None, secret);
            hk.expand(public.as_ref(), &mut buf).unwrap();
        }
        None => {
            buf.copy_from_slice(&public.as_ref()[..secp256k1::constants::SECRET_KEY_SIZE]);
        }
    };

    secp256k1_from_slice(&mut buf, public)
}

pub(crate) fn map_key(
    key: &PublicKey,
    secret: Option<&[u8; crate::secret::SECRET_LEN]>,
) -> SecretKey {
    match key {
        PublicKey::ED25519(k) => SecretKey::ED25519(map_ed25519(k, secret)),
        PublicKey::SECP256K1(k) => SecretKey::SECP256K1(map_secp256k1(k, secret)),
    }
}
